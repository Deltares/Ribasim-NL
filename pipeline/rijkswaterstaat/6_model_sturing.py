# %%
import warnings
from typing import Literal, cast

import geopandas as gpd
import numpy as np
import openpyxl
import pandas as pd
from ribasim import Node
from ribasim.nodes import (
    continuous_control,
    outlet,
    pid_control,
    pump,
)
from ribasim_nl.rws_kunstwerken import (
    FLOW_DEMAND_TABLE,
    PARALLEL_KWK,
    RWS_NODE_ID_OFFSET,
    SUBTRACT_PROPERTY,
    discrete_control_tables,
    flow_demand_tables,
    has_discrete_control,
    has_flow_demand,
    line_along,
    listen_node,
    listen_variable,
    read_control_thresholds,
    read_kwk_properties,
    read_qq_curve,
    read_rating_curve,
    read_static,
)
from ribasim_nl.split_basins import split_basin_along_route
from shapely.geometry import LineString, Point

from ribasim_nl import CloudStorage, Model, Network

warnings.filterwarnings(
    action="ignore",
    module="geopandas",
    message="CRS not set for some of the concatenation inputs.",
)

warnings.filterwarnings(
    action="ignore",
    module="openpyxl",
    message="Data Validation extension is not supported and will be removed",
)


cloud = CloudStorage()

# Structures of a verdeelsleutel, which 5_model_netwerk adds, that are replaced by their own sheet
# Keersluis Limmel gets a DiscreteControl that closes it at a high Maas, see https://github.com/Deltares/Ribasim-NL/issues/922
VERDEELSLEUTEL_KWK_FROM_SHEET = ["61F-001-01"]
# New structures and their control get free IDs, so the IDs of the nodes added after them stay the same: the structures
# next to another one, see `PARALLEL_KWK`, and all structures in these areas
GEBIEDEN_WITH_FREE_IDS = {"ARK-IJ-NZK", "IJsselmeer-Markermeer", "Rijn", "Rijn-Maasmonding", "Waal"}
# The DiscreteControl of these structures takes the ID of the ContinuousControl of the QQ relation that it replaced, so
# the IDs of the nodes added after them stay the same, see https://github.com/Deltares/Ribasim-NL/issues/973
CONTROL_NODE_IDS = {"39B-002-02": 10724, "39D-001-02": 10725}  # Prs. Irenesluis-Peilbeheer, Pr. Bernhardsluis


# %% functies
def read_outlet(kwk_df, listen_side: Literal["upstream", "downstream"] | None = None):
    if "QQ relatie" in kwk_df["Eigenschap"].to_numpy():
        qq_properties = read_qq_curve(kwk_df)
        return outlet.Static(
            flow_rate=[max(qq_properties["flow_rate"].to_list())],
        )
    else:
        return read_static(kwk_df, "Outlet", listen_side)


def add_flow_demand(model: Model, node_id: int, tables: list, name: str, demand_node_id: int | None = None) -> None:
    """Add a FlowDemand node next to structure `node_id`, as the minimum flow through it."""
    point = cast(Point, model.node.df.at[node_id, "geometry"])
    demand_node = model.flow_demand.add(
        Node(
            model.next_node_id if demand_node_id is None else demand_node_id,
            Point(point.x + 20, point.y - 20),
            name=name,
        ),
        tables,
    )
    model.link.add(demand_node, model.get_node(node_id))


def add_parallel_kwk(model: Model, network: Network, code: str, name: str, node_type: str, node_id: int) -> None:
    """Add the structure with `code` next to its main structure, see `PARALLEL_KWK`, with a Static table to replace."""
    parallel = PARALLEL_KWK[code]
    main_id = model.find_node_id(meta_code_waterbeheerder=parallel.main)
    upstream, downstream = model.upstream_node_id(main_id), model.downstream_node_id(main_id)
    assert isinstance(upstream, int | np.integer) and isinstance(downstream, int | np.integer), (
        f"Expected one node up- and downstream of {main_id}, got {upstream} and {downstream}"
    )
    if parallel.reverse:
        upstream, downstream = downstream, upstream
    main_point = cast(Point, model.node.df.at[main_id, "geometry"])
    point = Point(parallel.xy) if parallel.xy else Point(main_point.x + parallel.offset, main_point.y + parallel.offset)
    static = {"Outlet": outlet.Static, "Pump": pump.Static}[node_type](flow_rate=[0.0])
    node = model.get_component(node_type).add(Node(node_id, point, name=name, meta_code_waterbeheerder=code), [static])
    model.link.add(model.get_node(int(upstream)), node)
    downstream_line = None
    if parallel.downstream_network_node is not None:
        line = network.get_line(parallel.downstream_network_node, int(downstream), directed=False)
        downstream_line = LineString([*point.coords, *line.coords])
    model.link.add(node, model.get_node(int(downstream)), geometry=downstream_line)


def read_pid(control_properties, control_basin_id):
    if control_properties["Controle benedenstrooms"]:
        p = 500000
        i = 1e-07
    else:
        p = -500000
        i = -1e-07

    return [
        pid_control.Static(
            listen_node_id=[control_basin_id],
            target=[control_properties["Streefpeil bovenstrooms (m +NAP)"]],
            listen_node_type="Basin",
            proportional=[p],
            integral=[i],
            derivative=[0.0],
        )
    ]


# %% Paden
ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_netwerk/hws.toml")
kwk_dir = cloud.joinpath("Rijkswaterstaat/verwerkt/kunstwerken")
kwk_xlsx = kwk_dir.joinpath("kunstwerken.xlsx")

# %%Inlezen
all_kwk_df = pd.read_excel(kwk_xlsx, sheet_name="kunstwerken")
all_kwk_df.loc[:, "code"] = all_kwk_df["code"].astype(str)
model = Model.read(ribasim_toml)
network = Network.from_network_gpkg(cloud.joinpath("Rijkswaterstaat/verwerkt/netwerk.gpkg"))
starttime, endtime = pd.Timestamp(model.starttime), pd.Timestamp(model.endtime)

# %% IDs that no node has
free_node_ids = iter(sorted(set(range(1, model.next_node_id)) - set(model.node.df.index)))

# %% Itereren over kunstwerken


def update_kwk(gebied: str, kwk, kwk_df: pd.DataFrame) -> None:
    """Update structure `kwk` of `gebied` from its sheet, and add its control and FlowDemand."""
    kwk_properties = read_kwk_properties(kwk_df)
    free_ids = gebied in GEBIEDEN_WITH_FREE_IDS or kwk.code in PARALLEL_KWK

    # a structure next to another one is not in the network yet
    if kwk.code in PARALLEL_KWK:
        add_parallel_kwk(
            model, network, str(kwk.code), str(kwk.naam), kwk_properties["Ribasim type"], next(free_node_ids)
        )

    # find existing node_id in model
    node_id = model.find_node_id(meta_code_waterbeheerder=str(kwk_properties["Kunstwerkcode"]))

    # prepare static-data for updating node
    node_type = kwk_properties["Ribasim type"]
    listen_node_id, listen_side = (
        listen_node(model, kwk_df, node_id) if has_discrete_control(kwk_properties) else (None, None)
    )
    if node_type == "TabulatedRatingCurve":
        data = [read_rating_curve(kwk_df)]
    elif node_type == "Outlet":
        data = [read_outlet(kwk_df, listen_side)]
    elif node_type == "Pump":
        data = [read_static(kwk_df, "Pump", listen_side)]
    else:
        raise ValueError(f"node-type {node_type} not yet implemented")

    # update model
    model.update_node(
        node_id=node_id,
        node_type=node_type,
        data=data,
        node_properties={"name": kwk.naam},
    )

    # check if structure has pid control
    if "PidControl" in kwk_df["Eigenschap"].to_numpy():
        control_properties = kwk_df.loc[kwk_df.Eigenschap.to_list().index("PidControl") + 1 :][
            ["Eigenschap", "Waarde"]
        ].set_index("Eigenschap")["Waarde"]

        if control_properties["Controle benedenstrooms"]:
            # waterlichaam should be upstream
            control_basin_id = model.find_node_id(
                us_node_id=node_id,
                name=control_properties["Waterlichaam"],
            )
        else:  # waterlichaam should be donstream
            control_basin_id = model.find_node_id(
                ds_node_id=node_id,
                name=control_properties["Waterlichaam"],
            )

        # add control node to network
        model.add_control_node(
            to_node_id=node_id,
            data=read_pid(control_properties, control_basin_id),
            ctrl_type="PidControl",
            node_offset=20,
        )
        # check if network-direction is correct with control-parameters

    # check if structure has qq control
    if "QQ relatie" in kwk_df["Eigenschap"].to_numpy():
        qq_properties = read_qq_curve(kwk_df)

        qq_listen_node_id = model.find_node_id(meta_meetlocatie_code=str(kwk_properties["Meetlocatiecode"]))

        data = [
            continuous_control.Function(
                input=qq_properties["condition_flow_rate"].to_list(),
                output=qq_properties["flow_rate"].to_list(),
                controlled_variable="flow_rate",
            ),
            continuous_control.Variable(
                listen_node_id=[qq_listen_node_id],
                variable=["flow_rate"],
            ),
        ]

        model.add_control_node(
            to_node_id=node_id,
            data=data,
            ctrl_type="ContinuousControl",
            node_offset=20,
        )

    # check if structure has a DiscreteControl between aanvoer and afvoer
    if has_discrete_control(kwk_properties):
        assert listen_node_id is not None
        assert model.upstream_node_id(node_id, link_type="control") is None, f"{kwk.naam} has another control"
        thresholds = read_control_thresholds(kwk_df)
        label = thresholds["control_state"].iloc[0]
        levels = (
            f"{thresholds.threshold_high.iloc[0]:.3f}/{thresholds.threshold_low.iloc[0]:.3f}"
            if len(thresholds) == 1
            else "jaarpatroon"
        )
        subtract_node_id = None
        if SUBTRACT_PROPERTY in kwk_properties:
            subtract_node_id = int(kwk_properties[SUBTRACT_PROPERTY]) - RWS_NODE_ID_OFFSET
            assert model.node.df.at[subtract_node_id, "node_type"] == "Basin", f"{subtract_node_id} not a Basin"
        unit = "m3/s" if listen_variable(kwk_properties) == "flow_rate" else "m" if subtract_node_id else "m+NAP"
        control_node_id = None
        if kwk.code in CONTROL_NODE_IDS:
            control_node_id = CONTROL_NODE_IDS[kwk.code]
            assert control_node_id not in model.node.df.index, f"Node {control_node_id} for {kwk.naam} already exists"
        elif free_ids:
            control_node_id = next(free_node_ids)
        model.add_control_node(
            to_node_id=node_id,
            data=discrete_control_tables(kwk_df, listen_node_id, starttime, endtime, subtract_node_id),
            ctrl_type="DiscreteControl",
            node_offset=20,
            node_properties={"name": f"{label}: {levels} [{unit}]"},
            node_id=control_node_id,
        )

    # check if structure has a minimum flow, such as the losses of a lock
    if has_flow_demand(kwk_properties):
        tables = flow_demand_tables(kwk_df, starttime, endtime)
        static_df = model.get_component(node_type).static.df
        assert static_df is not None
        static_rows = static_df[static_df.node_id == node_id]
        # without allocation a FlowDemand is a minimum flow, also from an empty Basin, so it needs a level to stop
        assert static_rows["min_upstream_level"].notna().all(), f"FlowDemand of {kwk.naam} needs an upstream level"
        max_flow_rate = static_rows["max_flow_rate"].min()
        assert all(table.df is not None for table in tables)
        demand = max(cast(pd.DataFrame, table.df)["demand"].max() for table in tables)
        assert demand <= max_flow_rate, f"FlowDemand {demand} of {kwk.naam} above its capacity {max_flow_rate}"
        add_flow_demand(
            model,
            node_id,
            tables,
            name=f"minimum debiet {kwk.naam}: {demand} [m3/s]",
            demand_node_id=next(free_node_ids) if free_ids else None,
        )


def listens_to_missing_node(kwk_properties: pd.Series) -> bool:
    """Whether the DiscreteControl of a structure listens to a node that is not in the model yet."""
    if not has_discrete_control(kwk_properties) or "Meetlocatiecode" not in kwk_properties:
        return False
    return int(kwk_properties["Meetlocatiecode"]) - RWS_NODE_ID_OFFSET not in model.node.df.index


assert set(VERDEELSLEUTEL_KWK_FROM_SHEET) <= set(all_kwk_df.loc[~all_kwk_df.in_model, "code"])
all_kwk_df = all_kwk_df[all_kwk_df.in_model | all_kwk_df.code.isin(VERDEELSLEUTEL_KWK_FROM_SHEET)]
assert set(PARALLEL_KWK) <= set(all_kwk_df.code), "Expected the structures next to another one in kunstwerken.xlsx"
assert set(CONTROL_NODE_IDS) <= set(all_kwk_df.code), "Expected the structures with a control node ID"
# the structures that listen to a section of the Amsterdam-Rijnkanaal, which is split below
after_split = []
for gebied, kwks_df in all_kwk_df.groupby(by="gebied"):
    # get sheet-names
    file_name = kwk_dir / f"{gebied}.xlsx"
    workbook = openpyxl.open(file_name)
    sheet_names = workbook.sheetnames
    workbook.close()

    for kwk in kwks_df.itertuples():
        print(f"updating {kwk.naam}")
        # check if naam column exists as sheet in xlsx and read sheet
        if kwk.naam not in sheet_names:
            raise ValueError(f"{kwk.naam} not a sheet in {file_name}")

        kwk_df = pd.read_excel(file_name, sheet_name=cast(str, kwk.naam))
        kwk_properties = read_kwk_properties(kwk_df)

        # check if code-value is the same in both Excels
        if kwk_properties["Kunstwerkcode"] != kwk.code:
            raise ValueError(
                f"code for {kwk.naam} do not match in `{file_name.name}` and `{kwk_xlsx.name}`: {kwk_properties['Kunstwerkcode']} != {kwk.code}"
            )
        # a FlowDemand table is only read with `FlowDemand` ja
        has_demand_table = FLOW_DEMAND_TABLE in kwk_df["Eigenschap"].to_numpy()
        assert has_demand_table == has_flow_demand(kwk_properties), (
            f"FlowDemand table and property of {kwk.naam} differ"
        )
        if listens_to_missing_node(kwk_properties):
            after_split.append((str(gebied), kwk, kwk_df))
        else:
            update_kwk(str(gebied), kwk, kwk_df)

# %% Split the Amsterdam-Rijnkanaal north of the Lek in three, see https://github.com/Deltares/Ribasim-NL/issues/916
# It runs about 59 km from the Prinses Irenesluis to ManningResistance 4695 at the Noordzeekanaal. It is split after
# the control of the other structures, so the IDs of their nodes stay the same, and before the demands are added in
# 7_model_onttrekkingen, so they and the couplings connect to the section they are at.
ARK, IRENESLUIS, ARK_END = 5018, 5930, 4695
flow_df = model.link.df[model.link.df.link_type == "flow"]
(upstream_link_id,) = flow_df.index[(flow_df.from_node_id == IRENESLUIS) & (flow_df.to_node_id == ARK)]
(downstream_link_id,) = flow_df.index[(flow_df.from_node_id == ARK) & (flow_df.to_node_id == ARK_END)]
upstream_line = cast(LineString, flow_df.at[upstream_link_id, "geometry"])
downstream_line = cast(LineString, flow_df.at[downstream_link_id, "geometry"])
ark_route = LineString([*upstream_line.coords, *downstream_line.coords[1:]])
ark_basin_ids, _ = split_basin_along_route(
    model,
    ARK,
    ark_route,
    # ARK-Zuilen after gemaal De Aanvoerder, and ARK-Nigtevecht before Nigtevecht
    split_points=[Point(133742, 457096), Point(129815, 475689)],
    basin_names=["Amsterdam-Rijnkanaal Utrecht", "Amsterdam-Rijnkanaal Breukelen", "Amsterdam-Rijnkanaal Weesp"],
    manning_names=["ARK-Zuilen", "ARK-Nigtevecht"],
    manning_n=0.04,  # like ManningResistance 4695
    profile_slope=1.0,
)
assert ark_basin_ids[1] == ARK, "The Basin node of the ARK lies in Breukelen"
# Like the other ManningResistances, see 5_model_netwerk.py, the one at the end has the length of its links
assert model.manning_resistance.static.df is not None
end_links = model.link.df[(model.link.df.from_node_id == ARK_END) | (model.link.df.to_node_id == ARK_END)]
assert len(end_links) == 2, f"Expected two links of ManningResistance {ARK_END}"
model.manning_resistance.static.df.loc[model.manning_resistance.static.df.node_id == ARK_END, "length"] = (
    end_links.length.sum()
)

# %% The structures that listen to a section of the Amsterdam-Rijnkanaal, such as the Prinses Irenesluis, see #973
for gebied, kwk, kwk_df in after_split:
    print(f"updating {kwk.naam}")
    assert not listens_to_missing_node(read_kwk_properties(kwk_df)), f"{kwk.naam} listens to a node not in the model"
    update_kwk(gebied, kwk, kwk_df)

# %% The Goereese sluis drains to the sea along its channel and the fairway, which are not in the network
goereese_sluis = model.find_node_id(meta_code_waterbeheerder="37C-001-01")
sea = model.downstream_node_id(goereese_sluis)
# from just upstream of the sluis to past the boundary
goereese_bbox = (61500, 426960, 62200, 428900)
waterways = pd.concat(
    gpd.read_file(cloud.joinpath(f"Basisgegevens/OSM/waterway_{kind}.gpkg"), bbox=goereese_bbox)
    for kind in ("canal", "fairway")
)
goereese_line = line_along(
    waterways.geometry.clip_by_rect(*goereese_bbox).explode(),
    cast(Point, model.node.df.at[goereese_sluis, "geometry"]),
    cast(Point, model.node.df.at[sea, "geometry"]),
)
links = model.link.df
(goereese_link,) = links.index[(links.from_node_id == goereese_sluis) & (links.to_node_id == sea)]
links.loc[goereese_link, ["geometry"]] = goereese_line  # ty: ignore[invalid-assignment]

# %% write
ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_sturing/hws.toml")

model.write(ribasim_toml)

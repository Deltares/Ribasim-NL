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
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.rws_kunstwerken import (
    PARALLEL_KWK,
    SUBTRACT_PROPERTY,
    discrete_control_tables,
    flow_demand_tables,
    has_discrete_control,
    has_flow_demand,
    line_along,
    read_control_thresholds,
    read_kwk_properties,
    read_qq_curve,
    read_rating_curve,
    read_static,
)
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

UPDATE_KWK_DATA = True
# Structures of a verdeelsleutel, which 5_model_netwerk adds, that are replaced by their own sheet
# Keersluis Limmel gets a DiscreteControl that closes it at a high Maas, see https://github.com/Deltares/Ribasim-NL/issues/922
VERDEELSLEUTEL_KWK_FROM_SHEET = ["61F-001-01"]
# Node types between Basins that do not control the flow, so a DiscreteControl may listen across them
FREE_FLOW_NODE_TYPES = {"Basin", "ManningResistance", "LinearResistance", "Junction"}
RWS_NODE_ID_OFFSET = waterbeheercode["Rijkswaterstaat"] * 100_000
# New structures and their control get free IDs, so the IDs of the nodes added after them stay the same: the structures
# next to another one, see `PARALLEL_KWK`, and all structures in these areas
GEBIEDEN_WITH_FREE_IDS = {"IJsselmeer-Markermeer", "Rijn", "Rijn-Maasmonding", "Waal"}


# %% functies
def read_outlet(kwk_df, listen_side: Literal["upstream", "downstream"] | None = None):
    if "QQ relatie" in kwk_df["Eigenschap"].to_numpy():
        qq_properties = read_qq_curve(kwk_df)
        return outlet.Static(
            flow_rate=[max(qq_properties["flow_rate"].to_list())],
        )
    else:
        return read_static(kwk_df, "Outlet", listen_side)


def free_flow_basin_ids(model: Model, basin_id: int) -> set[int]:
    """Basins connected to `basin_id` without a structure that controls the flow in between."""
    links = model.link.df[model.link.df.link_type == "flow"]
    node_type = model.node.df["node_type"]
    found, front = {basin_id}, [basin_id]
    while front:
        node_id = front.pop()
        neighbors = links.loc[links.from_node_id == node_id, "to_node_id"].to_list()
        neighbors += links.loc[links.to_node_id == node_id, "from_node_id"].to_list()
        for neighbor in neighbors:
            if neighbor not in found and node_type[neighbor] in FREE_FLOW_NODE_TYPES:
                found.add(neighbor)
                front.append(neighbor)
    return {i for i in found if node_type[i] == "Basin"}


def listen_basin(model: Model, kwk_df: pd.DataFrame, node_id: int) -> tuple[int, Literal["upstream", "downstream"]]:
    """The Basin whose level switches the DiscreteControl of structure `node_id`, and its side of the structure.

    The Meetlocatiecode is the ID of that Basin in the coupled model. Without it, an afvoer structure listens to the
    Basin upstream, and an aanvoer structure to the Basin downstream. The Basin should be next to the structure, or
    connected to the Basin on one side of it, possibly through other Basins without structures in between, like the
    Veluwerandmeren.
    """
    kwk_properties = read_kwk_properties(kwk_df)
    upstream = model.upstream_node_id(node_id)
    downstream = model.downstream_node_id(node_id)
    assert isinstance(upstream, int | np.integer) and isinstance(downstream, int | np.integer), (
        f"Expected one node up- and downstream of {node_id}, got {upstream} and {downstream}"
    )
    if "Meetlocatiecode" in kwk_properties:
        basin_id = int(kwk_properties["Meetlocatiecode"]) - RWS_NODE_ID_OFFSET
        assert basin_id in model.node.df.index, f"Meetlocatiecode {kwk_properties['Meetlocatiecode']} not in the model"
    else:
        label = read_control_thresholds(kwk_df)["control_state"].iloc[0]
        basin_id = int(upstream if label == "afvoer" else downstream)
    assert model.node.df.at[basin_id, "node_type"] == "Basin", f"DiscreteControl listens to {basin_id}, not a Basin"
    neighbors: list[tuple[Literal["upstream", "downstream"], int]] = [
        ("upstream", int(upstream)),
        ("downstream", int(downstream)),
    ]
    # both sides can be connected further away, like the Waal and the Maas through the Merwede and the Hollandsch Diep
    sides = [side for side, neighbor in neighbors if neighbor == basin_id] or [
        side
        for side, neighbor in neighbors
        if model.node.df.at[neighbor, "node_type"] == "Basin" and basin_id in free_flow_basin_ids(model, neighbor)
    ]
    assert len(sides) == 1, f"Basin {basin_id} {model.node.df.at[basin_id, 'name']!r} not on one side of {node_id}"
    return basin_id, sides[0]


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

assert set(VERDEELSLEUTEL_KWK_FROM_SHEET) <= set(all_kwk_df.loc[~all_kwk_df.in_model, "code"])
all_kwk_df = all_kwk_df[all_kwk_df.in_model | all_kwk_df.code.isin(VERDEELSLEUTEL_KWK_FROM_SHEET)]
assert set(PARALLEL_KWK) <= set(all_kwk_df.code), "Expected the structures next to another one in kunstwerken.xlsx"
for gebied, kwks_df in all_kwk_df.groupby(by="gebied"):
    # get sheet-names
    file_name = kwk_dir / f"{gebied}.xlsx"
    workbook = openpyxl.open(file_name)
    sheet_names = workbook.sheetnames
    workbook.close()

    # kwk = next(i for i in kwks_df.itertuples() if i.naam == "Pr. Bernhardsluis")
    for kwk in kwks_df.itertuples():
        print(f"updating {kwk.naam}")
        # check if naam column exists as sheet in xlsx and read sheet
        if kwk.naam not in sheet_names:
            raise ValueError(f"{kwk.naam} not a sheet in {file_name}")

        kwk_df = pd.read_excel(file_name, sheet_name=cast(str, kwk.naam))

        # get properties
        kwk_properties = read_kwk_properties(kwk_df)
        free_ids = gebied in GEBIEDEN_WITH_FREE_IDS or kwk.code in PARALLEL_KWK

        # check if code-value is the same in both Excels
        if kwk_properties["Kunstwerkcode"] != kwk.code:
            raise ValueError(
                f"code for {kwk.naam} do not match in `{file_name.name}` and `{kwk_xlsx.name}`: {kwk_properties['Kunstwerkcode']} != {kwk.code}"
            )

        # a structure next to another one is not in the network yet
        if kwk.code in PARALLEL_KWK:
            add_parallel_kwk(
                model, network, str(kwk.code), str(kwk.naam), kwk_properties["Ribasim type"], next(free_node_ids)
            )

        # find existing node_id in model
        node_id = model.find_node_id(meta_code_waterbeheerder=str(kwk_properties["Kunstwerkcode"]))

        listen_basin_id = None
        if UPDATE_KWK_DATA:
            # prepare static-data for updating node
            node_type = kwk_properties["Ribasim type"]
            listen_basin_id, listen_side = (
                listen_basin(model, kwk_df, node_id) if has_discrete_control(kwk_properties) else (None, None)
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

            listen_node_id = model.find_node_id(meta_meetlocatie_code=str(kwk_properties["Meetlocatiecode"]))

            data = [
                continuous_control.Function(
                    input=qq_properties["condition_flow_rate"].to_list(),
                    output=qq_properties["flow_rate"].to_list(),
                    controlled_variable="flow_rate",
                ),
                continuous_control.Variable(
                    listen_node_id=[listen_node_id],
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
            assert listen_basin_id is not None, f"DiscreteControl of {kwk.naam} needs UPDATE_KWK_DATA"
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
            model.add_control_node(
                to_node_id=node_id,
                data=discrete_control_tables(kwk_df, listen_basin_id, starttime, endtime, subtract_node_id),
                ctrl_type="DiscreteControl",
                node_offset=20,
                node_properties={"name": f"{label}: {levels} [m{'' if subtract_node_id else '+NAP'}]"},
                node_id=next(free_node_ids) if free_ids else None,
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

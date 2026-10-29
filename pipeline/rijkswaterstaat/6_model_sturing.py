# %%
import warnings
from typing import cast

import numpy as np
import openpyxl
import pandas as pd
from ribasim import Node
from ribasim.nodes import (
    continuous_control,
    outlet,
    pid_control,
)
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.rws_kunstwerken import (
    discrete_control_tables,
    flow_demand_tables,
    has_discrete_control,
    has_flow_demand,
    read_control_thresholds,
    read_kwk_properties,
    read_qq_curve,
    read_rating_curve,
    read_static,
)
from shapely.geometry import Point

from ribasim_nl import CloudStorage, Model

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


# %% functies
def read_outlet(kwk_df):
    if "QQ relatie" in kwk_df["Eigenschap"].to_numpy():
        qq_properties = read_qq_curve(kwk_df)
        return outlet.Static(
            flow_rate=[max(qq_properties["flow_rate"].to_list())],
        )
    else:
        return read_static(kwk_df, "Outlet")


def free_flow_basin_ids(model: Model, basin_id: int) -> set[int]:
    """Basins connected to `basin_id` without a structure that controls the flow in between."""
    assert model.link.df is not None and model.node.df is not None
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


def listen_basin_id(model: Model, kwk_df: pd.DataFrame, node_id: int) -> int:
    """The Basin whose level switches the DiscreteControl of structure `node_id`.

    The Meetlocatiecode is the ID of that Basin in the coupled model. Without it, an afvoer structure listens to the
    Basin upstream, and an aanvoer structure to the Basin downstream. The Basin should be connected to the
    structure, possibly through other Basins without structures in between, like the Veluwerandmeren.
    """
    assert model.node.df is not None
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
    neighbors = {int(i) for i in (upstream, downstream) if model.node.df.at[i, "node_type"] == "Basin"}
    assert any(basin_id in free_flow_basin_ids(model, i) for i in neighbors), (
        f"Basin {basin_id} {model.node.df.at[basin_id, 'name']!r} is not connected to structure {node_id}"
    )
    return basin_id


def add_flow_demand(model: Model, node_id: int, tables: list, name: str) -> None:
    """Add a FlowDemand node next to structure `node_id`, as the minimum flow through it."""
    assert model.flow_demand.node is not None and model.node.df is not None
    point = cast(Point, model.node.df.at[node_id, "geometry"])
    demand_node = model.flow_demand.add(
        Node(model.next_node_id, Point(point.x + 20, point.y - 20), name=name),
        tables,
    )
    model.link.add(demand_node, model.get_node(node_id))


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
starttime, endtime = pd.Timestamp(model.starttime), pd.Timestamp(model.endtime)

# %% Itereren over kunstwerken

assert set(VERDEELSLEUTEL_KWK_FROM_SHEET) <= set(all_kwk_df.loc[~all_kwk_df.in_model, "code"])
all_kwk_df = all_kwk_df[all_kwk_df.in_model | all_kwk_df.code.isin(VERDEELSLEUTEL_KWK_FROM_SHEET)]
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

        # check if code-value is the same in both Excels
        if kwk_properties["Kunstwerkcode"] != kwk.code:
            raise ValueError(
                f"code for {kwk.naam} do not match in `{file_name.name}` and `{kwk_xlsx.name}`: {kwk_properties['Kunstwerkcode']} != {kwk.code}"
            )

        # find existing node_id in model
        node_id = model.find_node_id(meta_code_waterbeheerder=str(kwk_properties["Kunstwerkcode"]))

        if UPDATE_KWK_DATA:
            # prepare static-data for updating node
            node_type = kwk_properties["Ribasim type"]
            if node_type == "TabulatedRatingCurve":
                data = [read_rating_curve(kwk_df)]
            elif node_type == "Outlet":
                data = [read_outlet(kwk_df)]
            elif node_type == "Pump":
                data = [read_static(kwk_df, "Pump")]
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
            assert model.upstream_node_id(node_id, link_type="control") is None, f"{kwk.naam} has another control"
            thresholds = read_control_thresholds(kwk_df)
            label = thresholds["control_state"].iloc[0]
            levels = (
                f"{thresholds.threshold_high.iloc[0]:.3f}/{thresholds.threshold_low.iloc[0]:.3f}"
                if len(thresholds) == 1
                else "jaarpatroon"
            )
            model.add_control_node(
                to_node_id=node_id,
                data=discrete_control_tables(kwk_df, listen_basin_id(model, kwk_df, node_id), starttime, endtime),
                ctrl_type="DiscreteControl",
                node_offset=20,
                node_properties={"name": f"{label}: {levels} [m+NAP]"},
            )

        # check if structure has a minimum flow, such as the losses of a lock
        if has_flow_demand(kwk_properties):
            tables = flow_demand_tables(kwk_df, starttime, endtime)
            static_df = model.get_component(node_type).static.df
            assert static_df is not None
            max_flow_rate = static_df.loc[static_df.node_id == node_id, "max_flow_rate"].min()
            assert all(table.df is not None for table in tables)
            demand = max(cast(pd.DataFrame, table.df)["demand"].max() for table in tables)
            assert demand <= max_flow_rate, f"FlowDemand {demand} of {kwk.naam} above its capacity {max_flow_rate}"
            add_flow_demand(model, node_id, tables, name=f"minimum debiet {kwk.naam}: {demand} [m3/s]")

# %% write
ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_sturing/hws.toml")

model.write(ribasim_toml)

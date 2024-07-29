# %%
from datetime import datetime

import pandas as pd
import ribasim
from ribasim import nodes
from ribasim_nl import CloudStorage, discrete_control
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.model import add_control_node_to_network
from ribasim_nl.verdeelsleutels import read_verdeelsleutel, verdeelsleutel_to_control

cloud = CloudStorage()
waterbeheerder = "Rijkswaterstaat"

ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_network_upgraded", "hws.toml")
model = ribasim.Model.read(ribasim_toml)

verdeelsleutel_df = read_verdeelsleutel(cloud.joinpath("Rijkswaterstaat", "verwerkt", "verdeelsleutel_driel.xlsx"))


def add_pid(node_id, node_type, ctrl_node, pid):
    kwargs = {k: [v] for k, v in pid.items() if k != "listen_node_type"}
    model.pid_control.add(
        ctrl_node,
        [nodes.pid_control.Static(listen_node_type=pid["listen_node_type"], **kwargs)],
    )

    model.edge.add(
        model.pid_control[ctrl_node.node_id],
        getattr(model, pascal_to_snake_case(node_type))[node_id],
    )


# %% toevoegen Driel
# Driel toevoegen met een control op Fractional Nodes
name = "Driel"
code_waterbeheerder = "40A-004-02"

offset_node_id = (
    model.node_table().df[model.node_table().df["meta_code_waterbeheerder"] == code_waterbeheerder]
).node_id.to_numpy()[0]

model = verdeelsleutel_to_control(
    verdeelsleutel_df,
    model,
    code_waterbeheerder=code_waterbeheerder,
    offset_node_id=offset_node_id,
    waterbeheerder="Rijkswaterstaat",
)

# %% add Haringvliet
# Haringvliet toevoegen met control op outlet
name = "Haringvlietsluizen"
code_waterbeheerder = "37C-350-09"
flow_rate_haringvliet = [0, 0, 50, 50, 400, 2500, 3800, 5200, 6800, 8000, 9000]
flow_rate_lobith = [0, 1100, 1100, 1700, 2000, 4000, 6000, 8000, 10000, 12000, 15000]

node_ids = (
    model.node_table().df[model.node_table().df["meta_code_waterbeheerder"] == code_waterbeheerder].node_id.to_numpy()
)


ctrl_node = add_control_node_to_network(
    model,
    node_ids,
    ctrl_node_geom=(62430, 427430),
    meta_waterbeheerder="Rijkswaterstaat",
    name=name,
    meta_code_waterbeheerder=name,
)

listen_node_id = (
    model.node_table().df[model.node_table().df["meta_code_waterbeheerder"] == "LOBH"].node_id.to_numpy()[0]
)

listen_node_type = model.node_table().df.set_index("node_id").at[listen_node_id, "node_type"]


condition_df = discrete_control.condition(
    values=flow_rate_lobith,
    node_id=ctrl_node.node_id,
    listen_feature_id=listen_node_id,
    name=name,
)

logic_df = discrete_control.logic(
    node_id=ctrl_node.node_id,
    length=len(flow_rate_haringvliet),
    name=name,
)

model.discrete_control.add(
    ctrl_node,
    [
        nodes.discrete_control.Condition(
            listen_node_id=listen_node_id,
            listen_node_type=listen_node_type,
            variable="flow_rate",
            greater_than=condition_df["greater_than"].to_list(),
        ),
        nodes.discrete_control.Logic(
            truth_state=logic_df.truth_state.to_list(),
            control_state=logic_df.control_state.to_list(),
        ),
    ],
)

model.edge.add(
    model.discrete_control[ctrl_node.node_id],
    model.outlet[node_ids[0]],
)


outlet_df = discrete_control.node_table(values=flow_rate_lobith, variable="flow_rate", name=name, node_id=node_ids[0])
for col, dtype in model.outlet.static.df.dtypes.to_dict().items():
    if col not in outlet_df.columns:
        outlet_df.loc[:, [col]] = None
    outlet_df[col] = outlet_df[col].astype(dtype)


model.outlet.static.df = model.outlet.static.df[model.outlet.static.df.node_id != node_ids[0]]
model.outlet.static.df = pd.concat([model.outlet.static.df, outlet_df])

# %% toevoegen IJsselmeer

# toevoegen peil IJsselmeer door opgave peil waddenzee
node_ids = (
    model.node_table().df[model.node_table().df["meta_code_waterbeheerder"].isin(["KOBU", "OEBU"])].node_id.to_numpy()
)

time = pd.date_range(model.starttime, model.endtime)

day_of_year = [
    "01-01",
    "03-01",
    "03-11",
    "03-21",
    "04-01",
    "04-10",
    "04-15",
    "08-11",
    "08-15",
    "08-21",
    "08-31",
    "09-11",
    "09-15",
    "09-21",
    "10-01",
    "10-11",
    "10-21",
    "10-31",
    "12-31",
]

level = [
    -0.4,
    -0.1,
    -0.1,
    -0.1,
    -0.2,
    -0.2,
    -0.2,
    -0.2,
    -0.3,
    -0.3,
    -0.3,
    -0.3,
    -0.3,
    -0.3,
    -0.4,
    -0.4,
    -0.4,
    -0.4,
    -0.4,
]
level_cycle_df = pd.DataFrame(
    {
        "dayofyear": [datetime.strptime(i, "%m-%d").timetuple().tm_yday for i in day_of_year],
        "level": level,
    }
).set_index("dayofyear")


def get_level(timestamp, level_cycle_df):
    return level_cycle_df.at[level_cycle_df.index[level_cycle_df.index <= timestamp.dayofyear].max(), "level"]


time = pd.date_range(model.starttime, model.endtime)
level_df = pd.DataFrame({"time": time, "level": [get_level(i, level_cycle_df) for i in time]})

level_df = pd.concat(
    [pd.concat([level_df, pd.DataFrame({"node_id": [node_id] * len(level_df)})], axis=1) for node_id in node_ids],
    ignore_index=True,
)
model.level_boundary.time.df = level_df
model.level_boundary.static.df = model.level_boundary.static.df[~model.level_boundary.static.df.node_id.isin(node_ids)]

# %% toevoegen Irenesluizen

# PID control op ARK volgens peilbesluit op -0.4m NAP: https://www.helpdeskwater.nl/publish/pages/138359/41_peilbesluit_boezem_noordzeekanaal_amsterdam-rijnkanaal.pdf
name = "Irenesluizen"
code_waterbeheerder = "39B-002-02"

node_id, node_type = (
    model.node_table()
    .df[model.node_table().df["meta_code_waterbeheerder"] == code_waterbeheerder]
    .iloc[0][["node_id", "node_type"]]
)

condition = model.outlet.static.df.node_id == node_id
model.outlet.static.df.loc[condition, ["min_flow_rate", "max_flow_rate"]] = 0, 60

gdf = model.edge.df[model.edge.df.from_node_id == node_id]
gdf = gdf[gdf.to_node_type == "Basin"]
listen_node_id = gdf.iloc[0].to_node_id


ctrl_node = add_control_node_to_network(
    model,
    [node_id],
    meta_waterbeheerder="Rijkswaterstaat",
    name=name,
    meta_code_waterbeheerder=name,
    ctrl_type="PidControl",
)

pid = {
    "listen_node_type": "Basin",
    "listen_node_id": listen_node_id,
    "target": -0.4,
    "proportional": -50000,
    "integral": -1e-7,
    "derivative": 0,
}

add_pid(node_id, node_type, ctrl_node, pid)

# %% toevoegen Gemaal Panheel

# PID control op Kanaal Wessem-Nederweerd volgens peilbesluit op 28.65m NAP: https://www.helpdeskwater.nl/publish/pages/188982/watersystemen-midden-limburg-en-noord-brabantse-kanalen.pdf
name = "Gemaal Panheel"
code_waterbeheerder = "58C-001-06"

node_id, node_type = (
    model.node_table()
    .df[model.node_table().df["meta_code_waterbeheerder"] == code_waterbeheerder]
    .iloc[0][["node_id", "node_type"]]
)

condition = model.pump.static.df.node_id == node_id
model.pump.static.df.loc[condition, ["min_flow_rate", "max_flow_rate"]] = 0, 3

gdf = model.edge.df[model.edge.df.from_node_id == node_id]
gdf = gdf[gdf.to_node_type == "Basin"]
listen_node_id = gdf.iloc[0].to_node_id


ctrl_node = add_control_node_to_network(
    model,
    [node_id],
    meta_waterbeheerder="Rijkswaterstaat",
    name=name,
    meta_code_waterbeheerder=name,
    ctrl_type="PidControl",
    offset=20,
)

pid = {
    "listen_node_type": "Basin",
    "listen_node_id": listen_node_id,
    "target": 28.60,
    "proportional": -50000,
    "integral": -1e-7,
    "derivative": 0,
}

add_pid(node_id, node_type, ctrl_node, pid)

name = "Sluis Panheel"
code_waterbeheerder = "58C-001-04"

node_id, node_type = (
    model.node_table()
    .df[model.node_table().df["meta_code_waterbeheerder"] == code_waterbeheerder]
    .iloc[0][["node_id", "node_type"]]
)

condition = model.outlet.static.df.node_id == node_id
model.outlet.static.df.loc[condition, ["min_flow_rate", "max_flow_rate"]] = 2.5, 10

gdf = model.edge.df[model.edge.df.to_node_id == node_id]
gdf = gdf[gdf.from_node_type == "Basin"]
listen_node_id = gdf.iloc[0].from_node_id


ctrl_node = add_control_node_to_network(
    model,
    [node_id],
    meta_waterbeheerder="Rijkswaterstaat",
    name=name,
    meta_code_waterbeheerder=name,
    ctrl_type="PidControl",
    offset=20,
)
pid = {
    "listen_node_type": "Basin",
    "listen_node_id": listen_node_id,
    "target": 28.70,
    "proportional": -50000,
    "integral": -1e-7,
    "derivative": 0,
}

add_pid(node_id, node_type, ctrl_node, pid)


# %% wegschrijven model
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_sturing_upgraded", "hws.toml")
model.write(ribasim_toml)

# %%

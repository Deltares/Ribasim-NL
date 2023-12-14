# %%
import geopandas as gpd
import pandas as pd
import ribasim
from ribasim_nl import CloudStorage
from shapely.geometry import Point

cloud = CloudStorage()

ribasim_model_dir = cloud.joinpath(
    "Rijkswaterstaat", "modellen", "rijkswateren_2023_12_0"
)

model = ribasim.Model(filepath=ribasim_model_dir / "ribasim.toml")

lobith_rates = [898, 1590, 2600, 3630, 16000]  # let op sorteren!
nederrijn_fractions = [0.12195122, 0.095238095, 0.55, 0.567226891, 0.578668036]
ijssel_fractions = [0.87804878, 0.904761905, 0.45, 0.432773109, 0.421331964]
waal_fractions = [0.816462736, 0.80050665, 0.691714836, 0.672266593, 0.6353125]
pannerdensch_kanaal_fractions = [
    0.183537264,
    0.19949335,
    0.308285164,
    0.327733407,
    0.3646875,
]
control_states = [
    "minimum_afvoer",
    "volledig_gestuwd",
    "start_stuw_driel",
    "start_stuw_hagestein_amerongen",
    "maatgevende_afvoer",
]
pannerdensch_kanaal_id = 115
waal_id = 114
ijssel_id = 174
nederrijn_id = 173

fraction_nodes = [waal_id, pannerdensch_kanaal_id, ijssel_id, nederrijn_id]

control_node = len(model.network.node.df) + 1


# %% remove resistance-static

model.linear_resistance.static.df = model.linear_resistance.static.df[
    ~model.linear_resistance.static.df.node_id.isin(fraction_nodes)
]

# %% add fractional_flow
model.network.node.df.loc[fraction_nodes, ["type"]] = "FractionalFlow"

fractions = (
    waal_fractions
    + pannerdensch_kanaal_fractions
    + ijssel_fractions
    + nederrijn_fractions
)
fractional_flow = pd.DataFrame(
    {
        "node_id": [waal_id] * 5
        + [pannerdensch_kanaal_id] * 5
        + [ijssel_id] * 5
        + [nederrijn_id] * 5,
        "fraction": fractions,
        "control_state": control_states
        + control_states
        + control_states
        + control_states,
    }
)

model.fractional_flow = ribasim.FractionalFlow(static=fractional_flow)

# %% add control-data
condition = pd.DataFrame(
    {
        "node_id": [control_node] * len(lobith_rates),
        "listen_feature_id": [176] * len(lobith_rates),
        "variable": ["flow_rate"] * len(lobith_rates),
        "greater_than": lobith_rates,
    }
)

logic = pd.DataFrame(
    data={
        "node_id": [control_node] * len(lobith_rates),
        "truth_state": ["TFFFF", "TTFFF", "TTTFF", "TTTTF", "TTTTT"],
        "control_state": control_states,
    }
)

discrete_control = ribasim.DiscreteControl(condition=condition, logic=logic)

model.discrete_control = discrete_control
# %%

point = Point(193840, 440480)
node_id = control_node
gdf = gpd.GeoDataFrame(
    {"node_id": [node_id], "geometry": [point], "type": ["DiscreteControl"]},
    index=[node_id],
    crs=28992,
)

model.network.node.df = pd.concat([model.network.node.df, gdf])

# %%
from_id = [control_node] * 4
to_id = fraction_nodes

lines = model.network.node.geometry_from_connectivity(from_id, to_id)

gdf = gpd.GeoDataFrame(
    {
        "from_node_id": from_id,
        "to_node_id": to_id,
        "edge_type": ["control"] * len(to_id),
    },
    geometry=lines,
    crs=28992,
)

model.network.edge.df = pd.concat([model.network.edge.df, gdf])

# %%

ribasim_model_dir = cloud.joinpath("Rijkswaterstaat", "modellen", "rijkswateren")


model.write(ribasim_model_dir)

# %%

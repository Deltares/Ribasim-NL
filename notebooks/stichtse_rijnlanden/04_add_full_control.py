# %%
import inspect

import geopandas as gpd
import pandas as pd

from peilbeheerst_model import ribasim_parametrization
from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model, check_basin_level
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static

# execute model run
MODEL_EXEC: bool = False

# model settings
AUTHORITY: str = "StichtseRijnlanden"
SHORT_NAME: str = "hdsr"
MODEL_ID: str = "2025_7_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_lyr", "output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(AUTHORITY, "verwerkt", "4_ribasim", "peilgebieden_bewerkt.gpkg")
model_edits_extra_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "model_edits_aanvoer.gpkg")


cloud.synchronize(
    filepaths=[
        aanvoer_path,
    ]
)

# read model
model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoer_path)
aanvoergebieden_df_dissolved = aanvoergebieden_df.dissolve()
original_model = model.model_copy(deep=True)

# %%
update_basin_static(model=model, evaporation_mm_per_day=1)

# %%
# alle niet-gecontrolleerde basins krijgen een meta_streefpeil uit de final state van de parameterize_model.py
update_levels = model.basin_outstate.df.set_index("node_id")["level"]
basin_ids = model.basin.node.df[model.basin.node.df["meta_gestuwd"] == "False"].index
mask = model.basin.area.df["node_id"].isin(basin_ids)
model.basin.area.df.loc[mask, "meta_streefpeil"] = model.basin.area.df[mask]["node_id"].apply(
    lambda x: update_levels[x]
)
# model basin area
model.basin.area.df["meta_streefpeil"] = model.basin.area.df["meta_streefpeil"] + 0.02
model.outlet.static.df["min_upstream_level"] = model.outlet.static.df["min_upstream_level"] + 0.02
model.pump.static.df["min_upstream_level"] = model.pump.static.df["min_upstream_level"] + 0.02


# %%

add_from_to_nodes_and_levels(model)
# re-parameterize
ribasim_parametrization.set_aanvoer_flags(model, aanvoergebieden_df_dissolved, overruling_enabled=True)
ribasim_parametrization.determine_min_upstream_max_downstream_levels(
    model,
    AUTHORITY,
    aanvoer_upstream_offset=0.02,
    aanvoer_downstream_offset=0.0,
    afvoer_upstream_offset=0.02,
    afvoer_downstream_offset=0.0,
)
check_basin_level.add_check_basin_level(model=model)

# TODO: The addition of `ContinuousControl`-nodes is subsequently a minor modification:
"""To allow the addition of `ContinuousControl`-nodes, the branch 'continuous_control' must be merged first to access
the required function: `ribasim_parametrization.add_continuous_control(<model>)`. The expansion of adding the continuous
control requires a proper working schematisation of both 'afvoer'- and 'aanvoer'-situations, and so these should be
fixed and up-and-running beforehand.
"""
# ribasim_parametrization.add_continuous_control(model)

"""For the addition of `ContinuousControl`-nodes, it might be necessary to set `model.basin.static.df=None`, as the
`ContinuousControl`-nodes require `Time`-tables instead of `Static`-tables. If both are defined (for the same node,
Ribasim will raise an error and thus not execute.
"""
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.04
mask = model.outlet.static.df["meta_aanvoer"] == 0
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA
model.outlet.static.df.flow_rate = 100
model.pump.static.df.flow_rate = 100
model.outlet.static.df.max_flow_rate = original_model.outlet.static.df.max_flow_rate
model.pump.static.df.max_flow_rate = original_model.pump.static.df.max_flow_rate
model.basin.area.df["meta_streefpeil"] = model.basin.area.df["meta_streefpeil"] - 0.02
# set upstream level boundaries at 999 meters
# boundary_node_ids = [i for i in model.level_boundary.node.df.index if not model.upstream_node_id(i) is not None]
# model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id.isin(boundary_node_ids), "level"] = 999


# %% Alle inlaten op max 5m3/s gezet.
node_ids = model.outlet.node.df[model.outlet.node.df.meta_code_waterbeheerder.str.startswith("I")].index.to_numpy()
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "max_flow_rate"] = 5
# %%

model.remove_node(663, remove_edges=True)
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 45, "level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 541, "max_downstream_level"] = -1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1162, "max_flow_rate"] = 0.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 514, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1082, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1081, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 368, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 947, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 911, "max_flow_rate"] = 0.1


def set_values(df, node_ids, values: dict):
    """Hulpfunctie om meerdere kolommen tegelijk te updaten voor bepaalde node_ids."""
    mask = df["node_id"].isin(node_ids)
    for col, val in values.items():
        df.loc[mask, col] = val


# === 1. Bepaal upstream/downstream connection nodes ===
upstream_outlet_nodes = model.upstream_connection_node_ids(node_type="Outlet")
downstream_outlet_nodes = model.downstream_connection_node_ids(node_type="Outlet")
upstream_pump_nodes = model.upstream_connection_node_ids(node_type="Pump")
downstream_pump_nodes = model.downstream_connection_node_ids(node_type="Pump")

# === 2. Zet waardes voor upstream nodes ===
set_values(
    model.outlet.static.df,
    upstream_outlet_nodes,
    {
        "flow_rate": 10,
        "max_flow_rate": 10,
        "min_upstream_level": pd.NA,
    },
)
set_values(
    model.pump.static.df,
    upstream_pump_nodes,
    {
        "flow_rate": 10,
        "max_flow_rate": 10,
        "min_upstream_level": pd.NA,
    },
)

# === 3. Zet waardes voor downstream nodes ===
set_values(
    model.outlet.static.df,
    downstream_outlet_nodes,
    {
        "max_downstream_level": pd.NA,
    },
)
set_values(
    model.pump.static.df,
    downstream_pump_nodes,
    {
        "max_downstream_level": pd.NA,
    },
)

# === 2b. Verhoog mmin_upstream_level met offset voor downstream Outlets
mask = model.outlet.static.df["node_id"].isin(downstream_outlet_nodes)

model.outlet.static.df.loc[mask, "min_upstream_level"] = model.outlet.static.df.loc[mask, "min_upstream_level"] + 0.02
mask = model.pump.static.df["node_id"].isin(downstream_pump_nodes)
model.pump.static.df.loc[mask, "min_upstream_level"] = model.pump.static.df.loc[mask, "min_upstream_level"] + 0.02
model.level_boundary.static.df["level"] = model.level_boundary.static.df["level"] + 0.02
model.pump.static.df["max_downstream_level"] = model.pump.static.df["max_downstream_level"] - 0.02


# === 4. Zet max/min levels op NA voor niet-gestuwde, niet-verbonden outlets ===
non_control_nodes = model.outlet.node.df.query("meta_gestuwd == 'False'").index
excluded_nodes = set(upstream_outlet_nodes) | set(downstream_outlet_nodes)
# model.outlet.static.df.loc[
#    model.outlet.static.df["node_id"].isin(non_control_nodes) & ~model.outlet.static.df["node_id"].isin(excluded_nodes),
#    ["max_downstream_level", "min_upstream_level"],
# ] = pd.NA


# Afronden
# def round_to_2_decimals(x):
#     return round(x, 2) if pd.notnull(x) else x


# model.outlet.static.df["min_upstream_level"] = model.outlet.static.df["min_upstream_level"].apply(round_to_2_decimals)
# model.outlet.static.df["max_downstream_level"] = model.outlet.static.df["max_downstream_level"].apply(
#     round_to_2_decimals
# )
# model.pump.static.df["min_upstream_level"] = model.pump.static.df["min_upstream_level"].apply(round_to_2_decimals)
# model.pump.static.df["max_downstream_level"] = model.pump.static.df["max_downstream_level"].apply(round_to_2_decimals)

model.pump.static.df.flow_rate = 100

# %% Add inlaat

actions = ["add_basin", "update_node", "connect_basins"]
actions = [i for i in actions if i in gpd.list_layers(model_edits_extra_gpkg).name.to_list()]
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_extra_gpkg, layer=action, fid_as_index=True)
    if "order" in df.columns:
        df.sort_values("order", inplace=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# Waaiergemaal iets hoger (numeriek handig om peil te houden)
# model.pump.static.df.loc[model.pump.static.df.node_id == 567, "min_upstream_level"] = 0.56
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 759, "min_upstream_level"] = 0.56
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 756, "min_upstream_level"] = 0.56

model.pump.static.df.loc[model.pump.static.df.node_id == 1138, "min_upstream_level"] = 0.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2106, "min_upstream_level"] = 0.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1172, "min_upstream_level"] = 0.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1171, "min_upstream_level"] = 0.55
# Pompen min_upstream
model.pump.static.df.loc[model.pump.static.df.node_id == 561, "min_upstream_level"] = 1.32
model.pump.static.df.loc[model.pump.static.df.node_id == 653, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 532, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 633, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 603, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 605, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 532, "max_downstream_level"] = 1.55
model.pump.static.df.loc[model.pump.static.df.node_id == 557, "max_downstream_level"] = 0.54
model.pump.static.df.loc[model.pump.static.df.node_id == 531, "min_upstream_level"] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 545, "min_upstream_level"] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 563, "min_upstream_level"] = -0.85
model.pump.static.df.loc[model.pump.static.df.node_id == 563, "max_downstream_level"] = 0.52
model.pump.static.df.loc[model.pump.static.df.node_id == 608, "max_downstream_level"] = 2.71

# Afvoer pomp
model.pump.static.df.loc[model.pump.static.df.node_id == 572, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 578, "max_downstream_level"] = pd.NA


model.outlet.static.df.loc[model.outlet.static.df.node_id == 529, "min_upstream_level"] = 0.46
model.pump.static.df.loc[model.pump.static.df.node_id == 1373, "min_upstream_level"] = 0.46

model.outlet.static.df.loc[model.outlet.static.df.node_id == 105, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 155, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 196, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 181, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 196, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 291, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 340, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 339, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 341, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 342, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 343, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 344, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 405, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 405, "max_downstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 746, "min_upstream_level"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 409, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 422, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 423, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 424, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 809, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 249, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 265, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 266, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 333, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 557, "max_downstream_level"] = 1.55

model.outlet.static.df.loc[model.outlet.static.df.node_id == 959, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 408, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 315, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 983, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 164, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 210, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 930, "min_upstream_level"] = -1.08

# De Aanvoerder
model.pump.static.df.loc[model.pump.static.df.node_id == 542, "max_downstream_level"] = 0.01
model.outlet.static.df.loc[model.outlet.static.df.node_id == 836, "min_upstream_level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 107, "min_upstream_level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 258, "min_upstream_level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 743, "min_upstream_level"] = 0.01

model.outlet.static.df.loc[model.outlet.static.df.node_id == 2111, "max_downstream_level"] = -2.2
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 2108, "level"] = -2

model.basin.area.df.loc[model.basin.area.df.node_id == 1698, "meta_streefpeil"] = 0
model.basin.area.df.loc[model.basin.area.df.node_id == 1492, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1396, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1562, "meta_streefpeil"] = 0.52

model.outlet.static.df.loc[model.outlet.static.df.node_id == 535, "max_downstream_level"] = pd.NA

model.remove_node(node_id=86, remove_edges=True)
model.remove_node(node_id=669, remove_edges=True)
model.remove_node(node_id=737, remove_edges=True)
# model.remove_node(node_id=1197, remove_edges=True)
model.merge_basins(basin_id=1408, to_basin_id=1672)
model.merge_basins(basin_id=1524, to_basin_id=1975)
model.merge_basins(basin_id=1425, to_basin_id=1558)
model.merge_basins(basin_id=1995, to_basin_id=1646)
model.merge_basins(basin_id=1692, to_basin_id=1646)
model.merge_basins(basin_id=1514, to_basin_id=1577)
model.merge_basins(basin_id=1522, to_basin_id=1507)

# %%
# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
model.pump.static.df["meta_func_afvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 0
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    # TODO: Different ways of executing the model; choose the one that suits you best:
    ribasim_parametrization.tqdm_subprocess(["ribasim", ribasim_toml], print_other=False, suffix="init")
    # exit_code = model.run()

    # assert exit_code == 0

    """Note that currently, the Ribasim-model is unstable but it does execute, i.e., the model re-parametrisation is
    successful. This might be due to forcing the schematisation with precipitation while setting the 'sturing' of the
    outlets on 'aanvoer' instead of the more suitable 'afvoer'. This should no longer be a problem once the next step of
    adding `ContinuousControl`-nodes is implemented.
    """

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_all()

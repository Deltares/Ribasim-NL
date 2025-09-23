# %%
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
AUTHORITY: str = "DrentsOverijsselseDelta"
SHORT_NAME: str = "dod"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_lyr", "output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(AUTHORITY, "aangeleverd", "Na_levering", "HyDAMO_WM_20230720.gpkg")
model_edits_aanvoer_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "model_edits_aanvoer.gpkg")

cloud.synchronize(
    filepaths=[
        aanvoer_path,
    ]
)

# %%
# read model
model = Model.read(ribasim_toml)
original_model = model.model_copy(deep=True)
update_basin_static(model=model, precipitation_mm_per_day=1)

# alle niet-gecontrolleerde basins krijgen een meta_streefpeil uit de final state van de parameterize_model.py
update_levels = model.basin_outstate.df.set_index("node_id")["level"]
basin_ids = model.basin.node.df[model.basin.node.df["meta_gestuwd"] == "False"].index
mask = model.basin.area.df["node_id"].isin(basin_ids)
model.basin.area.df.loc[mask, "meta_streefpeil"] = model.basin.area.df[mask]["node_id"].apply(
    lambda x: update_levels[x]
)

# %%
add_from_to_nodes_and_levels(model)

# filter aanvoergebieden
aanvoergebieden_df = gpd.read_file(aanvoer_path, layer="afvoergebiedaanvoergebied")
aanvoergebieden_df = aanvoergebieden_df[aanvoergebieden_df["soortafvoeraanvoergebied"] == "Aanvoergebied"]
aanvoergebieden_df = gpd.GeoDataFrame({"geometry": list(aanvoergebieden_df.union_all().geoms)}, crs=28992)
aanvoergebieden_df_dissolved = aanvoergebieden_df.dissolve()

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

model.manning_resistance.static.df.loc[:, "manning_n"] = 0.04
mask = model.outlet.static.df["meta_aanvoer"] == 0
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA
model.outlet.static.df.flow_rate = 100
model.pump.static.df.flow_rate = 100
model.outlet.static.df.max_flow_rate = 100
model.pump.static.df.max_flow_rate = 100


# %% bovenstroomse outlets op 10m3/s zetten en boundary afvoer pumps/outlets
# geen downstreamm level en aanvoer  pumps/outlets geen upstream level
def set_values_where(df, updates, node_ids=None, key_col="node_id", mask=None):
    if mask is None:
        mask = df[key_col].isin(node_ids)
    sub = df.loc[mask]
    for col, val in updates.items():
        df.loc[mask, col] = val(sub) if callable(val) else val
    return int(mask.sum())


# === 1. Bepaal upstream/downstream connection nodes ===
upstream_outlet_nodes = model.upstream_connection_node_ids(node_type="Outlet")
downstream_outlet_nodes = model.downstream_connection_node_ids(node_type="Outlet")
upstream_pump_nodes = model.upstream_connection_node_ids(node_type="Pump")
downstream_pump_nodes = model.downstream_connection_node_ids(node_type="Pump")

# === 1a. Upstream outlets met aanvoer: max_downstream = min_upstream + 0.02 en min_upstream = NA
out_static = model.outlet.static.df
pump_static = model.pump.static.df
mask_upstream_aanvoer = out_static["node_id"].isin(upstream_outlet_nodes)

node_ids = model.outlet.node.df[model.outlet.node.df["meta_categorie"] == "hoofdwater"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# %%
# === 1a. Upstream outlets (bij boundaries) met aanvoer ===
set_values_where(
    out_static,
    mask=mask_upstream_aanvoer,
    updates={
        "max_downstream_level": lambda d: d["min_upstream_level"] + 0.02,
        "min_upstream_level": pd.NA,
    },
)

updates_plan = [
    # Upstream boundary: Outlets en Pumps
    (out_static, upstream_outlet_nodes, {"flow_rate": 100, "max_flow_rate": 100}),
    (pump_static, upstream_pump_nodes, {"flow_rate": 100, "max_flow_rate": 100, "min_upstream_level": pd.NA}),
    # Downstream boundary: Outlets en Pumps
    (out_static, downstream_outlet_nodes, {"max_downstream_level": pd.NA}),
    (pump_static, downstream_pump_nodes, {"max_downstream_level": pd.NA}),
]

for df, nodes, updates in updates_plan:
    set_values_where(df, node_ids=nodes, updates=updates)

# Alle pumps corrigeren met offset
set_values_where(
    pump_static,
    mask=pump_static.index.notna(),  # alle rijen
    updates={"max_downstream_level": lambda d: d["max_downstream_level"] - 0.02},
)

set_values_where(
    pump_static,
    node_ids=downstream_pump_nodes,
    updates={"min_upstream_level": lambda d: pd.to_numeric(d["min_upstream_level"], errors="coerce") + 0.02},
)
set_values_where(
    out_static,
    node_ids=downstream_outlet_nodes,
    updates={"min_upstream_level": lambda d: pd.to_numeric(d["min_upstream_level"], errors="coerce") + 0.02},
)

# Downstream boundary nodes
model.level_boundary.static.df["level"] = -999

# set upstream level boundaries at 999 meters
boundary_node_ids = [i for i in model.level_boundary.node.df.index if not model.upstream_node_id(i) is not None]
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id.isin(boundary_node_ids), "level"] = 999

# %%%
# %%
node_ids = model.outlet.static.df[model.outlet.static.df["meta_categorie"] == "Inlaat"]["node_id"].to_numpy()
model.outlet.static.df.loc[model.outlet.static.df["node_id"].isin(node_ids), "max_flow_rate"] = 0.1

node_ids_duikers = model.outlet.static.df[model.outlet.static.df["meta_code"].str.startswith("duiker", na=False)][
    "node_id"
].to_numpy()
model.outlet.static.df.loc[model.outlet.static.df["node_id"].isin(node_ids_duikers), "max_flow_rate"] = 0.1

# fixes vistrap eruit
model.remove_node(1414, remove_edges=True)
model.remove_node(1441, remove_edges=True)

# fixes flow_rate sluis max 0.1m3/s
model.outlet.static.df.loc[model.outlet.static.df.node_id == 523, "max_flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 538, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1280, "max_flow_rate"] = 0.1  # Inlaat
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1361, "max_flow_rate"] = 0  # Sluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2607, "max_flow_rate"] = 0.1  # Sluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2604, "max_flow_rate"] = 0  # check met hysterese
model.outlet.static.df.loc[model.outlet.static.df.node_id == 544, "max_flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1089, "max_downstream_level"] = pd.NA
# Streefpeil te laag
model.outlet.static.df.loc[model.outlet.static.df.node_id == 959, "min_upstream_level"] = 11
model.outlet.static.df.loc[model.outlet.static.df.node_id == 971, "min_upstream_level"] = 11
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1238, "min_upstream_level"] = 11
model.outlet.static.df.loc[model.outlet.static.df.node_id == 544, "min_upstream_level"] = -0.2

# Afvoer outlets en pumpen geen downstream waterlevel bij waterloop Paraduisslijs, basin levels kloppen niet
model.pump.static.df.loc[model.pump.static.df.node_id == 2594, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 2595, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 646, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 664, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 680, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 1101, "max_downstream_level"] = pd.NA

model.outlet.static.df.loc[model.outlet.static.df.node_id == 1382, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 541, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 454, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1081, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 932, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1219, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 387, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 490, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 519, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1066, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 988, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 445, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 520, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1036, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 543, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 926, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 446, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1368, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 521, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 953, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1325, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 464, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1406, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 515, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 965, "max_downstream_level"] = pd.NA


# Manning moet outlet zijn
model.update_node(node_id=1468, node_type="Outlet")
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1468, "min_upstream_level"] = 2.63

# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
check_basin_level.add_check_basin_level(model=model)
model.pump.static.df["meta_func_afvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 0
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    ribasim_parametrization.tqdm_subprocess(["ribasim", ribasim_toml], print_other=False, suffix="init")
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_all()

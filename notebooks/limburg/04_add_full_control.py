# %%


import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static

from peilbeheerst_model import ribasim_parametrization
from ribasim_nl import CloudStorage, Model, check_basin_level

# execute model run
MODEL_EXEC: bool = True

# model settings
AUTHORITY: str = "Limburg"
SHORT_NAME: str = "limburg"
MODEL_ID: str = "2025_7_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoer_gpkg = cloud.joinpath("Basisgegevens/waterverdeling/aanvoer.gpkg")
aanvoer_gdf = gpd.read_file(aanvoer_gpkg, layer="aanvoergebieden")
aanvoer_gdf = aanvoer_gdf[aanvoer_gdf["waterbeheerder"] == "Limburg"]
cloud.synchronize(
    filepaths=[
        aanvoer_gpkg,
    ]
)

# read model
model = Model.read(ribasim_toml)
original_model = model.model_copy(deep=True)
update_basin_static(model=model, evaporation_mm_per_day=1)

# alle niet-gecontrolleerde basins krijgen een meta_streefpeil uit de final state van de parameterize_model.py
update_levels = model.basin_outstate.df.set_index("node_id")["level"]
basin_ids = model.basin.node.df[model.basin.node.df["meta_gestuwd"] == "False"].index
mask = model.basin.area.df["node_id"].isin(basin_ids)
model.basin.area.df.loc[mask, "meta_streefpeil"] = model.basin.area.df[mask]["node_id"].apply(
    lambda x: update_levels[x]
)
add_from_to_nodes_and_levels(model)

# re-parameterize
ribasim_parametrization.set_aanvoer_flags(model, aanvoer_gdf, overruling_enabled=False)
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
model.outlet.static.df.flow_rate = original_model.outlet.static.df.flow_rate
model.pump.static.df.flow_rate = original_model.pump.static.df.flow_rate


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

model.merge_basins(basin_id=2461, to_basin_id=2070, are_connected=True)
model.merge_basins(basin_id=2070, to_basin_id=2360, are_connected=True)
model.merge_basins(basin_id=1885, to_basin_id=2360, are_connected=True)
model.merge_basins(basin_id=2205, to_basin_id=2144, are_connected=True)

# %% sturing uit alle niet-gestuwde outlets halen
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
non_control_mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[non_control_mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[non_control_mask, "max_downstream_level"] = pd.NA

# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.pump.static.df["meta_func_afvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 0
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    model.run()
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_all()

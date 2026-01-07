# %%
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim import Node
from ribasim.nodes import discrete_control, flow_demand, level_demand, outlet, pid_control, pump
from ribasim_nl.control import (
    add_controllers_and_demand_to_flushing_nodes,
    add_controllers_to_drain_nodes,
    add_controllers_to_flow_control_nodes,
    add_controllers_to_supply_nodes,
    control_nodes_from_supply_area,
    get_drain_nodes,
)
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import Point

from peilbeheerst_model import ribasim_parametrization
from ribasim_nl import CloudStorage, Model, check_basin_level

FORCE_REBUILD_PREPROCESSED = False  # True = altijd opnieuw preprocessen en overschrijven
LEVEL_DIFFERENCE_THRESHOLD = 0.02

# execute model run
MODEL_EXEC: bool = True

# model settings
AUTHORITY: str = "Noorderzijlvest"
SHORT_NAME: str = "nzv"
MODEL_ID: str = "2025_7_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
ribasim_toml_pre = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_preprocessed_model", f"{SHORT_NAME}_pre.toml")
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(
    AUTHORITY, "verwerkt/1_ontvangen_data//20250527/gebieden_met_wateraanvoermogelijkheid_noorderzijlvest.gpkg"
)
cloud.synchronize(filepaths=[aanvoer_path, qlr_path])


def cloud_path_exists(p) -> bool:
    try:
        return Path(p).exists()
    except TypeError:
        return getattr(p, "exists")()


def build_preprocessed_model(model: Model) -> Model:
    """Get first upstream basins of a node"""
    original_model = model.model_copy(deep=True)
    add_from_to_nodes_and_levels(model)

    aanvoergebieden_df = gpd.read_file(aanvoer_path)
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

    model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
    mask = model.outlet.static.df["meta_aanvoer"] == 0
    model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

    model.pump.static.df.max_flow_rate = original_model.pump.static.df.flow_rate
    model.pump.static.df.flow_rate = 20
    model.outlet.static.df.max_flow_rate = original_model.outlet.static.df.flow_rate
    model.outlet.static.df.flow_rate = 20


# =========================
# LOAD / BUILD SWITCH
# =========================
pre_exists = cloud_path_exists(ribasim_toml_pre)

if pre_exists and not FORCE_REBUILD_PREPROCESSED:
    print(f"Preprocessed model bestaat al → preprocess overslaan: {ribasim_toml_pre}")
    model = Model.read(ribasim_toml_pre)
else:
    print("Preprocessed model wordt (opnieuw) gebouwd...")
    model = Model.read(ribasim_toml)  # altijd opnieuw vanaf base
    build_preprocessed_model(model)  # ✅ hier al je preprocess
    model.write(ribasim_toml_pre)
    print(f"Preprocessed model saved: {ribasim_toml_pre}")
    model = Model.read(ribasim_toml_pre)

print("Loaded preprocessed model.")

# ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(
    AUTHORITY, "verwerkt/1_ontvangen_data//20250527/gebieden_met_wateraanvoermogelijkheid_noorderzijlvest.gpkg"
)

cloud.synchronize(filepaths=[aanvoer_path, qlr_path])


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
# === 1a. Upstream outlets (bij boundaries) met aanvoer, alleen aanvoer mogelijk ===
set_values_where(
    out_static,
    mask=mask_upstream_aanvoer,
    updates={
        "max_downstream_level": lambda d: d["min_upstream_level"],
        "min_upstream_level": pd.NA,
    },
)

updates_plan = [
    # Upstream boundary: Outlets en Pumps
    (pump_static, upstream_pump_nodes, {"min_upstream_level": pd.NA}),  # "flow_rate": 100, "max_flow_rate": 100,
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
    updates={"max_downstream_level": lambda d: d["max_downstream_level"]},
)

# set upstream level boundaries
boundary_node_ids = [i for i in model.level_boundary.node.df.index if model.upstream_node_id(i) is None]
mask = model.level_boundary.static.df.node_id.isin(boundary_node_ids)
model.level_boundary.static.df.loc[mask, "level"] += 0.4
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 6, "level"] = 6.3

# set downstream level boundaries
boundary_node_ids = [i for i in model.level_boundary.node.df.index if model.upstream_node_id(i) is not None]
mask = model.level_boundary.static.df.node_id.isin(boundary_node_ids)
model.level_boundary.static.df.loc[mask, "level"] -= 1
# %%
# Inlaten
node_ids = model.outlet.static.df[model.outlet.static.df["meta_name"].str.startswith("INL", na=False)][
    "node_id"
].to_numpy()
model.outlet.static.df.loc[model.outlet.static.df["node_id"].isin(node_ids), "max_downstream_level"] += 0.02


# %%
# Afvoer altijd mogelijk door max_downstream op NA te zetten
mask = ~model.pump.static.df["meta_code"].str.contains("iKGM|_i", case=False, na=False)
node_ids = model.pump.static.df.loc[mask, "node_id"].to_numpy()
model.pump.static.df.loc[model.pump.static.df["node_id"].isin(node_ids), "max_downstream_level"] = pd.NA

model.pump.static.df.loc[model.pump.static.df.node_id == 40, "min_upstream_level"] = -0.34

# Gaarkeuken
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1741, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1741, "max_flow_rate"] = 26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1741, "max_downstream_level"] = -0.93

# Oostersluis kan aanvoeren boosterpomp driewegsluis
model.pump.static.df.loc[model.pump.static.df.node_id == 142, "min_upstream_level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 142, "flow_rate"] = 2.5
model.pump.static.df.loc[model.pump.static.df.node_id == 142, "max_downstream_level"] = -1.2

# Gaarkeuken scheepvaartsluis flow op nul620581
model.pump.static.df.loc[model.pump.static.df.node_id == 1744, "flow_rate"] = 0

# verkeerde basinpeil
model.outlet.static.df.loc[model.outlet.static.df.node_id == 689, "max_downstream_level"] = 2.58
model.outlet.static.df.loc[model.outlet.static.df.node_id == 500, "min_upstream_level"] = 2.54

model.outlet.static.df.loc[model.outlet.static.df.node_id == 387, "max_downstream_level"] = -0.71
model.outlet.static.df.loc[model.outlet.static.df.node_id == 485, "max_downstream_level"] = -0.71
# Meerweg
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1758, "max_downstream_level"] = -0.89
model.outlet.static.df.loc[model.outlet.static.df.node_id == 723, "max_flow_rate"] = 0

# Aanvoer zuiden missen downstream_level
model.outlet.static.df.loc[model.outlet.static.df.node_id == 363, "max_downstream_level"] = 6.05
model.outlet.static.df.loc[model.outlet.static.df.node_id == 589, "max_downstream_level"] = 6.05

model.outlet.static.df.loc[model.outlet.static.df.node_id == 503, "max_downstream_level"] = pd.NA

# flow inlaten naar custom
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1750, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 687, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 698, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 699, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1739, "max_flow_rate"] = 0.032
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1742, "max_flow_rate"] = 0.105
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1751, "max_flow_rate"] = 0.043
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1743, "max_flow_rate"] = 0.072
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1758, "max_flow_rate"] = 0.021

# Manning downstream niet sturen op max_downstream_level
model.outlet.static.df.loc[model.outlet.static.df.node_id == 506, "min_upstream_level"] = -0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 379, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 380, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 573, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 443, "min_upstream_level"] = -0.73
model.outlet.static.df.loc[model.outlet.static.df.node_id == 443, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 516, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 578, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 579, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 585, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 389, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 456, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 509, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 545, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 582, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 602, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 604, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 665, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 577, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 595, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 596, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 615, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 616, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 383, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 618, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 394, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 471, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 577, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 612, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 623, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 536, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 431, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 431, "min_upstream_level"] = -0.58
model.outlet.static.df.loc[model.outlet.static.df.node_id == 417, "min_upstream_level"] = -0.58
model.outlet.static.df.loc[model.outlet.static.df.node_id == 472, "min_upstream_level"] = -0.45
model.outlet.static.df.loc[model.outlet.static.df.node_id == 557, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 558, "max_downstream_level"] = pd.NA

# HD LOUWES aanvoer naar Louwersmeer
model.outlet.static.df.loc[model.outlet.static.df.node_id == 727, "max_downstream_level"] = -0.93

# Kleine aanpassingen handmatig
model.outlet.static.df.loc[model.outlet.static.df.node_id == 379, "max_downstream_level"] = -0.56
model.outlet.static.df.loc[model.outlet.static.df.node_id == 568, "max_downstream_level"] = 4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 571, "max_downstream_level"] = 4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 483, "max_downstream_level"] = 8.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 659, "min_upstream_level"] = 8.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 397, "min_upstream_level"] = 8.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 648, "max_downstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 619, "min_upstream_level"] = -1.07
model.outlet.static.df.loc[model.outlet.static.df.node_id == 704, "max_downstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 696, "max_downstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 630, "max_downstream_level"] = -0.22
model.outlet.static.df.loc[model.outlet.static.df.node_id == 321, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 469, "max_downstream_level"] = 8.3

# Robbegat in aanvoer situatie streefpeil
model.outlet.static.df.loc[model.outlet.static.df.node_id == 412, "max_downstream_level"] = -2.65
model.outlet.static.df.loc[model.outlet.static.df.node_id == 442, "max_downstream_level"] = -2.65
model.outlet.static.df.loc[model.outlet.static.df.node_id == 635, "max_downstream_level"] = -2.65
model.outlet.static.df.loc[model.outlet.static.df.node_id == 566, "max_downstream_level"] = -2.65
model.outlet.static.df.loc[model.outlet.static.df.node_id == 645, "max_downstream_level"] = -2.65
model.outlet.static.df.loc[model.outlet.static.df.node_id == 349, "max_downstream_level"] = -2.65
# Vooraan!
model.outlet.static.df.loc[model.outlet.static.df.node_id == 377, "max_downstream_level"] = -0.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 505, "max_downstream_level"] = -0.67
model.outlet.static.df.loc[model.outlet.static.df.node_id == 427, "max_downstream_level"] = -0.71
model.outlet.static.df.loc[model.outlet.static.df.node_id == 837, "max_downstream_level"] = -0.55

# Diepswal
model.pump.static.df.loc[model.pump.static.df.node_id == 37, "max_downstream_level"] = 2.68

# Driewegsluis max_downstream verhogen, Manning knopen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1750, "max_downstream_level"] = -1.28

# Controllers komen uit op manning waterloop
model.outlet.static.df.loc[model.outlet.static.df.node_id == 539, "max_downstream_level"] = -1.28
model.outlet.static.df.loc[model.outlet.static.df.node_id == 509, "min_upstream_level"] = -1.18
model.outlet.static.df.loc[model.outlet.static.df.node_id == 509, "max_downstream_level"] = -1.28
model.outlet.static.df.loc[model.outlet.static.df.node_id == 646, "max_downstream_level"] = -1.28
model.outlet.static.df.loc[model.outlet.static.df.node_id == 508, "max_downstream_level"] = -1.28
model.outlet.static.df.loc[model.outlet.static.df.node_id == 517, "max_downstream_level"] = -1.28
# Aanvoergemaal Klei
model.outlet.static.df.loc[model.outlet.static.df.node_id == 507, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 129, "max_downstream_level"] = -0.75

# HD Louwes
model.outlet.static.df.loc[model.outlet.static.df.node_id == 479, "max_downstream_level"] = 8.75
model.pump.static.df.loc[model.pump.static.df.node_id == 39, "max_downstream_level"] = -0.37
model.pump.static.df.loc[model.pump.static.df.node_id == 186, "max_downstream_level"] = pd.NA

# Aanvoergemaal Blijcke
model.pump.static.df.loc[model.pump.static.df.node_id == 106, "max_downstream_level"] = -0.49

# Aanvoergemaal Oosternieland
model.pump.static.df.loc[model.pump.static.df.node_id == 143, "max_downstream_level"] = -1.18

model.outlet.static.df.loc[model.outlet.static.df.node_id == 691, "min_upstream_level"] = -0.36

# Outlets omlaag anders laten ze geen water door
model.outlet.static.df.loc[model.outlet.static.df.node_id == 351, "min_upstream_level"] = -0.39
model.outlet.static.df.loc[model.outlet.static.df.node_id == 466, "min_upstream_level"] = -0.39

# inlaat Lauwersmeer
model.outlet.static.df.loc[model.outlet.static.df.node_id == 714, "max_downstream_level"] = -2.02

# Oudendijk aanvoer gemaal -4cm upstream!
model.pump.static.df.loc[model.pump.static.df.node_id == 118, "min_upstream_level"] = -1.11

# Pump Grote Hadder
model.pump.static.df.loc[model.pump.static.df.node_id == 109, "max_downstream_level"] = -0.34

# Ziekolk
model.pump.static.df.loc[model.pump.static.df.node_id == 124, "max_downstream_level"] = -0.37

# Waterwolf spuisluizen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 728, "max_flow_rate"] = 9999
model.outlet.static.df.loc[model.outlet.static.df.node_id == 728, "max_downstream_level"] = -0.93
model.pump.static.df.loc[model.pump.static.df.node_id == 29, "min_upstream_level"] = -0.95
model.pump.static.df.loc[model.pump.static.df.node_id == 29, "max_downstream_level"] = -0.93
model.pump.static.df.loc[model.pump.static.df.node_id == 30, "min_upstream_level"] = -0.95
model.pump.static.df.loc[model.pump.static.df.node_id == 30, "max_downstream_level"] = -0.93

# Drie Delfzijlen
model.pump.static.df.loc[model.pump.static.df.node_id == 67, "min_upstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 732, "min_upstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 732, "max_flow_rate"] = 100

model.update_node(node_id=837, node_type="Outlet")


# inlaten naast pomp Rondpompen voorkomen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 721, "min_upstream_level"] = -0.93

# Leek 2 inlaten naast pomp: rondpompen voorkomen
model.pump.static.df.loc[model.pump.static.df.node_id == 36, "min_upstream_level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 36, "max_downstream_level"] = 0.68

# Pomp en inlaat naast elkaar: rondpompen voorkomen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 680, "max_downstream_level"] = -1.22
model.outlet.static.df.loc[model.outlet.static.df.node_id == 724, "flow_rate"] = 400
model.outlet.static.df.loc[model.outlet.static.df.node_id == 724, "max_flow_rate"] = 400

# Lage Waard
model.pump.static.df.loc[model.pump.static.df.node_id == 114, "max_downstream_level"] = -0.49

# Outlets Lauwersmeer aanpassen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1754, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1755, "max_downstream_level"] = pd.NA

model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "max_flow_rate"] = 15
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "flow_rate"] = 2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1754, "max_flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1755, "max_flow_rate"] = 0

model.outlet.static.df.loc[model.outlet.static.df.node_id == 1747, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "min_upstream_level"] = -0.97
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1754, "min_upstream_level"] = -0.97
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1755, "min_upstream_level"] = -0.97
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1747, "min_upstream_level"] = -0.97
model.outlet.static.df.loc[model.outlet.static.df.node_id == 724, "min_upstream_level"] = -0.89
# Robbegat afvoergemaal
model.pump.static.df.loc[model.pump.static.df.node_id == 42, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 43, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 42, "min_upstream_level"] += 0.02
model.pump.static.df.loc[model.pump.static.df.node_id == 43, "min_upstream_level"] += 0.02

model.outlet.static.df.loc[model.outlet.static.df.node_id == 1746, "max_flow_rate"] = 0.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1756, "max_flow_rate"] = 0.0  # Check! Sluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1747, "max_flow_rate"] = 0.0  # Check! Sluis

# Streefpeilen basin gelijk daardoor geen aanvoer
model.outlet.static.df.loc[model.outlet.static.df.node_id == 653, "max_downstream_level"] = 9.14
model.outlet.static.df.loc[model.outlet.static.df.node_id == 460, "min_upstream_level"] = 9.12
model.outlet.static.df.loc[model.outlet.static.df.node_id == 460, "max_downstream_level"] = 9.10
model.outlet.static.df.loc[model.outlet.static.df.node_id == 421, "min_upstream_level"] = 9.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 439, "min_upstream_level"] = 7.14


# Outlet 342
model.outlet.static.df.loc[model.outlet.static.df.node_id == 342, "min_upstream_level"] = -0.84


# Dokwerd is een inlaat naar Hunze en Aa's
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1752, "flow_rate"] = 0.0

# Afvoer outlets die naast aanvoergemaal liggen moet min_upstream gelijk aan max_downstream
model.outlet.static.df.loc[model.outlet.static.df.node_id == 545, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 521, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 398, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 605, "min_upstream_level"] += 0.02

# max_downstream omhoog door downstream manning
model.outlet.static.df.loc[model.outlet.static.df.node_id == 340, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 632, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 634, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 631, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 322, "min_upstream_level"] += 0.02


model.pump.static.df.loc[model.pump.static.df.node_id == 165, "max_downstream_level"] = 0.25
model.outlet.static.df.loc[model.outlet.static.df.node_id == 398, "max_downstream_level"] = -0.69

# aanvoer
model.update_node(node_id=943, node_type="Outlet")
model.outlet.static.df.loc[model.outlet.static.df.node_id == 943, "min_upstream_level"] = -0.51
model.outlet.static.df.loc[model.outlet.static.df.node_id == 943, "max_downstream_level"] = -0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 943, "flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 943, "max_flow_rate"] = 5

model.update_node(node_id=1017, node_type="Outlet")
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1017, "min_upstream_level"] = -0.71
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1017, "max_downstream_level"] = -0.9

exclude_ids = {
    1745,
    1746,
    1740,
    1756,
    1744,
    1738,
    716,
    683,
    725,
}  # scheepvaartsluizen moeten op flow_rate=0
df = model.outlet.static.df
mask = df["node_id"].isin(exclude_ids)
df.loc[mask, "flow_rate"] = 0.0
df.loc[mask, "max_flow_rate"] = 0.0

# %%
exclude_node_ids: set[int] = set()


def exclude(*node_ids):
    """Voeg node_id(s) toe aan de globale exclude set."""
    for x in node_ids:
        if x is None:
            continue
        if isinstance(x, (list, tuple, set, np.ndarray)):
            for y in x:
                if pd.notna(y):
                    exclude_node_ids.add(int(y))
        else:
            if pd.notna(x):
                exclude_node_ids.add(int(x))


# %%
# Rondpompen voorkomen bij 2 aanvoer en afvoer gemaal direct naast elkaar, min_upstream en max_downstream gelijk maken
# === INLINE: max_downstream_level(iKGM/iKST-pumps) = min_upstream_level(KGM/KST peer: pump óf outlet) ===
print("=== Corrigeren iKGM/KGM_i & iKST/KST_i-pompen (rondpompen voorkomen) ===")


# --- Helper om waarden iets te verschuiven ---
# --- Helper om waarden iets te verschuiven ---
def bump(v, delta):
    """Verhoog/verlaag scalar of array met delta; NaN blijft NaN."""
    if isinstance(v, (list, tuple, np.ndarray)):
        arr = pd.to_numeric(np.asarray(v), errors="coerce")
        arr = np.where(np.isnan(arr), arr, arr + float(delta))
        return arr.tolist()
    x = float(v)
    return x + float(delta) if not np.isnan(x) else v


# --- Dataframes ---
# --- Dataframes ---
pump_static_df = model.pump.static.df
outlet_static_df = model.outlet.static.df

# --- Kolommen bepalen ---
code_col_pump = "meta_code"
code_col_outlet = "meta_code"

min_us_col_pump = "min_upstream_level" if "min_upstream_level" in pump_static_df.columns else "min_upstream_water_level"
max_ds_col_pump = (
    "max_downstream_level" if "max_downstream_level" in pump_static_df.columns else "max_downstream_water_level"
)
min_us_col_outlet = (
    "min_upstream_level" if "min_upstream_level" in outlet_static_df.columns else "min_upstream_water_level"
)

print(f"Gebruik kolommen: Pump={min_us_col_pump}/{max_ds_col_pump}, Outlet={min_us_col_outlet}")
print(f"Codekolommen: pump={code_col_pump}, outlet={code_col_outlet}")

# -----------------------------------------------------------
# 1️⃣ Peers verzamelen (KGM/KST met geldige min_upstream)
# -----------------------------------------------------------
print(f"Gebruik kolommen: Pump={min_us_col_pump}/{max_ds_col_pump}, Outlet={min_us_col_outlet}")
print(f"Codekolommen: pump={code_col_pump}, outlet={code_col_outlet}")

# -----------------------------------------------------------
# 1️⃣ Peers verzamelen (KGM/KST met geldige min_upstream)
# -----------------------------------------------------------
peer_from_pumps = pump_static_df[[code_col_pump, min_us_col_pump]].copy()
peer_from_outlet = outlet_static_df[[code_col_outlet, min_us_col_outlet]].copy()

peer_from_pumps["code"] = peer_from_pumps[code_col_pump].astype(str).str.strip()
peer_from_outlet["code"] = peer_from_outlet[code_col_outlet].astype(str).str.strip()

peer_from_pumps = peer_from_pumps[
    peer_from_pumps["code"].str.upper().str.startswith(("KGM", "KST"), na=False)
    & peer_from_pumps[min_us_col_pump].notna()
].rename(columns={min_us_col_pump: "min_upstream_peer"})[["code", "min_upstream_peer"]]

peer_from_outlet = peer_from_outlet[
    peer_from_outlet["code"].str.upper().str.startswith(("KGM", "KST"), na=False)
    & peer_from_outlet[min_us_col_outlet].notna()
].rename(columns={min_us_col_outlet: "min_upstream_peer"})[["code", "min_upstream_peer"]]

peer_sources_df = pd.concat([peer_from_pumps, peer_from_outlet], ignore_index=True).drop_duplicates(subset=["code"])
code_to_min_upstream_peer = dict(
    zip(peer_sources_df["code"].str.upper().to_numpy(), peer_sources_df["min_upstream_peer"].astype(float).to_numpy())
)

print(f"Peers gevonden: {len(peer_sources_df)}")
if not peer_sources_df.empty:
    print("Voorbeelden peers:")
    print(peer_sources_df.head(10))
else:
    print("⚠️ Geen peers gevonden (controleer min_upstream-levels).")

# -----------------------------------------------------------
# 2️⃣ i-pompen vinden (i vooraan of _i achteraan)
# -----------------------------------------------------------
i_pumps_df = pump_static_df[[code_col_pump, "node_id", min_us_col_pump, max_ds_col_pump]].copy()
i_pumps_df["icode"] = i_pumps_df[code_col_pump].astype(str).str.strip()

# Herken zowel iKGM/iKST als KGM_i/KST_i
mask_i = (
    i_pumps_df["icode"].str.match(r"(?i)^iKGM")  # iKGM... (case-insensitive)
    | i_pumps_df["icode"].str.match(r"(?i)^iKST")
    | i_pumps_df["icode"].str.match(r"(?i)^KGM.*_i$")
    | i_pumps_df["icode"].str.match(r"(?i)^KST.*_i$")
)
i_pumps_df = i_pumps_df[mask_i].copy()

if i_pumps_df.empty:
    print("⚠️ Geen iKGM/iKST of KGM_i/KST_i-pompen gevonden.")
else:
    # Basiscode bepalen: verwijder leading 'i' of trailing '_i'
    i_pumps_df["base_code"] = (
        i_pumps_df["icode"]
        .str.replace("^i", "", case=False, regex=True)
        .str.replace("_i$", "", case=False, regex=True)
        .str.strip()
    )

    # Koppel met peers
    i_pumps_df["peer_min_upstream"] = i_pumps_df["base_code"].str.upper().map(code_to_min_upstream_peer)

    # Samenvatting
    missing = i_pumps_df[i_pumps_df["peer_min_upstream"].isna()]
    matched = i_pumps_df[i_pumps_df["peer_min_upstream"].notna()]

    print(f"Gevonden i-pompen: {len(i_pumps_df)} (waarvan {len(matched)} met geldige peer)")
    if not matched.empty:
        print(matched[[code_col_pump, "base_code", "peer_min_upstream"]].head(10))
    if not missing.empty:
        print(f"⚠️ Geen match voor {len(missing)} pompen:")
        print(missing[[code_col_pump, "base_code"]].head(10))

    # -------------------------------------------------------
    # 3️⃣ Aanpassen van matched i-pompen
    # -------------------------------------------------------
    if not matched.empty:
        node_to_new_maxds = dict(zip(matched["node_id"], matched["peer_min_upstream"]))
        mask_nodes = pump_static_df["node_id"].isin(node_to_new_maxds.keys())

        print(f"➡️ Ga {mask_nodes.sum()} pompen aanpassen (up/down-levels).")

        # Oude waarden
        before = pump_static_df.loc[mask_nodes, [code_col_pump, min_us_col_pump, max_ds_col_pump]].copy()

        # max_downstream = peer.min_upstream
        pump_static_df.loc[mask_nodes, max_ds_col_pump] = pump_static_df.loc[mask_nodes, "node_id"].map(
            node_to_new_maxds
        )
print(f"Peers gevonden: {len(peer_sources_df)}")
if not peer_sources_df.empty:
    print("Voorbeelden peers:")
    print(peer_sources_df.head(10))
else:
    print("⚠️ Geen peers gevonden (controleer min_upstream-levels).")

# -----------------------------------------------------------
# 2️⃣ i-pompen vinden (i vooraan of _i achteraan)
# -----------------------------------------------------------
i_pumps_df = pump_static_df[[code_col_pump, "node_id", min_us_col_pump, max_ds_col_pump]].copy()
i_pumps_df["icode"] = i_pumps_df[code_col_pump].astype(str).str.strip()

# Herken zowel iKGM/iKST als KGM_i/KST_i
mask_i = (
    i_pumps_df["icode"].str.match(r"(?i)^iKGM")  # iKGM... (case-insensitive)
    | i_pumps_df["icode"].str.match(r"(?i)^iKST")
    | i_pumps_df["icode"].str.match(r"(?i)^KGM.*_i$")
    | i_pumps_df["icode"].str.match(r"(?i)^KST.*_i$")
)
i_pumps_df = i_pumps_df[mask_i].copy()

if i_pumps_df.empty:
    print("⚠️ Geen iKGM/iKST of KGM_i/KST_i-pompen gevonden.")
else:
    # Basiscode bepalen: verwijder leading 'i' of trailing '_i'
    i_pumps_df["base_code"] = (
        i_pumps_df["icode"]
        .str.replace("^i", "", case=False, regex=True)
        .str.replace("_i$", "", case=False, regex=True)
        .str.strip()
    )

    # Koppel met peers
    i_pumps_df["peer_min_upstream"] = i_pumps_df["base_code"].str.upper().map(code_to_min_upstream_peer)

    # Samenvatting
    missing = i_pumps_df[i_pumps_df["peer_min_upstream"].isna()]
    matched = i_pumps_df[i_pumps_df["peer_min_upstream"].notna()]

    print(f"Gevonden i-pompen: {len(i_pumps_df)} (waarvan {len(matched)} met geldige peer)")
    if not matched.empty:
        print(matched[[code_col_pump, "base_code", "peer_min_upstream"]].head(10))
    if not missing.empty:
        print(f"⚠️ Geen match voor {len(missing)} pompen:")
        print(missing[[code_col_pump, "base_code"]].head(10))

    # -------------------------------------------------------
    # 3️⃣ Aanpassen van matched i-pompen
    # -------------------------------------------------------
    if not matched.empty:
        node_to_new_maxds = dict(zip(matched["node_id"], matched["peer_min_upstream"]))
        mask_nodes = pump_static_df["node_id"].isin(node_to_new_maxds.keys())

        print(f"➡️ Ga {mask_nodes.sum()} pompen aanpassen (up/down-levels).")

        # Oude waarden
        before = pump_static_df.loc[mask_nodes, [code_col_pump, min_us_col_pump, max_ds_col_pump]].copy()

        # max_downstream = peer.min_upstream
        pump_static_df.loc[mask_nodes, max_ds_col_pump] = pump_static_df.loc[mask_nodes, "node_id"].map(
            node_to_new_maxds
        )

        # min_upstream = min_upstream - 0.02
        pump_static_df.loc[mask_nodes, min_us_col_pump] = pump_static_df.loc[mask_nodes, min_us_col_pump].apply(
            lambda v: bump(v, -0.02)
        )
        # min_upstream = min_upstream - 0.02
        pump_static_df.loc[mask_nodes, min_us_col_pump] = pump_static_df.loc[mask_nodes, min_us_col_pump].apply(
            lambda v: bump(v, -0.02)
        )

        # Nieuwe waarden
        after = pump_static_df.loc[mask_nodes, [code_col_pump, min_us_col_pump, max_ds_col_pump]].copy()
        merged = before.merge(after, on=code_col_pump, suffixes=("_old", "_new"))

        print("✅ Aanpassingen (eerste 10):")
        print(merged.head(10))
    else:
        print("⚠️ Geen geldige koppelingen gevonden — niets aangepast.")

print("=== Klaar ===")


def build_exclude_rondpompen_node_ids(model) -> set[int]:
    pump_df = model.pump.static.df.copy()
    out_df = model.outlet.static.df.copy()
    code_col = "meta_code"
    if code_col not in pump_df.columns or code_col not in out_df.columns:
        return set()

    ip = pump_df[[code_col, "node_id"]].copy()
    ip["icode"] = ip[code_col].astype(str).str.strip()

    mask_i = (
        ip["icode"].str.match(r"(?i)^iKGM")
        | ip["icode"].str.match(r"(?i)^iKST")
        | ip["icode"].str.match(r"(?i)^KGM.*_i$")
        | ip["icode"].str.match(r"(?i)^KST.*_i$")
    )
    ip = ip[mask_i].copy()
    if ip.empty:
        return set()

    ip["base_code"] = (
        ip["icode"]
        .str.replace("^i", "", case=False, regex=True)
        .str.replace("_i$", "", case=False, regex=True)
        .str.strip()
    )
    base_codes = set(ip["base_code"].str.upper().dropna().unique())

    pump_peers = pump_df[[code_col, "node_id"]].copy()
    pump_peers["code"] = pump_peers[code_col].astype(str).str.strip().str.upper()
    pump_peer_ids = set(pump_peers.loc[pump_peers["code"].isin(base_codes), "node_id"].astype(int))

    out_peers = out_df[[code_col, "node_id"]].copy()
    out_peers["code"] = out_peers[code_col].astype(str).str.strip().str.upper()
    out_peer_ids = set(out_peers.loc[out_peers["code"].isin(base_codes), "node_id"].astype(int))

    i_ids = set(ip["node_id"].astype(int))
    return i_ids | pump_peer_ids | out_peer_ids


exclude_ids = {
    1745,
    1746,
    1740,
    1756,
    1744,
    1738,
    716,
    683,
    725,
    1741,
    471,
    742,
    743,
    712,
    605,
    611,
    614,
    623,
    516,
    554,
    707,
    548,
    680,
    640,
    641,
    537,
    400,
    457,
    730,
    726,
    537,
}
exclude_rondpompen = build_exclude_rondpompen_node_ids(model)
exclude(exclude_ids)
exclude(exclude_rondpompen)


# %%
def seasonal_steps_apr_oct(
    *,
    years=range(2015, 2026),
    on_value: float,
    off_value: float,
):
    series_start = pd.Timestamp(min(years), 1, 1)
    series_end_inclusive = pd.Timestamp(max(years), 12, 31)
    hard_end = series_end_inclusive + pd.Timedelta(days=1)

    events = [(series_start, float(off_value))]
    for y in years:
        on = pd.Timestamp(y, 4, 1)  # 1 april 00:00
        off = pd.Timestamp(y, 10, 1) + pd.Timedelta(days=1)  # 2 okt 00:00 (1 okt incl.)
        events.append((on, float(on_value)))
        events.append((off, float(off_value)))

    events.sort(key=lambda x: x[0])
    dedup = {}
    for ts, val in events:
        dedup[ts] = float(val)
    events = sorted(dedup.items(), key=lambda x: x[0])

    t_list, v_list = [], []
    for ts, val in events:
        if series_start <= ts <= hard_end:
            t_list.append(ts)
            v_list.append(val)

    if t_list[-1] != series_end_inclusive:
        t_list.append(series_end_inclusive)
        v_list.append(v_list[-1])
    if t_list[-1] != hard_end:
        t_list.append(hard_end)
        v_list.append(v_list[-1])

    return pd.to_datetime(t_list), np.array(v_list, dtype=float)


# %%


# Discrete control
def add_controller(
    model,
    node_ids,
    *,
    # --- luister-nodes ---
    listen_node_ids=None,
    listen_node_id: int = 1493,
    weights=None,
    # --- thresholds (single-mode) ---
    threshold_high: float = 7.68,
    threshold_low: float | None = None,
    threshold_delta: float | None = None,
    state_labels=("aanvoer", "afvoer"),
    # --- NIEUW: modes voor discrete control ---
    # "single" = bestaande gedrag
    dc_mode: str = "single",
    # ✅ toevoegen voor level_or_qmin
    qmin_on: float | None = None,
    qmin_off: float | None = None,
    q_listen_node_id: int | None = None,
    # --- flow instellingen voor OUTLETS ---
    flow_aanvoer_outlet="orig",
    flow_afvoer_outlet="orig",
    # --- max_flow instellingen voor OUTLETS ---
    max_flow_aanvoer_outlet="orig",
    max_flow_afvoer_outlet="orig",
    # --- pump instellingen ---
    flow_aanvoer_pump="orig",
    flow_afvoer_pump="orig",
    # --- max_flow instellingen voor PUMPS ---
    max_flow_aanvoer_pump="orig",
    max_flow_afvoer_pump="orig",
    # --- downstream instellingen ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,
    delta_max_ds_aanvoer=None,
    delta_max_ds_afvoer=None,
    # --- nieuw: delta's voor upstream_min levels ---
    delta_us_aanvoer=None,
    delta_us_afvoer=None,
    keep_min_us=True,
    min_upstream_aanvoer=None,
    min_upstream_afvoer=None,
    dc_offset=10.0,
    # ✅ voeg deze toe:
    level_threshold_time=None,
    level_threshold_high=None,
    level_threshold_low=None,
):
    """
    Voeg discrete controllers toe op pumps/outlets met uitgebreide overschrijfmogelijkheden.

    Extra (nieuw):
    - dc_mode="diff_with_gate":
        compound_variable 1 = level_us - level_ds (weights +1 en -1)
        condition 1: thresholds op verschil
        compound_variable 2 = level_us (gate)
        condition 2: upstream level moet boven gate om AFVOER te mogen
        logic: default aanvoer ("**"), alleen "TT" -> afvoer
    """
    out_df = model.outlet.static.df
    pump_df = model.pump.static.df

    outlet_ids = set(out_df["node_id"].astype(int))
    pump_ids = set(pump_df["node_id"].astype(int))

    # hysterese (single-mode)
    # - als threshold_delta is gezet: low = high - delta
    # - anders, als threshold_low expliciet is gezet: gebruik die
    # - anders: geen hysterese => low = high
    if threshold_delta is not None:
        threshold_low_used = float(threshold_high) - float(threshold_delta)
    elif threshold_low is not None:
        threshold_low_used = float(threshold_low)
    else:
        threshold_low_used = float(threshold_high)

    def _find_max_flow_col(row):
        for col in ("max_flow_rate", "max_flow", "max_discharge"):
            if col in row.index:
                return col
        return None

    def _split2(v):
        """Maak (v0,v1) uit list[2], anders (v,v). Leeg/NA -> (nan, nan)."""
        if isinstance(v, list) and len(v) == 2:
            a, b = v
            a = float(a) if pd.notna(a) else float("nan")
            b = float(b) if pd.notna(b) else float("nan")
            return a, b
        if pd.notna(v):
            try:
                x = float(v)
                return x, x
            except Exception:
                pass
        return float("nan"), float("nan")

    def _is_force_nan(x):
        """Herken waarden die expliciet 'overschrijf naar NaN' betekenen."""
        if x is pd.NA:
            return True
        if isinstance(x, float) and np.isnan(x):
            return True
        if isinstance(x, str) and x.lower() == "nan":
            return True
        return False

    def _resolve_max_flow_mode(mode, cur):
        """
        mode:
          - "orig" -> behoud huidige (cur)
          - pd.NA / np.nan / "nan" -> FORCE overschrijven naar NaN
          - numeriek -> overschrijven
          - None -> behoud huidig (orig)
        """  # NOQA D205
        if _is_force_nan(mode):
            return float("nan")
        if mode is None:
            return float(cur)
        if isinstance(mode, str) and mode == "orig":
            return float(cur)
        if isinstance(mode, (int, float)):
            return float(mode)
        raise ValueError(f"Onbekende max_flow mode: {mode!r}")

    def _resolve_max_ds(mode, base):
        """Return een numerieke waarde of base of NaN afhankelijk van mode (NA-safe)."""
        if mode is pd.NA or mode is None or (isinstance(mode, float) and np.isnan(mode)):
            return float("nan")
        if isinstance(mode, (int, float)):
            return float(mode)
        if isinstance(mode, str) and mode == "existing":
            return base
        if isinstance(mode, str) and mode.lower() == "nan":
            return float("nan")
        return base

    # listen nodes
    if listen_node_ids is not None:
        nodes_to_listen = listen_node_ids if isinstance(listen_node_ids, (list, tuple)) else [listen_node_ids]
    else:
        nodes_to_listen = [listen_node_id]

    # weights (single-mode / algemene fallback)
    if weights is None:
        weights = [1.0] * len(nodes_to_listen)
    elif isinstance(weights, (int, float)):
        weights = [float(weights)] * len(nodes_to_listen)

    if len(weights) != len(nodes_to_listen):
        raise ValueError(f"Gewichtslijst verkeerde lengte: {len(weights)} vs {len(nodes_to_listen)} listen_nodes")

    # helper: NA-safe float
    def _f(x):
        try:
            return float(x)
        except Exception:
            return float("nan")

    for nid in map(int, node_ids):
        # type detect
        if nid in outlet_ids:
            node_type = "Outlet"
            row = out_df.loc[out_df["node_id"] == nid].iloc[0]
            geom = model.outlet[nid].geometry
            parent = model.outlet[nid]
        elif nid in pump_ids:
            node_type = "Pump"
            row = pump_df.loc[pump_df["node_id"] == nid].iloc[0]
            geom = model.pump[nid].geometry
            parent = model.pump[nid]
        else:
            print(f"[skip] Node {nid}: geen pump/outlet")
            continue

        # orig basics
        flow_orig = _f(row.get("flow_rate")) if pd.notna(row.get("flow_rate")) else float("nan")
        max_ds_orig = _f(row.get("max_downstream_level")) if pd.notna(row.get("max_downstream_level")) else float("nan")
        min_us_orig = _f(row.get("min_upstream_level")) if pd.notna(row.get("min_upstream_level")) else float("nan")

        # flows per state + max_flow modes per state
        if node_type == "Outlet":
            flow0 = (
                flow_orig
                if (isinstance(flow_aanvoer_outlet, str) and flow_aanvoer_outlet == "orig")
                else _f(flow_aanvoer_outlet)
            )
            flow1 = (
                flow_orig
                if (isinstance(flow_afvoer_outlet, str) and flow_afvoer_outlet == "orig")
                else _f(flow_afvoer_outlet)
            )
            mf_mode0, mf_mode1 = max_flow_aanvoer_outlet, max_flow_afvoer_outlet
        else:
            flow0 = (
                flow_orig
                if (isinstance(flow_aanvoer_pump, str) and flow_aanvoer_pump == "orig")
                else _f(flow_aanvoer_pump)
            )
            flow1 = (
                flow_orig
                if (isinstance(flow_afvoer_pump, str) and flow_afvoer_pump == "orig")
                else _f(flow_afvoer_pump)
            )
            mf_mode0, mf_mode1 = max_flow_aanvoer_pump, max_flow_afvoer_pump

        # downstream (max_downstream_level)
        ds0 = _resolve_max_ds(max_ds_aanvoer, max_ds_orig)
        ds1 = _resolve_max_ds(max_ds_afvoer, max_ds_orig)

        if delta_max_ds_aanvoer is not None:
            ds0 = float(ds0) + float(delta_max_ds_aanvoer)
        if delta_max_ds_afvoer is not None:
            ds1 = float(ds1) + float(delta_max_ds_afvoer)

        # upstream (min_upstream_level) — basis + delta_us_*
        def _base_upstream(override, orig):
            if override is not None:
                return float(override)
            if keep_min_us and orig is not None and not np.isnan(orig):
                return float(orig)
            return float("nan")

        base0 = _base_upstream(min_upstream_aanvoer, min_us_orig)
        base1 = _base_upstream(min_upstream_afvoer, min_us_orig)

        if delta_us_aanvoer is not None and not np.isnan(base0):
            base0 = float(base0) + float(delta_us_aanvoer)
        if delta_us_afvoer is not None and not np.isnan(base1):
            base1 = float(base1) + float(delta_us_afvoer)

        # maak min_upstream lijst of None
        if (not np.isnan(base0)) or (not np.isnan(base1)):
            min_us_vals = [
                (float(base0) if not np.isnan(base0) else float("nan")),
                (float(base1) if not np.isnan(base1) else float("nan")),
            ]
        else:
            min_us_vals = None

        # static kwargs
        static_kwargs = {
            "control_state": list(state_labels),
            "flow_rate": [flow0, flow1],
            "max_downstream_level": [ds0, ds1],
        }
        if min_us_vals is not None:
            static_kwargs["min_upstream_level"] = min_us_vals

        # --- max_flow kolom: detectie & OVERSCHRIJF-LOGICA (NA-safe) ---
        max_flow_col = _find_max_flow_col(row)
        if max_flow_col is not None:
            cur0, cur1 = _split2(row.get(max_flow_col))
            out0 = _resolve_max_flow_mode(mf_mode0, cur0)
            out1 = _resolve_max_flow_mode(mf_mode1, cur1)
            static_kwargs[max_flow_col] = [out0, out1]

        # update node in model
        if node_type == "Outlet":
            model.update_node(nid, "Outlet", [outlet.Static(**static_kwargs)])
        else:
            model.update_node(nid, "Pump", [pump.Static(**static_kwargs)])

        # -----------------------------
        # DISCRETE CONTROL toevoegen
        # -----------------------------
        dc_node = Node(geometry=Point(geom.x + dc_offset, geom.y))

        if dc_mode == "supply_if_ds_low_else_drain":
            # listen_node_ids = [upstream_basin_id, downstream_basin_id]
            if not isinstance(nodes_to_listen, (list, tuple)) or len(nodes_to_listen) < 2:
                raise ValueError(
                    "dc_mode='supply_if_ds_low_else_drain' vereist listen_node_ids=[upstream_basin, downstream_basin]"
                )

            us_id = int(nodes_to_listen[0])
            ds_id = int(nodes_to_listen[1])

            # We gebruiken:
            # - threshold_low  = streef upstream (S)
            # - threshold_high = streef upstream + band (S_hi)
            # - max_ds_aanvoer = LOGISCHE downstream drempel M (ds laag?)
            if threshold_low is None:
                raise ValueError("supply_if_ds_low_else_drain vereist threshold_low = streef (S)")
            if threshold_high is None:
                raise ValueError("supply_if_ds_low_else_drain vereist threshold_high = streef+band (S_hi)")

            # ✅ LOGICA: downstream-drempel komt uit max_ds_aanvoer (moet numeriek zijn)
            if not isinstance(max_ds_aanvoer, (int, float)):
                raise ValueError(
                    "supply_if_ds_low_else_drain: max_ds_aanvoer moet numeriek zijn (M_logic), geen 'existing'/None"
                )

            S = float(threshold_low)
            S_hi = float(threshold_high)
            M = float(max_ds_aanvoer)  # <-- LET OP: NIET max_ds_afvoer!

            # compound 1 = upstream level
            # compound 2 = -downstream level  (ds <= M  <=>  -ds >= -M)
            variable_blocks = [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=us_id,
                    variable=["level"],
                    weight=[1.0],
                ),
                discrete_control.Variable(
                    compound_variable_id=2,
                    listen_node_id=ds_id,
                    variable=["level"],
                    weight=[-1.0],
                ),
            ]

            # Geen hysterese: threshold_low == threshold_high per condition
            # A: us >= S
            # B: us >= S_hi
            # C: ds <= M
            conds = [
                discrete_control.Condition(
                    compound_variable_id=[1],
                    condition_id=[1],
                    threshold_high=[S],
                    threshold_low=[S],
                ),
                discrete_control.Condition(
                    compound_variable_id=[1],
                    condition_id=[2],
                    threshold_high=[S_hi],
                    threshold_low=[S_hi],
                ),
                discrete_control.Condition(
                    compound_variable_id=[2],
                    condition_id=[3],
                    threshold_high=[-M],
                    threshold_low=[-M],
                ),
            ]

            AN = state_labels[0]  # aanvoer
            AF = state_labels[1]  # afvoer

            # Truth order = [A, B, C] = [us>=S, us>=S_hi, ds<=M]
            # - us < S           => A=F,B=F => AANVOER
            # - us >= S_hi       => B=T     => AFVOER
            # - us in [S,S_hi)   => A=T,B=F => C bepaalt: ds laag => AANVOER, anders AFVOER
            dc = model.discrete_control.add(
                dc_node,
                variable_blocks
                + conds
                + [
                    # us >= S_hi => AFVOER (ongeacht C)
                    discrete_control.Logic(truth_state=["TTT"], control_state=[AF]),
                    discrete_control.Logic(truth_state=["TTF"], control_state=[AF]),
                    # us in band: ds laag? => AANVOER, anders AFVOER
                    discrete_control.Logic(truth_state=["TFT"], control_state=[AN]),
                    discrete_control.Logic(truth_state=["TFF"], control_state=[AF]),
                    # us < S => AANVOER (ongeacht C)
                    discrete_control.Logic(truth_state=["FFT"], control_state=[AN]),
                    discrete_control.Logic(truth_state=["FFF"], control_state=[AN]),
                ],
            )

        elif dc_mode == "level_or_qmin":
            # 2 conditions:
            # A (id=1): level hoog?
            # B (id=2): Q te laag?

            level_listen = int(nodes_to_listen[0]) if len(nodes_to_listen) >= 1 else int(listen_node_id)
            flow_listen = int(q_listen_node_id) if q_listen_node_id is not None else int(nid)

            if threshold_high is None:
                raise ValueError("level_or_qmin vereist threshold_high")
            if qmin_on is None or qmin_off is None:
                raise ValueError("level_or_qmin vereist qmin_on en qmin_off")

            if threshold_delta is not None:
                lvl_low = float(threshold_high) - float(threshold_delta)
            elif threshold_low is not None:
                lvl_low = float(threshold_low)
            else:
                lvl_low = float(threshold_high)  # geen hysterese

            variable_blocks = [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=level_listen,
                    variable=["level"],
                    weight=[1.0],
                ),
                discrete_control.Variable(
                    compound_variable_id=2,
                    listen_node_id=flow_listen,
                    variable=["flow_rate"],
                    weight=[-1.0],  # -Q, zodat "Q te laag" => True
                ),
            ]

            conds = []

            # --- Conditie A (id=1): level hoog? (seizoensgebonden mogelijk) ---
            if level_threshold_time is None:
                conds.append(
                    discrete_control.Condition(
                        compound_variable_id=[1],
                        condition_id=[1],
                        threshold_high=[float(threshold_high)],
                        threshold_low=[float(lvl_low)],
                    )
                )
            else:
                th_time = pd.to_datetime(level_threshold_time)
                th_hi = np.asarray(level_threshold_high, dtype=float).reshape(-1)
                th_lo = (
                    np.asarray(level_threshold_low, dtype=float).reshape(-1)
                    if level_threshold_low is not None
                    else th_hi
                )

                if len(th_time) != len(th_hi) or len(th_time) != len(th_lo):
                    raise ValueError(
                        f"level_or_qmin season thresholds length mismatch: "
                        f"time={len(th_time)}, high={len(th_hi)}, low={len(th_lo)}"
                    )

                conds.append(
                    discrete_control.Condition(
                        compound_variable_id=1,  # scalar OK bij timeseries
                        condition_id=1,  # scalar OK bij timeseries
                        time=th_time,
                        threshold_high=th_hi,
                        threshold_low=th_lo,
                    )
                )

            # --- Conditie B (id=2): Q te laag? (altijd toevoegen!) ---
            conds.append(
                discrete_control.Condition(
                    compound_variable_id=[2],
                    condition_id=[2],
                    # True  als Q <= qmin_on  <=> -Q >= -qmin_on
                    # False als Q >= qmin_off <=> -Q <= -qmin_off
                    threshold_high=[-float(qmin_on)],
                    threshold_low=[-float(qmin_off)],
                )
            )

            # jouw gewenste truth table:
            # TT -> afvoer
            # TF -> afvoer
            # FT -> aanvoer
            # FF -> afvoer
            dc = model.discrete_control.add(
                dc_node,
                variable_blocks
                + conds
                + [
                    discrete_control.Logic(truth_state=["TT"], control_state=[state_labels[1]]),  # afvoer
                    discrete_control.Logic(truth_state=["TF"], control_state=[state_labels[1]]),  # afvoer
                    discrete_control.Logic(truth_state=["FT"], control_state=[state_labels[0]]),  # aanvoer
                    discrete_control.Logic(truth_state=["FF"], control_state=[state_labels[1]]),  # afvoer
                ],
            )

        else:
            # ---- bestaand single-mode gedrag ----
            truth = ["F", "T"]
            ctrl = [state_labels[0], state_labels[1]]

            variable_blocks = [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=int(lnid),
                    variable=["level"],
                    weight=[float(w)],
                )
                for lnid, w in zip(nodes_to_listen, weights)
            ]

            cond_kwargs = {
                "compound_variable_id": [1],
                "condition_id": [1],
                "threshold_high": [float(threshold_high)],
                "threshold_low": [float(threshold_low_used)],  # altijd meegeven
            }

            variable_blocks += [
                discrete_control.Condition(**cond_kwargs),
                discrete_control.Logic(truth_state=truth, control_state=ctrl),
            ]
            dc = model.discrete_control.add(dc_node, variable_blocks)

        model.link.add(dc, parent)
        print(f"[OK] controller toegevoegd aan {node_type} {nid}")


def _existing_dc_target_node_ids(model) -> set[int]:
    links = model.link.df
    src_col = "from_node_id" if "from_node_id" in links.columns else "from_node"
    dst_col = "to_node_id" if "to_node_id" in links.columns else "to_node"

    dc_node_ids = set(model.discrete_control.node.df.index.astype(int))
    dc_links = links[links[src_col].isin(dc_node_ids)]
    return set(dc_links[dst_col].astype(int).to_list())


# %%
# interne polder
# aanvoer
selected_node_ids = [721, 88]
exclude(selected_node_ids)

add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1241,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

target_nodes = [87]
exclude(target_nodes)
# afvoerpumps/oulets aanvoer dicht, afvoer open, ===
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1132,
    # thresholds
    threshold_high=3.13,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer=1000,  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # controllers werkt niet goed met NAN waarden daarom 1000
    delta_max_ds_aanvoer=0.0,  # geen extra
    delta_max_ds_afvoer=None,
    keep_min_us=True,
    delta_us_afvoer=0.02,  # Stuw hoger in winter dan zomer
)

target_nodes = [640, 641]
exclude(target_nodes)
# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1127,
    # thresholds
    threshold_high=-0.36,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,
    flow_afvoer_pump="orig",
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_afvoer=1000,  # voorkomt NaN
    keep_min_us=True,
    delta_us_afvoer=0.02,  # Stuw hoger in winter dan zomer
    delta_us_aanvoer=-0.04,  # Stuw hoger in winter dan zomer
)


# %% Marnerwaard

# aanvoer
selected_node_ids = [117, 129, 110, 107, 108, 123, 121, 701, 715]
exclude(selected_node_ids)

add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

# === afvoerpumps/oulets ===
selected_node_ids = [535, 536, 534, 533, 506, 443]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000.0,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)

# aanvoer en afvoer
target_nodes = [566, 482, 496]
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    # --- upstream constraint behouden ---
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

target_nodes = [645]
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    # --- upstream constraint behouden ---
    keep_min_us=True,
    min_upstream_afvoer=-2.2,
)

target_nodes = [360, 1017]
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    # --- upstream constraint behouden ---
    keep_min_us=True,
    min_upstream_afvoer=-0.71,
    min_upstream_aanvoer=-0.74,
    delta_us_aanvoer=-0.04,
)


# %%# === Aanvoergemalen/aanvoerpumps in het midNoorden ===
selected_node_ids = [165, 32, 146, 169, 35]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1026,
    # thresholds
    threshold_high=-1.14,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

# %%# === Aanvoergemalen/aanvoerpumps in het midNoorden ===


# === afvoerpumps/oulets ===
selected_node_ids = [723, 145, 147, 31, 34, 168]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1026,
    # thresholds
    threshold_high=-1.14,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)


# %%Letterbediep
# === afvoerpumps/oulets ===
selected_node_ids = [
    181,
]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1021,
    # thresholds
    threshold_high=-1.03,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)
selected_node_ids = [711]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1021,
    # thresholds
    threshold_high=-1.03,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

target_nodes = [643]
exclude(target_nodes)
# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1021,
    # thresholds
    threshold_high=-1.03,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,
    flow_afvoer_pump="orig",
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_afvoer=1000,  # voorkomt NaN
    keep_min_us=True,
    delta_us_afvoer=0.04,  # Stuw hoger in winter dan zomer
    delta_us_aanvoer=-0.04,  # Stuw hoger in winter dan zomer
)


# === afvoerpumps/oulets ===
selected_node_ids = [182]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1021,
    # thresholds
    threshold_high=-1.03,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)

# %%
# De verbetering
# === afvoerpumps/oulets ===
selected_node_ids = [47]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1332,
    # thresholds
    threshold_high=-1.72,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)

# %% Dokwerd

selected_node_ids = [
    1753,  # Dokwerd listen_node_id moet nog worden aangepast
]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1713,
    # thresholds
    threshold_high=-0.32,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=8,  # open bij laagwater → laat water in
    max_flow_aanvoer_outlet=18,
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

# %%
# Lauwersmeer
# === Aanvoergemalen/aanvoerpumps Grote pompen en outlets===
add_controller(
    model=model,
    node_ids=[29, 30],
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # ---- PUMP FLOWS ----
    flow_aanvoer_pump=75,  # laagwater → pomp staat uit
    max_flow_aanvoer_pump=75,
    flow_afvoer_pump=75,  # hoogwater → pomp draait op originele flow
    # ---- MAX DOWNSTREAM ----
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.0,
    max_ds_afvoer=1000,  # open grenzen bij hoogwater
    keep_min_us=True,
    min_upstream_afvoer=-0.93,
    min_upstream_aanvoer=-1,
    delta_us_aanvoer=-0.04,
)


selected_node_ids = [
    1748,  # afvoer Friesland
    1755,  # afvoer Friesland
    1754,  # afvoer Friesland
    1747,  # afvoer Friesland
]
exclude(selected_node_ids)

add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
    min_upstream_afvoer=-0.91,
)


# aanvoer
selected_node_ids = [708, 43, 714]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.05,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

# Aanvoer en afvoer
selected_node_ids = [
    727,
    728,  # HD Louwes Uitlaat en waterwolf outlet
]
exclude(selected_node_ids)

add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1132,
    # thresholds
    threshold_high=3.13,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0,
    flow_afvoer_outlet=400.0,  # hoogwater → afvoer open
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer=1000,  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
    min_upstream_afvoer=-0.93,
)
# === afvoerpumps/oulets ===
selected_node_ids = [385]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1132,
    # thresholds
    threshold_high=3.13,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)


# === afvoerpumps/oulets ===
selected_node_ids = [42]
exclude(selected_node_ids)
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)

# aan en afvoer
target_nodes = [
    635,
    442,
]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    # --- upstream constraint behouden ---
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

target_nodes = [720]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,
    max_flow_aanvoer_outlet="orig",
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    # --- upstream constraint behouden ---
    keep_min_us=True,
    min_upstream_afvoer=-0.9,
    delta_us_aanvoer=-0.04,
)

target_nodes = [412]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1277,
    # thresholds
    threshold_high=-0.9,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=0,
    max_flow_aanvoer_outlet=0,
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer=1000,
    max_ds_afvoer=1000,  # voorkomt NaN
    # --- upstream constraint behouden ---
    keep_min_us=True,
    min_upstream_afvoer=-0.9,
    delta_us_aanvoer=-0.04,
)


# %% Dwarsdiep
# =========================
# Dwarsdiep
# =========================


# ---------- helpers ----------
def _get_basin_target_level(model, basin_id: int) -> float | None:
    """Haal streefpeil uit model.basin.area.df['meta_streefpeil'] voor basin node_id."""
    df = model.basin.area.df
    row = df.loc[df["node_id"].astype(int) == int(basin_id)]
    if row.empty:
        return None
    val = row.iloc[0].get("meta_streefpeil", None)
    return None if pd.isna(val) else float(val)


def _basin_ids(model) -> set[int]:
    return set(model.basin.node.df.index.astype(int))


def dedup(seq):
    """Dedup list, behoud volgorde."""
    seen = set()
    out = []
    for x in seq:
        x = int(x)
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def find_upstream_basin_id(model, node_id: int) -> int | None:
    """Vind eerste upstream Basin via inkomende link Basin -> node."""
    links = model.link.df
    basin_ids = _basin_ids(model)
    froms = links.loc[links["to_node_id"] == int(node_id), "from_node_id"].astype(int).tolist()
    for fid in froms:
        if fid in basin_ids:
            return fid
    return None


def find_downstream_basin_id(model, start_node_id: int, max_hops: int = 6) -> int | None:
    """Vind eerste downstream Basin (BFS over links)."""
    links = model.link.df
    basin_ids = _basin_ids(model)

    frontier = [int(start_node_id)]
    visited = {int(start_node_id)}

    for _ in range(max_hops + 1):
        new_frontier = []
        for cur in frontier:
            tos = links.loc[links["from_node_id"] == cur, "to_node_id"].astype(int).tolist()
            for to in tos:
                if to in basin_ids:
                    return to
                if to not in visited:
                    visited.add(to)
                    new_frontier.append(to)
        if not new_frontier:
            break
        frontier = new_frontier

    return None


def _streef(model, basin_id: int | None) -> float | None:
    if basin_id is None:
        return None
    return _get_basin_target_level(model, basin_id)


# %%
# =========================
# 1) SUPPLY controllers (aanvoerknopen)
# listen = downstream basin
# threshold_high = streef downstream
# max_ds_aanvoer = streef downstream
# min_upstream: aanvoer = streef upstream - 0.04 ; afvoer = streef upstream
# =========================

# SUPPLY_KWARGS = {
#     # flows
#     "flow_aanvoer_outlet": 20,
#     "flow_afvoer_outlet": 0,
#     "max_flow_afvoer_outlet": 0,
#     "flow_afvoer_pump": 0,
#     "max_flow_afvoer_pump": 0,
#     "flow_aanvoer_pump": "orig",
#     "delta_max_ds_afvoer": None,
#     # upstream (we zetten min_upstream expliciet, dus delta's altijd None)
#     "keep_min_us": True,
#     "delta_us_aanvoer": None,
#     "delta_us_afvoer": None,
# }


# def add_supply_controllers_auto_ds_streef(
#     model,
#     target_nodes,
#     *,
#     base_kwargs,
#     max_ds_offset: float = 0.0,
#     min_us_aanvoer_offset: float = -0.04,
#     min_us_afvoer_offset: float = 0.0,
#     skip_if_no_upstream: bool = True,
# ):
#     target_nodes = dedup(target_nodes)
#     exclude(target_nodes)

#     for nid in target_nodes:
#         ds_basin = find_downstream_basin_id(model, nid)
#         S_ds = _streef(model, ds_basin)
#         if ds_basin is None or S_ds is None:
#             print(f"[skip] supply {nid}: ds_basin/streef ontbreekt")
#             continue
#         S_ds = float(S_ds)

#         kw = dict(base_kwargs)
#         # nooit delta_us doorschuiven
#         kw["delta_us_aanvoer"] = None
#         kw["delta_us_afvoer"] = None

#         # max_ds_aanvoer = streef(ds)
#         kw["max_ds_aanvoer"] = S_ds + float(max_ds_offset)
#         kw["delta_max_ds_aanvoer"] = 0.0

#         # min_us = streef(us) (optioneel)
#         us_basin = find_upstream_basin_id(model, nid)
#         S_us = _streef(model, us_basin)

#         if S_us is None:
#             msg = f"[warn] supply {nid}: us_basin/streef ontbreekt -> min_upstream niet gezet"
#             if skip_if_no_upstream:
#                 print(msg)
#             else:
#                 raise ValueError(msg)
#         else:
#             S_us = float(S_us)
#             kw["min_upstream_aanvoer"] = S_us + float(min_us_aanvoer_offset)
#             kw["min_upstream_afvoer"] = S_us + float(min_us_afvoer_offset)

#         add_controller(
#             model=model,
#             node_ids=[nid],
#             listen_node_id=int(ds_basin),
#             threshold_high=S_ds,
#             **kw,
#         )

#         if "min_upstream_aanvoer" in kw:
#             print(
#                 f"[OK] supply {nid}: ds={ds_basin} S_ds={S_ds:.3f} | "
#                 f"us={us_basin} S_us={float(S_us):.3f} "
#                 f"min_us=[{kw['min_upstream_aanvoer']:.3f},{kw['min_upstream_afvoer']:.3f}]"
#             )
#         else:
#             print(f"[OK] supply {nid}: ds={ds_basin} S_ds={S_ds:.3f} | min_upstream: (niet gezet)")


# %%


# %%
aanvoergebieden_gpkg = cloud.joinpath(r"Noorderzijlvest/verwerkt/aanvoergebieden.gpkg")
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


# supply nodes (inlaten) definieren. Bij Noorderzijlvest alles wat begint met INL (e.g. KGM143_i, iKGM018 en INL077)
# 37: Diepswal
# 38: Jonkersvaart
supply_nodes = [37, 38]

# drain nodes (uitlaten) definieren (intern, maar alléén drainerend)
# 551: KST0556
# 652: KST1072
drain_nodes = [551, 652]

# doorspoeling (uitlaten) definieren
# 357: KST0159 Trambaanstuw
# 357: KST0159 Trambaanstuw
# 393: KST0135 Ackerenstuw
# 350: KST0053 Lage Hamrikstuw
# 401: KST0148 Mastuw
# 501: KST0379 Ooster Lietsstuw
# 383: KST0113 Lage Rietstuw
flushing_nodes = {357: 0.02, 393: 0.012, 350: 0.015, 401: 0.017, 501: 0.034, 383: 0.013}

# aanmaken node_df met supply_nodes
node_df = model.node_table().df
node_df["supply_node"] = (
    node_df["meta_code_waterbeheerder"].str.startswith("INL")
    | node_df["meta_code_waterbeheerder"].str.startswith("i")
    | node_df["meta_code_waterbeheerder"].str.endswith("i")
    | node_df.index.isin(supply_nodes)
)

# input-variabelen bij latere functie
polygon = aanvoergebieden_df.at["jonkersvaart", "geometry"]
control_node_types = ["Outlet", "Pump"]
ignore_intersecting_links: list[int] = [1810]

# determine outflow_nodes, inflow_nodes and internal nodes
outflow_nodes, inflow_nodes, internal_nodes = control_nodes_from_supply_area(
    model=model,
    polygon=polygon,
    control_node_types=["Outlet", "Pump"],
    ignore_intersecting_links=ignore_intersecting_links,
)

# get internal drainage nodes. internal drain nodes + outflow_nodes = drain_nodes
control_nodes_df = node_df.loc[internal_nodes]
drain_nodes += sorted(
    get_drain_nodes(model=model, control_nodes_df=control_nodes_df, supply_bool_col="supply_node") + outflow_nodes
)
drain_nodes = [i for i in drain_nodes if i not in flushing_nodes.keys()]

# supply nodes = set of nodes defined as supply-nodes + inflow_nodes (even if not defined as inflow-nodes)
supply_nodes = sorted(set(control_nodes_df[control_nodes_df.supply_node].index.to_list() + inflow_nodes))

# supply nodes cannot be drain nodes at the same time
supply_in_drain_nodes = [i for i in supply_nodes if i in drain_nodes]
if supply_in_drain_nodes:
    raise ValueError(f"Nodes labeled as supply also in drain_nodes {supply_in_drain_nodes}")

# the rest are flow_control_nodes (doorlaten)
flow_control_nodes = sorted(
    [i for i in control_nodes_df.index if i not in drain_nodes + supply_nodes + list(flushing_nodes.keys())]
)

# %% 292 mist hier
# SUPPLY_NODES = [
#     36,
#     37,
#     38,
#     287,
#     288,
#     293,
#     295,
#     296,
#     687,
#     688,
#     689,
#     690,
#     694,
#     695,
#     696,
#     697,
#     698,
#     699,
#     704,
#     705,
#     706,
#     707,
# ]
# %%
# add_supply_controllers_auto_ds_streef(
#     model,
#     supply_nodes,
#     base_kwargs=SUPPLY_KWARGS,
#     max_ds_offset=0.0,
#     min_us_aanvoer_offset=-0.04,
#     min_us_afvoer_offset=0.0,
#     skip_if_no_upstream=True,
# )

add_controllers_to_supply_nodes(model=model, us_target_level_offset_supply=-0.04, supply_nodes=supply_nodes)
exclude(supply_nodes)

# =========================
# 2) DRAIN controllers (afvoerknopen)
# listen = upstream basin
# threshold_high = streef upstream
# min_upstream (beide states) = streef upstream
# =========================

# DRAIN_KWARGS = {
#     # outlets
#     "flow_aanvoer_outlet": 0.0,
#     "max_flow_aanvoer_outlet": 0,
#     "flow_afvoer_outlet": 100.0,
#     # pumps
#     "flow_aanvoer_pump": 0.0,
#     "flow_afvoer_pump": "orig",
#     # downstream hydrauliek
#     "max_ds_aanvoer": 1000,
#     "max_ds_afvoer": 1000,
#     "delta_max_ds_aanvoer": 0.0,
#     "delta_max_ds_afvoer": None,
#     # upstream
#     "keep_min_us": True,
#     "delta_us_aanvoer": None,
#     "delta_us_afvoer": None,
# }

add_controllers_to_drain_nodes(model=model, drain_nodes=drain_nodes)
exclude(drain_nodes)
# def add_controllers_to_drain_nodes(
#     model,
#     target_nodes,
#     *,
#     base_kwargs,
#     target_level_offset: float = 0.0,
#     skip_if_no_upstream: bool = False,
#     target_level_column: str = "meta_streefpeil",
# ):
#     target_nodes = dedup(target_nodes)
#     exclude(target_nodes)

#     for node_id in target_nodes:
#         us_basin = model.upstream_node_id(node_id=node_id)
#         target_level = model.basin.area.df.set_index("node_id").at[us_basin, target_level_column]

#         if us_basin is None or target_level is None:
#             msg = f"[warn] drain node {node_id}: target_level missing in upstream basin {us_basin}"
#             if skip_if_no_upstream:
#                 print(msg + " -> skip")
#                 continue
#             raise ValueError(msg)

#         target_level += float(target_level_offset)

#         kwargs = dict(base_kwargs)
#         kwargs["delta_us_aanvoer"] = None
#         kwargs["delta_us_afvoer"] = None
#         kwargs["min_upstream_aanvoer"] = target_level
#         kwargs["min_upstream_afvoer"] = target_level

#         add_controller(
#             model=model,
#             node_ids=[node_id],
#             listen_node_id=int(us_basin),
#             threshold_high=target_level,
#             **kwargs,
#         )

#         print(f"[OK] drain {node_id}: upstream basin={us_basin} min_upstream_level={target_level:.3f} ")


# waarom zijn 551 en 652 drain nodes en geen flow_control_nodes (doorlaten?)
# [424, 492] waarschijnlijk vergeten met interne drain_nodes (naast inlaat)
# nog filteren op flushing_demand_nodes
# DRAIN_NODES = [389, 433, 455, 502, 519, 549, 550, 551, 564, 565, 618, 652, 681]

# =========================
# 3) Doorstroomknopen - supply_if_ds_low_else_drain
# (jouw grote targets lijst)
# =========================


# def add_controls_supply_if_ds_low_else_drain(
#     model,
#     target_node_ids,
#     *,
#     band_hi: float = 0.02,  # S_hi = S + 0.02
#     gate_drop: float = 0.04,  # aanvoer: min_upstream zakt 0.04
#     ds_offset: float = 0.0,  # M_logic = streef_ds + ds_offset
#     max_ds_afvoer_hyd: float = 1000.0,
#     max_hops_ds: int = 6,
# ):
#     target_node_ids = dedup(target_node_ids)
#     exclude(target_node_ids)

#     for tid in target_node_ids:
#         us_basin_id = find_upstream_basin_id(model, tid)
#         ds_basin_id = find_downstream_basin_id(model, tid, max_hops=max_hops_ds)
#         if us_basin_id is None or ds_basin_id is None:
#             print(f"[skip] flowcontrol {tid}: upstream/downstream basin niet gevonden")
#             continue

#         S_us = _streef(model, us_basin_id)
#         S_ds = _streef(model, ds_basin_id)
#         if S_us is None or S_ds is None:
#             print(f"[skip] flowcontrol {tid}: streefpeil ontbreekt (us={us_basin_id}, ds={ds_basin_id})")
#             continue

#         S_us = float(S_us)
#         M_logic = float(S_ds) + float(ds_offset)

#         add_controller(
#             model=model,
#             node_ids=[tid],
#             listen_node_ids=[int(us_basin_id), int(ds_basin_id)],
#             dc_mode="supply_if_ds_low_else_drain",
#             threshold_low=S_us,
#             threshold_high=S_us + float(band_hi),
#             # LOGICA-drempel via max_ds_aanvoer (moet numeriek zijn)
#             max_ds_aanvoer=M_logic,
#             # HYD: afvoer niet blokkeren
#             max_ds_afvoer=float(max_ds_afvoer_hyd),
#             keep_min_us=False,
#             min_upstream_aanvoer=S_us - float(gate_drop),
#             min_upstream_afvoer=S_us,
#         )

#         print(
#             f"[OK] flowcontrol {tid}: us={us_basin_id}(S={S_us:.3f}) "
#             f"ds={ds_basin_id}(M_logic={M_logic:.3f}) S_hi={S_us + band_hi:.3f}"
#         )


# [356, 400, 457, 537, 544, 553, 553, 742, 743] liggen buiten gebied
# [128, 551, 608, 652] missen
# nog filteren op fushing_demand_nodes
# FLOWCONTROL_NODES = [
#     338,
#     339,
#     343,
#     345,
#     346,
#     355,
#     356,
#     362,
#     364,
#     367,
#     368,
#     368,
#     369,
#     374,
#     378,
#     382,
#     382,
#     395,
#     399,
#     399,
#     400,
#     405,
#     410,
#     414,
#     426,
#     430,
#     432,
#     438,
#     440,
#     448,
#     449,
#     453,
#     457,
#     463,
#     474,
#     478,
#     491,
#     495,
#     497,
#     497,
#     498,
#     498,
#     500,
#     504,
#     510,
#     511,
#     512,
#     514,
#     515,
#     515,
#     526,
#     527,
#     528,
#     529,
#     530,
#     537,
#     538,
#     540,
#     541,
#     541,
#     542,
#     542,
#     543,
#     544,
#     547,
#     548,
#     552,
#     553,
#     553,
#     554,
#     555,
#     556,
#     581,
#     600,
#     600,
#     610,
#     613,
#     620,
#     649,
#     655,
#     667,
#     742,
#     743,
# ]

add_controllers_to_flow_control_nodes(
    model=model, flow_control_nodes=flow_control_nodes, us_threshold_offset=LEVEL_DIFFERENCE_THRESHOLD
)
# add_controls_supply_if_ds_low_else_drain(
#     model,
#     flow_control_nodes,
#     band_hi=0.02,
#     gate_drop=0.04,
#     ds_offset=0.0,
#     max_ds_afvoer_hyd=1000.0,
# )


# =========================
# 4) Dwarsdiep outlets met doorspoeling (level_or_qmin)
# =========================


def add_doorspoeling_level_or_qmin(
    model,
    targets,  # [(name, demand_m3s, node_id)]
    *,
    years=range(2015, 2026),
    band: float = 0.003,
    hysteresis: float = 0.0001,
    qmin_on_offset: float = 0.001,
    qmin_off_offset: float = 0.002,
    aanvoer_drop: float = 0.06,  # jouw huidige gedrag; zet op 0.04 als je 4cm wil
    skip_if_no_upstream: bool = False,
):
    node_ids = [int(nid) for _, _, nid in targets]
    exclude(node_ids)

    basin_ids = _basin_ids(model)

    for name, demand_m3s, nid in targets:
        nid = int(nid)

        # upstream basin via inkomende link (Basin -> target)
        us_basin = next(
            (
                r.from_node_id
                for r in model.link.df.itertuples(index=False)
                if int(r.to_node_id) == nid and int(r.from_node_id) in basin_ids
            ),
            None,
        )
        S = _streef(model, us_basin)

        if us_basin is None or S is None:
            msg = f"[warn] doorspoeling {nid} ({name}): us_basin/streef ontbreekt"
            if skip_if_no_upstream:
                print(msg + " -> skip")
                continue
            raise ValueError(msg)

        S = float(S)

        t_lvl, lvl_hi = seasonal_steps_apr_oct(
            years=years,
            on_value=S + band,
            off_value=-1e6,
        )
        lvl_lo = np.where(lvl_hi > -1e5, lvl_hi - hysteresis, lvl_hi)

        qmin_on = float(demand_m3s + qmin_on_offset)
        qmin_off = float(demand_m3s + qmin_off_offset)

        add_controller(
            model=model,
            node_ids=[nid],
            dc_mode="level_or_qmin",
            listen_node_id=int(us_basin),
            threshold_high=S + band,
            threshold_delta=hysteresis,
            qmin_on=qmin_on,
            qmin_off=qmin_off,
            q_listen_node_id=nid,
            level_threshold_time=t_lvl,
            level_threshold_high=lvl_hi,
            level_threshold_low=lvl_lo,
            keep_min_us=False,
            min_upstream_afvoer=S,
            min_upstream_aanvoer=S - float(aanvoer_drop),
            max_ds_afvoer=1000,
            max_ds_aanvoer=1000,
            flow_aanvoer_outlet=0.0,
            max_flow_aanvoer_outlet=pd.NA,
            flow_afvoer_outlet="orig",
            flow_aanvoer_pump=0.0,
            flow_afvoer_pump="orig",
        )

        print(
            f"[OK] doorspoeling {nid} ({name}): us={us_basin} S={S:.3f} qmin_on={qmin_on:.3f} qmin_off={qmin_off:.3f}"
        )


add_controllers_and_demand_to_flushing_nodes(
    model=model, flushing_nodes=flushing_nodes, us_threshold_offset=LEVEL_DIFFERENCE_THRESHOLD
)

# DOORSPOELING_TARGETS = [
#     ("KST0159 Trambaanstuw", 0.020, 357),
#     ("KST0135 Ackerenstuw", 0.012, 393),
#     ("KST0053 Lage Hamrikstuw", 0.015, 350),
#     ("KST0148 Mastuw", 0.017, 401),
#     ("KST0379 Ooster Lietsstuw", 0.034, 501),
#     ("KST0113 Lage Rietstuw", 0.013, 383),
# ]

# add_doorspoeling_level_or_qmin(
#     model,
#     DOORSPOELING_TARGETS,
#     years=range(2015, 2026),
#     band=0.003,
#     hysteresis=0.0001,
#     qmin_on_offset=0.001,
#     qmin_off_offset=0.002,
#     aanvoer_drop=0.04,
# )


# %%
# === Peizerdiep,
# listen_node_id: max ds_aanvoer niet verhogen!

target_nodes = [446, 736, 719, 644, 589, 363, 493, 636, 634, 624, 340, 332, 403]
exclude(target_nodes)
# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.02,
    # --- flows ---
    flow_aanvoer_outlet=20,  # of bv. 0.0
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=1000,  # voorkomt NaN
    keep_min_us=True,
)

target_nodes = [
    322,
    567,
    494,
]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.02,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    # --- upstream constraint behouden ---
    keep_min_us=True,
)

target_nodes = [740, 741, 560, 44, 638, 633, 606, 575, 327]
exclude(target_nodes)
# afvoerpumps/oulets aanvoer dicht, afvoer open, ===
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.02,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # controllers werkt niet goed met NAN waarden daarom 1000
    delta_us_afvoer=0.04,  # Stuw hoger in winter dan zomer
    keep_min_us=True,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [45, 1739, 1742, 1751]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.02,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.04,  # extra aanvoer ivm Manning 1743
    max_ds_afvoer=9999,
    keep_min_us=True,
)
# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [1758, 48]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1332,
    # thresholds
    threshold_high=-1.7,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.04,  # extra aanvoer ivm Manning 1743
    max_ds_afvoer=9999,
    keep_min_us=True,
)


# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
# extra aanvoer voor 1743 door max_downstream +0.04 ivm Manning 1743
target_nodes = [1743]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.02,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    max_flow_aanvoer_outlet=0.5,
    flow_afvoer_pump=0,
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.04,
    max_ds_afvoer=9999,
    keep_min_us=True,
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen

target_nodes = [435, 499, 748, 326, 568, 569, 563, 570, 571, 572, 737, 738, 739]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.02,
    # threshold_low=7.50,        # optioneel: hysterese
    # --- flows ---
    flow_aanvoer_outlet=20,  # of bv. 0.0
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=1000,  # voorkomt NaN
    keep_min_us=True,
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen

target_nodes = [452, 651]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1332,
    # thresholds
    threshold_high=-1.7,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,  # of bv. 0.0
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=1000,  # voorkomt NaN
    keep_min_us=True,
)


# %%
# == Noordpolder,
target_nodes = [521, 524, 431, 417, 487, 40, 546, 545]
exclude(target_nodes)
# afvoerpumps/oulets aanvoer dicht, afvoer open, ===
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1280,
    # thresholds
    threshold_high=-0.34,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # controllers werkt niet goed met NAN waarden daarom 1000
    delta_max_ds_aanvoer=0.0,  # geen extra
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)


# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [691]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1280,
    # thresholds
    threshold_high=-0.34,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [118]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1280,
    # thresholds
    threshold_high=-0.34,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [116, 115, 112, 122]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1280,
    # thresholds
    threshold_high=-0.34,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
)


# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
target_nodes = [678]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1280,
    # thresholds
    threshold_high=-0.34,
    threshold_delta=0.001,
    # threshold_low=7.50,        # optioneel: hysterese
    # --- flows ---
    flow_aanvoer_outlet=20,  # of bv. 0.0
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=1000,  # voorkomt NaN
    keep_min_us=True,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [119, 125]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1280,
    # thresholds
    threshold_high=-0.34,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
)


# %% === Spijksterpompen

# Model moet in afvoer modus starten
model.basin.state.df.loc[model.basin.state.df.node_id == 1408, "level"] = -0.6
# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
target_nodes = [377, 427, 505, 485, 387]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1182,
    # thresholds
    threshold_high=-0.69,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,
    flow_afvoer_pump="orig",
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,
    delta_max_ds_aanvoer=0.02,
    # --- upstream constraint behouden ---
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)


# afvoer
target_nodes = [423, 731, 462, 837, 371, 428, 396, 398, 466, 336, 342, 472]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1182,
    # thresholds
    threshold_high=-0.69,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater  afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater uit
    # max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0  h_pump / h_out
    max_ds_afvoer=1000,  # controllers werkt niet goed met NAN waarden daarom 1000
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)

# afvoerpumps/oulets aanvoer dicht, afvoer open, ===
target_nodes = [41]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1182,
    # thresholds
    threshold_high=-0.69,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0,
    flow_afvoer_outlet=75.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=pd.NA,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # controllers werkt niet goed met NAN waarden daarom 1000
    delta_max_ds_afvoer=None,
    keep_min_us=True,
    min_upstream_afvoer=-0.69,
    min_upstream_aanvoer=-0.9,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [184, 943, 106, 111, 120]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1182,
    # thresholds
    threshold_high=-0.69,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20.0,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [114]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1182,
    # thresholds
    threshold_high=-0.69,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20.0,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_afvoer=0.02,
)
# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
target_nodes = [124]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1182,
    # thresholds
    threshold_high=-0.69,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,
    flow_afvoer_pump="orig",
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,
    delta_max_ds_aanvoer=0.02,
    # --- upstream constraint behouden ---
    keep_min_us=True,
    delta_us_afvoer=0.02,
)


# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [136, 139, 109]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1182,
    # thresholds
    threshold_high=-0.69,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
target_nodes = [351]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1182,
    # thresholds
    threshold_high=-0.69,
    threshold_delta=0.001,
    # threshold_low=7.50,        # optioneel: hysterese
    # --- flows ---
    flow_aanvoer_outlet=20,  # of bv. 0.0
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    delta_max_ds_aanvoer=0.02,
    # delta_max_ds_afvoer=None,  # optioneel
    # --- upstream constraint behouden ---
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

# %%
# === Fivelingo,afvoerpumps/oulets aanvoer dicht, afvoer open, ===

# Model moet in afvoer modus starten
# model.basin.state.df.loc[model.basin.state.df.node_id == 1172, "level"] = 0

# afvoer
target_nodes = [732, 67]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1172,
    # thresholds
    threshold_high=-1.26,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=50.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # controllers werkt niet goed met NAN waarden daarom 1000
    delta_max_ds_aanvoer=0.0,  # geen extra
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [390]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1172,
    # thresholds
    threshold_high=-1.26,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
target_nodes = [142, 143, 702, 703, 693]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1172,
    # thresholds
    threshold_high=-1.26,
    threshold_delta=0.001,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    keep_min_us=True,
    delta_us_aanvoer=-0.04,
)


# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
target_nodes = [508, 509, 646]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1172,
    # thresholds
    threshold_high=-1.26,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,  # of bv. 0.0
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    delta_max_ds_aanvoer=0.02,
    keep_min_us=True,
    min_upstream_afvoer=-1.18,
    min_upstream_aanvoer=-1.18,
    delta_us_aanvoer=-0.04,
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
target_nodes = [517]
exclude(target_nodes)
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1172,
    # thresholds
    threshold_high=-1.26,
    threshold_delta=0.001,
    # --- flows ---
    flow_aanvoer_outlet=20,  # of bv. 0.0
    flow_afvoer_outlet=100,  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    delta_max_ds_aanvoer=0.02,
    keep_min_us=True,
    min_upstream_afvoer=-1.14,
    min_upstream_aanvoer=-1.16,
    delta_us_aanvoer=-0.04,
)


# %%


def add_flow_demand_years(
    model,
    node_id: int,
    flow_m3s: float,
    *,
    years=range(2015, 2026),  # 2015..2025 incl.
    start_month_day=(4, 1),  # 1 april
    end_month_day=(10, 1),  # 1 oktober (incl.)
    priority: int = 1,
    dx: float = 12.0,
    name_prefix: str = "doorspoeling",
):
    # target node (pomp of outlet)
    if node_id in model.pump.node.df.index:
        target = model.pump[node_id]
    elif node_id in model.outlet.node.df.index:
        target = model.outlet[node_id]
    else:
        raise ValueError(f"Node {node_id} is geen pump of outlet.")

    sm, sd = start_month_day
    em, ed = end_month_day

    y0, y1 = min(years), max(years)

    # Volledige periode (geen knip): 1 jan y0 t/m 31 dec y1
    series_start = pd.Timestamp(y0, 1, 1)
    series_end_inclusive = pd.Timestamp(y1, 12, 31)

    # Step-events maken (oplossing B)
    events = []
    for y in years:
        start = pd.Timestamp(y, sm, sd)  # ON op 1 apr 00:00
        end_inclusive = pd.Timestamp(y, em, ed)  # 1 okt incl
        end_exclusive = end_inclusive + pd.Timedelta(days=1)  # OFF op 2 okt 00:00

        events.append((start, float(flow_m3s)))
        events.append((end_exclusive, 0.0))

    # Sorteer + dedup (laatste waarde wint bij gelijke timestamps)
    events.sort(key=lambda x: x[0])
    dedup = {}
    for ts, val in events:
        dedup[ts] = val
    events = sorted(dedup.items(), key=lambda x: x[0])

    # Bouw uiteindelijke reeks:
    # - altijd starten op series_start met 0
    # - alle events binnen (series_start, series_end_inclusive + 1 day] meenemen
    # - afsluiten op series_end_inclusive (en evt. +1 day als er nog een event valt)
    t_list = [series_start]
    d_list = [0.0]

    # We nemen events mee tot en met 1 dag na series_end_inclusive,
    # omdat "OFF" events op 2 okt kunnen vallen en binnen 2015-2025 horen.
    hard_end = series_end_inclusive + pd.Timedelta(days=1)

    for ts, val in events:
        if series_start < ts <= hard_end:
            t_list.append(ts)
            d_list.append(val)

    # Zorg voor netjes einde op 31 dec y1 (00:00 of 00:00 van die dag) + eventueel hard_end
    # Praktisch: voeg series_end_inclusive toe (als die nog niet in de lijst staat)
    if t_list[-1] != series_end_inclusive:
        # bepaal geldende waarde op series_end_inclusive
        end_val = d_list[-1]
        for ts, val in events:
            if ts <= series_end_inclusive:
                end_val = val
            else:
                break
        t_list.append(series_end_inclusive)
        d_list.append(end_val)

    # Als laatste event ná 31 dec valt (bijv. hard_end), dan kunnen we die ook toevoegen
    # zodat de step netjes "uitloopt" (optioneel maar vaak prettig).
    if t_list[-1] != hard_end:
        # waarde op hard_end
        hard_val = d_list[-1]
        for ts, val in events:
            if ts <= hard_end:
                hard_val = val
            else:
                break
        t_list.append(hard_end)
        d_list.append(hard_val)

    # flow_demand node maken
    fd = model.flow_demand.add(
        Node(
            geometry=Point(target.geometry.x + dx, target.geometry.y),
            name=f"{name_prefix}_{node_id}",
        ),
        [
            flow_demand.Time(
                time=pd.to_datetime(t_list),
                demand_priority=[priority] * len(t_list),
                demand=np.array(d_list, dtype=float),
            )
        ],
    )

    # koppelen aan pomp/outlet
    model.link.add(fd, target)

    print(
        f"[OK] Flow demand {flow_m3s:.3f} m³/s toegevoegd aan node {node_id} "
        f"voor jaren {y0}-{y1} (1 apr t/m 1 okt incl; oplossing B, GEEN knip)."
    )
    return fd


# targets = [
#     ("KST0159 Trambaanstuw", 0.020, 357),
#     ("KST0135 Ackerenstuw", 0.012, 393),
#     ("KST0053 Lage Hamrikstuw", 0.015, 350),
#     ("KST0148 Mastuw", 0.017, 401),
#     ("KST0379 Ooster Lietsstuw", 0.034, 501),
#     ("KST0113 Lage Rietstuw", 0.013, 383),
#     ("KGM015 Spijksterpompen", 0.40, 41),
#     ("KST0169 Grote Herculesstuw", 0.20, 412),
# ]

# for name, flow, nid in targets:
#     add_flow_demand_years(model, nid, flow, years=range(2015, 2026))


# %%


def add_level_demand(
    model,
    node_id: int,
    min_level: float,
    priority: int = 1,
    offset: float | None = 0.0,
    max_level: float | None = None,
    dx: float = 10,
):
    """
    Voeg een level_demand toe aan een basin-node.

    - Als max_level is gezet: die wordt gebruikt.
    - Anders: max_level = min_level + offset
    """
    if node_id not in model.basin.node.df.index:
        raise ValueError(f"Node {node_id} is geen Basin. level_demand moet op een Basin worden gezet.")

    if max_level is None:
        if offset is None:
            raise ValueError("Geef óf max_level óf offset op.")
        max_level = min_level + offset

    basin = model.basin[node_id]
    x, y = basin.geometry.x, basin.geometry.y

    ld = model.level_demand.add(
        Node(geometry=Point(x + dx, y), name=f"level_demand_{node_id}"),
        [
            level_demand.Static(
                min_level=[min_level],
                max_level=[max_level],  # meestal ook als lijst
                demand_priority=[priority],
            )
        ],
    )

    model.link.add(ld, basin)
    print(f"[OK] Level demand toegevoegd aan basin {node_id} → min={min_level} m, max={max_level} m (prio={priority})")
    return ld


# add_level_demand(model, node_id=1469, min_level=5.99, offset=0.2, priority=1)

check_basin_level.add_check_basin_level(model=model)
model.pump.static.df["meta_func_afvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 0

# Aanvoergemaal moet downstream level krijgen

# De Dijken
model.pump.static.df.loc[model.pump.static.df.node_id == 87, "min_upstream_level"] = -2.5
model.pump.static.df.loc[model.pump.static.df.node_id == 88, "max_downstream_level"] = -2.5

# Den Deel rondpompen voorkomen -4cm upstream!
model.pump.static.df.loc[model.pump.static.df.node_id == 35, "min_upstream_level"] = -1.11
model.pump.static.df.loc[model.pump.static.df.node_id == 35, "max_downstream_level"] = -1.14
model.pump.static.df.loc[model.pump.static.df.node_id == 34, "min_upstream_level"] = -1.14
model.outlet.static.df.loc[model.outlet.static.df.node_id == 726, "max_flow_rate"] = 0.0

# Usquert rondpompen voorkomen basin levell klopt niet?? moet -1.07 zijn? -4cm upstream!
model.pump.static.df.loc[model.pump.static.df.node_id == 169, "max_downstream_level"] = -1.14
model.pump.static.df.loc[model.pump.static.df.node_id == 168, "min_upstream_level"] = -1.14
model.pump.static.df.loc[model.pump.static.df.node_id == 169, "min_upstream_level"] = -1.11


# StadenLande rondpompen voorkomen
# Stad en Lande inlaat
model.pump.static.df.loc[model.pump.static.df.node_id == 32, "min_upstream_level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 32, "max_downstream_level"] = -1.07
model.pump.static.df.loc[model.pump.static.df.node_id == 31, "min_upstream_level"] = -1.07

# Abelstok
model.outlet.static.df.loc[model.outlet.static.df.node_id == 729, "min_upstream_level"] = -1.07
model.pump.static.df.loc[model.pump.static.df.node_id == 147, "min_upstream_level"] = -1.07

# Schaphalsterzijl
model.pump.static.df.loc[model.pump.static.df.node_id == 146, "max_downstream_level"] = -1.07
model.pump.static.df.loc[model.pump.static.df.node_id == 145, "min_upstream_level"] = -1.07
model.pump.static.df.loc[model.pump.static.df.node_id == 146, "min_upstream_level"] = -1

# Pieterburen
model.pump.static.df.loc[model.pump.static.df.node_id == 119, "min_upstream_level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 119, "max_downstream_level"] = -0.73
model.outlet.static.df.loc[model.outlet.static.df.node_id == 545, "min_upstream_level"] = -0.73

# ZwartePier rondpompen voorkomen
model.pump.static.df.loc[model.pump.static.df.node_id == 125, "min_upstream_level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 125, "max_downstream_level"] = -0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 546, "min_upstream_level"] = -0.5

# Wierhuizerklief
model.pump.static.df.loc[model.pump.static.df.node_id == 123, "max_downstream_level"] = -0.71
model.pump.static.df.loc[model.pump.static.df.node_id == 123, "min_upstream_level"] = -1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 534, "min_upstream_level"] = -0.71

# In Noordpolder
model.pump.static.df.loc[model.pump.static.df.node_id == 116, "max_downstream_level"] = -0.18
model.outlet.static.df.loc[model.outlet.static.df.node_id == 521, "min_upstream_level"] = -0.18

# Bockumerklief
model.pump.static.df.loc[model.pump.static.df.node_id == 107, "max_downstream_level"] = -0.71
model.outlet.static.df.loc[model.outlet.static.df.node_id == 535, "min_upstream_level"] = -0.71

# Egelnest
model.pump.static.df.loc[model.pump.static.df.node_id == 108, "min_upstream_level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 108, "max_downstream_level"] = -0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 536, "min_upstream_level"] = -0.5

# Hornhuizerklief
model.pump.static.df.loc[model.pump.static.df.node_id == 110, "max_downstream_level"] = -0.71
model.pump.static.df.loc[model.pump.static.df.node_id == 110, "min_upstream_level"] = -1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 533, "min_upstream_level"] = -0.71


# Aanvoergemaal Onrust downstream_level
model.pump.static.df.loc[model.pump.static.df.node_id == 117, "max_downstream_level"] = -0.71
model.pump.static.df.loc[model.pump.static.df.node_id == 117, "min_upstream_level"] = -0.73

# uitlaat benedestrooms Onrust
model.outlet.static.df.loc[model.outlet.static.df.node_id == 117, "min_upstream_level"] = -0.73

# Klei
model.pump.static.df.loc[model.pump.static.df.node_id == 129, "min_upstream_level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 129, "max_downstream_level"] = -0.73
model.outlet.static.df.loc[model.outlet.static.df.node_id == 507, "min_upstream_level"] = -0.73

# Kluten
model.pump.static.df.loc[model.pump.static.df.node_id == 112, "max_downstream_level"] = 0.37
model.outlet.static.df.loc[model.outlet.static.df.node_id == 524, "min_upstream_level"] = 0.37

# Buiten aanvoer gemaal flow_rate omhoog; bespreken met NZV
model.pump.static.df.loc[model.pump.static.df.node_id == 184, "max_downstream_level"] = -0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 398, "min_upstream_level"] = -0.57

# Oudendijk -4cm upstream!
model.pump.static.df.loc[model.pump.static.df.node_id == 118, "min_upstream_level"] = -1.11
model.pump.static.df.loc[model.pump.static.df.node_id == 118, "max_downstream_level"] = -0.21
model.outlet.static.df.loc[model.outlet.static.df.node_id == 487, "min_upstream_level"] = -0.21

# Slikken
model.pump.static.df.loc[model.pump.static.df.node_id == 121, "max_downstream_level"] = -0.28
model.outlet.static.df.loc[model.outlet.static.df.node_id == 609, "min_upstream_level"] = -0.28

# Vlakbij Eelderdiep
model.outlet.static.df.loc[model.outlet.static.df.node_id == 739, "min_upstream_level"] = -0.72
model.pump.static.df.loc[model.pump.static.df.node_id == 299, "max_downstream_level"] = -0.72

# Nieuwstad Aanvoergemaal en Spijk
model.pump.static.df.loc[model.pump.static.df.node_id == 136, "max_downstream_level"] = -0.69
model.pump.static.df.loc[model.pump.static.df.node_id == 139, "max_downstream_level"] = -0.69
model.pump.static.df.loc[model.pump.static.df.node_id == 136, "min_upstream_level"] = -1.26
model.pump.static.df.loc[model.pump.static.df.node_id == 139, "min_upstream_level"] = -1.26

model.outlet.static.df.loc[model.outlet.static.df.node_id == 377, "min_upstream_level"] = -0.49
model.outlet.static.df.loc[model.outlet.static.df.node_id == 377, "max_flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 693, "max_downstream_level"] = -1.14
model.outlet.static.df.loc[model.outlet.static.df.node_id == 693, "min_upstream_level"] = -0.59
model.outlet.static.df.loc[model.outlet.static.df.node_id == 390, "min_upstream_level"] = -0.59
model.outlet.static.df.loc[model.outlet.static.df.node_id == 390, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 702, "min_upstream_level"] = -0.71
model.outlet.static.df.loc[model.outlet.static.df.node_id == 703, "min_upstream_level"] = -0.71
model.outlet.static.df.loc[model.outlet.static.df.node_id == 703, "max_downstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 702, "max_downstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 722, "min_upstream_level"] = -1.26
# kortsluitingen oplossen noordwesten tov Spijksterpompen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 731, "min_upstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 387, "min_upstream_level"] = -0.59
model.outlet.static.df.loc[model.outlet.static.df.node_id == 837, "min_upstream_level"] = -0.59

model.outlet.static.df.loc[model.outlet.static.df.node_id == 428, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 396, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 371, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 462, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 423, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 336, "max_downstream_level"] = -0.47
# Naar noordpolder
model.outlet.static.df.loc[model.outlet.static.df.node_id == 678, "max_downstream_level"] = -0.34
# Ziekolk

model.outlet.static.df.loc[model.outlet.static.df.node_id == 336, "min_upstream_level"] = -0.39
model.pump.static.df.loc[model.pump.static.df.node_id == 81, "min_upstream_level"] = -1.18

# Quatre Bras downstream level geven
model.pump.static.df.loc[model.pump.static.df.node_id == 120, "max_downstream_level"] = -0.19
model.pump.static.df.loc[model.pump.static.df.node_id == 120, "min_upstream_level"] = -0.36

# Katershorn downstream level geven
model.pump.static.df.loc[model.pump.static.df.node_id == 111, "max_downstream_level"] = -0.45
model.pump.static.df.loc[model.pump.static.df.node_id == 111, "min_upstream_level"] = -0.36
#
model.pump.static.df.loc[model.pump.static.df.node_id == 115, "max_downstream_level"] = -0.34
model.pump.static.df.loc[model.pump.static.df.node_id == 691, "min_upstream_level"] = -0.36
#
model.pump.static.df.loc[model.pump.static.df.node_id == 122, "max_downstream_level"] = -0.34

# %%
# PID controller voor Gaarkeuken
years = [2017, 2020]
gaarkeuken_listen_node = 1186


def make_year_times(y):
    return [
        pd.Timestamp(y, 3, 31),  # net vóór actieve periode -> P=0
        pd.Timestamp(y, 4, 1),  # start actieve periode
        pd.Timestamp(y, 9, 30),  # einde actieve periode
        pd.Timestamp(y, 10, 1),  # na actieve periode -> P=0
    ]


pid_times_all = []
for y in sorted(years):
    pid_times_all.extend(make_year_times(y))

# Als model.starttime eerder is dan de eerste tijd in pid_times_all, voeg een start-marker toe
sim_start = pd.Timestamp(model.starttime)
if sim_start < min(pid_times_all):
    # alleen toevoegen als het nog niet exact bestaat
    if sim_start != pid_times_all[0]:
        pid_times_all.insert(0, sim_start)
pid_times_all = sorted(dict.fromkeys(pid_times_all))
pattern_proportional = [1e6, 1e6, 1e6, 1e6]
pattern_integral = [0.0, 0.0, 0.0, 0.0]
pattern_derivative = [0.0, 0.0, 0.0, 0.0]
pattern_target = [-0.93, -0.93, -0.93, -0.93]

proportional = []
integral = []
derivative = []
targets = []
listen_nodes = []

# helper: maak mapping van year -> its 4 times for matching
year_times_map = {y: make_year_times(y) for y in years}

for t in pid_times_all:
    assigned = False
    for y, times in year_times_map.items():
        if t in times:
            idx = times.index(t)  # 0..3
            proportional.append(pattern_proportional[idx])
            integral.append(pattern_integral[idx])
            derivative.append(pattern_derivative[idx])
            targets.append(pattern_target[idx])
            listen_nodes.append(gaarkeuken_listen_node)
            assigned = True
            break
    if not assigned:
        proportional.append(0.0)
        integral.append(0.0)
        derivative.append(0.0)
        targets.append(-0.93)
        listen_nodes.append(gaarkeuken_listen_node)

model.add_control_node(
    to_node_id=1741,
    data=[
        pid_control.Time(
            time=pid_times_all,
            listen_node_id=listen_nodes,
            target=targets,
            proportional=proportional,
            integral=integral,
            derivative=derivative,
        )
    ],
    ctrl_type="PidControl",
    node_offset=15,
)


# %% Controllers toevoegen voor gemalen/outlets die er nog geen hebben
# Regels:
#  - NOOIT iets toevoegen/overschrijven voor targets in exclude_node_ids
#  - NOOIT iets toevoegen voor targets die al een discrete_control hebben
#  - Alleen OUTLETS
#  - Extra regel 1: als upstream_node van een outlet een PUMP is -> géén controller toevoegen
#  - Extra regel 2: als upstream_node een BASIN is en die basin alleen aan déze outlet hangt -> géén controller toevoegen
#  - Voor overige outlets zonder DC: kies dichtstbijzijnde "anchor" (exclude ∩ has_dc)
#    en kopieer listen_node_id + thresholds van die anchor-controller


# -------------------------
# helpers: link kolomnamen
# -------------------------
def _link_cols(model):
    links = model.link.df
    src_col = "from_node_id" if "from_node_id" in links.columns else "from_node"
    dst_col = "to_node_id" if "to_node_id" in links.columns else "to_node"
    return links, src_col, dst_col


def _target_xy(model, outlet_id: int):
    """XY van outlet node."""
    oid = int(outlet_id)
    if oid not in model.outlet.node.df.index.astype(int):
        return None
    g = model.outlet.node.df.loc[oid, "geometry"]
    if g is None or g is pd.NA:
        return None
    return (float(g.x), float(g.y))


def _upstream_node_id(model, outlet_id: int):
    """Robuust upstream node_id ophalen."""
    try:
        us = model.upstream_node_id(int(outlet_id))
    except Exception:
        return None
    if us is None or us is pd.NA:
        return None
    try:
        return int(us)
    except Exception:
        return None


def _upstream_is_pump(model, outlet_id: int) -> bool:
    us = _upstream_node_id(model, outlet_id)
    if us is None:
        return False
    return us in set(model.pump.node.df.index.astype(int))


def _basin_only_connected_to_this_outlet(model, basin_id: int, outlet_id: int) -> bool:
    """
    True als basin_id in link.df alleen verbonden is met outlet_id (en verder met niets).
    Let op: de Basin->outlet link zélf telt dus niet als "verbonden genoeg".
    """  # NOQA
    links, src_col, dst_col = _link_cols(model)
    bid = int(basin_id)
    oid = int(outlet_id)

    src = pd.to_numeric(links[src_col], errors="coerce")
    dst = pd.to_numeric(links[dst_col], errors="coerce")

    m = (src == bid) | (dst == bid)
    if not m.any():
        return True  # echt los

    neighbors = set(pd.concat([src[m], dst[m]]).dropna().astype(int).tolist())
    neighbors.discard(bid)  # zichzelf weg
    neighbors.discard(oid)  # de outlet waar we nu naar kijken weg

    return len(neighbors) == 0


def _skip_if_upstream_basin_only_this_outlet(model, outlet_id: int) -> bool:
    """
    True als upstream node een Basin is en die basin alleen aan deze outlet hangt.
    """  # NOQA
    us = _upstream_node_id(model, outlet_id)
    if us is None:
        return False

    basin_ids = set(model.basin.node.df.index.astype(int)) if hasattr(model, "basin") else set()
    if us not in basin_ids:
        return False

    return _basin_only_connected_to_this_outlet(model, us, int(outlet_id))


def _dc_for_target(model, target_id: int):
    """Vind discrete_control node_id die naar target_id linkt (DC -> outlet)."""
    links, src_col, dst_col = _link_cols(model)

    dc_node_ids = set(model.discrete_control.node.df.index.astype(int))
    src = pd.to_numeric(links[src_col], errors="coerce")
    dst = pd.to_numeric(links[dst_col], errors="coerce")

    m = src.isin(dc_node_ids) & (dst == int(target_id))
    if not m.any():
        return None
    return int(src.loc[m].dropna().iloc[0])


def _controller_settings_from_dc(model, dc_id: int):
    """Haal listen_node_id + threshold_high/low uit de DC tabellen."""
    var_df = model.discrete_control.variable.df
    cond_df = model.discrete_control.condition.df

    v = var_df.loc[pd.to_numeric(var_df["node_id"], errors="coerce") == int(dc_id)].copy()
    if v.empty:
        return None

    # kies compound_variable_id==1 als die bestaat, anders eerste
    if "compound_variable_id" in v.columns and (v["compound_variable_id"] == 1).any():
        vrow = v.loc[v["compound_variable_id"] == 1].iloc[0]
    else:
        vrow = v.iloc[0]

    listen_id = vrow.get("listen_node_id", pd.NA)
    cvid = vrow.get("compound_variable_id", pd.NA)
    if pd.isna(listen_id) or pd.isna(cvid):
        return None

    c = cond_df.loc[
        (pd.to_numeric(cond_df["node_id"], errors="coerce") == int(dc_id)) & (cond_df["compound_variable_id"] == cvid)
    ]
    if c.empty:
        return None

    crow = c.iloc[0]
    th_high = crow.get("threshold_high", pd.NA)
    th_low = crow.get("threshold_low", pd.NA)
    if pd.isna(th_high) or pd.isna(th_low):
        return None

    return {
        "listen_node_id": int(listen_id),
        "threshold_high": float(th_high),
        "threshold_low": float(th_low),
    }


def add_auto_controllers_for_missing_outlets(
    model,
    *,
    exclude_node_ids: set[int],
    max_ds_afvoer=1000,
    max_ds_aanvoer="existing",
    delta_us_aanvoer=-0.04,  # ✅ jouw wens: min_upstream_level (aanvoer-state) 4cm lager
):
    exclude_node_ids = set(exclude_node_ids or [])

    # ✅ targets = alleen OUTLETS
    all_outlets = set(model.outlet.node.df.index.astype(int))

    # ✅ outlets die al een DC hebben
    has_dc_outlets = _existing_dc_target_node_ids(model) & all_outlets

    # ✅ protected = nooit iets toevoegen/aanzitten
    protected = exclude_node_ids | has_dc_outlets

    # ✅ anchors = exclude ∩ has_dc ∩ outlets
    anchor_outlets = sorted(exclude_node_ids & has_dc_outlets)
    print(f"[INFO] Anchor OUTLETS (exclude ∩ has_dc): {len(anchor_outlets)}")
    if not anchor_outlets:
        print("[WARN] Geen anchor outlets gevonden (exclude_node_ids moet OUTLETS bevatten die al controllers hebben).")
        return 0

    # anchor -> settings
    anchor_settings = {}
    for aid in anchor_outlets:
        dc_id = _dc_for_target(model, aid)
        if dc_id is None:
            continue
        s = _controller_settings_from_dc(model, dc_id)
        if s is not None:
            anchor_settings[aid] = s

    anchor_outlets = [a for a in anchor_outlets if a in anchor_settings]
    print(f"[INFO] Anchor OUTLETS met bruikbare settings: {len(anchor_outlets)}")
    if not anchor_outlets:
        print("[WARN] Anchors gevonden, maar geen bruikbare listen/thresholds in discrete_control tabellen.")
        return 0

    # ✅ kandidaten = outlets zonder DC én niet excluded
    candidates = sorted(all_outlets - protected)

    # precompute anchor xy
    anchor_xy = {}
    for a in anchor_outlets:
        xy = _target_xy(model, a)
        if xy is not None:
            anchor_xy[a] = xy
    if not anchor_xy:
        print("[WARN] Geen anchor outlets met geometry gevonden.")
        return 0

    added = 0
    skipped_upstream_pump = 0
    skipped_isolated_upstream_basin = 0
    skipped_no_xy = 0
    skipped_no_anchor = 0

    print(f"[INFO] OUTLETS zonder discrete_control (en niet excluded): {len(candidates)}")

    for oid in candidates:
        # safety
        if oid in protected:
            continue

        # regel: upstream is pump -> skip
        if _upstream_is_pump(model, oid):
            skipped_upstream_pump += 1
            continue

        # regel: upstream basin die alleen aan deze outlet hangt -> skip
        if _skip_if_upstream_basin_only_this_outlet(model, oid):
            skipped_isolated_upstream_basin += 1
            continue

        oxy = _target_xy(model, oid)
        if oxy is None:
            skipped_no_xy += 1
            continue

        # nearest anchor
        best_anchor = None
        best_d2 = np.inf
        for a, axy in anchor_xy.items():
            dx = oxy[0] - axy[0]
            dy = oxy[1] - axy[1]
            d2 = dx * dx + dy * dy
            if d2 < best_d2:
                best_d2 = d2
                best_anchor = a

        if best_anchor is None:
            skipped_no_anchor += 1
            continue

        s = anchor_settings[best_anchor]

        # ✅ toevoegen (nooit aan exclude of bestaande DC targets)
        add_controller(
            model=model,
            node_ids=[oid],
            listen_node_id=s["listen_node_id"],
            threshold_high=s["threshold_high"],
            threshold_low=s["threshold_low"],
            max_ds_afvoer=max_ds_afvoer,
            max_ds_aanvoer=max_ds_aanvoer,
            keep_min_us=True,
            delta_us_aanvoer=delta_us_aanvoer,  # ✅ aanvoer min_upstream -0.04
        )
        added += 1

    print(f"[OK] Auto-controllers toegevoegd (OUTLETS only): {added}")
    print(
        f"[INFO] Skipped: upstream_pump={skipped_upstream_pump}, "
        f"upstream_basin_only_this_outlet={skipped_isolated_upstream_basin}, "
        f"no_xy={skipped_no_xy}, no_anchor={skipped_no_anchor}"
    )
    print(f"[INFO] Protected outlets (exclude + already_has_dc): {len(protected)}")
    return added


added = add_auto_controllers_for_missing_outlets(
    model,
    exclude_node_ids=exclude_node_ids,
    max_ds_afvoer=1000,
    max_ds_aanvoer="existing",
    delta_us_aanvoer=-0.04,
)
print("Auto OUTLET controllers added:", added)


# %%
# warm start (prerun) en hoofd run met aparte forcings

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
model.solver.level_difference_threshold = LEVEL_DIFFERENCE_THRESHOLD

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=0.1)
model.write(ribasim_toml_dry)

# run hoofdmodel
if MODEL_EXEC:
    result = model.run()
    controle_output = Control(ribasim_toml=ribasim_toml_dry, qlr_path=qlr_path)
    indicators = controle_output.run_all()
    model = Model.read(ribasim_toml_dry)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model.write(ribasim_toml_wet)

# run prerun model
if MODEL_EXEC:
    prerun_result = model.run()
    controle_output = Control(ribasim_toml=ribasim_toml_wet, qlr_path=qlr_path)
    indicators = controle_output.run_all()
    model = Model.read(ribasim_toml_wet)

# hoofd run
update_basin_static(model=model, precipitation_mm_per_day=1.5)
model.write(ribasim_toml)
# run hoofdmodel
if MODEL_EXEC:
    result = model.run()
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_all()


# %% Check listen_node-ids


def export_discrete_controllers_to_gpkg(model, gpkg_path, layer_name="discrete_controllers"):
    dc_nodes = model.discrete_control.node.df.copy()
    dc_var = model.discrete_control.variable.df.copy()
    dc_cond = model.discrete_control.condition.df.copy()
    links = model.link.df.copy()

    # --- link kolommen ---
    src_col = "from_node_id" if "from_node_id" in links.columns else "from_node"
    dst_col = "to_node_id" if "to_node_id" in links.columns else "to_node"

    # --- dc_node_id kolom maken uit index (meestal index = node_id) ---
    dc_nodes2 = dc_nodes.reset_index()

    # bepaal hoe de reset_index-kolom heet
    # (vaak "node_id", soms "index")
    if "node_id" in dc_nodes2.columns and "dc_node_id" not in dc_nodes2.columns:
        dc_nodes2 = dc_nodes2.rename(columns={"node_id": "dc_node_id"})
    elif "index" in dc_nodes2.columns and "dc_node_id" not in dc_nodes2.columns:
        dc_nodes2 = dc_nodes2.rename(columns={"index": "dc_node_id"})
    elif "dc_node_id" not in dc_nodes2.columns:
        # laatste redmiddel: gebruik originele index expliciet
        dc_nodes2["dc_node_id"] = dc_nodes.index.astype(int)

    dc_nodes2["dc_node_id"] = pd.to_numeric(dc_nodes2["dc_node_id"], errors="coerce").astype("Int64")

    dc_node_ids = set(dc_nodes2["dc_node_id"].dropna().astype(int).tolist())

    # --- DC -> target ---
    dc_to_target = links[links[src_col].isin(dc_node_ids)][[src_col, dst_col]].copy()
    dc_to_target = dc_to_target.rename(columns={src_col: "dc_node_id", dst_col: "target_node_id"})
    dc_to_target["dc_node_id"] = pd.to_numeric(dc_to_target["dc_node_id"], errors="coerce").astype("Int64")
    dc_to_target["target_node_id"] = pd.to_numeric(dc_to_target["target_node_id"], errors="coerce").astype("Int64")

    outlet_ids = set(model.outlet.node.df.index.astype(int))
    pump_ids = set(model.pump.node.df.index.astype(int))

    def _target_type(nid):
        if pd.isna(nid):
            return None
        nid = int(nid)
        if nid in outlet_ids:
            return "Outlet"
        if nid in pump_ids:
            return "Pump"
        return "Unknown"

    dc_to_target["target_type"] = dc_to_target["target_node_id"].apply(_target_type)

    # --- Variable -> listen_node_id(s) ---
    var_df = dc_var.copy()
    if "node_id" not in var_df.columns:
        raise KeyError("discrete_control.variable.df mist kolom 'node_id'")

    var_df["dc_node_id"] = pd.to_numeric(var_df["node_id"], errors="coerce").astype("Int64")

    # meestal compound_variable_id == 1, maar pak anders alles
    if "compound_variable_id" in var_df.columns:
        v1 = var_df[var_df["compound_variable_id"] == 1].copy()
        if not v1.empty:
            var_df = v1

    if "listen_node_id" not in var_df.columns:
        raise KeyError("discrete_control.variable.df mist kolom 'listen_node_id'")

    listen_agg = (
        var_df.groupby("dc_node_id")["listen_node_id"]
        .apply(lambda x: ",".join(str(int(v)) for v in sorted(set(pd.to_numeric(x, errors="coerce").dropna()))))
        .reset_index()
    )

    # --- Conditions -> thresholds ---
    cond_df = dc_cond.copy()
    if "node_id" not in cond_df.columns:
        raise KeyError("discrete_control.condition.df mist kolom 'node_id'")

    cond_df["dc_node_id"] = pd.to_numeric(cond_df["node_id"], errors="coerce").astype("Int64")

    if "compound_variable_id" in cond_df.columns:
        c1 = cond_df[cond_df["compound_variable_id"] == 1].copy()
        if not c1.empty:
            cond_df = c1

    keep_cols = ["dc_node_id"]
    for c in ("threshold_high", "threshold_low"):
        if c in cond_df.columns:
            keep_cols.append(c)

    cond_df = cond_df[keep_cols].drop_duplicates("dc_node_id")

    # --- samenvoegen ---
    df = (
        dc_nodes2.merge(dc_to_target, on="dc_node_id", how="left")
        .merge(listen_agg, on="dc_node_id", how="left")
        .merge(cond_df, on="dc_node_id", how="left")
    )

    # --- geometry (controller node geometry) ---
    def _geom(row):
        g = row.get("geometry", None)
        if g is None or pd.isna(g):
            return None
        if hasattr(g, "x") and hasattr(g, "y"):
            return Point(float(g.x), float(g.y))
        return None

    gdf = gpd.GeoDataFrame(df, geometry=df.apply(_geom, axis=1), crs=getattr(model, "crs", None))

    gdf.to_file(gpkg_path, layer=layer_name, driver="GPKG")
    print(f"[OK] geschreven: {gpkg_path} (layer={layer_name}, n={len(gdf)})")
    return gdf


gpkg_path = cloud.joinpath(AUTHORITY, "analyse", "discrete_controllers_listen_nodes.gpkg")
gdf_ctrl = export_discrete_controllers_to_gpkg(model, gpkg_path)
# %%

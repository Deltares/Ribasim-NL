# %%

import geopandas as gpd
import numpy as np
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim import Node
from ribasim.nodes import discrete_control, flow_demand, level_demand, outlet, pid_control, pump
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.control import (
    add_controllers_to_supply_area,
)
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import Point

from ribasim_nl import CloudStorage, Model, check_basin_level

# =========================
# GLOBAL SETTINGS
# =========================

LEVEL_DIFFERENCE_THRESHOLD = 0.02  # sync model.solver.level_difference_threshold and control-settings
MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "Noorderzijlvest"  # authority
SHORT_NAME: str = "nzv"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 722: KSL001
EXCLUDE_NODES = {1745, 1746, 1740, 1756, 1744, 1738, 716, 683, 725, 722}

# =========================
# PATHS AND CLOUD SYNC
# =========================
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(
    AUTHORITY, "verwerkt/1_ontvangen_data//20250527/gebieden_met_wateraanvoermogelijkheid_noorderzijlvest.gpkg"
)
aanvoergebieden_gpkg = cloud.joinpath(r"Noorderzijlvest/verwerkt/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoer_path, qlr_path])


# =========================
# FUNCTIONS
# =========================


# =========================
# READ DATA
# =========================
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# aanmaken node_df en specificeren supply_nodes
# knopen die beginnen met INL, i of eindigen op i, maar niet op fictief
for node_type in CONTROL_NODE_TYPES:
    node_df = getattr(model, pascal_to_snake_case(node_type)).node.df
    node_df[IS_SUPPLY_NODE_COLUMN] = (
        node_df["meta_code_waterbeheerder"].str.startswith("INL")
        | node_df["meta_code_waterbeheerder"].str.startswith("i")
        | node_df["meta_code_waterbeheerder"].str.endswith("i")
    ) & ~(node_df.node_type.isin(CONTROL_NODE_TYPES) & node_df["meta_code_waterbeheerder"].str.endswith("fictief"))
    getattr(model, pascal_to_snake_case(node_type)).node.df = node_df


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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1750, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 687, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 698, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 699, "flow_rate"] = 0.5

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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 943, "min_upstream_level"] = -0.51
model.outlet.static.df.loc[model.outlet.static.df.node_id == 943, "max_downstream_level"] = -0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 943, "flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 943, "max_flow_rate"] = 5

model.outlet.static.df.loc[model.outlet.static.df.node_id == 1017, "min_upstream_level"] = -0.71
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1017, "max_downstream_level"] = -0.9

df = model.outlet.static.df
mask = df["node_id"].isin(EXCLUDE_NODES)
df.loc[mask, "flow_rate"] = 0.0
df.loc[mask, "max_flow_rate"] = 0.0

# %%

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


# EXCLUDE_NODES = {
#     1745,
#     1746,
#     1740,
#     1756,
#     1744,
#     1738,
#     716,
#     683,
#     725,
#     1741,
#     471,
#     742,
#     743,
#     712,
#     605,
#     611,
#     614,
#     623,
#     516,
#     554,
#     707,
#     548,
#     680,
#     640,
#     641,
#     537,
#     400,
#     457,
#     730,
#     726,
# }
exclude_rondpompen = build_exclude_rondpompen_node_ids(model)


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


# %%# === Aanvoergemalen/aanvoerpumps in het midNoorden ===
selected_node_ids = [165, 32, 146, 169, 35]
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

# %% Dokwerd

selected_node_ids = [
    1753,  # Dokwerd listen_node_id moet nog worden aangepast
]
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


# Aanvoer en afvoer
selected_node_ids = [
    727,
    728,  # HD Louwes Uitlaat en waterwolf outlet
]

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


# %% [markdown]
# # Aanpak sturing per aanvoergebied
#
# ## verplichte invoer
# - `polygon`: Polygoon van het aanvoergebied
# ## optionele invoer:
# - `flusing_nodes`: uitlaten met doorspoeldebiet (dict met {node_id:doorspoeldebiet}). Deze worden nooit automatisch gedetecteerd (!)
# - `ignore_intersecting_links`: links die het aanvoergebied snijden, geen control-nodes bevatten en waarbij dat geaccepteerd moet worden
# - `supply_nodes`: inlaten die niet automatisch worden gedetecteerd
# - `drain_nodes`: uitlaten die niet automatisch worden gedetecteerd
#
# ## In 1x, wanneer alles goed gaat
# `node_functions_df` kun je gebruiken om e.e.a. te plotten
# ```
# node_functions_df = add_controllers_to_supply_area(
#     model=model,
#     polygon=polygon,
#     ignore_intersecting_links= ignore_intersecting_links,
#     drain_nodes=drain_nodes,
#     flushing_nodes=flushing_nodes,
#     supply_nodes=supply_nodes,
#     level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
#     control_node_types=CONTROL_NODE_TYPES
# )
# ```
# ## Debuggen: in stappen
# 1. bepalen positie van alle knopen relatief tot aanvoergebied (inflow, outflow, internal)
# ```
# node_positions_df = get_control_nodes_position_from_supply_area(
#     model=model,
#     polygon=polygon,
#     control_node_types=CONTROL_NODE_TYPES,
#     ignore_intersecting_links=ignore_intersecting_links,
# )
# ```
# 2. bepalen functie van alle knopen (drain, supply, flow_control and flushing)
# ```
# node_functions_df = add_control_functions_to_connector_nodes(
#     model=model,
#     node_positions=node_positions_df["position"],
#     supply_nodes=supply_nodes,
#     drain_nodes=drain_nodes,
#     flushing_nodes=flushing_nodes,
#     is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
# )
# ```
#
# 3. toevoegen van controllers aan alle knopen
# ```
# add_controllers_to_connector_nodes(
#     model=model,
#     node_functions_df=node_functions_df,
#     level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
# )
# ```

# %%
# Toevoegen Marnerwaard, Noordpolder, Spijksterpompen & Fivelingoboezem

polygon = aanvoergebieden_df.loc[
    ["Marnerwaard", "Noordpolder", "Spijksterpompen", "Fivelingoboezem"], "geometry"
].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# 40: Noordpolder
# 412: Marnerwaard
# 442: Marnerwaard
# 837: Spijksterpompen
drain_nodes = [40, 442, 412, 837]

# handmatig opgegeven supply nodes (inlaten)
# 136: Nieuwstad
# 139: Spijk
# 943: Inlaat Spijksterpompen
supply_nodes = [136, 139, 943]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# from ribasim_nl.control import (
#     add_control_functions_to_connector_nodes,
#     add_controllers_to_connector_nodes,
#     get_control_nodes_position_from_supply_area,
# )

# node_positions_df = get_control_nodes_position_from_supply_area(
#     model=model,
#     polygon=polygon,
#     exclude_nodes=exclude_nodes,
#     control_node_types=CONTROL_NODE_TYPES,
#     ignore_intersecting_links=ignore_intersecting_links,
# )

# node_functions_df = add_control_functions_to_connector_nodes(
#     model=model,
#     node_positions=node_positions_df["position"],
#     supply_nodes=supply_nodes,
#     drain_nodes=drain_nodes,
#     flushing_nodes=flushing_nodes,
#     is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
# )

# add_controllers_to_connector_nodes(
#     model=model,
#     node_functions_df=node_functions_df,
#     level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
# )

# %%
# Toevoegen Jonkersvaart

polygon = aanvoergebieden_df.at["Jonkersvaart", "geometry"]

# links die intersecten die we kunnen negeren
# 1810: sloot naast prallelweg (A7) die vrij afwatert op Oude Diep
ignore_intersecting_links: list[int] = [1810]

# doorspoeling (op uitlaten)
# 357: KST0159 Trambaanstuw
# 357: KST0159 Trambaanstuw
# 393: KST0135 Ackerenstuw
# 350: KST0053 Lage Hamrikstuw
# 401: KST0148 Mastuw
# 501: KST0379 Ooster Lietsstuw
# 383: KST0113 Lage Rietstuw
flushing_nodes = {357: 0.02, 393: 0.012, 350: 0.015, 401: 0.017, 501: 0.034, 383: 0.013}

# handmatig opgegeven drain nodes (uitlaten) definieren
# 551: KST0556
# 652: KST1072
drain_nodes = [551, 652]

# handmatig opgegeven supply nodes (inlaten)
# 37: Diepswal
# 38: Jonkersvaart
supply_nodes = [37, 38]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Peizerdiep

polygon = aanvoergebieden_df.at["Peizerdiep", "geometry"]

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# node_id: Naam
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
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

# %%
# warm start (prerun) en hoofd run met aparte forcings

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
model.solver.level_difference_threshold = LEVEL_DIFFERENCE_THRESHOLD

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime
model.basin.area.df["meta_aanvoer"] = True
model.outlet.static.df["meta_aanvoer"] = 1
model.pump.static.df["meta_aanvoer"] = 1

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

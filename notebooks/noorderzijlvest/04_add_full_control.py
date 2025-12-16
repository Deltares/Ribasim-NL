# %%
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim import Node
from ribasim.nodes import discrete_control, flow_demand, level_demand, outlet, pid_control, pump
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import Point

from peilbeheerst_model import ribasim_parametrization
from ribasim_nl import CloudStorage, Model, check_basin_level

# keuzes:
USE_PREPROCESSED_MODEL = True  # wil je überhaupt met preprocessed werken?
FORCE_REBUILD_PREPROCESSED = False  # True = altijd opnieuw preprocessen en overschrijven

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
    # --- JOUW BLOK: basins/pumps/outlets maskers etc. ---
    def get_first_upstream_basins(model: Model, node_id: int) -> np.ndarray:
        us_basins = model.get_upstream_basins(node_id=node_id, stop_at_node_type="Basin")
        return us_basins[us_basins.node_id != node_id].node_id.to_numpy()

    def get_first_downstream_basins(model: Model, node_id: int) -> np.ndarray:
        ds_basins = model.get_downstream_basins(node_id=node_id, stop_at_node_type="Basin")
        return ds_basins[ds_basins.node_id != node_id].node_id.to_numpy()

    def is_controlled_basin(model: Model, node_id: int) -> bool:
        ds_node_ids = model._downstream_nodes(node_id=node_id, stop_at_node_type="Basin")
        return (
            not model.node_table()
            .df.loc[list(ds_node_ids)]
            .node_type.isin(["ManningResistance", "LinearResistance"])
            .any()
        )

    def has_all_upstream_controlled_basins(node_id: int, model: Model) -> bool:
        us_basins = get_first_upstream_basins(model=model, node_id=node_id)
        if len(us_basins) == 0:
            return False
        us2_basins = get_first_upstream_basins(model=model, node_id=us_basins[0])
        return all(is_controlled_basin(model=model, node_id=i) for i in us2_basins)

    def downstream_basin_is_controlled(node_id: int, model: Model) -> bool:
        ds_basins = get_first_downstream_basins(model=model, node_id=node_id)
        if len(ds_basins) == 0:
            return False
        return is_controlled_basin(model=model, node_id=int(ds_basins[0]))

    pumps_ds_basins_controlled = model.pump.node.df.apply(
        (lambda x: downstream_basin_is_controlled(node_id=x.name, model=model)), axis=1
    )
    outlets_ds_basins_controlled = model.outlet.node.df.apply(
        (lambda x: downstream_basin_is_controlled(node_id=x.name, model=model)), axis=1
    )
    pumps_us_basins_controlled = model.pump.node.df.apply(
        (lambda x: has_all_upstream_controlled_basins(node_id=x.name, model=model)), axis=1
    )
    outlets_us_basins_controlled = model.outlet.node.df.apply(
        (lambda x: has_all_upstream_controlled_basins(node_id=x.name, model=model)), axis=1
    )

    original_model = model.model_copy(deep=True)
    update_basin_static(model=model, evaporation_mm_per_day=0.1)

    # ✅ DIT MOET DUS ALLEEN HIER STAAN (in build), niet erbuiten:
    add_from_to_nodes_and_levels(model)

    aanvoergebieden_df = gpd.read_file(aanvoer_path)
    aanvoergebieden_df_dissolved = aanvoergebieden_df.dissolve()

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

    # als je die series later nodig hebt: return ze niet, maar reken ze later opnieuw uit óf sla ze op in meta.
    return model


# =========================
# LOAD / BUILD SWITCH
# =========================
if USE_PREPROCESSED_MODEL:
    pre_exists = cloud_path_exists(ribasim_toml_pre)

    if pre_exists and not FORCE_REBUILD_PREPROCESSED:
        print(f"Preprocessed model bestaat al → preprocess overslaan: {ribasim_toml_pre}")
        model = Model.read(ribasim_toml_pre)
    else:
        print("Preprocessed model wordt (opnieuw) gebouwd...")
        model = Model.read(ribasim_toml)  # altijd opnieuw vanaf base
        model = build_preprocessed_model(model)  # ✅ hier al je preprocess
        model.write(ribasim_toml_pre)
        print(f"Preprocessed model saved: {ribasim_toml_pre}")
        model = Model.read(ribasim_toml_pre)

    print("Loaded preprocessed model.")
else:
    print("USE_PREPROCESSED_MODEL=False → basis model laden")
    model = Model.read(ribasim_toml)

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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1741, "max_flow_rate"] = 24
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1741, "max_downstream_level"] = -0.93

# Oostersluis kan aanvoeren boosterpomp driewegsluis
model.pump.static.df.loc[model.pump.static.df.node_id == 142, "min_upstream_level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 142, "flow_rate"] = 2.5
model.pump.static.df.loc[model.pump.static.df.node_id == 142, "max_downstream_level"] = -1.2

# Gaarkeuken scheepvaartsluis flow op nul
model.pump.static.df.loc[model.pump.static.df.node_id == 1744, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 387, "max_downstream_level"] = -0.67

# Meerweg
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1758, "max_downstream_level"] = -0.89
model.outlet.static.df.loc[model.outlet.static.df.node_id == 723, "max_flow_rate"] = 0

# Aanvoer zuiden missen downstream_level
model.outlet.static.df.loc[model.outlet.static.df.node_id == 363, "max_downstream_level"] = 6.05
model.outlet.static.df.loc[model.outlet.static.df.node_id == 589, "max_downstream_level"] = 6.05

# flow inlaten naar custom
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1750, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 687, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 698, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 699, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1739, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1742, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1751, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1743, "max_flow_rate"] = 0.5

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

# Vooraan!
model.outlet.static.df.loc[model.outlet.static.df.node_id == 377, "max_downstream_level"] = -0.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 505, "max_downstream_level"] = -0.67
model.outlet.static.df.loc[model.outlet.static.df.node_id == 427, "max_downstream_level"] = -0.67
model.outlet.static.df.loc[model.outlet.static.df.node_id == 837, "max_downstream_level"] = -0.55

# Diepswal
model.pump.static.df.loc[model.pump.static.df.node_id == 37, "max_downstream_level"] = 2.7

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
# Jonkervaart aanvoer, heeft geen downstream level meegekregen
model.pump.static.df.loc[model.pump.static.df.node_id == 38, "max_downstream_level"] = 3.13
model.pump.static.df.loc[model.pump.static.df.node_id == 38, "min_upstream_level"] = 2.68

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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 565, "max_downstream_level"] = 0.7
model.pump.static.df.loc[model.pump.static.df.node_id == 36, "min_upstream_level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 36, "max_downstream_level"] = 0.7

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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "flow_rate"] = 15
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1754, "max_flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1755, "max_flow_rate"] = 0

model.outlet.static.df.loc[model.outlet.static.df.node_id == 1747, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "min_upstream_level"] = -0.93
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1754, "min_upstream_level"] = -0.93
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1755, "min_upstream_level"] = -0.93
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1747, "min_upstream_level"] = -0.93
model.outlet.static.df.loc[model.outlet.static.df.node_id == 724, "min_upstream_level"] = -0.93
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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 564, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 389, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 398, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 455, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 565, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 519, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 605, "min_upstream_level"] += 0.02
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

exclude_ids = {1745, 1746, 1740, 1756, 1744, 1738, 716, 683, 725}  # scheepvaartsluizen moeten op flow_rate=0
df = model.outlet.static.df
mask = df["node_id"].isin(exclude_ids)
df.loc[mask, "flow_rate"] = 0.0

# %%
# Rondpompen voorkomen bij 2 aanvoer en afvoer gemaal direct naast elkaar, min_upstream en max_downstream gelijk maken
# === INLINE: max_downstream_level(iKGM/iKST-pumps) = min_upstream_level(KGM/KST peer: pump óf outlet) ===
print("=== Corrigeren iKGM/KGM_i & iKST/KST_i-pompen (rondpompen voorkomen) ===")


# --- Helper om waarden iets te verschuiven ---
def bump(v, delta):
    """Verhoog/verlaag scalar of array met delta; NaN blijft NaN."""
    try:
        if isinstance(v, (list, tuple, np.ndarray)):
            arr = pd.to_numeric(np.asarray(v), errors="coerce")
            arr = np.where(np.isnan(arr), arr, arr + float(delta))
            return arr.tolist()
        x = float(v)
        return x + float(delta) if not np.isnan(x) else v
    except Exception:
        return v


# --- Dataframes ---
pump_static_df = model.pump.static.df
outlet_static_df = model.outlet.static.df

# --- Kolommen bepalen ---
code_col_pump = "meta_code_waterbeheerder" if "meta_code_waterbeheerder" in pump_static_df.columns else "meta_code"
code_col_outlet = "meta_code_waterbeheerder" if "meta_code_waterbeheerder" in outlet_static_df.columns else "meta_code"

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


# %%


def add_controller(
    model,
    node_ids,
    *,
    # --- luister-nodes ---
    listen_node_ids=None,
    listen_node_id: int = 1493,
    weights=None,
    # --- thresholds ---
    threshold_high: float = 7.68,
    threshold_low: float = 7.678,
    threshold_delta: float | None = None,
    state_labels=("aanvoer", "afvoer"),
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
):
    """
    Voeg discrete controllers toe op pumps/outlets met uitgebreide overschrijfmogelijkheden.

    Belangrijk:
    - max_ds_* : als je een numerieke waarde opgeeft, overschrijft die nu altijd het origineel.
    - delta_max_ds_* : wordt opgeteld bij de (opgegeven of originele) waarde.
    - min_upstream_* : je kunt absolute overrides geven of None laten en keep_min_us gebruiken.
    - delta_us_* : optionele offsets die bovenop de basiswaarde worden opgeteld (als basis numeriek).
    """
    out_df = model.outlet.static.df
    pump_df = model.pump.static.df

    outlet_ids = set(out_df["node_id"].astype(int))
    pump_ids = set(pump_df["node_id"].astype(int))

    # hysterese
    threshold_low_used = threshold_high - float(threshold_delta) if threshold_delta is not None else threshold_low

    def _resolve_max_ds(mode, base):
        """Return een numerieke waarde of base of NaN afhankelijk van mode."""
        if mode is None:
            return float("nan")
        if isinstance(mode, (int, float)):
            return float(mode)  # overschrijft altijd
        if mode == "existing":
            return base
        if mode == "nan":
            return float("nan")
        return base

    def _find_max_flow_col(row):
        for col in ("max_flow_rate", "max_flow", "max_discharge"):
            if col in row.index:
                return col
        return None

    def _num(v):
        return float(v) if isinstance(v, (int, float)) else None

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

    # listen nodes
    if listen_node_ids is not None:
        nodes_to_listen = listen_node_ids if isinstance(listen_node_ids, (list, tuple)) else [listen_node_ids]
    else:
        nodes_to_listen = [listen_node_id]

    # weights
    if weights is None:
        weights = [1.0] * len(nodes_to_listen)
    elif isinstance(weights, (int, float)):
        weights = [float(weights)] * len(nodes_to_listen)

    if len(weights) != len(nodes_to_listen):
        raise ValueError(f"Gewichtslijst verkeerde lengte: {len(weights)} vs {len(nodes_to_listen)} listen_nodes")

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
        # let op: sommige velden kunnen NaN zijn in de tabel
        flow_orig = float(row["flow_rate"]) if pd.notna(row.get("flow_rate")) else float("nan")
        max_ds_orig = float(row["max_downstream_level"]) if pd.notna(row.get("max_downstream_level")) else float("nan")
        min_us_orig = float(row["min_upstream_level"]) if pd.notna(row.get("min_upstream_level")) else float("nan")

        # flows per state
        if node_type == "Outlet":
            flow0 = flow_orig if flow_aanvoer_outlet == "orig" else float(flow_aanvoer_outlet)
            flow1 = flow_orig if flow_afvoer_outlet == "orig" else float(flow_afvoer_outlet)
            mf_mode0, mf_mode1 = max_flow_aanvoer_outlet, max_flow_afvoer_outlet
        else:
            flow0 = flow_orig if flow_aanvoer_pump == "orig" else float(flow_aanvoer_pump)
            flow1 = flow_orig if flow_afvoer_pump == "orig" else float(flow_afvoer_pump)
            mf_mode0, mf_mode1 = max_flow_aanvoer_pump, max_flow_afvoer_pump

        # downstream (max_downstream_level) — resolven en optioneel delta optellen
        ds0 = _resolve_max_ds(max_ds_aanvoer, max_ds_orig)
        ds1 = _resolve_max_ds(max_ds_afvoer, max_ds_orig)

        if delta_max_ds_aanvoer is not None:
            try:
                ds0 = float(ds0) + float(delta_max_ds_aanvoer)
            except Exception:
                raise ValueError("delta_max_ds_aanvoer moet numeriek of None zijn")
        if delta_max_ds_afvoer is not None:
            try:
                ds1 = float(ds1) + float(delta_max_ds_afvoer)
            except Exception:
                raise ValueError("delta_max_ds_afvoer moet numeriek of None zijn")

        # upstream (min_upstream_level) — basis + delta_us_*
        def _base_upstream(override, orig):
            """Bepaal basiswaarde: override -> origineel -> NaN"""
            if override is not None:
                return float(override)
            if keep_min_us and orig is not None and not np.isnan(orig):
                return float(orig)
            return float("nan")

        base0 = _base_upstream(min_upstream_aanvoer, min_us_orig)
        base1 = _base_upstream(min_upstream_afvoer, min_us_orig)

        if delta_us_aanvoer is not None:
            try:
                d0 = float(delta_us_aanvoer)
                if not np.isnan(base0):
                    base0 = float(base0) + d0
                else:
                    # keuze: als er geen basis is en je toch delta wilt toepassen,
                    # kun je hier base0 = d0 zetten. Voor nu laten we het NaN.
                    pass
            except Exception:
                raise ValueError("delta_us_aanvoer moet numeriek of None zijn")

        if delta_us_afvoer is not None:
            try:
                d1 = float(delta_us_afvoer)
                if not np.isnan(base1):
                    base1 = float(base1) + d1
                else:
                    pass
            except Exception:
                raise ValueError("delta_us_afvoer moet numeriek of None zijn")

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

        # --- max_flow kolom: detectie & overschrijven indien nodig ---
        max_flow_col = _find_max_flow_col(row)
        if max_flow_col is not None:
            cur0, cur1 = _split2(row.get(max_flow_col))

            v0 = _num(mf_mode0)  # numeriek of None (=orig)
            v1 = _num(mf_mode1)

            # "orig" betekent: gebruik cur0/cur1 (ook als NaN)
            out0 = cur0 if v0 is None else v0
            out1 = cur1 if v1 is None else v1

            static_kwargs[max_flow_col] = [float(out0), float(out1)]

        # update node in model
        if node_type == "Outlet":
            model.update_node(nid, "Outlet", [outlet.Static(**static_kwargs)])
        else:
            model.update_node(nid, "Pump", [pump.Static(**static_kwargs)])

        # discrete control toevoegen
        truth = ["F", "T"]
        ctrl = [state_labels[0], state_labels[1]]
        dc_node = Node(geometry=Point(geom.x + dc_offset, geom.y))

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
        }
        if threshold_low_used is not None:
            cond_kwargs["threshold_low"] = [float(threshold_low_used)]

        dc = model.discrete_control.add(
            dc_node,
            variable_blocks
            + [
                discrete_control.Condition(**cond_kwargs),
                discrete_control.Logic(truth_state=truth, control_state=ctrl),
            ],
        )

        model.link.add(dc, parent)
        print(f"[OK] controller toegevoegd aan {node_type} {nid}")


# %%


# %% Marnerwaard

# aanvoer
selected_node_ids = [117, 129, 110, 107, 108, 123, 121, 701, 715]
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
)

# === afvoerpumps/oulets ===
selected_node_ids = [535, 536, 534, 609, 533]

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
)


# %%# === Aanvoergemalen/aanvoerpumps in het midNoorden ===
selected_node_ids = [48, 165, 32, 146, 711, 721, 169, 35]

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
)


# === afvoerpumps/oulets ===
selected_node_ids = [47, 385, 538, 723, 145, 147, 31, 34, 168, 181, 182, 342]

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
    flow_aanvoer_outlet=10,  # open bij laagwater → laat water in
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
    delta_us_aanvoer=-0.02,
)

# %%
# Lauwersmeer
# === Aanvoergemalen/aanvoerpumps Grote pompen en outlets===
add_controller(
    model=model,
    node_ids=[29, 30],
    listen_node_id=1124,
    # thresholds
    threshold_high=-0.93,
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
    listen_node_id=1124,
    # thresholds
    threshold_high=-0.93,
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
    listen_node_id=1124,
    # thresholds
    threshold_high=-0.93,
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

# aanvoer
selected_node_ids = [708]
add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1124,
    # thresholds
    threshold_high=-0.93,
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
)
# === afvoerpumps/oulets ===
selected_node_ids = [42, 43]

add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1124,
    # thresholds
    threshold_high=-0.93,
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
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1124,
    # thresholds
    threshold_high=-0.93,
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
)

target_nodes = [412, 720]
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1124,
    # thresholds
    threshold_high=-0.93,
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
    min_upstream_afvoer=-0.9,
)


# %%
# === Dwarsdiep,
# listen_node_id: max ds_aanvoer niet verhogen!

# Model moet in afvoer modus starten
# model.basin.state.df.loc[model.basin.state.df.node_id == 1132, "level"] = 3.3
add_controller(
    model=model,
    node_ids=[38],
    listen_node_id=1132,
    # thresholds
    threshold_high=3.13,
    threshold_delta=0.025,
    # flows:
    flow_aanvoer_outlet=20,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    max_flow_afvoer_outlet=0,
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    max_flow_afvoer_pump=0,
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.0,
    max_ds_afvoer=9999,
    keep_min_us=True,
)


# afvoerpumps/oulets aanvoer dicht, afvoer open, ===
add_controller(
    model=model,
    node_ids=[350, 537, 383, 618, 681, 357, 401, 501, 519, 455, 565, 652, 389, 564, 551],
    listen_node_id=1132,
    # thresholds
    threshold_high=3.13,
    threshold_delta=0.025,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=100.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    max_flow_aanvoer_pump=0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer=-1000,  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # controllers werkt niet goed met NAN waarden daarom 1000
    delta_max_ds_aanvoer=0.0,  # geen extra
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)


# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
add_controller(
    model=model,
    node_ids=[687, 698, 699, 36, 37],
    listen_node_id=1132,
    # thresholds
    threshold_high=3.13,
    threshold_delta=0.025,
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
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[368],
    listen_node_id=1132,
    # thresholds
    threshold_high=3.13,
    threshold_delta=0.025,
    # --- flows ---
    flow_aanvoer_outlet=20,
    flow_afvoer_outlet=100,
    flow_afvoer_pump="orig",
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=1000,  # voorkomt NaN
    keep_min_us=True,
)

# %%
# === Peizerdiep,
# listen_node_id: max ds_aanvoer niet verhogen!

# Model moet in afvoer modus starten
# model.basin.state.df.loc[model.basin.state.df.node_id == 1215, "level"] = 0

# Deze moeten verlaagde max_downstream krijgen om niet teveel uit te zakken bij listen_node_id
# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[327, 446, 736, 719, 644, 589, 363],
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.05,
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

target_nodes = [
    322,
    567,
    494,
]
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.05,
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

# afvoerpumps/oulets aanvoer dicht, afvoer open, ===
add_controller(
    model=model,
    node_ids=[737, 738, 739, 740, 741, 560, 44],
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.05,
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
add_controller(
    model=model,
    node_ids=[45, 1739, 1742, 1751, 1758],
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.05,
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
    delta_max_ds_aanvoer=0.04,  # extra aanvoer ivm Manning 1743
    max_ds_afvoer=9999,
    keep_min_us=True,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
# extra aanvoer voor 1743 door max_downstream +0.04 ivm Manning 1743
add_controller(
    model=model,
    node_ids=[1743],
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.05,
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
add_controller(
    model=model,
    node_ids=[575, 606, 435, 499, 748, 326, 568, 569, 563, 570, 571, 572],
    listen_node_id=1215,
    # thresholds
    threshold_high=-0.81,
    threshold_delta=0.05,
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


# %%
# == Noordpolder,

# afvoerpumps/oulets aanvoer dicht, afvoer open, ===
add_controller(
    model=model,
    node_ids=[521, 524, 431, 417, 487, 40, 546, 545],
    listen_node_id=1280,
    # thresholds
    threshold_high=-0.36,
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
add_controller(
    model=model,
    node_ids=[116, 115, 691, 112, 118, 119, 125, 122],
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
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[678],
    listen_node_id=1172,
    # thresholds
    threshold_high=-1.26,
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

# %% === Spijksterpompen

# Model moet in afvoer modus starten
model.basin.state.df.loc[model.basin.state.df.node_id == 1408, "level"] = -0.6
# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[377, 427, 837, 505, 485, 387],
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
)

# afvoer
add_controller(
    model=model,
    node_ids=[423, 731, 462, 371, 428, 396, 398, 466, 336],
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
add_controller(
    model=model,
    node_ids=[41],
    listen_node_id=1182,
    # thresholds
    threshold_high=-0.69,
    threshold_delta=0.001,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    max_flow_aanvoer_outlet=0.0,
    flow_afvoer_outlet=75.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    # max_flow_aanvoer_pump=0.0,
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # controllers werkt niet goed met NAN waarden daarom 1000
    delta_max_ds_afvoer=None,
    keep_min_us=True,
    min_upstream_afvoer=-0.69,
    min_upstream_aanvoer=-0.69,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
add_controller(
    model=model,
    node_ids=[
        184,
        943,
        106,
        111,
        114,
        120,
    ],
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
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
add_controller(
    model=model,
    node_ids=[136, 139, 109, 390],
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
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[351],
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
)

# %%
# === Fivelingo,afvoerpumps/oulets aanvoer dicht, afvoer open, ===

# Model moet in afvoer modus starten
# model.basin.state.df.loc[model.basin.state.df.node_id == 1172, "level"] = 0

# afvoer
add_controller(
    model=model,
    node_ids=[732, 67, 472],
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
add_controller(
    model=model,
    node_ids=[142, 143, 702, 703, 693],
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
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[539, 124],
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
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[508, 509, 646],
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
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[517],
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
)


# %%
def add_doorspoeling(model, node_id: int, flow: float):
    # target node
    if node_id in model.pump.node.df.index:
        target = model.pump[node_id]
    elif node_id in model.outlet.node.df.index:
        target = model.outlet[node_id]
    else:
        raise ValueError(f"Node {node_id} is geen pump of outlet.")

    # dagelijkse tijdserie
    t = pd.date_range(model.starttime, model.endtime, freq="D")
    d = np.zeros(len(t))

    # periode: 1 maart t/m 31 augustus
    maanden = t.month
    dagen = t.day

    # juni -> augustus
    mask = (maanden == 6) | (maanden == 7) | (maanden == 8)

    d[mask] = float(flow)

    # flow_demand node maken
    alloc = model.flow_demand.add(
        Node(geometry=Point(target.geometry.x + 12, target.geometry.y), name=f"doorspoeling_{node_id}"),
        [
            flow_demand.Time(
                time=t,
                demand_priority=[1] * len(t),
                demand=d,
            )
        ],
    )

    # koppelen aan pomp/outlet
    model.link.add(alloc, target)

    print(f"[OK] Doorspoeling {flow} m³/s toegevoegd aan node {node_id} (1 maart t/m 31 aug)")

    return alloc


# for nid in [
#    41,
# ]:
#    add_doorspoeling(model, nid, 0.5)


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
        raise ValueError(f"Node {node_id} is geen Basin – level_demand moet op een Basin worden gezet.")

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

# Jonkersvaart
model.outlet.static.df.loc[model.outlet.static.df.node_id == 519, "min_upstream_level"] = 3.13

# Diepswal
model.outlet.static.df.loc[model.outlet.static.df.node_id == 455, "min_upstream_level"] = 2.72
model.outlet.static.df.loc[model.outlet.static.df.node_id == 565, "min_upstream_level"] = 2.72
model.pump.static.df.loc[model.pump.static.df.node_id == 37, "min_upstream_level"] = 0.7

# Leek rondpompen voorkomen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 564, "min_upstream_level"] = 0.72
model.outlet.static.df.loc[model.outlet.static.df.node_id == 389, "min_upstream_level"] = 0.72

# Den Deel rondpompen voorkomen -4cm upstream!
model.pump.static.df.loc[model.pump.static.df.node_id == 35, "min_upstream_level"] = -1.11
model.pump.static.df.loc[model.pump.static.df.node_id == 35, "max_downstream_level"] = -1.14
model.pump.static.df.loc[model.pump.static.df.node_id == 34, "min_upstream_level"] = -1.14

# Usquert rondpompen voorkomen basin levell klopt niet?? moet -1.07 zijn? -4cm upstream!
model.pump.static.df.loc[model.pump.static.df.node_id == 169, "max_downstream_level"] = -1.14
model.pump.static.df.loc[model.pump.static.df.node_id == 168, "min_upstream_level"] = -1.14
model.pump.static.df.loc[model.pump.static.df.node_id == 169, "min_upstream_level"] = -1.11
model.pump.static.df.loc[model.pump.static.df.node_id == 730, "flow_rate"] = 0

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
model.pump.static.df.loc[model.pump.static.df.node_id == 124, "min_upstream_level"] = -0.36
model.outlet.static.df.loc[model.outlet.static.df.node_id == 336, "min_upstream_level"] = -0.39

model.pump.static.df.loc[model.pump.static.df.node_id == 114, "min_upstream_level"] = -0.36

model.pump.static.df.loc[model.pump.static.df.node_id == 81, "min_upstream_level"] = -1.18

# Quatre Bras downstream level geven
model.pump.static.df.loc[model.pump.static.df.node_id == 120, "max_downstream_level"] = -0.19
model.pump.static.df.loc[model.pump.static.df.node_id == 120, "min_upstream_level"] = -0.36

# Katershorn downstream level geven
model.pump.static.df.loc[model.pump.static.df.node_id == 111, "max_downstream_level"] = -0.45
model.pump.static.df.loc[model.pump.static.df.node_id == 111, "min_upstream_level"] = -0.34
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

# %%
# warm start (prerun) en hoofd run met aparte forcings

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=0.5)

model.solver.level_difference_threshold = 0.02
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

# %%

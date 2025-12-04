# %%

import geopandas as gpd
import numpy as np
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim import Node
from ribasim.nodes import discrete_control, outlet, pump
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import Point

from peilbeheerst_model import ribasim_parametrization
from ribasim_nl import CloudStorage, Model, check_basin_level

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
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(
    AUTHORITY, "verwerkt/1_ontvangen_data//20250527/gebieden_met_wateraanvoermogelijkheid_noorderzijlvest.gpkg"
)

cloud.synchronize(filepaths=[aanvoer_path, qlr_path])

# read model
model = Model.read(ribasim_toml)


def get_first_upstream_basins(model: Model, node_id: int) -> np.ndarray:
    """Get first upstream basins of a node"""
    us_basins = model.get_upstream_basins(node_id=node_id, stop_at_node_type="Basin")
    return us_basins[us_basins.node_id != node_id].node_id.to_numpy()


def get_first_downstream_basins(model: Model, node_id: int) -> np.ndarray:
    """Get the first downstream basins of a node"""
    ds_basins = model.get_downstream_basins(node_id=node_id, stop_at_node_type="Basin")
    return ds_basins[ds_basins.node_id != node_id].node_id.to_numpy()


def is_controlled_basin(model: Model, node_id: int) -> bool:
    """node_id is Basin (!). Check if is controlled (no ManningResistance or LinearResistance)"""
    ds_node_ids = model._downstream_nodes(node_id=node_id, stop_at_node_type="Basin")
    return (
        not model.node_table().df.loc[list(ds_node_ids)].node_type.isin(["ManningResistance", "LinearResistance"]).any()
    )


def has_all_upstream_controlled_basins(node_id: int, model: Model) -> bool:
    """Find upstream basin of pump or outlet. So node_id should refer to connector-nodes only (!)"""
    us_basins = get_first_upstream_basins(model=model, node_id=node_id)
    if len(us_basins) == 0:  # No basins, so level boundary
        return False

    # get all upstream basins
    us2_basins = get_first_upstream_basins(model=model, node_id=us_basins[0])

    # check if all upstream basins are controlled
    all_controlled = all(is_controlled_basin(model=model, node_id=i) for i in us2_basins)

    # return True if no ManningResistance or LinearResistance nodes have been found
    return all_controlled


def downstream_basin_is_controlled(node_id: int, model=Model) -> bool:
    """Find if downstream basins are controlled by Pump or Outlet. So node_id should refer to connector-nodes only (!)"""
    ds_basins = get_first_downstream_basins(model=model, node_id=node_id)
    if len(ds_basins) == 0:  # No basins, so level boundary
        return False
    else:  # downstream basin shouldn't have any Manning or Linear Resistance (so controlled by Pump(s) or Outlet(s))
        return is_controlled_basin(model=model, node_id=ds_basins[0])


# @ngoorden deze series kun je gebruiken om pompen en outlets te masken verdrop in het script
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
update_basin_static(model=model, precipitation_mm_per_day=0.5)


# %%
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
model.level_boundary.static.df.loc[mask, "level"] += 0.2

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

model.pump.static.df.loc[
    model.pump.static.df.node_id == 40, "min_upstream_level"
] = -0.36  # Bij min_upstream_level =-0.35m NP geen afvoer mogelijk.

# Oostersluis kan aanvoeren boosterpomp driewegsluis
model.pump.static.df.loc[model.pump.static.df.node_id == 142, "flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 142, "max_downstream_level"] = -1.2

# Gaarkeuken scheepvaartsluis flow op nul
model.pump.static.df.loc[model.pump.static.df.node_id == 1744, "flow_rate"] = 0

# Min_upstream_level Oudendijk pump omlaag anders voert die alles af en gaat niks via Abelstok, streefpeil moet -1.07m zijn
model.pump.static.df.loc[model.pump.static.df.node_id == 118, "min_upstream_level"] = -1.07
model.outlet.static.df.loc[model.outlet.static.df.node_id == 487, "max_downstream_level"] = -1.07
model.outlet.static.df.loc[model.outlet.static.df.node_id == 729, "min_upstream_level"] = -1.07

# Meerweg
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1758, "max_downstream_level"] = -0.89

# flow inlaten naar custom
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1750, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 687, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 698, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 699, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1739, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1739, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1742, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1751, "flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1743, "flow_rate"] = 0.5

# Manning downstream niet sturen op max_downstream_level
model.outlet.static.df.loc[model.outlet.static.df.node_id == 379, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 380, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 573, "max_downstream_level"] = pd.NA
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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 557, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 558, "max_downstream_level"] = pd.NA


# kortsluitingen oplossen noordwesten tov Spijksterpompen
model.update_node(node_id=837, node_type="Outlet")
model.outlet.static.df.loc[model.outlet.static.df.node_id == 387, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 485, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 837, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 837, "min_upstream_level"] = -0.59
model.outlet.static.df.loc[model.outlet.static.df.node_id == 505, "max_downstream_level"] = -0.59
model.outlet.static.df.loc[model.outlet.static.df.node_id == 427, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 428, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 396, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 371, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 462, "max_downstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 423, "max_downstream_level"] = -0.69

# Kleine aanpassingen handmatig
model.outlet.static.df.loc[model.outlet.static.df.node_id == 379, "max_downstream_level"] = -0.56
model.outlet.static.df.loc[model.outlet.static.df.node_id == 568, "max_downstream_level"] = 4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 571, "max_downstream_level"] = 4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 483, "max_downstream_level"] = 8.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 659, "min_upstream_level"] = 8.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 397, "min_upstream_level"] = 8.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 648, "max_downstream_level"] += 0.04
model.outlet.static.df.loc[model.outlet.static.df.node_id == 619, "min_upstream_level"] = -1.07
model.outlet.static.df.loc[model.outlet.static.df.node_id == 704, "max_downstream_level"] += 0.06
model.outlet.static.df.loc[model.outlet.static.df.node_id == 696, "max_downstream_level"] += 0.06
model.outlet.static.df.loc[model.outlet.static.df.node_id == 630, "max_downstream_level"] = -0.22
model.outlet.static.df.loc[model.outlet.static.df.node_id == 321, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 469, "max_downstream_level"] = 8.3

# Robbegat in aanvoer situatie streefpeil
model.outlet.static.df.loc[model.outlet.static.df.node_id == 412, "max_downstream_level"] = -2.65
model.outlet.static.df.loc[model.outlet.static.df.node_id == 442, "max_downstream_level"] = -2.65
model.outlet.static.df.loc[model.outlet.static.df.node_id == 635, "max_downstream_level"] = -2.65
model.outlet.static.df.loc[model.outlet.static.df.node_id == 566, "max_downstream_level"] = -2.65
model.outlet.static.df.loc[model.outlet.static.df.node_id == 645, "max_downstream_level"] = -2.65


# Diepswal
model.pump.static.df.loc[model.pump.static.df.node_id == 37, "max_downstream_level"] = 2.72

# Driewegsluis max_downstream verhogen, Manning knopen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1750, "max_downstream_level"] = -1.24

# Controllers komen uit op manning waterloop
model.outlet.static.df.loc[model.outlet.static.df.node_id == 539, "max_downstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 509, "max_downstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 646, "max_downstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 508, "max_downstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 517, "max_downstream_level"] = -1.26
# Aanvoergemaal Klei
model.outlet.static.df.loc[model.outlet.static.df.node_id == 507, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 129, "max_downstream_level"] = -0.75

# HD Louwes
model.outlet.static.df.loc[model.outlet.static.df.node_id == 479, "max_downstream_level"] = 8.75
model.pump.static.df.loc[model.pump.static.df.node_id == 39, "max_downstream_level"] = -0.37
model.pump.static.df.loc[model.pump.static.df.node_id == 186, "max_downstream_level"] = pd.NA
# Jonkervaart aanvoer
model.pump.static.df.loc[model.pump.static.df.node_id == 38, "max_downstream_level"] = 3.11
model.pump.static.df.loc[model.pump.static.df.node_id == 38, "min_upstream_level"] = 2.68

# Aanvergemaal Onrust downstream_level
model.outlet.static.df.loc[model.outlet.static.df.node_id == 117, "max_downstream_level"] = -0.73

# Aanvoergemaal Blijcke
model.pump.static.df.loc[model.pump.static.df.node_id == 106, "max_downstream_level"] = -0.49

# Aanvoergemaal Oosternieland
model.pump.static.df.loc[model.pump.static.df.node_id == 143, "max_downstream_level"] = -1.18


# model.outlet.static.df.loc[model.outlet.static.df.node_id == 691, "min_upstream_level"] = -0.34

# Outets omlaag anders laten ze geen water door
model.outlet.static.df.loc[model.outlet.static.df.node_id == 351, "min_upstream_level"] = -0.41
model.outlet.static.df.loc[model.outlet.static.df.node_id == 466, "min_upstream_level"] = -0.41

# inlaat Lauwersmeer
model.outlet.static.df.loc[model.outlet.static.df.node_id == 714, "max_downstream_level"] = -2.02

#
model.pump.static.df.loc[model.pump.static.df.node_id == 116, "max_downstream_level"] = -0.2

#
model.pump.static.df.loc[model.pump.static.df.node_id == 115, "max_downstream_level"] = -0.26
# Onrust
model.pump.static.df.loc[model.pump.static.df.node_id == 117, "max_downstream_level"] = -0.73

# Kluten
model.pump.static.df.loc[model.pump.static.df.node_id == 112, "max_downstream_level"] = 0.37

# Slikken
model.pump.static.df.loc[model.pump.static.df.node_id == 121, "max_downstream_level"] = -0.28
#
model.pump.static.df.loc[model.pump.static.df.node_id == 122, "max_downstream_level"] = -0.33

# Buiten aanvoer gemaal flow_rate omhoog; bespreken met NZV
model.pump.static.df.loc[model.pump.static.df.node_id == 184, "max_downstream_level"] = -0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 184, "min_upstream_level"] -= 0.02

# Aanvoergemaal moet downstream level krijgen
model.pump.static.df.loc[model.pump.static.df.node_id == 119, "max_downstream_level"] = -0.75

# Oudendijk aanvoer gemaal flow_rate omhoog; bespreken met NZV
model.pump.static.df.loc[model.pump.static.df.node_id == 118, "max_downstream_level"] = -0.23
model.pump.static.df.loc[model.pump.static.df.node_id == 118, "min_upstream_level"] -= 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 118, "min_upstream_level"] = pd.NA

# Pump Grote Hadder
model.pump.static.df.loc[model.pump.static.df.node_id == 109, "max_downstream_level"] = -0.34
# Pump Bokumerklief
model.pump.static.df.loc[model.pump.static.df.node_id == 107, "max_downstream_level"] = -0.73
# Ziekolk
model.pump.static.df.loc[model.pump.static.df.node_id == 124, "max_downstream_level"] = -0.37

# Quatre Bras downstream level geven
model.pump.static.df.loc[model.pump.static.df.node_id == 120, "max_downstream_level"] = -0.17

# Quatre Katershorn downstream level geven
model.pump.static.df.loc[model.pump.static.df.node_id == 111, "max_downstream_level"] = -0.45

# Den Deel aanvoergemaal
model.pump.static.df.loc[model.pump.static.df.node_id == 35, "min_upstream_level"] = -1.07
model.pump.static.df.loc[model.pump.static.df.node_id == 35, "max_downstream_level"] = -1.16

# Waterwolf spuisluizen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 728, "flow_rate"] = 9999
model.outlet.static.df.loc[model.outlet.static.df.node_id == 728, "min_upstream_level"] = -0.93
model.pump.static.df.loc[model.pump.static.df.node_id == 29, "min_upstream_level"] = -0.93

# Drie Delfzijlen
model.pump.static.df.loc[model.pump.static.df.node_id == 67, "min_upstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 732, "min_upstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 732, "flow_rate"] = 50

# Spijkerpompen omhoog anders rondpompen in aanvoersituatie
model.pump.static.df.loc[model.pump.static.df.node_id == 41, "min_upstream_level"] = -0.69
model.outlet.static.df.loc[model.outlet.static.df.node_id == 731, "min_upstream_level"] = -0.69

# Zwarte Pier
model.pump.static.df.loc[model.pump.static.df.node_id == 125, "max_downstream_level"] = -0.46

# Wierhuizerklief
model.pump.static.df.loc[model.pump.static.df.node_id == 123, "max_downstream_level"] = -0.73

#
model.outlet.static.df.loc[model.outlet.static.df.node_id == 678, "max_downstream_level"] = -0.3

# inlaten naast pomp Rondpompen voorkomen, gelijk zetten aan min_upstream_level pomp
model.outlet.static.df.loc[model.outlet.static.df.node_id == 703, "max_downstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 702, "max_downstream_level"] = -1.26
model.outlet.static.df.loc[model.outlet.static.df.node_id == 721, "min_upstream_level"] = -0.93

# Leek 2 inlaten naast pomp: rondpompen voorkomen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 565, "max_downstream_level"] = 0.7
model.pump.static.df.loc[model.pump.static.df.node_id == 36, "min_upstream_level"] = -0.95
model.pump.static.df.loc[model.pump.static.df.node_id == 36, "max_downstream_level"] = 0.7

# Pomp en inlaat naast elkaar: rondpompen voorkomen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 680, "max_downstream_level"] = -1.22
model.outlet.static.df.loc[model.outlet.static.df.node_id == 724, "flow_rate"] = 400

# Lage Waard
model.pump.static.df.loc[model.pump.static.df.node_id == 114, "max_downstream_level"] = -0.45

# Outlets Lauwersmeer aanpassen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1754, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1755, "max_downstream_level"] = pd.NA

model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1754, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1755, "flow_rate"] = 0
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

# Usquert inlaat??? Peil verkeerd basin
model.pump.static.df.loc[model.pump.static.df.node_id == 169, "min_upstream_level"] = -1.07
model.pump.static.df.loc[model.pump.static.df.node_id == 169, "max_downstream_level"] = -1.14

# Stad en Lande inlaat
model.pump.static.df.loc[model.pump.static.df.node_id == 32, "min_upstream_level"] = -0.95
model.pump.static.df.loc[model.pump.static.df.node_id == 32, "max_downstream_level"] = -1

# Schaphalsterzijl
model.pump.static.df.loc[model.pump.static.df.node_id == 146, "min_upstream_level"] = -0.95
model.pump.static.df.loc[model.pump.static.df.node_id == 146, "max_downstream_level"] = -1

model.outlet.static.df.loc[model.outlet.static.df.node_id == 390, "min_upstream_level"] = -0.61

# Nieuwstad Aanvoergemaal
model.pump.static.df.loc[model.pump.static.df.node_id == 136, "max_downstream_level"] = -0.69

# Spijk
model.pump.static.df.loc[model.pump.static.df.node_id == 139, "min_upstream_level"] = -1.32
model.pump.static.df.loc[model.pump.static.df.node_id == 139, "max_downstream_level"] = -0.69

# Gemaal Dokwerd is een inlaat naar Hunze en Aa's
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1752, "flow_rate"] = 0.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1753, "flow_rate"] = 5.0

# Afvoer outlets die naast aanvoergemaal liggen moet min_upstrem gelijk aan max_downstream
model.outlet.static.df.loc[model.outlet.static.df.node_id == 545, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 521, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 564, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 389, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 398, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 455, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 565, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 519, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 605, "min_upstream_level"] += 0.02
model.outlet.static.df.loc[model.outlet.static.df.node_id == 165, "max_downstream_level"] = 0.25

exclude_ids = {1745, 1746, 1740, 1756, 1738, 716, 683}  # scheepvaartsluizen moeten op flow_rate=0
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
    if isinstance(v, (list, tuple, np.ndarray)):
        arr = pd.to_numeric(np.asarray(v), errors="coerce")
        arr = np.where(np.isnan(arr), arr, arr + float(delta))
        return arr.tolist()
    x = float(v)
    return x + float(delta) if not np.isnan(x) else v


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
# Discrete control toevoegen aan alle upstream outlets met aanvoer
def build_discrete_controls(
    model,
    out_static: pd.DataFrame,
    mask_upstream_aanvoer: pd.Series,
    exclude_ids=None,
    listen_node_id: int = 1132,
    # band=(2, 3.15),
    band=(7.622, 7.68),
    flow_open_default: float = 20.0,
    delta_h: float = 0.05,
    dc_offset: float = 10.0,  # x-offset voor DC-node
):
    # normaliseer exclude_ids naar set[int]
    exclude = set(map(int, exclude_ids or []))

    # kandidaat-outlets (als ints) en uitsluiters eruit
    upstream_outlet_ids = out_static.loc[mask_upstream_aanvoer, "node_id"].to_numpy(dtype=int).flatten()
    if exclude:
        upstream_outlet_ids = upstream_outlet_ids[~np.isin(upstream_outlet_ids, list(exclude))].astype(int).flatten()

    th_high = band[1]

    def _flow_open_from_value(val, default=flow_open_default):
        """Bepaal open-flow uit scalar of lijst; kies grootste niet-NaN, >0. Anders default."""
        if isinstance(val, list | tuple | np.ndarray):
            arr = pd.to_numeric(np.asarray(val), errors="coerce")
            if arr.size == 0 or np.all(np.isnan(arr)):
                return float(default)
            # neem max van niet-NaN waarden
            cand = np.nanmax(arr)
            return float(cand) if np.isfinite(cand) and cand > 0 else float(default)
        try:
            x = float(val)
            return float(x) if np.isfinite(x) and x > 0 else float(default)
        except Exception:
            return float(default)

    for outlet_id in upstream_outlet_ids:
        # safety
        if outlet_id in exclude:
            continue

        # h = huidige max_downstream_level
        h_vals = out_static.loc[out_static["node_id"] == outlet_id, "max_downstream_level"].to_numpy()
        if len(h_vals) != 1 or pd.isna(h_vals[0]):
            # geen geldige h → overslaan
            continue
        h = float(h_vals[0])

        # open-flow uit bestaande flow_rate (of default)
        fr_series = out_static.loc[out_static["node_id"] == outlet_id, "flow_rate"]
        flow_open = _flow_open_from_value(fr_series.iloc[0]) if not fr_series.empty else float(flow_open_default)

        # 1) outlet: 2 states met variabele max_downstream_level [h, h + delta_h]
        model.update_node(
            node_id=outlet_id,
            node_type="Outlet",
            data=[
                outlet.Static(
                    control_state=["closed", "open"],
                    flow_rate=[0.0, flow_open],
                    max_downstream_level=[h, h + delta_h],
                )
            ],
        )

        # 2) discrete control: luisteren naar listen_node_id met vaste band
        geom = model.outlet[outlet_id].geometry
        x0, y0 = geom.x, geom.y

        dc = model.discrete_control.add(
            Node(geometry=Point(x0 + dc_offset, y0)),
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=int(listen_node_id),
                    variable=["level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=1,
                    condition_id=[1],
                    threshold_high=[float(th_high)],  # True  als > th_high
                ),
                discrete_control.Logic(
                    truth_state=["T", "F"],
                    control_state=["closed", "open"],
                ),
            ],
        )

        # 3) koppel DC aan de outlet
        model.link.add(dc, model.outlet[outlet_id])


exclude_ids = {
    1744,
    1745,
    1746,
    1748,
    1740,
    1747,
    1755,
    1754,
    1756,
    1738,
    716,
    683,
    1752,
    1753,
    687,
    698,
    699,
}  # scheepvaartsluizen moeten op flow_rate=0
df = model.outlet.static.df
mask = df["node_id"].isin(exclude_ids)
df.loc[mask, "flow_rate"] = 0.05

build_discrete_controls(
    model=model,
    out_static=out_static,
    mask_upstream_aanvoer=mask_upstream_aanvoer,
    exclude_ids=exclude_ids,
    listen_node_id=1493,
    band=(7.62, 7.68),
    # listen_node_id=1132,
    # band=(2, 3.15),
    flow_open_default=20.0,
    delta_h=0.05,
)


# %%
def add_controller(
    model,
    node_ids,
    *,
    # --- luister-node ---
    listen_node_id: int = 1493,
    # --- thresholds ---
    threshold_high: float = 7.68,
    threshold_low: float | None = None,
    threshold_delta: float | None = None,
    state_labels=("aanvoer", "afvoer"),
    # --- flow instellingen voor OUTLETS ---
    flow_aanvoer_outlet="orig",
    flow_afvoer_outlet="orig",
    # --- flow instellingen voor PUMPS ---
    flow_aanvoer_pump="orig",
    flow_afvoer_pump="orig",
    # --- max_downstream instellingen ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,
    delta_max_ds_aanvoer=None,
    delta_max_ds_afvoer=None,
    keep_min_us=True,
    dc_offset=10.0,
):
    out_df = model.outlet.static.df
    pump_df = model.pump.static.df

    outlet_ids = set(out_df["node_id"].astype(int))
    pump_ids = set(pump_df["node_id"].astype(int))

    # automatische hysterese
    if threshold_delta is not None:
        threshold_low_used = threshold_high - float(threshold_delta)
    else:
        threshold_low_used = threshold_low

    # helper max_ds
    def _resolve_max_ds(mode, base):
        if mode is None:
            return float("nan")
        if isinstance(mode, (int, float)):
            return float(mode)
        if mode == "existing":
            return base
        if mode == "nan":
            return float("nan")
        return base

    # MAIN LOOP
    for nid in map(int, node_ids):
        # detecteer type
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

        # originele waarden
        flow_orig = float(row["flow_rate"])
        max_ds_orig = float(row["max_downstream_level"]) if pd.notna(row.get("max_downstream_level")) else float("nan")
        min_us_orig = float(row["min_upstream_level"]) if pd.notna(row.get("min_upstream_level")) else None

        # FLOWS
        if node_type == "Outlet":
            flow0 = flow_orig if flow_aanvoer_outlet == "orig" else float(flow_aanvoer_outlet)
            flow1 = flow_orig if flow_afvoer_outlet == "orig" else float(flow_afvoer_outlet)
        else:
            flow0 = flow_orig if flow_aanvoer_pump == "orig" else float(flow_aanvoer_pump)
            flow1 = flow_orig if flow_afvoer_pump == "orig" else float(flow_afvoer_pump)

        # MAX DOWNSTREAM
        ds0 = _resolve_max_ds(max_ds_aanvoer, max_ds_orig)
        ds1 = _resolve_max_ds(max_ds_afvoer, max_ds_orig)

        if delta_max_ds_aanvoer is not None:
            ds0 = max_ds_orig + float(delta_max_ds_aanvoer)

        if delta_max_ds_afvoer is not None:
            ds1 = max_ds_orig + float(delta_max_ds_afvoer)

        # MIN UPSTREAM
        # Only use min_upstream when it's a valid numeric value (not None or NaN).
        if keep_min_us and min_us_orig is not None and not np.isnan(min_us_orig):
            min_us = [float(min_us_orig), float(min_us_orig)]
        else:
            min_us = None

        # STATIC
        static_kwargs = {
            "control_state": list(state_labels),
            "flow_rate": [flow0, flow1],
            "max_downstream_level": [ds0, ds1],
        }
        if min_us is not None:
            static_kwargs["min_upstream_level"] = min_us

        if node_type == "Outlet":
            model.update_node(nid, "Outlet", [outlet.Static(**static_kwargs)])
        else:
            model.update_node(nid, "Pump", [pump.Static(**static_kwargs)])

        truth = ["F", "T"]

        ctrl = [state_labels[0], state_labels[1]]  # ["aanvoer", "afvoer"]

        dc_node = Node(geometry=Point(geom.x + dc_offset, geom.y))

        cond_kwargs = {
            "compound_variable_id": 1,
            "condition_id": [1],
            "threshold_high": [float(threshold_high)],
        }
        if threshold_low_used is not None:
            cond_kwargs["threshold_low"] = [float(threshold_low_used)]

        dc = model.discrete_control.add(
            dc_node,
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=int(listen_node_id),
                    variable=["level"],
                ),
                discrete_control.Condition(**cond_kwargs),
                discrete_control.Logic(
                    truth_state=truth,
                    control_state=ctrl,
                ),
            ],
        )

        model.link.add(dc, parent)
        print(f"[OK] controller toegevoegd aan {node_type} {nid}")


# %%
# %%
# Gemaal Waterwolf/Abelstok en Schaphalsterzijl staat in aanvoersituaties uit, Evt afvoer via sluis
# === afvoerpumps/oulets ===
selected_node_ids = [
    44,
    47,
    121,
    431,
    521,
    524,
    560,
    740,
    741,
    385,
    538,
    723,
    145,
    147,
    31,
    34,
    168,
    42,
    43,
    181,
    182,
    40,
    1748,
    1755,
    1754,
    1747,
    30,
    727,
]

add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1493,
    # thresholds
    threshold_high=7.68,
    threshold_delta=0.07,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    flow_afvoer_outlet=50.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # outlets: 1000
    delta_max_ds_aanvoer=0.0,  # geen extra
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)

# %%
# %%# === Aanvoergemalen/aanvoerpumps ===
selected_node_ids = [
    609,
    48,
    129,
    708,
    117,
    109,
    119,
    184,
    118,
    125,
    110,
    120,
    111,
    115,
    116,
    112,
    122,
    32,
    45,
    107,
    123,
    146,
    106,
    711,
    721,
    1753,
    505,
    169,
]

add_controller(
    model=model,
    node_ids=selected_node_ids,
    listen_node_id=1493,
    threshold_high=7.68,
    threshold_delta=0.07,
    # flows:
    flow_aanvoer_outlet=1.0,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.04,
    max_ds_afvoer=9999,
    # naming:
    state_labels=("closed", "open"),  # 0=closed, 1=open
    keep_min_us=True,
)

# %% pomp/outlets die zowel aanvoer als afvoer moeten kunnen doen
# === Gebruik ===
target_nodes = [
    327,
    719,
    412,
    442,
    644,
    736,
    446,
    739,
    737,
    738,
    322,
    326,
    567,
    569,
    494,
    499,
    572,
    575,
    606,
    748,
    435,
    563,
    635,
    566,
    645,
    570,
    571,
    568,
    428,
    423,
    485,
]
add_controller(
    model=model,
    node_ids=target_nodes,
    listen_node_id=1493,
    # --- thresholds ---
    threshold_high=7.68,
    # --- flows ---
    flow_aanvoer_outlet="orig",  # of bv. 0.0
    flow_afvoer_outlet="orig",  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    # --- upstream constraint behouden ---
    keep_min_us=True,
)

# %%
# Gemaal Waterwolf afvoer via sluis
# === Aanvoergemalen/aanvoerpumps Grote pompen en outlets===
add_controller(
    model=model,
    node_ids=[29, 728],
    listen_node_id=1493,
    # thresholds
    threshold_high=7.68,
    threshold_delta=0.07,
    # ---- OUTLET FLOWS ----
    flow_aanvoer_outlet=9999,  # laagwater → moet open (aanvoer)
    flow_afvoer_outlet=0,  # hoogwater → dicht (geen afvoer)
    # ---- PUMP FLOWS ----
    flow_aanvoer_pump=0,  # laagwater → pomp staat uit
    flow_afvoer_pump="orig",  # hoogwater → pomp draait op originele flow
    # ---- MAX DOWNSTREAM ----
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.04,
    max_ds_afvoer=1000,  # open grenzen bij hoogwater
    keep_min_us=True,
)
# %%
# === Dwarsdiep,
# afvoerpumps/oulets aanvoer dicht, afvoer open, ===
add_controller(
    model=model,
    node_ids=[350, 537, 383, 618, 681, 357, 401, 501, 519, 455, 565, 652, 389, 564, 551],
    listen_node_id=1132,
    # --- thresholds ---
    threshold_high=3.12,
    threshold_delta=0.02,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    flow_afvoer_outlet=20.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
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
    node_ids=[687, 698, 699, 36, 37, 38],
    listen_node_id=1132,
    # --- thresholds ---
    threshold_high=3.12,
    threshold_delta=0.02,
    # flows:
    flow_aanvoer_outlet=1.0,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    # naming:
    state_labels=("closed", "open"),  # 0=closed, 1=open
    keep_min_us=True,
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[368],
    listen_node_id=1132,
    # --- thresholds ---
    threshold_high=3.12,
    threshold_delta=0.02,
    # threshold_low=7.50,        # optioneel: hysterese
    # --- flows ---
    flow_aanvoer_outlet="orig",  # of bv. 0.0
    flow_afvoer_outlet="orig",  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    keep_min_us=True,
)

# %% === Spijksterpompen

# afvoerpumps/oulets aanvoer dicht, afvoer open, ===
add_controller(
    model=model,
    node_ids=[41, 731],
    listen_node_id=1182,
    # --- thresholds ---
    threshold_high=-0.68,
    threshold_delta=0.02,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    flow_afvoer_outlet=50.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
    flow_afvoer_pump="orig",  # hoogwater → originele afvoer
    # ---- MAX_DOWNSTREAM ----
    max_ds_aanvoer="existing",  # state0 → h_pump / h_out
    max_ds_afvoer=1000,  # controllers werkt niet goed met NAN waarden daarom 1000
    delta_max_ds_afvoer=None,
    keep_min_us=True,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
add_controller(
    model=model,
    node_ids=[136, 139, 371, 124, 114, 837],
    listen_node_id=1182,
    # --- thresholds ---
    threshold_high=-0.68,
    threshold_delta=0.02,
    # flows:
    flow_aanvoer_outlet=1.0,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    max_ds_afvoer=9999,
    # naming:
    state_labels=("closed", "open"),  # 0=closed, 1=open
    keep_min_us=True,
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[390, 396, 427, 462, 387],
    listen_node_id=1182,
    # --- thresholds ---
    threshold_high=-0.68,
    threshold_delta=0.02,
    # threshold_low=7.50,        # optioneel: hysterese
    # --- flows ---
    flow_aanvoer_outlet="orig",  # of bv. 0.0
    flow_afvoer_outlet="orig",  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    delta_max_ds_aanvoer=0.0,
    # delta_max_ds_afvoer=None,  # optioneel
    # --- upstream constraint behouden ---
    keep_min_us=True,
)

# %%
# === Fivelingo,afvoerpumps/oulets aanvoer dicht, afvoer open, ===
add_controller(
    model=model,
    node_ids=[732, 67],
    listen_node_id=1172,
    # --- thresholds ---
    threshold_high=-1.25,
    threshold_delta=0.02,
    # ---- OUTLET FLOWS (afvoer) ----
    flow_aanvoer_outlet=0.0,  # laagwater → dicht
    flow_afvoer_outlet=50.0,  # hoogwater → afvoer open
    # ---- PUMP FLOWS (afvoer) ----
    flow_aanvoer_pump=0.0,  # laagwater → uit
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
    node_ids=[142, 143, 702, 703, 517],
    listen_node_id=1172,
    # --- thresholds ---
    threshold_high=-1.25,
    threshold_delta=0.02,
    # flows:
    flow_aanvoer_outlet=1.0,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    # naming:
    state_labels=("closed", "open"),  # 0=closed, 1=open
    keep_min_us=True,
)

# Aanvoer inlaten/pumps (uit bij afvoer en aan bij aanvoer)
add_controller(
    model=model,
    node_ids=[693],
    listen_node_id=1265,
    # --- thresholds ---
    threshold_high=-1.16,
    threshold_delta=0.02,
    # flows:
    flow_aanvoer_outlet=1.0,  # open bij laagwater → laat water in
    flow_afvoer_outlet=0,  # dicht bij hoogwater → geen afvoer
    flow_afvoer_pump=0,  # idem voor pumps die alleen aanvoer doen
    flow_aanvoer_pump="orig",
    # max_ds:
    max_ds_aanvoer="existing",
    delta_max_ds_aanvoer=0.02,
    max_ds_afvoer=9999,
    # naming:
    state_labels=("closed", "open"),  # 0=closed, 1=open
    keep_min_us=True,
)

# interne gemalen/outlets in peilgebied die zowel aanvoer als afvoer moeten kunnen doen
add_controller(
    model=model,
    node_ids=[508, 509, 646, 539],
    listen_node_id=1172,
    # --- thresholds ---
    threshold_high=-1.25,
    threshold_delta=0.02,
    # --- flows ---
    flow_aanvoer_outlet="orig",  # of bv. 0.0
    flow_afvoer_outlet="orig",  # of bv. 50.0
    flow_afvoer_pump="orig",  # of bv. 80.0
    flow_aanvoer_pump="orig",
    # --- max downstream levels ---
    max_ds_aanvoer="existing",
    max_ds_afvoer=1000,  # voorkomt NaN
    delta_max_ds_aanvoer=0.0,
    keep_min_us=True,
)

# %%
# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
check_basin_level.add_check_basin_level(model=model)
model.pump.static.df["meta_func_afvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 0
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    result = model.run()
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_all()

# %%

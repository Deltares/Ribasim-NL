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
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_lyr", "output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(
    AUTHORITY,
    "verwerkt",
    "1_ontvangen_data",
    "",
    "20250527",
    "gebieden_met_wateraanvoermogelijkheid_noorderzijlvest.gpkg",
)

cloud.synchronize(
    filepaths=[
        aanvoer_path,
    ]
)

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
update_basin_static(model=model, precipitation_mm_per_day=1)


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
    # (out_static, upstream_outlet_nodes, {"flow_rate": 100, "max_flow_rate": 100}),
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
] = -0.36  # Check! Bij min_upstream_level =-0.35m NP geen afvoer mogelijk.

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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 564, "max_downstream_level"] = pd.NA
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
model.pump.static.df.loc[model.pump.static.df.node_id == 37, "max_downstream_level"] = 2.74

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
model.pump.static.df.loc[model.pump.static.df.node_id == 30, "min_upstream_level"] = -0.93
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
model.pump.static.df.loc[model.pump.static.df.node_id == 36, "max_downstream_level"] = 0.74

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

# Afvoer outlets die naast aanvoergemaal liggen moet min_upstrem gelijk aan max_downstrem
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
# Discrete control toevoegen aan alle upstream outlets met aanvoer
def build_discrete_controls(
    model,
    out_static: pd.DataFrame,
    mask_upstream_aanvoer: pd.Series,
    exclude_ids=None,
    listen_node_id: int = 1132,
    # band=(2, 3.15),
    band=(7.622, 7.68),
    flow_open_default: float = 20.0,  # <- default fallback
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
                    # threshold_low=[float(th_low)],   # False als < th_low (hysterese optioneel)
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
# aanvoer en afvoer outlets die uitkomen op Manning waterloop die geen aanvoer nodig heeft. Bij aanvoer moet flow op 0 staan zodat ze niet Manning waterlopen gaan aanvullen
# Bij afvoer mag flow niet op 0 staan anders werkt de afvoer niet meer. Gemaal Waterwolf/Abelstok en Schaphalsterzijl staat in aanvoersituaties uit, Evt afvoer via sluis
# === afvoerpumps/oulets ===
selected_node_ids = [
    44,
    47,
    350,
    431,
    681,
    357,
    537,
    401,
    501,
    455,
    560,
    740,
    741,
    565,
    652,
    551,
    618,
    383,
    385,
    368,
    538,
    564,
    389,
    723,
    145,
    147,
    31,
    34,
    168,
    41,
    42,
    43,
    731,
    181,
    182,
    67,
    40,
    732,
    519,
    1748,
    1755,
    1754,
    1747,
]
LISTEN_NODE_ID = 1493
# LISTEN_NODE_ID = 1132

DELTA_LOW = 0.07

basin = model.basin.area.df
out_static = model.outlet.static.df
pump_static = model.pump.static.df

# === 1) TH_HIGH uit basin.meta_streefpeil ===
th_high = None
row = basin.loc[basin["node_id"] == LISTEN_NODE_ID]
if not row.empty and "meta_streefpeil" in row.columns:
    val = row["meta_streefpeil"].iloc[0]
    if pd.notna(val):
        th_high = 7.68
        # th_high = 3.15
if th_high is None:
    raise ValueError(f"Kon 'meta_streefpeil' voor listen_node_id {LISTEN_NODE_ID} niet vinden.")
th_low = th_high - DELTA_LOW  # nu niet gebruikt


# === Helpers ===
def _flow_open_from_pump_row(val, default=1.0):
    if isinstance(val, list | tuple | np.ndarray):
        arr = pd.to_numeric(np.asarray(val), errors="coerce")
        if np.all(np.isnan(arr)):
            return float(default)
        return float(np.nanmax(arr))
    try:
        f = float(val)
        return f if f > 0 else float(default)
    except Exception:
        return float(default)


def _scalar_from(df, node_id, col_candidates):
    for col in col_candidates:
        if col and (col in df.columns):
            s = df.loc[df["node_id"] == node_id, col]
            if s.empty or pd.isna(s.iloc[0]):
                continue
            v = s.iloc[0]
            if isinstance(v, list | tuple | np.ndarray):
                if len(v) == 0 or pd.isna(v[0]):
                    continue
                try:
                    return float(v[0])
                except Exception:
                    continue
            try:
                x = float(v)
                if not pd.isna(x):
                    return x
            except Exception:
                pass
    return None


def _static_obj(factory_cls, **maybe_kwargs):
    """Maak een Static() object met alleen niet-None kwargs."""
    kwargs = {k: v for k, v in maybe_kwargs.items() if v is not None}
    return factory_cls(**kwargs)


# Kolomkandidaten
MIN_US_COLS_OUTLET = ["min_upstream_level", "min_upstream_water_level"]
MIN_US_COLS_PUMP = ["min_upstream_level", "min_upstream_water_level"]
MAX_DS_COLS_OUTLET = ["max_downstream_level", "max_downstream_water_level"]
MAX_DS_COLS_PUMP = ["max_downstream_level", "max_downstream_water_level"]  # optioneel

# Membership-sets
outlet_ids = set(out_static["node_id"].astype(int)) if "node_id" in out_static else set()
pump_ids = set(pump_static["node_id"].astype(int)) if "node_id" in pump_static else set()

# === 2) Per geselecteerde node: states + DC ===
for nid in selected_node_ids:
    try:
        nid_int = int(nid)
    except Exception:
        print(f"[skip] ongeldige node_id: {nid}")
        continue

    if nid_int in outlet_ids:
        # OUTLET
        h_out = _scalar_from(out_static, nid_int, MAX_DS_COLS_OUTLET)  # max_downstream
        m_out = _scalar_from(out_static, nid_int, MIN_US_COLS_OUTLET)  # min_upstream (optioneel)

        outlet_static_obj = _static_obj(
            outlet.Static,
            control_state=["open", "closed"],
            flow_rate=[20.0, 0.0],
            max_downstream_level=[1000, h_out],
            min_upstream_level=[m_out, m_out] if m_out is not None else None,
        )

        model.update_node(
            node_id=nid_int,
            node_type="Outlet",
            data=[outlet_static_obj],
        )

        geom = model.outlet[nid_int].geometry
        x0, y0 = geom.x, geom.y

        dc = model.discrete_control.add(
            Node(geometry=Point(x0 + 10, y0)),
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=LISTEN_NODE_ID,
                    variable=["level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=1,
                    condition_id=[1],
                    threshold_high=[th_high],
                    # threshold_low=[th_low],
                ),
                discrete_control.Logic(
                    truth_state=["T", "F"],
                    control_state=["open", "closed"],
                ),
            ],
        )
        model.link.add(dc, model.outlet[nid_int])

    elif nid_int in pump_ids:
        # PUMP
        flow_series = pump_static.loc[pump_static["node_id"] == nid_int, "flow_rate"]
        flow_open = _flow_open_from_pump_row(flow_series.iloc[0]) if not flow_series.empty else 1.0

        m_pump = _scalar_from(pump_static, nid_int, MIN_US_COLS_PUMP)  # optioneel
        h_pump = _scalar_from(pump_static, nid_int, MAX_DS_COLS_PUMP)  # optioneel

        pump_static_obj = _static_obj(
            pump.Static,
            control_state=["closed", "open"],
            flow_rate=[0.0, float(flow_open)],
            min_upstream_level=[m_pump, m_pump] if m_pump is not None else None,
            max_downstream_level=[h_pump, np.nan] if h_pump is not None else None,
        )

        model.update_node(
            node_id=nid_int,
            node_type="Pump",
            data=[pump_static_obj],
        )

        geom = model.pump[nid_int].geometry
        x0, y0 = geom.x, geom.y

        dc = model.discrete_control.add(
            Node(geometry=Point(x0 + 10, y0)),
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=LISTEN_NODE_ID,
                    variable=["level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=1,
                    condition_id=[1],
                    threshold_high=[th_high],
                    # threshold_low=[th_low],
                ),
                discrete_control.Logic(
                    truth_state=["T", "F"],
                    control_state=["open", "closed"],
                ),
            ],
        )
        model.link.add(dc, model.pump[nid_int])

    else:
        print(f"[skip] node {nid_int}: geen Outlet of Pump in static.df gevonden")

# aanvoer en afvoer outlets die uitkomen op Manning waterloop die geen aanvoer nodig heeft. Bij aanvoer moet flow op 0 staan zodat ze niet Manning waterlopen gaan aanvullen
# Bij afvoer mag flow niet op 0 staan anders werkt de afvoer niet meer. Gemaal Waterwolf afvoer via sluis
# === Aanvoergemalen/aanvoerpumps Grote pompen en outlets===
selected_node_ids = [29, 728]
LISTEN_NODE_ID = 1493
# LISTEN_NODE_ID = 1132
DELTA_LOW = 0.07

basin = model.basin.area.df
out_static = model.outlet.static.df
pump_static = model.pump.static.df

# === 1) TH_HIGH uit basin.meta_streefpeil ===
th_high = None
row = basin.loc[basin["node_id"] == LISTEN_NODE_ID]
if not row.empty and "meta_streefpeil" in row.columns:
    val = row["meta_streefpeil"].iloc[0]
    if pd.notna(val):
        th_high = 7.68
        # th_high = 3.15
if th_high is None:
    raise ValueError(f"Kon 'meta_streefpeil' voor listen_node_id {LISTEN_NODE_ID} niet vinden.")
th_low = th_high - DELTA_LOW  # nu niet gebruikt


# === Helpers ===
def _flow_open_from_pump_row(val, default=1.0):
    if isinstance(val, list | tuple | np.ndarray):
        arr = pd.to_numeric(np.asarray(val), errors="coerce")
        if np.all(np.isnan(arr)):
            return float(default)
        return float(np.nanmax(arr))
    try:
        f = float(val)
        return f if f > 0 else float(default)
    except Exception:
        return float(default)


def _scalar_from(df, node_id, col_candidates):
    for col in col_candidates:
        if col and (col in df.columns):
            s = df.loc[df["node_id"] == node_id, col]
            if s.empty or pd.isna(s.iloc[0]):
                continue
            v = s.iloc[0]
            if isinstance(v, list | tuple | np.ndarray):
                if len(v) == 0 or pd.isna(v[0]):
                    continue
                try:
                    return float(v[0])
                except Exception:
                    continue
            try:
                x = float(v)
                if not pd.isna(x):
                    return x
            except Exception:
                pass
    return None


def _static_obj(factory_cls, **maybe_kwargs):
    """Maak een Static() object met alleen niet-None kwargs."""
    kwargs = {k: v for k, v in maybe_kwargs.items() if v is not None}
    return factory_cls(**kwargs)


# Kolomkandidaten
MIN_US_COLS_OUTLET = ["min_upstream_level", "min_upstream_water_level"]
MIN_US_COLS_PUMP = ["min_upstream_level", "min_upstream_water_level"]
MAX_DS_COLS_OUTLET = ["max_downstream_level", "max_downstream_water_level"]
MAX_DS_COLS_PUMP = ["max_downstream_level", "max_downstream_water_level"]  # optioneel

# Membership-sets
outlet_ids = set(out_static["node_id"].astype(int)) if "node_id" in out_static else set()
pump_ids = set(pump_static["node_id"].astype(int)) if "node_id" in pump_static else set()

# === 2) Per geselecteerde node: states + DC ===
for nid in selected_node_ids:
    try:
        nid_int = int(nid)
    except Exception:
        print(f"[skip] ongeldige node_id: {nid}")
        continue

    if nid_int in outlet_ids:
        # OUTLET
        h_out = _scalar_from(out_static, nid_int, MAX_DS_COLS_OUTLET) + 0.02  # max_downstream
        m_out = _scalar_from(out_static, nid_int, MIN_US_COLS_OUTLET)  # min_upstream (optioneel)

        outlet_static_obj = _static_obj(
            outlet.Static,
            control_state=["open", "closed"],
            flow_rate=[9999, 0],
            max_downstream_level=[h_out, 9999],
            min_upstream_level=[m_out, m_out] if m_out is not None else None,
        )

        model.update_node(
            node_id=nid_int,
            node_type="Outlet",
            data=[outlet_static_obj],
        )

        geom = model.outlet[nid_int].geometry
        x0, y0 = geom.x, geom.y

        dc = model.discrete_control.add(
            Node(geometry=Point(x0 + 10, y0)),
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=LISTEN_NODE_ID,
                    variable=["level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=1,
                    condition_id=[1],
                    threshold_high=[th_high],
                    # threshold_low=[th_low],
                ),
                discrete_control.Logic(
                    truth_state=["T", "F"],
                    control_state=["open", "closed"],
                ),
            ],
        )
        model.link.add(dc, model.outlet[nid_int])

    elif nid_int in pump_ids:
        # PUMP
        flow_series = pump_static.loc[pump_static["node_id"] == nid_int, "flow_rate"]
        flow_open = _flow_open_from_pump_row(flow_series.iloc[0]) if not flow_series.empty else 1.0

        m_pump = _scalar_from(pump_static, nid_int, MIN_US_COLS_PUMP)  # optioneel
        h_pump = _scalar_from(pump_static, nid_int, MAX_DS_COLS_PUMP)  # optioneel

        pump_static_obj = _static_obj(
            pump.Static,
            control_state=["closed", "open"],
            flow_rate=[0.0, float(flow_open)],
            min_upstream_level=[m_pump, m_pump] if m_pump is not None else None,
            max_downstream_level=[h_pump, 9999] if h_pump is not None else None,
        )

        model.update_node(
            node_id=nid_int,
            node_type="Pump",
            data=[pump_static_obj],
        )

        geom = model.pump[nid_int].geometry
        x0, y0 = geom.x, geom.y

        dc = model.discrete_control.add(
            Node(geometry=Point(x0 + 10, y0)),
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=LISTEN_NODE_ID,
                    variable=["level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=1,
                    condition_id=[1],
                    threshold_high=[th_high],
                    # threshold_low=[th_low],
                ),
                discrete_control.Logic(
                    truth_state=["T", "F"],
                    control_state=["open", "closed"],
                ),
            ],
        )
        model.link.add(dc, model.pump[nid_int])

    else:
        print(f"[skip] node {nid_int}: geen Outlet of Pump in static.df gevonden")


# %%
# === Aanvoergemalen/aanvoerpumps ===
selected_node_ids = [
    121,
    48,
    129,
    708,
    117,
    109,
    119,
    184,
    118,  # Gemaal Oudendijk (#118) toegevoegd
    125,
    114,
    124,
    110,
    38,
    120,
    111,
    115,
    116,
    136,
    139,
    112,
    122,
    32,
    35,
    36,
    37,
    45,
    107,
    123,
    146,
    106,
    143,
    711,
    721,
    1753,
    505,
    837,
    142,
]
LISTEN_NODE_ID = 1493
# LISTEN_NODE_ID = 1132
DELTA_LOW = 0.07

basin = model.basin.area.df
out_static = model.outlet.static.df
pump_static = model.pump.static.df

# === 1) TH_HIGH uit basin.meta_streefpeil ===
th_high = None
row = basin.loc[basin["node_id"] == LISTEN_NODE_ID]
if not row.empty and "meta_streefpeil" in row.columns:
    val = row["meta_streefpeil"].iloc[0]
    if pd.notna(val):
        #  th_high = float(val) - 0.02
        th_high = 7.68
        # th_high = 3.15
if th_high is None:
    raise ValueError(f"Kon 'meta_streefpeil' voor listen_node_id {LISTEN_NODE_ID} niet vinden.")
th_low = th_high - DELTA_LOW  # nu niet gebruikt


# === Helpers ===
def _flow_open_from_pump_row(val, default=1.0):
    if isinstance(val, list | tuple | np.ndarray):
        arr = pd.to_numeric(np.asarray(val), errors="coerce")
        if np.all(np.isnan(arr)):
            return float(default)
        return float(np.nanmax(arr))
    try:
        f = float(val)
        return f if f > 0 else float(default)
    except Exception:
        return float(default)


def _scalar_from(df, node_id, col_candidates):
    for col in col_candidates:
        if col and (col in df.columns):
            s = df.loc[df["node_id"] == node_id, col]
            if s.empty or pd.isna(s.iloc[0]):
                continue
            v = s.iloc[0]
            if isinstance(v, list | tuple | np.ndarray):
                if len(v) == 0 or pd.isna(v[0]):
                    continue
                try:
                    return float(v[0])
                except Exception:
                    continue
            try:
                x = float(v)
                if not pd.isna(x):
                    return x
            except Exception:
                pass
    return None


def _static_obj(factory_cls, **maybe_kwargs):
    """Maak een Static() object met alleen niet-None kwargs."""
    kwargs = {k: v for k, v in maybe_kwargs.items() if v is not None}
    return factory_cls(**kwargs)


# Kolomkandidaten
MIN_US_COLS_OUTLET = ["min_upstream_level", "min_upstream_water_level"]
MIN_US_COLS_PUMP = ["min_upstream_level", "min_upstream_water_level"]
MAX_DS_COLS_OUTLET = ["max_downstream_level", "max_downstream_water_level"]
MAX_DS_COLS_PUMP = ["max_downstream_level", "max_downstream_water_level"]  # optioneel

# Membership-sets
outlet_ids = set(out_static["node_id"].astype(int)) if "node_id" in out_static else set()
pump_ids = set(pump_static["node_id"].astype(int)) if "node_id" in pump_static else set()

# === 2) Per geselecteerde node: states + DC ===
for nid in selected_node_ids:
    try:
        nid_int = int(nid)
    except Exception:
        print(f"[skip] ongeldige node_id: {nid}")
        continue

    if nid_int in outlet_ids:
        # OUTLET
        h_out = _scalar_from(out_static, nid_int, MAX_DS_COLS_OUTLET)  # max_downstream
        m_out = _scalar_from(out_static, nid_int, MIN_US_COLS_OUTLET)  # min_upstream (optioneel)

        if h_out is None:
            print(f"[skip] outlet {nid_int}: geen geldige max_downstream_level")
            continue

        outlet_static_obj = _static_obj(
            outlet.Static,
            control_state=["closed", "open"],
            flow_rate=[0.0, 1.0],
            max_downstream_level=[h_out + 0.02, h_out + 0.04],
            min_upstream_level=[m_out, m_out] if m_out is not None else None,
        )

        model.update_node(
            node_id=nid_int,
            node_type="Outlet",
            data=[outlet_static_obj],
        )

        geom = model.outlet[nid_int].geometry
        x0, y0 = geom.x, geom.y

        dc = model.discrete_control.add(
            Node(geometry=Point(x0 + 10, y0)),
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=LISTEN_NODE_ID,
                    variable=["level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=1,
                    condition_id=[1],
                    threshold_high=[th_high],
                    # threshold_low=[th_low],
                ),
                discrete_control.Logic(
                    truth_state=["T", "F"],
                    control_state=["closed", "open"],
                ),
            ],
        )
        model.link.add(dc, model.outlet[nid_int])

    elif nid_int in pump_ids:
        # PUMP
        flow_series = pump_static.loc[pump_static["node_id"] == nid_int, "flow_rate"]
        flow_open = _flow_open_from_pump_row(flow_series.iloc[0]) if not flow_series.empty else 1.0

        m_pump = _scalar_from(pump_static, nid_int, MIN_US_COLS_PUMP)  # optioneel
        h_pump = _scalar_from(pump_static, nid_int, MAX_DS_COLS_PUMP)  # optioneel

        pump_static_obj = _static_obj(
            pump.Static,
            control_state=["closed", "open"],
            flow_rate=[0.0, float(flow_open)],
            min_upstream_level=[m_pump, m_pump] if m_pump is not None else None,
            max_downstream_level=[h_pump, h_pump + 0.04] if h_pump is not None else None,
        )

        model.update_node(
            node_id=nid_int,
            node_type="Pump",
            data=[pump_static_obj],
        )

        geom = model.pump[nid_int].geometry
        x0, y0 = geom.x, geom.y

        dc = model.discrete_control.add(
            Node(geometry=Point(x0 + 10, y0)),
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=LISTEN_NODE_ID,
                    variable=["level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=1,
                    condition_id=[1],
                    threshold_high=[th_high],
                    # threshold_low=[th_low],
                ),
                discrete_control.Logic(
                    truth_state=["T", "F"],
                    control_state=["closed", "open"],
                ),
            ],
        )
        model.link.add(dc, model.pump[nid_int])

    else:
        print(f"[skip] node {nid_int}: geen Outlet of Pump in static.df gevonden")


def add_max_ds_controller(
    model,
    target_node_ids,
    listen_node_id: int,
    th_high: float = 7.68,
    # th_high: float = 3.15,
    offset: float = 10.0,
):
    """
    Voeg discrete controls toe aan geselecteerde pumps/outlets om sturing van het downstream-peil
    te regelen afhankelijk van het peil op een referentie-node (listen_node_id).

    Logica:
      - Bij **aanvoer** (peil listen_node_id < th_high):
          * De node werkt in 'aanvoer'-modus.
          * max_downstream_level = bestaande waarde uit het model (stuurt op downstream-peil).
      - Bij **afvoer** (peil listen_node_id > th_high):
          * De node werkt in 'afvoer'-modus.
          * max_downstream_level = NaN (geen beperking, vrij spuien).

    In beide situaties blijft de outlet/pump open (zelfde flow in beide states).
    """
    for node_id in target_node_ids:
        # --- node-type bepalen
        if node_id in model.outlet.node.df.index:
            node_type = "Outlet"
            geom = model.outlet[node_id].geometry
            static_df = model.outlet.static.df
        elif node_id in model.pump.node.df.index:
            node_type = "Pump"
            geom = model.pump[node_id].geometry
            static_df = model.pump.static.df
        else:
            print(f"[skip] node {node_id}: geen Pump of Outlet gevonden")
            continue

        # --- bestaande waarden ophalen
        row = static_df.loc[static_df["node_id"] == node_id]
        if row.empty:
            print(f"[skip] {node_type} {node_id}: geen static record gevonden")
            continue

        flow = float(row["flow_rate"].iloc[0]) if "flow_rate" in row else 1.0
        min_us = row["min_upstream_level"].iloc[0] if "min_upstream_level" in row else float("nan")
        max_ds_existing = row["max_downstream_level"].iloc[0] if "max_downstream_level" in row else float("nan")

        # --- discrete control toevoegen
        dc_node = Node(geometry=Point(geom.x + offset, geom.y))
        dc = model.discrete_control.add(
            dc_node,
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=int(listen_node_id),
                    variable=["level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=1,
                    condition_id=[1],
                    threshold_high=[float(th_high)],
                ),
                # Logica:
                #  - F (laag) = aanvoer
                #  - T (hoog) = afvoer
                discrete_control.Logic(
                    truth_state=["F", "T"],
                    control_state=["aanvoer", "afvoer"],
                ),
            ],
        )

        # --- nieuwe static node aanmaken met bestaande waarden
        if node_type == "Outlet":
            new_static = outlet.Static(
                control_state=["aanvoer", "afvoer"],
                flow_rate=[float(flow), float(flow)],  # altijd flow
                min_upstream_level=[min_us, min_us],
                max_downstream_level=[float(max_ds_existing), 1000],  # behoud bestaande waarde bij aanvoer
            )
            model.update_node(node_id=node_id, node_type=node_type, data=[new_static])
            model.link.add(dc, model.outlet[node_id])

        elif node_type == "Pump":
            new_static = pump.Static(
                control_state=["aanvoer", "afvoer"],
                flow_rate=[float(flow), float(flow)],
                min_upstream_level=[min_us, min_us],
                max_downstream_level=[float(max_ds_existing), float("nan")],
            )
            model.update_node(node_id=node_id, node_type=node_type, data=[new_static])
            model.link.add(dc, model.pump[node_id])

        print(f"[OK] Controller toegevoegd aan {node_type} node {node_id}")


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
    427,
    396,
    428,
    462,
    387,
    423,
    485,
    390,
    517,
    508,
    509,
    646,
    702,
    703,
    539,
]
add_max_ds_controller(
    model=model,
    target_node_ids=target_nodes,
    listen_node_id=1493,
    # listen_node_id=1132,
    th_high=7.68,
    # th_high=3.15,
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

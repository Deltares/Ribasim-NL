# %%

import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

LEVEL_DIFFERENCE_THRESHOLD = 0.02  # sync model.solver.level_difference_threshold and control-settings
MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "AaenMaas"
SHORT_NAME: str = "aam"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"
FLUSHING_SEASONAL: bool = False  # True = Apr–Oct aan (cyclisch), False = altijd aan (constant)

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 745: Sluis Engelen, alles via Crevecoeur

EXCLUDE_NODES = {}
EXCLUDE_SUPPLY_NODES = [745]

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(r"AaenMaas/verwerkt/sturing/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path, aanvoergebieden_gpkg])

# %%
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 30
model.outlet.static.df.flow_rate = 30
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate


# fixes:
# model.remove_node(node_id=574, remove_links=True)  # verwijderen Manning knoop naast outlet
# model.remove_node(node_id=602, remove_links=True)  # verwijderen Manning knoop naast outlet
model.remove_node(node_id=683, remove_links=True)  # verwijderen Manning knoop naast outlet
model.remove_node(node_id=684, remove_links=True)  # verwijderen Manning knoop naast outlet
model.remove_node(node_id=1053, remove_links=True)  # verwijderen Manning knoop naast outlet
model.remove_node(node_id=467, remove_links=True)  # verwijderen Manning knoop naast outlet
model.remove_node(node_id=439, remove_links=True)  # verwijderen Manning knoop naast outlet
model.remove_node(node_id=634, remove_links=True)  # verwijderen Manning knoop naast outlet
model.remove_node(node_id=587, remove_links=True)  # verwijderen Manning knoop naast outlet
model.remove_node(node_id=809, remove_links=True)
model.remove_node(node_id=213, remove_links=True)
model.remove_node(node_id=415, remove_links=True)
model.remove_node(node_id=364, remove_links=True)
model.remove_node(node_id=811, remove_links=True)
model.remove_node(node_id=682, remove_links=True)
model.remove_node(node_id=664, remove_links=True)


model.update_node(node_id=586, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=479, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=531, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=510, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=475, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=681, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1006, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1005, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=551, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=488, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1115, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=566, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=686, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=469, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=569, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=533, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=526, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=632, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1018, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=723, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=722, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=570, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=697, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=708, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=640, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=521, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1063, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=452, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1054, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=625, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1056, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=694, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=524, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=425, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=693, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=442, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=538, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=702, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=710, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=622, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=680, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=584, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=487, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1062, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=506, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=541, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=527, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=577, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1050, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=582, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1051, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=599, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=597, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=597, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=597, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=597, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=807, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=628, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=721, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=573, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1073, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=621, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=534, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=496, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=555, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=559, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=718, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=481, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=461, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=691, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1046, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=438, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=430, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=717, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1058, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=698, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=571, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1001, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=701, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=494, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=509, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=594, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1016, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=615, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=470, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=1014, node_type="Outlet")  # wordt outlet, was manning
model.update_node(node_id=490, node_type="Outlet")  # wordt outlet, was manning

# ligt benedenstrooms een stuw
model.update_node(node_id=1077, node_type="Outlet")  # wordt outlet, was manning
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1077, "min_upstream_level"] = 4.48499

model.redirect_link(link_id=1147, from_node_id=1219, to_node_id=172)

model.merge_basins(basin_id=1437, to_basin_id=1861, are_connected=True)
model.merge_basins(basin_id=1303, to_basin_id=1360, are_connected=True)
model.merge_basins(basin_id=1707, to_basin_id=1346, are_connected=True)
model.merge_basins(basin_id=1162, to_basin_id=1457, are_connected=True)
model.merge_basins(basin_id=1468, to_basin_id=1277, are_connected=True)
model.merge_basins(basin_id=1155, to_basin_id=1514, are_connected=True)
model.merge_basins(basin_id=1355, to_basin_id=1517, are_connected=True)
model.merge_basins(basin_id=1344, to_basin_id=1626, are_connected=True)
model.merge_basins(basin_id=1428, to_basin_id=1589, are_connected=True)
model.merge_basins(basin_id=1565, to_basin_id=1618, are_connected=True)
model.merge_basins(basin_id=2006, to_basin_id=1618, are_connected=True)
model.merge_basins(basin_id=1366, to_basin_id=1249, are_connected=True)
model.merge_basins(basin_id=1668, to_basin_id=1710, are_connected=True)
model.merge_basins(basin_id=1292, to_basin_id=1484, are_connected=True)
model.merge_basins(basin_id=1966, to_basin_id=1580, are_connected=True)
model.merge_basins(basin_id=1281, to_basin_id=1642, are_connected=True)
model.merge_basins(basin_id=1642, to_basin_id=1510, are_connected=True)

model.merge_basins(basin_id=1571, to_basin_id=1332, are_connected=True)
model.merge_basins(basin_id=1332, to_basin_id=1582, are_connected=True)
model.merge_basins(basin_id=1208, to_basin_id=1279, are_connected=True)
model.merge_basins(basin_id=1279, to_basin_id=1582, are_connected=True)
model.merge_basins(basin_id=1663, to_basin_id=1722, are_connected=True)
model.merge_basins(basin_id=1794, to_basin_id=1836, are_connected=True)
model.merge_basins(basin_id=1455, to_basin_id=1276, are_connected=True)
model.merge_basins(basin_id=1968, to_basin_id=1494, are_connected=True)

# Mierlo wordt aanvoer als afvoer gemaal
model.update_node(node_id=92, node_type="Pump")  # wordt outlet, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 92, "min_upstream_level"] = 16.56
model.update_node(node_id=226, node_type="Pump")  # wordt outlet, was outlet


# Gemaal Veluwe
model.update_node(node_id=100, node_type="Pump")  # wordt outlet, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 100, "min_upstream_level"] = 10.82

# Gemaal Kameren
model.update_node(node_id=95, node_type="Pump")  # wordt outlet, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 95, "min_upstream_level"] = 5.17


# %%
# Toevoegen Mierlo

polygon = aanvoergebieden_df.loc[["Mierlo"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    440,
    575,
    600,
    621,
    640,
    668,
    718,
    725,
    968,
    969,
    1229,
    1434,
    1562,
    1670,
    1671,
    1672,
    1674,
    1944,
    2936,
]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [92, 312, 353, 400]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [226, 280, 997]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [681, 776, 934, 1089, 974]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)
# %%
# Toevoegen Leijgraaf

polygon = aanvoergebieden_df.loc[["Leijgraaf"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    43,
    59,
    60,
    237,
    299,
    309,
    460,
    470,
    479,
    485,
    636,
    729,
    979,
    1027,
    1348,
    1443,
    1450,
    1526,
    1542,
    1552,
    1617,
    1635,
    1672,
    1712,
    1962,
    713,
]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [
    106,
    130,
    153,
    157,
    186,
    207,
    250,
    268,
    275,
    299,
    350,
    385,
    408,
    419,
    584,
    621,
    804,
    955,
    956,
    628,
    818,
    980,
    981,
]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [172, 192, 186, 235, 251, 278, 317, 379, 731]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [
    113,
    140,
    347,
    481,
    496,
    766,
    767,
    573,
    718,
    768,
    808,
    810,
    813,
    814,
    815,
    818,
    819,
    821,
    948,
    957,
    1050,
]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)
# %%
# Toevoegen Hurkske/Aa

polygon = aanvoergebieden_df.loc[["Hurkske/Aa"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    5,
    6,
    7,
    16,
    40,
    43,
    172,
    173,
    258,
    299,
    460,
    466,
    467,
    479,
    979,
    1146,
    1227,
    1397,
    1440,
    1483,
    1610,
    1962,
    1978,
]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [83, 107, 122, 139, 150, 170, 210, 244, 256, 342, 421, 304, 487, 597, 982, 1051]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [183, 375, 418, 521, 640, 1054]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [155, 212, 332, 823, 824, 1062]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Z-W West (Zuid-Willemsvaart West)

polygon = aanvoergebieden_df.loc[["Z-W West"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    5,
    6,
    7,
    221,
    253,
    265,
    266,
    267,
    622,
    624,
    638,
    694,
    1469,
    1479,
    1670,
    1671,
    1672,
]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [345, 795, 797, 1046]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [166, 211]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [146, 205, 239, 413, 438, 526, 533, 790, 791, 952]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Toevoegen Bakelse Aa

polygon = aanvoergebieden_df.loc[["Bakelse Aa"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [487, 489, 745, 754, 755, 760, 1586, 1623]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [847, 905]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [156, 601, 2035]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [361, 352]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)
# %%
# Toevoegen Peelsche Loop

polygon = aanvoergebieden_df.loc[["Peelsche Loop"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [767, 1259]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [843, 960]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [227, 358, 535, 2031]
# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = []

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)
# %%
# Toevoegen Neerkant

polygon = aanvoergebieden_df.loc[["Neerkant"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [472, 704, 781, 1409, 1650]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [324]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [335]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = []

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)
# %%
# Toevoegen Someren

polygon = aanvoergebieden_df.loc[["Someren"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [327, 328, 384, 385]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = []

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [269, 886]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)
# %%
# Toevoegen Helmond

polygon = aanvoergebieden_df.loc[["Helmond"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [706, 968, 969, 1229, 1944]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [217, 294, 668]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = []

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [748]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Toevoegen Whkanaal (Wilhelminakanaal)

polygon = aanvoergebieden_df.loc[["Whkanaal"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [273, 274, 440, 605, 606, 607, 1518, 1648, 1674, 221, 638, 694, 1479]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [288, 469, 479, 632]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [510, 531, 935]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [218, 372, 569, 686]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Toevoegen Maaskant-Oost

polygon = aanvoergebieden_df.loc[["Maaskant-Oost"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    138,
    143,
    175,
    190,
    388,
    499,
    528,
    529,
    603,
    604,
    648,
    659,
    660,
    676,
    801,
    981,
    1254,
    1458,
    1459,
    1481,
    1498,
    1544,
    1570,
    1611,
    1660,
    1694,
    1930,
    1933,
]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [124, 697, 919, 920, 366, 452, 490, 615, 922, 194, 151, 182, 144, 509, 594, 667, 1014, 1016]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [203, 657, 241, 384, 179, 313, 286, 753, 850]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [
    698,
    863,
    864,
    362,
    494,
    915,
    755,
    271,
    866,
    867,
    868,
    755,
    990,
    380,
    849,
    859,
    865,
    1001,
    1018,
    1058,
]


# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Maaskant-West

polygon = aanvoergebieden_df.loc[["Maaskant-West"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [928, 930, 282, 2018]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [370, 430, 459, 96, 221, 923, 924, 2017]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [734, 1067]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [722, 723, 1070]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Toevoegen Maaskant-Midden

polygon = aanvoergebieden_df.loc[["Maaskant-Midden"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    408,
    60,
    237,
    258,
    362,
    376,
    420,
    654,
    656,
    657,
    994,
    997,
    1065,
    1435,
    1492,
    1530,
]

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [
    198,
    289,
    394,
    135,
    257,
    138,
    336,
    356,
    395,
    321,
    252,
    119,
    410,
    414,
    736,
    126,
    488,
    925,
    996,
    209,
    551,
    266,
    341,
]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [80, 160, 259, 308, 181, 985, 392, 215]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = []


# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# EXCLUDE NODES op 0 m3/s zetten
mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, "flow_rate"] = 0
model.outlet.static.df.loc[mask, "min_flow_rate"] = 0
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0


# %% add all remaining inlets/outlets
# add all remaing outlets
# handmatig opgegeven flow control nodes definieren

flow_control_nodes = [701, 822, 917, 916]

# handmatig opgegeven supply nodes (inlaten)
#


supply_nodes = [369, 406, 2020, 2022, 527, 1502]
#

drain_nodes = [120, 210, 281, 292, 309, 360, 470, 613, 571, 577, 691, 747, 777, 748, 891, 971]


# Flushing nodes
# flushing_nodes = {919: 5}
flushing_nodes = {}

# %% Toevoegen waar nog geen sturing is toegevoegd

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
    us_threshold_offset=LEVEL_DIFFERENCE_THRESHOLD,
    flushing_seasonal=FLUSHING_SEASONAL,
)

# %%
# Gemaal Veluwe
model.pump.static.df.loc[model.pump.static.df.node_id == 100, "max_flow_rate"] = 1.5

# Gemaal Mierlo heeft Manning knopen, haal min_upstream waterstand niet, geen aanvoer, daarom verlaagd
model.pump.static.df.loc[model.pump.static.df.node_id == 226, "min_upstream_level"] = 14.35

model.outlet.static.df.loc[model.outlet.static.df.node_id == 934, "max_downstream_level"] = 12
model.outlet.static.df.loc[model.outlet.static.df.node_id == 212, "max_downstream_level"] = 3.12
# Crevecoeur
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2018, "max_flow_rate"] = 300

# %% Junctionfy(!)
model = junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
model.solver.level_difference_threshold = LEVEL_DIFFERENCE_THRESHOLD

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime


# %%

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=1)
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

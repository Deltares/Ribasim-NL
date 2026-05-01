# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

# Globale settings

MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "DrentsOverijsselseDelta"
SHORT_NAME: str = "dod"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten

EXCLUDE_NODES = {}

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(r"DrentsOverijsselseDelta/verwerkt/sturing/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


# %%
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 30
model.outlet.static.df.flow_rate = 30
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate

# Outlet Wsterkamp node #1125 richting omdraaien, is inlaat
for link_id in [341, 2427]:
    model.reverse_link(link_id=link_id)

# Outlet node #603 richting omdraaien, is inlaat
for link_id in [1618, 305]:
    model.reverse_link(link_id=link_id)

# Outlet node #1291 richting omdraaien, is inlaat
for link_id in [3051, 2652]:
    model.reverse_link(link_id=link_id)


# Trambrug Outlet node #597 richting omdraaien, is inlaat
for link_id in [249, 1604]:
    model.reverse_link(link_id=link_id)

# Kloosterveen node #965 richting omdraaien, is inlaat
for link_id in [1041, 2280]:
    model.reverse_link(link_id=link_id)

# make outlets from manning
outlet_ids = [1417, 1419, 1448, 1488, 1466, 1459, 1522]

for node_id in dict.fromkeys(outlet_ids):
    model.update_node(node_id=node_id, node_type="Outlet")

# Outlet Zedemuden moet kunnen inlaten, dus richting omdraaien evt takken toevoegen
for link_id in [
    3087,
    3088,
]:
    model.reverse_link(link_id=link_id)


# Verwijderen nutteloze kunstwerken
model.remove_node(264, remove_links=True)
model.remove_node(1174, remove_links=True)
model.remove_node(1229, remove_links=True)
model.remove_node(266, remove_links=True)
model.remove_node(268, remove_links=True)

# Streefpeil te laag
model.outlet.static.df.loc[model.outlet.static.df.node_id == 959, "min_upstream_level"] = 11.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 971, "min_upstream_level"] = 11.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1238, "min_upstream_level"] = 11.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 544, "min_upstream_level"] = -0.2


# Manning moet outlet zijn
model.update_node(node_id=1468, node_type="Outlet")
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1468, "min_upstream_level"] = 2.63


# %%

# %%
# Toevoegen Ankersmit

polygon = aanvoergebieden_df.loc[["Ankersmit"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [169, 170]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [1188, 152, 1199]

# handmatig opgegeven flow_control_nodes
flow_control_nodes = [1267]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %%
# Toevoegen Westerveld

polygon = aanvoergebieden_df.loc[["Westerveld"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [181, 604]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow_control_nodes
flow_control_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %%
# Toevoegen Overijssels Kanaal

polygon = aanvoergebieden_df.loc[["Overijssels Kanaal"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [1057, 2480, 2685]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [552]

# handmatig opgegeven flow_control_nodes
flow_control_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %%
# Toevoegen Vecht Twentekanalen

polygon = aanvoergebieden_df.loc[["Vecht-Twentekanalen"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [1332, 1391]

# handmatig opgegeven flow_control_nodes
flow_control_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %%
# Toevoegen Dedemsvaart

polygon = aanvoergebieden_df.loc[["Dedemsvaart"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [1680]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [201, 771, 859, 1210]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow_control_nodes
flow_control_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %%
# Toevoegen IJsselmeergebied

polygon = aanvoergebieden_df.loc[["IJsselmeergebied"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow_control_nodes
flow_control_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %%

# Toevoegen Hoogeveense Vaart

polygon = aanvoergebieden_df.loc[["Hoogeveense Vaart"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2970]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow_control_nodes
flow_control_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %%
# Toevoegen Wold Aa

polygon = aanvoergebieden_df.loc[["Wold Aa"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [296, 333, 481, 986, 337, 398, 431, 486, 998, 1028, 1052]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [545, 660, 633, 693, 1047]

# handmatig opgegeven flow_control_nodes
flow_control_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %%

# Toevoegen Drentse Hoofdvaart

polygon = aanvoergebieden_df.loc[["Drentse Hoofdvaart"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [687]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [111, 122, 297, 422, 490, 352, 324, 468, 475, 1271, 421, 953, 1372, 1373]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [332, 1068, 1142, 1303, 1307]

# handmatig opgegeven flow_control_nodes
flow_control_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %%

# Toevoegen BoezemNW

polygon = aanvoergebieden_df.loc[["BoezemNW"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [671, 2246, 2247]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [375, 657, 679, 688, 690, 1279, 698]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [954, 972, 987, 1035, 1278, 1302, 1401, 3122]

# handmatig opgegeven flow_control_nodes
flow_control_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=True,
)

# %% add all remaining inlets/outlets

flow_control_nodes = [227, 1317, 2662, 2666, 2664]

supply_nodes = [
    1246,
    1238,
    896,
    686,
    652,
    464,
    196,
    753,
    213,
    217,
    968,
    823,
    571,
    562,
    1344,
    1209,
    760,
    832,
    1190,
    906,
    195,
    2665,
    648,
    726,
    2659,
    2658,
    704,
    548,
    216,
    1115,
    1187,
    1312,
    647,
    650,
    676,
    694,
    699,
    1009,
    1054,
    1074,
    1059,
    1077,
    1025,
    1024,
    1141,
    2667,
    640,
    579,
    563,
    1221,
    1056,
    2657,
    284,
    951,
    976,
    219,
    564,
    574,
    570,
    823,
    977,
    1217,
    346,
    846,
    334,
    758,
    759,
    1207,
    631,
    370,
    645,
    636,
    706,
    669,
    665,
    632,
    638,
    503,
    637,
    1034,
    2650,
    615,
    878,
    801,
    800,
    783,
    955,
    195,
    634,
    1132,
    708,
    949,
    965,
    1024,
    712,
    1050,
    969,
    1182,
    741,
    713,
    1012,
    936,
    1086,
    1231,
    1291,
    1260,
    1261,
    1262,
    1273,
    2653,
    1473,
    2652,
    1294,
    2662,
    933,
    288,
    3116,
    3125,
    3124,
    3126,
    3119,
    3118,
    660,
    1047,
    333,
    918,
    1078,
    715,
    723,
    1243,
    3110,
    212,
    597,
]


drain_nodes = [
    113,
    315,
    351,
    1368,
    304,
    1386,
    451,
    3430,
    361,
    486,
    3567,
    1268,
    680,
    265,
    3128,
    1318,
    1346,
    1293,
    1259,
    1225,
    1226,
    599,
    942,
    1360,
    841,
    958,
    311,
    1366,
    1140,
    630,
    649,
    643,
    1129,
    580,
    501,
    239,
    150,
    1186,
    596,
    587,
    1091,
    446,
    445,
    541,
    454,
    1219,
    1066,
    932,
    521,
    1482,
    362,
    534,
    1366,
    444,
    1096,
    1216,
    325,
    276,
    267,
    1234,
    265,
    565,
    1350,
    1175,
    624,
    1100,
    589,
    529,
    584,
    190,
    487,
    697,
    700,
    590,
    630,
    515,
    1406,
    550,
    671,
    646,
    664,
    935,
    116,
    215,
    322,
    406,
    469,
    534,
    1029,
    1101,
    1193,
    1103,
    1366,
    369,
    378,
    849,
    1096,
    674,
    672,
    387,
    616,
    1189,
    186,
    701,
    780,
    1125,
    1173,
    1410,
    1409,
]


# Flushing nodes
flushing_nodes = {}

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
)

# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path).run_all()

# %%

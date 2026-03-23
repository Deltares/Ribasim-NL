# %%


import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

# %%

# Globale settings

LEVEL_DIFFERENCE_THRESHOLD = 0.02  # sync model.solver.level_difference_threshold and control-settings
MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "limburg"
SHORT_NAME: str = "limburg"
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
aanvoergebieden_gpkg = cloud.joinpath(r"Limburg/verwerkt/sturing/aanvoergebieden.gpkg")

cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])

# %%#
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# Gemaal Helenavaart
model.pump.static.df.loc[model.pump.static.df.node_id == 590, "flow_rate"] = 0.35
# Gemaal Beringe
model.pump.static.df.loc[model.pump.static.df.node_id == 583, "flow_rate"] = 0.65

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 20
model.outlet.static.df.flow_rate = 20
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate


# %% fixes
model.update_node(node_id=1194, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=905, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=906, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1155, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1186, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=852, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1056, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=839, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1055, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1054, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=842, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=855, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=857, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=856, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1134, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=936, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1133, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1057, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=933, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=899, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1139, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1140, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1141, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=897, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=898, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=901, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=902, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1138, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1195, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1207, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1107, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=845, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1157, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1104, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1052, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=823, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1154, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=826, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1145, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=829, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=828, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=830, node_type="Outlet")  # wordt  Outlet
model.update_node(node_id=1053, node_type="Outlet")  # wordt  Outlet
# Node 1204 is een outlet
model.update_node(node_id=1204, node_type="Outlet")  # wordt  Outlet
# Node 932 is een outlet
model.update_node(node_id=932, node_type="Outlet")  # wordt  Outlet
# Node 834 is een outlet
model.update_node(node_id=834, node_type="Outlet")  # wordt  Outlet
# Node 832 is een outlet
model.update_node(node_id=832, node_type="Outlet")  # wordt  Outlet
# Node 1120 is een outlet
model.update_node(node_id=1120, node_type="Outlet")  # wordt  Outlet
# Node 1201 is een outlet
model.update_node(node_id=1201, node_type="Outlet")  # wordt  Outlet
# Node 1203 is een outlet
model.update_node(node_id=1203, node_type="Outlet")  # wordt  Outlet
# Node 1146 is een outlet
model.update_node(node_id=1146, node_type="Outlet")  # wordt  Outlet
# Node 817 is een outlet
model.update_node(node_id=817, node_type="Outlet")  # wordt  Outlet
# Node 821 is een outlet
model.update_node(node_id=821, node_type="Outlet")  # wordt  Outlet
# Node 1165 is een outlet
model.update_node(node_id=1165, node_type="Outlet")  # wordt  Outlet
# Node 217 is een outlet
model.update_node(node_id=217, node_type="Outlet")  # wordt  Outlet
# Node 1166 is een outlet
model.update_node(node_id=1166, node_type="Outlet")  # wordt  Outlet
# Node 813 is een outlet
model.update_node(node_id=813, node_type="Outlet")  # wordt  Outlet
# Node 818 is een outlet
model.update_node(node_id=818, node_type="Outlet")  # wordt  Outlet
# Node 825 is een outlet
model.update_node(node_id=825, node_type="Outlet")  # wordt  Outlet
# Node 1213 is een outlet
model.update_node(node_id=1213, node_type="Outlet")  # wordt  Outlet
# Node 1217 is een outlet
model.update_node(node_id=1217, node_type="Outlet")  # wordt  Outlet
# Node 1216 is een outlet
model.update_node(node_id=1216, node_type="Outlet")  # wordt  Outlet
# Node 1066 is een outlet
model.update_node(node_id=1066, node_type="Outlet")  # wordt  Outlet
# Node 1102 is een outlet
model.update_node(node_id=1102, node_type="Outlet")  # wordt  Outlet
# Node 1143 is een outlet
model.update_node(node_id=1143, node_type="Outlet")  # wordt  Outlet
# Node 1158 is een outlet
model.update_node(node_id=1158, node_type="Outlet")  # wordt  Outlet
# Node 1159 is een outlet
model.update_node(node_id=1159, node_type="Outlet")  # wordt  Outlet
# Node 1205 is een outlet
model.update_node(node_id=1205, node_type="Outlet")  # wordt  Outlet
# Node 1206 is een outlet
model.update_node(node_id=1206, node_type="Outlet")  # wordt  Outlet
# Node 1208 is een outlet
model.update_node(node_id=1208, node_type="Outlet")  # wordt  Outlet
# Node 1209 is een outlet
model.update_node(node_id=1209, node_type="Outlet")  # wordt  Outlet
# Node 1210 is een outlet
model.update_node(node_id=1210, node_type="Outlet")  # wordt  Outlet
# Node 1211 is een outlet
model.update_node(node_id=1211, node_type="Outlet")  # wordt  Outlet
# Node 1214 is een outlet
model.update_node(node_id=1214, node_type="Outlet")  # wordt  Outlet
# Node 1215 is een outlet
model.update_node(node_id=1215, node_type="Outlet")  # wordt  Outlet
# Node 805 is een outlet
model.update_node(node_id=805, node_type="Outlet")  # wordt  Outlet
# Node 806 is een outlet
model.update_node(node_id=806, node_type="Outlet")  # wordt  Outlet
# Node 819 is een outlet
model.update_node(node_id=819, node_type="Outlet")  # wordt  Outlet
# Node 820 is een outlet
model.update_node(node_id=820, node_type="Outlet")  # wordt  Outlet
# Node 827 is een outlet
model.update_node(node_id=827, node_type="Outlet")  # wordt  Outlet
# Node 1243 is een outlet
model.update_node(node_id=1243, node_type="Outlet")  # wordt  Outlet
# Node 1244 is een outlet
model.update_node(node_id=1244, node_type="Outlet")  # wordt  Outlet
# Node 1244 is een outlet
model.update_node(node_id=1179, node_type="Outlet")  # wordt  Outlet
# Node 1244 is een outlet
model.update_node(node_id=904, node_type="Outlet")  # wordt  Outlet
# Node 1193 is een outlet
model.update_node(node_id=1193, node_type="Outlet")  # wordt  Outlet

model.merge_basins(basin_id=2334, to_basin_id=1763, are_connected=True)
model.merge_basins(basin_id=2461, to_basin_id=2070, are_connected=True)
model.merge_basins(basin_id=2070, to_basin_id=2360, are_connected=True)
model.merge_basins(basin_id=1885, to_basin_id=2360, are_connected=True)
model.merge_basins(basin_id=2205, to_basin_id=2144, are_connected=True)
model.merge_basins(basin_id=1387, to_basin_id=1941, are_connected=True)
model.merge_basins(basin_id=1983, to_basin_id=2053, are_connected=True)
model.merge_basins(basin_id=2181, to_basin_id=1582, are_connected=True)
model.merge_basins(basin_id=1575, to_basin_id=1809, are_connected=True)
model.merge_basins(basin_id=1481, to_basin_id=2316, are_connected=True)
model.merge_basins(basin_id=1972, to_basin_id=1697, are_connected=True)
model.merge_basins(basin_id=2163, to_basin_id=1658, are_connected=True)

model.remove_node(1314, remove_links=True)
model.remove_node(611, remove_links=True)
model.remove_node(1118, remove_links=True)
model.remove_node(1173, remove_links=True)


# %% Toevoegen Peelkanaal

polygon = aanvoergebieden_df.loc[["Peelkanaal"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [529]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# 1107:ST0826
flow_control_nodes = [1241]

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
    add_supply_nodes=True,
)

# %% Toevoegen Helenavaart

polygon = aanvoergebieden_df.loc[["Helenavaart"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    17,
    28,
    62,
    64,
    66,
    306,
    420,
    767,
    774,
    776,
    778,
    985,
    1036,
    1567,
    1718,
    1719,
    1727,
    1728,
    1732,
    1734,
    1979,
    1992,
    2023,
]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [160, 163, 165, 166, 167, 447, 494, 183, 834, 828, 829, 1054, 1104, 1120, 2499, 2500]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [526, 604, 757]

# 1107:ST0826
flow_control_nodes = [523, 827]

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
    add_supply_nodes=True,
)

# %% Toevoegen Beringe

polygon = aanvoergebieden_df.loc[["Beringe"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [82, 89, 92, 782, 1740, 1741, 1755, 1840, 1939, 2003]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [180, 181, 195, 196, 200, 253, 254, 255, 261, 408, 271]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [583, 709]

# 1107:ST0826
flow_control_nodes = [
    176,
    188,
    191,
    192,
    198,
    197,
    256,
    262,
    480,
    496,
    536,
    541,
    657,
    708,
    710,
    725,
    751,
    586,
    933,
    1055,
    1057,
    1133,
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
    add_supply_nodes=True,
)

# %% Toevoegen Rogge

polygon = aanvoergebieden_df.loc[["Rogge"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [155, 157, 709, 710, 711, 1812, 1813]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# 1107:ST0826
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
    add_supply_nodes=True,
)

# %% Tungelroysche beek

polygon = aanvoergebieden_df.loc[["Tungelroysche Beek"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [424, 709, 710, 711, 972, 1802, 1803, 1812, 1813, 1991, 2380, 2381, 2382]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# 1107:ST0826
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
    add_supply_nodes=True,
)

# %% Eendlossing

polygon = aanvoergebieden_df.loc[["Eendlossing"], "geometry"].union_all()

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

# 1107:ST0826
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
    add_supply_nodes=True,
)

# %% Oude Graaf

polygon = aanvoergebieden_df.loc[["Oude Graaf"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [822, 823, 824, 961, 962, 2009, 2010]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# 1107:ST0826
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
    add_supply_nodes=True,
)

# %% Nederweert Hoverlossing

polygon = aanvoergebieden_df.loc[["Nederweert Hoverlossing"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2011]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# 1107:ST0826
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
    add_supply_nodes=True,
)


# %% add all remaining inlets/outlets
# add all remaing outlets
# handmatig opgegeven flow control nodes definieren


flow_control_nodes = [220, 545, 471, 711]

# handmatig opgegeven supply nodes (inlaten)
#


supply_nodes = []
#

drain_nodes = [463, 692, 902, 936, 855, 856, 857]


# Flushing nodes
# flushing_nodes = {919: 5}
flushing_nodes = {}


add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
    us_threshold_offset=LEVEL_DIFFERENCE_THRESHOLD,
)

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

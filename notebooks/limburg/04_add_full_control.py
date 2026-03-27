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

# alle uitlaten en inlaten op 20m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 20
model.outlet.static.df.flow_rate = 20
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate
# %% fixes


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
supply_nodes = [526, 590, 604, 757]

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


supply_nodes = [238, 351, 411, 598, 731, 772, 1119, 2501, 2503]
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

# %% fixes inlaten max flow
# flow rates WATAK
flow_updates = {
    772: 0.02,  # Eendlossing
    583: 0.25,  # Gemaal Beringe
    590: 0.35,  # Gemaal Helenaveen (geen match → laten staan)
    351: 0.05,  # Houtstraatlossing
    2502: 0.05,  # Hushoverbeek
    604: 0.35,  # Inlaat Evertsoord (geen match)
    236: 0.09,  # Inlaat Groote Moost
    1223: 0.03,  # Inlaat Hulsenlossing
    410: 0.02,  # Inlaat Kleine Moost (Achterste Moost?)
    599: 0.12,  # Inlaat Rietbeek
    731: 0.125,  # Inlaat Weteringbeek
    2503: 0.05,  # Kampershoek
    238: 4.7,  # Katsberg opgehoogd met 2.3m3/s doordat Noordervaart naar van 3.8 naar 5.4m3/s
    1119: 0.08,  # Klein Leukerbeek
    534: 0.02,  # Nederweerter Hovenlossing
    464: 0.05,  # Nederweerter Riet
    1136: 0.05,  # Oude Graaf
    2501: 0.01,  # Roeven
    411: 0.05,  # Snepheiderbeek Inschatting
    773: 4.7,  # Verdeelwerk de Halte (geen match)
    598: 0.01,  # Waatskampoplossing
    230: 0.05,  # Zwartwaterlossing (geen match)
}

mask = model.outlet.static.df.node_id.isin(flow_updates.keys()) & (model.outlet.static.df["control_state"] == "aanvoer")

model.outlet.static.df.loc[mask, "max_flow_rate"] = model.outlet.static.df.loc[mask, "node_id"].map(flow_updates)

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

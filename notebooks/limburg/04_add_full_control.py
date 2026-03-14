# %%


import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import (
    add_controllers_to_supply_area,
)
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

cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])

# %%#
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 30
model.outlet.static.df.flow_rate = 30
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate
# %% Toevoegen

polygon = aanvoergebieden_df.loc[["Kromme Rijn/ARK"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2175]

# doorspoeling (op uitlaten)
# 41: Spijksterpompen

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# 864: Achterrijn Stuw
# 893: ST6050 2E Veld
# 969: ST0842 Trechtweg
# 971: ST0010
# 1126: Pelikaan
# 1145: ST003 Eindstuw Raaphofwetering
# 1168: ST0733
# 1223: ST0815
# 2110:
# 851: ST0014 Koppeldijk stuw
# 923: ST1264 Hevelstuw Ravensewetering
drain_nodes = [554, 851, 864, 893, 969, 971, 1126, 1145, 1168, 1223, 2110]

# handmatig opgegeven supply nodes (inlaten)
# 554: G0007 Koppeldijk gemaal
# 589: Mastwetering
# 624: G4481 Pelikaan
# 851: ST0014 Koppeldijk stuw
# 648: G3007 Trechtweg
# 649: Voorhavendijk

supply_nodes = [554, 589, 624, 648, 649]

# 1107:ST0826
flow_control_nodes = [1107]

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


# %% fixes
# Peelkanaal open verbinding
model.update_node(node_id=251, node_type="ManningResistance")  # wordt  Manning
# Node 1165 is een stuw
model.update_node(node_id=1156, node_type="Outlet")  # wordt  Outlet
# Node 217 is een stuw
model.update_node(node_id=217, node_type="Outlet")  # wordt  Outlet
# Node 1166 is een stuw
model.update_node(node_id=1166, node_type="Outlet")  # wordt  Outlet
# Node 813 is een stuw
model.update_node(node_id=813, node_type="Outlet")  # wordt  Outlet
# Node 1213 is een stuw
model.update_node(node_id=1213, node_type="Outlet")  # wordt  Outlet
# Node 1066 is een stuw
model.update_node(node_id=1066, node_type="Outlet")  # wordt  Outlet
# Node 1143 is een stuw
model.update_node(node_id=1143, node_type="Outlet")  # wordt  Outlet
# Node 1158 is een stuw
model.update_node(node_id=1158, node_type="Outlet")  # wordt  Outlet
# Node 1159 is een stuw
model.update_node(node_id=1159, node_type="Outlet")  # wordt  Outlet
# Node 1205 is een stuw
model.update_node(node_id=1205, node_type="Outlet")  # wordt  Outlet
# Node 1208 is een stuw
model.update_node(node_id=1208, node_type="Outlet")  # wordt  Outlet
# Node 1209 is een stuw
model.update_node(node_id=1209, node_type="Outlet")  # wordt  Outlet
# Node 1210 is een stuw
model.update_node(node_id=1210, node_type="Outlet")  # wordt  Outlet
# Node 1211 is een stuw
model.update_node(node_id=1211, node_type="Outlet")  # wordt  Outlet
# Node 820 is een stuw
model.update_node(node_id=820, node_type="Outlet")  # wordt  Outlet
# Node 827 is een stuw
model.update_node(node_id=827, node_type="Outlet")  # wordt  Outlet
# Node 1243 is een stuw
model.update_node(node_id=1243, node_type="Outlet")  # wordt  Outlet
# Node 1244 is een stuw
model.update_node(node_id=1244, node_type="Outlet")  # wordt  Outlet

model.merge_basins(basin_id=2461, to_basin_id=2070, are_connected=True)
model.merge_basins(basin_id=2070, to_basin_id=2360, are_connected=True)
model.merge_basins(basin_id=1885, to_basin_id=2360, are_connected=True)
model.merge_basins(basin_id=2205, to_basin_id=2144, are_connected=True)
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

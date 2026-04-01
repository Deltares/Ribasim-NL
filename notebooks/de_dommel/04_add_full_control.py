# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

LEVEL_DIFFERENCE_THRESHOLD = 0.02  # sync model.solver.level_difference_threshold and control-settings
MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "DeDommel"  # authority
SHORT_NAME: str = "dommel"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {}
EXCLUDE_SUPPLY_NODES = []

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")

aanvoergebieden_gpkg = cloud.joinpath(r"DeDommel/verwerkt/sturing/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[qlr_path, aanvoergebieden_gpkg])


# %%
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 30
model.outlet.static.df.flow_rate = 30
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate

# %% Fixes

outlet_ids = [968, 754, 705, 709, 710, 923, 753, 649, 766, 926]

for node_id in dict.fromkeys(outlet_ids):
    model.update_node(node_id=node_id, node_type="Outlet")

# %%
# Toevoegen Son en Breugel

polygon = aanvoergebieden_df.loc[["Son en Breugel"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [641, 649, 1089, 1392, 1875]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [419, 753, 766]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow_control_nodes
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
# Toevoegen Olen

polygon = aanvoergebieden_df.loc[["Olen"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [336, 349, 350, 381, 855, 856, 1548, 1700, 1854]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [506, 507, 509, 510, 602, 754]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [416, 923]

# handmatig opgegeven flow_control_nodes
flow_control_nodes = [212, 216, 217, 227, 236]

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
# Toevoegen Bocholt

polygon = aanvoergebieden_df.loc[["Bocholt"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [794, 836, 1660, 1661, 1934, 1973]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [705, 709, 710]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [369, 370, 590]

# handmatig opgegeven flow_control_nodes
flow_control_nodes = [375]

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


# %% add all remaining inlets/outlets

flow_control_nodes = [203]

supply_nodes = [405, 417, 1912]


drain_nodes = [210, 926]


# Flushing nodes
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
)


# %% Junctionfy(!)
# model = junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
model.solver.level_difference_threshold = LEVEL_DIFFERENCE_THRESHOLD

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# %% Verdeelwerk bij Eindhoven 10% afvgevoerd via Beatrixkanaal (max 20m3/s) en 90% via Dieze
model.outlet.static.df.loc[model.outlet.static.df.node_id == 379, "max_flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 293, "min_upstream_level"] = 16.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 210, "min_upstream_level"] = 16.2

# flow rates WATAK Olen en Sonse Heide
flow_updates = {
    417: 0.6,  # Olen
    405: 0.16,  # Sonse Heide
    1912: 0.25,  # Bocholt naar Herentals, max aanvoer??, niet in WATAK
}

mask = model.outlet.static.df.node_id.isin(flow_updates.keys()) & (model.outlet.static.df["control_state"] == "aanvoer")

model.outlet.static.df.loc[mask, "max_flow_rate"] = model.outlet.static.df.loc[mask, "node_id"].map(flow_updates)

# flow rates WATAK Olen en Sonse Heide
flow_updates = {
    121: 200,  # Vughterstuw
    210: 20,  # Blaatthem
    293: 200,  # Verdeelwerk
}

mask = model.outlet.static.df.node_id.isin(flow_updates.keys()) & (model.outlet.static.df["control_state"] == "afvoer")

model.outlet.static.df.loc[mask, "max_flow_rate"] = model.outlet.static.df.loc[mask, "node_id"].map(flow_updates)


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

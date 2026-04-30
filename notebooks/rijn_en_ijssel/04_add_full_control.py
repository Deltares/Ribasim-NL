# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "RijnenIJssel"  # authority
SHORT_NAME: str = "wrij"  # short_name used in toml-file
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
aanvoergebieden_gpkg = cloud.joinpath(rf"{AUTHORITY}/verwerkt/sturing/aanvoergebieden.gpkg")
# cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


# %%
# Read data
model = Model.read(ribasim_toml)


# markeer inlaten
model.node.df[IS_SUPPLY_NODE_COLUMN] = False
mask = model.node.df["name"].str.contains("inlaat", case=False, na=False)
model.node.df.loc[mask, IS_SUPPLY_NODE_COLUMN] = True


aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


# %%
# Schipbeek

polygon = aanvoergebieden_df.loc[["Schipbeek"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [59]
# 59: Noodoverloop Schipbeek

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [654, 479]
# 654: Gemaal Schipbeek (inlaat Twentekanaal)
# 479: AF96440002, inlaatduiker (?)

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=[306],
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Herkel

polygon = aanvoergebieden_df.loc[["Herkel"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
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
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Lochem

polygon = aanvoergebieden_df.loc[["Lochem"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
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
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# En de rest toevoegen

supply_nodes = [437, 438]
# 437 en 438, voor nu inlaten; verifieren bij Waterschap

add_controllers_to_uncontrolled_connector_nodes(
    model=model, exclude_nodes=list(EXCLUDE_NODES), supply_nodes=supply_nodes
)


# %% Junctionfy!
junctionify(model)

# %%
# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=0.1)
model.write(ribasim_toml_dry)

# run hoofdmodel
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml_dry, qlr_path=qlr_path).run_all()
    model = Model.read(ribasim_toml_dry)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model.write(ribasim_toml_wet)

# run prerun model
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml_wet, qlr_path=qlr_path).run_all()
    model = Model.read(ribasim_toml_wet)

# hoofd run
update_basin_static(model=model, precipitation_mm_per_day=1.5)
model.write(ribasim_toml)
# run hoofdmodel
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path).run_all()

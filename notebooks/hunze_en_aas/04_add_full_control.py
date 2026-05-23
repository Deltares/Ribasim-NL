# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "HunzeenAas"  # authority
SHORT_NAME: str = "hea"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {
    152,  # Dorkswerdersluis (Scheepvaart)
    161,  # Bulsterverlaat (Scheepvaart)
    174,  # Koppelsluis (Scheepvaart)
    183,  # Haansluis (scheepvaart)
    156,  # Vriescheloostersluis (Veelerveen)
}

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(rf"{AUTHORITY}/verwerkt/sturing/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


# %%
# Read data
model = Model.read(ribasim_toml)

# fixes (zo snel mogelijk)
remove_nodes = [152, 534, 678]
# 152: Dorkwerdersluis (scheepvaart)
# 534: Vispassage "de Bult"
# 678: Koker bij gemaal westerpolder
for node_id in remove_nodes:
    model.remove_node(node_id=node_id, remove_links=True)

model.merge_basins(node_id=1908, to_node_id=1372, are_connected=True)
model.merge_basins(node_id=1763, to_node_id=1381, are_connected=False)
model.reverse_direction_at_node(node_id=871)  # inlaat stond verkeerde kant op
model.reverse_direction_at_node(node_id=691)  # inlaat stond verkeerde kant op
model.update_node(
    node_id=166, node_type="ManningResistance"
)  # Grote Slapersluis; hier wordt ook water ingelaten; hopelijk lukt dit met ManningResistance
model.update_node(node_id=632, node_type="Pump")  # Inlaat Vestdijklaan lijkt een pomp(je) te zijn
model.update_node(node_id=1111, node_type="Outlet")  # Manning tussen basins met verschillende streefpeilen gaat niet
model.update_node(node_id=854, node_type="Pump")  # Inlaat Vestdijklaan lijkt een pomp(je) te zijn
model.update_node(node_id=854, node_type="Outlet")  # Uitlaat Nijlandsloop naar Anreperdiep

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 30.0
model.outlet.static.df.flow_rate = 30.0
model.pump.static.df.flow_rate = 50.0
model.pump.static.df.max_flow_rate = 50.0
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0.0
model.pump.static.df.loc[model.pump.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0.0


# capaciteit inlaten/doorlaten
model.pump.static.df.loc[model.pump.static.df.node_id == 20, "flow_rate"] = 20  # Aanvoergemaal Dorkwerd
model.pump.static.df.loc[model.pump.static.df.node_id == 972, "flow_rate"] = 7.5  # Aanvoergemaal Küpers
model.pump.static.df.loc[model.pump.static.df.node_id == 70, "flow_rate"] = 4.2  # Aanvoergemaal Vennix
model.pump.static.df.loc[model.pump.static.df.node_id == 107, "flow_rate"] = 1.92  # Aanvoergemaal Ter Apelkanaal
model.pump.static.df.loc[model.pump.static.df.node_id == 330, "flow_rate"] = (
    7.5  # De Bult (Afvoer-capaciteit gelijk aan Küpers!)
)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 767, "flow_rate"] = 3.6  # Inlaat Purit
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2014, "flow_rate"] = 0.3  # Inlaat Verl. Hoogeveense Vaart
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2011, "flow_rate"] = 0.3  # Inlaat Verl. Hoogeveense Vaart
model.outlet.static.df.loc[model.outlet.static.df.node_id == 847, "flow_rate"] = 0.3  # Inlaat Barriereweg

model.node.df[IS_SUPPLY_NODE_COLUMN] = False
mask = model.node.df["meta_code_waterbeheerder"].str.contains("KIN-", case=False, na=False)
model.node.df.loc[mask, IS_SUPPLY_NODE_COLUMN] = True

# user-defined drain_nodes
drain_nodes = [
    59,
    62,
    80,
    88,
    99,
    133,
    135,
    179,
    196,
    206,
    231,
    233,
    250,
    256,
    285,
    316,
    321,
    325,
    352,
    369,
    425,
    431,
    507,
    514,
    524,
    558,
    792,
    892,
    1046,
    79,
    297,
    30,
    447,
]
# user-defined supply_nodes
supply_nodes = [20, 62, 70, 107, 573, 972]
# user-defined flow_control_nodes
flow_control_nodes = [182, 330, 573, 634, 165, 505, 1017, 1111]


# markeer inlaten
model.node.df[IS_SUPPLY_NODE_COLUMN] = False
mask = model.node.df["meta_code_waterbeheerder"].str.contains("KIN-", case=False, na=False)
model.node.df.loc[mask, IS_SUPPLY_NODE_COLUMN] = True
model.node.df.loc[drain_nodes + flow_control_nodes, IS_SUPPLY_NODE_COLUMN] = False

# %%
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True)
conflicting_node_ids = aanvoergebieden_df.groupby("node_id")["aanvoergebied"].nunique()
conflicting_node_ids = conflicting_node_ids[conflicting_node_ids > 1].index
if len(conflicting_node_ids) > 0:
    conflicts = aanvoergebieden_df.loc[
        aanvoergebieden_df["node_id"].isin(conflicting_node_ids), ["node_id", "aanvoergebied"]
    ].sort_values(["node_id", "aanvoergebied"])
    raise ValueError(f"node_id values linked to multiple aanvoergebieden found:\n{conflicts.to_string(index=True)}")
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


# %%
# Hoogeveense Vaart

polygon = aanvoergebieden_df.loc[["Verl. Hoogeveense Vaart"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Borgerzijtak

polygon = aanvoergebieden_df.loc[["Borgerzijtak"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2719]
# 2453: Sifon onder Winschoterdiep

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Oldambt

polygon = aanvoergebieden_df.loc[["Oldambt"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2453]
# 2453: Sifon onder Winschoterdiep

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# TAK

polygon = aanvoergebieden_df.loc[["TAK"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [3650]
# 3650: Sifon onder Ter Apelkanaal

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Fiemel/Westerwolde

polygon = aanvoergebieden_df.loc[["Fiemel/Westerwolde"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [297, 1941]
# 3650: Sifon onder Ter Apelkanaal

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Küpers

polygon = aanvoergebieden_df.loc[["Küpers"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [235, 2776, 2778]
# 235: Manning richting Winschoterdiep
# 2776: Manning richting Wildervanckkanaal
# 2778: Manning richting Pekel Aa

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Vennix

polygon = aanvoergebieden_df.loc[["Vennix"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [3650]
# 3650: Sifon onder Ter Apelkanaal

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Drentse Aa

polygon = aanvoergebieden_df.loc[["Drentse Aa"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2751]
# ###: beschrijving

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Zuidlaardermeer/Winschoterdiep

polygon = aanvoergebieden_df.loc[["Zuidlaardermeer/Winschoterdiep"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2286, 2384, 2607]
# 2286: Manning Drentsche Diep
# 2384: Manning Winschoterdiep
# 2607: Manning Winschoterdiep

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Duurswold

node_ids = [829, 822, 810, 190, 162]
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "flow_rate"] = (
    0  # geen aanvoer vanuit de Eems (i.v.m. zout)
)

polygon = aanvoergebieden_df.loc[["Duurswold"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [747]
# 747: sifon onder Eemskanaal, dus helemaal prima

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# En de rest toevoegen


add_controllers_to_uncontrolled_connector_nodes(
    model=model, exclude_nodes=list(EXCLUDE_NODES), supply_nodes=supply_nodes
)

# Afvoer: hardcoded default 20 m3/s ophogen naar 100 m3/s voor uitlaten/doorlaten.
# Handmatig opgegeven capaciteiten blijven ongemoeid.
for static_df, manual_capacity_nodes in [
    (model.outlet.static.df, globals().get("outlet_max_flow_rate_by_node_id", {})),
    (model.pump.static.df, globals().get("pump_max_flow_rate_by_node_id", {})),
]:
    if "control_state" not in static_df.columns:
        continue
    afvoer_mask = (static_df.control_state == "afvoer") & ~static_df.node_id.isin(manual_capacity_nodes)
    for column in ["flow_rate", "max_flow_rate"]:
        default_capacity_mask = afvoer_mask & (static_df[column] == 20.0)
        static_df.loc[default_capacity_mask, column] = 100.0

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

# %%

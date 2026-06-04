# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import (
    add_controllers_to_supply_area as _add_controllers_to_supply_area,
)
from ribasim_nl.control import (
    add_controllers_to_uncontrolled_connector_nodes as _add_controllers_to_uncontrolled_connector_nodes,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from ribasim_nl.parametrization.manning_level import sync_basin_levels_along_manning_routes

from ribasim_nl import CloudStorage, Model


def _supply_flow_rate_by_node_id():
    return globals().get("outlet_max_flow_rate_by_node_id", {}) | globals().get("pump_max_flow_rate_by_node_id", {})


def add_controllers_to_supply_area(*args, **kwargs):
    kwargs.setdefault("supply_flow_rate", _supply_flow_rate_by_node_id())
    kwargs.setdefault("drain_flow_rate", _supply_flow_rate_by_node_id())
    return _add_controllers_to_supply_area(*args, **kwargs)


def add_controllers_to_uncontrolled_connector_nodes(*args, **kwargs):
    kwargs.setdefault("supply_flow_rate", _supply_flow_rate_by_node_id())
    kwargs.setdefault("drain_flow_rate", _supply_flow_rate_by_node_id())
    return _add_controllers_to_uncontrolled_connector_nodes(*args, **kwargs)


def set_flow_rate(static_df, max_flow_rate_by_node_id: dict[int, float], flow_rates: list[str] | None = None) -> None:
    max_flow_rate = static_df["node_id"].map(max_flow_rate_by_node_id)
    mask = max_flow_rate.notna()
    flow_rates = ["flow_rate", "max_flow_rate"] if flow_rates is None else flow_rates

    for flow_rate in flow_rates:
        static_df.loc[mask, flow_rate] = max_flow_rate[mask]


# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "RijnenIJssel"  # authority
SHORT_NAME: str = "wrij"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"
drain_nodes = [59]
supply_nodes = [654, 479, 437, 438, 677, 409, 1190, 390, 428]
flushing_nodes = {
    320: 3,
    337: 0.9,
    161: 0.1,
    349: 0.2,  # 0.1-0.3 m3/s volgens waterschap
}
flow_control_nodes = [306]

outlet_max_flow_rate_by_node_id = {
    120: 1.3,  # Lochem
    90: 0.6,  # Herkel
}
pump_max_flow_rate_by_node_id = {
    654: 1.4,  # Schipbeek
}

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {}

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

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 30.0
model.outlet.static.df.flow_rate = 30.0
model.pump.static.df.max_flow_rate = 30.0


outlet_max_flow_rate_by_node_id = {
    654: 1.4,  # Schipbeek
    120: 1.3,  # Lochem
    90: 0.6,  # Herkel
    437: 0.05,  # Sifon Boven-Slinge
    438: 0.05,  # Sifon Boven-Slinge
    63: 48,  # Haarlo -> Bolksbeek (Capaciteit uit DM, 40% van totaal)
    581: 72,  # Haarlo -> Berkel (60% van totaal)
    117: 88,  # Aflaatwerk Lochem
    320: 22,  # Verdeelwerk Lochem Berkel
    337: 0.9,  # Warken Berkel (Zutphen)
    161: 0.1,  # Warken Brummeler Laak (Gemaal Helbergen)
    121: 110,  # Eefde aflaatwerk
    59: 8,  # Noodoverloop Schipbeek Twentekanaal (uit waterakkoord Twentekanalen)
    71: 8.87,  # Capaciteit volgens DM rapportage
    555: 5.3,  # Capaciteit volgens DM rapportage
    623: 10.3,  # Capaciteit volgens DM rapportage
    1192: 5.2,  # Capaciteit volgens DM rapportage
    560: 1.0,  # Capaciteit volgens DM rapportage
}

# capaciteit inlaten
set_flow_rate(static_df=model.outlet.static.df, max_flow_rate_by_node_id=outlet_max_flow_rate_by_node_id)

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
# Herkel

polygon = aanvoergebieden_df.loc[["Herkel"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

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
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Lochem

polygon = aanvoergebieden_df.loc[["Lochem"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

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
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# En de rest toevoegen


add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    exclude_nodes=list(EXCLUDE_NODES),
    supply_nodes=supply_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
)

# %%
# Corrigeer basin-peilen/profielen langs open Manning-routes nadat alle full-control-controllers bekend zijn.
manning_level_updates = sync_basin_levels_along_manning_routes(
    model=model,
    output_path=cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", "manning_level_updates.csv"),
    basin_output_gpkg=cloud.joinpath(
        AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", "manning_level_basin_updates.gpkg"
    ),
    control_output_gpkg=cloud.joinpath(
        AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", "manning_level_control_updates.gpkg"
    ),
    protected_basin_node_ids=[777, 793, 857, 1068, 1085],
)

# %%

# Lochem aflaatwerk limiteren tot 32 m3/s
mask = (model.outlet.static.df.node_id == 117) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[mask, "max_flow_rate"] = 32

# Verdeelwerk Lochem Berkel limiteren tot 3 m3/s in aanvoerstand.
mask = (model.outlet.static.df.node_id == 320) & (model.outlet.static.df.control_state == "aanvoer")
model.outlet.static.df.loc[mask, "max_flow_rate"] = 3.1

# Controller 1330: ook bij truth_state FF naar aanvoer.
mask = (model.discrete_control.logic.df.node_id == 1330) & (model.discrete_control.logic.df.truth_state == "FF")
model.discrete_control.logic.df.loc[mask, "control_state"] = "aanvoer"

# Noodoverloop Twentekanaal pas bij onvoldoende door sifon (node_id 306)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 59, "min_upstream_level"] += 0.1

# doorlaten
min_flow_rates = {
    320: 3,
    161: 0.1,
    337: 0.9,
}

set_flow_rate(model.outlet.static.df, max_flow_rate_by_node_id=min_flow_rates, flow_rates=["min_flow_rate"])

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

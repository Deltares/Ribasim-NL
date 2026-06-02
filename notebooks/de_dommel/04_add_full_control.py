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
from shapely.geometry import MultiPolygon

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


# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "DeDommel"  # authority
SHORT_NAME: str = "dommel"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {}
EXCLUDE_SUPPLY_NODES = []

# Handmatige inlaatcapaciteiten uit WATAK.
outlet_max_flow_rate_by_node_id = {
    417: 0.6,  # Olen
    405: 0.16,  # Sonse Heide
    1912: 0.25,  # Bocholt naar Herentals, max aanvoer??, niet in WATAK
}

# Handmatige indeling control-, supply- en drain-nodes.
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [203, 212, 216, 217, 227, 236, 375]

supply_nodes = [369, 370, 405, 416, 417, 590, 923, 1067, 1912]

drain_nodes = [210, 419, 506, 507, 509, 510, 602, 611, 705, 709, 710, 753, 754, 766, 926, 967]

flushing_nodes = {}

SUPPLY_AREA_IGNORE_LINKS = {
    "Son en Breugel": [641, 649, 1089, 1392, 1875],
    "Olen": [336, 349, 350, 381, 855, 856, 1548, 1700, 1854],
    "Beekloop": [794, 836, 1660, 1661, 1934, 1973],
}
# fmt: on


# %%
# Helpers


def supply_area_polygon(aanvoergebieden_df: gpd.GeoDataFrame, area_name: str):
    polygon = aanvoergebieden_df.loc[[area_name], "geometry"].union_all()
    polygon = polygon.buffer(0).buffer(0)

    if isinstance(polygon, MultiPolygon):
        polygon = max(polygon.geoms, key=lambda g: g.area)

    return polygon


def add_supply_area_control(
    model: Model,
    aanvoergebieden_df: gpd.GeoDataFrame,
    area_name: str,
    ignore_intersecting_links: list[int],
    supply_nodes: list[int],
    drain_nodes: list[int],
    flow_control_nodes: list[int],
) -> None:
    add_controllers_to_supply_area(
        model=model,
        polygon=supply_area_polygon(aanvoergebieden_df, area_name),
        exclude_nodes=EXCLUDE_NODES,
        ignore_intersecting_links=ignore_intersecting_links,
        drain_nodes=drain_nodes,
        flushing_nodes={},
        supply_nodes=supply_nodes,
        flow_control_nodes=flow_control_nodes,
        control_node_types=CONTROL_NODE_TYPES,
    )


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
# Toevoegen aanvoergebieden

for area_name, ignore_intersecting_links in SUPPLY_AREA_IGNORE_LINKS.items():
    print(f"Toevoegen {area_name}")
    add_supply_area_control(
        model=model,
        aanvoergebieden_df=aanvoergebieden_df,
        area_name=area_name,
        ignore_intersecting_links=ignore_intersecting_links,
        drain_nodes=drain_nodes,
        supply_nodes=supply_nodes,
        flow_control_nodes=flow_control_nodes,
    )

# %% Toevoegen waar nog geen sturing is toegevoegd

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
)

# %%
# Corrigeer basin-peilen/profielen langs open Manning-routes nadat alle full-control-controllers bekend zijn.
manning_level_updates = sync_basin_levels_along_manning_routes(
    model=model,
    basin_output_gpkg=cloud.joinpath(
        AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", "manning_level_basin_updates.gpkg"
    ),
    control_output_gpkg=cloud.joinpath(
        AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", "manning_level_control_updates.gpkg"
    ),
)

# %% Junctionfy(!)
junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# %% Verdeelwerk bij Eindhoven 10% afvgevoerd via Beatrixkanaal (max 20m3/s) en 90% via Dieze
model.outlet.static.df.loc[model.outlet.static.df.node_id == 379, "max_flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 293, "min_upstream_level"] = 16.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 210, "min_upstream_level"] = 16.2

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

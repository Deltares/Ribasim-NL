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

MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "BrabantseDelta"  # authority
SHORT_NAME: str = "wbd"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {979}
EXCLUDE_SUPPLY_NODES = []
MANUAL_BASIN_LEVEL_NODE_IDS = [1354]


# Handmatige indeling control-, supply- en drain-nodes.
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [
    244, 332, 363, 408, 501, 529, 553, 570, 603, 684, 740, 756, 885
]

supply_nodes = [
    72, 218, 240, 241, 331, 338, 383, 410, 432, 499, 503, 511, 554, 563, 569, 574, 589, 604, 627,
    639, 663, 670, 690, 726, 732, 750, 751, 785, 786, 818, 859, 950, 971, 972, 973, 1055
]

drain_nodes = [
    69, 76, 93, 101, 102, 103, 104, 107, 110, 113, 125, 126, 127, 247, 249, 336, 339, 358, 362, 369,
    382, 425, 434, 455, 460, 470, 488, 500, 512, 513, 526, 528, 541, 545, 546, 564, 571, 575, 609,
    611, 626, 640, 649, 650, 651, 662, 666, 669, 688, 689, 691, 724, 727, 728, 729, 739, 782, 787,
    790, 793, 801, 886, 887, 932, 939, 940, 941, 959, 962, 988, 990
]
# fmt: on

SUPPLY_AREA_IGNORE_LINKS = {
    "Dinteloord": [],
    "Donge": [],
    "Fijnaart": [635],
    "Hartelweg": [],
    "Hoeven": [730, 731, 1105, 1106, 1554, 1758, 2233],
    "Leursche haven": [640, 730, 731, 1071, 1136, 1641, 1643, 1758, 2225, 2226],
    "Made": [],
    "Molenpolder": [],
    "Oud Gastel": [],
    "Patersheide": [],
    "Slikken": [],
    "Sprangsloot": [],
    "Steenbergen": [1028, 2227],
    "Weimeren": [1071, 2225, 2226],
    "Westplas": [],
}


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
) -> gpd.GeoDataFrame:
    return add_controllers_to_supply_area(
        model=model,
        polygon=supply_area_polygon(aanvoergebieden_df, area_name),
        exclude_nodes=EXCLUDE_NODES,
        ignore_intersecting_links=ignore_intersecting_links,
        drain_nodes=drain_nodes,
        flushing_nodes={},
        supply_nodes=supply_nodes,
        flow_control_nodes=flow_control_nodes,
        control_node_types=CONTROL_NODE_TYPES,
        add_supply_nodes=True,
    )


# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")

aanvoergebieden_gpkg = cloud.joinpath(r"BrabantseDelta/verwerkt/sturing/aanvoergebieden.gpkg")
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

# model.update_node(node_id=968, node_type="Outlet")  # wordt outlet, was manning
model.remove_node(977, remove_links=True)
model.remove_node(829, remove_links=True)
model.remove_node(1049, remove_links=True)

model.reverse_link(link_id=469)
model.reverse_link(link_id=1536)

model.reverse_link(link_id=2483)
model.reverse_link(link_id=2228)

model.reverse_link(link_id=1973)
model.reverse_link(link_id=861)
model.reverse_link(link_id=845)
model.reverse_link(link_id=1253)

# Rode Vaart
model.reverse_link(link_id=2458)
model.reverse_link(link_id=1685)
model.reverse_link(link_id=2459)
model.reverse_link(link_id=1687)

## Volkerak vol 128.5m, tijdelijk pomp
model.update_node(node_id=1056, node_type="Pump")

# %%
# Toevoegen aanvoergebieden

supply_area_control_node_ids: set[int] = set()
for area_name, ignore_intersecting_links in SUPPLY_AREA_IGNORE_LINKS.items():
    print(f"Toevoegen {area_name}")
    node_functions_df = add_supply_area_control(
        model=model,
        aanvoergebieden_df=aanvoergebieden_df,
        area_name=area_name,
        ignore_intersecting_links=ignore_intersecting_links,
        supply_nodes=supply_nodes,
        drain_nodes=drain_nodes,
        flow_control_nodes=flow_control_nodes,
    )
    if not node_functions_df.empty and node_functions_df["function"].isin(["supply", "flow_control"]).any():
        supply_area_control_node_ids.update(int(node_id) for node_id in node_functions_df.index)


# %%
# Add all remaining inlets/outlets

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes={},
    exclude_nodes=list(EXCLUDE_NODES),
)


# %%
# Corrigeer basin-peilen/profielen langs open Manning-routes pas nadat alle
# aanvoer/doorlaat/drain-controllers bekend zijn. Drain-only gebieden blijven zo buiten beeld,
# maar drains aan aangepaste basins krijgen wel bijgewerkte sturing.
manning_level_updates = sync_basin_levels_along_manning_routes(
    model=model,
    output_path=cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", "manning_level_updates.csv"),
    basin_output_gpkg=cloud.joinpath(
        AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", "manning_level_basin_updates.gpkg"
    ),
    control_output_gpkg=cloud.joinpath(
        AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", "manning_level_control_updates.gpkg"
    ),
    protected_basin_node_ids=MANUAL_BASIN_LEVEL_NODE_IDS,
    extra_control_node_ids=supply_area_control_node_ids,
)


# %% Junctionfy(!)
junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# %% Verdeelwerk bij Eindhoven 10% afvgevoerd via Beatrixkanaal (max 20m3/s) en 90% via Dieze
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 293, "flow_rate"] = 200
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 210, "min_upstream_level"] = 16.2

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

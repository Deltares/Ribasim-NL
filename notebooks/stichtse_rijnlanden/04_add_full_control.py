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
from ribasim_nl.parametrization.manning_level import sync_full_control_manning_levels
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
AUTHORITY: str = "StichtseRijnlanden"  # authority
SHORT_NAME: str = "hdsr"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 746: Oudewater sluis
# 750: Oude Leidseweg Sluis
# 753: Woerdenseverlaat SLuis
# 751: Montfoort Sluis
EXCLUDE_NODES = {486, 745, 746, 750, 753}
EXCLUDE_SUPPLY_NODES = []

# Handmatige indeling control-, supply- en drain-nodes.
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [
    134, 207, 405, 527, 636, 777, 778, 809, 814, 977, 1010, 1011, 1033, 1036,
    1038, 1039, 1050, 1059, 1063, 1107, 1153, 1154, 1155, 1279,
]

supply_nodes = [
    103, 358, 424, 425, 476, 481, 486, 506, 536, 542, 543, 553, 554, 564, 581,
    589, 593, 601, 624, 626, 627, 630, 637, 638, 639, 640, 648, 649, 650, 651,
    654, 655, 742, 747, 754, 761, 772, 797, 830, 840, 855, 890, 906, 911, 924,
    962, 976, 987, 1007, 1014, 1022, 1042, 1056, 1082, 1156, 2111,
]

drain_nodes = [
    139, 168, 173, 185, 198, 230, 298, 347, 411, 467, 477, 513, 545, 551, 554,
    588, 591, 598, 612, 633, 634, 761, 799, 818, 844, 851, 864, 887, 893, 894,
    920, 944, 956, 969, 971, 978, 979, 980, 993, 1033, 1077, 1126, 1145, 1168, 1203, 1223,
    2110,
]

flushing_nodes = {186: 1.25, 757: 3.0, 919: 5}

SUPPLY_AREA_IGNORE_LINKS = {
    "Kromme Rijn/ARK": [2175],
    "EVS": [1448],
    "Lopikerwaard": [404, 1305, 1618, 2271],
    "Leidsche-Oude Rijn": [292, 384, 762, 847, 1385, 1775, 2169],
    "Gek. Hollandsche IJssel": [227, 292, 638, 762, 847, 1385, 1775],
    "Utrecht-Noord": [],
    "Utrechtse Heuvelrug/Kromme Rijn": [2103],
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
aanvoer_path = aanvoer_path = cloud.joinpath(AUTHORITY, "verwerkt/4_ribasim/peilgebieden_bewerkt.gpkg")

aanvoergebieden_gpkg = cloud.joinpath(r"StichtseRijnlanden/verwerkt/sturing/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoer_path, qlr_path, aanvoergebieden_gpkg])


# %%
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = model.outlet.static.df.flow_rate
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate

# %%
# Identificeren aanvoerknopen en voorzien van afvoercapaciteit

# aanmaken node_df en specificeren supply_nodes
# knopen die beginnen met INL, i of eindigen op i, maar niet op fictief
for node_type in CONTROL_NODE_TYPES:
    node_df = model.get_component(node_type).node.df

    model.node.df.loc[node_df.index, IS_SUPPLY_NODE_COLUMN] = (
        node_df["meta_code_waterbeheerder"].str.startswith("INL")
        | node_df["meta_code_waterbeheerder"].str.startswith("i")
        | node_df["meta_code_waterbeheerder"].str.startswith("I")
        | node_df["meta_code_waterbeheerder"].str.endswith("i")
    ) & ~(
        node_df.node_type.isin(CONTROL_NODE_TYPES) & node_df["meta_code_waterbeheerder"].str.endswith("fictief")
        | node_df.index.isin(EXCLUDE_SUPPLY_NODES)
    )

    # force nan or 0 to 20 m3/s
    node_df = model.node.df.loc[node_df.index]
    node_ids = node_df[node_df[IS_SUPPLY_NODE_COLUMN]].index.values
    print(node_ids)
    static_df = model.get_component(node_type).static.df

    mask = static_df.node_id.isin(node_ids) & ((static_df.flow_rate == 0) | (static_df.flow_rate.isna()))

#    static_df.loc[mask, "flow_rate"] = 20

# %% model fixes
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 45, "level"] = -1.4

# doorslag staat normaal open
model.reverse_link(link_id=1470)
model.reverse_link(link_id=1063)

# De Pelikaan links omdraaien
model.reverse_link(link_id=578)
model.reverse_link(link_id=1447)
model.reverse_link(link_id=1223)
model.reverse_link(link_id=2200)

# 416: I2075
model.reverse_link(link_id=1399)
model.reverse_link(link_id=715)

# 138: I6125
model.reverse_link(link_id=2649)
model.reverse_link(link_id=1551)

# 545: Rijnvliet is afvoergemaal
model.reverse_link(link_id=847)
model.reverse_link(link_id=1775)

# node 2806 is een inlaat, dus flow_direction draaien
model.reverse_link(link_id=2711)
model.reverse_link(link_id=2005)

# Gemaal Terwijde
model.reverse_link(link_id=2073)
model.reverse_link(link_id=24)

model.update_node(node_id=730, node_type="ManningResistance")

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


# %%
# EXCLUDE NODES op 0 m3/s zetten
mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, "flow_rate"] = 0.0
model.outlet.static.df.loc[mask, "min_flow_rate"] = 0.0
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0.0


# %% Toevoegen waar nog geen sturing is toegevoegd

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
)

# %% Noordergemaal, node=536 slaat pas aan wanneer Wijk van Duurstede net genoeg kan leveren
model.pump.static.df.loc[model.pump.static.df.node_id == 536, "max_downstream_level"] -= 0.01
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1344, "max_downstream_level"] -= 0.01
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1345, "max_downstream_level"] -= 0.01

# 3 sifons, 468,469,470 onder Ark wordt later ingeschakeld dan inlaat Vreeswijk
model.outlet.static.df.loc[model.outlet.static.df.node_id == 468, "max_downstream_level"] -= 0.01
model.outlet.static.df.loc[model.outlet.static.df.node_id == 469, "max_downstream_level"] -= 0.01
model.outlet.static.df.loc[model.outlet.static.df.node_id == 470, "max_downstream_level"] -= 0.01

# Caspargauw gaat pas leveren als Wijk bij Duurstede aanvoer te laag is
model.pump.static.df.loc[model.pump.static.df.node_id == 601, "max_downstream_level"] -= 0.01

# %%
# Corrigeer basin-peilen/profielen langs open Manning-routes nadat alle full-control-controllers bekend zijn.
PROTECTED_MANNING_BASIN_NODE_IDS = [
    1376,
    1380,
    1387,
    1396,
    1401,
    1406,
    1414,
    1422,
    1426,
    1436,
    1452,
    1462,
    1474,
    1492,
    1501,
    1516,
    1562,
    1572,
    1576,
    1583,
    1586,
    1588,
    1654,
    1660,
    1668,
    1673,
    1698,
    1737,
    1757,
    1760,
    1766,
    1778,
    1836,
    1847,
    1886,
    1975,
    1986,
    1987,
    1988,
]
PROTECTED_MANNING_CONTROL_NODE_IDS: set[int] = set()


def sync_manning_level_controls(model: Model, *, write_reports: bool = False):
    return sync_full_control_manning_levels(
        model=model,
        output_dir=cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model"),
        write_reports=write_reports,
        protected_basin_node_ids=PROTECTED_MANNING_BASIN_NODE_IDS,
        protected_control_node_ids=PROTECTED_MANNING_CONTROL_NODE_IDS,
    )


manning_level_updates = sync_manning_level_controls(model, write_reports=True)

# %% Junctionfy(!)
junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime


# %%

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=1)
sync_manning_level_controls(model)
model.write(ribasim_toml_dry)

# run hoofdmodel
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml_dry, qlr_path=qlr_path).run_all()
    model = Model.read(ribasim_toml_dry)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
sync_manning_level_controls(model)
model.write(ribasim_toml_wet)

# run prerun model
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml_wet, qlr_path=qlr_path).run_all()
    model = Model.read(ribasim_toml_wet)

# hoofd run
update_basin_static(model=model, precipitation_mm_per_day=1.5)
sync_manning_level_controls(model)
model.write(ribasim_toml)
# run hoofdmodel
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path).run_all()

# %%

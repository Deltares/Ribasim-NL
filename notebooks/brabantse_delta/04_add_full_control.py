# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

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
outlet_max_flow_rate_from_results = {
    69: 2,  # Dalum/Spoorbaan
    71: 2,  # Dalum/Reuverlaan
    150: 3,  # stuw Galder
    189: 14,  # stuw Wernhout
    230: 12,  # stuw Watermolen
    233: 3,  # Het Laag
    240: 2,  # KDU01724
    241: 2,  # KDU18314
    302: 6,  # Molenbeek verdeelwerk
    408: 2,  # Haven van Zevenbergen
    461: 2,  # KDU01655
    475: 12,  # stuw Wielhoef
    486: 9,  # stuw Oude Turfvaartsestaat
    383: 2,  # dynamic max=0.70
    499: 3,  # Harmonie /vlotterinlaat 1; dynamic max=1.39
    576: 2,  # Stuw Ruigenhil/Oostdijk
    577: 2,  # KDU10783
    580: 2,  # KST01119
    581: 2,  # KDU10780
    593: 2,  # KDU14589
    699: 6,  # Weststadweg Houtsesteeg
    737: 2,  # KDU32322
    738: 2,  # KDU24022
    745: 2,  # Groenvenseweg
    794: 17,  # Het Vaartje/Waspik
    936: 9,  # KDU03763
    955: 2,  # KDU02582
    971: 2,  # Marksluis
    991: 5,  # KDU28116
    1048: 1,  # naam onbekend
    1055: 2,  # naam onbekend
    1272: 11,  # naam onbekend
    2323: 2,  # naam onbekend
    # Hoge source-capaciteiten vervangen door 1.5x de maximale flow uit het dynamic resultaat.
    105: 1,  # naam onbekend; dynamic max=0.01, oude max=148
    107: 1,  # naam onbekend; dynamic max=0.03, oude max=148
    113: 1,  # naam onbekend; dynamic max=0.00, oude max=172
    254: 1,  # naam onbekend; dynamic max=0.00, oude max=218
    367: 1,  # naam onbekend; dynamic max=0.30, oude max=400
    369: 1,  # naam onbekend; dynamic max=0.43, oude max=400
    371: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    375: 1,  # naam onbekend; dynamic max=0.37, oude max=400
    394: 1,  # naam onbekend; dynamic max=0.43, oude max=133
    398: 1,  # naam onbekend; dynamic max=0.00, oude max=580
    427: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    431: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    440: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    442: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    443: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    444: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    445: 1,  # naam onbekend; dynamic max=0.03, oude max=400
    447: 2,  # naam onbekend; dynamic max=0.87, oude max=400
    463: 2,  # naam onbekend; dynamic max=0.87, oude max=405
    464: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    526: 1,  # naam onbekend; dynamic max=0.07, oude max=133
    528: 1,  # naam onbekend; dynamic max=0.27, oude max=133
    529: 1,  # naam onbekend; dynamic max=0.10, oude max=133
    536: 3,  # naam onbekend; dynamic max=1.82, oude max=133
    552: 1,  # naam onbekend; dynamic max=0.05, oude max=147
    553: 1,  # naam onbekend; dynamic max=0.03, oude max=580
    557: 1,  # naam onbekend; dynamic max=0.22, oude max=400
    559: 1,  # naam onbekend; dynamic max=0.61, oude max=400
    564: 1,  # naam onbekend; dynamic max=0.16, oude max=580
    567: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    570: 1,  # naam onbekend; dynamic max=0.10, oude max=400
    571: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    572: 1,  # naam onbekend; dynamic max=0.10, oude max=400
    573: 1,  # naam onbekend; dynamic max=0.10, oude max=400
    592: 8,  # naam onbekend; dynamic max=4.73, oude max=423
    603: 2,  # naam onbekend; dynamic max=0.80, oude max=400
    619: 1,  # naam onbekend; dynamic max=0.24, oude max=400
    620: 1,  # naam onbekend; dynamic max=0.08, oude max=402
    621: 1,  # naam onbekend; dynamic max=0.01, oude max=401
    622: 1,  # naam onbekend; dynamic max=0.09, oude max=403
    624: 1,  # naam onbekend; dynamic max=0.50, oude max=404
    625: 2,  # naam onbekend; dynamic max=0.71, oude max=408
    626: 1,  # naam onbekend; dynamic max=0.08, oude max=412
    628: 2,  # naam onbekend; dynamic max=0.95, oude max=401
    629: 1,  # naam onbekend; dynamic max=0.01, oude max=401
    630: 1,  # naam onbekend; dynamic max=0.01, oude max=401
    631: 1,  # naam onbekend; dynamic max=0.08, oude max=401
    632: 1,  # naam onbekend; dynamic max=0.08, oude max=401
    633: 1,  # naam onbekend; dynamic max=0.50, oude max=404
    634: 1,  # naam onbekend; dynamic max=0.08, oude max=402
    635: 1,  # naam onbekend; dynamic max=0.08, oude max=402
    636: 1,  # naam onbekend; dynamic max=0.01, oude max=401
    637: 1,  # naam onbekend; dynamic max=0.01, oude max=401
    638: 1,  # naam onbekend; dynamic max=0.01, oude max=401
    640: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    641: 2,  # naam onbekend; dynamic max=1.23, oude max=400
    653: 1,  # naam onbekend; dynamic max=0.03, oude max=400
    655: 1,  # naam onbekend; dynamic max=0.37, oude max=400
    656: 1,  # naam onbekend; dynamic max=0.37, oude max=400
    657: 1,  # naam onbekend; dynamic max=0.10, oude max=400
    658: 1,  # naam onbekend; dynamic max=0.08, oude max=400
    660: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    669: 1,  # naam onbekend; dynamic max=0.05, oude max=400
    671: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    678: 1,  # naam onbekend; dynamic max=0.00, oude max=218
    679: 1,  # naam onbekend; dynamic max=0.00, oude max=218
    680: 1,  # naam onbekend; dynamic max=0.00, oude max=218
    682: 1,  # naam onbekend; dynamic max=0.08, oude max=218
    683: 1,  # naam onbekend; dynamic max=0.02, oude max=218
    684: 1,  # naam onbekend; dynamic max=0.09, oude max=218
    685: 1,  # naam onbekend; dynamic max=0.08, oude max=218
    686: 1,  # naam onbekend; dynamic max=0.08, oude max=218
    688: 1,  # naam onbekend; dynamic max=0.12, oude max=218
    691: 1,  # naam onbekend; dynamic max=0.12, oude max=218
    711: 1,  # naam onbekend; dynamic max=0.33, oude max=423
    715: 1,  # naam onbekend; dynamic max=0.11, oude max=423
    716: 1,  # naam onbekend; dynamic max=0.33, oude max=423
    717: 1,  # naam onbekend; dynamic max=0.24, oude max=423
    718: 1,  # naam onbekend; dynamic max=0.21, oude max=423
    748: 1,  # naam onbekend; dynamic max=0.05, oude max=148
    879: 1,  # naam onbekend; dynamic max=0.24, oude max=400
    880: 1,  # naam onbekend; dynamic max=0.22, oude max=400
    881: 1,  # naam onbekend; dynamic max=0.02, oude max=133
    883: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    885: 6,  # naam onbekend; dynamic max=3.96, oude max=133
    888: 2,  # naam onbekend; dynamic max=0.85, oude max=133
    889: 2,  # naam onbekend; dynamic max=0.85, oude max=133
    890: 1,  # naam onbekend; dynamic max=0.00, oude max=133
    934: 1,  # naam onbekend; dynamic max=0.37, oude max=133
    942: 2,  # naam onbekend; dynamic max=0.78, oude max=580
    944: 2,  # naam onbekend; dynamic max=0.96, oude max=400
    983: 1,  # naam onbekend; dynamic max=0.00, oude max=400
    989: 1,  # naam onbekend; dynamic max=0.24, oude max=400
    992: 1,  # naam onbekend; dynamic max=0.03, oude max=133
    1085: 1,  # naam onbekend; dynamic max=0.00, oude max=100
}
outlet_max_flow_rate_afvoer_by_node_id = {}
for max_flow_rates in (outlet_max_flow_rate_from_results,):
    for node_id, max_flow_rate in max_flow_rates.items():
        outlet_max_flow_rate_afvoer_by_node_id[node_id] = max(
            outlet_max_flow_rate_afvoer_by_node_id.get(node_id, 0.0),
            max_flow_rate,
        )
# Handmatige indeling control-, supply- en drain-nodes.
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [
    244, 332, 363, 408, 501, 529, 553, 570, 601,603, 684, 740, 756, 885
]

supply_nodes = [
    72, 218, 240, 241, 331, 338, 383, 410, 432, 499, 503, 511, 554, 563, 569, 574, 589, 600,604, 627,
    639, 663, 670, 690,704,706, 716, 726, 732, 750, 751, 785, 786, 818, 859, 950, 971, 972, 973, 1055,2323
]

drain_nodes = [
    69, 76, 93, 101, 102, 103, 104, 107, 110, 113, 125, 126, 127, 247, 249, 336, 339, 358, 362, 369, 372, 373, 374,
    382, 425, 434, 448, 455, 460, 470, 488, 500, 512, 513, 526, 528, 541, 545, 546, 564, 571, 575, 609,
    611, 626, 640, 649, 650, 651, 662, 666, 669, 688, 689, 691, 718, 724, 727, 728, 729, 739, 782, 787,
    790, 793, 801,806, 886, 887, 932, 939, 940, 941, 946, 958, 959, 962, 988, 990
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
        flow_rate_aanvoer=20.0,
        max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
        flow_rate_afvoer=100.0,
        max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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

outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

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
model.pump.static.df.loc[model.pump.static.df.node_id == 1056, ["flow_rate", "max_flow_rate"]] = 100

# Flow_rate was te laag ingesteld
model.pump.static.df.loc[model.pump.static.df.node_id == 376, ["flow_rate", "max_flow_rate"]] = 5

model.pump.static.df.loc[model.pump.static.df.node_id == 446, ["flow_rate", "max_flow_rate"]] = 5


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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
# Bestaande handmatige pompwaarden, zoals 376 en 446, blijven leidend.
pump_max_flow_rate_from_results = {
    208: 1,  # naam onbekend; oude static flow_rate=0.13
    244: 1,  # naam onbekend; oude static flow_rate=0.13
    413: 3,  # naam onbekend; oude static flow_rate=1.58
    434: 1,  # naam onbekend; oude static flow_rate=0.62
    453: 1,  # naam onbekend; oude static flow_rate=0.2
    498: 3,  # naam onbekend; oude static flow_rate=1.9
    510: 1,  # naam onbekend; oude static flow_rate=0.21
    527: 4,  # naam onbekend; oude static flow_rate=0.75
    540: 1,  # naam onbekend; oude static flow_rate=0.2
    561: 2,  # naam onbekend; dynamic max=0.77, oude static flow_rate=0.5
    609: 1,  # Canada; oude static flow_rate=0.05
    623: 1,  # naam onbekend; oude static flow_rate=0.2
    659: 2,  # naam onbekend; oude static flow_rate=1.58
    797: 3,  # naam onbekend; oude static flow_rate=1.58
    800: 2,  # naam onbekend; oude static flow_rate=0.08
    990: 1,  # naam onbekend; oude static flow_rate=0.04
    972: 1,  # naam onbekend; dynamic max=0.08, oude max=120
    984: 1,  # Gemaal bij Keersluis Leursche Haven; dynamic max=0.54, oude max=120
    985: 1,  # Gemaal bii Keersluis Laaksche Vaart; dynamic max=0.46, oude max=120
}
mask = (
    model.pump.static.df.node_id.isin(pump_max_flow_rate_from_results)
    & model.pump.static.df.flow_rate.notna()
    & (model.pump.static.df.flow_rate > 0)
)
model.pump.static.df.loc[mask, "max_flow_rate"] = model.pump.static.df.loc[mask, "node_id"].map(
    pump_max_flow_rate_from_results
)

# %% Junctionfy(!)
junctionify(model)
aanvoer_only_node_ids = set(supply_nodes) - set(drain_nodes) - set(flow_control_nodes)

# Aanvoer-cap: doorlaten/inlaten mogen in aanvoer niet de hoge afvoercapaciteit gebruiken.
aanvoer_outlet_mask = model.outlet.static.df.control_state == "aanvoer"
model.outlet.static.df.loc[aanvoer_outlet_mask, ["flow_rate", "max_flow_rate"]] = model.outlet.static.df.loc[
    aanvoer_outlet_mask, ["flow_rate", "max_flow_rate"]
].clip(upper=10.0)
zero_aanvoer_node_ids = {
    node_id for node_id, max_flow_rate in outlet_max_flow_rate_aanvoer_by_node_id.items() if max_flow_rate == 0
}
zero_aanvoer_mask = aanvoer_outlet_mask & model.outlet.static.df.node_id.isin(zero_aanvoer_node_ids)
model.outlet.static.df.loc[zero_aanvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

# Afvoer-cap: voorkom blokkades door te lage max_flow_rate in afvoer.
node_type_by_id = model.node.df["node_type"].to_dict()
flow_demand_controlled_node_ids = set(
    model.link.df.loc[
        model.link.df["from_node_id"].map(node_type_by_id).eq("FlowDemand"),
        "to_node_id",
    ]
    .dropna()
    .astype(int)
)
manual_max_flow_rate_node_ids = {376, 446, 1056}
protected_max_flow_rate_node_ids = set(EXCLUDE_NODES) | flow_demand_controlled_node_ids | manual_max_flow_rate_node_ids
for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = (
        static_df["control_state"].eq("afvoer")
        & static_df["flow_rate"].fillna(0).gt(0)
        & ~static_df["node_id"].isin(protected_max_flow_rate_node_ids)
    )
    static_df.loc[afvoer_mask, "max_flow_rate"] = (
        static_df.loc[afvoer_mask, "max_flow_rate"].fillna(0.5).clip(lower=0.5)
    )

for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = static_df["control_state"].eq("afvoer") & static_df["node_id"].isin(aanvoer_only_node_ids)
    static_df.loc[afvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

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

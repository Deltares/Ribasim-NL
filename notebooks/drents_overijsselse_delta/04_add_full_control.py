# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim import Node
from ribasim.nodes import level_boundary, pump
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely import Point

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "DrentsOverijsselseDelta"
SHORT_NAME: str = "dod"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"
MIN_FLOW_RATE_BY_NODE_ID = {
    # 378: 1.0,  # Rogatsluis # model wordt hier heel traag van
    # 541: 0.5,  # Paradijssluis # model wordt hier heel traag van
}


# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {522, 527, 528, 538, 544}


# %%
# Helpers


def set_static_values(static_df, node_values: dict[int, float], column: str) -> None:
    for node_id, value in node_values.items():
        mask = static_df.node_id == node_id
        static_df.loc[mask, column] = value
        if column in ["min_upstream_level", "max_downstream_level"]:
            mark_level_update_protected(static_df, mask, model=model)


def set_max_flow_rate(static_df, max_flow_rate_by_node_id: dict[int, float]) -> None:
    max_flow_rate = static_df["node_id"].map(max_flow_rate_by_node_id)
    mask = max_flow_rate.notna()

    static_df.loc[mask, "max_flow_rate"] = max_flow_rate[mask]
    static_df.loc[mask, "flow_rate"] = max_flow_rate[mask]


def update_nodes(model: Model, node_ids: list[int], node_type: str) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.update_node(node_id=node_id, node_type=node_type)


def add_supply_area_control(
    model: Model,
    aanvoergebieden_df: gpd.GeoDataFrame,
    area_name: str,
    ignore_intersecting_links: list[int],
    supply_nodes: list[int],
    drain_nodes: list[int],
    flow_control_nodes: list[int],
    flow_rate_aanvoer: float | None = None,
    max_flow_rate_aanvoer: float | None = None,
    flow_rate_afvoer: float | None = None,
    max_flow_rate_afvoer: float | None = None,
) -> None:
    polygon = aanvoergebieden_df.loc[[area_name], "geometry"].union_all()

    # toevoegen sturing
    add_controllers_to_supply_area(
        model=model,
        polygon=polygon,
        exclude_nodes=EXCLUDE_NODES,
        ignore_intersecting_links=ignore_intersecting_links,
        drain_nodes=drain_nodes,
        flushing_nodes={},  # doorspoeling op uitlaten
        supply_nodes=supply_nodes,
        flow_control_nodes=flow_control_nodes,
        control_node_types=CONTROL_NODE_TYPES,
        add_supply_nodes=True,
        flow_rate_aanvoer=flow_rate_aanvoer,
        max_flow_rate_aanvoer=max_flow_rate_aanvoer,
        flow_rate_afvoer=flow_rate_afvoer,
        max_flow_rate_afvoer=max_flow_rate_afvoer,
    )


def run_model_and_control(model: Model, ribasim_toml, qlr_path):
    model.write(ribasim_toml)

    if MODEL_EXEC:
        model.run()
        Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path).run_all()
        return Model.read(ribasim_toml)

    return model


# %%
# Definieren paden en syncen met cloud

cloud = CloudStorage()

ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "aanvoergebieden.gpkg")

cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


# %%
# Read data

model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# Extra aanvoerpomp bij basin 1901, met boundary op dezelfde plek/level als boundary 72.
boundary_level = model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 72, "level"].dropna()
dod_extra_boundary = model.level_boundary.add(
    Node(
        geometry=model.level_boundary[72].geometry, name="extra boundary bij 72", meta_couple_authority="Vechtstromen"
    ),
    tables=[level_boundary.Static(level=[float(boundary_level.iloc[0]) if not boundary_level.empty else 0.0])],
)
dod_extra_supply_pump = model.pump.add(
    Node(geometry=Point(240732.7023, 541529.6371), name="extra aanvoerpomp bij 3225"),
    tables=[pump.Static(flow_rate=[1.2], max_flow_rate=[1.2], min_upstream_level=[14.86])],
)
model.link.add(dod_extra_boundary, dod_extra_supply_pump)
model.link.add(dod_extra_supply_pump, model.get_node(1901))

# %%
# Linkrichting fixes
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
reverse_link_ids = [
    75, 120, 122, 210, 230, 249, 291, 294, 305, 524, 619, 747, 800, 805, 865, 873,
    889, 983, 997, 1020, 1021, 1041, 1046, 1097, 1103, 1158, 1165, 1244, 1260, 1265,
    1350, 1561, 1581, 1604, 1618, 1619, 1635, 1787, 1797, 1874, 1916, 1971, 2009,
    2050, 2071, 2082, 2093, 2110, 2111, 2122, 2166, 2273, 2280, 2361, 2369, 2402,
    2415, 2445, 2456, 2526, 2610, 2652, 2669, 2893, 2940, 2944, 2956, 2958, 2992,
    2993, 3051, 3073,3163,3164,3165,3166,3171,3172
]
# fmt: on

for link_id in reverse_link_ids:
    model.reverse_link(link_id=link_id)

model.redirect_link(368, to_node_id=2638)
model.redirect_link(364, to_node_id=2554)

# stuw.43145 is na de nieuwe fix al georienteerd als 1643 -> 3224 -> 2585.
model.redirect_link(1670, from_node_id=2585, to_node_id=234)


# %%
# Node type fixes


# Streefpeil te laag
set_static_values(
    model.outlet.static.df,
    {
        959: 11.0,
        971: 11.0,
        1238: 11.0,
        544: -0.2,
    },
    "min_upstream_level",
)

for node_id, to_node_id in [
    (2164, 2120),
    (1970, 1990),
    (2294, 1949),
    (1669, 1670),
    (2352, 1819),
    (1920, 1881),  # Noordscheschutsluis
]:
    model.merge_basins(node_id=node_id, to_node_id=to_node_id, are_connected=True)

# Manning moet outlet zijn
model.update_node(node_id=1468, node_type="Outlet")
mask = model.outlet.static.df.node_id == 1468
model.outlet.static.df.loc[mask, "min_upstream_level"] = 2.63
mark_level_update_protected(model.outlet.static.df, mask, model=model)

# Naamcorrectie: node_id 1344 is Zalk, niet Adsum
model.node.df.loc[1344, "name"] = "Zalk"


# %%
# Max-capaciteiten pompen en sluizen

outlet_max_flow_rate_by_node_id = {
    199: 200.0,  # Vechterweerd
    2582: 200.0,  # Vechterweerd
    454: 16.2,  # Paradijssluis
    387: 14.9,  # Haveltersluis
    490: 14.5,  # Uffeltersluis
    445: 12.2,  # Dieversluis
    446: 12.4,  # Haarsluis
    370: 13.4,  # Veenesluis
    1091: 999.9,  # Smildersluis
    534: 11.0,  # Noordscheschutsluis
    531: 29.2,  # Nieuwebrugsluis
    369: 29.6,  # Ossesluis
    378: 43.3,  # Rogatsluis
    1012: 0.01,  # kleine duiker
}
outlet_max_flow_rate_parameterized_zero_by_node_id = {
    1022: 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    1287: 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    1406: 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
}
outlet_max_flow_rate_from_results = {
    508: 2,  # duikersifonhevel.286070
    757: 2,  # duikersifonhevel.281013
    764: 2,  # duikersifonhevel.279722
    922: 2,  # duikersifonhevel.11883
    937: 2,  # duikersifonhevel.499162
    968: 2,  # duikersifonhevel.624652
    973: 2,  # duikersifonhevel.134543
    1080: 2,  # duikersifonhevel.748812
    1179: 2,  # duikersifonhevel.287091
    1183: 2,  # duikersifonhevel.168460
    1237: 2,  # duikersifonhevel.12121
    3193: 3,  # sluis.2281
    3206: 2,  # Wyberbuurseweg
    3226: 2,  # Snelweg
    3228: 2,  # naam onbekend
    3229: 2,  # naam onbekend
    3234: 2,  # naam onbekend
}
outlet_max_flow_rate_coupled_by_node_id = {
    198: 170,  # gekoppeld max=112.50, huidige max=83.20, link=5900281
    501: 5,  # duikersifonhevel.599665; gekoppeld max=2.21, parameterized=0.10
    519: 8,  # gekoppeld max=4.73, huidige max=0.00, link=5900891
    738: 3,  # duikersifonhevel.285812; gekoppeld max=1.74, parameterized=0.10
    756: 2,  # gekoppeld max=0.77, huidige max=0.10, link=5900486
    1064: 8,  # duikersifonhevel.134864; gekoppeld max=4.90, parameterized=0.10
    1074: 2,  # gekoppeld max=0.66, huidige max=0.00, link=5901233
    1077: 2,  # gekoppeld max=0.66, huidige max=0.00, link=5901239
    1171: 3,  # gekoppeld max=1.27, huidige max=0.10, link=5900197
    1297: 3,  # duikersifonhevel.586543; gekoppeld max=1.88, parameterized=0.10
    1298: 3,  # duikersifonhevel.586544; gekoppeld max=1.88, parameterized=0.10
    1324: 2,  # gekoppeld max=0.54, huidige max=0.10, link=5901353
    1468: 80,  # parameterized nul; gekoppeld max=52.24
    3193: 5,  # parameterized nul; gekoppeld max=2.75
    3202: 3,  # gekoppeld max=1.60, huidige max=0.00, link=5903104
    3231: 2,  # parameterized nul; gekoppeld max=0.15
    3233: 3,  # parameterized nul; gekoppeld max=1.36
    3234: 45,  # parameterized nul; gekoppeld max=29.20, huidige max=20.00, link=5903168
    3235: 2,  # parameterized nul; gekoppeld max=0.77
}
outlet_max_flow_rate_afvoer_by_node_id = {}
for max_flow_rates in (
    outlet_max_flow_rate_from_results,
    outlet_max_flow_rate_coupled_by_node_id,
    outlet_max_flow_rate_parameterized_zero_by_node_id,
):
    for node_id, max_flow_rate in max_flow_rates.items():
        outlet_max_flow_rate_afvoer_by_node_id[node_id] = max(
            outlet_max_flow_rate_afvoer_by_node_id.get(node_id, 0.0),
            max_flow_rate,
        )
outlet_max_flow_rate_afvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)
outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

# Handmatige inlaatcapaciteiten gelden ook in aanvoer; niet terugvallen op de default van 10 m3/s.
outlet_max_flow_rate_aanvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)

pump_max_flow_rate_by_node_id = {
    704: 6.7,  # Paradijssluis
    645: 6.6,  # Haveltersluis
    705: 6.3,  # Uffeltersluis
    669: 6.3,  # Dieversluis
    665: 5.8,  # Haarsluis
    631: 5.9,  # Veenesluis
    346: 6.8,  # Noordscheschutsluis
    676: 7.1,  # Nieuwebrugsluis
    694: 7.5,  # Ossesluis
    647: 8.2,  # Rogatsluis
    601: 1.0,  # Emsland 1
    600: 0.16,  # Emsland 2
    602: 1.06,  # Langewijk
    603: 1.0,  # De Stouwe
    563: 0.58,  # 't Raasje -> Raasje
    565: 0.24,  # Regterveld -> Rechterveld
    574: 0.12,  # Willem Meijerpolder
    568: 0.21,  # Stikkenpolder
    576: 0.06,  # Zwartemeerpolder
    621: 0.83,  # Mandjeswaard
    556: 0.32,  # de Pieper
    573: 0.62,  # Zuiderzeepolder
    572: 5.50,  # Veneriete
    3189: 0.35,  # Biesvelden
    554: 0.27,  # Groot - Cellemuiden -> Groot Cellemuiden
    567: 0.13,  # Stikkeldrecht
    1094: 0.83,  # Cellemuiden
    620: 4.83,  # Nieuw Lutterzijl I -> Lutterzijl Mastenbroek
    623: 3.75,  # Nieuw Lutterzijl II: Lutterzijl Koekoek
    3211: 2.23,  # 't Gansje; Node.name is leeg, gevonden via meta_code_waterbeheerder
    622: 0.11,  # Noorderwaard
    561: 1.08,  # Benoorden de Willemsvaart -> Ben de Willemsvaart
    562: 0.08,  # 't Katje -> Katje
    571: 0.58,  # 't Vosje -> Vosje
    575: 0.11,  # Zalkerbos -> Zalkerbos en Erfgenamen
    559: 1.83,  # Adsum; node_id 1344 heeft ook Node.name = Adsum, zie controlelijst
    1100: 3.00,  # Kamperveen
    1096: 3.00,  # Roggebot
    618: 0.03,  # Kardoezenpolder -> Kardoezen
    619: 0.09,  # Ketelpolder
    1103: 0.30,  # Inlaat Broekhuizen -> De Broekhuizen
    210: 1.00,  # De Stouwe
    598: 0.16,  # De Doorn -> de Doorn
    596: 5.75,  # Galgerak -> Galgenrak
    3199: 5.75,  # Galgerak -> Galgenrak
    599: 20.0,  # Streukelerzijl
    593: 5.0,  # Kloosterzijl
    557: 11.17,  # Kostverlorenzijl
    555: 2.50,  # Spoortipp -> Spoortippe
    579: 1.16,  # Westerveld
    604: 3.66,  # Herfte
    584: 0.66,  # Sekdoorn
    586: 2.33,  # Lierder&Molenbroek -> Lierder en Molenbroek
    588: 0.90,  # Bremmelerstraat
    580: 0.01,  # Linterzijl
    581: 4.00,  # Langeslag (gemaal) -> Langeslag
    605: 0.40,  # Vechterweerd
    195: 0.33,  # Marswetering -> Voorbezinkplas Marswetering
    590: 8.5,  # Ankersmit
    1190: 8.5,  # Ankersmit
    638: 1.17,  # Blijdenstein
    660: 0.97,  # Trijzen
    547: 53.3,  # Afvoergemaal Stroink
    3153: 6.4,  # Aanvoergemaal Stroink
    648: 3.0,  # Holthe
    652: 1.5,  # Smildersluis
    636: 1.3,  # Zwiggeltersluis
    3192: 127.5,  # Zedemuden
    # Todo: Capaciteiten nog navragen
    # 545: ...,   # Sultansmeer
    # 693: ...,   # Weerwille
    # 633: ...,   # Oldenhave
}
pump_max_flow_rate_parameterized_zero_by_node_id = {
    3200: 20.0,  # xlsx max_flow_rate nul; voorkom aanvoerblokkade
}
pump_max_flow_rate_all_by_node_id = {
    **pump_max_flow_rate_parameterized_zero_by_node_id,
    **pump_max_flow_rate_by_node_id,
}
max_flow_rate_aanvoer_by_node_id = {
    **outlet_max_flow_rate_aanvoer_by_node_id,
    **pump_max_flow_rate_all_by_node_id,
}

set_max_flow_rate(model.outlet.static.df, outlet_max_flow_rate_by_node_id)
set_max_flow_rate(model.pump.static.df, pump_max_flow_rate_all_by_node_id)


# %%
# Handmatige indeling control-, supply- en drain-nodes
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [
    107, 119, 125, 133, 146, 148, 152, 153, 154, 173, 193, 198, 199, 226, 227, 241,
    242, 243, 244, 247, 248, 260, 285, 301, 317, 328, 331, 338, 344, 350, 361, 373, 374, 385,
    401, 403, 408, 414, 421, 423, 426, 427, 439, 442, 451, 458, 517, 602, 605, 610, 673, 913, 937,
    938, 1088, 1089, 1115, 1152, 1157, 1228, 1263, 1317, 1328, 1347, 1363, 1414, 2662, 2664, 2666,
    3130, 3136, 3229, 3234
]

supply_nodes = [
    152, 182, 192, 195, 196, 206, 212, 213, 216, 219, 233, 282, 284, 287, 288, 332,
    334, 346, 370,372, 440, 464, 503, 508, 511, 517, 526, 545, 549, 552, 562, 563, 564,
    570, 571, 579, 597, 604, 607, 615, 617, 631, 633, 634, 636, 637, 638, 645, 647,
    648, 651, 652, 660, 661,665, 669, 676, 693, 694, 699, 704, 708, 712, 713, 715, 723,
    726, 741, 748, 753, 758, 759, 760, 774, 775, 783, 785, 786, 787, 800, 801, 813,
    816, 821, 823, 829, 831, 832, 846, 861, 863, 868, 878, 894, 896, 898, 902, 906,
    914, 918, 920, 933, 936, 949, 951, 954, 965, 968, 969, 972, 976, 977, 983, 984,
    987, 1002, 1009, 1012, 1024, 1025, 1034, 1035, 1045, 1047, 1050, 1054, 1056, 1058, 1059, 1068,
    1074, 1077, 1078, 1086, 1103, 1109, 1132, 1141, 1142, 1152, 1182, 1187, 1188, 1190, 1196, 1199,
    1202, 1203, 1207, 1209, 1211, 1213, 1214, 1216, 1217, 1221, 1231, 1238, 1243, 1246, 1247, 1255,
    1260, 1261, 1262, 1278, 1279, 1283, 1289, 1291, 1294, 1302, 1303, 1307, 1311, 1312, 1325, 1326,
    1332, 1336, 1344, 1391, 1401, 1418, 1423, 1444, 1452, 1473, 2650, 2652, 2653, 2657, 2658, 2659,
    2662, 2665, 2667, 3110, 3116, 3118, 3119, 3122, 3124, 3125, 3126, 3190, 3193, 3196, 3199, 3202,
    3203, 3224, 3232, 3233,3237
]

drain_nodes = [
    106, 111, 113, 116, 122, 137, 143, 144, 145, 150, 152, 157, 159, 161, 165, 168,
    169, 170, 171, 173, 174, 177, 181, 183, 186, 190, 197, 200, 201, 202, 205, 208, 210,
    211, 218, 221, 230, 232, 234, 239, 246, 251, 252, 262, 263, 265, 267, 271, 274,
    276, 293, 295, 296, 297, 300, 304, 305, 307, 309, 311, 315, 322, 323, 324, 325,
    326, 330, 333, 335, 337, 347, 349, 351, 352, 359, 362, 363, 366, 368, 369, 373,
    375, 377, 378, 381, 382, 384, 387, 394, 395, 398, 402, 405, 416, 418, 421, 422, 428,
    431, 436, 437, 438, 444, 445, 446, 447, 452, 454, 456, 461, 468, 469, 473, 475,
    476, 480, 481, 485, 486, 487, 488, 489, 490, 493, 501, 503, 505, 513, 515, 521,
    525, 529, 534, 541, 557, 565, 580, 582, 583, 584, 586, 587, 588, 590, 591, 596,
    599, 608, 609, 614, 616, 623, 624, 625, 627, 630, 640, 642, 643, 644, 646, 649,
    653, 654, 657, 658, 659, 664, 666, 671, 672, 674, 679, 680, 681, 688, 690, 697,
    698, 700, 701, 709, 716, 721, 725, 732, 738, 771, 774, 777, 780, 794, 802, 815,
    816, 841, 849, 851, 852, 854, 859, 873, 884, 886, 893, 900, 909, 919, 922, 932,
    942, 953, 950,955, 957, 958, 963, 971, 975, 983, 986, 999, 1007, 1017, 1018, 1028, 1032, 1033,1039,
    1049, 1051, 1052, 1060, 1064, 1066, 1091, 1094, 1096, 1098, 1100, 1101, 1113, 1125, 1129, 1140, 1148,
    1150, 1154, 1155, 1156, 1159, 1169, 1173, 1175, 1186, 1189, 1193, 1205, 1210, 1215, 1219, 1225,
    1226, 1233, 1234, 1248, 1257, 1259, 1264, 1268, 1269, 1271, 1273, 1279, 1318, 1320, 1323, 1337,
    1346, 1350, 1360, 1366, 1368, 1369, 1372, 1373, 1376, 1379, 1380, 1381, 1382, 1386, 1394, 1395,
    1406, 1409, 1410, 1428, 1433, 1448, 1459, 1460, 1462, 1466, 1482,1512, 3106, 3112, 3128, 3192, 3194,
    3195, 3198, 3221,3235
]
# fmt: on

supply_nodes.append(dod_extra_supply_pump.node_id)


# %%
# Kunstwerk-in/-uit ruimtelijk koppelen aan Ribasim-nodes

kunstwerk_in_path = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "kunstwerk_in.gpkg")
kunstwerk_uit_path = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "kunstwerk_uit.gpkg")

cloud.synchronize(filepaths=[kunstwerk_in_path, kunstwerk_uit_path])

manual_flow_control_nodes = list(flow_control_nodes)
manual_supply_nodes = list(supply_nodes)
manual_drain_nodes = list(drain_nodes)


def get_nearest_control_nodes(
    model: Model,
    kunstwerk_path,
    max_distance: float = 3,
    node_types: list[str] = CONTROL_NODE_TYPES,
    label: str = "kunstwerk",
) -> list[int]:
    kunstwerk_gdf = gpd.read_file(kunstwerk_path, fid_as_index=True).to_crs(model.crs)

    control_node_gdf = model.node.df[model.node.df["node_type"].isin(node_types)].copy()
    control_node_gdf["ribasim_node_id"] = control_node_gdf.index
    control_node_gdf = gpd.GeoDataFrame(control_node_gdf, geometry="geometry", crs=model.crs)

    nearest_gdf = gpd.sjoin_nearest(
        kunstwerk_gdf,
        control_node_gdf[["ribasim_node_id", "node_type", "geometry"]],
        how="left",
        max_distance=max_distance,
        distance_col="distance",
    )

    matched_gdf = (
        nearest_gdf.dropna(subset=["ribasim_node_id"])
        .sort_values(["distance", "ribasim_node_id"])
        .groupby(level=0)
        .first()
    )

    matched_node_ids = matched_gdf["ribasim_node_id"].astype(int).drop_duplicates().to_list()
    unmatched_count = len(kunstwerk_gdf.index.difference(matched_gdf.index))

    print(
        f"{label}: {len(matched_node_ids)} nodes gevonden, "
        f"{unmatched_count} kunstwerken zonder node binnen {max_distance} m"
    )

    return matched_node_ids


def print_node_list_diff(label: str, before_nodes: list[int], after_nodes: list[int]) -> None:
    before_nodes = set(before_nodes)
    after_nodes = set(after_nodes)

    added_nodes = sorted(after_nodes - before_nodes)
    removed_nodes = sorted(before_nodes - after_nodes)

    print(f"{label}: {len(added_nodes)} toegevoegd, {len(removed_nodes)} verwijderd t.o.v. handmatig")
    print(f"{label} toegevoegd: {added_nodes}")
    print(f"{label} verwijderd: {removed_nodes}")


def print_manual_role_conflicts(
    kunstwerk_supply_nodes: list[int],
    kunstwerk_drain_nodes: list[int],
    manual_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> None:
    kunstwerk_supply_nodes = set(kunstwerk_supply_nodes)
    kunstwerk_drain_nodes = set(kunstwerk_drain_nodes)
    manual_supply_nodes = set(manual_supply_nodes)
    manual_drain_nodes = set(manual_drain_nodes)
    manual_flow_control_nodes = set(manual_flow_control_nodes)

    print(
        "kunstwerk_in overruled door handmatig: "
        f"flow_control={sorted(kunstwerk_supply_nodes & manual_flow_control_nodes)}, "
        f"drain={sorted(kunstwerk_supply_nodes & manual_drain_nodes)}"
    )
    print(
        "kunstwerk_uit overruled door handmatig: "
        f"flow_control={sorted(kunstwerk_drain_nodes & manual_flow_control_nodes)}, "
        f"supply={sorted(kunstwerk_drain_nodes & manual_supply_nodes)}"
    )


def combine_control_node_roles(
    kunstwerk_supply_nodes: list[int],
    kunstwerk_drain_nodes: list[int],
    manual_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> tuple[list[int], list[int], list[int]]:
    flow_control_nodes = list(dict.fromkeys(manual_flow_control_nodes))
    supply_nodes = list(dict.fromkeys(node_id for node_id in manual_supply_nodes if node_id not in flow_control_nodes))
    drain_nodes = list(
        dict.fromkeys(
            node_id
            for node_id in manual_drain_nodes
            if node_id not in flow_control_nodes and node_id not in supply_nodes
        )
    )

    manual_nodes = set(flow_control_nodes + supply_nodes + drain_nodes)
    kunstwerk_supply_nodes = list(
        dict.fromkeys(node_id for node_id in kunstwerk_supply_nodes if node_id not in manual_nodes)
    )
    kunstwerk_drain_nodes = list(
        dict.fromkeys(
            node_id
            for node_id in kunstwerk_drain_nodes
            if node_id not in manual_nodes and node_id not in kunstwerk_supply_nodes
        )
    )

    supply_nodes += kunstwerk_supply_nodes
    drain_nodes += kunstwerk_drain_nodes

    return supply_nodes, drain_nodes, flow_control_nodes


kunstwerk_supply_nodes = get_nearest_control_nodes(
    model=model,
    kunstwerk_path=kunstwerk_in_path,
    max_distance=3.0,
    label="kunstwerk_in",
)

kunstwerk_drain_nodes = get_nearest_control_nodes(
    model=model,
    kunstwerk_path=kunstwerk_uit_path,
    max_distance=3.0,
    label="kunstwerk_uit",
)

print_manual_role_conflicts(
    kunstwerk_supply_nodes=kunstwerk_supply_nodes,
    kunstwerk_drain_nodes=kunstwerk_drain_nodes,
    manual_supply_nodes=manual_supply_nodes,
    manual_drain_nodes=manual_drain_nodes,
    manual_flow_control_nodes=manual_flow_control_nodes,
)

supply_nodes, drain_nodes, flow_control_nodes = combine_control_node_roles(
    kunstwerk_supply_nodes=kunstwerk_supply_nodes,
    kunstwerk_drain_nodes=kunstwerk_drain_nodes,
    manual_supply_nodes=manual_supply_nodes,
    manual_drain_nodes=manual_drain_nodes,
    manual_flow_control_nodes=manual_flow_control_nodes,
)

print_node_list_diff("supply_nodes", manual_supply_nodes, supply_nodes)
print_node_list_diff("drain_nodes", manual_drain_nodes, drain_nodes)


# %%
# Toevoegen aanvoergebieden
# Per gebied: links die intersecten die we kunnen negeren.

supply_area_ignore_links = {
    "Ankersmit": [932, 971, 1144, 2315, 2329, 2330, 2388, 2759, 2766],
    "Westerveld": [363, 1000, 1001, 3079, 3080],
    "Overijssels Kanaal": [1057, 1581, 2480, 2685],
    "Vecht-Twentekanalen": [3079, 3080],
    "Dedemsvaart": [287, 2453, 1680, 1094, 1274, 2164, 2344, 3079, 3080],
    "IJsselmeergebied": [1094, 1274, 2164, 2344],
    "Hoogeveense Vaart": [32, 653, 654, 765, 809, 2499, 2519, 2555, 2572, 2757, 2812, 2970, 3079, 3080],
    "Wold Aa": [],
    "Drentse Hoofdvaart": [687, 1024],
    "BoezemNW": [671, 2246, 2247],
}

for area_name, ignore_intersecting_links in supply_area_ignore_links.items():
    print(f"Toevoegen {area_name}")
    add_supply_area_control(
        model=model,
        aanvoergebieden_df=aanvoergebieden_df,
        area_name=area_name,
        ignore_intersecting_links=ignore_intersecting_links,
        supply_nodes=supply_nodes,
        drain_nodes=drain_nodes,
        flow_control_nodes=flow_control_nodes,
        flow_rate_aanvoer=20.0,
        max_flow_rate_aanvoer=max_flow_rate_aanvoer_by_node_id,
        flow_rate_afvoer=100.0,
        max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    )


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
    max_flow_rate_aanvoer=max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# Holthe max_downstream iets lager gezet omdat deze pas aangaat als andere inlaten niet meer kunnen aanleveren.
mask = model.pump.static.df.node_id == 648
model.pump.static.df.loc[mask, "max_downstream_level"] -= 0.04
mark_level_update_protected(model.pump.static.df, mask, model=model)

# Noordscheschutsluis: basin 1920 is gemerged naar 1881; aanvoer stopt 1 cm onder benedenstrooms streefpeil.
noordscheschutsluis_pump_node_id = 346
downstream_basin_ids = model.link.df.loc[
    (model.link.df.from_node_id == noordscheschutsluis_pump_node_id)
    & model.link.df.to_node_id.isin(model.basin.node.df.index),
    "to_node_id",
].unique()
if len(downstream_basin_ids) != 1:
    raise ValueError(
        f"Noordscheschutsluis {noordscheschutsluis_pump_node_id}: "
        f"verwacht 1 benedenstrooms basin, gevonden {downstream_basin_ids}"
    )

downstream_basin_id = int(downstream_basin_ids[0])
downstream_target_level = model.basin.area.df.loc[
    model.basin.area.df.node_id == downstream_basin_id,
    "meta_streefpeil",
].dropna()
if downstream_target_level.empty:
    raise ValueError(
        f"Noordscheschutsluis {noordscheschutsluis_pump_node_id}: "
        f"geen streefpeil gevonden voor benedenstrooms basin {downstream_basin_id}"
    )

max_downstream_level = float(downstream_target_level.iloc[0]) - 0.01
mask = model.pump.static.df.node_id == noordscheschutsluis_pump_node_id
if "control_state" in model.pump.static.df.columns:
    aanvoer_mask = mask & (model.pump.static.df.control_state == "aanvoer")
    if aanvoer_mask.any():
        mask = aanvoer_mask
model.pump.static.df.loc[mask, "max_downstream_level"] = max_downstream_level
mark_level_update_protected(model.pump.static.df, mask, model=model)

set_static_values(
    model.level_boundary.static.df,
    {
        100: -0.2,
        47: -0.2,
        90: -0.2,
    },
    "level",
)


# %%
# EXCLUDE_NODES op 0 m3/s zetten

mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, ["flow_rate", "min_flow_rate", "max_flow_rate"]] = 0.0


# %%
# Junctionify(!)

junctionify(model)


# %%
# Laatste handmatige correcties

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
# Bestaande handmatige pompwaarden blijven leidend.
pump_max_flow_rate_from_results = {
    607: 2,  # Haverslag; oude static flow_rate=0.764
    614: 1,  # Holstweg; oude static flow_rate=0.157
    1095: 1,  # Zwaantje; oude static flow_rate=0
    1344: 2,  # Adsum; gekoppeld max=1.78, huidige max=1.00, link=5901361
    1349: 1,  # Oosterbroek Mallegat; oude static flow_rate=0
}
mask = (
    model.pump.static.df.node_id.isin(pump_max_flow_rate_from_results)
    & model.pump.static.df.flow_rate.notna()
    & (model.pump.static.df.flow_rate > 0)
)
model.pump.static.df.loc[mask, "max_flow_rate"] = model.pump.static.df.loc[mask, "node_id"].map(
    pump_max_flow_rate_from_results
)

# Nieuwe aanvoerpomp: alleen debiet in aanvoer, afvoer blijft 0.
mask = model.pump.static.df.node_id == dod_extra_supply_pump.node_id
model.pump.static.df.loc[mask, "min_upstream_level"] = 14.86
model.pump.static.df.loc[mask, "max_downstream_level"] = 17.7
mark_level_update_protected(model.pump.static.df, mask, model=model)
model.pump.static.df.loc[mask & (model.pump.static.df.control_state == "aanvoer"), ["flow_rate", "max_flow_rate"]] = 1.2
model.pump.static.df.loc[mask & (model.pump.static.df.control_state == "afvoer"), ["flow_rate", "max_flow_rate"]] = 0.0

# Handmatige ondergrens voor node 3235.
for static_df in [model.outlet.static.df, model.pump.static.df]:
    mask = static_df.node_id == 3235
    static_df.loc[mask, "min_upstream_level"] = 17.7
    mark_level_update_protected(static_df, mask, model=model)
    mask = static_df.node_id == 3233
    static_df.loc[mask, "max_downstream_level"] = 12.94
    mark_level_update_protected(static_df, mask, model=model)

# Minimale afvoer naar buitenwater op specifieke kunstwerken, voor alle control_states.
for node_id, min_flow_rate in MIN_FLOW_RATE_BY_NODE_ID.items():
    for static_df in (model.outlet.static.df, model.pump.static.df):
        if "min_flow_rate" not in static_df.columns:
            continue
        static_df.loc[static_df.node_id == node_id, "min_flow_rate"] = min_flow_rate

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

for static_df, max_flow_rate_by_node_id in (
    (model.outlet.static.df, outlet_max_flow_rate_by_node_id),
    (model.pump.static.df, pump_max_flow_rate_all_by_node_id),
):
    max_flow_rate = static_df["node_id"].map(max_flow_rate_by_node_id)
    aanvoer_mask = (
        static_df["control_state"].eq("aanvoer")
        & static_df["node_id"].isin(aanvoer_only_node_ids)
        & max_flow_rate.notna()
    )
    static_df.loc[aanvoer_mask, "flow_rate"] = max_flow_rate[aanvoer_mask]
    static_df.loc[aanvoer_mask, "max_flow_rate"] = max_flow_rate[aanvoer_mask]

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
manual_max_flow_rate_node_ids = set(outlet_max_flow_rate_by_node_id)
manual_max_flow_rate_node_ids.update(pump_max_flow_rate_by_node_id)
manual_max_flow_rate_node_ids.add(dod_extra_supply_pump.node_id)
protected_max_flow_rate_node_ids = set(EXCLUDE_NODES) | flow_demand_controlled_node_ids | manual_max_flow_rate_node_ids
for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = (
        static_df["control_state"].eq("afvoer")
        & static_df["flow_rate"].fillna(0).gt(0)
        & ~static_df["node_id"].isin(protected_max_flow_rate_node_ids)
    )
    static_df.loc[afvoer_mask, "max_flow_rate"] = static_df.loc[afvoer_mask, "max_flow_rate"].fillna(10).clip(lower=10)

for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = static_df["control_state"].eq("afvoer") & static_df["node_id"].isin(aanvoer_only_node_ids)
    static_df.loc[afvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

# Vechterweerd moet in beide richtingen de volledige capaciteit behouden.
vechterweerd_mask = model.outlet.static.df.node_id.isin([199, 2582])
model.outlet.static.df.loc[vechterweerd_mask, ["flow_rate", "max_flow_rate"]] = 200.0

# %%
# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=1)
model = run_model_and_control(model, ribasim_toml_dry, qlr_path)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model = run_model_and_control(model, ribasim_toml_wet, qlr_path)

# hoofd run
update_basin_static(model=model, precipitation_mm_per_day=1.5)
model = run_model_and_control(model, ribasim_toml, qlr_path)

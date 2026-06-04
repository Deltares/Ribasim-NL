# %%

import inspect
from datetime import datetime
from typing import Literal

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim.nodes import flow_demand, outlet
from ribasim_nl.control import (
    _offset_new_node,
    _target_level,
    get_node_table_with_from_to_node_ids,
    mark_level_update_protected,
)
from ribasim_nl.control import (
    add_controllers_to_supply_area as _add_controllers_to_supply_area,
)
from ribasim_nl.control import (
    add_controllers_to_supply_nodes as _add_controllers_to_supply_nodes,
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


def _call_with_supported_kwargs(func, *args, **kwargs):
    signature = inspect.signature(func)
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()):
        return func(*args, **kwargs)
    return func(*args, **{key: value for key, value in kwargs.items() if key in signature.parameters})


def add_controllers_to_supply_area(*args, **kwargs):
    kwargs.setdefault("supply_flow_rate", _supply_flow_rate_by_node_id())
    kwargs.setdefault("drain_flow_rate", _supply_flow_rate_by_node_id())
    return _call_with_supported_kwargs(_add_controllers_to_supply_area, *args, **kwargs)


def add_controllers_to_supply_nodes(*args, **kwargs):
    kwargs.setdefault("supply_flow_rate", _supply_flow_rate_by_node_id())
    return _call_with_supported_kwargs(_add_controllers_to_supply_nodes, *args, **kwargs)


def add_controllers_to_uncontrolled_connector_nodes(*args, **kwargs):
    kwargs.setdefault("supply_flow_rate", _supply_flow_rate_by_node_id())
    kwargs.setdefault("drain_flow_rate", _supply_flow_rate_by_node_id())
    return _call_with_supported_kwargs(_add_controllers_to_uncontrolled_connector_nodes, *args, **kwargs)


# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "AaenMaas"
SHORT_NAME: str = "aam"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 745: Sluis Engelen, alles via Crevecoeur
EXCLUDE_NODES = {247, 584, 745, 955, 956, 960, 980, 680, 237, 270, 330, 737}
outlet_max_flow_rate_by_node_id = {
    161: 12.0,  # Schabbert
}
pump_max_flow_rate_by_node_id = {
    100: 5.0,  # Gemaal Veluwe
    105: 1.0,  # Grote Wetering
}

flushing_nodes: dict[int, float] = {}
drain_nodes = [
    85,
    92,
    280,
    312,
    353,
    369,
    400,
    1089,
    534,
    558,
    1047,
    106,
    130,
    153,
    157,
    186,
    207,
    250,
    268,
    275,
    299,
    350,
    385,
    408,
    419,
    621,
    804,
    628,
    818,
    1051,
    83,
    107,
    139,
    150,
    170,
    210,
    244,
    256,
    342,
    388,
    421,
    304,
    487,
    597,
    982,
    345,
    486,
    656,
    670,
    795,
    797,
    1046,
    847,
    760,
    905,
    122,
    843,
    318,
    324,
    669,
    676,
    529,
    539,
    217,
    294,
    668,
    549,
    288,
    469,
    479,
    632,
    81,
    919,
    920,
    366,
    452,
    615,
    922,
    194,
    151,
    182,
    144,
    509,
    594,
    667,
    1016,
    272,
    370,
    409,
    430,
    459,
    96,
    221,
    923,
    924,
    940,
    941,
    2017,
    998,
    937,
    112,
    159,
    198,
    394,
    135,
    257,
    138,
    336,
    356,
    395,
    321,
    252,
    119,
    410,
    414,
    736,
    126,
    488,
    925,
    209,
    551,
    265,
    266,
    303,
    341,
    398,
    872,
    444,
    120,
    267,
    281,
    292,
    360,
    470,
    613,
    571,
    577,
    691,
    747,
    777,
    748,
    891,
    911,
    971,
    981,
    1067,
]
supply_nodes: list[int] = [2015, 307, 309]
flow_control_nodes = [
    3097,
    1093,
    681,
    774,
    775,
    776,
    934,
    974,
    130,
    140,
    347,
    377,
    481,
    496,
    628,
    766,
    767,
    573,
    718,
    768,
    806,
    808,
    810,
    813,
    814,
    815,
    818,
    819,
    821,
    948,
    957,
    1050,
    243,
    155,
    212,
    332,
    823,
    824,
    1051,
    1062,
    100,
    205,
    239,
    413,
    438,
    526,
    533,
    790,
    791,
    952,
    328,
    361,
    352,
    708,
    154,
    158,
    177,
    338,
    538,
    622,
    710,
    826,
    828,
    954,
    383,
    269,
    886,
    748,
    218,
    469,
    127,
    249,
    697,
    698,
    863,
    864,
    362,
    490,
    494,
    915,
    755,
    271,
    857,
    858,
    866,
    867,
    868,
    990,
    380,
    849,
    859,
    865,
    1001,
    1014,
    1018,
    1058,
    370,
    722,
    1070,
    333,
    701,
    822,
    917,
    916,
]


def add_discharge_supply_nodes(
    discharge_supply_nodes: dict[Literal["summer", "winter"] : float],
    us_target_level_offset_supply: float = -0.04,
    new_nodes_offset: float = 10,
    demand_node_angle: int = 45,
    demand_name_prefix: str = "inlaat",
):
    # get tables and nodes
    node_table_df = get_node_table_with_from_to_node_ids(model, node_ids=list(discharge_supply_nodes.keys()))
    node_types = model.node.df["node_type"]

    # demand parameters
    summer_season_start: tuple[int, int] = (4, 1)
    winter_season_start: tuple[int, int] = (10, 1)
    time = [
        datetime(2020, *summer_season_start),
        datetime(2020, *winter_season_start),
        datetime(2021, *summer_season_start),
    ]

    for node_id, demand in discharge_supply_nodes.items():
        print(node_id)
        demand_flow_rate_summer = demand["summer"]
        demand_flow_rate_winter = demand["winter"]

        print(f"Adding supply_node {node_id}: summer={demand_flow_rate_summer} | winter={demand_flow_rate_winter}")
        demand_flow_str = (
            str(demand_flow_rate_summer)
            if demand_flow_rate_summer == demand_flow_rate_winter
            else f"{demand_flow_rate_summer}/{demand_flow_rate_winter}"
        )

        # update static table
        us_target_level = _target_level(
            model=model,
            node_id=node_table_df.at[node_id, "from_node_id"],
            target_level_column="meta_streefpeil",
            node_types=node_types,
            allow_missing=False,
        )

        model.update_node(
            node_id,
            "Outlet",
            [
                outlet.Static(
                    min_upstream_level=[us_target_level + us_target_level_offset_supply],
                    flow_rate=[0],
                    min_flow_rate=[float("nan")],
                    max_flow_rate=[float("nan")],
                )
            ],
        )

        # demand parameters
        demand_node_name = f"{demand_name_prefix} {demand_flow_str} [m3/s]"
        demand_tables = [
            flow_demand.Time(
                time=time,
                demand=[
                    float(demand_flow_rate_summer),
                    float(demand_flow_rate_winter),
                    float(demand_flow_rate_summer),
                ],
                demand_priority=[1, 1, 1],
            )
        ]
        cyclic = True
        node = model.get_node(node_id=node_id)
        demand_node = model.flow_demand.add(
            _offset_new_node(
                node=node,
                offset=new_nodes_offset,
                angle=demand_node_angle,
                name=demand_node_name,
                cyclic_time=cyclic,
            ),
            tables=demand_tables,
        )
        model.link.add(demand_node, node)


# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(r"AaenMaas/verwerkt/sturing/aanvoergebieden.gpkg")
aanvoerpunten_shp = cloud.joinpath(
    "AaenMaas/verwerkt/1_ontvangen_data/wateraanvoer_27-2-2026/wateraanvoersysteem_WAM.shp"
)

cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path, aanvoerpunten_shp])

# %%
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 20
model.outlet.static.df.flow_rate = 20
model.pump.static.df.loc[model.pump.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate

# fixes
# Schabbert
model.outlet.static.df.loc[model.outlet.static.df.node_id == 161, "flow_rate"] = 12

# Was manning, wordt outlet, duiker
model.update_node(node_id=549, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=670, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=486, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=656, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=537, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=627, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=631, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=669, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=712, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=464, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=491, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=483, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=491, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=595, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=697, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=1043, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=720, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=610, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=450, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=706, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=529, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=539, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=652, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=675, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=676, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=511, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=655, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=558, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=1047, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=630, node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=444, node_type="Outlet")  # wordt outlet, was Manning

model.remove_node(820, remove_links=True)
model.remove_node(1054, remove_links=True)

# Mierlo wordt aanvoer als afvoer gemaal
model.update_node(node_id=92, node_type="Pump")  # wordt outlet, was outlet
model.update_node(node_id=226, node_type="Pump")  # wordt outlet, was outlet


# Teveel inlaten naar Oefeltse Raam
model.remove_node(574, remove_links=True)


# %%
# Toevoegen alle aanvoer-knopen met flow_demand
meta_codes = {
    # algemeen
    246: "294EC",
    298: "WSL_Meij",
    968: "275JD",
    875: "287GB",
    397: "287DF",
    894: "275ZKS",
    367: "261LGD",
    # Bakelse Aa
    3095: "261CZA",
    601: "261JS",
    905: "261CQA",
    761: "261JN",
    156: "261JIA",
    # Peelsche Loop
    358: "261N",
    535: "253V",
    3091: "251U",
    417: "253LG",
    829: "251LAA",
    # Maaskant-Oost
    203: "114P18",
    850: "114FF",
    144: "108TDE",
    2022: "WSL_VBMB",
    657: "114LAC",
    3089: "114WRD",
    # Maaskant-Midden
    160: "104MGT",
    392: "103BIB",
    # Mierlo
    226: "234FL_1",
    340: "217L",
    # Whkanaal
    314: "234CJ",
    # Z_W West
    211: "203KDZ",
    # Helmond
    737: "201JVL",
    # Peelrijt Dommel
    3094: "280X",
    # Hurkske Aa
    161: "257IA",
    93: "254BAA",
    191: "235VGZ",
}

for node_id, code in meta_codes.items():
    model.node.df.at[node_id, "meta_code_waterbeheerder"] = code

summer_col = "ZOMER_DROO"
winter_col = "WINTER"
code_col = "CODE_1"
discharge_supply_df = gpd.read_file(aanvoerpunten_shp).rename(
    columns={summer_col: "summer", winter_col: "winter", code_col: "code"}
)[["code", "summer", "winter", "geometry"]]
discharge_supply_df.index += 1

# drop Na
discharge_supply_df = discharge_supply_df[discharge_supply_df[["summer", "winter"]].notna().all(axis=1)]

# drop %
discharge_supply_df = discharge_supply_df[
    ~(discharge_supply_df["summer"].str.endswith("%") | discharge_supply_df["winter"].str.endswith("%"))
]

# convert to numeric
discharge_supply_df["summer"] = discharge_supply_df["summer"].str.replace(",", ".").astype(float)
discharge_supply_df["winter"] = discharge_supply_df["winter"].str.replace(",", ".").astype(float)

# make code (and node type) table
node_table_df = model.node.df


code_df = node_table_df[node_table_df["meta_code_waterbeheerder"].notna()][
    ["meta_code_waterbeheerder", "node_type"]
].rename(columns={"meta_code_waterbeheerder": "code"})
code_df = code_df.reset_index(drop=False).set_index("code")

discharge_supply_df = discharge_supply_df[discharge_supply_df["code"].isin(code_df.index.to_list())]
discharge_supply_df["node_id"] = [code_df.at[i, "node_id"] for i in discharge_supply_df.code.values]
discharge_supply_nodes = {
    int(row.node_id): {"summer": row.summer, "winter": row.winter} for row in discharge_supply_df.itertuples()
}

# checken in laatste file Aa en Maas; dit kúnnen geen inlaten zijn
discharge_supply_nodes.pop(383, None)
discharge_supply_nodes.pop(957, None)
discharge_supply_nodes.pop(100, None)

discharge_supply_df.to_file(cloud.joinpath("AaenMaas/verwerkt/sturing/aanvoerpunten.gpkg"))


# add level supply nodes, no flow-demand-node, but discrete control on downstream basin level
level_supply_nodes = [
    80,
    99,
    149,
    160,
    161,
    166,
    181,
    183,
    186,
    203,
    208,
    211,
    215,
    227,
    231,
    246,
    251,
    276,
    278,
    308,
    335,
    355,
    375,
    379,
    392,
    406,
    510,
    521,
    527,
    531,
    566,
    625,
    630,
    640,
    723,
    731,
    734,
    753,
    850,
    909,
    985,
    2020,
    3086,
]

double_defined = [i for i in level_supply_nodes if i in discharge_supply_nodes]
if double_defined:
    raise ValueError(f"these nodes are labelled as q-supply and level-supply {double_defined}")

supply_nodes_df = get_node_table_with_from_to_node_ids(model=model, node_ids=level_supply_nodes)

all_nodes = list(discharge_supply_nodes.keys()) + level_supply_nodes

model.node.df[IS_SUPPLY_NODE_COLUMN] = model.node.df.index.isin(all_nodes)


# Gemaal Veluwe
model.update_node(node_id=100, node_type="Pump")  # wordt outlet, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 100, "flow_rate"] = 5

# Gemaal Kameren
model.update_node(node_id=95, node_type="Pump")  # wordt outlet, was outlet


# Grote Wetering is een gemaal
model.update_node(node_id=105, node_type="Pump")  # wordt pump was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 105, "flow_rate"] = 1


# %%
# Toevoegen Mierlo

polygon = aanvoergebieden_df.loc[["Mierlo"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    440,
    575,
    600,
    621,
    640,
    668,
    718,
    725,
    968,
    969,
    1229,
    1434,
    1562,
    1670,
    1671,
    1672,
    1674,
    1944,
    2936,
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)
# %%
# Toevoegen Leijgraaf

polygon = aanvoergebieden_df.loc[["Leijgraaf"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    43,
    59,
    60,
    237,
    299,
    309,
    460,
    470,
    479,
    485,
    636,
    729,
    979,
    1027,
    1348,
    1443,
    1450,
    1526,
    1542,
    1552,
    1617,
    1635,
    1672,
    1712,
    1962,
    713,
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)
# %%
# Toevoegen Hurkske/Aa

polygon = aanvoergebieden_df.loc[["Hurkske/Aa"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    5,
    6,
    7,
    16,
    40,
    43,
    172,
    173,
    258,
    299,
    460,
    466,
    467,
    479,
    979,
    1146,
    1227,
    1397,
    1440,
    1483,
    1610,
    1962,
    1978,
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)

# %%
# Toevoegen Z-W West (Zuid-Willemsvaart West)

polygon = aanvoergebieden_df.loc[["Z-W West"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    5,
    6,
    7,
    221,
    253,
    265,
    266,
    267,
    622,
    624,
    638,
    694,
    1469,
    1479,
    1670,
    1671,
    1672,
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)


# %%
# Toevoegen Bakelse Aa

polygon = aanvoergebieden_df.loc[["Bakelse Aa"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [487, 489, 745, 754, 755, 760, 1586, 1623]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)
# %%
# Toevoegen Peelsche Loop

polygon = aanvoergebieden_df.loc[["Peelsche Loop"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [767, 1259]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)
# %%
# Toevoegen Neerkant

polygon = aanvoergebieden_df.loc[["Neerkant"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [472, 704, 781, 1409, 1650]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)


# %%
# Toevoegen Someren

polygon = aanvoergebieden_df.loc[["Someren"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [327, 328, 384, 385]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)
# %%
# Toevoegen Helmond

polygon = aanvoergebieden_df.loc[["Helmond"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [706, 968, 969, 1229, 1944]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)


# %%
# Toevoegen Whkanaal (Wilhelminakanaal)

polygon = aanvoergebieden_df.loc[["Whkanaal"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [273, 274, 440, 605, 606, 607, 1518, 1648, 1674, 221, 638, 694, 1479]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)


# %%
# Toevoegen Maaskant-Oost

polygon = aanvoergebieden_df.loc[["Maaskant-Oost"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    138,
    143,
    175,
    190,
    388,
    476,
    499,
    528,
    529,
    603,
    604,
    648,
    649,
    659,
    660,
    676,
    801,
    981,
    1254,
    1458,
    1459,
    1481,
    1498,
    1544,
    1570,
    1611,
    1660,
    1694,
    1930,
    1933,
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)

# %%
# Toevoegen Maaskant-West

polygon = aanvoergebieden_df.loc[["Maaskant-West"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [928, 930, 282, 2018]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)


# %%
# Toevoegen Maaskant-Midden

polygon = aanvoergebieden_df.loc[["Maaskant-Midden"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    408,
    60,
    237,
    258,
    362,
    376,
    420,
    654,
    656,
    657,
    994,
    997,
    1065,
    1435,
    1492,
    1530,
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    add_supply_nodes=False,
)


# %%
# EXCLUDE NODES op 0 m3/s zetten

mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, "flow_rate"] = 0
model.outlet.static.df.loc[mask, "min_flow_rate"] = 0
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0

# %% Toevoegen sturing op inlaten

# add discharge supply nodes -> no control, but flow-demand-node
add_discharge_supply_nodes(discharge_supply_nodes=discharge_supply_nodes)

# add level supply nodes -> discrete control, no flow-demand
add_controllers_to_supply_nodes(
    model=model,
    us_target_level_offset_supply=-0.04,
    supply_nodes_df=supply_nodes_df,
)

# %% add all remaining inlets/outlets
add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
)

# %%

# Crevecoeur
mask = (model.outlet.static.df.node_id == 2018) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[mask, "flow_rate"] = 300
model.outlet.static.df.loc[mask, "max_flow_rate"] = 300

# 220: #253ZOM Deze stuw zit niet in model en lastig in te bouwen met huidige basins. Daarom Outlet 220 (253BG) en flow op 0 gezet
model.outlet.static.df.loc[model.outlet.static.df.node_id == 220, "flow_rate"] = 0

# Procentuele Verdeling Peelsche Loop uitlaten naar aanvoergebied Leygraaf opgelegd door flow_rate
model.outlet.static.df.loc[model.outlet.static.df.node_id == 680, "flow_rate"] = 50
model.outlet.static.df.loc[model.outlet.static.df.node_id == 955, "flow_rate"] = 25
model.outlet.static.df.loc[model.outlet.static.df.node_id == 956, "flow_rate"] = 20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 584, "flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 247, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 960, "flow_rate"] = 16.67
model.outlet.static.df.loc[model.outlet.static.df.node_id == 980, "flow_rate"] = 5

model.outlet.static.df.loc[model.outlet.static.df.node_id == 680, "max_flow_rate"] = 50
model.outlet.static.df.loc[model.outlet.static.df.node_id == 955, "max_flow_rate"] = 25
model.outlet.static.df.loc[model.outlet.static.df.node_id == 956, "max_flow_rate"] = 20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 584, "max_flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 247, "max_flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 960, "max_flow_rate"] = 16.67
model.outlet.static.df.loc[model.outlet.static.df.node_id == 980, "max_flow_rate"] = 5

reverse_link_ids = [226, 1172]


for link_id in reverse_link_ids:
    model.reverse_link(link_id=link_id)


# Inlaat Waranda pump
model.update_node(node_id=3089, node_type="Pump")  # wordt pump was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 3089, "flow_rate"] = 0
mask = model.pump.static.df.node_id == 3089
model.pump.static.df.loc[mask, "min_upstream_level"] = 10.36
mark_level_update_protected(model.pump.static.df, mask)

# Inlaat Sambeek
model.update_node(node_id=124, node_type="Pump")  # wordt pump, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 124, "flow_rate"] = 0

# Gemaal Mierlo
model.update_node(node_id=226, node_type="Pump")  # wordt pump, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 226, "flow_rate"] = 0

# Inlaten aan Drongelens kanaal krijgen pd.NA bij min_upstream_level
mask = model.outlet.static.df.node_id.isin([98, 103])
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
mark_level_update_protected(model.outlet.static.df, mask)

boundary_ids = [9, 13, 39, 38, 53, 1958, 1568, 3085, 33, 32, 31, 59, 54, 44, 42, 64, 63]
mask = model.level_boundary.static.df["node_id"].isin(boundary_ids)
model.level_boundary.static.df.loc[mask, "level"] = model.level_boundary.static.df.loc[mask, "level"] + 0.04

# 201JSL is een RWS-inlaat naar Aa en Maas, geen afvoer richting Aa en Maas.
mask = (model.outlet.static.df.node_id == 309) & (model.outlet.static.df.control_state == "aanvoer")
model.outlet.static.df.loc[mask, "flow_rate"] = 20
model.outlet.static.df.loc[mask, "max_flow_rate"] = 20
mask = (model.outlet.static.df.node_id == 309) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[mask, "flow_rate"] = 0
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0

# %%
# Corrigeer basin-peilen/profielen langs open Manning-routes nadat alle full-control-controllers bekend zijn.
PROTECTED_MANNING_BASIN_NODE_IDS = [1149, 1331, 1419, 1446, 1475, 1565, 1572, 1665, 1801, 1852, 1885, 1959]
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

# Node 291 remains 4 cm below its upstream basin bottom after the Manning sync.
mask = (model.outlet.static.df.node_id == 291) & (model.outlet.static.df.min_upstream_level < 15.1)
model.outlet.static.df.loc[mask, "min_upstream_level"] = 15.1
mark_level_update_protected(model.outlet.static.df, mask)

# %% Junctionfy(!)
junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime


# %%

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=0.1)
model.starttime = datetime(2020, 5, 1)
model.endtime = datetime(2020, 9, 1)
sync_manning_level_controls(model)
model.write(ribasim_toml_dry)

# run hoofdmodel
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml_dry, qlr_path=qlr_path).run_all()
    model = Model.read(ribasim_toml_dry)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model.starttime = datetime(2020, 1, 1)
model.endtime = datetime(2020, 4, 1)
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

# %%

from datetime import datetime
from typing import Literal

import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim.nodes import flow_demand, outlet
from ribasim_nl.control import (
    _offset_new_node,
    _target_level,
    add_controllers_to_supply_area,
    add_controllers_to_supply_nodes,
    add_controllers_to_uncontrolled_connector_nodes,
    get_node_table_with_from_to_node_ids,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "AaenMaas"
SHORT_NAME: str = "aam"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 745: Sluis Engelen, alles via Crevecoeur
EXCLUDE_NODES = {247, 584, 745, 955, 956, 960, 980, 680}


def add_discharge_supply_nodes(
    discharge_supply_nodes: dict[Literal["summer", "winter"] : float],
    us_target_level_offset_supply: float = -0.04,
    new_nodes_offset: float = 10,
    demand_node_angle: int = 45,
    demand_name_prefix: str = "inlaat",
):
    # get tables and nodes
    node_table_df = get_node_table_with_from_to_node_ids(model, node_ids=list(discharge_supply_nodes.keys()))
    node_types = model.node_table().df["node_type"]

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
    r"AaenMaas\verwerkt\1_ontvangen_data\wateraanvoer_27-2-2026\wateraanvoersysteem_WAM.shp"
)

cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path, aanvoerpunten_shp])

# %%
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 30
model.outlet.static.df.flow_rate = 30
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate

# fixes
# Schabbert
model.outlet.static.df.loc[model.outlet.static.df.node_id == 161, "flow_rate"] = 12


# Mierlo wordt aanvoer als afvoer gemaal
model.update_node(node_id=92, node_type="Pump")  # wordt outlet, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 92, "min_upstream_level"] = 16.56
model.update_node(node_id=226, node_type="Pump")  # wordt outlet, was outlet

# Teveel inlaten naar Oefeltse Raam
model.remove_node(295, remove_links=True)
model.remove_node(574, remove_links=True)

# Gemaal Veluwe
model.update_node(node_id=100, node_type="Pump")  # wordt outlet, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 100, "min_upstream_level"] = 10.82

# Gemaal Kameren
model.update_node(node_id=95, node_type="Pump")  # wordt outlet, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 95, "min_upstream_level"] = 5.17

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

discharge_supply_df.to_file(cloud.joinpath(r"AaenMaas\verwerkt\sturing\aanvoerpunten.gpkg"))


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
    211,
    215,
    227,
    251,
    276,
    278,
    280,
    308,
    335,
    369,
    375,
    379,
    392,
    406,
    510,
    521,
    527,
    531,
    640,
    734,
    753,
    850,
    985,
    1054,
    2020,
]

double_defined = [i for i in level_supply_nodes if i in discharge_supply_nodes.keys()]
if double_defined:
    raise ValueError(f"these nodes are labelled as q-supply and level-supply {double_defined}")

supply_nodes_df = get_node_table_with_from_to_node_ids(model=model, node_ids=level_supply_nodes)

all_nodes = list(discharge_supply_nodes.keys()) + level_supply_nodes

model.node.df[IS_SUPPLY_NODE_COLUMN] = model.node.df.index.isin(all_nodes)

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [85, 92, 312, 353, 400, 1089]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [226]
# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [681, 774, 775, 776, 934, 974]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [
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
]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [186, 251, 278, 379]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [
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
    807,
    808,
    810,
    813,
    814,
    815,
    818,
    819,
    821,
    948,
    981,
    1050,
]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [83, 107, 139, 150, 170, 210, 244, 256, 342, 421, 304, 487, 597, 982, 1051]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [183, 375, 521, 640, 1054]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [155, 212, 332, 388, 823, 824, 1051, 1062]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [345, 795, 797, 1046]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [166, 211]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [205, 239, 413, 438, 526, 533, 790, 791, 952]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [847, 760, 905]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = []

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [328, 361, 352, 708]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [122, 843, 318]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [227]
# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [154, 158, 177, 338, 538, 622, 710, 826, 828, 909, 954]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [324]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = [335]
# handmatig opgegeven supply nodes (inlaten)

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [246]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [269, 886]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [217, 294, 668]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven supply nodes (inlaten)
flow_control_nodes = [748]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [288, 469, 479, 632]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [355, 510, 531]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [218, 469]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [81, 919, 920, 366, 452, 615, 922, 194, 151, 182, 144, 509, 594, 667, 1016]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [203, 753, 850]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [
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
    755,
    990,
    380,
    849,
    859,
    865,
    1001,
    1014,
    1018,
    1058,
]


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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [370, 430, 459, 96, 221, 923, 924, 2017]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [734]

# handmatig opgegeven supply nodes (inlaten)

flow_control_nodes = [722, 723, 1067, 1070]

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

# doorspoeling (op uitlaten)
#

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
#

drain_nodes = [
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
]

# handmatig opgegeven supply nodes (inlaten)

supply_nodes = [80, 160, 308, 181, 985, 392, 215]

# handmatig opgegeven supply nodes (inlaten)

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
# add all remaing outlets
# handmatig opgegeven flow control nodes definieren

flow_control_nodes = [333, 701, 822, 917, 916]

# handmatig opgegeven supply nodes (inlaten)
#


supply_nodes = [2020, 2022, 527, 1502, 3086]
#

drain_nodes = [120, 210, 267, 281, 292, 309, 360, 470, 613, 571, 577, 691, 747, 777, 748, 891, 971]


# Flushing nodes
# flushing_nodes = {919: 5}
flushing_nodes = {}


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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2018, "max_flow_rate"] = 300

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

model.outlet.static.df.loc[model.outlet.static.df.node_id == 680, "min_upstream_level"] = 18.97
model.outlet.static.df.loc[model.outlet.static.df.node_id == 955, "min_upstream_level"] = 17.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 956, "min_upstream_level"] = 17.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 584, "min_upstream_level"] = 17.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 247, "min_upstream_level"] = 18.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 960, "min_upstream_level"] = 18.97
model.outlet.static.df.loc[model.outlet.static.df.node_id == 980, "min_upstream_level"] = 11.85


# Gemaal Veluwe
model.update_node(node_id=100, node_type="Pump")  # wordt pump was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 100, "flow_rate"] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 100, "min_upstream_level"] = 10.78

# Inlaat Waranda pump
model.update_node(node_id=3089, node_type="Pump")  # wordt pump was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 3089, "flow_rate"] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 3089, "min_upstream_level"] = 10.36

# Inlaat Sambeek
model.update_node(node_id=124, node_type="Pump")  # wordt pump, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 124, "flow_rate"] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 124, "min_upstream_level"] = 10.36

# Gemaal Mierlo
model.update_node(node_id=226, node_type="Pump")  # wordt pump, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == 226, "flow_rate"] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 226, "min_upstream_level"] = 14.3

# Alle inlaten met demand_nodes moeten min_upstream_level van streefpeil hebben zodat ze in afvoerstand staan (+0.04m)
#
outlet_ids = [
    259,
    298,
    3818,
    283,
    390,
    894,
    335,
    367,
    3089,
    3090,
    3095,
    601,
    156,
    358,
    535,
    996,
    3091,
    657,
    2022,
    935,
    997,
    226,
    3094,
    2025,
    340,
]
mask = model.outlet.static.df["node_id"].isin(outlet_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = model.outlet.static.df.loc[mask, "min_upstream_level"] + 0.04

boundary_ids = [9, 13, 39, 38, 53, 1958, 1568, 3085, 33, 32, 31, 59, 54, 44, 42, 64, 63]
mask = model.level_boundary.static.df["node_id"].isin(boundary_ids)
model.level_boundary.static.df.loc[mask, "level"] = model.level_boundary.static.df.loc[mask, "level"] + 0.04

# %% Junctionfy(!)
model = junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime


# %%

# hoofd run met verdamping
model.starttime
update_basin_static(model=model, evaporation_mm_per_day=0.1)
model.starttime = datetime(2020, 5, 1)
model.endtime = datetime(2020, 9, 1)
model.write(ribasim_toml_dry)

# run hoofdmodel
if MODEL_EXEC:
    result = model.run()
    controle_output = Control(ribasim_toml=ribasim_toml_dry, qlr_path=qlr_path)
    indicators = controle_output.run_all()
    model = Model.read(ribasim_toml_dry)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model.starttime = datetime(2020, 1, 1)
model.endtime = datetime(2020, 4, 1)
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

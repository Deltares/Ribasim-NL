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
AUTHORITY: str = "Limburg"
SHORT_NAME: str = "limburg"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {651}

# %%

# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(r"Limburg/verwerkt/sturing/aanvoergebieden.gpkg")

cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])

# %%#
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


# alle uitlaten en inlaten op 20m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 20
model.outlet.static.df.flow_rate = 20
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate

# %%
# Node 651 moet dicht zijn na overleg Limburg
model.pump.static.df.loc[model.pump.static.df.node_id == 651, "max_flow_rate"] = 0

# Peelkanaal op grenssloot ligt in verkeerde richting
for link_id in [529, 1036]:
    model.reverse_link(link_id=link_id)


# %%
# Note: when using a FlowDemand, the parallel node must be configured as a drain node.
# The node with the FlowDemand is supplied first, because its min_upstream_level
# is lower than that of the Outlet. Any remaining discharge is then conveyed
# through the Outlet.


def add_discharge_supply_nodes(
    discharge_supply_nodes: dict[int, dict[Literal["summer", "winter"], float]],
    us_target_level_offset_supply: float = -0.04,
    new_nodes_offset: float = 10,
    demand_node_angle: int = 45,
    demand_name_prefix: str = "inlaat",
):
    # get tables and nodes
    node_table_df = get_node_table_with_from_to_node_ids(
        model,
        node_ids=list(discharge_supply_nodes.keys()),
    )
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

        # bepaal upstream target level
        us_target_level = _target_level(
            model=model,
            node_id=node_table_df.at[node_id, "from_node_id"],
            target_level_column="meta_streefpeil",
            node_types=node_types,
            allow_missing=False,
        )

        # zorg dat node een Outlet is
        model.update_node(node_id, "Outlet")

        # update outlet static
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

        # demand node toevoegen
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

        # link flow_demand -> outlet
        model.link.add(demand_node, node)


# %%
# Handmatige koppeling naam -> node_id
name_to_node = {
    "Gemaal Beringe": 583,
    "Gemaal Helenaveen": 590,
    "Zijtak Helenavaart": 532,
    "Houtstraatlossing": 351,
    "Hushoverbeek": 2502,
    "Inlaat Groote Moost": 236,
    "Achterste Moost": 410,
    "Inlaat Hulsenlossing": 1223,
    "Inlaat Kleine Moost": 410,
    "Inlaat Rietbeek": 599,
    "Inlaat Weteringbeek": 731,
    "Kampershoek": 2503,
    "Katsberg": 238,
    "Nederweerter Hovenlossing": 534,
    "Nederweerter Riet": 464,
    "Oude Graaf": 1136,
    "Snepheiderbeek": 411,
    "Snepheiderbeek_1": 750,
    "Waatskamplossing": 598,
    "Eendlossing": 772,
    "Inlaat Evertsoord": 604,
    "Klein Leukerbeek": 1119,
    "Roeven": 2501,
    "Halte Grenssloot": 773,
    "Halte Peelkanaal": 535,
    "Peelkanaal naar Grenssloot": 821,
    "Molenakker": 2504,
    "Zwartwaterlossing": 230,
    "AVL Staart": 819,
    "Bruine Peelloop": 659,
    "De Graskuilen": 1205,
    "Boksloot": 1203,
    "Dekershorst": 197,
    "Everlose beek": 256,
    "Lange Heide": 709,
}

# %%
# Debieten in liter per seconde
flow_demand_data_ls = {
    # Zuid-Willemsvaart
    "Inlaat Weteringbeek": {"summer": 80, "winter": 40},
    "Oude Graaf": {"summer": 10, "winter": 10},
    "Houtstraatlossing": {"summer": 30, "winter": 10},
    "Nederweerter Riet": {"summer": 10, "winter": 10},
    "Hushoverbeek": {"summer": 10, "winter": 10},  # uit "wijk Hushoven"
    "Kampershoek": {
        "summer": 10,
        "winter": 10,
    },  # tabelregel heeft geen expliciete waarden; hier conservatief gelijk gehouden
    # Noordervaart
    "Inlaat Hulsenlossing": {"summer": 10, "winter": 10},
    "Inlaat Rietbeek": {"summer": 25, "winter": 10},
    "Waatskamplossing": {"summer": 15, "winter": 10},
    "Snepheiderbeek": {"summer": 40, "winter": 20},  # tabel geeft bandbreedte zomer 10-20, winter 30-40 (verkeerd om)
    "Snepheiderbeek_1": {"summer": 40, "winter": 20},  # Bovenstrooms Snepheiderbeek is echte inlaat
    # Grote aanvoeren
    "Gemaal Beringe": {"summer": 550, "winter": 350},
    "Gemaal Helenaveen": {"summer": 350, "winter": 350},  # winter in tabel: laatste 2 jaar 350, normaal 150
    "Zijtak Helenavaart": {"summer": 350, "winter": 350},  # winter in tabel: laatste 2 jaar
    "Katsberg": {"summer": 3400, "winter": 3400},  # winter in tabel 1000-3400; hier bovengrens gekozen
    "Eendlossing": {"summer": 20, "winter": 10},  # WATAK
    "Klein Leukerbeek": {"summer": 10, "winter": 10},  # GIHO kaart
    "Roeven": {"summer": 10, "winter": 10},  # WATAK
    # afgeleid uit relevante tabelregels
    "Inlaat Evertsoord": {"summer": 350, "winter": 350},  # gekoppeld aan AVL Evertsoord / gemaal Helenaveen
    "Halte Grenssloot": {"summer": 650, "winter": 600},
    "Halte Peelkanaal": {
        "summer": 1500,
        "winter": 500,
    },
    "Peelkanaal naar Grenssloot": {"summer": 75, "winter": 50},
    "Molenakker": {"summer": 20, "winter": 20},  # inclusief: Zuid_Willemsvaart op AVL Ringbaan Oost
    "Nederweerter Hovenlossing": {"summer": 10, "winter": 10},
    "Inlaat Groote Moost": {"summer": 20, "winter": 0},
    "Achterste Moost": {"summer": 20, "winter": 0},
    "Zwartwaterlossing": {"summer": 10, "winter": 0},
    "AVL Staart": {"summer": 75, "winter": 75},
    "Bruine Peelloop": {"summer": 40, "winter": 30},
    "De Graskuilen": {"summer": 60, "winter": 0},
    "Boksloot": {"summer": 40, "winter": 0},
    "Dekershorst": {"summer": 120, "winter": 80},
    "Everlose beek": {"summer": 75, "winter": 75},
    "Lange Heide": {"summer": 30, "winter": 25},
}


# %%
# Omzetten naar node_id -> debieten in m3/s
discharge_supply_nodes: dict[int, dict[Literal["summer", "winter"], float]] = {}

for name, values in flow_demand_data_ls.items():
    if name not in name_to_node:
        print(f"geen match voor {name}")
        continue

    node_id = name_to_node[name]

    # L/s -> m3/s
    discharge_supply_nodes[node_id] = {
        "summer": values["summer"] / 1000,
        "winter": values["winter"] / 1000,
    }

    print(
        f"{name} -> node {node_id} | "
        f"summer={discharge_supply_nodes[node_id]['summer']} m3/s | "
        f"winter={discharge_supply_nodes[node_id]['winter']} m3/s"
    )

# %%
# add level supply nodes, no flow-demand-node, but discrete control on downstream basin level
level_supply_nodes = [
    526,  # Helenavaart
    757,  # Helenavaart
    525,  # Aanvoerleiding Meteriks Veld
    445,  # Aanvoerleiding Vredepeel
]

double_defined = [i for i in level_supply_nodes if i in discharge_supply_nodes]
if double_defined:
    raise ValueError(f"these nodes are labelled as q-supply and level-supply {double_defined}")

supply_nodes_df = get_node_table_with_from_to_node_ids(model=model, node_ids=level_supply_nodes)

all_nodes = list(discharge_supply_nodes.keys()) + level_supply_nodes
model.node.df[IS_SUPPLY_NODE_COLUMN] = model.node.df.index.isin(all_nodes)

# %% Toevoegen Peelkanaal

polygon = aanvoergebieden_df.loc[["Peelkanaal"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
ignore_intersecting_links: list[int] = [529]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow control nodes (doorlaten)
flow_control_nodes = [1241]

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


# %% Toevoegen Oostrumsche beek

polygon = aanvoergebieden_df.loc[["Oostrumsche beek"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
ignore_intersecting_links: list[int] = [
    17,
    28,
    62,
    64,
    66,
    306,
    420,
    767,
    774,
    776,
    778,
    985,
    1036,
    1567,
    1718,
    1719,
    1727,
    1728,
    1732,
    1734,
    1979,
    1992,
    2023,
]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [160, 447]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow control nodes (doorlaten)
flow_control_nodes = []

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
# Toevoegen Groote Molenbeek

polygon = aanvoergebieden_df.loc[["Groote Molenbeek"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
ignore_intersecting_links: list[int] = [
    17,
    28,
    62,
    64,
    66,
    161,
    306,
    420,
    650,
    767,
    774,
    776,
    778,
    985,
    1036,
    1567,
    1718,
    1719,
    1727,
    1728,
    1732,
    1734,
    1979,
    1992,
    2023,
]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [163, 165, 166, 167, 494, 183, 834, 828, 829, 932, 1054, 1104, 1120, 1204, 1244, 2499]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow control nodes (doorlaten)
flow_control_nodes = [164, 523, 652, 653, 711, 827, 2500]

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
# %% Toevoegen Everlose beek

polygon = aanvoergebieden_df.loc[["Everlose beek"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [82, 89, 92, 782, 1740, 1741, 1755, 1840, 1939, 2003]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [180, 181, 195, 196, 198, 200, 253, 254, 255, 261, 354, 408, 586, 271, 710]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow control nodes (doorlaten)
flow_control_nodes = [
    176,
    179,
    188,
    191,
    192,
    198,
    262,
    471,
    480,
    496,
    536,
    541,
    579,
    657,
    708,
    710,
    725,
    751,
    586,
    839,
    933,
    1055,
    1057,
    1133,
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

# %% Toevoegen Wijnbeek

polygon = aanvoergebieden_df.loc[["Wijnbeek"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [155, 157, 709, 710, 711, 1812, 1813]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow control nodes (doorlaten)
flow_control_nodes = []

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

# %% Tungelroysche beek

polygon = aanvoergebieden_df.loc[["Tungelroysche Beek"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
ignore_intersecting_links: list[int] = [
    831,
    832,
    837,
    838,
    839,
    840,
    1806,
    1814,
    1815,
    424,
    709,
    710,
    711,
    972,
    1802,
    1803,
    1812,
    1813,
    1991,
    2380,
    2381,
    2382,
]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow control nodes (doorlaten)
flow_control_nodes = []

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

# %% Eendlossing

polygon = aanvoergebieden_df.loc[["Eendlossing"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow control nodes (doorlaten)
flow_control_nodes = []

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

# %% Oude Graaf

polygon = aanvoergebieden_df.loc[["Oude Graaf"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [822, 823, 824, 961, 962, 2009, 2010]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow control nodes (doorlaten)
flow_control_nodes = []

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

# %% Nederweert Hoverlossing

polygon = aanvoergebieden_df.loc[["Nederweert Hoverlossing"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2011]

# doorspoeling (op uitlaten)
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven flow control nodes (doorlaten)
flow_control_nodes = []

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


flow_control_nodes = [220, 252, 545, 471, 711, 2493, 2494, 2496, 2497]

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# handmatig opgegeven drain nodes (uitlaten) definieren
drain_nodes = [463, 692, 902, 936, 855, 856, 857]


# Flushing nodes
flushing_nodes = {}


add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
)


boundary_levels = {
    32: 31.65,  # Noordervaart
    33: 31.65,
    133: 31.65,
    93: 31.65,
    112: 31.65,
    94: 31.65,
    95: 31.65,
    96: 31.65,
    97: 31.65,
    98: 31.65,
    99: 31.65,
    100: 31.65,
    101: 31.65,
    124: 31.65,
    125: 31.65,
    150: 31.65,
    136: 31.65,
    122: 31.65,
    123: 31.65,
    118: 28.65,  # Zuid-Willemsvaart
    119: 28.65,
    3: 31.4,
}

for node_id, level in boundary_levels.items():
    model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == node_id, "level"] = level

# %% fixes
# Gemaal Helenavaart
model.update_node(node_id=590, node_type="Pump")
model.pump.static.df.loc[model.pump.static.df.node_id == 590, "min_upstream_level"] = 31.12
model.pump.static.df.loc[model.pump.static.df.node_id == 590, "flow_rate"] = 0

# Gemaal Beringe
model.update_node(node_id=583, node_type="Pump")
model.pump.static.df.loc[model.pump.static.df.node_id == 583, "min_upstream_level"] = 31
model.pump.static.df.loc[model.pump.static.df.node_id == 583, "flow_rate"] = 0
# %%
fixed_levels = {
    535: 31.12,  # Helenavaart-Grenssloot
    773: 31.12,
    2493: 31.4,
    750: 31,
    411: 31,
    604: 31.12,
    932: 31.685,
}

df = model.outlet.static.df
df["min_upstream_level"] = df["node_id"].map(fixed_levels).fillna(df["min_upstream_level"])

# Procentuele Verdeling 90/10 Heide
model.outlet.static.df.loc[model.outlet.static.df.node_id == 616, "flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 639, "flow_rate"] = 50

# Bijna alles gaat via Boabel, afvoer laag zetten
model.outlet.static.df.loc[model.outlet.static.df.node_id == 177, "flow_rate"] = 1

# %% Junctionfy(!)
model = junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime
# %%

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

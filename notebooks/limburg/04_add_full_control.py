# %%
from datetime import datetime
from typing import Literal

import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim.nodes import flow_demand, outlet, pump
from ribasim_nl.control import (
    _offset_new_node,
    _target_level,
    add_controllers_to_supply_area,
    add_controllers_to_supply_nodes,
    add_controllers_to_uncontrolled_connector_nodes,
    get_node_table_with_from_to_node_ids,
    mark_level_update_protected,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import MultiPolygon, Point

from ribasim_nl import CloudStorage, Model

# %%

# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "Limburg"
SHORT_NAME: str = "limburg"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# Node_id: #335, Millnermolen gaat nauwelijks water door alles via AR(Millen) node_id: #365
EXCLUDE_NODES = {335, 651, 552}
outlet_max_flow_rate_from_results = {
    249: 2,  # S_98125
    595: 2,  # inlaat_haelensebeek_uffelsebeek
    596: 2,  # inlaat_tungelroysebeek
    663: 150,  # S_96267
    683: 5,  # S_97911
    783: 150,  # S_96126
    1240: 32,  # naam onbekend
    1302: 150,  # W_261427_0___0
    2483: 17,  # naam onbekend
}
outlet_max_flow_rate_coupled_by_node_id = {
    208: 6,  # Onderbeek; gekoppeld max=3.82, parameterized=1.54
    221: 144,  # Rijksweg A2,75; gekoppeld max=95.65, parameterized=13.10
    426: 5,  # Wellse Molenbeek; gekoppeld max=2.35, parameterized=0.94
    563: 150,  # Rijksweg; gekoppeld max=99.82, parameterized=7.62
    792: 150,  # S_96308; gekoppeld max=99.64, parameterized=7.63
    895: 144,  # W_244840_0; gekoppeld max=95.65, parameterized=13.10
}
outlet_max_flow_rate_parameterized_zero_by_node_id = {
    2494: 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
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
flushing_nodes: dict[int, float] = {}
drain_nodes = [
    160,
    163,
    165,
    166,
    167,
    180,
    181,
    183,
    195,
    196,
    198,
    200,
    253,
    254,
    255,
    261,
    271,
    354,
    408,
    447,
    463,
    494,
    586,
    692,
    710,
    828,
    829,
    834,
    855,
    856,
    857,
    902,
    932,
    936,
    1054,
    1104,
    1120,
    1204,
    1244,
    2499,
]
supply_nodes: list[int] = [311, 595, 596, 683]
flow_control_nodes = [
    164,
    176,
    179,
    188,
    191,
    192,
    198,
    220,
    252,
    262,
    411,
    471,
    480,
    496,
    523,
    536,
    541,
    545,
    579,
    590,
    652,
    653,
    657,
    708,
    710,
    711,
    725,
    751,
    827,
    839,
    933,
    1055,
    1057,
    1133,
    1241,
    2493,
    2494,
    2496,
    2500,
]

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

outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


model.pump.static.df.loc[model.pump.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0

# Erg klein basin, numerieke problemen
model.merge_basins(node_id=2394, to_node_id=1507, are_connected=True)
model.merge_basins(node_id=1672, to_node_id=1556, are_connected=True)
model.merge_basins(node_id=1416, to_node_id=2408, are_connected=True)
# %%
# Node 651 moet dicht zijn na overleg Limburg
model.pump.static.df.loc[model.pump.static.df.node_id == 651, "max_flow_rate"] = 0

# Peelkanaal op grenssloot ligt in verkeerde richting
for link_id in [529, 1036]:
    model.reverse_link(link_id=link_id)

# Gemaal Helenavaart
model.update_node(node_id=590, node_type="Pump")

# Rijskweg
model.update_node(node_id=558, node_type="ManningResistance")

# Gemaal Beringe
model.update_node(node_id=583, node_type="Pump")

# Overbodig
model.remove_node(node_id=250, remove_links=True)
model.remove_node(node_id=938, remove_links=True)
model.remove_node(node_id=939, remove_links=True)
model.remove_node(node_id=2497, remove_links=True)  # Parallel aan doorlaat 2496 tussen hetzelfde basin-paar.

# Verplaats node 788, behoud bestaande verbindingen en update gekoppelde linkgeometrieen.
model.move_node(node_id=788, geometry=Point(193403.3, 352653.3))

# Redirect link 2208 naar node 1802.
model.redirect_link(link_id=2208, to_node_id=1802)

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

        # Gemaal Beringe blijft een Pump; de andere discharge supply nodes modelleren we als Outlet.
        if node_id == 583:
            model.update_node(
                node_id,
                "Pump",
                [
                    pump.Static(
                        min_upstream_level=[us_target_level + us_target_level_offset_supply],
                        flow_rate=[0],
                        min_flow_rate=[float("nan")],
                        max_flow_rate=[float("nan")],
                    )
                ],
            )
        else:
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
    #  "Snepheiderbeek": 411,
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
    #  "Snepheiderbeek": {"summer": 40, "winter": 20},  # tabel geeft bandbreedte zomer 10-20, winter 30-40 (verkeerd om)
    "Snepheiderbeek_1": {"summer": 40, "winter": 20},  # Bovenstrooms Snepheiderbeek is echte inlaat
    # Grote aanvoeren
    "Gemaal Beringe": {"summer": 550, "winter": 350},
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

# FlowDemand-inlaten krijgen hun debiet uit FlowDemand, niet uit afvoer-capaciteit overrides.
for node_id in discharge_supply_nodes:
    outlet_max_flow_rate_afvoer_by_node_id.pop(node_id, None)

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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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
    411,
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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
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
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
)


# %% add all remaining inlets/outlets
add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)


# %% fixes

# %%

# Procentuele Verdeling 90/10 Heide: alleen afvoer-capaciteit begrenzen.
afvoer_mask_616 = (model.outlet.static.df.node_id == 616) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[afvoer_mask_616, "flow_rate"] = 5
model.outlet.static.df.loc[afvoer_mask_616, "max_flow_rate"] = 100

afvoer_mask_639 = (model.outlet.static.df.node_id == 639) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[afvoer_mask_639, "flow_rate"] = 50
model.outlet.static.df.loc[afvoer_mask_639, "max_flow_rate"] = 100

# Bijna alles gaat via Boabel: alleen afvoer-capaciteit laag zetten.
afvoer_mask_177 = (model.outlet.static.df.node_id == 177) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[afvoer_mask_177, "flow_rate"] = 1
model.outlet.static.df.loc[afvoer_mask_177, "max_flow_rate"] = 100

# S_97911 is een RWS-inlaat naar Limburg, geen afvoer richting Limburg.
aanvoer_mask_683 = (model.outlet.static.df.node_id == 683) & (model.outlet.static.df.control_state == "aanvoer")
model.outlet.static.df.loc[aanvoer_mask_683, "flow_rate"] = 100
model.outlet.static.df.loc[aanvoer_mask_683, "max_flow_rate"] = 100
afvoer_mask_683 = (model.outlet.static.df.node_id == 683) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[afvoer_mask_683, "flow_rate"] = 0
model.outlet.static.df.loc[afvoer_mask_683, "max_flow_rate"] = 0

boundary_levels = {120: 30.75, 121: 30.75, 124: 30.75, 125: 30.75, 132: 31.545, 3: 31.545, 136: 32, 95: 30, 98: 30}
for node_id, level in boundary_levels.items():
    model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == node_id, "level"] = level

# Gemaal Helenaveen blijft een uitlaat: min_upstream en bijbehorende threshold 4 cm omlaag.
# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
pump_max_flow_rate_from_results = {
    590: 5,  # Gemaal Helenaveen; oude static flow_rate=0.0101
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

# Bescherm handmatig ingestelde doorlaat tussen Limburg en RWS tegen latere coupling-level updates.
mark_level_update_protected(model.outlet.static.df, model.outlet.static.df["node_id"].isin([2496]), model=model)

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
manual_max_flow_rate_node_ids = {177, 616, 639, 683}
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

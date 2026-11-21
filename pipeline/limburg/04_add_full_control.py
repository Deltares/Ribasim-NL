# %%
from datetime import datetime
from typing import Literal, cast

import geopandas as gpd
import pandas as pd
from ribasim.nodes import flow_demand, outlet, pump
from ribasim_nl.control import (
    _offset_new_node,
    _target_level,
    add_controllers_to_supply_area,
    add_controllers_to_supply_nodes,
    add_controllers_to_uncontrolled_connector_nodes,
    get_node_table_with_from_to_node_ids,
)
from ribasim_nl.coupling_level_apply import sync_static_controller_thresholds
from ribasim_nl.junctions import junctionify
from ribasim_nl.run_model import write_steady_state_scenarios
from shapely.geometry import MultiPolygon, Point

from ribasim_nl import CloudStorage, Model

# %%

# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "Limburg"
SHORT_NAME: str = "limburg"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# %%

# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(r"Limburg/verwerkt/sturing/aanvoergebieden.gpkg")


# %%#
# Read data
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# Node_id: #335, Millnermolen gaat nauwelijks water door alles via AR(Millen) node_id: #365
EXCLUDE_NODES = {n("S_98575"), n("S_212268")}
outlet_max_flow_rate_from_results = {
    n("S_98125"): 5,  # S_98125
    n("inlaat_haelensebeek_uffelsebeek"): 2,  # inlaat_haelensebeek_uffelsebeek
    n("inlaat_tungelroysebeek"): 2,  # inlaat_tungelroysebeek
    n("S_96267"): 150,  # S_96267
    n("S_97911"): 5,  # S_97911
    n("S_96126"): 150,  # S_96126
    n("ow:W_242161_0"): 32,  # naam onbekend
    n("W_261427_0___0"): 150,  # W_261427_0___0
    n("loc:176772,317353"): 17,  # naam onbekend
}
outlet_max_flow_rate_coupled_by_node_id = {
    n("D_16911"): 6,  # Onderbeek; gekoppeld max=3.82, parameterized=1.54
    n("D_16548"): 144,  # Rijksweg A2,75; gekoppeld max=95.65, parameterized=13.10
    n("D_25316"): 5,  # Wellse Molenbeek; gekoppeld max=2.35, parameterized=0.94
    n("D_13922"): 150,  # Rijksweg; gekoppeld max=99.82, parameterized=7.62
    n("S_96308"): 150,  # S_96308; gekoppeld max=99.64, parameterized=7.63
    n("W_244840_0"): 144,  # W_244840_0; gekoppeld max=95.65, parameterized=13.10
    n("D_24227"): 10,  # Peelkanaal was te laag
}
outlet_max_flow_rate_parameterized_zero_by_node_id = {
    n("loc:190139,382403"): 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
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
    n("S_281999"),
    n("D_23806"),
    n("S_98935"),
    n("S_97731"),
    n("D_19734"),
    n("D_20281"),
    n("S_224861"),
    n("D_18630"),
    n("D_14902"),
    n("D_19951"),
    n("D_21290"),
    n("S_97530"),
    n("D_19948"),
    n("S_98262"),
    n("D_19804"),
    n("D_19805"),
    n("S_96498"),
    n("D_21294"),
    n("S_96241"),
    n("S_161721"),
    n("D_23513"),
    n("D_14900"),
    n("D_25217"),
    n("D_19076"),
    n("G_270436"),
    n("G_27616"),
    n("S_97827"),
    n("S_97755"),
    n("W_2370_0___1"),
    n("W_2371_0___2"),
    n("W_2867_0___4"),
    n("ow:W_3135_0"),
    n("W_2133_0___1"),
    n("W_1818_0___2"),
    n("W_2543_0___6"),
    n("W_242613_0___3"),
    n("ow:W_184016_0"),
    n("W_153227_0___18"),
    n("W_2987_0___6"),
    n("W_3003_1___0"),
    n("W_308819_0___4"),
    n("D_19935"),
]
supply_nodes: list[int] = [
    n("S_97728"),
    n("inlaat_haelensebeek_uffelsebeek"),
    n("inlaat_tungelroysebeek"),
    n("S_97911"),
]
flow_control_nodes = [
    n("S_97736"),
    n("S_98067"),
    n("S_98908"),
    n("S_98259"),
    n("D_21787"),
    n("S_97020"),
    n("S_97530"),
    n("S_96405"),
    n("S_98244"),
    n("S_96896"),
    n("D_23544"),
    n("D_23614"),
    n("D_15567"),
    n("D_23026"),
    n("D_18926"),
    n("D_24227"),
    n("S_97735"),
    n("D_293974"),
    n("D_24815"),
    n("D_228941"),
    n("D_18787"),
    n("S_97630"),
    n("S_97855"),
    n("S_294013"),
    n("S_97756"),
    n("S_97755"),
    n("S_210230"),
    n("S_98254"),
    n("S_97998"),
    n("W_2296_6"),
    n("W_2032_0___10"),
    n("W_3003_2___0"),
    n("W_2425_1___1"),
    n("ow:W_3003_0"),
    n("ow:W_293737_0"),
    n("ow:W_1791_0"),
    n("W_242594_0___0"),
    n("W_292386_0___4"),
    n("W_281080_0"),
    n("loc:189059,384157"),
    n("loc:190139,382403"),
    n("loc:190414,375171"),
    n("D_17972"),
    n("D_20117"),  # Eeuwselseloop is geen inlaat maar een doorlaat naar de benedenstroomse rand.
]

# %%

assert model.outlet.static.df is not None
outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


assert model.pump.static.df is not None
model.pump.static.df.loc[model.pump.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0

# Erg klein basin, numerieke problemen
model.merge_basins(node_id=n("up:S_98740"), to_node_id=n("up:D_21450"), are_connected=True)
model.merge_basins(node_id=n("up:S_98592"), to_node_id=n("up:D_15297"), are_connected=True)
model.merge_basins(node_id=n("up:S_282446"), to_node_id=n("up:S_98244"), are_connected=True)
# %%
# Node 651 moet dicht zijn na overleg Limburg
model.pump.static.df.loc[model.pump.static.df.node_id == n("S_212268"), "max_flow_rate"] = 0

# Peelkanaal op grenssloot ligt in verkeerde richting
for link_id in [lk("W_3056_0", "up:S_98244"), lk("up:S_97631", "W_3056_0")]:
    model.reverse_link(link_id=link_id)

# Gemaal Helenavaart
model.update_node(node_id=n("G_27616"), node_type="Pump")

# Rijskweg
model.update_node(node_id=n("D_13960"), node_type="ManningResistance")

# Gemaal Beringe
model.update_node(node_id=n("G_27621"), node_type="Pump")

# Overbodig
model.remove_node(node_id=n("S_98248"), remove_links=True)
model.remove_node(node_id=n("bnd:4"), remove_links=True)
model.remove_node(node_id=n("W_243184_0"), remove_links=True)
model.remove_node(node_id=n("W_204334_0"), remove_links=True)
model.remove_node(
    node_id=n("loc:190379,375116"), remove_links=True
)  # Parallel aan doorlaat 2496 tussen hetzelfde basin-paar.
model.remove_node(node_id=n("bnd:120"), remove_links=True)  # LevelBoundary van verwijderde parallelle doorlaat 2497.

# Verplaats node 788 naar de overgang van Basin 1466 naar 1347, behoud bestaande verbindingen en update gekoppelde
# linkgeometrieen, zie #871. Eerder stond 788 op de locatie van node 1162 bij Roermond.
model.move_node(node_id=n("S_96162"), geometry=Point(181887.6, 320695.9))

# Redirect link 2208 naar node 1802.
model.redirect_link(link_id=lk("W_261466_0___3", "up:S_96174"), to_node_id=n("up:S_96174"))

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
            node_id=cast(int, node_table_df.at[node_id, "from_node_id"]),
            target_level_column="meta_streefpeil",
            node_types=node_types,
            allow_missing=False,
        )
        assert us_target_level is not None

        # Gemaal Beringe blijft een Pump; de andere discharge supply nodes modelleren we als Outlet.
        if node_id == n("G_27621"):
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
    "Gemaal Beringe": n("G_27621"),
    "Zijtak Helenavaart": n("D_19818"),
    "Houtstraatlossing": n("D_14965"),
    "Hushoverbeek": n("D_281344"),
    "Inlaat Groote Moost": n("S_98847"),
    "Achterste Moost": n("D_25958"),
    "Inlaat Hulsenlossing": n("ow:W_2827_1"),
    "Inlaat Kleine Moost": n("D_25958"),
    "Inlaat Rietbeek": n("S_98409"),
    "Inlaat Weteringbeek": n("S_98009"),
    "Kampershoek": n("loc:179521,364568"),
    "Katsberg": n("S_282001"),
    "Nederweerter Hovenlossing": n("D_23657"),
    "Nederweerter Riet": n("D_25545"),
    "Oude Graaf": n("ow:W_308197_0"),
    #  "Snepheiderbeek": n("D_23544"),
    "Snepheiderbeek_1": n("S_173552"),
    "Waatskamplossing": n("S_97872"),
    "Eendlossing": n("S_97698"),
    "Inlaat Evertsoord": n("S_97322"),
    "Klein Leukerbeek": n("W_153168_0"),
    "Roeven": n("S_97840"),
    #  "Halte Grenssloot": n("loc:190414,375171"),
    #  "Halte Peelkanaal": n("D_24227"),
    "Peelkanaal naar Grenssloot": n("W_3056_0"),
    "Molenakker": n("loc:178318,363929"),
    "Zwartwaterlossing": n("D_294341"),
    "AVL Staart": n("W_3092_1___0"),
    "Bruine Peelloop": n("S_97486"),
    "De Graskuilen": n("ow:W_2187_2"),
    "Boksloot": n("W_2680_0___0"),
    "Dekershorst": n("S_97529"),
    "Everlose beek": n("S_98925"),
    "Lange Heide": n("S_97522"),
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
    "Zijtak Helenavaart": {"summer": 350, "winter": 150},  # winter in tabel: laatste 2 jaar
    "Katsberg": {"summer": 4000, "winter": 1000},  # gemiddelde aanvoertabelhenk08012021.xlsx
    "Eendlossing": {"summer": 20, "winter": 10},  # WATAK
    "Klein Leukerbeek": {"summer": 10, "winter": 10},  # GIHO kaart
    "Roeven": {"summer": 10, "winter": 10},  # WATAK
    # afgeleid uit relevante tabelregels
    "Inlaat Evertsoord": {"summer": 75, "winter": 50},  # 230_030N in GIHO figuur??
    "Halte Grenssloot": {
        "summer": 1600,
        "winter": 400,
    },  # aanvoertabelhenk08012021.xlsx
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
    n("D_18479"),  # Helenavaart
    n("S_97928"),  # Helenavaart
    n("D_20316"),  # Aanvoerleiding Meteriks Veld
    n("D_20183"),  # Aanvoerleiding Vredepeel
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
ignore_intersecting_links: list[int] = [lk("up:S_98244", "W_3056_0")]

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
    lk("W_2990_0___13", "up:D_20798"),
    lk("ow:W_243784_0", "up:ow:W_243786_0"),
    lk("W_243766_0___5", "up:W_243769_0___8"),
    lk("W_243723_0", "up:ow:W_243763_0"),
    lk("ow:W_243020_0", "up:ow:W_243763_0"),
    lk("W_243761_0___1", "up:W_243697_0___3"),
    lk("W_2591_0___1", "up:D_25561"),
    lk("W_243114_0___4", "up:W_283442_0"),
    lk("W_2987_0___6", "up:ow:W_243707_0"),
    lk("W_2632_0___5", "up:W_243716_0___2"),
    lk("ow:W_243707_0", "up:W_243716_0___2"),
    lk("ow:W_243044_2@1", "up:ow:W_243044_2@2"),
    lk("W_3056_0", "up:S_97631"),
    lk("up:S_96550", "W_2826_1___0"),
    lk("up:W_153210_1___4", "W_153210_1___4"),
    lk("up:W_2826_0", "W_2826_0"),
    lk("up:W_2370_0___1", "W_2370_0___1"),
    lk("up:W_2371_0___2", "W_2371_0___2"),
    lk("up:W_2682_0___1", "W_2682_0___1"),
    lk("up:W_2632_0___5", "W_2867_0___4"),
    lk("up:W_153227_0___18", "W_153227_0___18"),
    lk("up:W_2987_0___6", "W_2987_0___6"),
    lk("up:ow:W_243763_0", "ow:W_243763_0"),
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
    lk("W_2990_0___13", "up:D_20798"),
    lk("ow:W_243784_0", "up:ow:W_243786_0"),
    lk("W_243766_0___5", "up:W_243769_0___8"),
    lk("W_243723_0", "up:ow:W_243763_0"),
    lk("ow:W_243020_0", "up:ow:W_243763_0"),
    lk("D_23771", "up:W_242073_0___3"),
    lk("W_243761_0___1", "up:W_243697_0___3"),
    lk("W_2591_0___1", "up:D_25561"),
    lk("ow:W_3045_0", "up:W_294359_0___2"),
    lk("W_243114_0___4", "up:W_283442_0"),
    lk("W_2987_0___6", "up:ow:W_243707_0"),
    lk("W_2632_0___5", "up:W_243716_0___2"),
    lk("ow:W_243707_0", "up:W_243716_0___2"),
    lk("ow:W_243044_2@1", "up:ow:W_243044_2@2"),
    lk("W_3056_0", "up:S_97631"),
    lk("up:S_96550", "W_2826_1___0"),
    lk("up:W_153210_1___4", "W_153210_1___4"),
    lk("up:W_2826_0", "W_2826_0"),
    lk("up:W_2370_0___1", "W_2370_0___1"),
    lk("up:W_2371_0___2", "W_2371_0___2"),
    lk("up:W_2682_0___1", "W_2682_0___1"),
    lk("up:W_2632_0___5", "W_2867_0___4"),
    lk("up:W_153227_0___18", "W_153227_0___18"),
    lk("up:W_2987_0___6", "W_2987_0___6"),
    lk("up:ow:W_243763_0", "ow:W_243763_0"),
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
ignore_intersecting_links: list[int] = [
    lk("ow:W_1791_0", "up:W_242594_0___0"),
    lk("ow:W_3135_0", "up:G_270436"),
    lk("W_2133_0___1", "up:S_97517"),
    lk("W_242959_0___1", "up:S_98908"),
    lk("up:W_242318_0___9", "W_242318_0___9"),
    lk("up:W_242959_0___1", "W_242959_0___1"),
    lk("up:W_2025_0___1", "W_2025_0___1"),
    lk("up:ow:W_2397_0", "ow:W_2397_0"),
    lk("up:W_242594_0___0", "W_242594_0___0"),
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

# %% Toevoegen Wijnbeek

polygon = aanvoergebieden_df.loc[["Wijnbeek"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("W_153262_0___1", "up:D_294341"),
    lk("ow:W_241962_0", "up:D_294341"),
    lk("W_294359_0___2", "up:S_172786"),
    lk("W_2031_0___12", "up:S_172786"),
    lk("W_168768_0", "up:S_172786"),
    lk("up:W_294359_0___2", "W_294359_0___2"),
    lk("up:W_2031_0___12", "W_2031_0___12"),
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

# %% Tungelroysche beek

polygon = aanvoergebieden_df.loc[["Tungelroysche Beek"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
ignore_intersecting_links: list[int] = [
    lk("ow:W_242291_0", "up:W_243306_0___16"),
    lk("D_20183", "up:ow:W_3216_0"),
    lk("W_243304_0___3", "up:W_243306_0___16"),
    lk("W_243242_0", "up:S_168779"),
    lk("W_153285_3", "up:S_168779"),
    lk("W_3153_0___8", "up:W_242503_0___7"),
    lk("ow:W_242514_0", "up:W_242503_0___7"),
    lk("up:ow:W_242291_0", "ow:W_242291_0"),
    lk("up:W_243242_0", "W_243242_0"),
    lk("up:W_153285_3", "W_153285_3"),
    lk("ow:W_2243_0", "up:S_201714"),
    lk("W_294359_0___2", "up:S_172786"),
    lk("W_2031_0___12", "up:S_172786"),
    lk("W_168768_0", "up:S_172786"),
    lk("W_153162_0___4", "up:S_212243"),
    lk("up:W_3108_0", "W_3108_0"),
    lk("up:W_2886_0___2", "W_2886_0___2"),
    lk("up:W_294359_0___2", "W_294359_0___2"),
    lk("up:W_2031_0___12", "W_2031_0___12"),
    lk("up:W_242128_0___8", "W_242128_0___8"),
    lk("loc:189325,353515", "ow:W_309566_0"),
    lk("loc:189325,353515", "ow:W_224499_0"),
    lk("loc:189325,353515", "ow:W_245631_1"),
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
ignore_intersecting_links: list[int] = [
    lk("W_2543_0___6", "up:W_242914_0"),
    lk("W_2863_0___2", "up:W_242914_0"),
    lk("ow:W_225943_0", "up:W_242914_0"),
    lk("ow:W_2267_0", "up:D_25073"),
    lk("ow:W_308185_0", "up:D_25073"),
    lk("up:ow:W_310345_0", "ow:W_310345_0"),
    lk("up:W_3233_0", "W_3233_0"),
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

# %% Nederweert Hoverlossing

polygon = aanvoergebieden_df.loc[["Nederweert Hoverlossing"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [lk("up:W_3025_1___6", "W_3025_1___6")]

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
afvoer_mask_616 = (model.outlet.static.df.node_id == n("S_96719")) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[afvoer_mask_616, "flow_rate"] = 5
model.outlet.static.df.loc[afvoer_mask_616, "max_flow_rate"] = 100

afvoer_mask_639 = (model.outlet.static.df.node_id == n("S_96782")) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[afvoer_mask_639, "flow_rate"] = 50
model.outlet.static.df.loc[afvoer_mask_639, "max_flow_rate"] = 100

# Bijna alles gaat via Boabel: alleen afvoer-capaciteit laag zetten.
afvoer_mask_177 = (model.outlet.static.df.node_id == n("S_224635")) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[afvoer_mask_177, "flow_rate"] = 1
model.outlet.static.df.loc[afvoer_mask_177, "max_flow_rate"] = 100

# S_97911 is een RWS-inlaat naar Limburg, geen afvoer richting Limburg.
aanvoer_mask_683 = (model.outlet.static.df.node_id == n("S_97911")) & (
    model.outlet.static.df.control_state == "aanvoer"
)
model.outlet.static.df.loc[aanvoer_mask_683, "flow_rate"] = 100
model.outlet.static.df.loc[aanvoer_mask_683, "max_flow_rate"] = 100
afvoer_mask_683 = (model.outlet.static.df.node_id == n("S_97911")) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[afvoer_mask_683, "flow_rate"] = 0
model.outlet.static.df.loc[afvoer_mask_683, "max_flow_rate"] = 0

boundary_levels = {
    n("bnd:99"): 31,
    n("bnd:121"): 31,
    n("bnd:124"): 31,
    n("bnd:125"): 31,
    n("bnd:132"): 31.545,
    n("bnd:3"): 31.545,
    n("bnd:136"): 32,
    n("bnd:95"): 30,
    n("bnd:98"): 30,
}
assert model.level_boundary.static.df is not None
for node_id, level in boundary_levels.items():
    model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == node_id, "level"] = level

# Gemaal Helenaveen blijft een uitlaat: min_upstream en bijbehorende threshold 4 cm omlaag.
helenaveen_pump_id = n("G_27616")
helenaveen_upstream_node_id = model.upstream_node_id(helenaveen_pump_id)
if isinstance(helenaveen_upstream_node_id, pd.Series):
    helenaveen_upstream_node_id = int(helenaveen_upstream_node_id.iloc[0])
helenaveen_upstream_level = _target_level(
    model=model,
    node_types=model.node.df["node_type"],
    node_id=int(helenaveen_upstream_node_id),
    target_level_column="meta_streefpeil",
    allow_missing=False,
)
assert helenaveen_upstream_level is not None
helenaveen_min_upstream_level = float(helenaveen_upstream_level) - 0.04
mask = model.pump.static.df.node_id == helenaveen_pump_id
model.pump.static.df.loc[mask, "min_upstream_level"] = helenaveen_min_upstream_level
sync_static_controller_thresholds(
    model=model,
    target_node_ids={helenaveen_pump_id},
    tolerance=1e-6,
)

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
pump_max_flow_rate_from_results = {
    n("G_27616"): 5,  # Gemaal Helenaveen; oude static flow_rate=0.0101
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
manual_max_flow_rate_node_ids = {n("S_224635"), n("S_96719"), n("S_96782"), n("S_97911")}
protected_max_flow_rate_node_ids = set(EXCLUDE_NODES) | flow_demand_controlled_node_ids | manual_max_flow_rate_node_ids
for static_df in (model.outlet.static.df, model.pump.static.df):
    assert static_df is not None
    afvoer_mask = (
        static_df["control_state"].eq("afvoer")
        & static_df["flow_rate"].fillna(0).gt(0)
        & ~static_df["node_id"].isin(protected_max_flow_rate_node_ids)
    )
    static_df.loc[afvoer_mask, "max_flow_rate"] = static_df.loc[afvoer_mask, "max_flow_rate"].fillna(10).clip(lower=10)

for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = static_df["control_state"].eq("afvoer") & static_df["node_id"].isin(aanvoer_only_node_ids)
    static_df.loc[afvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

# Model run


assert model.discrete_control.condition.df is not None
model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# %%

# write the dry and wet steady state scenarios and the full control model
model = write_steady_state_scenarios(
    model,
    AUTHORITY,
    SHORT_NAME,
    dry_evaporation_mm_per_day=0.1,
    model_exec=MODEL_EXEC,
    qlr_path=qlr_path,
)

# %%

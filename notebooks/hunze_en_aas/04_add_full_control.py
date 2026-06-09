# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "HunzeenAas"  # authority
SHORT_NAME: str = "hea"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"
MIN_FLOW_RATE_BY_NODE_ID = {}
outlet_max_flow_rate_by_node_id = {
    330: 7.5,  # De Bult
    767: 3.6,  # Inlaat Purit
    2014: 0.3,  # Inlaat Verl. Hoogeveense Vaart
    2011: 0.3,  # Inlaat Verl. Hoogeveense Vaart
    847: 0.3,  # Inlaat Barriereweg
    829: 0.0,  # Geen aanvoer vanuit de Eems (i.v.m. zout)
    822: 0.0,
    810: 0.0,
    190: 0.0,
    162: 0.0,
}
outlet_max_flow_rate_coupled_by_node_id = {
    154: 2,  # gekoppeld max=0.92, huidige max=0.00, link=3300493
    194: 66,  # Vispassage; gekoppeld max=43.20, parameterized=36.20
    208: 9,  # gekoppeld max=5.82, huidige max=1.39, link=3300617
    264: 84,  # KST-A-12710; gekoppeld max=55.19, parameterized=30.80
    275: 9,  # Vispassage de Hemmen; gekoppeld max=5.76, parameterized=3.16
    290: 51,  # gekoppeld max=33.40, huidige max=33.10, link=3300788
    291: 3,  # gekoppeld max=1.87, huidige max=0.00, link=3300670
    302: 9,  # stuw De Hemmen; gekoppeld max=5.76, parameterized=3.16
    336: 66,  # KST-A-13340; gekoppeld max=43.20, parameterized=36.20
    411: 3,  # Lange Runde; gekoppeld max=1.38, parameterized=0.34
    438: 24,  # KST-A-00027; gekoppeld max=15.13, parameterized=11.70
    440: 24,  # TT Assen; gekoppeld max=15.13, parameterized=11.70
    477: 84,  # Vispassage; gekoppeld max=55.19, parameterized=30.80
    497: 9,  # KST-A-11930; gekoppeld max=5.71, parameterized=1.40
    500: 20,  # gekoppeld max=12.00, huidige max=0.225, link=3300502
    617: 6,  # gekoppeld max=3.07, huidige max=0.00, link=3300989
    727: 2,  # parameterized nul; gekoppeld max=0.84
    754: 3,  # gekoppeld max=1.19, huidige max=0.00, link=3301299
    781: 2,  # gekoppeld max=0.92, huidige max=0.00, link=3300494
    787: 2,  # gekoppeld max=0.86, huidige max=0.00, link=3301023
    840: 2,  # gekoppeld max=0.71, huidige max=0.00, link=3300528
    852: 2,  # gekoppeld max=0.51, huidige max=0.00, link=3301334
    1112: 44,  # gekoppeld max=28.69, parameterized=3.25
    1122: 6,  # gekoppeld max=3.11, huidige max=0.00, link=3300566
    2008: 2,  # parameterized nul; gekoppeld max=0.32
    2010: 9,  # gekoppeld max=5.73, parameterized=4.37
    2015: 3,  # gekoppeld max=1.34, parameterized=0.10
}
outlet_max_flow_rate_afvoer_by_node_id = {}
for max_flow_rates in (outlet_max_flow_rate_coupled_by_node_id,):
    for node_id, max_flow_rate in max_flow_rates.items():
        outlet_max_flow_rate_afvoer_by_node_id[node_id] = max(
            outlet_max_flow_rate_afvoer_by_node_id.get(node_id, 0.0),
            max_flow_rate,
        )
outlet_max_flow_rate_afvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)
pump_max_flow_rate_by_node_id = {
    20: 20.0,  # Aanvoergemaal Dorkwerd
    972: 7.5,  # Aanvoergemaal Kupers
    70: 4.2,  # Aanvoergemaal Vennix
    107: 1.92,  # Aanvoergemaal Ter Apelkanaal
}
ALWAYS_ON_PUMP_MIN_FLOW_RATE_BY_NODE_ID: dict[int, float] = {}
ALWAYS_ON_PUMP_MAX_DOWNSTREAM_LEVEL: float = 99999.0
ALWAYS_ON_PUMP_MIN_UPSTREAM_LEVEL_OFFSET: float = -1.0

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {
    152,  # Dorkswerdersluis (Scheepvaart)
    153,  # Sluis Peelo
    156,  # Vriescheloostersluis (Veelerveen)
    161,  # Bulsterverlaat (Scheepvaart)
    165,  # 2e verlaat Stadskanaal
    167,  # Koppelsluis Pekelerhoofddiep
    174,  # Koppelsluis (Scheepvaart)
    183,  # Haansluis (scheepvaart)
    188,
    736,  # Springersverlaat 1e verlaat
    776,
    832,
}


# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(rf"{AUTHORITY}/verwerkt/sturing/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


def upstream_basin_target_level(model: Model, node_id: int) -> float | None:
    upstream_node_ids = model.link.df.loc[model.link.df.to_node_id == node_id, "from_node_id"].unique()
    basin_levels = model.basin.area.df.dropna(subset=["meta_streefpeil"]).groupby("node_id")["meta_streefpeil"].first()
    target_levels = basin_levels[basin_levels.index.isin(upstream_node_ids)]
    if target_levels.empty:
        return None
    return float(target_levels.iloc[0])


def configure_always_on_pumps(model: Model) -> None:
    for node_id, min_flow_rate in ALWAYS_ON_PUMP_MIN_FLOW_RATE_BY_NODE_ID.items():
        mask = model.pump.static.df.node_id == node_id
        if not mask.any():
            raise KeyError(f"Pump node_id={node_id} not found in pump static table")

        flow_rate = model.pump.static.df.loc[mask, "flow_rate"].max()
        max_flow_rate = model.pump.static.df.loc[mask, "max_flow_rate"].max()
        current_min_upstream_level = model.pump.static.df.loc[mask, "min_upstream_level"].dropna().max()
        target_level = upstream_basin_target_level(model=model, node_id=node_id)
        if target_level is None:
            if current_min_upstream_level is None:
                raise KeyError(
                    f"No upstream basin with meta_streefpeil and no existing min_upstream_level found for node_id={node_id}"
                )
            target_level = float(current_min_upstream_level)

        model.pump.static.df.loc[mask, "flow_rate"] = flow_rate
        model.pump.static.df.loc[mask, "max_flow_rate"] = max_flow_rate
        model.pump.static.df.loc[mask, "min_flow_rate"] = min_flow_rate
        model.pump.static.df.loc[mask, "min_upstream_level"] = target_level + ALWAYS_ON_PUMP_MIN_UPSTREAM_LEVEL_OFFSET
        model.pump.static.df.loc[mask, "max_downstream_level"] = ALWAYS_ON_PUMP_MAX_DOWNSTREAM_LEVEL
        mark_level_update_protected(model.pump.static.df, mask, model=model)


# %%
# Read data
model = Model.read(ribasim_toml)

outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

# Handmatige inlaatcapaciteiten gelden ook in aanvoer; niet terugvallen op de default van 10 m3/s.
outlet_max_flow_rate_aanvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)

# fixes (zo snel mogelijk)
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 6, "level"] = 0.0  # Noordzee
remove_nodes = [152, 534, 678]
# 152: Dorkwerdersluis (scheepvaart)
# 534: Vispassage "de Bult"
# 678: Koker bij gemaal westerpolder

model.redirect_link(link_id=2853, to_node_id=1987)
for node_id in remove_nodes:
    model.remove_node(node_id=node_id, remove_links=True)

model.merge_basins(node_id=1621, to_node_id=1601, are_connected=True)
model.merge_basins(node_id=1325, to_node_id=1666, are_connected=True)
model.merge_basins(node_id=1908, to_node_id=1372, are_connected=True)
model.merge_basins(node_id=1763, to_node_id=1381, are_connected=False)
model.reverse_direction_at_node(node_id=871)  # inlaat stond verkeerde kant op
model.reverse_direction_at_node(node_id=691)  # inlaat stond verkeerde kant op
model.update_node(
    node_id=166, node_type="ManningResistance"
)  # Grote Slapersluis; hier wordt ook water ingelaten; hopelijk lukt dit met ManningResistance
model.update_node(node_id=632, node_type="Pump")  # Inlaat Vestdijklaan lijkt een pomp(je) te zijn
model.update_node(node_id=1111, node_type="Outlet")  # Manning tussen basins met verschillende streefpeilen gaat niet
model.update_node(node_id=1122, node_type="Outlet")  # Manning tussen basins met verschillende streefpeilen gaat niet
model.update_node(node_id=854, node_type="Pump")  # Inlaat Vestdijklaan lijkt een pomp(je) te zijn
model.update_node(node_id=854, node_type="Outlet")  # Uitlaat Nijlandsloop naar Anreperdiep
for node_id in [147, 192]:
    model.update_node(node_id=node_id, node_type="Outlet")
for node_id in ALWAYS_ON_PUMP_MIN_FLOW_RATE_BY_NODE_ID:
    model.update_node(node_id=node_id, node_type="Pump", node_properties={"meta_function": "pump"})

model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0.0
model.pump.static.df.loc[model.pump.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0.0


# capaciteit inlaten/doorlaten
model.pump.static.df.loc[model.pump.static.df.node_id == 20, "flow_rate"] = 20  # Aanvoergemaal Dorkwerd
model.pump.static.df.loc[model.pump.static.df.node_id == 972, "flow_rate"] = 7.5  # Aanvoergemaal Küpers
model.pump.static.df.loc[model.pump.static.df.node_id == 70, "flow_rate"] = 4.2  # Aanvoergemaal Vennix
model.pump.static.df.loc[model.pump.static.df.node_id == 107, "flow_rate"] = 1.92  # Aanvoergemaal Ter Apelkanaal
model.outlet.static.df.loc[model.outlet.static.df.node_id == 330, "flow_rate"] = (
    7.5  # De Bult (Afvoer-capaciteit gelijk aan Küpers!)
)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 767, "flow_rate"] = 3.6  # Inlaat Purit
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2014, "flow_rate"] = 0.3  # Inlaat Verl. Hoogeveense Vaart
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2011, "flow_rate"] = 0.3  # Inlaat Verl. Hoogeveense Vaart
model.outlet.static.df.loc[model.outlet.static.df.node_id == 847, "flow_rate"] = 0.3  # Inlaat Barriereweg

model.node.df[IS_SUPPLY_NODE_COLUMN] = False
mask = model.node.df["meta_code_waterbeheerder"].str.contains("KIN-", case=False, na=False)
model.node.df.loc[mask, IS_SUPPLY_NODE_COLUMN] = True

# user-defined drain_nodes
drain_nodes = [
    39,
    50,
    59,
    62,
    71,
    80,
    88,
    99,
    103,
    105,
    112,
    133,
    135,
    179,
    196,
    206,
    231,
    233,
    250,
    253,
    256,
    257,
    285,
    316,
    321,
    325,
    345,
    352,
    369,
    425,
    431,
    507,
    514,
    523,
    524,
    528,
    558,
    603,
    628,
    792,
    801,
    870,
    892,
    966,
    988,
    1043,
    1046,
    1109,
    79,
    297,
    30,
    447,
]
# user-defined supply_nodes
supply_nodes = [20, 62, 70, 107, 179, 573, 891, 972, 1122]
# user-defined flow_control_nodes
flow_control_nodes = [182, 330, 571, 573, 634, 679, 165, 505, 1017, 1111]
# user-defined flushing_nodes voor de overige connectoren
uncontrolled_flushing_nodes = {192: 1.65}


# markeer inlaten
model.node.df[IS_SUPPLY_NODE_COLUMN] = False
mask = model.node.df["meta_code_waterbeheerder"].str.contains("KIN-", case=False, na=False)
model.node.df.loc[mask, IS_SUPPLY_NODE_COLUMN] = True
model.node.df.loc[drain_nodes + flow_control_nodes, IS_SUPPLY_NODE_COLUMN] = False

# %%
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True)
conflicting_node_ids = aanvoergebieden_df.groupby("node_id")["aanvoergebied"].nunique()
conflicting_node_ids = conflicting_node_ids[conflicting_node_ids > 1].index
if len(conflicting_node_ids) > 0:
    conflicts = aanvoergebieden_df.loc[
        aanvoergebieden_df["node_id"].isin(conflicting_node_ids), ["node_id", "aanvoergebied"]
    ].sort_values(["node_id", "aanvoergebied"])
    raise ValueError(f"node_id values linked to multiple aanvoergebieden found:\n{conflicts.to_string(index=True)}")
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


# %%
# Hoogeveense Vaart

polygon = aanvoergebieden_df.loc[["Verl. Hoogeveense Vaart"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)


# %%
# Borgerzijtak

polygon = aanvoergebieden_df.loc[["Borgerzijtak"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2719]
# 2453: Sifon onder Winschoterdiep

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)


# %%
# Oldambt

polygon = aanvoergebieden_df.loc[["Oldambt"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2453]
# 2453: Sifon onder Winschoterdiep

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# TAK

polygon = aanvoergebieden_df.loc[["TAK"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [3650]
# 3650: Sifon onder Ter Apelkanaal

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# Fiemel/Westerwolde

polygon = aanvoergebieden_df.loc[["Fiemel/Westerwolde"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [297, 1941]
# 3650: Sifon onder Ter Apelkanaal

# doorspoeling (op uitlaten)
flushing_nodes = {147: 1.0}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)


# %%
# Küpers

polygon = aanvoergebieden_df.loc[["Küpers"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [235, 2776, 2778]
# 235: Manning richting Winschoterdiep
# 2776: Manning richting Wildervanckkanaal
# 2778: Manning richting Pekel Aa

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# Vennix

polygon = aanvoergebieden_df.loc[["Vennix"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [3650]
# 3650: Sifon onder Ter Apelkanaal

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# Drentse Aa

polygon = aanvoergebieden_df.loc[["Drentse Aa"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2751]
# ###: beschrijving

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# Zuidlaardermeer/Winschoterdiep

polygon = aanvoergebieden_df.loc[["Zuidlaardermeer/Winschoterdiep"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2286, 2384, 2607]
# 2286: Manning Drentsche Diep
# 2384: Manning Winschoterdiep
# 2607: Manning Winschoterdiep

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# Duurswold

node_ids = [829, 822, 810, 190, 162]
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "flow_rate"] = (
    0  # geen aanvoer vanuit de Eems (i.v.m. zout)
)

polygon = aanvoergebieden_df.loc[["Duurswold"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [747]
# 747: sifon onder Eemskanaal, dus helemaal prima

# doorspoeling (op uitlaten)
flushing_nodes = {}

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)


# %%
# En de rest toevoegen


add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    exclude_nodes=list(EXCLUDE_NODES),
    supply_nodes=supply_nodes,
    flushing_nodes=uncontrolled_flushing_nodes,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
# Bestaande handmatige pompwaarden blijven leidend.
pump_max_flow_rate_from_results = {
    35: 2,  # Gemaal Breebaart; oude static flow_rate=0.00233
    53: 1,  # Gemaal Wildervank; oude static flow_rate=0.383
    65: 2,  # Gemaal Valthermond; oude static flow_rate=0.0917
    83: 20,  # Gemaal Holtkamp; oude static flow_rate=0.267
    89: 5,  # gekoppeld max=4.67, huidige max=2.67, link=3300163
    101: 20,  # Aanvoergemaal Alteveer; oude static flow_rate=0.192
    116: 1,  # gekoppeld max=0.92, huidige max=0.667, link=3300384
    118: 2,  # Gemaal Haansvaart; oude static flow_rate=0.0833
    122: 1,  # Gemaal Herleving; oude static flow_rate=0.3
    128: 1,  # Gemaal Wildervank Sportterrein; oude static flow_rate=0.333
    131: 1,  # Aanvoergemaal Veenhuizerstukken; oude static flow_rate=0.117
    939: 1,  # Aanvoergemaal Nieuw Buinen-Hospers; oude static flow_rate=0.0267
    958: 1,  # Aanvoergemaal Tweede Exloermond; oude static flow_rate=0.2
}
mask = (
    model.pump.static.df.node_id.isin(pump_max_flow_rate_from_results)
    & model.pump.static.df.flow_rate.notna()
    & (model.pump.static.df.flow_rate > 0)
)
model.pump.static.df.loc[mask, "max_flow_rate"] = model.pump.static.df.loc[mask, "node_id"].map(
    pump_max_flow_rate_from_results
)

# %% Junctionfy!
junctionify(model)

# Minimale afvoer naar buitenwater op specifieke kunstwerken, voor alle control_states.
for node_id, min_flow_rate in MIN_FLOW_RATE_BY_NODE_ID.items():
    for static_df in (model.outlet.static.df, model.pump.static.df):
        if "min_flow_rate" not in static_df.columns:
            continue
        mask = static_df.node_id == node_id
        static_df.loc[mask, "min_flow_rate"] = min_flow_rate

configure_always_on_pumps(model)

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
    (model.pump.static.df, pump_max_flow_rate_by_node_id),
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

# %%
# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

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

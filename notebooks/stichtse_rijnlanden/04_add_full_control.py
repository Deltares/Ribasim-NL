# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
    mark_max_downstream_level_update_protected,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "StichtseRijnlanden"  # authority
SHORT_NAME: str = "hdsr"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 746: Oudewater sluis
# 750: Oude Leidseweg Sluis
# 753: Woerdenseverlaat SLuis
# 751: Montfoort Sluis
EXCLUDE_NODES = {486, 745, 746, 750, 753, 754}
EXCLUDE_SUPPLY_NODES = [80, 145, 354, 414, 928]
outlet_max_flow_rate_from_results = {
    515: 48,  # ST0225
    758: 2,  # Zuidersluis
}
outlet_max_flow_rate_coupled_by_node_id = {
    114: 2,  # gekoppeld max=0.91, huidige max=0.00, link=1400109
    124: 2,  # gekoppeld max=0.99, huidige max=0.00, link=1400130
    130: 3,  # gekoppeld max=1.17, huidige max=0.00, link=1400146
    141: 2,  # gekoppeld max=0.94, huidige max=0.00, link=1400174
    150: 3,  # gekoppeld max=1.37, huidige max=0.00, link=1400188
    155: 11,  # gekoppeld max=6.70, huidige max=0.00, link=1400196
    183: 2,  # gekoppeld max=0.51, huidige max=0.00, link=1400283
    190: 15,  # gekoppeld max=9.99, huidige max=0.00, link=1400297
    226: 17,  # gekoppeld max=10.51, huidige max=0.00, link=1400364
    249: 8,  # gekoppeld max=4.23, huidige max=0.00, link=1400409
    341: 2,  # gekoppeld max=0.89, huidige max=0.00, link=1400622
    342: 2,  # gekoppeld max=0.89, huidige max=0.00, link=1400623
    343: 8,  # gekoppeld max=4.23, huidige max=0.00, link=1400410
    351: 2,  # gekoppeld max=0.59, huidige max=0.00, link=1400634
    354: 5,  # gekoppeld max=2.74, huidige max=0.00, link=8000969
    368: 5,  # gekoppeld max=2.57, huidige max=0.00, link=1400656
    372: 6,  # gekoppeld max=3.85, huidige max=0.00, link=1400660
    399: 2,  # gekoppeld max=0.52, huidige max=0.00, link=1400702
    401: 3,  # gekoppeld max=1.01, huidige max=0.00, link=1400704
    409: 2,  # gekoppeld max=0.89, huidige max=0.00, link=1400624
    468: 3,  # gekoppeld max=1.70, huidige max=0.00, link=1400748
    481: 14,  # gekoppeld max=8.82, huidige max=0.00, link=1400043
    488: 2,  # gekoppeld max=0.99, huidige max=0.00, link=1400131
    489: 2,  # gekoppeld max=0.99, huidige max=0.00, link=1400132
    747: 3,  # gekoppeld max=1.06, huidige max=0.00, link=1400903
    754: 3,  # gekoppeld max=1.64, huidige max=0.00, link=1401470
    761: 9,  # gekoppeld max=5.18, huidige max=0.00, link=1402642
    772: 18,  # gekoppeld max=11.53, huidige max=0.00, link=1400292
    829: 101,  # ST4148; gekoppeld max=66.29, parameterized=20.00
    872: 62,  # Kockengen Stuw; gekoppeld max=40.92, parameterized=20.00
    1042: 3,  # gekoppeld max=1.45, huidige max=0.00, link=1400793
    1214: 59,  # ST4146; gekoppeld max=38.65, parameterized=20.00
}
outlet_max_flow_rate_afvoer_by_node_id = {}
for max_flow_rates in (
    outlet_max_flow_rate_from_results,
    outlet_max_flow_rate_coupled_by_node_id,
):
    for node_id, max_flow_rate in max_flow_rates.items():
        outlet_max_flow_rate_afvoer_by_node_id[node_id] = max(
            outlet_max_flow_rate_afvoer_by_node_id.get(node_id, 0.0),
            max_flow_rate,
        )

# Capaciteiten uit het HKV-distributiemodel; in full control op de juiste richting zetten.
pump_text_capacity_aanvoer_by_node_id = {
    481: 10.0,  # Inlaat Wijk bij Duurstede
    536: 12.0,  # Noordergemaal
    542: 7.0,  # Inlaatgemaal de Aanvoerder
    601: 3.3,  # Gemaal Caspargouwse Wetering
    623: 5.34,  # Doorlaat Keulevaart
}
pump_text_capacity_afvoer_by_node_id = {
    541: 11.2,  # Gemaal de Koekoek
    623: 5.34,  # Doorlaat Keulevaart
}
outlet_text_capacity_aanvoer_by_node_id = {
    141: 6.2,  # Inlaat Hekendorp
    252: 18.0,  # Inlaatsluis Vreeswijk
    742: 6.2,  # Haanwijkersluis
}
outlet_text_capacity_afvoer_by_node_id = {
    752: 28.0,  # Spuisluis Oog in Al
    755: 25.0,  # Spui- en schutsluis Bodegraven
    757: 16.0,  # Weerdsluis
    758: 7.0,  # Zuidersluis
    761: 6.0,  # Aflaatwerk Caspargouwse Wetering
}
#
# Handmatige indeling control-, supply- en drain-nodes.
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [
    207, 233, 527, 623, 636, 762, 777, 778, 809, 814, 889, 922, 977, 1010, 1011, 1033, 1036,
    1038, 1039, 1050, 1059, 1107, 1153, 1154, 1155, 1279,
]

supply_nodes = [
    100, 103, 358, 424, 425, 476, 481, 486, 506, 534, 536, 542, 543, 553, 554, 564, 581,
    589, 593, 601, 624, 626, 627, 630, 634, 637, 638, 639, 640, 648, 649, 650, 651,
    654, 655, 742, 747, 754, 772, 797, 830, 840, 855, 890, 906, 911, 924,
    962, 976, 987, 1014, 1022, 1042, 1056, 1082, 1156, 2111,
]

drain_nodes = [
    80, 134, 139, 145, 168, 173, 185, 198, 230, 298, 347, 354, 411, 414, 454, 467, 477, 513, 545, 551, 554,
    588, 591, 598, 612, 633, 678, 740, 761, 799, 810,818, 844, 851, 864, 887, 893, 894,
    920, 928, 944, 956, 969, 971, 975, 978, 979, 980, 993, 1006,1007, 1033, 1077, 1126, 1145, 1168, 1203, 1223,
    1063, 2104, 2110,
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
        flow_rate_aanvoer=20.0,
        max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
        flow_rate_afvoer=100.0,
        max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    )


def set_manual_capacity(static_df, capacities_by_node_id: dict[int, float], control_state: str) -> None:
    for node_id, capacity in capacities_by_node_id.items():
        mask = static_df["node_id"].eq(node_id)
        if "control_state" in static_df.columns:
            mask &= static_df["control_state"].eq(control_state)
        static_df.loc[mask, ["flow_rate", "max_flow_rate"]] = capacity


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

outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

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


# %% model fixes
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 45, "level"] = -1.4
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id.isin([47, 49]), "level"] = 0.54

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

# 481: 1400481 is een gemaal, dus pump in full control
model.update_node(node_id=481, node_type="Pump")

model.update_node(node_id=405, node_type="ManningResistance")
model.update_node(node_id=730, node_type="ManningResistance")

model.remove_node(node_id=1345, remove_links=True)

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

# Zuidersluis beetje uitlaat anders vollopen Merwedekanaal benenden Lek
model.outlet.static.df.loc[model.outlet.static.df.node_id == 758, ["flow_rate", "max_flow_rate"]] = 1
# %% Toevoegen waar nog geen sturing is toegevoegd

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

node_type_by_id = model.node.df["node_type"].to_dict()


def discrete_control_node_id(target_node_id: int) -> int:
    control_node_ids = model.link.df.loc[
        model.link.df["link_type"].eq("control")
        & model.link.df["to_node_id"].eq(target_node_id)
        & model.link.df["from_node_id"].map(node_type_by_id).eq("DiscreteControl"),
        "from_node_id",
    ].astype(int)
    if len(control_node_ids) != 1:
        raise ValueError(f"Expected one DiscreteControl for node {target_node_id}, found {control_node_ids.to_list()}")
    return int(control_node_ids.iloc[0])


# Keulevaart, node 623: in aanvoer sturen min_upstream-1m.
for static_df in (model.outlet.static.df, model.pump.static.df):
    mask = static_df["node_id"].eq(623) & static_df["control_state"].eq("aanvoer")
    static_df.loc[mask, "min_upstream_level"] -= 1
    mark_level_update_protected(static_df, mask, model=model)


# Werkhoven stuw, node 919, in aanvoer 4 cm eerder open.
for static_df in (model.outlet.static.df, model.pump.static.df):
    mask = static_df["node_id"].eq(919) & static_df["control_state"].eq("aanvoer")
    static_df.loc[mask, "min_upstream_level"] -= 0.1
    mark_level_update_protected(static_df, mask, model=model)

# Cothen stuw, node 777, in aanvoer 10 cm eerder open.
for static_df in (model.outlet.static.df, model.pump.static.df):
    mask = static_df["node_id"].eq(777) & static_df["control_state"].eq("aanvoer")
    static_df.loc[mask, "min_upstream_level"] -= 0.1
    mark_level_update_protected(static_df, mask, model=model)

# Caspargauw Zemelen, node 596, in aanvoer 10 cm eerder open.
mask = model.pump.static.df["node_id"].eq(596) & model.pump.static.df["control_state"].eq("aanvoer")
model.pump.static.df.loc[mask, "min_upstream_level"] -= 1
mark_level_update_protected(model.pump.static.df, mask, model=model)

# Caspargauw inlaat, node 601, voert aan wanneer Wijk bij Duurstede pomp 481 meer dan 6 m3/s voert.
control_node_id = discrete_control_node_id(601)
mask = model.discrete_control.variable.df["node_id"].eq(control_node_id) & model.discrete_control.variable.df[
    "compound_variable_id"
].eq(1)
model.discrete_control.variable.df.loc[mask, ["listen_node_id", "variable", "weight"]] = [481, "flow_rate", -1]
mask = model.discrete_control.condition.df["node_id"].eq(control_node_id) & model.discrete_control.condition.df[
    "compound_variable_id"
].eq(1)
model.discrete_control.condition.df.loc[mask, ["threshold_high", "threshold_low"]] = -6.0

# Caspargauw uitlaat, node 761, voert af wanneer Werkhoven stuw 919 meer dan 6 m3/s voert.
control_node_id = discrete_control_node_id(761)
mask = model.discrete_control.variable.df["node_id"].eq(control_node_id) & model.discrete_control.variable.df[
    "compound_variable_id"
].eq(1)
model.discrete_control.variable.df.loc[mask, ["listen_node_id", "variable", "weight"]] = [919, "flow_rate", 1]
mask = model.discrete_control.condition.df["node_id"].eq(control_node_id) & model.discrete_control.condition.df[
    "compound_variable_id"
].eq(1)
model.discrete_control.condition.df.loc[mask, ["threshold_high", "threshold_low"]] = 6.0

# %% Noordergemaal, node=536 slaat pas aan wanneer Wijk van Duurstede net genoeg kan leveren
mask = (model.pump.static.df.node_id == 536) & model.pump.static.df.max_downstream_level.notna()
model.pump.static.df.loc[mask, "max_downstream_level"] -= 0.01
mark_max_downstream_level_update_protected(model.pump.static.df, mask, model=model)
mask = model.outlet.static.df.node_id == 1344
model.outlet.static.df.loc[mask, "max_downstream_level"] -= 0.01
mark_max_downstream_level_update_protected(model.outlet.static.df, mask, model=model)

# 3 sifons, 468,469,470 onder Ark wordt later ingeschakeld dan inlaat Vreeswijk
mask = model.outlet.static.df.node_id.isin([468, 469, 470])
model.outlet.static.df.loc[mask, "max_downstream_level"] -= 0.01
mark_max_downstream_level_update_protected(model.outlet.static.df, mask, model=model)

# Caspargauw gaat pas leveren als Wijk bij Duurstede aanvoer te laag is
mask = model.pump.static.df.node_id == 601
model.pump.static.df.loc[mask, "max_downstream_level"] -= 0.01
mark_max_downstream_level_update_protected(model.pump.static.df, mask, model=model)

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
pump_max_flow_rate_from_results = {
    506: 1,  # Papekopperdijk; oude static flow_rate=0.0667
    513: 50,  # Terwijde; oude static flow_rate=0.45
    529: 2,  # Fort Overeind; gekoppeld max=1.26, huidige max=1.00, link=1400825
    534: 6.2,  # Zwaan, De; oude static flow_rate=0.4
    538: 1,  # Wijkersloot; oude static flow_rate=0.25
    542: 1,  # gekoppeld max=0.75, huidige max=0.00, link=1400841
    546: 2,  # Ossenwaard; oude static flow_rate=0.333
    551: 1,  # Biester; oude static flow_rate=0.0617
    555: 21,  # Vuylcop-West; oude static flow_rate=1.17
    559: 1,  # Hoon - West, De; oude static flow_rate=0.0667
    561: 1,  # Gemaal Rijnwijck; oude static flow_rate=0.5
    568: 1,  # Waalseweg; oude static flow_rate=0.0833
    578: 9,  # gekoppeld max=8.09, huidige max=0.70, link=1400003
    579: 1,  # gekoppeld max=0.67, huidige max=0.00, link=1400686
    585: 3,  # Smidsdijk; oude static flow_rate=1.37
    588: 4,  # Blokhoven; oude static flow_rate=1.25
    592: 8,  # Snel En Polanen; oude static flow_rate=0.5
    593: 1,  # naam onbekend; oude static flow_rate=0.025
    598: 25,  # Vleuterweide; oude static flow_rate=0.892
    602: 3,  # Beerschoten; oude static flow_rate=0.5
    603: 2,  # Fortuin; oude static flow_rate=0.217
    609: 3,  # Westraven; oude static flow_rate=0.167
    612: 3,  # gekoppeld max=2.36, huidige max=0.238, link=1400257
    611: 15,  # Gerverscop; oude static flow_rate=1.58
    619: 15,  # Kockengen; oude static flow_rate=1.95
    621: 1,  # Grechtkade; oude static flow_rate=0.00753
    623: 10,  # Keulevaart, De; oude static flow_rate=5.34
    624: 2,  # Pelikaan; oude static flow_rate=0.1
    626: 2,  # Strijp, De; oude static flow_rate=0.0333
    627: 1,  # Overeind; oude static flow_rate=0.25
    631: 1,  # Oude Meije; oude static flow_rate=0.533
    632: 1,  # Toegang, De; oude static flow_rate=0.267
    633: 2,  # Voordorp; oude static flow_rate=0.833
    634: 6.2,  # Hazepad 'T; oude static flow_rate=1.37
    636: 1,  # Tappersheul; oude static flow_rate=0.0417
    637: 1,  # Hwvz Diemerbroek 56; oude static flow_rate=0.0667
    640: 20,  # Schoonhoven; oude static flow_rate=0.167
    641: 15,  # Hwvz Teckop; oude static flow_rate=0.0333
    642: 5,  # gekoppeld max=4.20, huidige max=0.45, link=1400211
    643: 1,  # Heul, De (Zuurhout); oude static flow_rate=0.0583
    644: 1,  # Tureluur; oude static flow_rate=0.383
    646: 20,  # Rembrandtkade/Minstroom; oude static flow_rate=0.167
    647: 5,  # Leesloot; gekoppeld max=4.14, huidige max=1.00, link=1400730
    649: 1,  # gekoppeld max=0.67, huidige max=0.00, link=1400970
    651: 10,  # Pothoek; oude static flow_rate=0.2
    652: 20,  # Moersbergen; oude static flow_rate=0.0833
    656: 3,  # Weerdenburg; oude static flow_rate=0.133
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
manual_max_flow_rate_node_ids = {758}
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

set_manual_capacity(model.pump.static.df, pump_text_capacity_aanvoer_by_node_id, "aanvoer")
set_manual_capacity(model.pump.static.df, pump_text_capacity_afvoer_by_node_id, "afvoer")
set_manual_capacity(model.outlet.static.df, outlet_text_capacity_aanvoer_by_node_id, "aanvoer")
set_manual_capacity(model.outlet.static.df, outlet_text_capacity_afvoer_by_node_id, "afvoer")

# Aanvoer richting basin 2089 uitzetten; anders kan het model rondpompen via de Zwaan.
control_node_ids = set(
    model.discrete_control.variable.df.loc[
        model.discrete_control.variable.df["listen_node_id"].eq(2089),
        "node_id",
    ].astype(int)
)
target_node_ids = set(
    model.link.df.loc[
        model.link.df["link_type"].eq("control") & model.link.df["from_node_id"].isin(control_node_ids),
        "to_node_id",
    ].astype(int)
)
target_node_ids.discard(100)
for static_df in (model.outlet.static.df, model.pump.static.df):
    mask = static_df["node_id"].isin(target_node_ids) & static_df["control_state"].eq("aanvoer")
    static_df.loc[mask, "flow_rate"] = 0.0

# Aanvoer uitzetten vanwege rondpompen bij Keulevaart.
mask = model.outlet.static.df["node_id"].isin([321, 322, 323, 324, 325, 326]) & model.outlet.static.df[
    "control_state"
].eq("aanvoer")
model.outlet.static.df.loc[mask, "flow_rate"] = 0.0

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

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

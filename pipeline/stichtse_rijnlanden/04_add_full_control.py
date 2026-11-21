# %%
from typing import cast

import geopandas as gpd
import numpy as np
import pandas as pd
from pandera.typing import DataFrame
from ribasim import Node
from ribasim.nodes import discrete_control, pump
from ribasim.schemas import DiscreteControlConditionSchema, DiscreteControlLogicSchema, DiscreteControlVariableSchema
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
    supply_area_polygon,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.layout import move_nodes
from ribasim_nl.run_model import write_steady_state_scenarios
from shapely.geometry import LineString, Point

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "StichtseRijnlanden"  # authority
SHORT_NAME: str = "hdsr"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoer_path = aanvoer_path = cloud.joinpath(AUTHORITY, "verwerkt/4_ribasim/peilgebieden_bewerkt.gpkg")

aanvoergebieden_gpkg = cloud.joinpath(r"StichtseRijnlanden/verwerkt/sturing/aanvoergebieden.gpkg")


# %%
# Read data
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 746: Oudewater sluis
# 750: Oude Leidseweg Sluis
# 753: Woerdenseverlaat SLuis
# 751: Montfoort Sluis
EXCLUDE_NODES = {n("ST0236"), n("SL0010"), n("SL0008"), n("SL0014"), n("SL0006")}
EXCLUDE_SUPPLY_NODES = []
outlet_max_flow_rate_from_results = {
    n("ST0225"): 48,  # ST0225
    n("SL0016"): 2,  # Zuidersluis
}
outlet_max_flow_rate_coupled_by_node_id = {
    n("I6145"): 2,  # gekoppeld max=0.91, huidige max=0.00, link=1400109
    n("I6038"): 2,  # gekoppeld max=0.99, huidige max=0.00, link=1400130
    n("AF0090"): 3,  # gekoppeld max=1.17, huidige max=0.00, link=1400146
    n("I6098"): 2,  # gekoppeld max=0.94, huidige max=0.00, link=1400174
    n("I1574"): 3,  # gekoppeld max=1.37, huidige max=0.00, link=1400188
    n("I6053"): 11,  # gekoppeld max=6.70, huidige max=0.00, link=1400196
    n("I1247"): 2,  # gekoppeld max=0.51, huidige max=0.00, link=1400283
    n("I6188"): 15,  # gekoppeld max=9.99, huidige max=0.00, link=1400297
    n("I0021"): 17,  # gekoppeld max=10.51, huidige max=0.00, link=1400364
    n("I0551"): 8,  # gekoppeld max=4.23, huidige max=0.00, link=1400409
    n("I0550"): 2,  # gekoppeld max=0.89, huidige max=0.00, link=1400622
    n("I0553"): 2,  # gekoppeld max=0.89, huidige max=0.00, link=1400623
    n("I0186"): 8,  # gekoppeld max=4.23, huidige max=0.00, link=1400410
    n("I6102"): 2,  # gekoppeld max=0.59, huidige max=0.00, link=1400634
    n("I6116"): 5,  # gekoppeld max=2.74, huidige max=0.00, link=8000969
    n("I1096"): 5,  # gekoppeld max=2.57, huidige max=0.00, link=1400656
    n("I1151"): 6,  # gekoppeld max=3.85, huidige max=0.00, link=1400660
    n("I1435"): 2,  # gekoppeld max=0.52, huidige max=0.00, link=1400702
    n("I6088"): 3,  # gekoppeld max=1.01, huidige max=0.00, link=1400704
    n("I2328"): 2,  # gekoppeld max=0.89, huidige max=0.00, link=1400624
    n("SY0070"): 3,  # gekoppeld max=1.70, huidige max=0.00, link=1400748
    n("AF0093"): 14,  # gekoppeld max=8.82, huidige max=0.00, link=1400043
    n("I6036"): 2,  # gekoppeld max=0.99, huidige max=0.00, link=1400131
    n("I6037"): 2,  # gekoppeld max=0.99, huidige max=0.00, link=1400132
    n("SL0501"): 3,  # gekoppeld max=1.06, huidige max=0.00, link=1400903
    n("SL0030"): 3,  # gekoppeld max=1.64, huidige max=0.00, link=1401470
    n("ST3003"): 9,  # gekoppeld max=5.18, huidige max=0.00, link=1402642
    n("ST6113"): 18,  # gekoppeld max=11.53, huidige max=0.00, link=1400292
    n("ST4148"): 101,  # ST4148; gekoppeld max=66.29, parameterized=20.00
    n("ST1919"): 62,  # Kockengen Stuw; gekoppeld max=40.92, parameterized=20.00
    n("ST0069"): 3,  # gekoppeld max=1.45, huidige max=0.00, link=1400793
    n("ST4146"): 59,  # ST4146; gekoppeld max=38.65, parameterized=20.00
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
#
# Handmatige indeling control-, supply- en drain-nodes.
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [
    n("I5051"), n("I6023"), n("AF0095"), n("I1655"), n("G6025"), n("ST3001"), n("ST3912"), n("ST1066"), n("ST6055"),
    n("ST1433"), n("ST1257"), n("ST1353"), n("ST4112"), n("ST5011"), n("ST1758"), n("ST5009"), n("ST0477"), n("ST5022"),
    n("ST1091"), n("ST0826"), n("ST6092"), n("ST0479"), n("ST7263"), n("ST1888"),
]

supply_nodes = [
    n("I6000"), n("AF0082"), n("I6189"), n("AF0038"), n("D13544"), n("AF0093"), n("ST0236"), n("G0039"), n("G6007"),
    n("G4031"), n("G8012"), n("G0096"), n("G0007"), n("G6032"), n("G8001"), n("G0005"), n("G8014"), n("G3004"),
    n("G4481"), n("G2005"), n("G4015"), n("G6020"), n("G0040"), n("G6024"), n("G6023"), n("G0085"), n("G3007"),
    n("G3012"), n("G1016"), n("G4023"), n("G2014"), n("G0011"), n("SL0003"), n("SL0501"), n("SL0030"), n("ST3003"),
    n("ST6113"), n("ST0945"), n("ST3928"), n("ST1615"), n("ST4036"), n("ST0946"), n("ST0779"), n("ST0722"), n("ST1194"),
    n("ST2903"), n("ST0850"), n("ST0409"), n("ST3120"), n("ST0439"), n("ST1319"), n("ST0069"), n("ST0485"), n("ST0243"),
    n("ST1064"), n("SCHRAALLANDEN"),
]

drain_nodes = [
    n("I1580"), n("I6118"), n("I2003"), n("AF0032"), n("I1317"), n("I6207"), n("SY3441"), n("SY1901"), n("G0038"),
    n("G0087"), n("G4022"), n("G0007"), n("G8002"), n("G4027"), n("G0068"), n("G3003"), n("G0089"), n("G1010"),
    n("G3024"), n("ST3003"), n("ST0360"), n("ST1926"), n("ST1541"), n("ST0014"), n("ST5322"), n("ST3128"), n("ST6050"),
    n("ST2901"), n("ST1449"), n("ST0895"), n("ST1014"), n("ST0842"), n("ST0010"), n("ST0814"), n("ST4007"), n("ST4022"),
    n("ST7229"), n("ST0712"), n("ST4112"), n("ST0725"), n("ST8009"), n("ST0003"), n("ST0733"), n("ST4115"), n("ST0815"),
    n("loc:136117,451389"),
]

flushing_nodes = {n("I6073"): 1.25, n("SL0009"): 3.0, n("ST2028"): 5}

SUPPLY_AREA_IGNORE_LINKS = {
    "Kromme Rijn/ARK": [lk("up:G3004", "ow:H012324")],
    "EVS": [lk("up:I0427", "ST0003")],
    "Lopikerwaard": [
        lk("ow:H057607", "up:OW575"), lk("H057916___3", "up:H071914___1"), lk("up:H057916___3", "H057916___3"),
        lk("up:H078195", "OW636"),
    ],
    "Leidsche-Oude Rijn": [
        lk("ST6113", "up:ST6098"), lk("H041348___2", "up:H041347___1"), lk("ST0236", "up:ST4058"),
        lk("G0087", "up:I1042"), lk("up:I1042", "ST0236"), lk("up:G4032", "G0087"), lk("up:G0068", "ST6113"),
    ],
    "Gek. Hollandsche IJssel": [
        lk("ST0771", "up:ST6098"), lk("ST6113", "up:ST6098"), lk("OW618", "up:H063301"), lk("ST0236", "up:ST4058"),
        lk("G0087", "up:I1042"), lk("up:I1042", "ST0236"), lk("up:G4032", "G0087"),
    ],
    "Utrecht-Noord": [],
    "Utrechtse Heuvelrug/Kromme Rijn": [lk("up:H012352___1", "H012343___2")],
}
# fmt: on


# %%
# Helpers


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


# %%
assert model.outlet.static.df is not None
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
assert model.level_boundary.static.df is not None
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == n("bnd:45"), "level"] = -1.4

# doorslag staat normaal open
model.reverse_link(link_id=lk("up:H063301", "SL0030"))
model.reverse_link(link_id=lk("SL0030", "loc:135314,450583"))

# De Pelikaan links omdraaien
model.reverse_link(link_id=lk("G4481", "up:I0427"))
model.reverse_link(link_id=lk("up:I0427", "ST8009"))
model.reverse_link(link_id=lk("ST8009", "up:G4481"))
model.reverse_link(link_id=lk("up:G4481", "G4481"))

# 416: I2075
model.reverse_link(link_id=lk("I2075", "up:I0027"))
model.reverse_link(link_id=lk("up:G2901", "I2075"))

# 138: I6125
model.reverse_link(link_id=lk("I6125", "bnd:15"))
model.reverse_link(link_id=lk("up:I6125", "I6125"))

# 545: Rijnvliet is afvoergemaal
model.reverse_link(link_id=lk("G0087", "up:I1042"))
model.reverse_link(link_id=lk("up:G4032", "G0087"))

# node 2806 is een inlaat, dus flow_direction draaien
model.reverse_link(link_id=lk("I0880", "bnd:70"))
model.reverse_link(link_id=lk("up:I0901", "I0880"))

# Gemaal Terwijde
model.reverse_link(link_id=lk("up:G0038", "G0038"))
model.reverse_link(link_id=lk("G0038", "up:I1096"))

# Caspergouw, see https://github.com/Deltares/Ribasim-NL/issues/913: Caspargauw (G3003) drains Basin 1728 to the ARK,
# and Caspargauw Zemelen (G3004) supplies it, from Basin 1619
CASPARGAUW, CASPARGAUW_ZEMELEN, CASPERGOUW_BASIN = n("G3003"), n("G3004"), n("up:G3004")
assert model.node.df.at[CASPARGAUW, "meta_code_waterbeheerder"] == "G3003"
assert model.node.df.at[CASPARGAUW_ZEMELEN, "meta_code_waterbeheerder"] == "G3004"
for link_id in (
    lk("bnd:9", "G3003"),
    lk("G3003", "up:G3004"),
    lk("up:G3004", "G3004"),
    lk("G3004", "up:I2216"),
):  # LevelBoundary 9 -> 601 -> 1728 -> 596 -> 1619
    model.reverse_link(link_id=link_id)
assert model.upstream_node_id(CASPARGAUW) == CASPERGOUW_BASIN
assert model.downstream_node_id(CASPARGAUW_ZEMELEN) == CASPERGOUW_BASIN
model.node.df.loc[CASPARGAUW_ZEMELEN, IS_SUPPLY_NODE_COLUMN] = True
assert CASPARGAUW_ZEMELEN in supply_nodes and CASPARGAUW in drain_nodes

model.update_node(node_id=n("OW689"), node_type="ManningResistance")

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
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("SL0016"), ["flow_rate", "max_flow_rate"]] = 1
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

# %% Noordergemaal, node=536 slaat pas aan wanneer Wijk van Duurstede net genoeg kan leveren
assert model.pump.static.df is not None
mask = model.pump.static.df.node_id == n("G6007")
model.pump.static.df.loc[mask, "max_downstream_level"] -= 0.01
mark_level_update_protected(model.pump.static.df, mask, model=model)
mask = model.outlet.static.df.node_id.isin([n("ow:H078553")])
model.outlet.static.df.loc[mask, "max_downstream_level"] -= 0.01
mark_level_update_protected(model.outlet.static.df, mask, model=model)

# sifon SY0070 onder het ARK wordt later ingeschakeld dan inlaat Vreeswijk; SY6009 en SY0071 zijn verwijderd
mask = model.outlet.static.df.node_id.isin([n("SY0070")])
model.outlet.static.df.loc[mask, "max_downstream_level"] -= 0.01
mark_level_update_protected(model.outlet.static.df, mask, model=model)

# Caspargouw wordt pas aangevoerd als Wijk bij Duurstede aanvoer te laag is
mask = model.pump.static.df.node_id == CASPARGAUW_ZEMELEN
model.pump.static.df.loc[mask, "max_downstream_level"] -= 0.01
mark_level_update_protected(model.pump.static.df, mask, model=model)

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
pump_max_flow_rate_from_results = {
    n("G0039"): 1,  # Papekopperdijk; oude static flow_rate=0.0667
    n("G0038"): 50,  # Terwijde; oude static flow_rate=0.45
    n("G4028"): 2,  # Fort Overeind; gekoppeld max=1.26, huidige max=1.00, link=1400825
    n("G5001"): 1,  # Zwaan, De; oude static flow_rate=0.4
    n("G8013"): 1,  # Wijkersloot; oude static flow_rate=0.25
    n("G4031"): 1,  # gekoppeld max=0.75, huidige max=0.00, link=1400841
    n("G3001"): 2,  # Ossenwaard; oude static flow_rate=0.333
    n("G4022"): 1,  # Biester; oude static flow_rate=0.0617
    n("G4025"): 21,  # Vuylcop-West; oude static flow_rate=1.17
    n("G4029"): 1,  # Hoon - West, De; oude static flow_rate=0.0667
    n("G1003"): 1,  # Gemaal Rijnwijck; oude static flow_rate=0.5
    n("G0088"): 1,  # Waalseweg; oude static flow_rate=0.0833
    n("G4056"): 9,  # gekoppeld max=8.09, huidige max=0.70, link=1400003
    n("G1004"): 1,  # gekoppeld max=0.67, huidige max=0.00, link=1400686
    n("G3002"): 3,  # Smidsdijk; oude static flow_rate=1.37
    n("G8002"): 4,  # Blokhoven; oude static flow_rate=1.25
    n("G4059"): 8,  # Snel En Polanen; oude static flow_rate=0.5
    n("G8014"): 1,  # naam onbekend; oude static flow_rate=0.025
    n("G0068"): 25,  # Vleuterweide; oude static flow_rate=0.892
    n("G1002"): 3,  # Beerschoten; oude static flow_rate=0.5
    n("G0009"): 2,  # Fortuin; oude static flow_rate=0.217
    n("G6004"): 3,  # Westraven; oude static flow_rate=0.167
    n("G0089"): 3,  # gekoppeld max=2.36, huidige max=0.238, link=1400257
    n("G4039"): 15,  # Gerverscop; oude static flow_rate=1.58
    n("G4041"): 15,  # Kockengen; oude static flow_rate=1.95
    n("G4043"): 1,  # Grechtkade; oude static flow_rate=0.00753
    n("G3025"): 10,  # Keulevaart, De; oude static flow_rate=5.34
    n("G4481"): 2,  # Pelikaan; oude static flow_rate=0.1
    n("G2005"): 2,  # Strijp, De; oude static flow_rate=0.0333
    n("G4015"): 1,  # Overeind; oude static flow_rate=0.25
    n("G4050"): 1,  # Oude Meije; oude static flow_rate=0.533
    n("G0029"): 1,  # Toegang, De; oude static flow_rate=0.267
    n("G1010"): 2,  # Voordorp; oude static flow_rate=0.833
    n("G3024"): 3,  # Hazepad 'T; oude static flow_rate=1.37
    n("G6025"): 1,  # Tappersheul; oude static flow_rate=0.0417
    n("G0040"): 1,  # Hwvz Diemerbroek 56; oude static flow_rate=0.0667
    n("G0085"): 20,  # Schoonhoven; oude static flow_rate=0.167
    n("G0043"): 15,  # Hwvz Teckop; oude static flow_rate=0.0333
    n("G3022"): 5,  # gekoppeld max=4.20, huidige max=0.45, link=1400211
    n("G4020"): 1,  # Heul, De (Zuurhout); oude static flow_rate=0.0583
    n("G0006"): 1,  # Tureluur; oude static flow_rate=0.383
    n("G1006"): 20,  # Rembrandtkade/Minstroom; oude static flow_rate=0.167
    n("G4371"): 5,  # Leesloot; gekoppeld max=4.14, huidige max=1.00, link=1400730
    n("G3012"): 1,  # gekoppeld max=0.67, huidige max=0.00, link=1400970
    n("G4023"): 3,  # Pothoek; oude static flow_rate=0.2
    n("G0046"): 20,  # Moersbergen; oude static flow_rate=0.0833
    n("G4311"): 3,  # Weerdenburg; oude static flow_rate=0.133
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


def straight_link(model: Model, from_node_id: int, to_node_id: int) -> None:
    """Make the only flow link from `from_node_id` to `to_node_id` straight.

    It gets a vertex halfway, as straight links of two points are routed along the waterways later, see
    07_add_dynamic_forcing.py.
    """
    links = model.link.df
    (link_id,) = links.index[
        (links.link_type == "flow") & (links.from_node_id == from_node_id) & (links.to_node_id == to_node_id)
    ]
    start, end = model.get_node(from_node_id).geometry, model.get_node(to_node_id).geometry
    middle = LineString([start, end]).interpolate(0.5, normalized=True)
    model.link.df.loc[[link_id], "geometry"] = gpd.GeoSeries(
        [LineString([start, middle, end])], index=[link_id], crs=links.crs
    )


# Caspergouw without crossing links, see #913: the drains of Basin 1728 branch off a Junction on the straight line to
# Caspargouw Stuw (ST3003), and its supplies join on a Junction on the straight line from Caspargauw Zemelen. The
# connectors are moved apart at least the minimum distance of `tidy_layout`, so it leaves them in place.
CASPARGOUW_STUW, CASPERGOUW_INLAAT = n("ST3003"), n("ow:H078553")
(caspergouw_drain_junction,) = {model.upstream_node_id(CASPARGAUW), model.upstream_node_id(CASPARGOUW_STUW)}
caspergouw_supply_junction = model.downstream_node_id(CASPARGAUW_ZEMELEN)
assert caspergouw_supply_junction == model.downstream_node_id(CASPERGOUW_INLAAT)
assert model.get_node_type(caspergouw_drain_junction) == model.get_node_type(caspergouw_supply_junction) == "Junction"
assert model.upstream_node_id(caspergouw_drain_junction) == CASPERGOUW_BASIN
assert model.downstream_node_id(caspergouw_supply_junction) == CASPERGOUW_BASIN
caspergouw_locations = {
    CASPARGOUW_STUW: Point(146254, 444077),
    CASPARGAUW: Point(146262, 444089),
    CASPARGAUW_ZEMELEN: Point(146240, 444103),
    CASPERGOUW_INLAAT: Point(146238, 444121),
}
basin_point = model.get_node(CASPERGOUW_BASIN).geometry
caspergouw_locations[caspergouw_drain_junction] = LineString(
    [basin_point, caspergouw_locations[CASPARGOUW_STUW]]
).interpolate(17.0)
caspergouw_locations[caspergouw_supply_junction] = LineString(
    [caspergouw_locations[CASPARGAUW_ZEMELEN], basin_point]
).interpolate(0.5, normalized=True)
move_nodes(model, caspergouw_locations)
for from_node_id, to_node_id in (
    (CASPERGOUW_BASIN, caspergouw_drain_junction),
    (caspergouw_drain_junction, CASPARGOUW_STUW),
    (caspergouw_drain_junction, CASPARGAUW),
    (CASPARGAUW_ZEMELEN, caspergouw_supply_junction),
    (CASPERGOUW_INLAAT, caspergouw_supply_junction),
    (caspergouw_supply_junction, CASPERGOUW_BASIN),
):
    straight_link(model, from_node_id, to_node_id)

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
manual_max_flow_rate_node_ids = {n("SL0016")}
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

# %% Wijk bij Duurstede stuurt op het debiet bij ADCP Amelisweerd, zie #855
# De control_states heten geen aanvoer/afvoer, zodat de koppelpeil-synchronisatie deze sturing niet aanpast.
WIJK_BIJ_DUURSTEDE = n("AF0093")
AMELISWEERD = n("H072046")  # ManningResistance in de Kromme Rijn benedenstrooms van Basin 1673, bij ADCP Amelisweerd
AMELISWEERD_THRESHOLDS = [6.0, 7.0]  # m3/s; regime 1 bij <= 6, regime 2 bij > 6, regime 3 en 4 bij > 7
WIJK_BIJ_DUURSTEDE_FLOW_RATES = {"regime 1": 5.0, "regime 2": 3.5, "regime 3+4": 1.0}  # m3/s
FLOW_THRESHOLD_HYSTERESIS = 0.1  # m3/s


def set_flow_regime_control(
    model: Model,
    node_id: int,
    listen_node_id: int,
    thresholds: list[float],
    flow_rates: dict[str, float],
    name: str,
) -> None:
    """Let the DiscreteControl of Outlet `node_id` set its flow rate per regime of the flow at `listen_node_id`.

    Regime i applies while the flow exceeds i of the ascending `thresholds`. The levels of the aanvoer state are kept.
    """
    assert len(flow_rates) == len(thresholds) + 1, "Expected one flow rate per regime"
    assert thresholds == sorted(thresholds), "Expected ascending thresholds"
    assert model.get_node_type(node_id) == "Outlet", f"Expected Outlet for node {node_id}"
    assert model.get_node_type(listen_node_id) in {"ManningResistance", "Outlet", "Pump", "TabulatedRatingCurve"}

    links = model.link.df
    control_node_ids = links.loc[(links.link_type == "control") & (links.to_node_id == node_id), "from_node_id"]
    assert len(control_node_ids) == 1, f"Expected one DiscreteControl for node {node_id}, got {len(control_node_ids)}"
    control_node_id = int(control_node_ids.iloc[0])
    assert (links.loc[links.link_type == "control", "from_node_id"] == control_node_id).sum() == 1

    static = model.outlet.static.df
    assert static is not None
    aanvoer = static[(static.node_id == node_id) & (static.control_state == "aanvoer")]
    assert len(aanvoer) == 1, f"Expected one aanvoer row for node {node_id}"
    regime_rows = [
        aanvoer.assign(control_state=state, flow_rate=flow_rate, max_flow_rate=flow_rate)
        for state, flow_rate in flow_rates.items()
    ]
    model.outlet.static.df = pd.concat([static[static.node_id != node_id], *regime_rows], ignore_index=True)

    # the old listen link is removed, and a new one added
    model.link.df = links[~((links.link_type == "listen") & (links.to_node_id == control_node_id))]
    model.link.add(model.get_node(listen_node_id), model.get_node(control_node_id))

    dc = model.discrete_control
    for table in (dc.variable, dc.condition, dc.logic):
        assert table.df is not None
        table.df = table.df[table.df.node_id != control_node_id]  # ty: ignore[invalid-assignment]
    dc.variable.df = DataFrame[DiscreteControlVariableSchema](
        pd.concat(
            [
                dc.variable.df,
                pd.DataFrame(
                    {
                        "node_id": [control_node_id],
                        "compound_variable_id": [1],
                        "listen_node_id": [listen_node_id],
                        "variable": ["flow_rate"],
                        "weight": [1.0],
                    }
                ),
            ],
            ignore_index=True,
        )
    )
    dc.condition.df = DataFrame[DiscreteControlConditionSchema](
        pd.concat(
            [
                dc.condition.df,
                pd.DataFrame(
                    {
                        "node_id": control_node_id,
                        "compound_variable_id": 1,
                        "condition_id": range(1, len(thresholds) + 1),
                        "threshold_high": thresholds,
                        "threshold_low": [threshold - FLOW_THRESHOLD_HYSTERESIS for threshold in thresholds],
                        "time": model.starttime,
                    }
                ),
            ],
            ignore_index=True,
        )
    )
    # with ascending thresholds, the truth states are a run of T followed by F
    states = list(flow_rates)
    truth_states = ["T" * i + "F" * (len(thresholds) - i) for i in range(len(states))]
    dc.logic.df = DataFrame[DiscreteControlLogicSchema](
        pd.concat(
            [
                dc.logic.df,
                pd.DataFrame({"node_id": control_node_id, "truth_state": truth_states, "control_state": states}),
            ],
            ignore_index=True,
        )
    )
    model.node.df.loc[control_node_id, "name"] = name


set_flow_regime_control(
    model,
    node_id=WIJK_BIJ_DUURSTEDE,
    listen_node_id=AMELISWEERD,
    thresholds=AMELISWEERD_THRESHOLDS,
    flow_rates=WIJK_BIJ_DUURSTEDE_FLOW_RATES,
    name="debiet Amelisweerd: 6/7 [m3/s]",
)

# %% Gemaal Kromme Rijn, naast de inlaat bij Wijk bij Duurstede, zie #978
# Het gemaal vervangt de noodpompen in droge perioden: het pompt als Amelisweerd in regime 1 zit, en de Lek te laag
# staat voor de inlaat, die niet onder zijn min_upstream_level inlaat. De bovengrens op de Lek is dat level, dus bij het
# peil van de LevelBoundary in het regionale model blijft het gemaal uit.
GEMAAL_KROMME_RIJN_FLOW_RATE = 9.0  # m3/s
LEK_THRESHOLD_RANGE = 0.04  # m

lek, kromme_rijn = model.upstream_node_id(WIJK_BIJ_DUURSTEDE), model.downstream_node_id(WIJK_BIJ_DUURSTEDE)
assert isinstance(lek, int | np.integer) and isinstance(kromme_rijn, int | np.integer)
assert model.get_node_type(int(lek)) == "LevelBoundary", f"Expected the Lek upstream of {WIJK_BIJ_DUURSTEDE}"
inlaat = model.outlet.static.df[model.outlet.static.df.node_id == WIJK_BIJ_DUURSTEDE]
assert inlaat[["min_upstream_level", "max_downstream_level"]].nunique().eq(1).all(), "Expected one level per side"
lek_threshold = float(inlaat["min_upstream_level"].iloc[0])
inlaat_point = cast(Point, model.node.df.at[WIJK_BIJ_DUURSTEDE, "geometry"])

gemaal_node = model.pump.add(
    Node(geometry=Point(inlaat_point.x + 20, inlaat_point.y - 20), name="Gemaal Kromme Rijn"),
    [
        pump.Static(
            control_state=["gemaal aan", "gemaal uit"],
            flow_rate=[GEMAAL_KROMME_RIJN_FLOW_RATE, 0.0],
            max_flow_rate=[GEMAAL_KROMME_RIJN_FLOW_RATE] * 2,
            max_downstream_level=[float(inlaat["max_downstream_level"].iloc[0])] * 2,
        )
    ],
)
model.link.add(model.get_node(int(lek)), gemaal_node)
model.link.add(gemaal_node, model.get_node(int(kromme_rijn)))

gemaal_control = model.discrete_control.add(
    Node(
        geometry=Point(inlaat_point.x + 40, inlaat_point.y - 40),
        name=f"debiet Amelisweerd: {AMELISWEERD_THRESHOLDS[0]:.0f} [m3/s] en Lek: {lek_threshold} [m+NAP]",
    ),
    [
        discrete_control.Variable(
            compound_variable_id=[1, 2], listen_node_id=[AMELISWEERD, int(lek)], variable=["flow_rate", "level"]
        ),
        discrete_control.Condition(
            compound_variable_id=[1, 2],
            condition_id=[1, 1],
            threshold_high=[AMELISWEERD_THRESHOLDS[0], lek_threshold],
            threshold_low=[AMELISWEERD_THRESHOLDS[0] - FLOW_THRESHOLD_HYSTERESIS, lek_threshold - LEK_THRESHOLD_RANGE],
            time=[model.starttime] * 2,
        ),
        discrete_control.Logic(
            truth_state=["FF", "FT", "TF", "TT"],
            control_state=["gemaal aan", "gemaal uit", "gemaal uit", "gemaal uit"],
        ),
    ],
)
model.link.add(gemaal_control, gemaal_node)

# %% Namen, zie #855
model.node.df.loc[n("I1580"), "name"] = "Oog in Al, spuisluis"
model.node.df.loc[n("SL0015"), "name"] = "Oog in Al, Muntsluis"

# Model run


assert model.discrete_control.condition.df is not None
model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# %%

# write the dry and wet steady state scenarios and the full control model
model = write_steady_state_scenarios(
    model,
    AUTHORITY,
    SHORT_NAME,
    dry_evaporation_mm_per_day=1,
    model_exec=MODEL_EXEC,
    qlr_path=qlr_path,
)

# %%

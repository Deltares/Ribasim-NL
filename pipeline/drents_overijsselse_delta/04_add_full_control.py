# %%
import geopandas as gpd
from ribasim import Node
from ribasim.nodes import level_boundary, pump
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
    print_node_list_diff,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.run_model import write_steady_state_scenarios
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


cloud = CloudStorage()

ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "aanvoergebieden.gpkg")


# %%
# Read data

model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {n("sluis.3321"), n("sluis.3328"), n("sluis.3329"), n("sluis.1644"), n("sluis.2285")}


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


def add_supply_area_control(
    model: Model,
    aanvoergebieden_df: gpd.GeoDataFrame,
    area_name: str,
    ignore_intersecting_links: list[int],
    supply_nodes: list[int],
    drain_nodes: list[int],
    flow_control_nodes: list[int],
    flow_rate_aanvoer: float | dict[int, float] | None = None,
    max_flow_rate_aanvoer: float | dict[int, float] | None = None,
    flow_rate_afvoer: float | dict[int, float] | None = None,
    max_flow_rate_afvoer: float | dict[int, float] | None = None,
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


# %%
# Definieren paden en syncen met cloud

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# Extra aanvoerpomp bij basin up:duikersifonhevel.24641, met boundary op dezelfde plek/level als boundary bnd:72.
assert model.level_boundary.static.df is not None
boundary_level = model.level_boundary.static.df.loc[
    model.level_boundary.static.df.node_id == n("bnd:72"), "level"
].dropna()
dod_extra_boundary = model.level_boundary.add(
    Node(
        geometry=model.level_boundary[n("bnd:72")].geometry,
        name="extra boundary bij 72",
        meta_couple_authority="Vechtstromen",
    ),
    tables=[level_boundary.Static(level=[float(boundary_level.iloc[0]) if not boundary_level.empty else 0.0])],
)
dod_extra_supply_pump = model.pump.add(
    Node(geometry=Point(240732.7023, 541529.6371), name="extra aanvoerpomp bij 3225"),
    tables=[pump.Static(flow_rate=[1.2], max_flow_rate=[1.2], min_upstream_level=[14.86])],
)
model.link.add(dod_extra_boundary, dod_extra_supply_pump)
model.link.add(dod_extra_supply_pump, model.get_node(n("up:duikersifonhevel.24641")))

# %%
# Linkrichting fixes
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
reverse_link_ids = [
    lk("duikersifonhevel.285046", "up:duikersifonhevel.285061"), lk("duikersifonhevel.276424", "up:duikersifonhevel.276493"), lk("duikersifonhevel.276450", "up:duikersifonhevel.276493"), lk("duikersifonhevel.778260", "up:duikersifonhevel.283277"), lk("duikersifonhevel.425643", "up:gemaal.11499"), lk("gemaal.11489", "up:duikersifonhevel.283836"), lk("stuw.43402", "up:duikersifonhevel.282439"), lk("duikersifonhevel.285308", "up:duikersifonhevel.282439"), lk("gemaal.11496", "up:ow:hydroobject.367639"), lk("duikersifonhevel.17184", "up:duikersifonhevel.17078"), lk("stuw.2350", "up:duikersifonhevel.16109"), lk("up:duikersifonhevel.23536", "stuw.2095"), lk("duikersifonhevel.24685", "up:duikersifonhevel.25195"), lk("stuw.4307", "up:hydroobject.10168___6"), lk("duikersifonhevel.24642", "up:duikersifonhevel.24641"), lk("gemaal.65", "up:gemaal.94"),
    lk("stuw.2066", "down:bnd:98"), lk("gemaal.11499", "up:sluis.3335"), lk("up:duikersifonhevel.277495", "gemaal.11524"), lk("gemaal.123", "up:duikersifonhevel.12017"), lk("gemaal.141", "up:stuw.4460"), lk("duikersifonhevel.17221", "up:duikersifonhevel.24754"), lk("ow:hydroobject.10384", "up:sluis.1966"), lk("duikersifonhevel.286295", "up:duikersifonhevel.278402"), lk("duikersifonhevel.278248", "up:stuw.43005"), lk("duikersifonhevel.11883", "up:gemaal.38"), lk("duikersifonhevel.561264", "up:duikersifonhevel.276269"), lk("ow:hydroobject.211426", "up:duikersifonhevel.285494"), lk("up:stuw.2095", "duikersifonhevel.20543"), lk("duikersifonhevel.25036", "up:duikersifonhevel.167820"),
    lk("duikersifonhevel.167820", "up:stuw.2066"), lk("up:sluis.3335", "duikersifonhevel.425643"), lk("up:duikersifonhevel.402808", "ow:hydroobject.211426"), lk("up:gemaal.11489", "gemaal.11489"), lk("up:gemaal.11496", "gemaal.11496"), lk("up:duikersifonhevel.283277", "stuw.43402"), lk("up:duikersifonhevel.283277", "gemaal.19590"), lk("up:duikersifonhevel.24685", "duikersifonhevel.24685"), lk("up:duikersifonhevel.15715", "stuw.2350"), lk("up:duikersifonhevel.25036", "duikersifonhevel.25036"), lk("stuw.2095", "up:stuw.2095"), lk("up:duikersifonhevel.27203", "stuw.4307"), lk("up:gemaal.141", "gemaal.141"),
    lk("up:stuw.2066", "stuw.2066"), lk("up:duikersifonhevel.17184", "duikersifonhevel.17184"), lk("up:duikersifonhevel.25321", "gemaal.123"), lk("up:gemaal.11462", "gemaal.11469"), lk("up:duikersifonhevel.20543", "gemaal.65"), lk("duikersifonhevel.20543", "up:duikersifonhevel.20543"), lk("up:duikersifonhevel.286295", "duikersifonhevel.286295"), lk("up:gemaal.11499", "gemaal.11499"), lk("up:gemaal.38", "gemaal.38"), lk("up:duikersifonhevel.17221", "duikersifonhevel.17221"), lk("up:duikersifonhevel.278248", "duikersifonhevel.278248"), lk("up:duikersifonhevel.276424", "duikersifonhevel.276424"), lk("up:duikersifonhevel.285308", "duikersifonhevel.285308"),
    lk("up:duikersifonhevel.285046", "duikersifonhevel.285046"), lk("up:duikersifonhevel.276904", "duikersifonhevel.561264"), lk("up:duikersifonhevel.276450", "duikersifonhevel.276450"), lk("up:duikersifonhevel.24642", "duikersifonhevel.24642"), lk("up:duikersifonhevel.25618", "ow:hydroobject.10384"), lk("up:duikersifonhevel.24636", "duikersifonhevel.24636"), lk("up:duikersifonhevel.167820", "duikersifonhevel.167820"), lk("sluis.1973", "bnd:28"), lk("bnd:39", "duikersifonhevel.11883"), lk("gemaal.38", "bnd:41"), lk("loc:204572,507737", "duikersifonhevel.778260"), lk("gemaal.19590", "loc:204572,507737"), lk("gemaal.11469", "bnd:63"),
    lk("gemaal.11524", "bnd:64"), lk("duikersifonhevel.24636", "bnd:102"), lk("loc:197542,525242", "sluis.1973"),lk("bnd:25", "loc:231794,557486"),lk("loc:231794,557486", "up:duikersifonhevel.24754"),lk("bnd:62", "loc:236707,526926"),lk("loc:236707,526926", "up:duikersifonhevel.23536"),lk("loc:240741,541524", "loc:240733,541530"),lk("loc:240733,541530", "up:duikersifonhevel.24641")
]
# fmt: on

for link_id in reverse_link_ids:
    model.reverse_link(link_id=link_id)

model.redirect_link(lk("duikersifonhevel.284720", "up:duikersifonhevel.276575"), to_node_id=n("loc:216070,482387"))
model.redirect_link(lk("stuw.43135", "loc:216070,482387"), to_node_id=n("up:duikersifonhevel.276575"))

# stuw.43145 is na de nieuwe fix al georienteerd als 1643 -> 3224 -> 2585.
model.redirect_link(lk("up:stuw.43145", "stuw.43145"), from_node_id=n("loc:206332,496857"), to_node_id=n("stuw.43145"))


# %%
# Node type fixes


# Streefpeil te laag
set_static_values(
    model.outlet.static.df,
    {
        n("duikersifonhevel.18943"): 11.0,
        n("duikersifonhevel.20346"): 11.0,
        n("duikersifonhevel.20174"): 11.0,
        n("sluis.2285"): -0.2,
    },
    "min_upstream_level",
)

for node_id, to_node_id in [
    (n("up:stuw.2085"), n("up:duikersifonhevel.24720")),
    (n("up:ow:hydroobject.201870"), n("up:duikersifonhevel.339112")),
    (n("up:ow:hydroobject.429079"), n("up:duikersifonhevel.284656")),
    (n("up:stuw.43614"), n("up:stuw.43606")),
    (n("up:stuw.2141"), n("up:duikersifonhevel.55770")),
    (n("up:sluis.1964"), n("up:duikersifonhevel.23536")),  # Noordscheschutsluis
]:
    model.merge_basins(node_id=node_id, to_node_id=to_node_id, are_connected=True)

# Manning moet outlet zijn
model.update_node(node_id=n("ow:hydroobject.367639"), node_type="Outlet")
assert model.outlet.static.df is not None
mask = model.outlet.static.df.node_id == n("ow:hydroobject.367639")
model.outlet.static.df.loc[mask, "min_upstream_level"] = 2.63
mark_level_update_protected(model.outlet.static.df, mask, model=model)

# Naamcorrectie: node_id 1344 is Zalk, niet Adsum
model.node.df.loc[n("gemaal.11411@2"), "name"] = "Zalk"


# %%
# Max-capaciteiten pompen en sluizen

outlet_max_flow_rate_by_node_id = {
    n("stuw.50984"): 200.0,  # Vechterweerd
    n("loc:203907,508001"): 200.0,  # Vechterweerd
    n("stuw.112142"): 16.2,  # Paradijssluis
    n("stuw.112141"): 14.9,  # Haveltersluis
    n("stuw.112140"): 14.5,  # Uffeltersluis
    n("stuw.112139"): 12.2,  # Dieversluis
    n("stuw.111818"): 12.4,  # Haarsluis
    n("stuw.111817"): 13.4,  # Veenesluis
    n("sluis.1968"): 999.9,  # Smildersluis
    n("sluis.1964"): 11.0,  # Noordscheschutsluis
    n("sluis.1963"): 29.2,  # Nieuwebrugsluis
    n("stuw.112458"): 29.6,  # Ossesluis
    n("stuw.112457"): 43.3,  # Rogatsluis
    n("duikersifonhevel.24669"): 0.01,  # kleine duiker
}
outlet_max_flow_rate_parameterized_zero_by_node_id = {
    n("duikersifonhevel.20133"): 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    n("duikersifonhevel.1017472"): 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    n("ow:hydroobject.542376"): 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
}
outlet_max_flow_rate_from_results = {
    n("duikersifonhevel.286070"): 2,  # duikersifonhevel.286070
    n("duikersifonhevel.281013"): 2,  # duikersifonhevel.281013
    n("duikersifonhevel.279722"): 2,  # duikersifonhevel.279722
    n("duikersifonhevel.11883"): 2,  # duikersifonhevel.11883
    n("duikersifonhevel.499162"): 2,  # duikersifonhevel.499162
    n("duikersifonhevel.624652"): 2,  # duikersifonhevel.624652
    n("duikersifonhevel.134543"): 2,  # duikersifonhevel.134543
    n("duikersifonhevel.748812"): 2,  # duikersifonhevel.748812
    n("duikersifonhevel.287091"): 2,  # duikersifonhevel.287091
    n("duikersifonhevel.168460"): 2,  # duikersifonhevel.168460
    n("duikersifonhevel.12121"): 2,  # duikersifonhevel.12121
    n("sluis.2281"): 3,  # sluis.2281
    n("Wyberbuurseweg"): 2,  # Wyberbuurseweg
    n("Snelweg"): 2,  # Snelweg
    n("loc:229756,516188"): 2,  # naam onbekend
    n("loc:233438,522718"): 2,  # naam onbekend
    n("loc:222297,503293"): 2,  # naam onbekend
}
outlet_max_flow_rate_coupled_by_node_id = {
    n("stuw.50968"): 170,  # gekoppeld max=112.50, huidige max=83.20, link=5900281
    n("duikersifonhevel.599665"): 5,  # duikersifonhevel.599665; gekoppeld max=2.21, parameterized=0.10
    n("sluis.1656"): 8,  # gekoppeld max=4.73, huidige max=0.00, link=5900891
    n("duikersifonhevel.285812"): 3,  # duikersifonhevel.285812; gekoppeld max=1.74, parameterized=0.10
    n("duikersifonhevel.281304"): 2,  # gekoppeld max=0.77, huidige max=0.10, link=5900486
    n("duikersifonhevel.134864"): 8,  # duikersifonhevel.134864; gekoppeld max=4.90, parameterized=0.10
    n("duikersifonhevel.20131"): 2,  # gekoppeld max=0.66, huidige max=0.00, link=5901233
    n("duikersifonhevel.24598"): 2,  # gekoppeld max=0.66, huidige max=0.00, link=5901239
    n("duikersifonhevel.286046"): 3,  # gekoppeld max=1.27, huidige max=0.10, link=5900197
    n("duikersifonhevel.586543"): 3,  # duikersifonhevel.586543; gekoppeld max=1.88, parameterized=0.10
    n("duikersifonhevel.586544"): 3,  # duikersifonhevel.586544; gekoppeld max=1.88, parameterized=0.10
    n("duikersifonhevel.284492"): 2,  # gekoppeld max=0.54, huidige max=0.10, link=5901353
    n("ow:hydroobject.367639"): 80,  # parameterized nul; gekoppeld max=52.24
    n("sluis.2281"): 5,  # parameterized nul; gekoppeld max=2.75
    n("Westeinde_1"): 3,  # gekoppeld max=1.60, huidige max=0.00, link=5903104
    n("loc:236149,526264"): 2,  # parameterized nul; gekoppeld max=0.15
    n("loc:236707,526926"): 3,  # parameterized nul; gekoppeld max=1.36
    n("loc:222297,503293"): 45,  # parameterized nul; gekoppeld max=29.20, huidige max=20.00, link=5903168
    n("loc:240732,541530"): 2,  # parameterized nul; gekoppeld max=0.77
}
outlet_max_flow_rate_afvoer_by_node_id: dict[int, float] = {}
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
    n("gemaal.21188"): 6.7,  # Paradijssluis
    n("gemaal.21189"): 6.6,  # Haveltersluis
    n("gemaal.21190"): 6.3,  # Uffeltersluis
    n("gemaal.21191"): 6.3,  # Dieversluis
    n("gemaal.21193"): 5.8,  # Haarsluis
    n("gemaal.21192"): 5.9,  # Veenesluis
    n("stuw.112138"): 6.8,  # Noordscheschutsluis
    n("gemaal.9451"): 7.1,  # Nieuwebrugsluis
    n("gemaal.21195"): 7.5,  # Ossesluis
    n("gemaal.21196"): 8.2,  # Rogatsluis
    n("gemaal.11494"): 1.0,  # Emsland 1
    n("gemaal.11493"): 0.16,  # Emsland 2
    n("gemaal.11495"): 1.06,  # Langewijk
    n("gemaal.11496"): 1.0,  # De Stouwe
    n("gemaal.11424"): 0.58,  # 't Raasje -> Raasje
    n("gemaal.11430"): 0.24,  # Regterveld -> Rechterveld
    n("gemaal.11443"): 0.12,  # Willem Meijerpolder
    n("gemaal.11434"): 0.21,  # Stikkenpolder
    n("gemaal.11445"): 0.06,  # Zwartemeerpolder
    n("gemaal.11550"): 0.83,  # Mandjeswaard
    n("gemaal.11404"): 0.32,  # de Pieper
    n("gemaal.11442"): 0.62,  # Zuiderzeepolder
    n("gemaal.11440"): 5.50,  # Veneriete
    n("gemaal.11413"): 0.35,  # Biesvelden
    n("gemaal.11398"): 0.27,  # Groot - Cellemuiden -> Groot Cellemuiden
    n("gemaal.11433"): 0.13,  # Stikkeldrecht
    n("gemaal.11399"): 0.83,  # Cellemuiden
    n("gemaal.11548"): 4.83,  # Nieuw Lutterzijl I -> Lutterzijl Mastenbroek
    n("gemaal.11552"): 3.75,  # Nieuw Lutterzijl II: Lutterzijl Koekoek
    n("Gemaal 't Gansje"): 2.23,  # 't Gansje; Node.name is leeg, gevonden via meta_code_waterbeheerder
    n("gemaal.11551"): 0.11,  # Noorderwaard
    n("gemaal.11416"): 1.08,  # Benoorden de Willemsvaart -> Ben de Willemsvaart
    n("gemaal.11421"): 0.08,  # 't Katje -> Katje
    n("gemaal.11438"): 0.58,  # 't Vosje -> Vosje
    n("gemaal.11444"): 0.11,  # Zalkerbos -> Zalkerbos en Erfgenamen
    n("gemaal.11411@1"): 1.83,  # Adsum; node_id 1344 heeft ook Node.name = Adsum, zie controlelijst
    n("gemaal.20228"): 3.00,  # Kamperveen
    n("gemaal.11428"): 3.00,  # Roggebot
    n("gemaal.11540"): 0.03,  # Kardoezenpolder -> Kardoezen
    n("gemaal.11541"): 0.09,  # Ketelpolder
    n("gemaal.19590"): 0.30,  # Inlaat Broekhuizen -> De Broekhuizen
    n("stuw.43459"): 1.00,  # De Stouwe
    n("gemaal.11491"): 0.16,  # De Doorn -> de Doorn
    n("gemaal.11488"): 5.75,  # Galgerak -> Galgenrak
    n("Galgenrak (KIN)"): 5.75,  # Galgerak -> Galgenrak
    n("gemaal.11492"): 20.0,  # Streukelerzijl
    n("gemaal.11483"): 5.0,  # Kloosterzijl
    n("gemaal.11406"): 11.17,  # Kostverlorenzijl
    n("gemaal.11403"): 2.50,  # Spoortipp -> Spoortippe
    n("gemaal.11450"): 1.16,  # Westerveld
    n("gemaal.11499"): 3.66,  # Herfte
    n("gemaal.11461"): 0.66,  # Sekdoorn
    n("gemaal.11464"): 2.33,  # Lierder&Molenbroek -> Lierder en Molenbroek
    n("gemaal.11466"): 0.90,  # Bremmelerstraat
    n("gemaal.11452"): 0.01,  # Linterzijl
    n("gemaal.11453"): 4.00,  # Langeslag (gemaal) -> Langeslag
    n("gemaal.11506"): 0.40,  # Vechterweerd
    n("stuw.47752"): 0.33,  # Marswetering -> Voorbezinkplas Marswetering
    n("gemaal.11472"): 8.5,  # Ankersmit
    n("gemaal.11472_aanvoer"): 8.5,  # Ankersmit
    n("gemaal.101"): 1.17,  # Blijdenstein
    n("gemaal.100"): 0.97,  # Trijzen
    n("gemaal.19"): 53.3,  # Afvoergemaal Stroink
    n("gemaal.76"): 3.0,  # Holthe
    n("gemaal.91"): 1.5,  # Smildersluis
    n("gemaal.92"): 1.3,  # Zwiggeltersluis
    n("gemaal.145"): 127.5,  # Zedemuden
    # Todo: Capaciteiten nog navragen
    # 545: ...,   # Sultansmeer
    # 693: ...,   # Weerwille
    # 633: ...,   # Oldenhave
}
pump_max_flow_rate_parameterized_zero_by_node_id = {
    n("Stroink (KIN)"): 20.0,  # xlsx max_flow_rate nul; voorkom aanvoerblokkade
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
    n("stuw.2031"), n("stuw.2141"), n("stuw.42900"), n("stuw.42922"), n("stuw.42992"), n("stuw.43008"), n("stuw.43016"), n("stuw.43017"), n("stuw.43019"), n("stuw.43638"), n("stuw.43774"), n("stuw.50968"), n("stuw.50984"), n("stuw.43555"), n("stuw.43560"), n("stuw.43169"),
    n("stuw.43173"), n("stuw.43174"), n("stuw.43181"), n("stuw.43200"), n("stuw.43201"), n("stuw.43233"), n("stuw.43185"), n("stuw.1772"), n("stuw.558"), n("stuw.618"), n("stuw.1922"), n("stuw.1774"), n("stuw.1836"), n("stuw.2153"), n("stuw.1748"), n("stuw.565"), n("stuw.594"),
    n("stuw.1830"), n("stuw.1738"), n("stuw.1735"), n("stuw.97401"), n("stuw.557"), n("stuw.16980"), n("stuw.614"), n("stuw.1850"), n("stuw.697"), n("stuw.16978"), n("stuw.1753"), n("stuw.2029"), n("duikersifonhevel.25520"), n("gemaal.11495"), n("gemaal.11506"), n("gemaal.11516"), n("gemaal.153"), n("stuw.115979"), n("duikersifonhevel.499162"),
    n("duikersifonhevel.561264"), n("stuw.42913"), n("stuw.42958"), n("duikersifonhevel.25618"), n("gemaal.139"), n("stuw.1705"), n("stuw.43206"), n("duikersifonhevel.25043"), n("duikersifonhevel.628820"), n("gemaal.4"), n("stuw.2161"),
    n("loc:233438,522718"), n("loc:222297,503293")
]

supply_nodes = [
    n("stuw.43016"), n("stuw.43764"), n("stuw.47752"), n("stuw.43518"), n("stuw.43411"), n("stuw.43476"), n("stuw.43477"), n("stuw.43483"), n("stuw.43527"), n("stuw.43620"), n("stuw.43783"), n("stuw.43621"), n("stuw.43260"), n("stuw.43348"), n("stuw.2350"),
    n("stuw.118857"), n("stuw.112138"), n("stuw.111817"),n("stuw.2053"), n("stuw.101242"), n("duikersifonhevel.778260"), n("duikersifonhevel.286070"), n("duikersifonhevel.723852"), n("duikersifonhevel.25520"), n("sluis.3327"), n("gemaal.99"), n("gemaal.65"), n("gemaal.11395"), n("gemaal.11421"), n("gemaal.11424"), n("gemaal.11429"),
    n("stuw.43368"), n("gemaal.11438"), n("gemaal.11450"), n("gemaal.11489"), n("gemaal.11499"), n("gemaal.11511"), n("gemaal.11530"), n("gemaal.11533"), n("gemaal.21192"), n("gemaal.108"), n("gemaal.141"), n("gemaal.92"), n("gemaal.110"), n("gemaal.101"), n("gemaal.21189"), n("gemaal.21196"),
    n("gemaal.76"), n("gemaal.122"), n("gemaal.91"), n("gemaal.100"), n("gemaal.97"),n("gemaal.21193"), n("gemaal.21191"), n("gemaal.9451"), n("gemaal.98"), n("gemaal.21195"), n("gemaal.94"), n("gemaal.21188"), n("duikersifonhevel.746906"), n("duikersifonhevel.748491"), n("duikersifonhevel.134226"), n("duikersifonhevel.24652"), n("duikersifonhevel.26039"),
    n("duikersifonhevel.24754"), n("duikersifonhevel.285090"), n("duikersifonhevel.284682"), n("duikersifonhevel.283836"), n("duikersifonhevel.280497"), n("duikersifonhevel.280504"), n("duikersifonhevel.280076"), n("duikersifonhevel.278248"), n("duikersifonhevel.278290"), n("duikersifonhevel.276728"), n("duikersifonhevel.276424"), n("duikersifonhevel.276426"), n("duikersifonhevel.276493"), n("duikersifonhevel.284658"), n("duikersifonhevel.284669"), n("duikersifonhevel.465930"),
    n("duikersifonhevel.482106"), n("duikersifonhevel.411752"), n("duikersifonhevel.425596"), n("duikersifonhevel.285046"), n("duikersifonhevel.286007"), n("duikersifonhevel.286035"), n("duikersifonhevel.280532"), n("duikersifonhevel.286281"), n("duikersifonhevel.276450"), n("duikersifonhevel.276622"), n("duikersifonhevel.276338"), n("duikersifonhevel.282385"), n("duikersifonhevel.284656"), n("duikersifonhevel.284668"), n("duikersifonhevel.480266"), n("duikersifonhevel.464602"),
    n("duikersifonhevel.24623"), n("duikersifonhevel.24651"), n("duikersifonhevel.12017"), n("duikersifonhevel.23536"), n("duikersifonhevel.25229"), n("duikersifonhevel.134542"), n("duikersifonhevel.783378"), n("duikersifonhevel.25453"), n("duikersifonhevel.17221"), n("duikersifonhevel.624652"), n("duikersifonhevel.11327"), n("duikersifonhevel.78490"), n("duikersifonhevel.612491"), n("duikersifonhevel.22626"), n("duikersifonhevel.25050"), n("duikersifonhevel.24646"),
    n("duikersifonhevel.784030"), n("duikersifonhevel.11958"), n("duikersifonhevel.24682"), n("duikersifonhevel.24669"), n("duikersifonhevel.26969"), n("duikersifonhevel.15543"), n("duikersifonhevel.583342"), n("duikersifonhevel.758412"), n("duikersifonhevel.24687"), n("duikersifonhevel.25663"), n("duikersifonhevel.750731"), n("duikersifonhevel.25037"), n("duikersifonhevel.78810"), n("duikersifonhevel.12412"), n("duikersifonhevel.25076"), n("duikersifonhevel.16109"),
    n("duikersifonhevel.20131"), n("duikersifonhevel.24598"), n("duikersifonhevel.26057"), n("duikersifonhevel.13178"), n("gemaal.19590"), n("stuw.43711"), n("duikersifonhevel.714253"), n("duikersifonhevel.740814"), n("duikersifonhevel.15715"), n("duikersifonhevel.11344"), n("gemaal.11520"), n("gemaal.11478"), n("gemaal.11472_aanvoer"), n("duikersifonhevel.286284"), n("gemaal.11477"),
    n("duikersifonhevel.777932"), n("duikersifonhevel.777931"), n("duikersifonhevel.280483"), n("duikersifonhevel.283277"), n("duikersifonhevel.284570"), n("stuw.43398"), n("stuw.43429"), n("duikersifonhevel.24685"), n("duikersifonhevel.25195"), n("duikersifonhevel.77530"), n("duikersifonhevel.435130"), n("duikersifonhevel.20174"), n("duikersifonhevel.25225"), n("duikersifonhevel.25053"), n("duikersifonhevel.20051"), n("duikersifonhevel.710427"),
    n("duikersifonhevel.22973"), n("duikersifonhevel.24690"), n("duikersifonhevel.25599"), n("duikersifonhevel.17092"), n("duikersifonhevel.25321"), n("KOR-03827"), n("duikersifonhevel.402808"), n("duikersifonhevel.24636"), n("duikersifonhevel.26990"), n("duikersifonhevel.595189"), n("duikersifonhevel.16081"), n("duikersifonhevel.17131"), n("duikersifonhevel.24772"), n("duikersifonhevel.18971"), n("duikersifonhevel.584303"), n("duikersifonhevel.277495"),
    n("duikersifonhevel.275113"), n("duikersifonhevel.213118"), n("gemaal.11411@2"), n("stuw.43416"), n("ow:hydroobject.497881"), n("ow:hydroobject.203309"), n("ow:hydroobject.10384"), n("ow:hydroobject.211426"), n("ow:hydroobject.236366"),
    n("loc:206536,521945"), n("sluis.2281"), n("duikersifonhevel.25276"), n("Galgenrak (KIN)"), n("Westeinde_1"),
    n("loc:227572,521656"), n("stuw.43145@2"), n("loc:231794,557486"), n("loc:236707,526926"),n("loc:240733,541530")
]

drain_nodes = [
    n("stuw.1769"), n("stuw.606"), n("stuw.2098"), n("stuw.1991"), n("stuw.1859"), n("stuw.42949"), n("stuw.42973"), n("stuw.42989"), n("stuw.42991"), n("stuw.43010"), n("stuw.43016"), n("stuw.43044"), n("stuw.43053"), n("stuw.43058"), n("stuw.43071"), n("stuw.43097"),
    n("stuw.43107"), n("stuw.43108"), n("stuw.43113"), n("stuw.43638"), n("stuw.43645"), n("stuw.43669"), n("stuw.43679"), n("stuw.43685"), n("stuw.43700"), n("stuw.43735"), n("stuw.49368"), n("stuw.52264"), n("stuw.42899"), n("stuw.42990"), n("stuw.43402"), n("stuw.43419"), n("stuw.43459"),
    n("stuw.43460"), n("stuw.43522"), n("stuw.43540"), n("stuw.43606"), n("stuw.43615"), n("stuw.43145"), n("stuw.43167"), n("stuw.43197"), n("stuw.43205"), n("stuw.43212"), n("stuw.43257"), n("stuw.43258"), n("stuw.43263"), n("stuw.43357"), n("stuw.58008"), n("stuw.64728"),
    n("stuw.80728"), n("stuw.2170"), n("stuw.1854"), n("stuw.1927"), n("stuw.1821"), n("stuw.1813"), n("stuw.2280"), n("stuw.2158"), n("stuw.2107"), n("stuw.508"), n("stuw.2159"), n("stuw.1914"), n("stuw.91321"), n("stuw.378"), n("stuw.1817"), n("stuw.2154"),
    n("stuw.1788"), n("stuw.93883"), n("stuw.1934"), n("stuw.1749"), n("stuw.1929"), n("stuw.105097"), n("stuw.691"), n("stuw.1913"), n("stuw.533"), n("stuw.1736"), n("stuw.118858"), n("stuw.376"), n("stuw.2142"), n("stuw.4419"), n("stuw.112458"), n("stuw.565"),
    n("stuw.368"), n("stuw.35256"), n("stuw.112457"), n("stuw.491"), n("stuw.4327"), n("stuw.1902"), n("stuw.112141"), n("stuw.1682"), n("stuw.93882"), n("stuw.2270"), n("stuw.4339"), n("stuw.1702"), n("stuw.1904"), n("stuw.93881"), n("stuw.557"), n("stuw.1808"), n("stuw.2040"),
    n("stuw.1875"), n("stuw.21778"), n("stuw.4236"), n("stuw.607"), n("stuw.2305"), n("stuw.112139"), n("stuw.111818"), n("stuw.1694"), n("stuw.4378"), n("stuw.112142"), n("stuw.93884"), n("stuw.88744"), n("stuw.698"), n("stuw.2094"), n("stuw.1703"), n("stuw.534"),
    n("stuw.2143"), n("stuw.4424"), n("stuw.1924"), n("stuw.1798"), n("stuw.2273"), n("stuw.2066"), n("stuw.2099"), n("stuw.104474"), n("stuw.112140"), n("stuw.2111"), n("duikersifonhevel.599665"), n("duikersifonhevel.778260"), n("duikersifonhevel.16103"), n("duikersifonhevel.22221"), n("duikersifonhevel.13119"), n("sluis.1653"),
    n("sluis.3326"), n("sluis.3330"), n("sluis.1964"), n("sluis.1962"), n("gemaal.11406"), n("gemaal.11430"), n("gemaal.11452"), n("gemaal.11454"), n("gemaal.11458"), n("gemaal.11461"), n("gemaal.11464"), n("gemaal.11465"), n("gemaal.11466"), n("gemaal.11472"), n("gemaal.11474"), n("gemaal.11488"),
    n("gemaal.11492"), n("gemaal.11513"), n("gemaal.11514"), n("gemaal.11529"), n("gemaal.11531"), n("gemaal.11552"), n("gemaal.11553"), n("gemaal.13170"), n("gemaal.43"), n("gemaal.3"), n("gemaal.8"), n("gemaal.49"), n("gemaal.64"), n("gemaal.62"), n("gemaal.70"), n("gemaal.63"),
    n("gemaal.67"), n("gemaal.23"), n("gemaal.50"), n("gemaal.39"), n("gemaal.59"), n("gemaal.69"), n("gemaal.36"), n("gemaal.6"), n("gemaal.37"), n("gemaal.30"), n("gemaal.32"), n("gemaal.7"), n("gemaal.79"), n("gemaal.33"), n("gemaal.34"), n("gemaal.1"),
    n("gemaal.40"), n("gemaal.2"), n("gemaal.20"), n("duikersifonhevel.27337"), n("duikersifonhevel.24798"), n("duikersifonhevel.20031"), n("duikersifonhevel.21857"), n("duikersifonhevel.286048"), n("duikersifonhevel.285812"), n("duikersifonhevel.278402"), n("duikersifonhevel.278248"), n("duikersifonhevel.277717"), n("duikersifonhevel.276673"), n("duikersifonhevel.276224"), n("duikersifonhevel.294712"), n("duikersifonhevel.480730"),
    n("duikersifonhevel.482106"), n("duikersifonhevel.285367"), n("duikersifonhevel.276851"), n("duikersifonhevel.277189"), n("duikersifonhevel.276904"), n("duikersifonhevel.278403"), n("duikersifonhevel.276269"), n("duikersifonhevel.278284"), n("duikersifonhevel.284149"), n("duikersifonhevel.283210"), n("duikersifonhevel.425643"), n("duikersifonhevel.24234"), n("duikersifonhevel.20075"), n("duikersifonhevel.11883"), n("duikersifonhevel.19391"),
    n("duikersifonhevel.26034"), n("duikersifonhevel.13316"), n("duikersifonhevel.24921"),n("duikersifonhevel.776980"), n("duikersifonhevel.20146"), n("duikersifonhevel.756835"), n("duikersifonhevel.10800"), n("duikersifonhevel.20346"), n("duikersifonhevel.761611"), n("duikersifonhevel.25050"), n("duikersifonhevel.26651"), n("duikersifonhevel.20281"), n("duikersifonhevel.22706"), n("duikersifonhevel.20805"), n("duikersifonhevel.21789"), n("duikersifonhevel.24674"), n("duikersifonhevel.18955"), n("duikersifonhevel.19012"),n("duikersifonhevel.26836"),
    n("duikersifonhevel.20410"), n("duikersifonhevel.79450"), n("duikersifonhevel.23861"), n("duikersifonhevel.18988"), n("duikersifonhevel.134864"), n("duikersifonhevel.129740"), n("sluis.1968"), n("gemaal.11399"), n("gemaal.11428"), n("gemaal.21"), n("gemaal.20228"), n("gemaal.5"), n("stuw.43781"), n("gemaal.15410"), n("stuw.4460"), n("duikersifonhevel.13045"), n("stuw.2109"),
    n("stuw.2130"), n("stuw.2004"), n("stuw.2030"), n("stuw.2038"), n("gemaal.11473"), n("stuw.1884"), n("stuw.43529"), n("stuw.43473"), n("gemaal.11471"), n("gemaal.11448"), n("duikersifonhevel.424568"), n("duikersifonhevel.1000048"), n("gemaal.11518"), n("stuw.16982"), n("sluis.1961"), n("stuw.67288"),
    n("stuw.67304"), n("stuw.43517"), n("stuw.43361"), n("stuw.33621"), n("stuw.1732"), n("gemaal.52"), n("stuw.43076"), n("gemaal.11502"), n("stuw.4262"), n("stuw.29461"), n("stuw.2323"), n("duikersifonhevel.25321"), n("duikersifonhevel.24720"), n("duikersifonhevel.167820"), n("duikersifonhevel.284481"), n("duikersifonhevel.24633"),
    n("gemaal.119"), n("gemaal.8491"), n("meppelersluis"), n("stuw.2160"), n("stuw.530"), n("stuw.548"), n("stuw.564"), n("stuw.568"), n("stuw.32024"), n("stuw.4391"), n("stuw.473"), n("stuw.119177"), n("stuw_noksloot"), n("stuw.1746"), n("stuw.1988"), n("stuw.88424"),
    n("ow:hydroobject.542376"), n("ow:hydroobject.13008"), n("ow:hydroobject.7406"), n("ow:hydroobject.10133"), n("ow:hydroobject.9069"), n("ow:hydroobject.200446"), n("ow:hydroobject.601606"), n("ow:hydroobject.203937"), n("ow:hydroobject.201586"), n("ow:hydroobject.214582"), n("ow:hydroobject.353849"),n("ow:hydroobject.11593"), n("gemaal.145"), n("gemaal.27"),
    n("gemaal.26"), n("gemaal.4330"), n("loc:205276,484889"),n("loc:240732,541530")
]
# fmt: on

supply_nodes.append(dod_extra_supply_pump.node_id)


# %%
# Kunstwerk-in/-uit ruimtelijk koppelen aan Ribasim-nodes

kunstwerk_in_path = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "kunstwerk_in.gpkg")
kunstwerk_uit_path = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "kunstwerk_uit.gpkg")


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


def print_manual_role_conflicts(
    kunstwerk_supply_nodes: list[int],
    kunstwerk_drain_nodes: list[int],
    manual_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> None:
    kunstwerk_supply_node_set = set(kunstwerk_supply_nodes)
    kunstwerk_drain_node_set = set(kunstwerk_drain_nodes)
    print(
        "kunstwerk_in overruled door handmatig: "
        f"flow_control={sorted(kunstwerk_supply_node_set & set(manual_flow_control_nodes))}, "
        f"drain={sorted(kunstwerk_supply_node_set & set(manual_drain_nodes))}"
    )
    print(
        "kunstwerk_uit overruled door handmatig: "
        f"flow_control={sorted(kunstwerk_drain_node_set & set(manual_flow_control_nodes))}, "
        f"supply={sorted(kunstwerk_drain_node_set & set(manual_supply_nodes))}"
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
    "Ankersmit": [
        lk("ow:hydroobject.213337", "up:gemaal.11472"),
        lk("ow:hydroobject.214102", "up:duikersifonhevel.464602"),
        lk("ow:hydroobject.202351", "up:ow:hydroobject.213739"),
        lk("up:duikersifonhevel.287090", "ow:hydroobject.214102"),
        lk("up:duikersifonhevel.285803", "ow:hydroobject.236366"),
        lk("up:duikersifonhevel.285803", "ow:hydroobject.213337"),
        lk("up:duikersifonhevel.284644", "ow:hydroobject.203309"),
        lk("up:ow:hydroobject.465561", "ow:hydroobject.465561"),
        lk("up:ow:hydroobject.201493", "ow:hydroobject.201493"),
    ],
    "Westerveld": [
        lk("ow:hydroobject.201493", "up:duikersifonhevel.276338"),
        lk("ow:hydroobject.390680", "up:duikersifonhevel.276338"),
        lk("bnd:58", "loc:206536,521945"),
        lk("loc:206536,521945", "bnd:59"),
    ],
    "Overijssels Kanaal": [
        lk("ow:hydroobject.211407", "up:duikersifonhevel.276622"),
        lk("ow:hydroobject.211426", "up:duikersifonhevel.402808"),
        lk("up:duikersifonhevel.284666", "ow:hydroobject.211407"),
        lk("up:KOR-03827", "ow:hydroobject.200957"),
    ],
    "Vecht-Twentekanalen": [
        lk("bnd:58", "loc:206536,521945"),
        lk("loc:206536,521945", "bnd:59"),
    ],
    "Dedemsvaart": [
        lk("duikersifonhevel.278403", "up:duikersifonhevel.278947"),
        lk("up:duikersifonhevel.278403", "duikersifonhevel.278403"),
        lk("up:ow:hydroobject.203632", "ow:hydroobject.203632"),
        lk("ow:hydroobject.209113", "up:duikersifonhevel.278407"),
        lk("ow:hydroobject.212111", "up:stuw.43481"),
        lk("up:duikersifonhevel.284680", "ow:hydroobject.212111"),
        lk("up:duikersifonhevel.281304", "ow:hydroobject.209113"),
        lk("bnd:58", "loc:206536,521945"),
        lk("loc:206536,521945", "bnd:59"),
    ],
    "IJsselmeergebied": [
        lk("ow:hydroobject.209113", "up:duikersifonhevel.278407"),
        lk("ow:hydroobject.212111", "up:stuw.43481"),
        lk("up:duikersifonhevel.284680", "ow:hydroobject.212111"),
        lk("up:duikersifonhevel.281304", "ow:hydroobject.209113"),
    ],
    "Hoogeveense Vaart": [
        lk("hydroobject.10168___6", "up:duikersifonhevel.24682"),
        lk("ow:hydroobject.10157", "up:duikersifonhevel.23536"),
        lk("ow:hydroobject.11593", "up:duikersifonhevel.27017"),
        lk("ow:hydroobject.12723", "up:hydroobject.10168___6"),
        lk("up:duikersifonhevel.18943", "ow:hydroobject.10133"),
        lk("up:duikersifonhevel.24682", "ow:hydroobject.10157"),
        lk("up:duikersifonhevel.100602", "ow:hydroobject.11593"),
        lk("up:ow:hydroobject.12723", "ow:hydroobject.12723"),
        lk("up:hydroobject.10168___6", "hydroobject.10168___6"),
        lk("ow:hydroobject.372441_3_2", "bnd:59"),
        lk("bnd:58", "loc:206536,521945"),
        lk("loc:206536,521945", "bnd:59"),
    ],
    "Wold Aa": [],
    "Drentse Hoofdvaart": [
        lk("ow:hydroobject.9552", "up:stuw.1748"),
        lk("ow:hydroobject.522565", "up:duikersifonhevel.17219"),
    ],
    "BoezemNW": [
        lk("ow:hydroobject.557104", "up:duikersifonhevel.11301"),
        lk("up:duikersifonhevel.11301", "ow:hydroobject.427482"),
        lk("up:duikersifonhevel.11301", "ow:hydroobject.556788-0_2"),
    ],
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
assert model.pump.static.df is not None
mask = model.pump.static.df.node_id == n("gemaal.76")
model.pump.static.df.loc[mask, "max_downstream_level"] -= 0.04
mark_level_update_protected(model.pump.static.df, mask, model=model)

# Noordscheschutsluis: basin 1920 is gemerged naar 1881; aanvoer stopt 1 cm onder benedenstrooms streefpeil.
noordscheschutsluis_pump_node_id = n("stuw.112138")
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
assert model.basin.area.df is not None
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
        n("bnd:100"): -0.2,
        n("bnd:47"): -0.2,
        n("bnd:90"): -0.2,
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
    n("gemaal.11511"): 2,  # Haverslag; oude static flow_rate=0.764
    n("gemaal.11529"): 1,  # Holstweg; oude static flow_rate=0.157
    n("gemaal.11418"): 1,  # Zwaantje; oude static flow_rate=0
    n("gemaal.11411@2"): 2,  # Adsum; gekoppeld max=1.78, huidige max=1.00, link=5901361
    n("gemaal.57"): 1,  # Oosterbroek Mallegat; oude static flow_rate=0
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
    assert static_df is not None
    mask = static_df.node_id == n("loc:240732,541530")
    static_df.loc[mask, "min_upstream_level"] = 17.7
    mark_level_update_protected(static_df, mask, model=model)
    mask = static_df.node_id == n("loc:236707,526926")
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
vechterweerd_mask = model.outlet.static.df.node_id.isin([n("stuw.50984"), n("loc:203907,508001")])
model.outlet.static.df.loc[vechterweerd_mask, ["flow_rate", "max_flow_rate"]] = 200.0

# Outlet 2582 is then the same in both control states, so we remove its DiscreteControl and aanvoer state
control_links = model.link.df[
    (model.link.df.link_type == "control") & (model.link.df.to_node_id == n("loc:203907,508001"))
]
assert len(control_links) == 1, "expected one DiscreteControl for Vechterweerd outlet 2582"
model.remove_node(int(control_links.from_node_id.iloc[0]), remove_links=True)
static_df = model.outlet.static.df
static_df = static_df[~((static_df.node_id == n("loc:203907,508001")) & (static_df.control_state == "aanvoer"))].copy()
static_df.loc[static_df.node_id == n("loc:203907,508001"), "control_state"] = None
model.outlet.static.df = static_df

# %%
# Model run


# write the dry and wet steady state scenarios and the full control model
model = write_steady_state_scenarios(
    model,
    AUTHORITY,
    SHORT_NAME,
    dry_evaporation_mm_per_day=1,
    model_exec=MODEL_EXEC,
    qlr_path=qlr_path,
)

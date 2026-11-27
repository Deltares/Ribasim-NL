# %%
import geopandas as gpd
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    supply_area_polygon,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.run_model import write_steady_state_scenarios

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "BrabantseDelta"  # authority
SHORT_NAME: str = "wbd"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")

aanvoergebieden_gpkg = cloud.joinpath(r"BrabantseDelta/verwerkt/sturing/aanvoergebieden.gpkg")


# %%
# Read data
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {n("KSL00217")}
EXCLUDE_SUPPLY_NODES = []
MANUAL_BASIN_LEVEL_NODE_IDS = [n("up:KST02510")]
outlet_max_flow_rate_from_results = {
    n("KST00407"): 2,  # Dalum/Spoorbaan
    n("KST00406"): 2,  # Dalum/Reuverlaan
    n("KST00207"): 3,  # stuw Galder
    n("KST00559"): 14,  # stuw Wernhout
    n("KST00213"): 12,  # stuw Watermolen
    n("KST00209"): 3,  # Het Laag
    n("KDU01724"): 2,  # KDU01724
    n("KDU18314"): 2,  # KDU18314
    n("KST01827"): 6,  # Molenbeek verdeelwerk
    n("KST02872"): 2,  # Haven van Zevenbergen
    n("KDU01655"): 2,  # KDU01655
    n("KST00558"): 12,  # stuw Wielhoef
    n("KST01802"): 9,  # stuw Oude Turfvaartsestaat
    n("KDU04571"): 2,  # dynamic max=0.70
    n("KST02376"): 3,  # Harmonie /vlotterinlaat 1; dynamic max=1.39
    n("KST01068"): 2,  # Stuw Ruigenhil/Oostdijk
    n("KDU10783"): 2,  # KDU10783
    n("KST01119"): 2,  # KST01119
    n("KDU10780"): 2,  # KDU10780
    n("KDU14589"): 2,  # KDU14589
    n("KST00330"): 6,  # Weststadweg Houtsesteeg
    n("KDU32322"): 2,  # KDU32322
    n("KDU24022"): 2,  # KDU24022
    n("KST00441"): 2,  # Groenvenseweg
    n("KST00513"): 17,  # Het Vaartje/Waspik
    n("KDU03763"): 9,  # KDU03763
    n("KDU02582"): 2,  # KDU02582
    n("KSL00215"): 2,  # Marksluis
    n("KDU28116"): 5,  # KDU28116
    n("ow:OVK12040"): 1,  # naam onbekend
    n("ow:OVK12037"): 2,  # naam onbekend
    n("ow:OVK00692"): 11,  # naam onbekend
    n("inlaat-donge"): 2,  # naam onbekend
    # Hoge source-capaciteiten vervangen door 1.5x de maximale flow uit het dynamic resultaat.
    n("KST00393"): 1,  # naam onbekend; dynamic max=0.01, oude max=148
    n("KST02717"): 1,  # naam onbekend; dynamic max=0.03, oude max=148
    n("KST00531"): 1,  # naam onbekend; dynamic max=0.00, oude max=172
    n("KST02766"): 1,  # naam onbekend; dynamic max=0.00, oude max=218
    n("KST01414"): 1,  # naam onbekend; dynamic max=0.30, oude max=400
    n("KDU06955"): 1,  # naam onbekend; dynamic max=0.43, oude max=400
    n("KDU44883"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KDU30201"): 1,  # naam onbekend; dynamic max=0.37, oude max=400
    n("KDU03347"): 1,  # naam onbekend; dynamic max=0.43, oude max=133
    n("KDU03125"): 1,  # naam onbekend; dynamic max=0.00, oude max=580
    n("KST01213"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KDU10709"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KDU02911"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KSL00223"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KDU14177"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KDU08736"): 1,  # naam onbekend; dynamic max=0.03, oude max=400
    n("KDU02918"): 2,  # naam onbekend; dynamic max=0.87, oude max=400
    n("KST02725"): 2,  # naam onbekend; dynamic max=0.87, oude max=405
    n("KDU26403"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KST02139"): 1,  # naam onbekend; dynamic max=0.07, oude max=133
    n("KST02972"): 1,  # naam onbekend; dynamic max=0.27, oude max=133
    n("KST01763"): 1,  # naam onbekend; dynamic max=0.10, oude max=133
    n("KST02353"): 3,  # naam onbekend; dynamic max=1.82, oude max=133
    n("KST01306"): 1,  # naam onbekend; dynamic max=0.05, oude max=147
    n("KST01258"): 1,  # naam onbekend; dynamic max=0.03, oude max=580
    n("KST02518"): 1,  # naam onbekend; dynamic max=0.22, oude max=400
    n("KST01197"): 1,  # naam onbekend; dynamic max=0.61, oude max=400
    n("KST01077"): 1,  # naam onbekend; dynamic max=0.16, oude max=580
    n("KDU22318"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KST02973"): 1,  # naam onbekend; dynamic max=0.10, oude max=400
    n("KST01156"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KDU32595"): 1,  # naam onbekend; dynamic max=0.10, oude max=400
    n("KST01133"): 1,  # naam onbekend; dynamic max=0.10, oude max=400
    n("KDU00308"): 8,  # naam onbekend; dynamic max=4.73, oude max=423
    n("KDU09972"): 2,  # naam onbekend; dynamic max=0.80, oude max=400
    n("KST00340"): 1,  # naam onbekend; dynamic max=0.24, oude max=400
    n("KST00372"): 1,  # naam onbekend; dynamic max=0.08, oude max=402
    n("KST00941"): 1,  # naam onbekend; dynamic max=0.01, oude max=401
    n("KST00327"): 1,  # naam onbekend; dynamic max=0.09, oude max=403
    n("KST00828"): 1,  # naam onbekend; dynamic max=0.50, oude max=404
    n("KST00303"): 2,  # naam onbekend; dynamic max=0.71, oude max=408
    n("KST00852"): 1,  # naam onbekend; dynamic max=0.08, oude max=412
    n("KST00322"): 2,  # naam onbekend; dynamic max=0.95, oude max=401
    n("KST00853"): 1,  # naam onbekend; dynamic max=0.01, oude max=401
    n("KST00334"): 1,  # naam onbekend; dynamic max=0.08, oude max=401
    n("KST00335"): 1,  # naam onbekend; dynamic max=0.08, oude max=401
    n("KST00302"): 1,  # naam onbekend; dynamic max=0.50, oude max=404
    n("KDU19798"): 1,  # naam onbekend; dynamic max=0.08, oude max=402
    n("KST00912"): 1,  # naam onbekend; dynamic max=0.08, oude max=402
    n("KDU02900"): 1,  # naam onbekend; dynamic max=0.01, oude max=401
    n("KST02155"): 1,  # naam onbekend; dynamic max=0.01, oude max=401
    n("KST00308"): 1,  # naam onbekend; dynamic max=0.01, oude max=401
    n("KST00839"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KDU08892"): 2,  # naam onbekend; dynamic max=1.23, oude max=400
    n("KST01034"): 1,  # naam onbekend; dynamic max=0.37, oude max=400
    n("KDU06613"): 1,  # naam onbekend; dynamic max=0.37, oude max=400
    n("KDU44303"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KST01054"): 1,  # naam onbekend; dynamic max=0.05, oude max=400
    n("KST00937"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KST02538"): 1,  # naam onbekend; dynamic max=0.00, oude max=218
    n("KDU24121"): 1,  # naam onbekend; dynamic max=0.00, oude max=218
    n("KST02708"): 1,  # naam onbekend; dynamic max=0.00, oude max=218
    n("KDU17373"): 1,  # naam onbekend; dynamic max=0.08, oude max=218
    n("KST02715"): 1,  # naam onbekend; dynamic max=0.02, oude max=218
    n("KST02707"): 1,  # naam onbekend; dynamic max=0.09, oude max=218
    n("KDU17272"): 1,  # naam onbekend; dynamic max=0.08, oude max=218
    n("KST02712"): 1,  # naam onbekend; dynamic max=0.08, oude max=218
    n("KST02709"): 1,  # naam onbekend; dynamic max=0.12, oude max=218
    n("KDU16810"): 1,  # naam onbekend; dynamic max=0.12, oude max=218
    n("KST00323"): 1,  # naam onbekend; dynamic max=0.33, oude max=423
    n("KST00324"): 1,  # naam onbekend; dynamic max=0.11, oude max=423
    n("KST00832"): 1,  # naam onbekend; dynamic max=0.33, oude max=423
    n("KST00371"): 1,  # naam onbekend; dynamic max=0.24, oude max=423
    n("KST00856"): 1,  # naam onbekend; dynamic max=0.21, oude max=423
    n("KST00740"): 1,  # naam onbekend; dynamic max=0.05, oude max=148
    n("KST00309"): 1,  # naam onbekend; dynamic max=0.24, oude max=400
    n("KDU07017"): 1,  # naam onbekend; dynamic max=0.22, oude max=400
    n("KDU02450"): 1,  # naam onbekend; dynamic max=0.02, oude max=133
    n("KDU22027"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KDU04105"): 6,  # naam onbekend; dynamic max=3.96, oude max=133
    n("KST01784"): 2,  # naam onbekend; dynamic max=0.85, oude max=133
    n("KDU04190"): 2,  # naam onbekend; dynamic max=0.85, oude max=133
    n("KDU45824"): 1,  # naam onbekend; dynamic max=0.00, oude max=133
    n("KDU04504"): 1,  # naam onbekend; dynamic max=0.37, oude max=133
    n("KDU32069"): 2,  # naam onbekend; dynamic max=0.78, oude max=580
    n("KDU03256"): 2,  # naam onbekend; dynamic max=0.96, oude max=400
    n("KDU06019"): 1,  # naam onbekend; dynamic max=0.00, oude max=400
    n("KST00917"): 1,  # naam onbekend; dynamic max=0.24, oude max=400
    n("KST01310"): 1,  # naam onbekend; dynamic max=0.03, oude max=133
    n("ow:OVK04500"): 1,  # naam onbekend; dynamic max=0.00, oude max=100
}
outlet_max_flow_rate_afvoer_by_node_id = {}
for max_flow_rates in (outlet_max_flow_rate_from_results,):
    for node_id, max_flow_rate in max_flow_rates.items():
        outlet_max_flow_rate_afvoer_by_node_id[node_id] = max(
            outlet_max_flow_rate_afvoer_by_node_id.get(node_id, 0.0),
            max_flow_rate,
        )
# Handmatige indeling control-, supply- en drain-nodes.
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [
    n("GEM_KGM00051"), n("KDU02531"), n("KST01786"), n("KST02872"), n("KST01938"), n("KST01763"), n("KST01258"), n("KST02973"), n("KST01109"),n("KDU09972"), n("KST02707"), n("KST00420"), n("KST00878"), n("KDU04105")
]

supply_nodes = [
    n("KDU00218"), n("GEM_KGM00391"), n("KDU01724"), n("KDU18314"), n("KDU32788"), n("KST02285"), n("KDU04571"), n("KDU02941"), n("KDU03124"), n("KST02376"), n("KDU30006"), n("KST01338"), n("KDU10014"), n("GEM_KGM00205"), n("KDU14555"), n("KST01131"), n("KSL00202"), n("KST01093"),n("KDU09982"), n("KDU17690"),
    n("KST00887"), n("KST00702"), n("KDU03244"), n("KDU17337"),n("GEM_KGM00067"),n("KST00829"), n("KST00832"), n("KDU13943"), n("KST00457"), n("GEM_KGM00061"), n("GEM_KGM00059"), n("KST00395"), n("KST02306"), n("KST02843"), n("KST01640"), n("GEM_KGM00041"), n("KSL00215"), n("KGM00043"), n("ow:OVK12037"),n("inlaat-donge"),
    # the supply routes of https://github.com/Deltares/Ribasim-NL/issues/1055: from the Wilhelminakanaal above Sluis III,
    # and along the old Donge
    n("KST02534"), n("KST02866"),
]

drain_nodes = [
    n("KST00407"), n("KST02860"), n("KST00816"), n("KST00741"), n("KST00522"), n("KST00450"), n("KST00505"), n("KST02717"), n("KST00724"), n("KST00531"), n("KST02885"), n("KST00398"), n("KST02890"), n("KDU03581"), n("KST00827"), n("KST02287"), n("KDU05896"), n("KST01787"), n("KST01772"), n("KDU06955"), n("KDU05728"), n("KST02400"), n("KDU30360"),
    n("GEM_KGM00119"), n("KST02290"), n("GEM_KGM00215"), n("KST00307"), n("GEM_KGM00018"), n("KDU20052"), n("KST02386"), n("KDU11005"), n("KDU03143"), n("KST01913"), n("KST01911"), n("KST02139"), n("KST02972"), n("KST01877"), n("KST01359"), n("KST01904"), n("KST01077"), n("KST01156"), n("KST01117"), n("KGM00105"),
    n("KST02405"), n("KST00852"), n("KST00839"), n("KST01096"), n("KDU05759"), n("KST01048"), n("KST02966"), n("KST00700"), n("KST01054"), n("KST02709"), n("KST02710"), n("KDU16810"), n("KST00856"), n("KST02949"), n("KDU45058"), n("KST00820"), n("KST02175"), n("KST00477"), n("KST00469"), n("KST01266"),
    n("GEM_KGM00006"), n("KST00426"), n("KST00413"),n("KST02895"), n("KDU04857"), n("KDU04851"), n("KST00115"), n("KDU04449"), n("KDU30310"), n("KDU30005"), n("KDU32538"), n("KST01960"), n("KDU02470"), n("KDU02457"), n("Theodorushaven"), n("GEM_KGM00206")
]
# fmt: on

SUPPLY_AREA_IGNORE_LINKS = {
    "Dinteloord": [],
    "Donge": [],
    "Fijnaart": [lk("ow:OVK12154", "up:KDU02937")],
    "Hartelweg": [],
    "Hoeven": [
        lk("OVK04467-1___2", "up:KDU28032"),
        lk("ow:OVK04344@2", "up:KDU28032"),
        lk("ow:OVK04469-1", "up:KST01414"),
        lk("ow:OVK07869-0", "up:KDU03208"),
        lk("up:KST01414", "OVK04467-1___2"),
        lk("up:KDU28032", "ow:OVK07869-0"),
        lk("up:ow:OVK04469-1", "ow:OVK04469-1"),
    ],
    "Leursche haven": [
        lk("ow:OVK07815", "up:KDU02968"),
        lk("OVK04467-1___2", "up:KDU28032"),
        lk("ow:OVK04344@2", "up:KDU28032"),
        lk("OVK04089___1", "up:GEM_KGM00355"),
        lk("ow:OVK07819", "up:KDU02941"),
        lk("up:KDU02941", "ow:OVK07815"),
        lk("up:KDU03252", "ow:OVK07819"),
        lk("up:KDU28032", "ow:OVK07869-0"),
        lk("up:ow:OVK04090", "ow:OVK04090"),
        lk("up:OVK04089___1", "OVK04089___1"),
    ],
    "Made": [],
    "Molenpolder": [],
    "Oud Gastel": [],
    "Patersheide": [],
    "Slikken": [],
    "Sprangsloot": [],
    "Steenbergen": [lk("OVK04601___5", "up:ow:OVK04503"), lk("up:KDU19778", "ow:OVK06051")],
    "Weimeren": [
        lk("OVK04089___1", "up:GEM_KGM00355"),
        lk("up:ow:OVK04090", "ow:OVK04090"),
        lk("up:OVK04089___1", "OVK04089___1"),
    ],
    "Westplas": [],
}


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
) -> gpd.GeoDataFrame:
    return add_controllers_to_supply_area(
        model=model,
        polygon=supply_area_polygon(aanvoergebieden_df, area_name),
        exclude_nodes=EXCLUDE_NODES,
        ignore_intersecting_links=ignore_intersecting_links,
        drain_nodes=drain_nodes,
        flushing_nodes={},
        supply_nodes=supply_nodes,
        flow_control_nodes=flow_control_nodes,
        control_node_types=CONTROL_NODE_TYPES,
        add_supply_nodes=True,
        flow_rate_aanvoer=20.0,
        max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
        flow_rate_afvoer=100.0,
        max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    )


# %%

assert model.outlet.static.df is not None
outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# %% Fixes

# model.update_node(node_id=968, node_type="Outlet")  # wordt outlet, was manning
model.remove_node(n("KGM00052"), remove_links=True)
model.remove_node(n("GEM_KGM00053"), remove_links=True)
model.remove_node(n("ow:OVK12030"), remove_links=True)

model.reverse_link(link_id=lk("KST00114", "up:KST02972"))
model.reverse_link(link_id=lk("up:GEM_KGM00139", "KST00114"))

model.reverse_link(link_id=lk("ow:OVK12040", "bnd:36"))
model.reverse_link(link_id=lk("up:OVK06052", "ow:OVK12040"))

model.reverse_link(link_id=lk("up:KDU00331", "KDU11184"))
model.reverse_link(link_id=lk("KDU11184", "up:KST00531"))
model.reverse_link(link_id=lk("KDU03574", "up:KST00724"))
model.reverse_link(link_id=lk("up:GEM_KGM00051", "KDU03574"))

# https://github.com/Deltares/Ribasim-NL/issues/1055: the Wilhelminakanaal above Sluis III, from which KST02534 lets in,
# is at 12.55 m, the min_upstream_level of Sluis III. koppelen couples the boundary to it
assert model.level_boundary.static.df is not None
level_boundary = model.level_boundary.static.df
level_boundary.loc[level_boundary.node_id == n("bnd:63"), "level"] = 12.55

# https://github.com/Deltares/Ribasim-NL/issues/1055: KST02917 drains up:KST00507 into up:KDU01599 as an overflow, see
# below
model.reverse_link(link_id=lk("up:KDU01599", "KST02917"))
model.reverse_link(link_id=lk("KST02917", "up:KST00507"))

# Roode Vaart: Outlet 589 laat in vanuit het Hollandsch Diep; Pump 972 blijft de Roode Vaart afmalen
model.reverse_link(link_id=lk("KSL00202", "bnd:22"))
model.reverse_link(link_id=lk("up:KDU02937", "KSL00202"))

## Dintelsas Volkerak hoort hydraulisch een uitlaat naar RWS te zijn.
## Tijdelijk als pomp modelleren, omdat afwatering anders vastloopt op hoge RWS-waterstanden.
temporary_rws_pump_node_id = n("ow:OVK12034")
model.update_node(node_id=temporary_rws_pump_node_id, node_type="Pump")
model.node.df.loc[temporary_rws_pump_node_id, "name"] = (
    "Dintelsas Volkerak tijdelijke pomp naar RWS (eigenlijk uitlaat)"
)
assert model.pump.static.df is not None
model.pump.static.df.loc[model.pump.static.df.node_id == temporary_rws_pump_node_id, ["flow_rate", "max_flow_rate"]] = (
    100
)

## Uitlaat naar het Schelde-Rijnkanaal loopt in gekoppelde runs hydraulisch vast door negatieve delta h.
## Tijdelijk als pomp modelleren; hydraulisch hoort dit een uitlaat naar RWS te zijn.
temporary_zoommeer_pump_node_id = n("KDU45738")
model.update_node(node_id=temporary_zoommeer_pump_node_id, node_type="Pump")
model.node.df.loc[temporary_zoommeer_pump_node_id, "name"] = "Zoommeer tijdelijke pomp naar RWS (eigenlijk uitlaat)"

# Flow_rate was te laag ingesteld
model.pump.static.df.loc[model.pump.static.df.node_id == n("GEM_KGM00399"), ["flow_rate", "max_flow_rate"]] = 5

model.pump.static.df.loc[model.pump.static.df.node_id == n("GEM_KGM00073"), ["flow_rate", "max_flow_rate"]] = 5


# %%
# Toevoegen aanvoergebieden

supply_area_control_node_ids: set[int] = set()
for area_name, ignore_intersecting_links in SUPPLY_AREA_IGNORE_LINKS.items():
    print(f"Toevoegen {area_name}")
    node_functions_df = add_supply_area_control(
        model=model,
        aanvoergebieden_df=aanvoergebieden_df,
        area_name=area_name,
        ignore_intersecting_links=ignore_intersecting_links,
        supply_nodes=supply_nodes,
        drain_nodes=drain_nodes,
        flow_control_nodes=flow_control_nodes,
    )
    if not node_functions_df.empty and node_functions_df["function"].isin(["supply", "flow_control"]).any():
        supply_area_control_node_ids.update(int(node_id) for node_id in node_functions_df.index)


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
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# https://github.com/Deltares/Ribasim-NL/issues/1055: the overflow KST02917 drains up:KST00507 from 2 cm above the
# target level that weir KST00507 keeps
OVERFLOW_OFFSET = 0.02
overflow, overflow_control = n("KST02917"), n("discrete_control:KST02917")
assert model.outlet.static.df is not None and model.discrete_control.condition.df is not None
static, condition = model.outlet.static.df, model.discrete_control.condition.df
static.loc[static.node_id == overflow, "min_upstream_level"] += OVERFLOW_OFFSET
condition.loc[condition.node_id == overflow_control, ["threshold_high", "threshold_low"]] += OVERFLOW_OFFSET

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
# Bestaande handmatige pompwaarden, zoals 376 en 446, blijven leidend.
pump_max_flow_rate_from_results = {
    n("GEM_KGM00345"): 1,  # naam onbekend; oude static flow_rate=0.13
    n("GEM_KGM00051"): 1,  # naam onbekend; oude static flow_rate=0.13
    n("GEM_KGM00311"): 3,  # naam onbekend; oude static flow_rate=1.58
    n("GEM_KGM00215"): 1,  # naam onbekend; oude static flow_rate=0.62
    n("GEM_KGM00025"): 1,  # naam onbekend; oude static flow_rate=0.2
    n("GEM_KGM00022"): 3,  # naam onbekend; oude static flow_rate=1.9
    n("GEM_KGM00107"): 1,  # naam onbekend; oude static flow_rate=0.21
    n("GEM_KGM00111"): 4,  # naam onbekend; oude static flow_rate=0.75
    n("GEM_KGM00350"): 1,  # naam onbekend; oude static flow_rate=0.2
    n("GEM_KGM00309"): 2,  # naam onbekend; dynamic max=0.77, oude static flow_rate=0.5
    n("KGM00105"): 1,  # Canada; oude static flow_rate=0.05
    n("GEM_KGM00003"): 1,  # naam onbekend; oude static flow_rate=0.2
    n("GEM_KGM00355"): 2,  # naam onbekend; oude static flow_rate=1.58
    n("GEM_KGM00426"): 3,  # naam onbekend; oude static flow_rate=1.58
    n("GEM_KGM00038"): 2,  # naam onbekend; oude static flow_rate=0.08
    n("GEM_KGM00206"): 1,  # naam onbekend; oude static flow_rate=0.04
    n("KGM00425"): 5,  # Roode Vaart naar Hollandsch Diep; 15 mm/dag over de 28.6 km2 die erop afwatert
    n("KGM00421"): 1,  # Gemaal bij Keersluis Leursche Haven; dynamic max=0.54, oude max=120
    n("KGM00420"): 1,  # Gemaal bii Keersluis Laaksche Vaart; dynamic max=0.46, oude max=120
    n("KDU45738"): 24.1,  # Tijdelijke pomp naar Zoommeer; behoud de oude afvoercapaciteit van de uitlaat.
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
manual_max_flow_rate_node_ids = {n("GEM_KGM00399"), n("GEM_KGM00073"), n("KDU45738"), n("ow:OVK12034")}
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

# %% Verdeelwerk bij Eindhoven 10% afvgevoerd via Beatrixkanaal (max 20m3/s) en 90% via Dieze
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 293, "flow_rate"] = 200
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 210, "min_upstream_level"] = 16.2

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

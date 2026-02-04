# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

LEVEL_DIFFERENCE_THRESHOLD = 0.02  # sync model.solver.level_difference_threshold and control-settings
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
EXCLUDE_NODES = {486, 746, 750, 753}
EXCLUDE_SUPPLY_NODES = []

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

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# alle uitlaten en inlaten op 50m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.flow_rate = 30
model.pump.static.df.flow_rate = 30

# %%
# Identificeren aanvoerknopen en voorzien van afvoercapaciteit

# aanmaken node_df en specificeren supply_nodes
# knopen die beginnen met INL, i of eindigen op i, maar niet op fictief
for node_type in CONTROL_NODE_TYPES:
    node_df = getattr(model, pascal_to_snake_case(node_type)).node.df

    node_df[IS_SUPPLY_NODE_COLUMN] = (
        node_df["meta_code_waterbeheerder"].str.startswith("INL")
        | node_df["meta_code_waterbeheerder"].str.startswith("i")
        | node_df["meta_code_waterbeheerder"].str.startswith("I")
        | node_df["meta_code_waterbeheerder"].str.endswith("i")
    ) & ~(
        node_df.node_type.isin(CONTROL_NODE_TYPES) & node_df["meta_code_waterbeheerder"].str.endswith("fictief")
        | node_df.index.isin(EXCLUDE_SUPPLY_NODES)
    )

    getattr(model, pascal_to_snake_case(node_type)).node.df = node_df

    # force nan or 0 to 20 m3/s
    node_ids = node_df[node_df[IS_SUPPLY_NODE_COLUMN]].index.values
    print(node_ids)
    static_df = getattr(model, pascal_to_snake_case(node_type)).static.df

    mask = static_df.node_id.isin(node_ids) & ((static_df.flow_rate == 0) | (static_df.flow_rate.isna()))

    static_df.loc[mask, "flow_rate"] = 20

# %% model fixes
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 45, "level"] = -1.4

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

model.update_node(node_id=730, node_type="ManningResistance")
model.outlet.static.df.loc[model.outlet.static.df.node_id == 548, "max_flow_rate"] = 20

# %%
# Toevoegen Kromme Rijn/ARK

polygon = aanvoergebieden_df.loc[["Kromme Rijn/ARK"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [2175]

# doorspoeling (op uitlaten)
# 41: Spijksterpompen

flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# 864: Achterrijn Stuw
# 893: ST6050 2E Veld
# 969: ST0842 Trechtweg
# 971: ST0010
# 1126: Pelikaan
# 1145: ST003 Eindstuw Raaphofwetering
# 1168: ST0733
# 1223: ST0815
# 2110:
# 851: ST0014 Koppeldijk stuw
# 923: ST1264 Hevelstuw Ravensewetering
drain_nodes = [554, 851, 864, 893, 969, 971, 1126, 1145, 1168, 1223, 2110]

# handmatig opgegeven supply nodes (inlaten)
# 554: G0007 Koppeldijk gemaal
# 589: Mastwetering
# 624: G4481 Pelikaan
# 851: ST0014 Koppeldijk stuw
# 648: G3007 Trechtweg
# 649: Voorhavendijk

supply_nodes = [554, 589, 624, 648, 649]

# 1107:ST0826
flow_control_nodes = [1107]

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
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen ARK/Lek
polygon = aanvoergebieden_df.at["ARK/Lek", "geometry"]
# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
ignore_intersecting_links: list[int] = [1448]

# doorspoeling (op uitlaten)
flushing_nodes = {}  # {357: 0.02, 393: 0.012, 350: 0.015, 401: 0.017, 501: 0.034, 383: 0.013}

# handmatig opgegeven drain nodes (uitlaten) definieren
# 978: ST4007 Overeind Stuw
# 980: ST7229
# 588: Blokhoven
# 551: Biester
# 591: Vuylcop-Oost
# 612: Polder Tull En T Waal
# 844: ST1541
# 993:T Klooster
drain_nodes = [978, 980, 551, 588, 612, 591, 844, 979, 993]

# handmatig opgegeven supply nodes (inlaten)
# 627: G4015 Overeind
# 651: G4023 Pothoek
# 840: Polder Tull En T Waal
# 855: ST4036
# 976: ST0850
supply_nodes = [627, 651, 840, 855, 976]

# 977: Blokhoven
flow_control_nodes = [977]

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
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Toevoegen Lopikerwaard

polygon = aanvoergebieden_df.at["Lopikerwaard", "geometry"]
# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [404, 1305, 1618, 2271]

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# node_id: Naam
# 298: AF0032
# 467: SY3441
# 634: Hazepad 'T
# 920: ST1449
# 818: Zevenhoven stuw
drain_nodes = [298, 634, 818, 920]

# handmatig opgegeven supply nodes (inlaten)
# node_id: Naam
# 424: I6189
# 630: Blokland
# 1007: Hazepad 'T stuw
# 1156: ST1064

supply_nodes = [424, 630, 1007, 1156]

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
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Toevoegen Leidsche-Oude Rijn

polygon = aanvoergebieden_df.at["Leidsche-Oude Rijn", "geometry"]
# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
# link_id: beschrijving
# 384: basin area niet ok
ignore_intersecting_links: list[int] = [292, 384, 762, 847, 1385, 1775, 2169]

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# node_id: Naam
# 513: Gemaal: Terwijde
# 1077: ST0725
# 598: Vleuterweide
drain_nodes = [513, 598, 1077]

# handmatig opgegeven supply nodes (inlaten)
# 553:G0096
# 593: G8014
# 797: ST0945
# 890:ST0946
# 911: Vleuterwijde Oost
# 1081: ST1356
# 1082: ST0243
supply_nodes = [553, 593, 797, 890, 911, 1082]

# 207: I6023
# 527: I1655
flow_control_nodes = [207, 527]

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
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Toevoegen Hollandsche IJssel

polygon = aanvoergebieden_df.at["Hollandsche IJssel", "geometry"]
# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
# link_id: beschrijving
# areas verkeerd gedefinieerd of doorsnijdijng 2 areas
ignore_intersecting_links: list[int] = [227, 292, 638, 762, 847, 1385, 1775]

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# node_id: Naam
# 347: I1317 Nog checken!
drain_nodes = [347]

# handmatig opgegeven supply nodes (inlaten)
# 564: Reyerscop
# 754: Doorslag
# 830: ST3928
# 1022: ST1319
supply_nodes = [564, 754, 830, 1022]

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
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Utrecht-Noord

polygon = aanvoergebieden_df.at["Utrecht-Noord", "geometry"]
# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# node_id: Naam
# 477: SY1901
# 633: Voordorp
# 799 Maartendsdijk stuw
# 944: ST0895
drain_nodes = [477, 633, 799, 944]

# handmatig opgegeven supply nodes (inlaten)
# 581: Maartensdijk Pomp
# 650: Groenkan
supply_nodes = [581, 650, 924]


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
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# Toevoegen Utrechtse Heuvelrug/Kromme Rijn

polygon = aanvoergebieden_df.at["Utrechtse Heuvelrug/Kromme Rijn", "geometry"]
# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
# link_id: beschrijving
# 2103: basin area niet ok
ignore_intersecting_links: list[int] = [2103]

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# node_id: Naam
# 894: ST2901
# 956: ST1014
drain_nodes = [894, 956]

# handmatig opgegeven supply nodes (inlaten)
# 626: De Strijp
# 654: De Wijngaard
# 655: Slot Zeist
supply_nodes = [626, 654, 655]

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
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)


# %%
# EXCLUDE NODES op 0 m3/s zetten
mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, "flow_rate"] = 0
model.outlet.static.df.loc[mask, "min_flow_rate"] = 0
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0


# %% add all remaining inlets/outlets
# add all remaing outlets
# handmatig opgegeven flow control nodes definieren
# 405: AF0095
# 636: Trappersheul
# 777: Cothen stuw
# 778: ST3912
# 814: ST6055
# 809: Hoek de stuw
# 919: Werkhoven
# 1036: ST5011
# 1063: Prinses Irenebrug
# 1154: ST0479
# 1010: ST1257
# 1011: ST1353
# 1033: ST4112
# 1038: ST1758
# 1039: ST5009
# 1050: ST0477
# 1153: ST6092
# 1059:ST5022
# 1155: ST7263
# 1279: ST1888

flow_control_nodes = [
    134,
    405,
    636,
    777,
    778,
    809,
    814,
    919,
    1010,
    1011,
    1033,
    1036,
    1038,
    1039,
    1050,
    1059,
    1155,
    1063,
    1153,
    1154,
    1279,
]

# handmatig opgegeven supply nodes (inlaten)
# 103:I6000
# 230: I2003
# 358: AF0082
# 476, D13544  Vraag aan Daniel: waarom zit deze node niet in Leidsche Rijn Noord?
# 481: inlaat Wijk bij Duurstede
# 506: Papekopperdijk
# 536: Noordergemaal
# 542: Aanvoerder, De
# 543: Rondweg

# 560: Amerongerwetering
# 561: Gemaal Rijnwijck  eruit!!
# 570:Weerdesteinsesloot  #dubbel
# 579:Sandwijck  #dubbel
# 626: Strijp, De  #dubbel

# 638: Oosteinde Waarder Oost
# 639:Oosteinde Waarder West
# 640:Schoonhoven
# 747: Goejanverwelle Sluis: Wordt deze open gezet in droge tijden?
# 906: ST0779
# 962: ST2903
# 987: ST0409
# 742, Haanwijkersluis
# 1014: ST0439
# 1042: Hwvz Diemerbroek
# 1056: Ruige Weide Stuw
# 1194: H078195
# 2111: Inlaat bij Nieuwkoop (Checken)
# 425: AF0038
# 637: Hwvz Diemerbroek 56

supply_nodes = [
    103,
    358,
    425,
    481,
    476,
    486,
    506,
    536,
    542,
    543,
    637,
    638,
    639,
    640,
    747,
    772,
    742,
    906,
    962,
    987,
    1014,
    1042,
    1056,
    2111,
]
# 185: Westraven
# 411: I6207 Check!
# 545: Rijnvliet
# 173, 168, 139, 198, Oog in Al

drain_nodes = [173, 168, 139, 185, 198, 230, 411, 467, 545, 887]

# %% Toevoegen waar nog geen sturing is toegevoegd

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
    us_threshold_offset=LEVEL_DIFFERENCE_THRESHOLD,
)

# %% Noordergemaal, node=536 slaat pas aan wanneer Wijk van Duurstede net genoeg kan leveren
model.pump.static.df.loc[model.pump.static.df.node_id == 536, "max_downstream_level"] -= 0.01

# 3 sifons, 468,469,470 onder Ark wordt later ingeschakeld dan inlaat Vreeswijk
model.outlet.static.df.loc[model.outlet.static.df.node_id == 468, "max_downstream_level"] -= 0.01
model.outlet.static.df.loc[model.outlet.static.df.node_id == 469, "max_downstream_level"] -= 0.01
model.outlet.static.df.loc[model.outlet.static.df.node_id == 470, "max_downstream_level"] -= 0.01


# %% Junctionfy(!)
model = junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
model.solver.level_difference_threshold = LEVEL_DIFFERENCE_THRESHOLD

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime


# %%

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=1)
model.write(ribasim_toml_dry)

# run hoofdmodel
if MODEL_EXEC:
    result = model.run()
    controle_output = Control(ribasim_toml=ribasim_toml_dry, qlr_path=qlr_path)
    indicators = controle_output.run_all()
    model = Model.read(ribasim_toml_dry)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
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

# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
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
# 750: Oude Leidseweg Sluis
EXCLUDE_NODES = {486, 545, 750, 772}
EXCLUDE_SUPPLY_NODES = []

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoer_path = aanvoer_path = cloud.joinpath(AUTHORITY, "verwerkt/4_ribasim/peilgebieden_bewerkt.gpkg")

aanvoergebieden_gpkg = cloud.joinpath(r"StichtseRijnlanden/verwerkt/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoer_path, qlr_path])


# %%
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

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
model.remove_node(node_id=1344, remove_links=True)
model.remove_node(node_id=1345, remove_links=True)

# node 2806 is een inlaat, dus flow_direction draaien
model.reverse_link(link_id=300)
model.reverse_link(link_id=1686)


# %% [markdown]
# # Aanpak sturing per aanvoergebied
#
# ## verplichte invoer
# - `polygon`: Polygoon van het aanvoergebied
# ## optionele invoer:
# - `flushing_nodes`: uitlaten met doorspoeldebiet (dict met {node_id:doorspoeldebiet}). Deze worden nooit automatisch gedetecteerd (!)
# - `ignore_intersecting_links`: links die het aanvoergebied snijden, geen control-nodes bevatten en waarbij dat geaccepteerd moet worden
# - `supply_nodes`: inlaten die niet automatisch worden gedetecteerd
# - `drain_nodes`: uitlaten die niet automatisch worden gedetecteerd
#
# ## In 1x, wanneer alles goed gaat
# `node_functions_df` kun je gebruiken om e.e.a. te plotten
# ```
# node_functions_df = add_controllers_to_supply_area(
#     model=model,
#     polygon=polygon,
#     ignore_intersecting_links= ignore_intersecting_links,
#     drain_nodes=drain_nodes,
#     flushing_nodes=flushing_nodes,
#     supply_nodes=supply_nodes,
#     level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
#     control_node_types=CONTROL_NODE_TYPES
# )
# ```
# ## Debuggen: in stappen
# 1. bepalen positie van alle knopen relatief tot aanvoergebied (inflow, outflow, internal)
# ```
# node_positions_df = get_control_nodes_position_from_supply_area(
#     model=model,
#     polygon=polygon,
#     control_node_types=CONTROL_NODE_TYPES,
#     ignore_intersecting_links=ignore_intersecting_links,
# )
# ```
# 2. bepalen functie van alle knopen (drain, supply, flow_control and flushing)
# ```
# node_functions_df = add_control_functions_to_connector_nodes(
#     model=model,
#     node_positions=node_positions_df["position"],
#     supply_nodes=supply_nodes,
#     drain_nodes=drain_nodes,
#     flushing_nodes=flushing_nodes,
#     is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
# )
# ```
#
# 3. toevoegen van controllers aan alle knopen
# ```
# add_controllers_to_connector_nodes(
#     model=model,
#     node_functions_df=node_functions_df,
#     level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
# )
# ```
# %%
# Toevoegen Kromme Rijn/Amsterdam-Rijnkanaal

polygon = aanvoergebieden_df.loc[["Kromme Rijn/Amsterdam-Rijnkanaal"], "geometry"].union_all()

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

# 624: G4481 Pelikaan
# 864: Achterrijn Stuw
# 969: ST0842 Trechtweg
# 923: ST1264 Hevelstuw Ravensewetering
# 1145: ST003 Eindstuw Raaphofwetering
# 2110:
drain_nodes = [554, 624, 864, 969, 923, 1145, 2110]

# handmatig opgegeven supply nodes (inlaten)
# 554: G0007 Koppeldijk gemaal
# 851: ST0014 Koppeldijk stuw
# 1126: ST8009 Pelikaan
# 648: G3007 Trechtweg
# 893: ST6050 2E Veld
supply_nodes = [554, 851, 1126, 648, 893]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Amsterdam-Rijnkanaal/Lek
polygon = aanvoergebieden_df.at["Amsterdam-Rijnkanaal/Lek", "geometry"]
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
# 591: Vuylcop-Oost
drain_nodes = [978, 980, 591, 979]

# handmatig opgegeven supply nodes (inlaten)
# 627: G4015 Overeind
# 651: G4023 Pothoek
supply_nodes = [627, 651]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Gek. Hollandse IJssel

polygon = aanvoergebieden_df.at["Gek. Hollandse IJssel", "geometry"]
# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [1305, 1618]

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# node_id: Naam
# 634: Hazepad 'T
drain_nodes = [634]

# handmatig opgegeven supply nodes (inlaten)
# node_id: Naam
# 424: I6189
# 630: Blokland
supply_nodes = [424, 630]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Leidse Rijn-Noord

polygon = aanvoergebieden_df.at["Leidse Rijn-Noord", "geometry"]
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
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
# 598: Vleuterweide
# 1014: ST0439
supply_nodes = [598]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Leidse Rijn-Zuid

polygon = aanvoergebieden_df.at["Leidse Rijn-Zuid", "geometry"]
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
supply_nodes = [564, 754]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Stadsgebied Utrecht

polygon = aanvoergebieden_df.at["Stadsgebied Utrecht", "geometry"]
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
drain_nodes = [477]

# handmatig opgegeven supply nodes (inlaten)
# 581: Maartensdijk Pomp
# 633: Voordorp
# 799: Maartendsdijk Pomp
# 650: Groenkan
# 924: ST1194
supply_nodes = [581, 633, 650, 799, 924]

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
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

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
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
# 747: Goejanverwelle stuw
# 777: Cothen stuw
# 778: ST3912
# 814: ST6055
# 809: Hoek de stuw
# 919: Werkhoven
# 1063: Prinses Irenebrug
# 1154: ST0479
flow_control_nodes = [747, 777, 778, 545, 809, 814, 919, 1063, 1154]

# handmatig opgegeven supply nodes (inlaten)
# 103:I6000
# 358: AF0082
# 481: inlaat Wijk bij Duurstede
# 506: Papekopperdijk
# 542: Aanvoerder, De
# 543: Rondweg
# 560: Amerongerwetering
# 561: Gemaal Rijnwijck  eruit!!
# 570:Weerdesteinsesloot  #dubbel
# 579:Sandwijck  #dubbel
# 626: Strijp, De  #dubbel
# 637: Hwvz Diemerbroek 56
# 638: Oosteinde Waarder Oost
# 639:Oosteinde Waarder West
# 640:Schoonhoven
# 906: ST0779
# 1014: ST0439
# 1056: Ruige Weide Stuw

supply_nodes = [103, 358, 481, 486, 506, 542, 543, 637, 638, 639, 640, 772, 906, 1014, 1056]

# %% Toevoegen waar nog geen sturing is toegevoegd

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
    us_threshold_offset=LEVEL_DIFFERENCE_THRESHOLD,
)

# %%

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
model.solver.level_difference_threshold = LEVEL_DIFFERENCE_THRESHOLD

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime
# model.basin.area.df["meta_aanvoer"] = True
# model.outlet.static.df["meta_aanvoer"] = 1
# model.pump.static.df["meta_func_aanvoer"] = 1
# model.pump.static.df["meta_func_afvoer"] = 1

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

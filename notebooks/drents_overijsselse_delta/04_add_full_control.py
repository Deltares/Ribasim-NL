# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
from ribasim_nl.parametrization.basin_tables import update_basin_static

from ribasim_nl import CloudStorage, Model

# Globale settings

MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "DrentsOverijsselseDelta"
SHORT_NAME: str = "dod"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten

EXCLUDE_NODES = {}

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(r"DrentsOverijsselseDelta/verwerkt/sturing/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


# %%
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# alle uitlaten en inlaten op 30m3/s, geen cap verdeling. Dit wordt de max flow in model.
# En als flow_rate niet bekend is de flow
model.outlet.static.df.max_flow_rate = 30
model.outlet.static.df.flow_rate = 100
model.pump.static.df.max_flow_rate = model.pump.static.df.flow_rate

# Outlet Wsterkamp node #1125 richting omdraaien, is inlaat
for link_id in [341, 2427]:
    model.reverse_link(link_id=link_id)

# Outlet node #603 richting omdraaien, is inlaat
for link_id in [1618, 305]:
    model.reverse_link(link_id=link_id)

# Outlet node #1291 richting omdraaien, is inlaat
for link_id in [3051, 2652]:
    model.reverse_link(link_id=link_id)


# Trambrug Outlet node #597 richting omdraaien, is inlaat
for link_id in [249, 1604]:
    model.reverse_link(link_id=link_id)

# Broammeule Noord #629 is een aanvoergemaal
for link_id in [1020, 2082]:
    model.reverse_link(link_id=link_id)

# Kloosterveen node #965 richting omdraaien, is inlaat
for link_id in [1041, 2280]:
    model.reverse_link(link_id=link_id)

# Katereer node #589 richting omdraaien, is inlaat
for link_id in [2992, 2093]:
    model.reverse_link(link_id=link_id)

# node #1444 richting omdraaien, is inlaat, zit niet op exacte plaats :-(
for link_id in [1244, 1581]:
    model.reverse_link(link_id=link_id)


# make outlets from manning
outlet_ids = [1417, 1418, 1419, 1428, 1444, 1448, 1452, 1488, 1466, 1459, 1522]

for node_id in dict.fromkeys(outlet_ids):
    model.update_node(node_id=node_id, node_type="Outlet")

# Oude Wetering #906 is een gemaal
model.update_node(node_id=906, node_type="Pump")

# Outlet Zedemuden moet kunnen inlaten, dus richting omdraaien evt takken toevoegen
for link_id in [
    3087,
    3088,
]:
    model.reverse_link(link_id=link_id)


# Verwijderen nutteloze kunstwerken
model.remove_node(530, remove_links=True)
model.remove_node(264, remove_links=True)
model.remove_node(1174, remove_links=True)
model.remove_node(1229, remove_links=True)
model.remove_node(266, remove_links=True)
model.remove_node(268, remove_links=True)

# Streefpeil te laag
model.outlet.static.df.loc[model.outlet.static.df.node_id == 959, "min_upstream_level"] = 11.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 971, "min_upstream_level"] = 11.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1238, "min_upstream_level"] = 11.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 544, "min_upstream_level"] = -0.2

model.merge_basins(node_id=1970, to_node_id=1990, are_connected=True)
# Manning moet outlet zijn
model.update_node(node_id=1468, node_type="Outlet")
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1468, "min_upstream_level"] = 2.63


flow_control_nodes = [152, 227, 241, 285, 385, 610, 937, 1317, 2662, 2664, 2666, 125, 1088, 193]

# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.
# fmt: off
supply_nodes = [
    152, 195,192, 196, 212, 213, 216, 219, 284, 288,
    332, 333, 334, 346, 370, 464, 503,508, 511,545, 548, 550,552,
    562, 563, 564, 570, 571, 574, 579, 597, 615, 617,631,
    632, 633, 634, 636, 637, 638, 640, 645, 647, 648,
    650, 652, 660, 665, 669, 676, 686, 693, 694, 699,
    704, 706, 708, 712, 713, 715, 723, 726, 741,748, 753,
    758, 759, 760, 775, 783,786,787, 800, 801,813, 816,823, 832, 846,868,
    878, 894,896, 898,902,906, 918,920, 933, 936, 949, 951, 954, 955,
    965, 968, 969, 972, 976, 977,983, 987, 1002,1009, 1012, 1024,
    1025, 1034, 1035, 1047, 1050, 1054, 1056, 1058,1059, 1068, 1074,
    1077, 1078, 1086, 1109,1115, 1132, 1141, 1142, 1182, 1187, 1188,
    1190, 1199, 1207, 1209, 1217, 1221, 1289,1231, 1238, 1243, 1246,
    1260, 1261, 1262, 1273, 1278,1279, 1283,1291, 1294, 1302, 1303, 1307,
    1312,1325, 1332, 1344, 1391, 1401,1418, 1452,1473, 2650, 2652, 2653, 2657,
    2658, 2659, 2662, 2665, 2667, 3110, 3116, 3118, 3119, 3122,
    3124, 3125, 3126,
]


drain_nodes = [
    111, 113, 116, 122,137,143,144,145, 150, 152,154,157, 159,161,169, 170, 173,174,177,181, 186, 190,
    201,202,208,210, 211,218,221,232,234, 239,246,251, 265, 267,271, 276,295, 296, 297, 304, 311,
    315, 322, 324, 325, 337, 351, 352, 361, 362, 366,369,
    375,377, 378, 387, 395,398, 406, 418,421, 422, 428,431,438, 444, 445,
    446, 451, 454, 456,461,468, 469, 475, 481, 486, 487,489, 490,
    501, 515, 521, 529, 534, 541, 550, 565, 580,582,591,
    583, 584,586, 587, 588, 590, 596, 599, 604, 608, 609,614,616, 624,625, 630,
    643, 646, 649,654, 657, 664, 671, 672, 674, 679, 680,
    688, 690, 697, 698, 700, 701, 771,777, 780, 815,816,841, 849,
    859, 873,884,893,932, 942, 953, 958, 986, 1007, 1028,
    1052, 1066, 1091,1094, 1096, 1100, 1101, 1103, 1113,1125, 1129, 1140,
    1159, 1173, 1175, 1186, 1189, 1193, 1210, 1216, 1219, 1225, 1226,1233,
    1234, 1259,1264, 1268, 1271, 1293, 1318, 1323,1346, 1350, 1360,
    1366, 1368, 1372, 1373, 1381,1386,1394, 1395,1406, 1409, 1410, 1448,1482, 3128,3153,3156,
    3430, 3567,
]
# fmt: on

# %%
# Kunstwerk-in/-uit ruimtelijk koppelen aan Ribasim-nodes

kunstwerk_in_path = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "kunstwerk_in.gpkg")
kunstwerk_uit_path = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "kunstwerk_uit.gpkg")

cloud.synchronize(filepaths=[kunstwerk_in_path, kunstwerk_uit_path])


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
    kunstwerk_gdf = gpd.read_file(kunstwerk_path, fid_as_index=True)

    control_node_gdf = model.node.df[model.node.df["node_type"].isin(node_types)].copy()
    control_node_gdf["ribasim_node_id"] = control_node_gdf.index

    control_node_gdf = gpd.GeoDataFrame(
        control_node_gdf,
        geometry="geometry",
        crs=model.crs,
    )

    kunstwerk_gdf = kunstwerk_gdf.to_crs(model.crs)

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


def print_node_list_diff(label: str, before_nodes: list[int], after_nodes: list[int]) -> None:
    before_nodes = set(before_nodes)
    after_nodes = set(after_nodes)

    added_nodes = sorted(after_nodes - before_nodes)
    removed_nodes = sorted(before_nodes - after_nodes)

    print(f"{label}: {len(added_nodes)} toegevoegd, {len(removed_nodes)} verwijderd t.o.v. handmatig")
    print(f"{label} toegevoegd: {added_nodes}")
    print(f"{label} verwijderd: {removed_nodes}")


def print_manual_role_conflicts(
    kunstwerk_supply_nodes: list[int],
    kunstwerk_drain_nodes: list[int],
    manual_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> None:
    kunstwerk_supply_nodes = set(kunstwerk_supply_nodes)
    kunstwerk_drain_nodes = set(kunstwerk_drain_nodes)
    manual_supply_nodes = set(manual_supply_nodes)
    manual_drain_nodes = set(manual_drain_nodes)
    manual_flow_control_nodes = set(manual_flow_control_nodes)

    print(
        "kunstwerk_in overruled door handmatig: "
        f"flow_control={sorted(kunstwerk_supply_nodes & manual_flow_control_nodes)}, "
        f"drain={sorted(kunstwerk_supply_nodes & manual_drain_nodes)}"
    )
    print(
        "kunstwerk_uit overruled door handmatig: "
        f"flow_control={sorted(kunstwerk_drain_nodes & manual_flow_control_nodes)}, "
        f"supply={sorted(kunstwerk_drain_nodes & manual_supply_nodes)}"
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

# %%
# Toevoegen Ankersmit

polygon = aanvoergebieden_df.loc[["Ankersmit"], "geometry"].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [932, 971, 2315, 2329, 2330, 2388, 2759, 2766]

# doorspoeling (op uitlaten)
flushing_nodes = {}

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
    add_supply_nodes=True,
)

# %%
# Toevoegen Westerveld

polygon = aanvoergebieden_df.loc[["Westerveld"], "geometry"].union_all()

# # links die intersecten die we kunnen negeren
# # link_id: beschrijving
ignore_intersecting_links: list[int] = [363, 1000, 1001]

# doorspoeling (op uitlaten)
flushing_nodes = {}

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
    add_supply_nodes=True,
)

# %%
# Toevoegen Overijssels Kanaal

polygon = aanvoergebieden_df.loc[["Overijssels Kanaal"], "geometry"].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [1057, 1581, 2480, 2685]

# doorspoeling (op uitlaten)
flushing_nodes = {}

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
    add_supply_nodes=True,
)

# %%
# Toevoegen Vecht Twentekanalen

polygon = aanvoergebieden_df.loc[["Vecht-Twentekanalen"], "geometry"].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}


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
    add_supply_nodes=True,
)

# %%
# Toevoegen Dedemsvaart

polygon = aanvoergebieden_df.loc[["Dedemsvaart"], "geometry"].union_all()


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [1680, 1094, 1274, 2164, 2344]

# doorspoeling (op uitlaten)
flushing_nodes = {}

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
    add_supply_nodes=True,
)

# %%
# Toevoegen IJsselmeergebied

polygon = aanvoergebieden_df.loc[["IJsselmeergebied"], "geometry"].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [1094, 1274, 2164, 2344]

# doorspoeling (op uitlaten)
flushing_nodes = {}


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
    add_supply_nodes=True,
)

# %%

# Toevoegen Hoogeveense Vaart

polygon = aanvoergebieden_df.loc[["Hoogeveense Vaart"], "geometry"].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [32, 653, 765, 809, 2499, 2519, 2555, 2572, 2757, 2812, 2970]

# doorspoeling (op uitlaten)
flushing_nodes = {}

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
    add_supply_nodes=True,
)

# %%
# Toevoegen Wold Aa

polygon = aanvoergebieden_df.loc[["Wold Aa"], "geometry"].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
flushing_nodes = {}

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
    add_supply_nodes=True,
)

# %%

# Toevoegen Drentse Hoofdvaart

polygon = aanvoergebieden_df.loc[["Drentse Hoofdvaart"], "geometry"].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [687, 1024]

# doorspoeling (op uitlaten)
flushing_nodes = {}

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
    add_supply_nodes=True,
)

# %%

# Toevoegen BoezemNW

polygon = aanvoergebieden_df.loc[["BoezemNW"], "geometry"].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [671, 2246, 2247]

# doorspoeling (op uitlaten)
flushing_nodes = {}

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
    add_supply_nodes=True,
)

# %% add all remaining inlets/outlets

# # Flushing nodes
flushing_nodes = {}

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
)

# %%
# %% Junctionfy(!)
# model = junctionify(model)

# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
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

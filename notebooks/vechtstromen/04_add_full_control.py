# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static

from ribasim_nl import CloudStorage, Model

# execute model run
MODEL_EXEC: bool = True

# model settings
AUTHORITY: str = "Vechtstromen"
SHORT_NAME: str = "vechtstromen"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"


# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {626}


# %%
# Helpers


def update_nodes(model: Model, node_ids: list[int], node_type: str) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.update_node(node_id=node_id, node_type=node_type)


def remove_nodes(model: Model, node_ids: list[int]) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.remove_node(node_id, remove_links=True)


def set_static_values(static_df, node_values: dict[int, float], column: str) -> None:
    for node_id, value in node_values.items():
        static_df.loc[static_df.node_id == node_id, column] = value


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
    )


def run_model_and_control(model: Model, ribasim_toml, qlr_path):
    model.write(ribasim_toml)

    if MODEL_EXEC:
        model.run()
        Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path).run_all()
        return Model.read(ribasim_toml)

    return model


# %%
# Definieren paden en syncen met cloud

cloud = CloudStorage()

ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "aanvoergebieden.gpkg")

cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


# %%
# Read data

model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# Alle uitlaten en inlaten op 100 m3/s, geen cap verdeling. Dit wordt de max flow in model.
for static_df in [model.outlet.static.df, model.pump.static.df]:
    static_df["max_flow_rate"] = 100.0
    static_df["flow_rate"] = 100.0


# %%
# Linkrichting fixes
reverse_link_ids = [36, 1777]

for link_id in reverse_link_ids:
    model.reverse_link(link_id=link_id)

# model.redirect_link(368, to_node_id=2638)


# %%
# Node type fixes

# make outlet nodes
update_nodes(
    model,
    [],
    "Outlet",
)

# make manning nodes
update_nodes(model, [1359, 1166, 1136, 1137, 1168, 1169], "ManningResistance")

# make pump nodes
update_nodes(model, [], "Pump")

# Verwijderen nutteloze kunstwerken voor LHM
remove_nodes(model, [321, 392, 488, 1343, 1167])

# Verwijderen dubbele sluispompen. Capaciteit staat op de dichtstbijzijnde pomp per sluis.
remove_nodes(model, [])

# Streefpeil te laag
set_static_values(
    model.outlet.static.df,
    {},
    "min_upstream_level",
)

for node_id, to_node_id in [
    #   (1970, 1990),
]:
    model.merge_basins(node_id=node_id, to_node_id=to_node_id, are_connected=True)


# %%
# Max-capaciteiten pompen en sluizen

outlet_max_flow_rate_by_node_id = {
    #   454: 16.2,  # Paradijssluis
}

pump_max_flow_rate_by_node_id = {
    #   704: 6.7,  # Paradijssluis
}

set_max_flow_rate(model.outlet.static.df, outlet_max_flow_rate_by_node_id)
set_max_flow_rate(model.pump.static.df, pump_max_flow_rate_by_node_id)


# %%
# Kunstwerk-in/-uit ruimtelijk koppelen aan Ribasim-nodes


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


# %%
#
# #Toevoegen aanvoergebieden
# Handmatige indeling control-, supply- en drain-nodes

flow_control_nodes = [52]

supply_nodes = [501, 611, 726, 979, 1069, 720, 730, 846, 621, 334, 658, 432, 564, 547, 684, 648, 961]

drain_nodes = [
    490,
    240,
    134,
    1050,
    650,
    605,
    181,
    614,
    583,
    1051,
    429,
    699,
    637,
    36,
    829,
    99,
    283,
    303,
    106,
    734,
    882,
    303,
    432,
    106,
    79,
    154,
    771,
    859,
    162,
    379,
    864,
    215,
    273,
    741,
]


# Per gebied: links die intersecten die we kunnen negeren.

supply_area_ignore_links = {
    "Boven Regge": [868, 1072, 1257, 1258, 1259, 1260, 2508, 2640],
    "Regge": [
        404,
        405,
        429,
        473,
        474,
        512,
        612,
        622,
        623,
        624,
        800,
        868,
        1072,
        1156,
        1243,
        1249,
        1250,
        1938,
        2499,
        2508,
        2580,
        2582,
        2594,
        2641,
    ],
    "De Pollen": [436, 992, 2479],
    "Lateraalkanaal": [1058, 1113, 1156, 1252, 1394, 1396],
    "Bolscherbeek": [],
    "Vriezenveen": [1291],
    "Dooze": [133, 1236, 1783, 2411],
    "Schipbeek": [541],
    "Dinkel": [],
    "Overijsselsch Kanaal noord": [133, 1172, 2671],
    "Vecht": [17, 41, 42, 1046, 1047],
    "Coevorden-Zwinderen": [2109, 2398, 2425, 2436],
    "Geesbrug": [],
    "Braambergersloot": [71, 2109, 2777],
    "Nieuwe Drostendiep": [71, 255, 2362, 2404, 2623],
    "Schoonebeek": [],
    "Oranjekanaal": [2414, 2604],
    "Oosterwijk": [2633],
    "Oosterhesselen": [],
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
)


# %%
# EXCLUDE_NODES op 0 m3/s zetten

mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, ["flow_rate", "min_flow_rate", "max_flow_rate"]] = 0.0


# %%
# Junctionify(!)

model = junctionify(model)


# %%
# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=1)
model = run_model_and_control(model, ribasim_toml_dry, qlr_path)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model = run_model_and_control(model, ribasim_toml_wet, qlr_path)

# hoofd run
update_basin_static(model=model, precipitation_mm_per_day=1.5)
model = run_model_and_control(model, ribasim_toml, qlr_path)


# node 1122 (Dooze) moet een inlaat worden ipv Manning
model.update_node(node_id=1122, node_type="Outlet")
# fixes:

# Inlaatschuif Zwinderseveld #Oranjekanaal: Gemaal richting omgedraaid voor inlaat
# for link_id in [2822, 1337]:
#    model.reverse_link(link_id=link_id)

# Inlaat Stroomstukkendijk #736 richting omdraaien? anders geen aanvoer

# Vriezenveen Veenkamp #768, 2 richtingen?

# Inlaatschuif Zwinderseveld #250
for link_id in [1674, 549]:
    model.reverse_link(link_id=link_id)


model.remove_node(node_id=665, remove_links=True)

# 1353 = Drentse stuw
model.update_node(node_id=1353, node_type="Outlet")

model.remove_node(node_id=206, remove_links=True)  # Dooze, wordt later weer toegevoegd als inlaat


# %%Duikers voor nu op 0.1m3/s ! Nog verbeteren
node_ids = model.outlet.node.df[model.outlet.node.df["meta_object_type"] == "duikersifonhevel"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0.1


# %% fixes:

model.outlet.static.df.loc[model.outlet.static.df.node_id == 971, "max_flow_rate"] = 100.0
# remove vistrap Hancate
model.remove_node(305, remove_links=True)
# Sluis Koning Willem Allexander
model.outlet.static.df.loc[model.outlet.static.df.node_id == 40, "max_flow_rate"] = 0.0
# Geen flow anders veel te veel door Manning knoop
model.outlet.static.df.loc[model.outlet.static.df.node_id == 26, "min_upstream_level"] = 12.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 52, "max_downstream_level"] = 9.15
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1269, "max_downstream_level"] = 9.15
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1269, "min_upstream_level"] = 9.15
model.outlet.static.df.loc[model.outlet.static.df.node_id == 982, "max_downstream_level"] = 9.15
model.outlet.static.df.loc[model.outlet.static.df.node_id == 982, "min_upstream_level"] = 9.15
# %%


# %%
# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
# check_basin_level.add_check_basin_level(model=model)
model.pump.static.df["meta_func_afvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 0
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path).run_all()

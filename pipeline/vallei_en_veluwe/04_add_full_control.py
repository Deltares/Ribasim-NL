# %%

import math
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import geopandas as gpd
from ribasim import Node
from ribasim.nodes import level_boundary
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
    print_node_list_diff,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.run_model import write_steady_state_scenarios
from shapely.geometry import LineString
from shapely.ops import substring

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "ValleienVeluwe"
SHORT_NAME: str = "venv"
# MODEL_ID: str = "2025_5_0"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"
INLAATWERKEN_LAYERS_BY_NODE_TYPE = {
    "Pump": ["gemaal"],
    "Outlet": ["stuw", "sluis", "duiker"],
}
INLAATWERKEN_CODE_COLUMN = "meta_code_waterbeheerder"

# %%
# Definieren paden en syncen met cloud

cloud = CloudStorage()

ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "aanvoergebieden.gpkg")
inlaatwerken_gdb = cloud.joinpath(
    AUTHORITY,
    "verwerkt",
    "1_ontvangen_data",
    "20250425_HarmenVandeWerfhorst",
    "Inlaatwerken en gebieden.gdb",
)


# %%
# Read data

model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {n("KDU-16059"), n("KDU-16060"), n("KDU-21474"), n("KDU-19264")}


# %%
# Helpers


def remove_basin_with_level_boundaries(
    model: Model,
    basin_id: int,
    distance_from_connector: float = 30.0,
    remove_neighbor_node_ids: set[int] | None = None,
) -> list[int]:
    remove_neighbor_node_ids = remove_neighbor_node_ids or set()
    links_to_basin = model.link.df[(model.link.df.from_node_id == basin_id) | (model.link.df.to_node_id == basin_id)]

    if links_to_basin.empty:
        model.remove_node(basin_id, remove_links=True)
        model.remove_nodes(list(remove_neighbor_node_ids), remove_links=True)
        return []

    boundary_level = None
    area_df = model.basin.area.df
    state_df = model.basin.state.df
    if area_df is not None:
        streefpeil = area_df.loc[area_df.node_id == basin_id, "meta_streefpeil"].dropna()
        if not streefpeil.empty:
            boundary_level = float(streefpeil.iloc[0])
    if state_df is not None and boundary_level is None:
        state_level = state_df.loc[state_df.node_id == basin_id, "level"].dropna()
        if not state_level.empty:
            boundary_level = float(state_level.iloc[0])
    if boundary_level is None:
        boundary_level = 0.0

    boundary_data = [level_boundary.Static(level=[boundary_level])]
    link_ids_to_remove = links_to_basin.index.to_list()
    next_boundary_node_id = max(int(model.node.df.index.max()) + 1, 10000)
    boundary_connector_node_ids = []
    model.remove_links(link_ids_to_remove)
    # the names of the new boundaries refer to nodes by their key, see docs/dev/node-keys.qmd
    keys = model.node.df["meta_key"]

    for row in links_to_basin.itertuples():
        link_geometry = cast(LineString, row.geometry)
        distance = min(distance_from_connector, link_geometry.length / 2)

        if row.from_node_id == basin_id:
            connector_id = cast(int, row.to_node_id)
            if connector_id in remove_neighbor_node_ids:
                continue
            boundary_geometry = link_geometry.interpolate(link_geometry.length - distance)
            boundary_link_geometry = cast(
                LineString, substring(link_geometry, link_geometry.length - distance, link_geometry.length)
            )
            boundary_node = model.level_boundary.add(
                Node(
                    node_id=next_boundary_node_id,
                    geometry=boundary_geometry,
                    name=f"Boundary basin {keys[basin_id]} - {keys[connector_id]}",
                ),
                boundary_data,
            )
            next_boundary_node_id += 1
            boundary_connector_node_ids.append(connector_id)
            model.link.add(boundary_node, model.get_node(connector_id), geometry=boundary_link_geometry)
        else:
            connector_id = cast(int, row.from_node_id)
            if connector_id in remove_neighbor_node_ids:
                continue
            boundary_geometry = link_geometry.interpolate(distance)
            boundary_link_geometry = cast(LineString, substring(link_geometry, 0, distance))
            boundary_node = model.level_boundary.add(
                Node(
                    node_id=next_boundary_node_id,
                    geometry=boundary_geometry,
                    name=f"Boundary basin {keys[basin_id]} - {keys[connector_id]}",
                ),
                boundary_data,
            )
            next_boundary_node_id += 1
            boundary_connector_node_ids.append(connector_id)
            model.link.add(model.get_node(connector_id), boundary_node, geometry=boundary_link_geometry)

    model.remove_node(basin_id, remove_links=True)
    model.remove_nodes(list(remove_neighbor_node_ids), remove_links=True)

    return boundary_connector_node_ids


def set_static_values(static_df, node_values: Mapping[int, object], column: str) -> None:
    for node_id, value in node_values.items():
        static_df.loc[static_df.node_id == node_id, column] = value


def set_node_meta_values(node_df, node_values: Mapping[int, object], column: str) -> None:
    for node_id, value in node_values.items():
        if node_id in node_df.index:
            node_df.loc[node_id, column] = value


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
        flow_rate_aanvoer=20.0,
        max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
        flow_rate_afvoer=100.0,
        max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    )


# %%
# Linkrichting fixes
reverse_link_ids = [
    lk("up:KGM-93", "KST-1777"),
    lk("KST-1777", "up:KDU-19433"),
    lk("up:KGM-93", "KSL-24"),
    lk("KSL-24", "up:WL_14947___1"),
    lk("KGM-59", "up:ow:WL_105031"),
    lk("up:KDU-15134", "KGM-59"),
    lk("ow:WL_15541", "up:KDU-69898"),
    lk("up:KST-1219", "ow:WL_15541"),
    lk("up:KST-2117", "KST-2119"),
    lk("KST-2119", "up:KST-2118"),
]

for link_id in reverse_link_ids:
    model.reverse_link(link_id=link_id)

model.redirect_link(lk("KST-1428", "up:KST-813"), to_node_id=n("up:KST-1455"))

# Rijkswaterstaat Basin 1134 verwijderen en alle aangrenzende kunstwerken op een boundary zetten.
assert model.level_boundary.node.df.meta_couple_authority.notna().all(), (
    "Zorg dat eerst je LevelBoundaries overal meta_couple_authority hebben"
)
basin_1134_boundary_connector_node_ids = remove_basin_with_level_boundaries(
    model,
    basin_id=n("up:WL_14947___1"),
    distance_from_connector=30.0,
    remove_neighbor_node_ids={n("WL_14941")},
)

model.node.df.loc[
    model.node.df.meta_couple_authority.isna() & (model.node.df.node_type == "LevelBoundary"),
    "meta_couple_authority",
] = "Rijkswaterstaat"

# %%
# Node type fixes

# make outlet nodes
model.update_nodes(
    [n("ow:WL_15541")],
    "Outlet",
)

# make manning nodes
model.update_nodes([], "ManningResistance")

# make pump nodes
model.update_nodes([n("Mr. Baron van der Feltz")], "Pump")  # Aanvoergemaal De Wenden

# Verwijderen nutteloze kunstwerken voor LHM
model.remove_nodes([n("KST-2314"), n("KST-2312"), n("KST-2113"), n("KST-2115"), n("bnd:42")], remove_links=True)

# Verwijderen dubbele sluispompen. Capaciteit staat op de dichtstbijzijnde pomp per sluis.
model.remove_nodes([], remove_links=True)

# Streefpeil te laag
set_static_values(
    model.outlet.static.df,
    {
        #        959: 11.0
    },
    "min_upstream_level",
)

for node_id, to_node_id in [
    (n("up:KDU-3478"), n("up:KGM-11")),
]:
    model.merge_basins(node_id=node_id, to_node_id=to_node_id, are_connected=True)

# %%
# Max-capaciteiten pompen en sluizen
GEMALEN_URL = "https://www.gemalen.nl/gemaal_detail.asp?gem_id={gem_id}"

outlet_max_flow_rate_by_node_id = {
    n("KST-2307"): 2,  # Grebbesluis
    n("KST-1828"): 0.1,  # Schele Duiker
    n("ow:WL_15541"): 0.1,  # Kleine duiker naar groot basin
}
outlet_max_flow_rate_from_results = {
    n("KDU-8110"): 2,  # KDU-8110
    n("KST-4425"): 2,  # KST-4425
    n("KSL-23"): 2,  # Sluis de Laak ( Laakse Duiker )
    n("Inlaat de Wenden"): 2,  # Inlaat de Wenden
}
outlet_max_flow_rate_coupled_by_node_id = {
    n("KDU-4285"): 150,  # gekoppeld max=100.00, huidige max=0.00, link=4300038
    n("KDU-5871"): 77,  # gekoppeld max=50.37, huidige max=25.10, link=8000978
    n("ow:WL_1790"): 3,  # gekoppeld max=1.73, huidige max=0.00, link=8000949
}

pump_max_flow_rate_by_node_id = {
    n("KGM-78"): 1.167,  # Kleine Melm (70 m3/min)
    n("KGM-79"): 2.0,  # Zeldert (120 m3/min)
    n("KGM-23"): 3.267,  # J.G.W. Baron van Sytzama / Leuvenheim (196 m3/min)
    n("KGM-21"): 4.933,  # Mr. L.A.S.J Baron van der Feltz, Middelbeek-Voorsterbeek (296 m3/min)
    n("KGM-19"): 14.167,  # Mr. A.C. Baron van der Feltz (850 m3/min)
    #   241: 0.0,  # Laag Helbergen (0 m3/min)
    #   242: 0.0,  # Pabstendam (0 m3/min)
    n("KGM-92"): 2.9,  # Tydeman (174 m3/min)
    n("KGM-111"): 3.0,  # Putten / Puttergemaal (180 m3/min)
    n("KGM-91"): 2.5,  # Westdijk (150 m3/min)
    n("KGM-14"): 0.333,  # Avervoordseweg (20 m3/min)
    n("KGM-18"): 27.667,  # Veluwe (1660 m3/min)
    n("KGM-75"): 3.35,  # Malesluis / Coelhorst (201 m3/min)
    n("KGM-93"): 4.0,  # Eemnes (240 m3/min)
    n("KGM-135"): 4.933,  # Mr. L.A.S.J Baron van der Feltz, Middelbeek-Lage Leiding (296 m3/min)
    n("KGM-113"): 5.4,  # Nijkerk (324 m3/min)
    n("KGM-83"): 2.167,  # Veendijk (130 m3/min)
    n("KGM-139"): 0.5,  # Oranje Nassau's Oord (30 m3/min)
    n("KGM-16"): 5.833,  # Antlia (350 m3/min)
    n("KGM-22"): 3.0,  # F.C. Colenbrander (180 m3/min)
    n("KGM-81"): 1.733,  # De Haar (104 m3/min)
    n("KGM-3"): 8.0,  # De Wenden (480 m3/min)
    n("KGM-134"): 3.35,  # Malesluis / Coelhorst (201 m3/min)
    n("KGM-8"): 0.1,  # Inlaat Goorpomp
    n("KGM-98"): 1.667,  # Bolwerk (100 m3/min)
    #   474: 0.0,  # Maatpolder (0 m3/min)
    n("KGM-164"): 2.9,  # Doornbos (174 m3/min)
    n("KGM-17"): 0.667,  # Hoenwaard (40 m3/min)
    n("Outlet"): 0.5,  # Inlaatgemaal Maatpolder, handmatig
    n("Inlaatgemaal maatpolder"): 0.17,  # Gemaal Meentweg
    n("Gemaal Meentweg"): 1.0,  # Gemaal Oostsingel
    n("Gemaal Oostsingel"): 3.35,  # Inlaat Malesluis, match op meta_code_waterbeheerder (201 m3/min)
    n("Inlaatgemaal Nijbroek"): 0.667,  # Aanvoergemaal Hoenwaard, match op meta_code_waterbeheerder (40 m3/min)
    n("Aanvoergemaal Hoenwaard"): 5.833,  # Aanvoergemaal Antlia, match op meta_code_waterbeheerder (350 m3/min)
}

gemalen_url_by_node_id = {
    n("KGM-78"): GEMALEN_URL.format(gem_id=146),  # Kleine Melm
    n("KGM-79"): GEMALEN_URL.format(gem_id=930),  # Zeldert
    n("KGM-23"): GEMALEN_URL.format(gem_id=76),  # J.G.W. Baron van Sytzama / Leuvenheim
    n("KGM-21"): GEMALEN_URL.format(gem_id=6),  # Mr. L.A.S.J Baron van der Feltz
    n("KGM-19"): GEMALEN_URL.format(gem_id=5),  # Mr. A.C. Baron van der Feltz
    n("KGM-165"): GEMALEN_URL.format(gem_id=2228),  # Laag Helbergen
    n("KGM-172"): GEMALEN_URL.format(gem_id=7),  # Pabstendam
    n("KGM-92"): GEMALEN_URL.format(gem_id=542),  # Tydeman
    n("KGM-111"): GEMALEN_URL.format(gem_id=240),  # Putten
    n("KGM-91"): GEMALEN_URL.format(gem_id=940),  # Westdijk
    n("KGM-14"): GEMALEN_URL.format(gem_id=1499),  # Avervoordseweg
    n("KGM-18"): GEMALEN_URL.format(gem_id=1323),  # Veluwe
    n("KGM-75"): GEMALEN_URL.format(gem_id=495),  # Malesluis / Coelhorst
    n("KGM-93"): GEMALEN_URL.format(gem_id=950),  # Eemnes
    n("KGM-135"): GEMALEN_URL.format(gem_id=6),  # Mr. L.A.S.J Baron van der Feltz
    n("KGM-113"): GEMALEN_URL.format(gem_id=233),  # Nijkerk
    n("KGM-83"): GEMALEN_URL.format(gem_id=941),  # Veendijk
    n("KGM-139"): GEMALEN_URL.format(gem_id=1627),  # Oranje Nassau's Oord
    n("KGM-16"): GEMALEN_URL.format(gem_id=2028),  # Antlia
    n("KGM-22"): GEMALEN_URL.format(gem_id=75),  # F.C. Colenbrander
    n("KGM-81"): GEMALEN_URL.format(gem_id=932),  # De Haar
    n("KGM-3"): GEMALEN_URL.format(gem_id=238),  # De Wenden
    n("KGM-134"): GEMALEN_URL.format(gem_id=495),  # Malesluis / Coelhorst
    n("KGM-98"): GEMALEN_URL.format(gem_id=798),  # Bolwerk
    n("KGM-121"): GEMALEN_URL.format(gem_id=2478),  # Maatpolder
    n("KGM-164"): GEMALEN_URL.format(gem_id=2461),  # Doornbos
    n("KGM-17"): GEMALEN_URL.format(gem_id=151),  # Hoenwaard
    n("Outlet"): GEMALEN_URL.format(gem_id=2478),  # Inlaatgemaal Maatpolder
    n("Gemaal Oostsingel"): GEMALEN_URL.format(gem_id=495),  # Inlaat Malesluis
    n("Inlaatgemaal Nijbroek"): GEMALEN_URL.format(gem_id=151),  # Aanvoergemaal Hoenwaard
    n("Aanvoergemaal Hoenwaard"): GEMALEN_URL.format(gem_id=2028),  # Aanvoergemaal Antlia
}

set_max_flow_rate(model.outlet.static.df, outlet_max_flow_rate_by_node_id)
set_max_flow_rate(model.pump.static.df, pump_max_flow_rate_by_node_id)
set_node_meta_values(model.node.df, gemalen_url_by_node_id, "meta_url")
set_static_values(model.pump.static.df, gemalen_url_by_node_id, "meta_url")


# %%
# Handmatige indeling control-, supply- en drain-nodes
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [n("KDU-5351"), n("KDU-16519"), n("KST-174"), n("KST-753"), n("KST-74"), n("KST-1334"),
n("KST-68"), n("KST-59"), n("KST-2067"), n("KST-58"), n("KST-4629"), n("KST-3280"), n("KST-67"), n("KST-57"),
n("KST-56"), n("KDU-3478"), n("KDU-3850"), n("KST-2315"), n("KST-2311"), n("KST-421"), n("KST-2071"), n("KST-2313"),
n("KSL-3"), n("KST-296"), n("KSL-1"), n("KST-2316"), n("KST-358"), n("up:KST-2492"), n("up:KDU-2039"), n("Riethaere"),
n("Stuw Vloeddijk")]

supply_nodes = [n("KDU-4285"), n("KDU-18689"), n("KSL-10"), n("KDU-4691"), n("KDU-19264"), n("KDU-2325"), n("KDU-2652"),
n("KDU-16059"), n("KDU-2347"), n("KSL-24"), n("KST-665"), n("KST-4173"), n("KST-249"), n("KST-823"), n("KST-3524"),
n("KDU-1009"), n("KGM-98"), n("KST-1059"), n("KST-1731"), n("KST-3052"), n("KST-3926"), n("KST-4289"), n("KST-2118"),
n("KST-4167"), n("KST-4034"), n("KST-1084"), n("KST-296"), n("KDU-69898"), n("KDU-79377"), n("KDU-20889"),
n("ow:WL_15541"), n("Goorpomp"), n("Outlet"), n("Gemaal Meentweg"), n("Inlaat Malesluis"), n("Mr. Baron van der Feltz"),
n("Aanvoergemaal Emsterbroek"), n("Aanvoergemaal Papenkamp"), n("Inlaatgemaal Nijbroek"), n("Aanvoergemaal Hoenwaard"),
n("Aanvoergemaal Antlia"), n("Inlaat Nijoever"), n("Inlaat Luttenbroek")]

drain_nodes = [n("KDU-40934"), n("KDU-271"), n("KDU-2260"), n("KDU-45121"), n("KDU-5947"), n("KDU-30337"),
n("KDU-77303"), n("KDU-1024"), n("KDU-719"), n("KDU-17313"), n("KDU-15567"), n("KDU-76563"), n("KDU-76692"),
n("KDU-1738"), n("KDU-19355"), n("KDU-25756"), n("KDU-8110"), n("KDU-1500"), n("KDU-2636"), n("KDU-75161"),
n("KDU-8107"), n("KDU-1023"), n("KDU-1023"), n("KDU-76664"), n("KDU-1926"), n("KDU-8106"), n("KDU-8391"), n("KDU-8391"),
n("KDU-4933"), n("KDU-2295"), n("KDU-18818"), n("KDU-16925"), n("KDU-18796"), n("KDU-19433"), n("KDU-18602"),
n("KDU-16519"), n("KGM-10"), n("KGM-78"), n("KGM-79"), n("KGM-36"), n("KGM-28"), n("KGM-34"), n("KGM-126"), n("KGM-19"),
n("KGM-92"), n("KGM-35"), n("KGM-35"), n("KGM-127"), n("KGM-163"), n("KGM-14"), n("KGM-18"), n("KGM-75"), n("KGM-40"),
n("KGM-40"), n("KGM-132"), n("KGM-93"), n("KGM-101"), n("KSL-5"), n("KSL-22"), n("KST-229"), n("KST-2492"),
n("KST-1175"), n("KST-974"), n("KST-1200"), n("KST-1012"), n("KST-1488"), n("KST-842"), n("KST-1606"), n("KST-1010"),
n("KST-2114"), n("KST-4403"), n("KST-1208"), n("KST-114"), n("KST-298"), n("KST-55"), n("KST-2214"), n("KST-4168"),
n("KST-1428"), n("KST-73"), n("KST-1015"), n("KST-2067"), n("KST-4782"), n("KST-210"), n("KST-1011"), n("KST-4395"),
n("KST-53"), n("KST-4394"), n("KST-1559"), n("KST-671"), n("KST-1004"), n("KST-1079"),
n("stuw_valleikanaal_amersfoort"), n("KDU-17159"), n("KDU-1580"), n("KDU-78169"), n("KDU-2317"), n("KDU-1801"),
n("KDU-60071"), n("KGM-37"), n("KDU-1711"), n("KDU-874"), n("KDU-76045"), n("KDU-1812"), n("KGM-39"), n("KGM-81"),
n("KGM-3"), n("KGM-134"), n("KGM-8"), n("KGM-164"), n("KGM-42"), n("KGM-42"), n("KST-219"), n("KST-2091"),
n("KST-1059"), n("KST-1760"), n("KST-4181"), n("KST-4398"), n("KST-813"), n("KST-2090"), n("KST-48"), n("KST-3866"),
n("KST-1430"), n("KST-4289"), n("KST-1454"), n("KST-4167"), n("KST-782"), n("KST-782"), n("KST-3053"), n("KSL-19"),
n("KST-3162"), n("KDU-4883"), n("KST-804"), n("KGM-17"), n("KST-4662"), n("KST-4663"), n("KDU-76900"), n("KDU-15568"),
n("KST-4846"), n("KST-3976"), n("KDU-20889"), n("KDU-20036"), n("KST-4815"), n("KST-1316"), n("KST-1315"),
n("KST-1314"), n("KST-4950"), n("KST-4952"), n("KST-148"), n("KST-246"), n("KST-212"), n("KST-4848"), n("KST-206"),
n("KST-4176"), n("KST-4176"), n("up:KGM-8"), n("Gemaal Oostsingel"), n("Vlieterweg")]
# fmt: on


# %%
# Inlaatwerken uit de GDB koppelen aan Ribasim-nodes


manual_flow_control_nodes = list(flow_control_nodes)
manual_supply_nodes = list(supply_nodes)
manual_drain_nodes = list(drain_nodes)


def get_node_df(model: Model, node_type: str):
    if node_type == "Outlet":
        return model.outlet.node.df
    if node_type == "Pump":
        return model.pump.node.df

    raise ValueError(f"Onbekend node_type: {node_type}")


def get_supply_nodes_from_gdb(
    model: Model,
    inlaatwerken_path: Path,
    layers_by_node_type: dict[str, list[str]] = INLAATWERKEN_LAYERS_BY_NODE_TYPE,
) -> list[int]:
    supply_node_ids = []

    for node_type, layers in layers_by_node_type.items():
        node_df = get_node_df(model, node_type).copy()
        if INLAATWERKEN_CODE_COLUMN not in node_df.columns:
            print(
                f"inlaatwerken {node_type}: geen matches, "
                f"want model.{node_type.lower()}.node.df heeft geen kolom '{INLAATWERKEN_CODE_COLUMN}'"
            )
            continue

        node_df[INLAATWERKEN_CODE_COLUMN] = node_df[INLAATWERKEN_CODE_COLUMN].astype("string").str.strip()

        for layer in layers:
            inlaatwerk_gdf = gpd.read_file(inlaatwerken_path, layer=layer)
            if "CODE" not in inlaatwerk_gdf.columns:
                print(f"inlaatwerken {layer}: geen matches, want de GDB-laag heeft geen kolom 'CODE'")
                continue

            inlaatwerk_codes = set(inlaatwerk_gdf["CODE"].dropna().astype(str).str.strip())
            matched_node_ids = (
                node_df[node_df[INLAATWERKEN_CODE_COLUMN].isin(inlaatwerk_codes)].index.astype(int).to_list()
            )
            matched_codes = set(node_df.loc[matched_node_ids, INLAATWERKEN_CODE_COLUMN].dropna().astype(str))
            unmatched_codes = sorted(inlaatwerk_codes - matched_codes)

            if not matched_node_ids:
                print(
                    f"inlaatwerken {layer}: geen {node_type}-nodes gevonden met "
                    f"CODE == model.{node_type.lower()}.node.df.{INLAATWERKEN_CODE_COLUMN}"
                )
            else:
                print(
                    f"inlaatwerken {layer}: {len(matched_node_ids)} {node_type}-nodes gevonden op "
                    f"CODE/{INLAATWERKEN_CODE_COLUMN}, {len(unmatched_codes)} codes zonder match"
                )
            if unmatched_codes:
                print(f"inlaatwerken {layer} codes zonder match: {unmatched_codes}")

            supply_node_ids.extend(matched_node_ids)

    return list(dict.fromkeys(supply_node_ids))


def print_manual_role_conflicts(
    inlaatwerk_supply_nodes: list[int],
    manual_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> None:
    inlaatwerk_supply_node_set = set(inlaatwerk_supply_nodes)
    print(
        "inlaatwerken genegeerd door handmatige indeling: "
        f"flow_control={sorted(inlaatwerk_supply_node_set & set(manual_flow_control_nodes))}, "
        f"drain={sorted(inlaatwerk_supply_node_set & set(manual_drain_nodes))}, "
        f"supply={sorted(inlaatwerk_supply_node_set & set(manual_supply_nodes))}"
    )


def combine_control_node_roles(
    inlaatwerk_supply_nodes: list[int],
    manual_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> tuple[list[int], list[int], list[int]]:
    inlaatwerk_supply_nodes = list(dict.fromkeys(inlaatwerk_supply_nodes))
    # Handmatige rollen hebben voorrang; GDB-inlaten vullen alleen nog niet-ingedeelde nodes aan.
    manual_role_node_set = set(manual_supply_nodes) | set(manual_drain_nodes) | set(manual_flow_control_nodes)
    inlaatwerk_supply_nodes = [node_id for node_id in inlaatwerk_supply_nodes if node_id not in manual_role_node_set]

    flow_control_nodes = list(dict.fromkeys(manual_flow_control_nodes))
    supply_nodes = list(
        dict.fromkeys(
            [node_id for node_id in manual_supply_nodes if node_id not in flow_control_nodes] + inlaatwerk_supply_nodes
        )
    )
    drain_nodes = list(
        dict.fromkeys(
            node_id
            for node_id in manual_drain_nodes
            if node_id not in flow_control_nodes and node_id not in supply_nodes
        )
    )

    return supply_nodes, drain_nodes, flow_control_nodes


inlaatwerk_supply_nodes = get_supply_nodes_from_gdb(model=model, inlaatwerken_path=inlaatwerken_gdb)

print_manual_role_conflicts(
    inlaatwerk_supply_nodes=inlaatwerk_supply_nodes,
    manual_supply_nodes=manual_supply_nodes,
    manual_drain_nodes=manual_drain_nodes,
    manual_flow_control_nodes=manual_flow_control_nodes,
)

supply_nodes, drain_nodes, flow_control_nodes = combine_control_node_roles(
    inlaatwerk_supply_nodes=inlaatwerk_supply_nodes,
    manual_supply_nodes=manual_supply_nodes,
    manual_drain_nodes=manual_drain_nodes,
    manual_flow_control_nodes=manual_flow_control_nodes,
)

assert model.outlet.static.df is not None
supply_outlet_mask = model.outlet.static.df.node_id.isin(supply_nodes) & ~model.outlet.static.df.node_id.isin(
    outlet_max_flow_rate_by_node_id
)
model.outlet.static.df.loc[supply_outlet_mask, ["flow_rate", "max_flow_rate"]] = 1.0
outlet_max_flow_rate_by_node_id.update(
    dict.fromkeys(model.outlet.static.df.loc[supply_outlet_mask, "node_id"].astype(int), 1.0)
)
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
outlet_max_flow_rate_afvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)
outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

# Handmatige inlaatcapaciteiten gelden ook in aanvoer; niet terugvallen op de default van 10 m3/s.
outlet_max_flow_rate_aanvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)

boundary_supply_node_ids = set(basin_1134_boundary_connector_node_ids) & set(supply_nodes)
for static_df in [model.outlet.static.df, model.pump.static.df]:
    assert static_df is not None
    mask = static_df.node_id.isin(boundary_supply_node_ids)
    static_df.loc[mask, "min_upstream_level"] = -math.inf
    mark_level_update_protected(static_df, mask, model=model)

print_node_list_diff("supply_nodes", manual_supply_nodes, supply_nodes)
print_node_list_diff("drain_nodes", manual_drain_nodes, drain_nodes)


# %%
# Toevoegen aanvoergebieden
# Per gebied: links die intersecten die we kunnen negeren.

supply_area_ignore_links = {
    "Arkemheem": [lk("up:ow:WL_114600", "ow:WL_114600")],
    "Eemland": [],
    "Gelderse vallei": [
        lk("WL_20022___2", "up:WL_17840"),
        lk("WL_105176", "up:WL_17840"),
        lk("WL_17858___3", "up:KST-2118"),
        lk("up:KDU-15520", "WL_105176"),
        lk("up:KDU-16925", "ow:WL_113271"),
        lk("ow:WL_15541", "up:KST-1219"),
        lk("up:WL_20022___2", "WL_20022___2"),
        lk("up:WL_17858___3", "WL_17858___3"),
    ],
    "IJsselvallei": [
        lk("ow:WL_2756", "up:KSL-5"),
        lk("ow:WL_2728", "up:KSL-5"),
        lk("ow:WL_3150", "up:KSL-5"),
        lk("ow:WL_1401", "up:KDU-2295"),
        lk("up:ow:WL_1401", "ow:WL_1401"),
        lk("up:ow:WL_3219", "ow:WL_3219"),
        lk("up:WL_2870___0", "WL_2870___0"),
    ],
    "Noordoost Veluwe": [],
    "Noordwest Veluwe": [lk("WL_114529", "up:KDU-5351")],
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

# Valleikanaal
flushing_nodes = {n("stuw_valleikanaal_amersfoort"): 1}

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

# %%
# EXCLUDE_NODES zonder supply-rol op 0 m3/s zetten

mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, ["flow_rate", "min_flow_rate", "max_flow_rate"]] = 0.0

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
# Bestaande handmatige pompwaarden blijven leidend.
pump_max_flow_rate_from_results = {
    n("KGM-127"): 1,  # Gemaal Bestevaer; oude static flow_rate=0.049
    n("KGM-25"): 50,  # gekoppeld max=50.00, huidige max=0.0562, link=8000977
    n("KGM-4"): 1,  # Dieren; oude static flow_rate=0
    n("KGM-24"): 50,  # gekoppeld max=50.00, huidige max=0.0562, link=4300240
    n("Inlaat Malesluis"): 2,  # gekoppeld max=1.80, huidige max=0.00, link=4301400
    n("Aanvoergemaal Antlia"): 1,  # naam onbekend; oude static flow_rate=0
}
assert model.pump.static.df is not None
mask = (
    model.pump.static.df.node_id.isin(pump_max_flow_rate_from_results)
    & model.pump.static.df.flow_rate.notna()
    & (model.pump.static.df.flow_rate > 0)
)
model.pump.static.df.loc[mask, "max_flow_rate"] = model.pump.static.df.loc[mask, "node_id"].map(
    pump_max_flow_rate_from_results
)

# %%
# Junctionify(!)

model = junctionify(model)

aanvoer_only_node_ids = set(supply_nodes) - set(drain_nodes) - set(flow_control_nodes)

# Aanvoer-cap: doorlaten/inlaten mogen in aanvoer niet de hoge afvoercapaciteit gebruiken.
assert model.outlet.static.df is not None
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
    assert static_df is not None
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
    assert static_df is not None
    afvoer_mask = (
        static_df["control_state"].eq("afvoer")
        & static_df["flow_rate"].fillna(0).gt(0)
        & ~static_df["node_id"].isin(protected_max_flow_rate_node_ids)
    )
    static_df.loc[afvoer_mask, "max_flow_rate"] = static_df.loc[afvoer_mask, "max_flow_rate"].fillna(10).clip(lower=10)

for static_df in (model.outlet.static.df, model.pump.static.df):
    assert static_df is not None
    afvoer_mask = static_df["control_state"].eq("afvoer") & static_df["node_id"].isin(aanvoer_only_node_ids)
    static_df.loc[afvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

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

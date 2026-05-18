# %%

from pathlib import Path

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim import Node
from ribasim.nodes import level_boundary
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely.ops import substring

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = True  # execute model run
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

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {201, 593, 67, 137, 232}


# %%
# Helpers


def update_nodes(model: Model, node_ids: list[int], node_type: str) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.update_node(node_id=node_id, node_type=node_type)


def remove_nodes(model: Model, node_ids: list[int]) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.remove_node(node_id, remove_links=True)


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
        remove_nodes(model, list(remove_neighbor_node_ids))
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

    for row in links_to_basin.itertuples():
        link_geometry = row.geometry
        distance = min(distance_from_connector, link_geometry.length / 2)

        if row.from_node_id == basin_id:
            connector_id = row.to_node_id
            if connector_id in remove_neighbor_node_ids:
                continue
            boundary_geometry = link_geometry.interpolate(link_geometry.length - distance)
            boundary_link_geometry = substring(link_geometry, link_geometry.length - distance, link_geometry.length)
            boundary_node = model.level_boundary.add(
                Node(
                    node_id=next_boundary_node_id,
                    geometry=boundary_geometry,
                    name=f"Boundary basin {basin_id} - {connector_id}",
                ),
                boundary_data,
            )
            next_boundary_node_id += 1
            boundary_connector_node_ids.append(connector_id)
            model.link.add(boundary_node, model.get_node(connector_id), geometry=boundary_link_geometry)
        else:
            connector_id = row.from_node_id
            if connector_id in remove_neighbor_node_ids:
                continue
            boundary_geometry = link_geometry.interpolate(distance)
            boundary_link_geometry = substring(link_geometry, 0, distance)
            boundary_node = model.level_boundary.add(
                Node(
                    node_id=next_boundary_node_id,
                    geometry=boundary_geometry,
                    name=f"Boundary basin {basin_id} - {connector_id}",
                ),
                boundary_data,
            )
            next_boundary_node_id += 1
            boundary_connector_node_ids.append(connector_id)
            model.link.add(model.get_node(connector_id), boundary_node, geometry=boundary_link_geometry)

    model.remove_node(basin_id, remove_links=True)
    remove_nodes(model, list(remove_neighbor_node_ids))

    return boundary_connector_node_ids


def set_static_values(static_df, node_values: dict[int, object], column: str) -> None:
    for node_id, value in node_values.items():
        static_df.loc[static_df.node_id == node_id, column] = value


def set_node_meta_values(node_df, node_values: dict[int, object], column: str) -> None:
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
    static_df["max_flow_rate"] = 30.0
    static_df["flow_rate"] = 30.0


# %%
# Linkrichting fixes
reverse_link_ids = [244, 875, 926, 422, 351, 853, 605, 1018, 378]


for link_id in reverse_link_ids:
    model.reverse_link(link_id=link_id)

model.redirect_link(278, to_node_id=936)

# Basin 1134 verwijderen en alle aangrenzende kunstwerken op een boundary zetten.
basin_1134_boundary_connector_node_ids = remove_basin_with_level_boundaries(
    model,
    basin_id=1134,
    distance_from_connector=30.0,
    remove_neighbor_node_ids={691},
)


# %%
# Node type fixes

# make outlet nodes
update_nodes(
    model,
    [646],
    "Outlet",
)

# make manning nodes
update_nodes(model, [], "ManningResistance")

# make pump nodes
update_nodes(model, [1287], "Pump")  # Aanvoergemaal De Wenden

# Verwijderen nutteloze kunstwerken voor LHM
remove_nodes(model, [484, 490, 501, 525, 42])

# Verwijderen dubbele sluispompen. Capaciteit staat op de dichtstbijzijnde pomp per sluis.
remove_nodes(model, [])

# Streefpeil te laag
set_static_values(
    model.outlet.static.df,
    {
        #        959: 11.0
    },
    "min_upstream_level",
)

for node_id, to_node_id in [
    #    (844, 936),
]:
    model.merge_basins(node_id=node_id, to_node_id=to_node_id, are_connected=True)

# %%
# Max-capaciteiten pompen en sluizen
GEMALEN_URL = "https://www.gemalen.nl/gemaal_detail.asp?gem_id={gem_id}"

outlet_max_flow_rate_by_node_id = {
    288: 2,  # Grebbesluis
    400: 0.1,  # Schele Duiker
    646: 0.1,  # Kleine duiker naar groot basin
}

pump_max_flow_rate_by_node_id = {
    229: 1.167,  # Kleine Melm (70 m3/min)
    231: 2.0,  # Zeldert (120 m3/min)
    234: 3.267,  # J.G.W. Baron van Sytzama / Leuvenheim (196 m3/min)
    239: 4.933,  # Mr. L.A.S.J Baron van der Feltz, Middelbeek-Voorsterbeek (296 m3/min)
    240: 14.167,  # Mr. A.C. Baron van der Feltz (850 m3/min)
    #   241: 0.0,  # Laag Helbergen (0 m3/min)
    #   242: 0.0,  # Pabstendam (0 m3/min)
    243: 2.9,  # Tydeman (174 m3/min)
    250: 3.0,  # Putten / Puttergemaal (180 m3/min)
    254: 2.5,  # Westdijk (150 m3/min)
    255: 0.333,  # Avervoordseweg (20 m3/min)
    257: 27.667,  # Veluwe (1660 m3/min)
    258: 3.35,  # Malesluis / Coelhorst (201 m3/min)
    264: 4.0,  # Eemnes (240 m3/min)
    266: 4.933,  # Mr. L.A.S.J Baron van der Feltz, Middelbeek-Lage Leiding (296 m3/min)
    267: 5.4,  # Nijkerk (324 m3/min)
    268: 2.167,  # Veendijk (130 m3/min)
    457: 0.5,  # Oranje Nassau's Oord (30 m3/min)
    459: 5.833,  # Antlia (350 m3/min)
    460: 3.0,  # F.C. Colenbrander (180 m3/min)
    466: 1.733,  # De Haar (104 m3/min)
    468: 8.0,  # De Wenden (480 m3/min)
    469: 3.35,  # Malesluis / Coelhorst (201 m3/min)
    470: 0.1,  # Inlaat Goorpomp
    471: 1.667,  # Bolwerk (100 m3/min)
    #   474: 0.0,  # Maatpolder (0 m3/min)
    475: 2.9,  # Doornbos (174 m3/min)
    573: 0.667,  # Hoenwaard (40 m3/min)
    1282: 0.5,  # Inlaatgemaal Maatpolder, handmatig
    1283: 0.17,  # Gemaal Meentweg
    1284: 1.0,  # Gemaal Oostsingel
    1285: 3.35,  # Inlaat Malesluis, match op meta_code_waterbeheerder (201 m3/min)
    1292: 0.667,  # Aanvoergemaal Hoenwaard, match op meta_code_waterbeheerder (40 m3/min)
    1293: 5.833,  # Aanvoergemaal Antlia, match op meta_code_waterbeheerder (350 m3/min)
}

gemalen_url_by_node_id = {
    229: GEMALEN_URL.format(gem_id=146),  # Kleine Melm
    231: GEMALEN_URL.format(gem_id=930),  # Zeldert
    234: GEMALEN_URL.format(gem_id=76),  # J.G.W. Baron van Sytzama / Leuvenheim
    239: GEMALEN_URL.format(gem_id=6),  # Mr. L.A.S.J Baron van der Feltz
    240: GEMALEN_URL.format(gem_id=5),  # Mr. A.C. Baron van der Feltz
    241: GEMALEN_URL.format(gem_id=2228),  # Laag Helbergen
    242: GEMALEN_URL.format(gem_id=7),  # Pabstendam
    243: GEMALEN_URL.format(gem_id=542),  # Tydeman
    250: GEMALEN_URL.format(gem_id=240),  # Putten
    254: GEMALEN_URL.format(gem_id=940),  # Westdijk
    255: GEMALEN_URL.format(gem_id=1499),  # Avervoordseweg
    257: GEMALEN_URL.format(gem_id=1323),  # Veluwe
    258: GEMALEN_URL.format(gem_id=495),  # Malesluis / Coelhorst
    264: GEMALEN_URL.format(gem_id=950),  # Eemnes
    266: GEMALEN_URL.format(gem_id=6),  # Mr. L.A.S.J Baron van der Feltz
    267: GEMALEN_URL.format(gem_id=233),  # Nijkerk
    268: GEMALEN_URL.format(gem_id=941),  # Veendijk
    457: GEMALEN_URL.format(gem_id=1627),  # Oranje Nassau's Oord
    459: GEMALEN_URL.format(gem_id=2028),  # Antlia
    460: GEMALEN_URL.format(gem_id=75),  # F.C. Colenbrander
    466: GEMALEN_URL.format(gem_id=932),  # De Haar
    468: GEMALEN_URL.format(gem_id=238),  # De Wenden
    469: GEMALEN_URL.format(gem_id=495),  # Malesluis / Coelhorst
    471: GEMALEN_URL.format(gem_id=798),  # Bolwerk
    474: GEMALEN_URL.format(gem_id=2478),  # Maatpolder
    475: GEMALEN_URL.format(gem_id=2461),  # Doornbos
    573: GEMALEN_URL.format(gem_id=151),  # Hoenwaard
    1282: GEMALEN_URL.format(gem_id=2478),  # Inlaatgemaal Maatpolder
    1285: GEMALEN_URL.format(gem_id=495),  # Inlaat Malesluis
    1292: GEMALEN_URL.format(gem_id=151),  # Aanvoergemaal Hoenwaard
    1293: GEMALEN_URL.format(gem_id=2028),  # Aanvoergemaal Antlia
}

set_max_flow_rate(model.outlet.static.df, outlet_max_flow_rate_by_node_id)
set_max_flow_rate(model.pump.static.df, pump_max_flow_rate_by_node_id)
set_node_meta_values(model.node.df, gemalen_url_by_node_id, "meta_url")
set_static_values(model.pump.static.df, gemalen_url_by_node_id, "meta_url")


# %%
# Handmatige indeling control-, supply- en drain-nodes
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [344,276,224,373,1279,574,632,434,387,533,85,393,1288,374,403,306,404,273, 526,482,436, 538, 760, 859, 1018, 1266, 1306,311,386,551,580,1691,578,570

]

supply_nodes = [302,492,220,508,413,1285,604,269,646,339,1280,1289,1295,503,164,145,1290,1903,1286,332,536,1291,312,574,599,91,530,120,137,201,282,341,352,506,512,1283,1294,1292,555,545,471,491,528,588,226

]

drain_nodes = [405,232,409,258,229,142,248,547,621,287,337,385,336,505,124,629,279,382,481,470,282,187,640,283,643,456,432,444,438,260,476,245,259,435,235,237,462,415,518,643,265,236,633,468,376,477,1057,228,175,616,564,1284,272,625,70,107,191,1912,429,623,624,245,476,240,179,354,181,392,351,257,475,87,358,498,161,198,255,369,350,93,583,584,172,75,259,179,90,173,270,573,571,572,208,326,455,524,592,638,1492,357,116,188,620,304,396,607,188,99,527,537,238,224,412,469,537,562,223,84,373,334,68,210,591,101,489,516,166,314,178,79,536,406,383,330,309,371,619,76,180,216,220,231,243,264,320,491,492,402,408,422,604,252,528,534,596,433,509,589,361,1297

]
# fmt: on


# %%
# Inlaatwerken uit de GDB koppelen aan Ribasim-nodes

inlaatwerken_gdb = Path(
    r"d:\Projecten\D2306.LHM_RIBASIM\02.brongegevens\ValleienVeluwe\verwerkt"
    r"\1_ontvangen_data\20250425_HarmenVandeWerfhorst\Inlaatwerken en gebieden.gdb"
)

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


def print_node_list_diff(label: str, before_nodes: list[int], after_nodes: list[int]) -> None:
    before_nodes = set(before_nodes)
    after_nodes = set(after_nodes)

    added_nodes = sorted(after_nodes - before_nodes)
    removed_nodes = sorted(before_nodes - after_nodes)

    print(f"{label}: {len(added_nodes)} toegevoegd, {len(removed_nodes)} verwijderd t.o.v. handmatig")
    print(f"{label} toegevoegd: {added_nodes}")
    print(f"{label} verwijderd: {removed_nodes}")


def print_manual_role_conflicts(
    inlaatwerk_supply_nodes: list[int],
    manual_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> None:
    inlaatwerk_supply_nodes = set(inlaatwerk_supply_nodes)
    manual_supply_nodes = set(manual_supply_nodes)
    manual_drain_nodes = set(manual_drain_nodes)
    manual_flow_control_nodes = set(manual_flow_control_nodes)

    print(
        "inlaatwerken genegeerd door handmatige indeling: "
        f"flow_control={sorted(inlaatwerk_supply_nodes & manual_flow_control_nodes)}, "
        f"drain={sorted(inlaatwerk_supply_nodes & manual_drain_nodes)}, "
        f"supply={sorted(inlaatwerk_supply_nodes & manual_supply_nodes)}"
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

supply_outlet_mask = model.outlet.static.df.node_id.isin(supply_nodes) & ~model.outlet.static.df.node_id.isin(
    outlet_max_flow_rate_by_node_id
)
model.outlet.static.df.loc[supply_outlet_mask, ["flow_rate", "max_flow_rate"]] = 1.0

boundary_supply_node_ids = set(basin_1134_boundary_connector_node_ids) & set(supply_nodes)
for static_df in [model.outlet.static.df, model.pump.static.df]:
    mask = static_df.node_id.isin(boundary_supply_node_ids)
    static_df.loc[mask, "min_upstream_level"] = pd.NA

print_node_list_diff("supply_nodes", manual_supply_nodes, supply_nodes)
print_node_list_diff("drain_nodes", manual_drain_nodes, drain_nodes)


# %%
# Toevoegen aanvoergebieden
# Per gebied: links die intersecten die we kunnen negeren.

supply_area_ignore_links = {
    "Arkemheem": [1293],
    "Eemland": [],
    "Gelderse vallei": [435, 436, 538, 760, 859, 1018, 1266, 1306],
    "IJsselvallei": [418, 419, 420, 610, 1254, 1296, 1298],
    "Noordoost Veluwe": [],
    "Noordwest Veluwe": [547],
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
flushing_nodes = {409: 1}

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
)


# %%
# EXCLUDE_NODES zonder supply-rol op 0 m3/s zetten

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

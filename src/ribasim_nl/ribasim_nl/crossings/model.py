"""Convert the crossings of a level controlled water authority into its input model, `<authority>_boezemmodel_<version>`.

A port of `crossings_to_ribasim` of `peilbeheerst_model` by HKV (Ron Bruijns), in the version of December 2024 that made
the input models, and of May 2025 for Wetterskip Fryslân. The peilgebieden, or their aggregation areas, become Basins, and
the crossings between them connector nodes. The node IDs follow from the order of the crossings, so the port keeps the
order of all operations, and the tables of the Ribasim version of then, ribasim 2024.11, until the model is written.
Some joins align rows by their index rather than by node ID; the port keeps them as they were, since the metadata of the
input models follows from them.
"""

import itertools
from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
import shapely
from bokeh.palettes import Category10
from pandera.typing.geopandas import GeoDataFrame
from ribasim.geometry.link import LinkSchema
from ribasim.geometry.node import NodeSchema
from shapely.geometry import LineString, MultiPolygon, Point, Polygon

from ribasim_nl.model import Model

CRS = "EPSG:28992"

# the layers of the postprocessed data that are read, in December 2024
POSTPROCESSED_LAYERS = ["hydroobject", "gemaal", "stuw", "peilgebied", "streefpeil", "duikersifonhevel"]

# the sort keys of ribasim 2024.11, with which the tables were written
SORT_KEYS = {
    "Basin / state": ["node_id"],
    "Basin / static": ["node_id"],
    "Basin / profile": ["node_id", "level"],
    "Basin / area": ["node_id"],
    "TabulatedRatingCurve / static": ["node_id", "control_state", "level"],
    "Pump / static": ["node_id", "control_state"],
    "ManningResistance / static": ["node_id", "control_state"],
    "LevelBoundary / static": ["node_id"],
    "FlowBoundary / static": ["node_id"],
}

# the node types that are written, in the order of ribasim 2024.11
NODE_TYPES = ["Basin", "Pump", "TabulatedRatingCurve", "ManningResistance", "LevelBoundary", "FlowBoundary", "Terminal"]


@dataclass(frozen=True)
class Options:
    """The differences between the input models of the water authorities.

    :param link_changes: link geometries to replace, as (from, to) of the link to change and (from, to) of the link with
        the geometry to use, the manual changes of Hollands Noorderkwartier
    :param refill_streefpeil: fill the missing streefpeilen of the crossings from other crossings of the same
        peilgebied, for Schieland en de Krimpenerwaard
    :param all_layers: read all layers of the postprocessed data, as in May 2025, rather than `POSTPROCESSED_LAYERS`
    :param unknown_pump_function: the function of pumps without one, aanvoer in December 2024, afvoer in May 2025
    :param add_names: name the pumps after their gemaal, as in May 2025
    :param endtime: the end time of the model
    """

    link_changes: tuple[tuple[tuple[int, int], tuple[int, int]], ...] = ()
    refill_streefpeil: bool = False
    all_layers: bool = False
    unknown_pump_function: str = "aanvoer"
    add_names: bool = False
    endtime: str = "2024-01-02"


@dataclass
class Tables:
    """The tables of a model as ribasim 2024.11 kept them in memory, with the index they were given."""

    link: gpd.GeoDataFrame
    nodes: dict[str, pd.DataFrame] = field(default_factory=dict)
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)


def to_object[T: pd.DataFrame](df: T) -> T:
    """Convert the string columns to object, with None for missing values, as pandas 2 read them.

    The 2024 code assigns numbers to columns of strings, and selects missing values with `is None`.
    """
    for column in df.columns:
        if pd.api.types.is_string_dtype(df[column]) and df[column].dtype != object:
            values = df[column].astype(object)
            df[column] = values.where(values.notna(), None)
    return df


def read_layer(path: Path, layer: str) -> gpd.GeoDataFrame:
    """Read a layer, with string columns as object."""
    return to_object(gpd.read_file(path, layer=layer))


def read_postprocessed(path: Path, all_layers: bool = False) -> dict[str, gpd.GeoDataFrame]:
    """The layers of the postprocessed data that the conversion uses."""
    layers = list(pyogrio.list_layers(path)[:, 0]) if all_layers else POSTPROCESSED_LAYERS
    return {layer: read_layer(path, layer) for layer in layers}


def read_crossings(path: Path) -> gpd.GeoDataFrame:
    """The crossings in use after the aggregation of links and areas, as 2D points."""
    crossings = read_layer(path, "crossings_hydroobject_filtered")
    for column in ["in_use", "agg_links_in_use", "agg_areas_in_use"]:
        crossings = crossings[crossings[column].astype(bool)].reset_index(drop=True)
    crossings["geometry"] = gpd.GeoSeries(
        gpd.points_from_xy(x=crossings.geometry.x, y=crossings.geometry.y), index=crossings.index, crs=crossings.crs
    )
    return crossings


def routing_processor(
    post_processed_data: dict[str, gpd.GeoDataFrame], crossings: gpd.GeoDataFrame
) -> tuple[dict[str, gpd.GeoDataFrame], gpd.GeoDataFrame]:
    """Replace the peilgebieden of the crossings by their aggregation areas, and add the location of the basins.

    A basin is located at the representative point of its aggregation area, or else of its peilgebied.
    """
    crossings["peilgebied_from_original"] = crossings["peilgebied_from"]
    crossings["peilgebied_to_original"] = crossings["peilgebied_to"]

    # peilgebieden without an aggregation area keep their own
    crossings["agg_area_from"] = crossings["agg_area_from"].where(
        crossings["agg_area_from"].notna(), crossings["peilgebied_from"]
    )
    crossings["agg_area_to"] = crossings["agg_area_to"].where(
        crossings["agg_area_to"].notna(), crossings["peilgebied_to"]
    )
    crossings["peilgebied_from"] = crossings["agg_area_from"]
    crossings["peilgebied_to"] = crossings["agg_area_to"]

    aggregation_areas = post_processed_data["aggregation_areas"].rename(columns={"CODE": "code"})
    aggregation_areas["globalid"] = aggregation_areas["code"]
    post_processed_data["aggregation_areas"] = aggregation_areas
    peilgebied = pd.concat(
        [
            aggregation_areas[["code", "globalid", "geometry"]],
            post_processed_data["peilgebied"][["code", "globalid", "peilgebied_cat", "geometry"]],
        ]
    )
    peilgebied = gpd.GeoDataFrame(peilgebied, geometry="geometry")
    peilgebied["centroid_geometry"] = peilgebied.representative_point()
    post_processed_data["peilgebied"] = peilgebied

    assert peilgebied["globalid"].is_unique, "aggregation areas and peilgebieden share an ID"
    lookup = peilgebied.set_index("globalid")
    for side in ["from", "to"]:
        keys = crossings[f"peilgebied_{side}"]
        crossings[f"{side}_centroid_geometry"] = keys.map(lookup["centroid_geometry"]).where(keys.notna(), None)
        crossings[f"{side}_polygon_geometry"] = keys.map(lookup["geometry"]).where(keys.notna(), None)
    return post_processed_data, crossings


def assign_node_ids(crossings: gpd.GeoDataFrame, refill_streefpeil: bool = False) -> gpd.GeoDataFrame:
    """Number the basins in order of first appearance of their area, and the crossings after them, in their order.

    The peilgebieden that are a number keep it, the others are numbered after the largest number.
    """
    letters = crossings["peilgebied_from"].str.contains("[a-zA-Z_]", na=False)
    letters_to = crossings["peilgebied_to"].str.contains("[a-zA-Z_]", na=False)
    letters_df = pd.concat([crossings.loc[letters, "peilgebied_from"], crossings.loc[letters_to, "peilgebied_to"]])
    letters_df = letters_df.unique()

    numbers_df = pd.concat([crossings.loc[~letters, "peilgebied_from"], crossings.loc[~letters_to, "peilgebied_to"]])
    numbers_df = numbers_df.unique()
    max_number = max(0, max(numbers_df.astype(int))) if len(numbers_df) > 0 and pd.notna(numbers_df[0]) else 0

    crossings["peilgebied_from"] = crossings["peilgebied_from"].astype(object)
    crossings["peilgebied_to"] = crossings["peilgebied_to"].astype(object)
    for count, globalid in enumerate(letters_df):
        number = count + max_number + 1
        crossings.loc[crossings["peilgebied_from"] == globalid, "peilgebied_from"] = number
        crossings.loc[crossings["peilgebied_to"] == globalid, "peilgebied_to"] = number

    basin_max = max(
        max(v for v in crossings["peilgebied_from"] if pd.notna(v)),
        max(v for v in crossings["peilgebied_to"] if pd.notna(v)),
    )
    crossings = crossings.reset_index(drop=True)
    crossings["node_id"] = crossings.index + basin_max + 1

    if refill_streefpeil:
        # fill the missing streefpeilen from other crossings of the same peilgebied
        refill = []
        for side in ["from", "to"]:
            part = crossings.dropna(subset=f"streefpeil_{side}")[[f"peilgebied_{side}", f"streefpeil_{side}"]]
            refill.append(part.set_axis(["peilgebied", "streefpeil"], axis=1))
        refill = pd.concat(refill).dropna(subset="peilgebied").reset_index(drop=True)
        for side in ["from", "to"]:
            crossings = crossings.merge(refill, left_on=f"peilgebied_{side}", right_on="peilgebied", how="left")
            crossings[f"streefpeil_{side}"] = crossings[f"streefpeil_{side}"].fillna(crossings["streefpeil"])
            crossings = crossings.drop(columns=["peilgebied", "streefpeil"])
        crossings = crossings.drop_duplicates(subset=["hydroobject", "peilgebieden", "stuw", "gemaal"])
        crossings = crossings.reset_index(drop=True)
    return crossings


def _as_int(values: pd.Series) -> pd.Series:
    """`astype(int64, errors="ignore")`: integers if all values are, else unchanged."""
    try:
        return values.astype(np.int64)
    except (TypeError, ValueError):
        return values


def create_links(crossings: gpd.GeoDataFrame) -> pd.DataFrame:
    """Two links per crossing: from the upstream basin to the crossing, and from the crossing to the downstream basin."""
    links = pd.DataFrame()
    links["from"] = pd.concat([_as_int(crossings["peilgebied_from"]), _as_int(crossings["node_id"])]).to_numpy()
    links["to"] = pd.concat([_as_int(crossings["node_id"]), _as_int(crossings["peilgebied_to"])]).to_numpy()
    links["from_coord"] = pd.concat([crossings["from_centroid_geometry"], crossings["geometry"]]).to_numpy()
    links["to_coord"] = pd.concat([crossings["geometry"], crossings["to_centroid_geometry"]]).to_numpy()

    # links to a missing basin have no coordinates
    links = links.dropna()
    links = links.drop(links[(links["from"] == -999) | (links["to"] == -999)].index)
    links = links.reset_index(drop=True)

    links["from_x"] = links["from_coord"].apply(lambda geom: geom.x)
    links["from_y"] = links["from_coord"].apply(lambda geom: geom.y)
    links["to_x"] = links["to_coord"].apply(lambda geom: geom.x)
    links["to_y"] = links["to_coord"].apply(lambda geom: geom.y)
    links["from_coord"] = gpd.points_from_xy(x=links["from_x"], y=links["from_y"])
    links["to_coord"] = gpd.points_from_xy(x=links["to_x"], y=links["to_y"])
    links["from_coord"] = links["from_coord"].astype(object)
    links["to_coord"] = links["to_coord"].astype(object)
    links["line_geom"] = _lines(links["from_coord"], links["to_coord"])  # ty: ignore[invalid-assignment]
    return links


def _lines(start: pd.Series, end: pd.Series) -> list[LineString]:
    """The straight lines from each start to each end point."""
    return [LineString([a, b]) for a, b in zip(start, end, strict=True)]


def _boundary_nodes(
    nodes: pd.DataFrame,
    links: pd.DataFrame,
    crossings: pd.DataFrame,
    node_type: str,
    dx: float,
    downstream: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Add a boundary node 1 m beside each crossing, numbered after the last node, linked to or from the crossing."""
    first = max(nodes["node_id"]) + 1
    new = gpd.GeoDataFrame(
        {
            "node_id": np.arange(first, first + len(crossings)),
            "type": node_type,
            "geometry": [Point(point.x + dx, point.y) for point in crossings["geometry"]],
        }
    )
    new = new.drop_duplicates(subset="node_id")

    new_links = pd.DataFrame()
    if downstream:
        new_links["from"] = crossings["node_id"]
        new_links["to"] = new["node_id"].to_numpy()
        new_links["from_coord"] = crossings["geometry"]
        new_links["to_coord"] = new["geometry"].to_numpy()
    else:
        new_links["to"] = crossings["node_id"]
        new_links["from"] = new["node_id"].to_numpy()
        new_links["to_coord"] = crossings["geometry"]
        new_links["from_coord"] = new["geometry"].to_numpy()
    new_links["line_geom"] = _lines(new_links["from_coord"], new_links["to_coord"])  # ty: ignore[invalid-assignment]
    new_links = new_links.reset_index(drop=True)
    nodes = pd.concat([nodes, new]).reset_index(drop=True)
    links = pd.concat([links, new_links]).reset_index(drop=True)
    return nodes, links


def create_nodes(crossings: gpd.GeoDataFrame, links: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The nodes: basins, the crossings as connector nodes, and boundary nodes at the crossings of the model boundary.

    A crossing between two basins is a ManningResistance if their streefpeilen are equal, else a TabulatedRatingCurve,
    and a Pump if it has a gemaal. An inflowing crossing is a FlowBoundary, or a Pump with a LevelBoundary upstream if it
    has a gemaal. An outflowing crossing is a TabulatedRatingCurve with a Terminal downstream.
    """
    nodes_basins = pd.DataFrame()
    nodes_basins["node_id"] = pd.concat([crossings["peilgebied_from"], crossings["peilgebied_to"]]).to_numpy()
    nodes_basins["type"] = "Basin"
    nodes_basins["geometry"] = pd.concat(
        [crossings["from_centroid_geometry"], crossings["to_centroid_geometry"]]
    ).to_numpy()
    nodes_basins["basins_area_geom"] = pd.concat(
        [crossings["from_polygon_geometry"], crossings["to_polygon_geometry"]]
    ).to_numpy()
    streefpeil = pd.concat([crossings["streefpeil_from"], crossings["streefpeil_to"]]).astype(object).to_numpy()
    nodes_basins["streefpeil"] = pd.Series(streefpeil, dtype=object).where(pd.notna(streefpeil), "Onbekend streefpeil")
    nodes_basins = nodes_basins.loc[nodes_basins["node_id"] != -999].dropna()
    nodes_basins = nodes_basins.drop_duplicates(subset="node_id").reset_index(drop=True)

    between = crossings["peilgebied_from"].notna() & crossings["peilgebied_to"].notna()
    trc = crossings[between]
    boundary = crossings[~between].reset_index(drop=True)
    fb = boundary.loc[boundary["peilgebied_from"].isna()].reset_index(drop=True)
    trc_terminals = boundary.loc[boundary["peilgebied_from"].notna()].reset_index(drop=True)

    # between peilgebieden with equal streefpeil, ManningResistance
    mr = trc.loc[trc["streefpeil_from"] == trc["streefpeil_to"]]
    mr = mr[~mr["crossing_type"].astype(str).str.contains("-1")]
    mr = mr.loc[mr["peilgebied_from"].notna() & mr["peilgebied_to"].notna()]
    trc = trc[~trc["node_id"].isin(mr["node_id"])]

    def connector(rows: pd.DataFrame, node_type: str) -> pd.DataFrame:
        return pd.DataFrame({"node_id": rows["node_id"], "type": node_type, "geometry": rows["geometry"]})

    nodes_fb = connector(fb, "FlowBoundary").drop_duplicates(subset="node_id")
    nodes_trc_ter = connector(trc_terminals, "TabulatedRatingCurve").drop_duplicates(subset="node_id")
    nodes = pd.concat(
        [
            nodes_basins,
            connector(mr, "ManningResistance"),
            connector(trc, "TabulatedRatingCurve"),
            nodes_fb,
            nodes_trc_ter,
        ]
    )
    nodes = nodes.reset_index(drop=True)
    nodes.index = nodes["node_id"].to_numpy()
    nodes.loc[nodes["node_id"].isin(crossings.dropna(subset="gemaal")["node_id"]), "type"] = "Pump"

    if len(nodes_trc_ter) > 0:
        nodes, links = _boundary_nodes(nodes, links, trc_terminals, "Terminal", 1.0, downstream=True)
    # node IDs are unique, so the unstable sort of 2024 gives this order as well
    nodes = nodes.sort_values(by="node_id", kind="stable").reset_index(drop=True)

    # an inflowing crossing with a gemaal gets a LevelBoundary upstream
    pumps_in = crossings.dropna(subset="gemaal")
    pumps_in = pumps_in.loc[pumps_in["peilgebied_from"].isna()]
    if len(pumps_in) > 0:
        nodes, links = _boundary_nodes(nodes, links, pumps_in, "LevelBoundary", -1.0, downstream=False)
    nodes.index += 1
    return nodes, links


def _first_last_points(lines: pd.Series) -> tuple[pd.Series, pd.Series]:
    """The first and last points of lines."""
    first = [Point(geom.coords[0]) for geom in lines]
    last = [Point(geom.coords[-1]) for geom in lines]
    return pd.Series(first, index=lines.index, dtype=object), pd.Series(last, index=lines.index, dtype=object)


def _from_wkt(values: pd.Series) -> pd.Series:
    """The geometries of the WKT of each value."""
    return pd.Series(shapely.from_wkt(values.astype(str).to_numpy()), index=values.index, dtype=object)


def _discard_duplicate_boezems(boezems: pd.DataFrame) -> pd.DataFrame:
    """Keep the closest shortest path per link.

    As in 2024, the result updates the rows of `boezems` with the same position, not the same link.
    """
    temp = boezems.dropna(subset=["shortest_path"]).copy()
    temp["shortest_path"] = _from_wkt(temp["shortest_path"])
    first, last = _first_last_points(temp["shortest_path"])
    distance_first_from = [a.distance(b) for a, b in zip(first, temp["from_coord"], strict=True)]
    distance_last_to = [a.distance(b) for a, b in zip(last, temp["to_coord"], strict=True)]
    temp["correct_distance"] = np.add(distance_first_from, distance_last_to)
    # as in 2024, the incorrect distance is the same as the correct one
    temp["incorrect_distance"] = temp["correct_distance"]
    temp["shortest_distance"] = temp[["correct_distance", "incorrect_distance"]].min(axis=1)
    temp = temp.iloc[_quicksort_order(temp["shortest_distance"])]
    temp = temp.drop_duplicates(subset=["from", "to"], keep="first").reset_index(drop=True)
    boezems = boezems.copy()
    boezems.update(temp)
    return boezems


def _quicksort_order(values: pd.Series) -> np.ndarray:
    from ribasim_nl.lumping.utils import quicksort_order

    return quicksort_order(values.to_numpy(dtype=float))


def embed_boezems(
    links: pd.DataFrame,
    post_processed_data: dict[str, gpd.GeoDataFrame],
    crossings: gpd.GeoDataFrame,
    shortest_paths: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Draw the links to and from the boezem basins along the shortest paths through the boezem."""
    peilgebied = post_processed_data["peilgebied"]
    boezem_globalids = peilgebied.loc[peilgebied["peilgebied_cat"] == 1, "globalid"].drop_duplicates()

    def with_shortest_path(side: str, other: str, link_coord: str, link_node: str) -> pd.DataFrame:
        boezems = shortest_paths.loc[shortest_paths[f"peilgebied_{side}"].isin(boezem_globalids)]
        temp = boezems.merge(
            crossings,
            left_on=["hydroobject", f"peilgebied_{side}"],
            right_on=["hydroobject", f"peilgebied_{side}_original"],
            how="left",
        )
        result = links.merge(
            temp, left_on=[link_coord, link_node], right_on=[f"{side}_centroid_geometry", "node_id_y"], how="left"
        )
        return _discard_duplicate_boezems(result)

    correct = with_shortest_path("from", "to", "from_coord", "to")
    incorrect = with_shortest_path("to", "from", "to_coord", "from")
    correct = correct.drop_duplicates(subset=["from", "to"]).reset_index(drop=True)
    incorrect = incorrect.drop_duplicates(subset=["from", "to"]).reset_index(drop=True)

    links = links.rename(columns={"line_geom": "line_geom_oud"})
    links["line_geom"] = None
    for found in [incorrect, correct]:
        merged = links.merge(found, how="left", on=["from", "to"], suffixes=("", "_SP"))
        has_path = merged["shortest_path"].notna().to_numpy()
        links.loc[has_path, "line_geom"] = merged.loc[has_path, "shortest_path"].to_numpy()

    links["bool_SP"] = links["line_geom"].notna()
    links["line_geom"] = links["line_geom"].where(links["line_geom"].notna(), links["line_geom_oud"])
    links["line_geom"] = _from_wkt(links["line_geom"])

    # reverse the shortest paths that run from the end to the start of their link
    check = links.loc[links["bool_SP"]].copy()
    start, end = _first_last_points(check["line_geom"])
    d_start_from = np.array([a.distance(b) for a, b in zip(start, check["from_coord"], strict=True)])
    d_end_to = np.array([a.distance(b) for a, b in zip(end, check["to_coord"], strict=True)])
    d_start_to = np.array([a.distance(b) for a, b in zip(start, check["to_coord"], strict=True)])
    d_end_from = np.array([a.distance(b) for a, b in zip(end, check["from_coord"], strict=True)])
    reverse = d_start_from + d_end_to > d_start_to + d_end_from
    check["line_geom"] = [  # ty: ignore[invalid-assignment]
        LineString(list(geom.coords)[::-1]) if rev else geom
        for geom, rev in zip(check["line_geom"], reverse, strict=True)
    ]
    links.loc[check.index, "line_geom"] = check["line_geom"]
    return links


def change_link_geometries(
    links: pd.DataFrame, changes: tuple[tuple[tuple[int, int], tuple[int, int]], ...]
) -> pd.DataFrame:
    """Give links the geometry of another link, by (from, to) node IDs; for each, (link to change, link to use)."""
    for (from_change, to_change), (from_geom, to_geom) in changes:
        geometry = links.loc[(links["from"] == from_geom) & (links["to"] == to_geom), "line_geom"]
        # as in 2024, the geometry is assigned by index, so to a row with the same index only
        selection = (links["from"] == from_change) & (links["to"] == to_change)
        links.loc[selection, "line_geom"] = geometry
    return links


def _node_table(rows: pd.DataFrame, node_type: str) -> pd.DataFrame:
    """A node table of ribasim 2024.11, indexed by node ID."""
    rows = rows.reset_index(drop=True)
    node = pd.DataFrame(
        {
            "node_id": rows["node_id"],
            "node_type": node_type,
            "name": np.nan,
            "subnetwork_id": np.nan,
            "geometry": rows["geometry"],
        }
    )
    node["meta_node_id"] = node["node_id"].astype(int)
    return node.set_index("node_id")


def network_tables(nodes: pd.DataFrame, links: pd.DataFrame) -> Tables:
    """The tables of each node type, with default values, and the links."""
    link_types = nodes.set_index("node_id")["type"]
    link = gpd.GeoDataFrame(
        {
            "from_node_id": links["from"].astype(int),
            "meta_from_node_type": links["from"].map(link_types).to_numpy(),
            "to_node_id": links["to"].astype(int),
            "meta_to_node_type": links["to"].map(link_types).to_numpy(),
            "link_type": "flow",
            "name": None,
            "geometry": links["line_geom"],
        }
    ).reset_index(drop=True)
    assert link["meta_from_node_type"].notna().all() and link["meta_to_node_type"].notna().all(), "links to no node"
    link.index.name = "link_id"
    tables = Tables(link=link)

    def of_type(node_type: str) -> pd.DataFrame:
        return nodes.loc[nodes["type"] == node_type]

    # Basin, of which the state keeps the index of `nodes`
    basins = of_type("Basin")
    basin_nodes = basins[["node_id", "streefpeil", "geometry", "basins_area_geom"]].reset_index(drop=True)
    tables.nodes["Basin"] = _node_table(basin_nodes, "Basin")
    tables.tables["Basin / profile"] = pd.concat(
        [
            pd.DataFrame({"node_id": basins["node_id"], "area": 0.1, "level": -9.999}),
            pd.DataFrame({"node_id": basins["node_id"], "area": 3000.0, "level": 3.0}),
        ]
    )
    state = pd.DataFrame({"node_id": basins["node_id"], "level": basins["streefpeil"]})
    state.loc[state["level"] == "Onbekend streefpeil", "level"] = 9.999
    state["level"] = state["level"].astype(float)
    tables.tables["Basin / state"] = state
    tables.tables["Basin / static"] = pd.DataFrame(
        {
            "node_id": basin_nodes["node_id"],
            "drainage": 0.0,
            "potential_evaporation": 0.0,
            "infiltration": 0.0,
            "precipitation": 0.0,
        }
    )
    area = gpd.GeoDataFrame(
        {
            "node_id": basin_nodes["node_id"],
            "meta_streefpeil": basin_nodes["streefpeil"],
            "geometry": [
                MultiPolygon([geom]) if isinstance(geom, Polygon) else geom for geom in basin_nodes["basins_area_geom"]
            ],
        },
        geometry="geometry",
        crs=CRS,
    )
    area["meta_node_id"] = area["node_id"].astype(int)
    tables.tables["Basin / area"] = area

    # TabulatedRatingCurve, with two dummy points
    trc = of_type("TabulatedRatingCurve").reset_index(drop=True)
    tables.nodes["TabulatedRatingCurve"] = _node_table(trc, "TabulatedRatingCurve")
    qh = pd.concat(
        [
            pd.DataFrame({"node_id": trc["node_id"], "level": 0.0, "flow_rate": 0.0}),
            pd.DataFrame({"node_id": trc["node_id"], "level": 1.0, "flow_rate": 1.0}),
        ]
    )
    qh = qh.sort_values(by=["node_id", "level"]).reset_index(drop=True)
    qh.index += 1
    tables.tables["TabulatedRatingCurve / static"] = pd.DataFrame(
        {"node_id": qh["node_id"], "control_state": None, "level": qh["level"], "flow_rate": qh["flow_rate"]}
    )

    pumps = of_type("Pump").reset_index(drop=True)
    tables.nodes["Pump"] = _node_table(pumps, "Pump")
    tables.tables["Pump / static"] = pd.DataFrame(
        {
            "node_id": pumps["node_id"],
            "flow_rate": 0.0,
            "min_flow_rate": np.nan,
            "max_flow_rate": np.nan,
            "control_state": None,
        }
    )

    level_boundaries = of_type("LevelBoundary").reset_index(drop=True)
    tables.nodes["LevelBoundary"] = _node_table(level_boundaries, "LevelBoundary")
    tables.tables["LevelBoundary / static"] = pd.DataFrame({"node_id": level_boundaries["node_id"], "level": 0.0})

    flow_boundaries = of_type("FlowBoundary").reset_index(drop=True)
    tables.nodes["FlowBoundary"] = _node_table(flow_boundaries, "FlowBoundary")
    tables.tables["FlowBoundary / static"] = pd.DataFrame({"node_id": flow_boundaries["node_id"], "flow_rate": 0.0})

    manning = of_type("ManningResistance").reset_index(drop=True)
    tables.nodes["ManningResistance"] = _node_table(manning, "ManningResistance")
    tables.tables["ManningResistance / static"] = pd.DataFrame(
        {
            "node_id": manning["node_id"],
            "length": 1000.0,
            "manning_n": 0.02,
            "profile_width": 2.0,
            "profile_slope": 3.0,
            "control_state": None,
        }
    )

    tables.nodes["Terminal"] = _node_table(of_type("Terminal"), "Terminal")
    return tables


def _within_boezem(tables: Tables, boezem: gpd.GeoDataFrame) -> pd.Index:
    """The index of the Basin state rows whose node lies within a boezem.

    As in 2024, the state rows get the location of the Basin node with the same index.
    """
    state = tables.tables["Basin / state"].copy()
    state["geometry"] = tables.nodes["Basin"]["geometry"]
    state = gpd.GeoDataFrame(state, geometry="geometry", crs=CRS)
    points_within = gpd.sjoin(state, boezem.to_crs(CRS), how="inner", predicate="within")
    return points_within.index


def checks(
    tables: Tables,
    nodes: pd.DataFrame,
    links: pd.DataFrame,
    post_processed_data: dict[str, gpd.GeoDataFrame],
    crossings: gpd.GeoDataFrame,
    all_crossings: gpd.GeoDataFrame,
) -> dict[str, gpd.GeoDataFrame]:
    """The layers of `database_checks.gpkg`, of which `boezem` and `inlaten_uitlaten_boezems` are used for metadata."""
    result: dict[str, gpd.GeoDataFrame] = {}
    node_points = gpd.GeoDataFrame(nodes[["node_id", "type", "geometry"]], geometry="geometry", crs=CRS)

    unique_first = np.unique(links["from"].astype(int))
    unique_second = np.unique(links["to"].astype(int))
    # as in 2024, the sinks are off by one
    sinks = node_points.loc[node_points["node_id"].isin(np.setdiff1d(unique_second, unique_first) + 1)]
    result["sinks"] = sinks.loc[sinks["type"] == "Basin"]
    sources = node_points.loc[node_points["node_id"].isin(np.setdiff1d(unique_first, unique_second))]
    result["sources"] = sources.loc[sources["type"] == "Basin"]

    pg_from_to = pd.to_numeric(pd.concat([crossings["peilgebied_from"], crossings["peilgebied_to"]]), errors="coerce")
    unconnected = node_points[~node_points["node_id"].isin(pg_from_to)]
    result["basins_without_connection"] = unconnected.loc[unconnected["type"] == "Basin"]

    gemaal = post_processed_data["gemaal"]
    gemaal["in_use"] = gemaal["globalid"].isin(all_crossings.dropna(subset="gemaal")["gemaal"])
    result["gemaal_in_use_True_False"] = gemaal

    peilgebied = post_processed_data["peilgebied"]
    pg_st = peilgebied.merge(post_processed_data["streefpeil"], on="globalid", suffixes=("", "_streefpeil"))
    result["peilgebied_met_streefpeil"] = pg_st[["waterhoogte", "code", "globalid", "geometry"]]
    result["boezem"] = peilgebied.loc[peilgebied["peilgebied_cat"] > 0][
        ["code", "globalid", "geometry", "peilgebied_cat"]
    ]
    result["HKV_internal_crossings"] = crossings.drop(
        columns=["from_centroid_geometry", "to_centroid_geometry", "from_polygon_geometry", "to_polygon_geometry"]
    )
    result["duikersifonhevel"] = post_processed_data["duikersifonhevel"]
    result["hydroobjecten"] = post_processed_data["hydroobject"]

    # the connector nodes to and from the boezems, and their boundary connections
    state = tables.tables["Basin / state"]
    boezem_nodes = state["node_id"].loc[_within_boezem(tables, result["boezem"])]
    link = tables.link
    from_boezem = link.loc[link["from_node_id"].isin(boezem_nodes)]
    to_boezem = link.loc[link["to_node_id"].isin(boezem_nodes)]
    trc_node = tables.nodes["TabulatedRatingCurve"]
    pump_node = tables.nodes["Pump"]
    fb_node = tables.nodes["FlowBoundary"]

    def select(node: pd.DataFrame, ids: pd.Series, kind: str) -> pd.DataFrame:
        selected = node.loc[node.index.isin(ids)].copy()
        selected["meta_type_verbinding"] = kind
        return selected

    inlaten_trc = select(
        trc_node,
        from_boezem.loc[from_boezem["meta_to_node_type"].isin(["TabulatedRatingCurve", "Outlet"]), "to_node_id"],
        "Inlaat",
    )
    inlaten_gemalen = select(
        pump_node, from_boezem.loc[from_boezem["meta_to_node_type"] == "Pump", "to_node_id"], "Inlaat"
    )
    inlaten_fb = select(
        fb_node, to_boezem.loc[to_boezem["meta_from_node_type"] == "FlowBoundary", "from_node_id"], "Inlaat boundary"
    )
    uitlaten_trc = select(
        trc_node,
        to_boezem.loc[to_boezem["meta_from_node_type"].isin(["TabulatedRatingCurve", "Outlet"]), "from_node_id"],
        "Uitlaat",
    )
    uitlaten_gemalen = select(
        pump_node, to_boezem.loc[to_boezem["meta_from_node_type"] == "Pump", "from_node_id"], "Uitlaat"
    )
    uitlaten_fb = select(
        fb_node, to_boezem.loc[to_boezem["meta_from_node_type"] == "FlowBoundary", "from_node_id"], "Inlaat boundary"
    )

    # connector nodes between a boezem and a boundary node
    connectors = ["Pump", "TabulatedRatingCurve", "Outlet"]
    boundaries = ["FlowBoundary", "LevelBoundary", "Terminal"]
    bcn_from = from_boezem.loc[from_boezem["meta_to_node_type"].isin(connectors), "to_node_id"]
    bcn_from = link.loc[link["from_node_id"].isin(bcn_from)]
    bcn_from = bcn_from.loc[bcn_from["meta_to_node_type"].isin(boundaries)]
    bcn_from = pd.concat(
        [
            trc_node.loc[trc_node.index.isin(bcn_from["from_node_id"])],
            pump_node.loc[pump_node.index.isin(bcn_from["from_node_id"])],
        ]
    )
    bcn_from["meta_type_verbinding"] = "Uitlaat boundary"
    bcn_to = to_boezem.loc[to_boezem["meta_from_node_type"].isin(connectors), "from_node_id"]
    bcn_to = link.loc[link["to_node_id"].isin(bcn_to)]
    bcn_to = bcn_to.loc[bcn_to["meta_from_node_type"].isin(boundaries)]
    bcn_to = pd.concat(
        [
            trc_node.loc[trc_node.index.isin(bcn_to["to_node_id"])],
            pump_node.loc[pump_node.index.isin(bcn_to["to_node_id"])],
        ]
    )
    bcn_to["meta_type_verbinding"] = "Inlaat boundary"

    inlaten_uitlaten = pd.concat(
        [inlaten_trc, inlaten_gemalen, inlaten_fb, uitlaten_trc, uitlaten_gemalen, uitlaten_fb, bcn_from, bcn_to]
    )
    inlaten_uitlaten = inlaten_uitlaten.reset_index(drop=True)
    result["inlaten_uitlaten_boezems"] = gpd.GeoDataFrame(inlaten_uitlaten, geometry="geometry", crs=CRS)
    return {name: gpd.GeoDataFrame(layer, geometry="geometry", crs=CRS) for name, layer in result.items()}


def add_meta_data(
    tables: Tables,
    check: dict[str, gpd.GeoDataFrame],
    post_processed_data: dict[str, gpd.GeoDataFrame],
    crossings: gpd.GeoDataFrame,
    unknown_pump_function: str,
) -> None:
    """Add the category of the basins and links, the function of the pumps, and how connectors connect to a boezem."""
    state = tables.tables["Basin / state"]
    state["meta_categorie"] = "doorgaand"
    state.loc[_within_boezem(tables, check["boezem"]), "meta_categorie"] = "hoofdwater"

    # the function of the pumps, from their gemaal at the same location
    pump_static = tables.tables["Pump / static"]
    crossings_pump = crossings.loc[crossings["gemaal"].notna()][["node_id", "gemaal", "geometry"]]
    coupled = crossings_pump.merge(
        post_processed_data["gemaal"][["globalid", "func_afvoer", "func_aanvoer", "func_circulatie"]],
        left_on="gemaal",
        right_on="globalid",
    )
    # the node ID of the crossing, since the node table has it as index
    coupled["geometry"] = coupled["geometry"].astype(object)
    pump_node = tables.nodes["Pump"].assign(geometry=lambda df: df["geometry"].astype(object))
    coupled = coupled.merge(pump_node, on="geometry", suffixes=("", "_duplicate"))
    pump_function = coupled[["node_id", "func_afvoer", "func_aanvoer", "func_circulatie"]]
    coupled_pump_function = pump_static.merge(pump_function, on="node_id")
    coupled_pump_function = coupled_pump_function.rename(
        columns={
            "func_afvoer": "meta_func_afvoer",
            "func_aanvoer": "meta_func_aanvoer",
            "func_circulatie": "meta_func_circulatie",
        }
    ).drop_duplicates(subset="node_id")
    functions = ["meta_func_afvoer", "meta_func_aanvoer", "meta_func_circulatie"]
    pump_static = pump_static.merge(coupled_pump_function[["node_id", *functions]], on="node_id", how="left")
    # pumps without any function get the default one, pumps without a gemaal keep missing values
    unknown = (pump_static[functions] == 0).all(axis=1)
    column = f"meta_func_{unknown_pump_function}"
    dtype = pump_static[column].dtype
    pump_static.loc[unknown, column] = (
        1.0 if pd.api.types.is_float_dtype(dtype) else 1 if pd.api.types.is_integer_dtype(dtype) else True
    )
    tables.tables["Pump / static"] = pump_static

    # links to and from the boezems are hoofdwater
    link = tables.link
    link["meta_categorie"] = "doorgaand"
    boezems = state.loc[state["meta_categorie"] == "hoofdwater", "node_id"]
    link.loc[link["from_node_id"].isin(boezems) | link["to_node_id"].isin(boezems), "meta_categorie"] = "hoofdwater"

    color_cycle = itertools.cycle(Category10[10])
    area = tables.tables["Basin / area"]
    area["meta_color"] = [next(color_cycle) for _ in range(len(area))]

    # as in 2024, joined by index rather than by node ID
    verbinding = check["inlaten_uitlaten_boezems"][["meta_type_verbinding"]]
    for name in ["TabulatedRatingCurve / static", "Pump / static", "FlowBoundary / static"]:
        tables.tables[name] = tables.tables[name].merge(verbinding, left_index=True, right_index=True, how="left")


def add_relevant_names(
    tables: Tables, post_processed_data: dict[str, gpd.GeoDataFrame], crossings: gpd.GeoDataFrame
) -> None:
    """Name the pumps after the gemaal at their location, as in May 2025.

    The code of May 2025 also named the basins after the aggregation area with the same geometry, but found none, since
    ribasim 2025.3 changed the geometries of the basin areas on assignment, so the basins have no name.
    """
    pump_node = tables.nodes["Pump"]
    pumps = pump_node[["geometry"]].astype(object).reset_index()
    gemalen = crossings.loc[crossings["gemaal"].notna()].rename(columns={"gemaal": "globalid"})
    gemalen = pd.DataFrame({"geometry": gemalen["geometry"].astype(object), "globalid": gemalen["globalid"]})
    pumps = pumps.merge(gemalen, how="left", on="geometry")
    pumps = pumps.merge(post_processed_data["gemaal"][["globalid", "meta_name"]], on="globalid", how="left")
    pumps = pumps.set_index("node_id")
    pump_node = pump_node.drop(columns="name").merge(
        pumps[["meta_name"]], left_index=True, right_index=True, how="left"
    )
    tables.nodes["Pump"] = pump_node.rename(columns={"meta_name": "name"}).drop_duplicates()


def sorted_table(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Sort a table as ribasim 2024.11 did when writing it; stable for several keys, and unique single keys."""
    assert len(keys) > 1 or df[keys[0]].is_unique, f"cannot reproduce the unstable sort of non-unique {keys}"
    df = df.sort_values(keys, kind="stable", ignore_index=True)
    df.index.name = "fid"
    return df


def to_model(tables: Tables, endtime: str) -> Model:
    """A Ribasim model of the tables, as ribasim 2024.11 wrote them and the current version reads them."""
    model = Model(starttime="2024-01-01", endtime=endtime, crs=CRS)
    model.use_validation = False

    node = pd.concat([tables.nodes[node_type] for node_type in NODE_TYPES if len(tables.nodes[node_type]) > 0])
    node = node.sort_index(kind="stable")
    node.index.name = "node_id"
    node["name"] = node["name"].where(node["name"].notna(), "")
    model.node.df = GeoDataFrame[NodeSchema](node.drop(columns=["subnetwork_id"]), geometry="geometry", crs=CRS)
    model.link.df = GeoDataFrame[LinkSchema](tables.link.assign(name=""), geometry="geometry", crs=CRS)

    model_tables = {
        "Basin / state": model.basin.state,
        "Basin / static": model.basin.static,
        "Basin / profile": model.basin.profile,
        "Basin / area": model.basin.area,
        "TabulatedRatingCurve / static": model.tabulated_rating_curve.static,
        "Pump / static": model.pump.static,
        "ManningResistance / static": model.manning_resistance.static,
        "LevelBoundary / static": model.level_boundary.static,
        "FlowBoundary / static": model.flow_boundary.static,
    }
    for name, table in model_tables.items():
        df = tables.tables[name]
        if len(df) > 0:
            table.df = sorted_table(df, SORT_KEYS[name])  # ty: ignore[invalid-assignment]
    return model


def boezem_model(
    postprocessed_path: Path,
    crossings_path: Path,
    shortest_path_path: Path,
    options: Options,
) -> tuple[Model, dict[str, gpd.GeoDataFrame]]:
    """The input model of a level controlled water authority, and the layers of its `database_checks.gpkg`."""
    post_processed_data = read_postprocessed(postprocessed_path, all_layers=options.all_layers)
    crossings = read_crossings(crossings_path)
    post_processed_data["aggregation_areas"] = read_layer(crossings_path, "aggregation_area").to_crs(CRS)

    post_processed_data, crossings = routing_processor(post_processed_data, crossings)
    crossings = assign_node_ids(crossings, refill_streefpeil=options.refill_streefpeil)
    links = create_links(crossings)
    nodes, links = create_nodes(crossings, links)
    shortest_paths = to_object(gpd.read_file(shortest_path_path).set_crs(CRS, allow_override=True))
    links = embed_boezems(links, post_processed_data, crossings, shortest_paths)
    links = change_link_geometries(links, options.link_changes)

    tables = network_tables(nodes, links)
    all_crossings = read_layer(crossings_path, "crossings_hydroobject_filtered")
    check = checks(tables, nodes, links, post_processed_data, crossings, all_crossings)
    add_meta_data(tables, check, post_processed_data, crossings, options.unknown_pump_function)
    if options.add_names:
        add_relevant_names(tables, post_processed_data, crossings)
    return to_model(tables, options.endtime), check

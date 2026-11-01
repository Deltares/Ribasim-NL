from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
import pytest
from ribasim_nl.lumping.dhydro import read_ini_sections
from ribasim_nl.lumping.hydamo import (
    apply_hydamo_changes,
    read_areas,
    read_boundaries,
    read_hydamo_network,
    read_split_nodes,
)
from ribasim_nl.lumping.model import lumped_model, number_lost_basins, read_krw
from ribasim_nl.lumping.network import area_basins, assign_areas, fill_holes, lump_network
from ribasim_nl.lumping.utils import merge_grouped, quicksort_order, read_layer, split_edges_by_dx
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon, box

CRS = "EPSG:28992"


def test_merge_grouped():
    left = pd.DataFrame({"key": [2, 1, 2, 3], "a": ["w", "x", "y", "z"]})
    right = pd.DataFrame({"key": [1, 2, 2, 4], "b": [10, 20, 21, 40]})
    merged = merge_grouped(left, right, on="key")
    # grouped by key in order of first appearance in left, where pandas >= 2.2 keeps the order of left
    assert merged["a"].tolist() == ["w", "w", "y", "y", "x"]
    assert merged["b"].tolist() == [20, 21, 20, 21, 10]
    assert merged.index.tolist() == [0, 1, 2, 3, 4]
    with pytest.raises(AssertionError):
        merge_grouped(left, right, on="key", how="left")


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        # an insertion sort only
        ([3, 1, 2, 1, 3], [1, 3, 2, 0, 4]),
        # partitions around the median of three
        (
            [4, 1, 1, 3, 0, 2, 2, 2, 4, 4, 1, 2, 0, 4, 4, 2, 2],
            [4, 12, 1, 2, 10, 16, 5, 6, 7, 15, 11, 3, 14, 0, 9, 13, 8],
        ),
        (
            [2.5, 1.5, 2.5, 2.5, 1.5, 2.5, 2.5, 0.5, 0.5, 0.5, 0.5, 2.5, 2.5, 0.5, 1.5,
             2.5, 0.5, 2.5, 0.5, 1.5, 2.5, 0.5, 1.5, 0.5, 2.5, 0.5, 2.5, 1.5, 1.5, 1.5],
            [25, 23, 21, 7, 8, 9, 10, 18, 16, 13, 14, 27, 22, 19, 29,
             4, 1, 28, 15, 12, 17, 11, 6, 20, 5, 3, 24, 2, 26, 0],
        ),
        # partitions so unbalanced, made with McIlroy's adversary, that the last one is sorted with heapsort
        (
            [0, 10, 1, 11, 2, 19, 3, 18, 4, 19, 5, 18, 6, 17, 7, 17, 8, 16, 9, 0,
             1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 15, 14, 14, 13, 13, 12, 12, 16, 11],
            [0, 19, 2, 20, 4, 21, 6, 22, 8, 23, 10, 24, 12, 25, 14, 26, 16, 27, 18, 28,
             1, 29, 3, 39, 36, 37, 35, 34, 33, 32, 31, 30, 17, 38, 15, 13, 11, 7, 9, 5],
        ),
    ],
)  # fmt: skip
def test_quicksort_order(values: list[float], expected: list[int]):
    """The orders of `numpy.argsort` in numpy 1.26 without AVX-512, which differ from a stable sort."""
    assert quicksort_order(values).tolist() == expected
    assert quicksort_order(pd.Series(values)).tolist() == expected


def test_quicksort_order_nan():
    with pytest.raises(AssertionError, match="NaN"):
        quicksort_order([1.0, np.nan, 0.0])


def test_split_edges_by_dx():
    edges = gpd.GeoDataFrame(
        {"branch_id": ["long", "short"]},
        geometry=[LineString([(0, 0), (600, 0)]), LineString([(0, 10), (300, 10)])],
        crs=CRS,
    )
    split = split_edges_by_dx(edges, dx=250.0)
    # the long edge is split in round(600 / 250) = 2 sections, which follow the edges that are not split
    assert split["edge_id"].tolist() == ["short", "long___0", "long___1"]
    assert split.length.tolist() == [300.0, 300.0, 300.0]


def test_read_ini_sections(tmp_path: Path):
    path = tmp_path / "boundaries.ext"
    path.write_text(
        "[General]\nfileVersion = 2.01\n\n[Boundary]\nnodeId = N1\n\n[Lateral]\nid = L1\n\n[Boundary]\nnodeId = N2\n"
        "\n[Boundaries]\nnodeId = N3\n"
    )
    # only the sections with exactly this name
    assert read_ini_sections(path, "Boundary")["nodeid"].tolist() == ["N1", "N2"]
    assert read_ini_sections(path, "Lateral")["id"].tolist() == ["L1"]


def test_read_layer_remove_z(tmp_path: Path):
    path = tmp_path / "lines.gpkg"
    lines = gpd.GeoDataFrame(
        {"code": ["L1"]}, geometry=[MultiLineString([[(0, 0, 1), (1, 0, 1)], [(2, 0, 1), (3, 0, 1)]])], crs=CRS
    )
    lines.to_file(path, layer="lines")
    read = read_layer(path, "lines", explode=False, remove_z=True)
    assert read is not None
    assert not read.has_z.any()
    assert read.geometry[0].equals(MultiLineString([[(0, 0), (1, 0)], [(2, 0), (3, 0)]]))
    assert read_layer(path, "other") is None


def test_read_krw_without_layers(tmp_path: Path):
    path = tmp_path / "krw.gpkg"
    gpd.GeoDataFrame({"owmident": ["K1"]}, geometry=[box(0, 0, 1, 1)], crs=CRS).to_file(path, layer="krw_polygon")
    with pytest.raises(AssertionError, match="no layer krw_line or krw_vlak"):
        read_krw(path)


def test_area_basins_degenerate_area():
    edges = gpd.GeoDataFrame({"basin": [0]}, geometry=[LineString([(0, 0), (100, 0)])], crs=CRS)
    # an area without geometry drains to no basin
    areas = gpd.GeoDataFrame({"code": ["A1", "A2"], "area": [1, 2]}, geometry=[box(0, -10, 50, 10), None], crs=CRS)
    areas = area_basins(edges, areas)
    assert areas.set_index("code")["basin"].to_dict() == {"A1": 0, "A2": -1}
    # a line and a point, when made valid
    degenerate = MultiPolygon([Polygon([(0, 0), (1, 0), (2, 0)]), Polygon([(5, 5)] * 4)])
    areas = gpd.GeoDataFrame(
        {"code": ["A1", "A2"], "area": [1, 2]}, geometry=[box(0, -10, 50, 10), degenerate], crs=CRS
    )
    with pytest.raises(AssertionError, match="area A2 has no polygon"):
        area_basins(edges, areas)


def areas_of(boxes: dict[str, tuple[float, float, float, float]], basins: dict[str, int]) -> gpd.GeoDataFrame:
    """Areas with codes, numbers and the basin they drain to, from boxes."""
    codes = list(boxes)
    return gpd.GeoDataFrame(
        {"code": codes, "area": range(1, len(codes) + 1), "basin": [basins.get(c, -1) for c in codes]},
        geometry=[box(*b) for b in boxes.values()],
        crs=CRS,
    )


def test_assign_areas():
    """Areas that drain to no kept basin go to the basin they border longest, preferably in their drainage area."""
    areas = areas_of(
        {
            # A and B drain to basins 1 and 2, C to basin 3, which is not kept
            "A": (0, 0, 10, 10),
            "B": (10, 0, 20, 10),
            "C": (0, 10, 20, 12),
            # D borders only C, so it reaches a basin through C
            "D": (0, 12, 10, 13),
            # E borders A longest, but B in its drainage area
            "E": (0, -6, 12, 0),
            # F borders no area, so it goes to the nearest
            "F": (30, 0, 31, 1),
        },
        {"A": 1, "B": 2, "C": 3},
    )
    # A, C and D in drainage area X, B, E and F in Y
    drainage_areas = gpd.GeoDataFrame(
        {"code": ["X", "Y"]},
        geometry=[box(0, 0, 10, 20), box(10, -10, 40, 20).union(box(0, -10, 10, 0))],
        crs=CRS,
    )
    basins = pd.DataFrame({"basin": [1, 2]})
    areas, basin_areas = assign_areas(areas, basins, drainage_areas)
    assert areas.set_index("code")["basin"].to_dict() == {"A": 1, "B": 2, "C": 1, "D": 1, "E": 2, "F": 2}
    assert basin_areas["basin"].tolist() == [1, 2]
    assert (basin_areas.geom_type == "MultiPolygon").all()
    assert basin_areas.area.sum() == pytest.approx(areas.area.sum())


def test_assign_areas_to_basins_within():
    """The areas of a basin that is not kept go together to the one basin without area within them."""
    areas = areas_of({"A": (0, 0, 10, 10), "B": (10, 0, 20, 10), "C": (20, 0, 30, 10)}, {"A": 1, "B": 3, "C": 3})
    basins = gpd.GeoDataFrame({"basin": [1, 2]}, geometry=[Point(5, 5), Point(15, 5)], crs=CRS)
    areas, _ = assign_areas(areas, basins)
    assert areas.set_index("code")["basin"].to_dict() == {"A": 1, "B": 2, "C": 2}


def test_assign_areas_fills_gaps():
    """Gaps between the basin areas up to the maximum area go to the basin area they border longest."""
    areas = areas_of({"A": (0, 0, 30, 10), "B": (0, 11, 30, 20), "C": (0, 10, 3, 11), "D": (4, 10, 30, 11)}, {})
    areas["basin"] = [1, 2, 2, 2]
    _, basin_areas = assign_areas(areas, pd.DataFrame({"basin": [1, 2]}), max_gap_area=2.0)
    # the gap between C and D, of 1 m², goes to basin 2, which borders it on three sides
    assert basin_areas.set_index("basin").area.to_dict() == {1: 300.0, 2: 300.0}
    _, basin_areas = assign_areas(areas, pd.DataFrame({"basin": [1, 2]}), max_gap_area=0.5)
    assert basin_areas.set_index("basin").area.to_dict() == {1: 300.0, 2: 299.0}


def test_number_lost_basins():
    """Lost basins get the next node IDs where the validator of the fix scripts finds them, in its order."""
    nodes = gpd.GeoDataFrame(
        {"node_id": [1, 2, 3, 4], "node_type": ["LevelBoundary", "ManningResistance", "Basin", "Pump"]},
        geometry=[Point(0, 0), Point(10, 0), Point(20, 0), Point(30, 0)],
        crs=CRS,
    )
    # the node ID of basin L1 is that of ManningResistance 2, L2 and L3 have no node
    lost = gpd.GeoDataFrame(
        {"node_id": [2, 9, 7], "node_type": "Basin", "meta_id": [11, 12, 13]},
        geometry=[Point(15, 5), Point(40, 0), Point(50, 0)],
        crs=CRS,
    )
    links = gpd.GeoDataFrame(
        {"from_node_id": [2, 2, 2, 1, 9, 1], "to_node_id": [3, 4, 4, 9, 1, 7], "link_type": "flow"},
        geometry=[
            LineString([(10, 0), (20, 0)]),
            LineString([(15, 5), (30, 0)]),
            # a duplicate of the previous link
            LineString([(15, 5), (30, 0)]),
            # the validator does not check the end of a link whose start is at its node, if that has no node
            LineString([(0, 0), (40, 0)]),
            LineString([(40, 0), (0, 0)]),
            LineString([(0, 0), (50, 0)]),
        ],
        crs=CRS,
    )
    links.index = pd.RangeIndex(1, 7, name="link_id")
    new_nodes, new_links = number_lost_basins(nodes, links, lost)
    assert new_nodes["node_id"].tolist() == [1, 2, 3, 4, 5, 6]
    assert new_nodes.set_index("node_id").loc[[5, 6], "meta_id"].tolist() == [11, 12]
    assert new_links.index.tolist() == [1, 2, 4, 5, 6]
    assert list(zip(new_links["from_node_id"], new_links["to_node_id"], strict=True)) == [
        (2, 3),
        (5, 4),
        (1, 6),
        (6, 1),
        # L3 is not found, so it stays out, and its link as it was
        (1, 7),
    ]
    # node IDs that a fix script gave
    new_nodes, _ = number_lost_basins(nodes, links, lost, node_ids={9: 100})
    assert new_nodes.set_index("meta_id").loc[[11, 12], "node_id"].tolist() == [5, 100]


def test_fill_holes():
    basin_areas = gpd.GeoDataFrame({"node_id": [1, 2]}, geometry=[box(0, 0, 10, 10), box(20, 0, 30, 10)], crs=CRS)
    # the uncovered part of A goes to basin 1, which covers most of A, and B to basin 2, which it borders longest
    areas = gpd.GeoDataFrame({"code": ["A", "B"]}, geometry=[box(0, 0, 10, 12), box(12, 0, 20, 10)], crs=CRS)
    filled = fill_holes(basin_areas, areas)
    assert filled.set_index("node_id").area.to_dict() == {1: 120.0, 2: 180.0}


def test_apply_hydamo_changes(tmp_path: Path):
    source, removals, additions, target = (tmp_path / f"{name}.gpkg" for name in ("hydamo", "remove", "add", "target"))
    stuw = gpd.GeoDataFrame({"code": ["S1", "S2"], "hoogte": [1.0, 2.0]}, geometry=[Point(0, 0), Point(1, 0)], crs=CRS)
    stuw.to_file(source, layer="stuw")
    hydroobject = gpd.GeoDataFrame({"code": ["H1"]}, geometry=[LineString([(0, 0), (1, 0)])], crs=CRS)
    hydroobject.to_file(source, layer="hydroobject")
    pyogrio.write_dataframe(pd.DataFrame({"code": ["P1"]}), source, layer="pomp")
    stuw.iloc[[0]].to_file(removals, layer="stuw")
    gpd.GeoDataFrame({"code": ["S3"], "hoogte": [3]}, geometry=[Point(2, 0)], crs=CRS).to_file(additions, layer="stuw")

    apply_hydamo_changes(source, removals, additions, target)
    result = gpd.read_file(target, layer="stuw")
    assert result["code"].tolist() == ["S2", "S3"]
    # as in 2024, the row numbers before the additions
    assert result["index"].tolist() == [1, 0]
    assert result["hoogte"].dtype == np.float64
    pd.testing.assert_frame_equal(gpd.read_file(target, layer="hydroobject"), hydroobject)
    # a layer without geometries is written as attribute table
    assert pyogrio.read_dataframe(target, layer="pomp")["code"].tolist() == ["P1"]


@pytest.fixture
def lumping_input(tmp_path: Path) -> Path:
    """Two hydroobjects from x=0 to 2000, with a weir at x=1000, under a culvert, and open water at x=1500.

    It flows from a FlowBoundary at x=0 to a LevelBoundary at x=2000. Areas A1 and A2 are on the waterways, A3
    north of A1 is not.
    """
    hydamo = tmp_path / "hydamo.gpkg"
    gpd.GeoDataFrame(
        {"code": ["H1", "H2"]},
        geometry=[LineString([(0, 0), (1000, 0)]), LineString([(1000, 0), (2000, 0)])],
        crs=CRS,
    ).to_file(hydamo, layer="hydroobject")
    gpd.GeoDataFrame({"code": ["S1"]}, geometry=[Point(1000, 0)], crs=CRS).to_file(hydamo, layer="stuw")
    gpd.GeoDataFrame({"code": ["D1"]}, geometry=[Point(1000, 0)], crs=CRS).to_file(hydamo, layer="duikersifonhevel")

    ribasim_input = tmp_path / "ribasim_input.gpkg"
    gpd.GeoDataFrame(
        {"split_node": [1, 2], "split_node_id": [None, None], "object_type": [None, None], "object_function": ["", ""]},
        geometry=[Point(1000, 0), Point(1500, 0)],
        crs=CRS,
    ).to_file(ribasim_input, layer="split_nodes")
    gpd.GeoDataFrame(
        {"boundary_name": ["in", "out"], "boundary_type": ["FlowBoundary", None]},
        geometry=[Point(0, 0), Point(2000, 0)],
        crs=CRS,
    ).to_file(ribasim_input, layer="boundaries")

    areas = tmp_path / "areas.gpkg"
    gpd.GeoDataFrame(
        {"code": ["A1", "A2", "A3"]},
        geometry=[box(-100, -100, 1000, 100), box(1000, -100, 1500, 100), box(-100, 100, 1000, 400)],
        crs=CRS,
    ).to_file(areas, layer="areas")
    gpd.GeoDataFrame({"CODE": ["D1"]}, geometry=[box(-200, -200, 2200, 500)], crs=CRS).to_file(
        areas, layer="drainage_areas"
    )
    return tmp_path


def test_read_boundaries_without_type(lumping_input: Path):
    network = read_hydamo_network(lumping_input / "hydamo.gpkg", dx=250.0)
    ribasim_input = lumping_input / "ribasim_input.gpkg"
    gpd.GeoDataFrame({"boundary_name": ["in", "out"]}, geometry=[Point(0, 0), Point(2000, 0)], crs=CRS).to_file(
        ribasim_input, layer="boundaries_untyped"
    )
    boundaries = read_boundaries(ribasim_input, "boundaries_untyped", network.nodes, buffer_distance=1.0)
    assert boundaries["boundary_type"].tolist() == ["LevelBoundary", "LevelBoundary"]


def test_lumped_model(lumping_input: Path):
    network = read_hydamo_network(lumping_input / "hydamo.gpkg", dx=250.0)
    assert len(network.edges) == 8
    areas = read_areas(lumping_input / "areas.gpkg", "areas")
    drainage_areas = read_layer(lumping_input / "areas.gpkg", "drainage_areas", explode=False, code_column="CODE")
    ribasim_input = lumping_input / "ribasim_input.gpkg"
    split_nodes, network = read_split_nodes(ribasim_input, "split_nodes", network, buffer_distance=1.0)
    # the weir is preferred over the culvert at the same location, and open water gets the ID of its edge
    assert split_nodes["split_node_id"].tolist() == ["S1", "H2___1"]
    assert split_nodes["object_type"].tolist() == ["stuw", "openwater"]
    boundaries = read_boundaries(ribasim_input, "boundaries", network.nodes, buffer_distance=1.0)
    assert boundaries["boundary_type"].tolist() == ["FlowBoundary", "LevelBoundary"]

    lumped = lump_network(network, split_nodes, boundaries, areas, remove_isolated=True)
    krw = gpd.GeoDataFrame({"meta_krw_id": ["K1"]}, geometry=[box(900, -50, 1100, 50)], crs=CRS)
    model = lumped_model(lumped, "NL99", krw, drainage_areas=drainage_areas, max_gap_area=5000.0)

    # the boundaries, the split nodes and the basins, without the basin between the open water and the LevelBoundary
    nodes = model.node.df
    assert nodes.index.tolist() == [1, 2, 3, 4, 5, 6]
    assert nodes["node_type"].tolist() == [
        "FlowBoundary",
        "LevelBoundary",
        "TabulatedRatingCurve",
        "ManningResistance",
        "Basin",
        "Basin",
    ]
    assert nodes.loc[[3, 4], "name"].tolist() == ["S1", "H2___1"]
    # the weir is on the KRW water body
    assert nodes["meta_categorie"].tolist() == ["doorgaand"] * 2 + ["hoofdwater"] + ["doorgaand"] * 3
    assert (nodes["meta_code"] == "NL99").all()

    links = model.link.df
    assert links.index.tolist() == [1, 2, 3, 4, 5]
    assert list(zip(links["from_node_id"], links["to_node_id"], strict=True)) == [
        (3, 6),
        (5, 3),
        (6, 4),
        (1, 5),
        (4, 2),
    ]

    # A3 is assigned to the basin of A1, with which it shares the longest border
    area = model.basin.area.df
    assert area is not None
    assert area["node_id"].tolist() == [5, 6]
    assert area.area.tolist() == [550_000.0, 100_000.0]

    assert model.tabulated_rating_curve.static.df is not None
    assert model.tabulated_rating_curve.static.df["node_id"].tolist() == [3, 3]
    assert model.basin.profile.df is not None
    assert model.basin.profile.df["node_id"].tolist() == [5, 5, 6, 6]
    assert model.pump.static.df is None

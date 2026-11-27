from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
import pytest
from ribasim_nl.lumping.connect import connect_lines, join_parts, snap_ends, split_at_junctions
from ribasim_nl.lumping.dhydro import read_ini_sections
from ribasim_nl.lumping.gkw_hydamo import prepare_hydroobjects, read_authority, select_hydroobjects
from ribasim_nl.lumping.hydamo import (
    apply_hydamo_changes,
    read_areas,
    read_boundaries,
    read_hydamo_network,
    read_network_without_bypasses,
    read_split_nodes,
)
from ribasim_nl.lumping.model import lumped_model, number_lost_basins, read_krw
from ribasim_nl.lumping.network import area_basins, assign_areas, fill_holes, find_bypasses, lump_network, separates
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


def test_apply_hydamo_changes_connects(tmp_path: Path):
    """With a connect distance, an added hydroobject splits the one it ends on, and the removals are checked."""
    source, removals, additions, target = (tmp_path / f"{name}.gpkg" for name in ("hydamo", "remove", "add", "target"))
    hydroobject = gpd.GeoDataFrame(
        {"code": ["H1", "H2"]},
        geometry=[LineString([(0, 0), (100, 0)]), LineString([(0, 50), (100, 50)])],
        crs=CRS,
    )
    hydroobject.to_file(source, layer="hydroobject")
    hydroobject.iloc[[1]].to_file(removals, layer="hydroobject")
    # ends 0.2 m beside H1, and has no code
    added = gpd.GeoDataFrame({"code": [None]}, geometry=[LineString([(40, 30), (40, 0.2)])], crs=CRS)
    added.to_file(additions, layer="hydroobject")

    apply_hydamo_changes(source, removals, additions, target, connect_distance=0.5)
    result = gpd.read_file(target, layer="hydroobject")
    assert result["code"].fillna("").tolist() == ["", "H1-0", "H1-1"]
    assert result.geometry.iloc[1].coords[-1] == result.geometry.iloc[2].coords[0] == (40.0, 0.2)
    # without a connect distance the hydroobjects stay as they are
    apply_hydamo_changes(source, removals, additions, target)
    assert gpd.read_file(target, layer="hydroobject")["code"].fillna("").tolist() == ["H1", ""]

    # a removal that removes nothing is an error, and so are two hydroobjects with the same code
    gpd.GeoDataFrame({"code": ["H9"]}, geometry=[LineString([(0, 0), (1, 1)])], crs=CRS).to_file(
        removals, layer="hydroobject"
    )
    with pytest.raises(AssertionError, match="no objects to remove with code \\['H9'\\]"):
        apply_hydamo_changes(source, removals, additions, target, connect_distance=0.5)
    hydroobject.iloc[[1]].to_file(removals, layer="hydroobject")
    added.assign(code="H1").to_file(additions, layer="hydroobject")
    with pytest.raises(AssertionError, match="several hydroobjects with the codes \\['H1'\\]"):
        apply_hydamo_changes(source, removals, additions, target, connect_distance=0.5)


def test_join_parts():
    lines = gpd.GeoDataFrame(
        {"code": ["A", "A", "B", "B", "C", None, None], "naam": ["a1", "a2", "b1", "b2", "c", "x", "y"]},
        geometry=[
            # the second part of A ends where the first starts
            LineString([(10, 0, 5), (20, 0, 5)]),
            LineString([(0, 0, 5), (10, 0, 5)]),
            # the parts of B do not follow each other
            LineString([(0, 10), (5, 10)]),
            LineString([(0, 20), (30, 20)]),
            # the parts of C do, in one geometry
            MultiLineString([[(0, 30), (10, 30)], [(10, 30), (20, 30)]]),
            LineString([(0, 40), (10, 40)]),
            LineString([(10, 40), (20, 40)]),
        ],
        crs=CRS,
    )
    joined = join_parts(lines)
    assert not joined.geometry.has_z.any()
    # the lines without code are not joined
    assert joined["code"].fillna("").tolist() == ["", "", "A", "B", "B.2", "C"]
    joined = joined.set_index("code")
    assert joined.geometry["A"].equals(LineString([(0, 0), (10, 0), (20, 0)]))
    assert joined.at["A", "naam"] == "a2"
    # the longest part keeps the code
    assert joined.geometry["B"].length == 30
    assert joined.geometry["B.2"].length == 5
    assert joined.geometry["C"].equals(LineString([(0, 30), (10, 30), (20, 30)]))


def test_snap_ends():
    lines = gpd.GeoDataFrame(
        {"code": ["A", "B", "C", "D"]},
        geometry=[
            LineString([(0, 0), (10, 0)]),
            LineString([(10.2, 0.1), (20, 0)]),
            LineString([(10.2, 0.1), (10, 10)]),
            # shorter than the tolerance
            LineString([(20, 0), (20.3, 0)]),
        ],
        crs=CRS,
    )
    snapped = snap_ends(lines, 0.5).set_index("code").geometry
    # the end of A moves to the coordinate that two lines start at
    assert snapped["A"].coords[-1] == (10.2, 0.1)
    assert snapped["B"].equals(lines.geometry[1])
    assert snapped["D"].equals(lines.geometry[3])
    assert snap_ends(lines, 0.1).geometry.equals(lines.geometry)


def test_split_at_junctions():
    lines = gpd.GeoDataFrame(
        {"code": ["B", "A", "C", "D", None, "E"]},
        geometry=[
            LineString([(0, 0), (50, 0), (100, 0)]),
            # ends 0.2 m beside B, and D starts on it
            LineString([(40, 30), (40, 0.2)]),
            # ends 0.3 m beyond the end of B
            LineString([(100.3, 20), (100.3, 0)]),
            LineString([(40, 20), (60, 20)]),
            # E ends on a line without code
            LineString([(0, 100), (100, 100)]),
            LineString([(50, 120), (50, 100)]),
        ],
        crs=CRS,
    )
    split = split_at_junctions(lines, 0.5)
    # the lines that are not split, then the pieces in order of the code of their line, those without code last
    assert split["code"].fillna("").tolist() == ["D", "E", "A-0", "A-1", "B-0", "B-1", "C-0", "", ""]
    geometry = split.set_index("code").geometry
    assert geometry["A-0"].equals(LineString([(40, 30), (40, 20)]))
    assert geometry["B-0"].equals(LineString([(0, 0), (40, 0.2)]))
    # B is extended to the end of C, which is renamed though it stays as it is
    assert geometry["B-1"].equals(LineString([(40, 0.2), (50, 0), (100, 0), (100.3, 0)]))
    assert geometry["C-0"].equals(lines.geometry[2])
    assert split.geometry.iloc[-2].equals(LineString([(0, 100), (50, 100)]))
    # nothing is left to connect
    pd.testing.assert_frame_equal(split_at_junctions(split, 0.5), split)
    with pytest.raises(AssertionError, match="named after its code"):
        split_at_junctions(pd.concat([lines, lines.iloc[[0]]]), 0.5)


def test_connect_lines():
    """Ends that nearly meet are joined, instead of extending one line to the other and renaming it."""
    lines = gpd.GeoDataFrame(
        {"code": ["A", "B"]},
        geometry=[LineString([(0, 0, 1), (10, 0, 1)]), LineString([(10.0005, 0, 1), (20, 0, 1)])],
        crs=CRS,
    )
    assert split_at_junctions(lines, 0.5)["code"].tolist() == ["A-0", "B-0"]
    connected = connect_lines(lines, 0.5)
    assert connected["code"].tolist() == ["A", "B"]
    assert connected.geometry[0].coords[-1] == connected.geometry[1].coords[0]
    assert not connected.geometry.has_z.any()


def test_select_hydroobjects():
    hydroobject = gpd.GeoDataFrame(
        {"code": ["P", "S", "T"], "categorieoppwaterlichaam": ["primair", "secundair", "tertiair"]},
        geometry=[LineString([(0, 0), (10, 0)]), LineString([(10, 0), (10, 10)]), LineString([(10, 0), (20, 0)])],
        crs=CRS,
    )
    assert select_hydroobjects(hydroobject, ["primair"])["code"].tolist() == ["P"]
    assert select_hydroobjects(hydroobject, ["primair"], ["T"])["code"].tolist() == ["P", "T"]
    with pytest.raises(AssertionError, match="no hydroobjects with codes \\['X'\\]"):
        select_hydroobjects(hydroobject, ["primair"], ["X"])
    with pytest.raises(AssertionError, match="no hydroobjects of categories"):
        select_hydroobjects(hydroobject, ["overig"])
    # a hydroobject that the categories select already is not selected by its code
    with pytest.raises(AssertionError, match="are of categories"):
        select_hydroobjects(hydroobject, ["primair"], ["P"])
    prepared = prepare_hydroobjects(hydroobject, ["secundair"])
    assert prepared["code"].tolist() == ["S"]


def test_read_authority(tmp_path: Path):
    """The objects of a water authority are selected by their nen3610id, since codes are not unique nationally."""
    package = tmp_path / "package.gpkg"
    ids = ["NL.WBHCODE.25.Hydroobject.1", "NL.WBHCODE.27.Hydroobject.1", "NL.WBHCODE.2.Hydroobject.1"]
    line = LineString([(0, 0), (10, 0)])
    gpd.GeoDataFrame({"code": ["H1", "H1", "H1"], "nen3610id": ids}, geometry=[line] * 3, crs=CRS).to_file(
        package, layer="hydroobject"
    )
    gpd.GeoDataFrame({"code": ["S1"], "nen3610id": ["NL.WBHCODE.27.Stuw.1"]}, geometry=[Point(0, 0)], crs=CRS).to_file(
        package, layer="stuw"
    )
    pyogrio.write_dataframe(
        pd.DataFrame({"code": ["P1"], "nen3610id": ["NL.WBHCODE.25.Pomp.1"]}), package, layer="pomp"
    )

    hydroobject = read_authority("BrabantseDelta", "hydroobject", package)
    assert hydroobject["nen3610id"].tolist() == ["NL.WBHCODE.25.Hydroobject.1"]
    assert read_authority("BrabantseDelta", "stuw", package).empty
    with pytest.raises(AssertionError, match="no geometries"):
        read_authority("BrabantseDelta", "pomp", package)


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


@pytest.fixture
def bypass_input(tmp_path: Path) -> Path:
    """A watercourse from x=0 to 200 with a weir at x=100, and a newer fish passage from x=90 to 110 around it.

    The watercourse is in three pieces, as `ribasim_nl.lumping.connect` splits it where the fish passage ends on it.
    It flows from a FlowBoundary at x=0 to a LevelBoundary at x=200.
    """
    hydamo = tmp_path / "hydamo.gpkg"
    gpd.GeoDataFrame(
        {
            "code": ["A-0", "A-1", "A-2", "F"],
            "created_date": [None, None, None, "2025-05-19"],
        },
        geometry=[
            LineString([(0, 0), (90, 0)]),
            LineString([(90, 0), (110, 0)]),
            LineString([(110, 0), (200, 0)]),
            LineString([(90, 0), (100, 10), (110, 0)]),
        ],
        crs=CRS,
    ).to_file(hydamo, layer="hydroobject")
    gpd.GeoDataFrame({"code": ["S1"]}, geometry=[Point(100, 0)], crs=CRS).to_file(hydamo, layer="stuw")
    ribasim_input = tmp_path / "ribasim_input.gpkg"
    gpd.GeoDataFrame(
        # the split node takes its ID and type from the weir
        {"split_node": [1], "split_node_id": [None], "object_type": [None], "object_function": [""]},
        geometry=[Point(100, 0)],
        crs=CRS,
    ).to_file(ribasim_input, layer="split_nodes")
    gpd.GeoDataFrame(
        {"boundary_name": ["in", "out"], "boundary_type": ["FlowBoundary", None]},
        geometry=[Point(0, 0), Point(200, 0)],
        crs=CRS,
    ).to_file(ribasim_input, layer="boundaries")
    return tmp_path


def test_find_bypasses(bypass_input: Path):
    """The fish passage bypasses the weir, and the way around it is the fish passage and the pieces next to it."""
    network = read_hydamo_network(bypass_input / "hydamo.gpkg", dx=250.0)
    assert network.edges["hydroobject"].tolist() == ["A-0", "A-1", "A-2", "F"]
    split_nodes, network = read_split_nodes(bypass_input / "ribasim_input.gpkg", "split_nodes", network, 1.0)
    graph, bypasses = find_bypasses(network, split_nodes)
    (bypass,) = bypasses
    edges = network.edges.set_index("edge_no")
    assert set(edges.loc[bypass.edge_nos, "hydroobject"]) == {"A-1", "F"}
    fish_passage = list(edges.index[edges["hydroobject"] == "F"])
    anchors = {n for n in graph if n >= 998_000_000_000}
    # without the fish passage the weir separates, but a side without another split node is not enough
    assert not separates(graph, bypass, fish_passage, anchors)
    boundaries = read_boundaries(bypass_input / "ribasim_input.gpkg", "boundaries", network.nodes, buffer_distance=1.0)
    assert separates(graph, bypass, fish_passage, anchors | set(boundaries["node_no"]))
    # without the network the split node does not
    network = read_hydamo_network(bypass_input / "hydamo.gpkg", dx=250.0, exclude=["F"])
    split_nodes, network = read_split_nodes(bypass_input / "ribasim_input.gpkg", "split_nodes", network, 1.0)
    assert find_bypasses(network, split_nodes)[1] == []


def test_read_network_without_bypasses(bypass_input: Path):
    """The newer fish passage is left out, but not if it is longer than the limit, or as old as the weir."""
    hydamo, ribasim_input = bypass_input / "hydamo.gpkg", bypass_input / "ribasim_input.gpkg"
    split_nodes, network = read_network_without_bypasses(hydamo, ribasim_input, dx=250.0, buffer_distance=1.0)
    assert "F" not in set(network.edges["hydroobject"])
    assert split_nodes["split_node_id"].tolist() == ["S1"]
    with pytest.raises(AssertionError, match="S1: the way around it is \\['A-1', 'F'\\]"):
        read_network_without_bypasses(hydamo, ribasim_input, dx=250.0, buffer_distance=1.0, max_bypass_length=20.0)
    hydroobject = gpd.read_file(hydamo, layer="hydroobject")
    hydroobject.assign(created_date=None).to_file(hydamo, layer="hydroobject")
    with pytest.raises(AssertionError, match="split nodes that are bypassed"):
        read_network_without_bypasses(hydamo, ribasim_input, dx=250.0, buffer_distance=1.0)


def test_read_split_nodes_off_network(bypass_input: Path):
    """A split node that is not on the network is an error, unless it is known."""
    ribasim_input = bypass_input / "ribasim_input.gpkg"
    gpd.GeoDataFrame(
        {"split_node": [1, 2], "split_node_id": [None, "S2"], "object_type": [None, "stuw"], "object_function": ""},
        geometry=[Point(100, 0), Point(50, 30)],
        crs=CRS,
    ).to_file(ribasim_input, layer="split_nodes")
    network = read_hydamo_network(bypass_input / "hydamo.gpkg", dx=250.0, exclude=["F"])
    with pytest.raises(AssertionError, match="not on the network"):
        read_split_nodes(ribasim_input, "split_nodes", network, buffer_distance=1.0)
    split_nodes, _ = read_split_nodes(ribasim_input, "split_nodes", network, buffer_distance=1.0, off_network=["S2"])
    assert split_nodes["split_node_id"].tolist() == ["S1"]
    with pytest.raises(AssertionError, match="on the network after all"):
        read_split_nodes(ribasim_input, "split_nodes", network, buffer_distance=1.0, off_network=["S1", "S2"])


def test_apply_hydamo_changes_removes_pieces(tmp_path: Path):
    """With connected lines, a removal by code also removes the pieces the line was split into."""
    source, removals, additions, target = (tmp_path / f"{name}.gpkg" for name in ("hydamo", "remove", "add", "target"))
    gpd.GeoDataFrame(
        {"code": ["H1-0", "H1-1", "H10", "H2"]},
        geometry=[
            LineString([(0, 0), (50, 0)]),
            LineString([(50, 0), (100, 0)]),
            LineString([(0, 9), (9, 9)]),
            LineString([(0, 50), (100, 50)]),
        ],
        crs=CRS,
    ).to_file(source, layer="hydroobject")
    gpd.GeoDataFrame({"code": ["H1"]}, geometry=[LineString([(0, 0), (1, 1)])], crs=CRS).to_file(
        removals, layer="hydroobject"
    )
    gpd.GeoDataFrame({"code": []}, geometry=[], crs=CRS).to_file(additions, layer="hydroobject")
    apply_hydamo_changes(source, removals, additions, target, connect_distance=0.5)
    assert sorted(gpd.read_file(target, layer="hydroobject")["code"]) == ["H10", "H2"]
    # without connecting, as for the data of 2024, only the code itself
    apply_hydamo_changes(source, removals, additions, target)
    assert sorted(gpd.read_file(target, layer="hydroobject")["code"]) == ["H1-0", "H1-1", "H10", "H2"]

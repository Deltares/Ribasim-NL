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
from ribasim_nl.lumping.model import lumped_model, read_krw
from ribasim_nl.lumping.network import create_basin_areas, lump_network
from ribasim_nl.lumping.utils import (
    merge_grouped,
    overlay_intersection,
    quicksort_order,
    read_layer,
    split_edges_by_dx,
)
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


def test_overlay_intersection():
    df1 = gpd.GeoDataFrame({"name": ["A", "B"]}, geometry=[box(0, 0, 2, 2), box(2, 0, 4, 2)], crs=CRS)
    df2 = gpd.GeoDataFrame({"name": ["C", "D"]}, geometry=[box(1, 0, 3, 1), box(1, 1, 3, 2)], crs=CRS)
    result = overlay_intersection(df1, df2)
    # grouped by the polygon of df2, as in geopandas < 1
    assert result[["name_1", "name_2"]].to_numpy().tolist() == [["A", "C"], ["B", "C"], ["A", "D"], ["B", "D"]]
    assert result.area.tolist() == [1.0, 1.0, 1.0, 1.0]
    assert result.crs == CRS


def test_overlay_intersection_polygons_only():
    df1 = gpd.GeoDataFrame({"a": [1]}, geometry=[box(0, 0, 2, 2)], crs=CRS)
    # overlaps the right half of the square and touches its top, which gives a polygon and a line
    l_shape = Polygon([(1, 0), (3, 0), (3, 3), (0, 3), (0, 2), (1, 2)])
    # only touches the square
    touching = box(0, 2, 2, 3)
    df2 = gpd.GeoDataFrame({"b": [1, 2]}, geometry=[l_shape, touching], crs=CRS)
    result = overlay_intersection(df1, df2)
    assert result["b"].tolist() == [1]
    assert result.geom_type.tolist() == ["Polygon"]
    assert result.area.tolist() == [2.0]


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


def test_create_basin_areas_degenerate_area():
    edges = gpd.GeoDataFrame({"basin": [0]}, geometry=[LineString([(0, 0), (100, 0)])], crs=CRS)
    # as in 2024, an area without geometry gets no basin
    areas = gpd.GeoDataFrame({"code": ["A1", "A2"], "area": [1, 2]}, geometry=[box(0, -10, 50, 10), None], crs=CRS)
    areas, _ = create_basin_areas(edges, areas)
    assert areas.set_index("code")["basin"].to_dict() == {"A1": 0, "A2": -1}
    # a line and a point, when made valid
    degenerate = MultiPolygon([Polygon([(0, 0), (1, 0), (2, 0)]), Polygon([(5, 5)] * 4)])
    areas = gpd.GeoDataFrame(
        {"code": ["A1", "A2"], "area": [1, 2]}, geometry=[box(0, -10, 50, 10), degenerate], crs=CRS
    )
    with pytest.raises(AssertionError, match="area A2 has no polygon"):
        create_basin_areas(edges, areas)


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

    lumped = lump_network(
        network,
        split_nodes,
        boundaries,
        areas,
        drainage_areas=drainage_areas,
        remove_isolated=True,
        remove_holes_min_area=5000.0,
    )
    krw = gpd.GeoDataFrame({"meta_krw_id": ["K1"]}, geometry=[box(900, -50, 1100, 50)], crs=CRS)
    model = lumped_model(lumped, "NL99", krw)

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

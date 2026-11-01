from pathlib import Path

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import pytest
from ribasim_nl.crossings.model import Options, boezem_model, to_object
from ribasim_nl.crossings.parse import ParseCrossings, pandas2_strings, sort_index_2024, sort_values_2024
from ribasim_nl.crossings.shortest_path import connect_components, shortest_paths
from shapely.geometry import LineString, Point, box

CRS = "EPSG:28992"

# values whose order with the unstable sort of numpy 1.26 differs from a stable sort, see test_quicksort_order
VALUES = [4, 1, 1, 3, 0, 2, 2, 2, 4, 4, 1, 2, 0, 4, 4, 2, 2]
ORDER = [4, 12, 1, 2, 10, 16, 5, 6, 7, 15, 11, 3, 14, 0, 9, 13, 8]


def test_sort_index_2024():
    df = pd.DataFrame({"row": range(len(VALUES))}, index=VALUES)
    assert sort_index_2024(df)["row"].tolist() == ORDER
    # a monotonic index is not sorted again, which would reorder its duplicates
    df = pd.DataFrame({"row": [2, 0, 1]}, index=[0, 1, 1])
    assert sort_index_2024(df) is df


def test_sort_values_2024():
    values = [*VALUES[:3], np.nan, *VALUES[3:], np.nan]
    df = pd.DataFrame({"value": values, "row": range(len(values))})
    rows = np.array([i for i, v in enumerate(values) if not np.isnan(v)])
    # the missing values last, in their order
    assert sort_values_2024(df, "value")["row"].tolist() == [*rows[ORDER], 3, len(values) - 1]


def test_pandas2_strings():
    def missing() -> tuple[object, object]:
        series = pd.Series(["a", None])
        return series.dtype, series.iloc[1]

    # strings as object, missing as None, which do not differ from each other, unlike NaN
    dtype, value = pandas2_strings(missing)()
    assert pd.api.types.is_object_dtype(dtype)
    assert value is None


def test_to_object():
    df = to_object(pd.DataFrame({"a": pd.Series(["x", None], dtype="str"), "b": [1.0, 2.0]}))
    assert pd.api.types.is_object_dtype(df["a"])
    assert df["a"].tolist() == ["x", None]
    assert df["b"].dtype == float


def test_connect_components():
    graph = nx.Graph()
    xy = {1: (0, 0), 2: (10, 0), 3: (20, 0), 4: (23, 0), 5: (23, 5), 6: (23, 20)}
    for node, point in xy.items():
        graph.add_node(node, geometry=Point(point))
    graph.add_edge(1, 2)
    graph.add_edge(2, 3)
    graph.add_edge(4, 5)
    connect_components(graph)
    assert nx.is_connected(graph)
    # first the closest component, then node 6, which is then closer to node 5 than to node 3
    added = [edge for edge in graph.edges if edge not in [(1, 2), (2, 3), (4, 5)]]
    assert sorted(tuple(sorted(edge)) for edge in added) == [(3, 4), (5, 6)]
    # as in 2024, without a length, so the shortest paths count them as 1
    assert "length" not in graph.edges[5, 6]
    assert graph.edges[5, 6]["geometry"].equals(LineString([(23, 5), (23, 20)]))


@pytest.fixture
def postprocessed(tmp_path: Path) -> Path:
    """The postprocessed data of a boezem A and a peilgebied B with a gemaal between them, and a culvert in B."""
    path = tmp_path / "postprocessed.gpkg"
    areas = [box(0, 0, 100, 100), box(100, 0, 200, 100)]
    layers = {
        "hydroobject": gpd.GeoDataFrame(
            {"code": ["h1", "h2"], "nen3610id": ["n1", "n2"], "globalid": ["h1", "h2"]},
            geometry=[LineString([(20, 50), (100, 50)]), LineString([(100, 50), (180, 50)])],
            crs=CRS,
        ),
        "gemaal": gpd.GeoDataFrame(
            {
                "code": ["g1"],
                "nen3610id": ["n3"],
                "globalid": ["g1"],
                "func_afvoer": [1],
                "func_aanvoer": [0],
                "func_circulatie": [0],
            },
            geometry=[Point(100, 51)],
            crs=CRS,
        ),
        "stuw": gpd.GeoDataFrame(
            {"code": ["s1"], "nen3610id": ["n4"], "globalid": ["s1"]}, geometry=[Point(150, 90)], crs=CRS
        ),
        "duikersifonhevel": gpd.GeoDataFrame(
            {"code": ["d1"], "nen3610id": ["n5"], "globalid": ["d1"]},
            geometry=[LineString([(140, 50), (150, 50)])],
            crs=CRS,
        ),
        "peilgebied": gpd.GeoDataFrame(
            {"code": ["A", "B"], "nen3610id": ["n6", "n7"], "globalid": ["pA", "pB"], "peilgebied_cat": [1, 0]},
            geometry=areas,
            crs=CRS,
        ),
        "streefpeil": gpd.GeoDataFrame(
            {"waterhoogte": [-0.4, -1.0], "globalid": ["pA", "pB"]}, geometry=[None, None], crs=CRS
        ),
        "aggregation_area": gpd.GeoDataFrame(
            {"code": ["A", "B"], "nen3610id": ["n8", "n9"], "globalid": ["aA", "aB"]}, geometry=areas, crs=CRS
        ),
    }
    for layer, df in layers.items():
        df.to_file(path, layer=layer)
    return path


def test_boezem_model(tmp_path: Path, postprocessed: Path):
    """The crossings, shortest paths and input model of a boezem and a peilgebied."""
    cross = ParseCrossings(postprocessed, agg_peilgebieden_layer="aggregation_area", agg_peilgebieden_column="code")
    df_hydro, df_culverts, df_crossings = cross.find_crossings_with_peilgebieden("hydroobject", "duikersifonhevel")
    crossings_path = tmp_path / "crossings.gpkg"
    cross.write_crossings(crossings_path, df_hydro, "duikersifonhevel", df_culverts, df_crossings)

    crossings = df_crossings[df_crossings["agg_areas_in_use"]]
    assert len(crossings) == 1
    crossing = crossings.iloc[0]
    # the gemaal pumps from B to the boezem A, against the direction of the hydro-objects
    assert (crossing["peilgebied_from"], crossing["peilgebied_to"], crossing["gemaal"]) == ("pB", "pA", "g1")
    assert (crossing["agg_area_from"], crossing["agg_area_to"]) == ("B", "A")
    assert crossing.geometry.distance(Point(100, 50)) < 1e-6

    paths = shortest_paths(crossings_path)
    shortest_path_path = tmp_path / "shortest_path.gpkg"
    paths.to_file(shortest_path_path, layer="shortest_path")

    model, checks = boezem_model(postprocessed, crossings_path, shortest_path_path, Options())
    node = model.node.df
    # the basins are numbered in order of appearance in the crossings, B before A, followed by the crossings
    assert node["node_type"].tolist() == ["Basin", "Basin", "Pump"]
    assert model.link.df[["from_node_id", "to_node_id"]].to_numpy().tolist() == [[1, 3], [3, 2]]
    assert model.basin.state.df["meta_categorie"].tolist() == ["doorgaand", "hoofdwater"]
    assert model.pump.static.df["meta_func_afvoer"].tolist() == [1]
    assert set(checks) >= {"sinks", "sources", "boezem", "inlaten_uitlaten_boezems"}


def test_excluded_crossings(postprocessed: Path):
    kwargs = {"agg_peilgebieden_layer": "aggregation_area", "agg_peilgebieden_column": "code"}
    # as found, in the direction of the hydro-object, before the gemaal reverses it
    cross = ParseCrossings(postprocessed, excluded_crossings=[("pA", "pB", 100.0, 50.0)], **kwargs)
    df_hydro, _, df_crossings = cross.find_crossings_with_peilgebieden("hydroobject", "duikersifonhevel")
    assert len(df_hydro) == 0
    assert len(df_crossings) == 0

    cross = ParseCrossings(postprocessed, excluded_crossings=[("pB", "pA", 100.0, 50.0)], **kwargs)
    with pytest.raises(AssertionError, match="were not found"):
        cross.find_crossings_with_peilgebieden("hydroobject", "duikersifonhevel")

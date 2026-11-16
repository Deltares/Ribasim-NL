import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from ribasim_nl.crossings.aggregation import (
    aggregation_area_of,
    merge_small,
    refine_aggregation_areas,
    shared_boundaries,
)
from shapely.geometry import box

CRS = "EPSG:28992"


def _peilgebieden() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    """Peilgebieden of two polders, the delivered aggregation areas, and their target levels.

    Polder P has the large peilgebieden A and B beside each other, with the small C and D on top of A and B, and the
    small E, without a level, on top of C. Polder Q has only the small F, which touches B.
    """
    polygons = {
        "A": box(0, 0, 100, 100),
        "B": box(100, 0, 200, 100),
        "C": box(0, 100, 100, 110),
        "D": box(100, 100, 200, 110),
        "E": box(0, 110, 100, 115),
        "F": box(200, 0, 210, 100),
    }
    peilgebied = gpd.GeoDataFrame(
        {"code": list(polygons), "globalid": [f"id_{code}" for code in polygons], "peilgebied_cat": 0},
        geometry=list(polygons.values()),
        crs=CRS,
    )
    areas = gpd.GeoDataFrame({"code": ["P", "Q"]}, geometry=[box(0, 0, 200, 115), box(200, 0, 210, 100)], crs=CRS)
    levels = {"id_A": -2.0, "id_B": -1.0, "id_C": -1.1, "id_D": -1.9, "id_F": -1.0}
    streefpeil = pd.DataFrame({"globalid": list(levels), "waterhoogte": list(levels.values())})
    return peilgebied, areas, streefpeil


def test_aggregation_area_of():
    peilgebied, areas, _ = _peilgebieden()
    # a peilgebied that the aggregation areas cover for 75%, and one of another category
    peilgebied.loc[len(peilgebied)] = ["G", "id_G", 0, box(-10, 0, 30, 100)]
    peilgebied.loc[len(peilgebied)] = ["H", "id_H", 2, box(0, 0, 10, 10)]
    area_of = aggregation_area_of(peilgebied, areas, "code", 0.8)
    assert area_of.tolist() == ["P", "P", "P", "P", "P", "Q", None, None]
    assert aggregation_area_of(peilgebied, areas, "code", 0.7).tolist()[6] == "P"


def test_shared_boundaries():
    polygons = np.array([box(0, 0, 100, 100), box(100.2, 0, 200, 100), box(0, 100, 5, 200), box(100, 100, 200, 200)])
    shared = shared_boundaries(polygons)
    # a gap of 0.2 m still shares the boundary; polygons 0 and 3, and 2 and 3, only touch or come near at a corner
    assert shared[["a", "b"]].to_numpy().tolist() == [[0, 1], [0, 2], [1, 3]]
    # up to the tolerance of 0.5 m longer at each end
    assert shared["length"].round().tolist() == [101.0, 6.0, 101.0]


def test_merge_small():
    peilgebied, _, streefpeil = _peilgebieden()
    polygons = peilgebied.geometry.to_numpy()
    area = peilgebied.area.to_numpy()
    level = peilgebied["globalid"].map(streefpeil.set_index("globalid")["waterhoogte"]).to_numpy(dtype=float).copy()
    within = np.array(["P", "P", "P", "P", "P", "Q"])
    shared = shared_boundaries(polygons)
    # nothing is smaller than 100 m2, but E has no level, so it joins C, its only neighbour
    assert merge_small(area, level, within, shared, 100.0).tolist() == [0, 1, 2, 3, 2, 5]
    # E joins C, its only neighbour; together they are 1500 m2. F has no neighbour in its polder.
    assert merge_small(area, level, within, shared, 1000.0).tolist() == [0, 1, 2, 3, 2, 5]
    # D joins C with E, beside it, whose level is closer than that of B below it
    assert merge_small(area, level, within, shared, 2000.0).tolist() == [0, 1, 2, 2, 2, 5]
    # together they have the level of C, the largest, and join B, whose level is closer than that of A
    assert merge_small(area, level, within, shared, 5000.0).tolist() == [0, 1, 1, 1, 1, 5]
    # a large peilgebied without a level joins a neighbour with one: A joins B, of the two with the longest boundary
    level[0] = np.nan
    assert merge_small(area, level, within, shared, 100.0).tolist() == [1, 1, 2, 3, 2, 5]
    # without any level, the peilgebieden of a polder join into one
    unknown = np.full(len(area), np.nan)
    assert merge_small(area, unknown, within, shared, 100.0).tolist() == [0, 0, 0, 0, 0, 5]


def test_refine_aggregation_areas():
    peilgebied, areas, streefpeil = _peilgebieden()
    kwargs = {"column": "code", "threshold": 0.8}
    # every peilgebied with a target level its own area; E, without one, joins C
    refined, area_of = refine_aggregation_areas(peilgebied, areas, streefpeil, min_area=0.0, **kwargs)
    assert refined["code"].tolist() == ["A", "B", "C", "D", "F"]
    assert area_of.tolist() == ["A", "B", "C", "D", "C", "F"]
    assert refined["delivered"].tolist() == ["P", "P", "P", "P", "Q"]

    refined, area_of = refine_aggregation_areas(peilgebied, areas, streefpeil, min_area=5000.0, **kwargs)
    assert refined["code"].tolist() == ["A", "B", "F"]
    assert area_of.tolist() == ["A", "B", "B", "B", "B", "F"]
    assert refined["streefpeil"].tolist() == [-2.0, -1.0, -1.0]
    assert refined.area.tolist() == [10000.0, 12500.0, 1000.0]
    assert refined["globalid"].is_unique

    # an area is named after its largest peilgebied, and has its level: D, E and C here, of which C is the largest
    peilgebied = peilgebied.loc[[2, 3, 4]].reset_index(drop=True)
    refined, area_of = refine_aggregation_areas(peilgebied, areas, streefpeil, min_area=2500.0, **kwargs)
    assert refined["code"].tolist() == ["C"]
    assert refined["streefpeil"].tolist() == [-1.1]
    assert area_of.tolist() == ["C", "C", "C"]

    peilgebied.loc[1, "code"] = "C"
    with pytest.raises(AssertionError, match="not unique"):
        refine_aggregation_areas(peilgebied, areas, streefpeil, min_area=0.0, **kwargs)


def test_refine_aggregation_areas_cleans_levels():
    peilgebied, areas, streefpeil = _peilgebieden()
    kwargs = {"column": "code", "threshold": 0.8, "min_area": 0.0}
    # B has the no-data level 99 m, so it has no level, and joins A, of its neighbours A and D the first
    streefpeil.loc[streefpeil["globalid"] == "id_B", "waterhoogte"] = 99.0
    with pytest.raises(AssertionError, match="out of range"):
        refine_aggregation_areas(peilgebied, areas, streefpeil, **kwargs)
    refined, area_of = refine_aggregation_areas(peilgebied, areas, streefpeil, no_data_levels=(99.0,), **kwargs)
    assert area_of.tolist() == ["A", "A", "C", "D", "C", "F"]
    assert refined["streefpeil"].tolist() == [-2.0, -1.1, -1.9, -1.0]
    # a wrong level is corrected
    refined, _ = refine_aggregation_areas(
        peilgebied, areas, streefpeil, no_data_levels=(99.0,), corrections={-2.0: -2.5}, **kwargs
    )
    assert refined["streefpeil"].tolist() == [-2.5, -1.1, -1.9, -1.0]

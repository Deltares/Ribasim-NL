import geopandas as gpd
import numpy as np
from ribasim_nl.profiles.depth import depth_from_hydrotopes
from ribasim_nl.profiles.hydrotopes import Hydrotope, HydrotopeTable
from shapely.geometry import LineString, box


def test_depth_from_hydrotopes():
    table = HydrotopeTable(
        Hydrotope(1, "a", (1.0, 1.5, 2.0, 2.5)),
        Hydrotope(2, "b", (2.0, 2.5, 3.0, 3.5)),
        Hydrotope(3, "no depth", (np.nan, np.nan, np.nan, np.nan)),
    )
    # the hydrotope without depth comes first, so it would win a tie on row order
    hydrotope_map = gpd.GeoDataFrame(
        {"HYDROTYPE2": [3, 1, 2]}, geometry=[box(0, 10, 10, 20), box(0, 0, 10, 10), box(10, 0, 20, 10)]
    )
    hydro_objects = gpd.GeoDataFrame(
        {"width": [0.5, 0.5, 0.5, 2.0]},
        geometry=[
            LineString([(1, 1), (9, 1)]),  # within hydrotope 1
            LineString([(11, 1), (19, 1)]),  # within hydrotope 2
            LineString([(1, 10), (9, 10)]),  # on the boundary of hydrotopes 1 and 3
            LineString([(8, 5), (18, 5)]),  # mostly in hydrotope 2
        ],
        index=[40, 30, 20, 10],
    )
    result = depth_from_hydrotopes(hydro_objects, hydrotope_map, table, drop_na=False)

    # each hydro-object gets the hydrotope it overlaps most, by its own label
    assert result.index.tolist() == [40, 30, 20, 10]
    assert result["ht_code"].tolist() == [1, 2, 1, 2]
    assert result["depth"].tolist() == [1.0, 2.0, 1.0, 2.5]

import geopandas as gpd
import pandas as pd
import pytest
import rasterio
import shapely
from ribasim import Node
from ribasim.nodes import basin, outlet
from ribasim_nl.berging import VdGaastBerging, get_manning_resistance, parse_meta_aanvoer, supply_area_basin_ids
from ribasim_nl.profiles.implement import bergend_manning_static
from shapely.geometry import MultiPolygon, Point, box

from ribasim_nl import CloudStorage, Model, berging


def add_basin(model: Model, node_id: int, point: Point, area: shapely.Polygon, **kwargs) -> None:
    model.basin.add(
        Node(node_id, point, **kwargs),
        [
            basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
            basin.State(level=[0.5]),
            basin.Area(geometry=[MultiPolygon([area])], meta_streefpeil=[0.5]),
        ],
    )


@pytest.fixture
def model() -> Model:
    """Doorgaand Basins 1, 2 and 3 of 1 km2 next to each other, with Outlet 4 from Basin 1 to Basin 2."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id in (1, 2, 3):
        x = (node_id - 1) * 1000
        add_basin(model, node_id, Point(x, 0), box(x - 500, -500, x + 500, 500), meta_categorie="doorgaand")
    model.outlet.add(Node(4, Point(500, 0)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.outlet[4])
    model.link.add(model.outlet[4], model.basin[2])
    return model


def test_supply_area_basin_ids(model):
    add_basin(model, 5, Point(10, 0), box(-500, -500, 500, 500), meta_categorie="bergend")
    # all of Basin 1, 70% of Basin 2 and 30% of Basin 3; not the bergend Basin 5
    supply_areas = gpd.GeoDataFrame(
        {"meta_aanvoer": [True, True]},
        geometry=[box(-500, -500, 1200, 500), box(2200, -500, 2800, 500)],
        crs=model.crs,
    )
    assert supply_area_basin_ids(model, supply_areas) == {1, 2}
    assert supply_area_basin_ids(model, supply_areas, min_overlap=0.8) == {1}


def test_supply_area_basin_ids_ignores_missing_geometry(model):
    supply_areas = gpd.GeoDataFrame(
        {"meta_aanvoer": [1.0, 1.0]}, geometry=[box(-500, -500, 500, 500), None], crs=model.crs
    )
    assert supply_area_basin_ids(model, supply_areas) == {1}


def test_supply_area_basin_ids_only_supplied(model):
    """Only the supply areas with meta_aanvoer true select basins."""
    geometry = [box(-500, -500, 500, 500), box(500, -500, 1500, 500), box(1500, -500, 2500, 500)]
    supply_areas = gpd.GeoDataFrame({"meta_aanvoer": ["true", 0.0, None]}, geometry=geometry, crs=model.crs)
    assert supply_area_basin_ids(model, supply_areas) == {1}
    supply_areas["meta_aanvoer"] = False
    assert supply_area_basin_ids(model, supply_areas) == set()


def test_parse_meta_aanvoer():
    values = pd.Series([True, False, 1.0, 0.0, 1, "true", "False", "TRUE", None, float("nan")], dtype=object)
    expected = [True, False, True, False, True, True, False, True, False, False]
    assert parse_meta_aanvoer(values).tolist() == expected
    with pytest.raises(AssertionError, match="Unknown meta_aanvoer values"):
        parse_meta_aanvoer(pd.Series(["ja", 2.0]))


def test_get_manning_resistance():
    area = 2.5e6
    static = get_manning_resistance(area).df
    expected = bergend_manning_static(pd.Series([area]))
    assert static is not None
    columns = ["length", "manning_n", "profile_width", "profile_slope"]
    pd.testing.assert_frame_equal(static[columns].reset_index(drop=True), expected[columns])


def test_vd_gaast_berging_manning_basin_ids(model, monkeypatch, tmp_path):
    """Storing basins of the manning_basin_ids get a ManningResistance, the others a TabulatedRatingCurve."""

    # skip sampling the LHM and Van der Gaast rasters
    def add_basin_statistics(df, **kwargs):
        return df.assign(ghg=0.5, glg=1.5, ma=10.0, maaiveld=2.0, maaiveld_min=1.0, maaiveld_max=3.0)

    monkeypatch.setattr(berging, "add_basin_statistics", add_basin_statistics)
    monkeypatch.setattr(berging, "get_basin_profile", lambda **kwargs: basin.Profile(level=[0.0, 3.0], area=[1e4, 2e4]))

    VdGaastBerging(model=model, cloud=CloudStorage(tmp_path), manning_basin_ids={1, 3}).add()

    link_df = model.link.df
    nodes = model.node.df
    for basin_id, node_type in [(1, "ManningResistance"), (2, "TabulatedRatingCurve"), (3, "ManningResistance")]:
        from_ids = link_df.loc[link_df["to_node_id"] == basin_id, "from_node_id"]
        connector_id = from_ids[nodes.loc[from_ids, "meta_categorie"].to_numpy() == "bergend"].item()
        assert nodes.at[connector_id, "node_type"] == node_type
        bergend_id = link_df.loc[link_df["to_node_id"] == connector_id, "from_node_id"].item()
        assert nodes.at[bergend_id, "meta_categorie"] == "bergend"

    static_df = model.manning_resistance.static.df
    assert static_df is not None
    assert len(static_df) == 2
    assert (static_df["length"] == bergend_manning_static(pd.Series([1e6]))["length"].item()).all()


def test_vd_gaast_berging_unknown_manning_basin_ids(model, tmp_path):
    with pytest.raises(AssertionError, match="not Basins"):
        VdGaastBerging(model=model, cloud=CloudStorage(tmp_path), manning_basin_ids={99}).add()


def test_band_index():
    profile = {"driver": "GTiff", "width": 1, "height": 1, "count": 2, "dtype": "float32"}
    with rasterio.MemoryFile() as memfile:
        with memfile.open(**profile) as dst:
            dst.descriptions = ("maaiveld", "opp_primair")
        with memfile.open() as src:
            assert berging.band_index(src, "maaiveld") == 1
            assert berging.band_index(src, "opp_primair") == 2
            with pytest.raises(AssertionError, match="opp_secundair"):
                berging.band_index(src, "opp_secundair")

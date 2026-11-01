import geopandas as gpd
import pytest
from ribasim import Node
from ribasim.nodes import basin, flow_boundary, level_boundary, outlet
from shapely.geometry import LineString, MultiPolygon, Point, Polygon

from ribasim_nl import Model


def square(x: float) -> MultiPolygon:
    return MultiPolygon([Polygon([(x - 5, -5), (x + 5, -5), (x + 5, 5), (x - 5, 5)])])


@pytest.fixture
def model() -> Model:
    """Three basins in a row, connected by outlets, draining to a LevelBoundary.

    Basin 1 -> Outlet 4 -> Basin 2 -> Outlet 5 -> Basin 3 -> Outlet 6 -> LevelBoundary 7
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id, x in ((1, 0), (2, 20), (3, 40)):
        model.basin.add(
            Node(node_id, Point(x, 0), name=f"basin {node_id}", meta_categorie="doorgaand"),
            [
                basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
                basin.State(level=[0.5]),
                basin.Area(geometry=[square(x)]),
            ],
        )
    for node_id, x in ((4, 10), (5, 30), (6, 50)):
        model.outlet.add(Node(node_id, Point(x, 0), name=f"outlet {node_id}"), [outlet.Static(flow_rate=[1.0])])
    model.level_boundary.add(Node(7, Point(60, 0)), [level_boundary.Static(level=[0.0])])
    for from_node, to_node in (
        (model.basin[1], model.outlet[4]),
        (model.outlet[4], model.basin[2]),
        (model.basin[2], model.outlet[5]),
        (model.outlet[5], model.basin[3]),
        (model.basin[3], model.outlet[6]),
        (model.outlet[6], model.level_boundary[7]),
    ):
        model.link.add(from_node, to_node)
    return model


def test_filter_nodes(model):
    assert model.filter_nodes("Basin").index.tolist() == [1, 2, 3]
    assert model.filter_nodes("Outlet").index.tolist() == [4, 5, 6]
    assert model.filter_nodes("Pump").empty


def test_update_node(model):
    model.update_node(5, "Pump", node_properties={"meta_function": "aanvoer"})
    assert model.get_node_type(5) == "Pump"
    assert model.node.df.at[5, "name"] == "outlet 5"
    assert model.node.df.at[5, "meta_function"] == "aanvoer"
    assert 5 not in model.outlet.static.df.node_id.to_numpy()
    assert 5 in model.pump.static.df.node_id.to_numpy()
    # links are untouched
    assert len(model.link.df) == 6


def test_remove_node(model):
    model.remove_node(5, remove_links=True)
    assert 5 not in model.node.df.index
    assert 5 not in model.outlet.static.df.node_id.to_numpy()
    assert not ((model.link.df.from_node_id == 5) | (model.link.df.to_node_id == 5)).any()
    assert len(model.link.df) == 4


def test_merge_basins(model):
    model.merge_basins(node_id=2, to_node_id=3)
    # the connecting outlet and basin 2 are removed
    assert 2 not in model.node.df.index
    assert 5 not in model.node.df.index
    # the inflow to basin 2 now flows into basin 3
    assert ((model.link.df.from_node_id == 4) & (model.link.df.to_node_id == 3)).any()
    # the basin area of basin 2 is merged into basin 3
    area = model.basin.area.df.set_index("node_id").at[3, "geometry"]
    assert area.area == pytest.approx(200.0)


def test_add_links(model):
    n_links = len(model.link.df)
    max_link_id = model.link.df.index.max()
    node = model.add_node(node_type="Outlet", geometry=Point(20, 10), meta_categorie="bergend")
    assert model.node.df.at[node.node_id, "meta_categorie"] == "bergend"

    model.add_links([(model.basin[1], node), (node, model.basin[3])])
    assert len(model.link.df) == n_links + 2
    assert model.link.df.index[-2:].to_list() == [max_link_id + 1, max_link_id + 2]
    new_links = model.link.df.iloc[-2:]
    assert new_links.from_node_id.to_list() == [1, node.node_id]
    assert new_links.to_node_id.to_list() == [node.node_id, 3]
    assert (new_links.link_type == "flow").all()
    assert new_links.geometry.iloc[0].equals(LineString([(0, 0), (20, 10)]))


def test_replace_flow_boundaries_by_level_boundaries(model):
    model.flow_boundary.add(Node(8, Point(-30, 0), name="inflow"), [flow_boundary.Static(flow_rate=[1.0])])
    model.link.add(model.flow_boundary[8], model.basin[1])

    model.replace_flow_boundaries_by_level_boundaries()

    assert model.flow_boundary.node.df.empty
    assert model.get_node_type(8) == "LevelBoundary"
    # an Outlet 10 m downstream of the boundary, on the former link to Basin 1
    (outlet_id,) = model.link.df.loc[model.link.df.from_node_id == 8, "to_node_id"]
    assert model.get_node_type(outlet_id) == "Outlet"
    assert model.node.df.at[outlet_id, "name"] == "inflow"
    assert model.node.df.at[outlet_id, "geometry"].equals(Point(-20, 0))
    assert model.link.df.loc[model.link.df.from_node_id == outlet_id, "to_node_id"].to_list() == [1]


def test_merge_basins_not_neighbors(model):
    with pytest.raises(ValueError, match="not a direct neighbor"):
        model.merge_basins(node_id=1, to_node_id=3)
    with pytest.raises(ValueError, match="is not a basin"):
        model.merge_basins(node_id=4, to_node_id=3)


def test_select_basin_area(model):
    # the area of Basin 2 with another vertex, and a corner moved 1e-9 m, as another version of GEOS can give
    polygon = Polygon([(15, -5), (20, -5), (25, -5), (25, 5), (15, 5 + 1e-9)])
    assert model.select_basin_area(polygon).tolist() == [False, True, False]
    model.remove_basin_area(MultiPolygon([polygon]))
    assert model.basin.area.df is not None
    assert model.basin.area.df["node_id"].tolist() == [1, 3]
    with pytest.raises(ValueError, match="Not any basin area"):
        model.select_basin_area(Polygon([(-5, -5), (5, -5), (5, 6), (-5, 6)]))


def test_select_basin_area_parts(model):
    """Basin areas that another version of GEOS could have merged or separated, and slivers."""
    far = Polygon([(95, -5), (105, -5), (105, 5), (95, 5)])
    left, right = Polygon([(15, -5), (20, -5), (20, 5), (15, 5)]), Polygon([(20, -5), (25, -5), (25, 5), (20, 5)])
    # a sliver against Basin 2, and one within Basin 1
    outer = Polygon([(25, -5), (25 + 1e-6, -5), (25 + 1e-6, 5), (25, 5)])
    inner = Polygon([(4, -5), (4 + 1e-6, -5), (4 + 1e-6, 5), (4, 5)])
    area = gpd.GeoDataFrame(
        {"node_id": [1, 2, 2, 3, 3, 1]},
        geometry=[
            MultiPolygon([*square(0).geoms, far]),
            *(MultiPolygon([p]) for p in (left, right, outer)),
            square(40),
            MultiPolygon([inner]),
        ],
        crs=model.crs,
    )
    area.index.name = "fid"
    model.basin.area.df = area
    # an area of two parts
    assert model.select_basin_area(MultiPolygon([*square(0).geoms, far])).tolist() == [True] + [False] * 5
    assert model.select_basin_area(inner).tolist() == [False] * 5 + [True]
    # the areas that together make up the polygon, without the sliver against it
    assert model.select_basin_area(square(20)).tolist() == [False, True, True, False, False, False]
    # a polygon that no basin area equals, makes up or contains is an error, also next to a polygon that is
    with pytest.raises(ValueError, match="polygon 1 of input geometry"):
        model.select_basin_area(MultiPolygon([left, Polygon([(60, -5), (70, -5), (70, 5), (60, 5)])]))
    # the polygon is split from the area that contains it
    model.remove_basin_area(Polygon([(35, -5), (40, -5), (40, 5), (35, 5)]))
    assert model.basin.area.df is not None
    assert model.basin.area.df["node_id"].tolist() == [1, 2, 2, 3, 3, 1]
    assert model.basin.area.df.at[4, "geometry"].equals(Polygon([(40, -5), (45, -5), (45, 5), (40, 5)]))
    assert model.basin.area.df.crs == model.crs


def test_select_basin_area_covering(model):
    """A polygon that several basin areas cover is split from each, and slivers are not selected."""
    assert model.basin.area.df is not None
    model.basin.area.df.loc[model.basin.area.df["node_id"] == 2, "geometry"] = square(10)
    # the right half of Basin 1 and the left half of Basin 2, around Outlet 4
    polygon = Polygon([(0, -5), (15, -5), (15, 5), (0, 5)])
    selected = model.select_basin_area(polygon)
    assert model.basin.area.df is not None
    assert sorted(model.basin.area.df.loc[selected, "node_id"]) == [1, 2]
    assert model.basin.area.df.loc[selected].area.sum() == pytest.approx(polygon.area)
    # a sliver within Basin 3 selects nothing
    sliver = Polygon([(40, -5), (40 + 1e-6, -5), (40 + 1e-6, 5), (40, 5)])
    assert not model.select_basin_area(sliver).any()


def test_add_basin_area(model):
    """An added basin area is taken from the other basin areas."""
    model.add_basin_area(Polygon([(0, -5), (15, -5), (15, 5), (0, 5)]), node_id=2)
    model.dissolve_basin_area()
    area = model.basin.area.df
    assert area is not None
    assert area.set_index("node_id").area.to_dict() == {1: 50.0, 2: 250.0, 3: 100.0}
    # a sliver is not added
    model.add_basin_area(Polygon([(40, -5), (40 + 1e-6, -5), (40 + 1e-6, 5), (40, 5)]), node_id=2)
    assert len(model.basin.area.df) == 3


def test_dissolve_basin_area(model):
    model.dissolve_basin_area()
    area = model.basin.area.df
    assert area is not None
    assert area["node_id"].tolist() == [1, 2, 3]
    assert (area.geom_type == "MultiPolygon").all()
    area.loc[area["node_id"] == 3, "node_id"] = 6
    with pytest.raises(AssertionError, match="no Basin: \\[6\\]"):
        model.dissolve_basin_area()


def test_fill_basin_area_holes(model, tmp_path):
    """Uncovered parts of areas go to the basin covering most of the area, uncovered areas to their neighbour."""
    areas_gpkg = tmp_path / "areas.gpkg"
    gpd.GeoDataFrame(
        # A around Basin 1, B against Basin 3, C beyond Basin 3, D within the excluded part
        {"code": ["A", "B", "C", "D"]},
        geometry=[
            Polygon([(-5, -5), (5, -5), (5, 8), (-5, 8)]),
            Polygon([(27, -5), (35, -5), (35, 5), (27, 5)]),
            Polygon([(45, -5), (55, -5), (55, 5), (45, 5)]),
            Polygon([(55, -5), (65, -5), (65, 5), (55, 5)]),
        ],
        crs=model.crs,
    ).to_file(areas_gpkg, layer="areas")
    gpd.GeoDataFrame(
        {"CODE": ["X"]}, geometry=[Polygon([(-10, -10), (70, -10), (70, 10), (-10, 10)])], crs=model.crs
    ).to_file(areas_gpkg, layer="drainage_areas")
    model.fill_basin_area_holes(areas_gpkg, excluded=Polygon([(55, -5), (65, -5), (65, 5), (55, 5)]))
    area = model.basin.area.df
    assert area is not None
    # Basin 1 gets the uncovered part of A, Basin 3 the rest of B and C, which it borders, and nothing gets D
    assert area.set_index("node_id").area.round(6).to_dict() == {1: 130.0, 2: 100.0, 3: 280.0}

    # a Basin without basin area is an error
    model.basin.area.df = area[area["node_id"] != 2]
    with pytest.raises(AssertionError, match=r"Basins without basin area: \[2\]"):
        model.fill_basin_area_holes(areas_gpkg)


def test_split_basin(model):
    """The parts of a split basin area keep its node, but for one with a Basin without area."""
    model.split_basin(geometry=LineString([(0, -10), (0, 10)]))
    area = model.basin.area.df
    assert area is not None
    assert area.loc[area.intersects(Point(-2, 0)) | area.intersects(Point(2, 0)), "node_id"].tolist() == [1, 1]


def test_apply_edits(model, tmp_path):
    edits_gpkg = tmp_path / "model_edits.gpkg"
    gpd.GeoDataFrame(
        {"node_id": [6, 4], "node_type": ["Pump", "ManningResistance"], "order": [2, 1]},
        geometry=[Point(50, 0), Point(10, 0)],
        crs=28992,
    ).to_file(edits_gpkg, layer="update_node")
    gpd.GeoDataFrame({"node_id": [5], "remove_links": [True]}, geometry=[Point(30, 0)], crs=28992).to_file(
        edits_gpkg, layer="remove_node"
    )

    model.apply_edits(edits_gpkg, ["update_node", "remove_node"], sort_by_order=True)
    assert model.get_node_type(6) == "Pump"
    assert model.get_node_type(4) == "ManningResistance"
    assert 5 not in model.node.df.index
    assert not ((model.link.df.from_node_id == 5) | (model.link.df.to_node_id == 5)).any()

    # errors tell which edit failed
    gpd.GeoDataFrame({"node_id": [99], "node_type": ["Pump"]}, geometry=[Point(0, 0)], crs=28992).to_file(
        tmp_path / "invalid_edits.gpkg", layer="update_node"
    )
    with pytest.raises(KeyError) as excinfo:
        model.apply_edits(tmp_path / "invalid_edits.gpkg", ["update_node"])
    assert "in model edit update_node" in str(excinfo.value.__notes__)


def test_ids_after_removal(model):
    """New ids continue from the remaining maximum, as after a validated assignment of the tables."""
    max_link_id = model.link.df.index.max()
    model.remove_link(from_node_id=6, to_node_id=7, remove_disconnected_nodes=False)
    model.link.add(model.outlet[6], model.level_boundary[7])
    assert model.link.df.index.max() == max_link_id

    model.remove_node(7, remove_links=True)
    node = model.level_boundary.add(Node(geometry=Point(60, 0)), [level_boundary.Static(level=[0.0])])
    assert node.node_id == 7


def test_prefix_index(model):
    from ribasim_nl.reset_index import prefix_index

    model = prefix_index(model, prefix_id=7, max_digits=4)
    assert model.node.df.index.to_list() == [70001, 70002, 70003, 70004, 70005, 70006, 70007]
    assert model.node.df["meta_node_id_waterbeheerder"].to_list() == [1, 2, 3, 4, 5, 6, 7]
    assert model.link.df.from_node_id.to_list()[:2] == [70001, 70004]
    assert model.outlet.static.df.node_id.to_list() == [70004, 70005, 70006]
    assert model.outlet.static.df.node_id.dtype == "int32"


def test_set_link_id():
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.level_boundary.add(Node(1, Point(0, 0)), [level_boundary.Static(level=[0.0])])
    model.outlet.add(Node(2, Point(1, 0)), [outlet.Static(flow_rate=[1.0])])
    model.level_boundary.add(Node(3, Point(2, 0)), [level_boundary.Static(level=[0.0])])
    model.link.add(model.level_boundary[1], model.outlet[2])
    model.link.add(model.outlet[2], model.level_boundary[3])
    model.remove_links([1])
    model.set_link_id(2, 1)
    assert model.link.df.index.tolist() == [1]
    assert model.link.df.at[1, "from_node_id"] == 2
    # the released ID 2 can be reused without a warning
    model.link.add(model.level_boundary[1], model.outlet[2], link_id=2)
    assert sorted(model.link.df.index) == [1, 2]
    with pytest.raises(AssertionError, match="already exists"):
        model.set_link_id(1, 2)


def test_imposed_settings(model, tmp_path):
    assert model.experimental.concentration
    model.write(tmp_path / "model.toml")
    assert Model.read(tmp_path / "model.toml").experimental.concentration

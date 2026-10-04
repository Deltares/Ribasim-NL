import geopandas as gpd
import pytest
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet
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


def test_merge_basins_not_neighbors(model):
    with pytest.raises(ValueError, match="not a direct neighbor"):
        model.merge_basins(node_id=1, to_node_id=3)
    with pytest.raises(ValueError, match="is not a basin"):
        model.merge_basins(node_id=4, to_node_id=3)


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

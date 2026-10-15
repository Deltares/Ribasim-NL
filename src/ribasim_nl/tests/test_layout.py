import pytest
import shapely
from ribasim import Node
from ribasim.nodes import basin, manning_resistance, outlet
from ribasim_nl.layout import (
    coincident_nodes,
    offset_links,
    overlapping_links,
    place_bergend_nodes,
    resolve_overlaps,
    separate_nodes,
    tidy_layout,
)
from ribasim_nl.validation import _layout_issues
from shapely.geometry import LineString, MultiPolygon, Point, box

from ribasim_nl import Model


def add_basin(model: Model, node_id: int, point: Point, area: shapely.Polygon, **kwargs) -> None:
    model.basin.add(
        Node(node_id, point, **kwargs),
        [
            basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
            basin.State(level=[0.5]),
            basin.Area(geometry=[MultiPolygon([area])]),
        ],
    )


@pytest.fixture
def model() -> Model:
    """Basin 1 -> Outlet 3 -> Basin 2 and back through Outlet 4, along the same route, with a bergend Basin 5."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    add_basin(model, 1, Point(0, 0), box(-500, -500, 500, 500), meta_categorie="doorgaand")
    add_basin(model, 2, Point(1000, 0), box(500, -500, 1500, 500), meta_categorie="doorgaand")
    # the bergend Basin is next to the doorgaand Basin, as in the pipeline
    add_basin(model, 5, Point(10, 0), box(-500, -500, 500, 500), meta_categorie="bergend")
    for node_id in (3, 4):
        model.outlet.add(Node(node_id, Point(500, 0)), [outlet.Static(flow_rate=[1.0])])
    model.manning_resistance.add(
        Node(6, Point(5, 0)),
        [manning_resistance.Static(length=[100.0], manning_n=[0.04], profile_width=[10.0], profile_slope=[3.0])],
    )
    route = [(0, 0), (250, 0), (250, 100), (500, 0)]
    model.link.add(model.basin[1], model.outlet[3], geometry=LineString(route))
    model.link.add(model.outlet[3], model.basin[2])
    model.link.add(model.basin[2], model.outlet[4])
    model.link.add(model.outlet[4], model.basin[1], geometry=LineString(route[::-1]))
    model.link.add(model.basin[5], model.manning_resistance[6])
    model.link.add(model.manning_resistance[6], model.basin[1])
    return model


def test_place_bergend_nodes(model):
    place_bergend_nodes(model)
    nodes = model.node.df.geometry
    assert nodes[5].distance(nodes[1]) == pytest.approx(100)
    assert nodes[6].distance(nodes[1]) == pytest.approx(50)
    assert box(-500, -500, 500, 500).contains(nodes[5])
    # the links of the moved nodes are moved along
    assert model.link.df.geometry.iloc[4].coords[0] == nodes[5].coords[0]


def test_offset_links(model):
    def overlap() -> float:
        there, back = model.link.df.geometry.iloc[0], model.link.df.geometry.iloc[3]
        return there.intersection(back).length

    assert overlap() > 500
    offset_links(model, 5.0)
    assert overlap() == 0
    # the ends stay at the nodes, the inner points move to the right of the direction, 10 m apart
    there, back = model.link.df.geometry.iloc[0], model.link.df.geometry.iloc[3]
    assert there.coords[0] == (0, 0) and there.coords[-1] == (500, 0)
    assert back.coords[0] == (500, 0) and back.coords[-1] == (0, 0)
    assert there.coords[1][1] < 0 < back.coords[-2][1]
    assert there.distance(back.interpolate(0.5, normalized=True)) == pytest.approx(10, rel=0.1)


def test_offset_links_bend_repeated_point(model):
    """A straight link with a repeated point is bent as well."""
    link_id = model.link.df.index[1]
    model.link.df.loc[[link_id], "geometry"] = [LineString([(500, 0), (1000, 0), (1000, 0)])]
    offset_links(model, 5.0, [link_id], bend=True)
    assert model.link.df.at[link_id, "geometry"].coords[:] == [(500, 0), (750, -5), (1000, 0)]


def test_separate_nodes(model):
    assert coincident_nodes(model, 1.0).tolist() == [[3], [4]]
    separate_nodes(model, 10.0)
    nodes = model.node.df.geometry
    assert coincident_nodes(model, 9.999).shape[1] == 0
    # Basins are never moved, and the later of equally movable nodes is moved
    assert nodes[1].equals(Point(0, 0))
    assert nodes[3].equals(Point(500, 0))
    assert nodes[4].distance(nodes[3]) == pytest.approx(10)
    assert model.link.df.geometry.iloc[2].coords[-1] == nodes[4].coords[0]


def test_tidy_layout(model):
    issues = _layout_issues(model)
    assert len(issues) == 2
    assert "[(3, 4)]" in issues[0] and "Nodes" in issues[0]
    assert "Links" in issues[1]
    tidy_layout(model)
    assert _layout_issues(model) == []
    model.validate_ribasim_nl()


def test_resolve_observation_overlaps(model):
    """Two Observation nodes in line with the observed Outlet."""
    for node_id, x in ((7, 510), (8, 520)):
        model.observation.add(Node(node_id, Point(x, 0)))
        model.link.add(model.observation[node_id], model.outlet[3])
    observation_links = set(model.link.df.index[model.link.df["link_type"] == "observation"])

    def observation_overlaps() -> list[tuple[int, int]]:
        return [(a, b) for a, b in overlapping_links(model, 1.0).T if {a, b} <= observation_links]

    assert len(observation_overlaps()) == 1
    resolve_overlaps(model)
    assert observation_overlaps() == []

from ribasim import Node
from ribasim.nodes import basin, outlet, pump
from ribasim_nl.control import get_control_nodes_position_from_supply_area
from shapely.geometry import LineString, Point, box

from ribasim_nl import Model


def test_links_passing_through_a_supply_area_are_not_crossings():
    """Pump 2 lies between Basins 1 and 3 outside the area; Outlet 4 lets Basin 3 into Basin 5 inside it."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    profile = basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0])
    for node_id, point in [(1, Point(-50, 50)), (3, Point(50, 50)), (5, Point(50, -50))]:
        model.basin.add(Node(node_id, point), [profile])
    model.pump.add(Node(2, Point(0, 50)), [pump.Static(flow_rate=[1.0])])
    model.outlet.add(Node(4, Point(50, 20)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.pump[2])
    model.link.add(model.pump[2], model.basin[3])
    model.link.add(model.basin[3], model.outlet[4])
    model.link.add(model.outlet[4], model.basin[5])
    # the link from Basin 1 to Pump 2 follows a watercourse through a corner of the area
    link_df = model.link.df
    link_df.loc[(link_df.from_node_id == 1), "geometry"] = LineString([(-50, 50), (-20, 0), (0, 50)])

    supply_area = box(-100, -100, 100, 10)
    positions = get_control_nodes_position_from_supply_area(model, supply_area)
    assert positions["position"].to_dict() == {4: "inflow"}

from ribasim import Node
from ribasim.nodes import basin, flow_demand, outlet
from ribasim_nl.allocation import connected_subnetwork_ids, enable_allocation
from shapely.geometry import Point

from ribasim_nl import Model


def two_part_model() -> Model:
    """Two unconnected parts: Basins 1 and 4 with a Junction and a FlowDemand, and Basin 10 draining to a Terminal.

    Observation 7 observes Outlet 2, and Observation 13 observes nothing.
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    basin_tables = [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
    for node_id in (1, 4, 10):
        model.basin.add(Node(node_id, Point(node_id, 0)), basin_tables)
    for node_id in (2, 5, 11):
        model.outlet.add(Node(node_id, Point(node_id, 0)), [outlet.Static(flow_rate=[1.0])])
    model.junction.add(Node(3, Point(3, 0)))
    model.flow_demand.add(Node(6, Point(6, 0)), [flow_demand.Static(demand=[1.0], demand_priority=[1])])
    model.terminal.add(Node(12, Point(12, 0)))
    for node_id in (7, 13):
        model.observation.add(Node(node_id, Point(node_id, 1)))

    model.link.add(model.basin[1], model.outlet[2])
    model.link.add(model.basin[1], model.outlet[5])
    model.link.add(model.outlet[2], model.junction[3])
    model.link.add(model.outlet[5], model.junction[3])
    model.link.add(model.junction[3], model.basin[4])
    model.link.add(model.flow_demand[6], model.outlet[2])
    model.link.add(model.observation[7], model.outlet[2])
    model.link.add(model.basin[10], model.outlet[11])
    model.link.add(model.outlet[11], model.terminal[12])
    return model


def test_connected_subnetwork_ids():
    model = two_part_model()
    link_types = set(model.link.df["link_type"])
    assert link_types == {"flow", "control", "observation"}, link_types

    subnetwork_id = connected_subnetwork_ids(model)
    # the largest part comes first, connected through the Junction and the control link of the FlowDemand
    assert subnetwork_id[[1, 2, 3, 4, 5, 6]].tolist() == [2] * 6
    assert subnetwork_id[[10, 11, 12]].tolist() == [3] * 3
    assert subnetwork_id[[7, 13]].isna().all()


def test_connected_subnetwork_ids_junction_tree_of_basin():
    """Like the Ribasim core, a Junction tree with one Basin as neighbor connects it in any direction.

    Both links point into Junction 14, but its tree connects Outlet 16 with Basin 10, so the two parts are one.
    """
    model = two_part_model()
    model.junction.add(Node(14, Point(14, 0)))
    model.outlet.add(Node(16, Point(16, 0)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.outlet[16])
    model.link.add(model.outlet[16], model.junction[14])
    model.link.add(model.basin[10], model.junction[14])

    subnetwork_id = connected_subnetwork_ids(model)
    assert subnetwork_id[[1, 2, 3, 4, 5, 6, 10, 11, 12, 14, 16]].tolist() == [2] * 11
    assert subnetwork_id[[7, 13]].isna().all()


def test_enable_allocation():
    model = two_part_model()
    enable_allocation(model)
    assert model.experimental.allocation
    assert model.node.df["subnetwork_id"].dropna().unique().tolist() == [2, 3]

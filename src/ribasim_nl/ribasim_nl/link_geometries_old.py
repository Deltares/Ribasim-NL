# %%

from networkx import NetworkXNoPath
from shapely.geometry import LineString

from ribasim_nl.geometry import drop_z
from ribasim_nl.model import Model
from ribasim_nl.network import Network


class LinkGeometryError(Exception):
    def __init__(self, message, from_node_id, to_node_id):
        self.from_node_id = from_node_id
        self.to_node_id = to_node_id
        self.message = message
        super().__init__(self.__str__())

    def __str__(self):
        return f"{self.message} between node_id {self.from_node_id} and {self.to_node_id}"


def link_geometry_from_hydroobject(
    model: Model, network: Network, from_node_id, to_node_id, max_straight_line_ratio: float = 5
) -> LineString:
    """Get an edge-geometry between two model nodes

    Args:
        model (Model): ribasim_nl model
        network (Network): ribasim_nl Network
        from_node_id (int): start node_id
        to_node_id (int): to node_id
        max_straight_line_ratio (float): max straight_line ratio

    Returns
    -------
        LineString: Edge geometry
    """
    geometry = None

    from_point = drop_z(model.node_table().df.at[from_node_id, "geometry"])
    to_point = drop_z(model.node_table().df.at[to_node_id, "geometry"])
    straight_line_distance = from_point.distance(to_point)
    # get or add node_from
    distance = network.nodes.distance(from_point)
    if distance.min() < 0.1:
        node_from = distance.idxmin()
    else:
        node_from = network.add_node(from_point, max_distance=10, align_distance=1)

    # get or add node_to
    distance = network.nodes.distance(to_point)
    if distance.min() < 0.1:
        node_to = distance.idxmin()
    else:
        node_to = network.add_node(to_point, max_distance=10, align_distance=1)

    if (node_from is not None) and (node_to is not None):
        # get line geometry
        try:
            geometry = network.get_line(node_from, node_to)
        except NetworkXNoPath:
            raise LinkGeometryError("No path", from_node_id, to_node_id)

    if not isinstance(geometry, LineString):
        raise LinkGeometryError("No LineString found", from_node_id, to_node_id)
    else:
        if geometry.length == 0:
            raise LinkGeometryError(f"LineString.length = {geometry.length}", from_node_id, to_node_id)
        if (geometry.length / straight_line_distance) > max_straight_line_ratio:
            raise LinkGeometryError(
                f"LineString.length ({geometry.length}) / straight_line_distance {straight_line_distance} > {max_straight_line_ratio}",
                from_node_id,
                to_node_id,
            )
    return geometry


def fix_link_geometries(model, network, max_straight_line_ratio: float = 5):
    # fix edge geometries
    for row in model.link.df.itertuples():
        try:
            geometry = link_geometry_from_hydroobject(
                model=model,
                network=network,
                from_node_id=row.from_node_id,
                to_node_id=row.to_node_id,
                max_straight_line_ratio=max_straight_line_ratio,
            )
            model.link.df.loc[row.Index, "geometry"] = geometry
        except LinkGeometryError as e:
            print(e)
            continue

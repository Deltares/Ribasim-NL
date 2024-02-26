# %%
import geopandas as gpd
import pandas as pd
import ribasim
from ribasim import Model, Network
from shapely.geometry import LineString, Point

from ribasim_nl.case_conversions import pascal_to_snake_case

# TODO: get this from ribasim somehow
CLASS_TABLES = {
    "Basin": ["static", "profile", "state"],
    "LinearResistance": ["static"],
    "ManningResistance": ["static"],
    "Pump": ["static"],
    "Outlet": ["static"],
    "Terminal": ["static"],
    "FlowBoundary": ["static"],
    "LevelBoundary": ["static", "time"],
    "FractionalFlow": ["static"],
    "TabulatedRatingCurve": ["static"],
}

TABLES = [
    j
    for r in [
        [f"{pascal_to_snake_case(k)}.{i}.df" for i in v]
        for k, v in CLASS_TABLES.items()
    ]
    for j in r
]


def get_table(model: Model, table: str = "basin.static.df"):
    if table not in TABLES:
        raise ValueError(f"the value of table should be in {TABLES} not {table}")

    else:
        attributes = table.split(".")
        value = model
        for attr in attributes:
            if hasattr(value, attr):
                value = getattr(value, attr)
            else:
                return None
    return value


def add_control_node_to_network(
    network: Network,
    node_ids: list[int],
    offset=100,
    offset_node_id: int | None = None,
    ctrl_node_geom: tuple | None = None,
    **kwargs,
) -> int:
    """Add a control node and control edge to a ribasim.Network

    Parameters
    ----------
    network : Network
        Ribasim.Network with Node and Edge tables
    node_ids : list[int]
        Nodes to connect the control node
    offset : int, optional
        left-side offset of control node to node_id, in case len(node_ids) == 1 or offset_node_id is specified.
        In other case this value is ignored. By default 100
    offset_node_id : int | None, optional
        User can explicitly specify a offset_node_id for a left-offset of the control-node. by default None
    ctrl_node_geom : tuple | None, optional
        User can explicitly specify an (x, y) tuple . by default None


    Extra kwargs will be added as attributes to the control-node

    Returns
    -------
    network, int
        updated network and node_id of the control-node
    """

    # see if we have one node-id to offset ctrl-node from
    if offset_node_id is not None:
        node_id = offset_node_id
    elif len(node_ids) == 1:
        node_id = node_ids[0]

    if ctrl_node_geom is None:
        if node_id is not None:  # if node-id we take the left-offset
            linestring = (
                network.edge.df[network.edge.df["to_node_id"] == node_id]
                .iloc[0]
                .geometry
            )
            ctrl_node_geom = Point(
                linestring.parallel_offset(offset, "left").coords[-1]
            )
        else:  # if not we take the centroid of all node_ids
            ctrl_node_geom = network.node.df[
                network.node.df.index.isin(node_ids)
            ].unary_union.centroid
    else:
        ctrl_node_geom = Point(ctrl_node_geom)

    # ad the ctrl-node to the network
    ctrl_node_id = network.node.df.index.max() + 1
    ctrl_node = {
        "type": "DiscreteControl",
        "meta_node_id": ctrl_node_id,
        "geometry": ctrl_node_geom,
        **kwargs,
    }

    network.node.df.loc[ctrl_node_id] = ctrl_node
    network.node.df.crs = 28992

    # add edge(s) to the network
    ctrl_edge_gdf = gpd.GeoDataFrame(
        [
            {
                "from_node_id": ctrl_node_id,
                "to_node_id": i,
                "edge_type": "control",
                "geometry": LineString(
                    (ctrl_node_geom, network.node.df.at[i, "geometry"])
                ),
            }
            for i in node_ids
        ]
    )

    network.edge = ribasim.Edge(
        df=pd.concat([network.edge.df, ctrl_edge_gdf], ignore_index=True)
    )
    return ctrl_node_id


def update_table(table, new_table):
    node_ids = new_table.node_id.unique()
    table = table[~table.node_id.isin(node_ids)]
    table = pd.concat([table, new_table])
    table.reset_index(inplace=True)
    return table

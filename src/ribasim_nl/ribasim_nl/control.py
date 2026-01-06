import pandas as pd
from ribasim import Node, nodes
from ribasim.nodes import discrete_control
from shapely.geometry import Point, Polygon

from ribasim_nl import Model
from ribasim_nl.case_conversions import pascal_to_snake_case


def _offset_new_node(node: Node, x_offset: float = 10, y_offset: float = 0):
    """Create a new Ribasim.Node with x_offset and y_offset of existing node."""
    return Node(geometry=Point(node.geometry.x + x_offset, node.geometry.y + y_offset))


def control_nodes_from_supply_area(
    model: Model,
    polygon: Polygon,
    control_node_types: list[str] = ["Outlet", "Pump"],
    ignore_intersecting_links: list[int] = [],
) -> tuple[list[int], list[int], list[int]]:
    """_summary_

    Parameters
    ----------
    model : Model
        Ribasim Model
    polygon : Polygon
        Polygon containing supply area
    control_node_types : list[str], optional
        Node_types considered to be control nodes , by default ["Outlet", "Pump"]
    ignore_intersecting_links : list[int], optional
        Optional list of links that can be ignored in producing outflow or inflow control nodes.
        Be cautious (!), only add id's to this list if you are sure it won't affect supply, by default []

    Returns
    -------
    tuple[list[int], list[int], list[int]]
        outflow_nodes, inflow_nodes, internal_nodes -> (from area to elsewhere, from elsewhere to area, within area)

    Raises
    ------
    ValueError
        In case intersecting links are found that are not managed by a node_type defined in `control_node_types`
    """
    # fix polygon, get exterior and polygonize so we won't miss anythin within area
    exterior = polygon.exterior
    polygon = Polygon(exterior)

    # read node_table for further use
    node_df = model.node_table().df

    # intersecting links and direction (if not outflow, then inward)
    link_intersect_df = model.link.df[model.link.df.intersects(exterior)]
    link_intersect_df.loc[:, ["outflow"]] = node_df.loc[link_intersect_df["from_node_id"]].within(polygon).to_numpy()
    link_intersect_df.loc[:, ["from_node_type"]] = node_df.loc[
        link_intersect_df["from_node_id"], "node_type"
    ].to_numpy()
    link_intersect_df.loc[:, ["to_node_type"]] = node_df.loc[link_intersect_df["to_node_id"], "node_type"].to_numpy()
    link_intersect_df = link_intersect_df[~link_intersect_df.index.isin(ignore_intersecting_links)]

    # check if all intersections have control nodes
    link_with_control_node = link_intersect_df.from_node_type.isin(
        control_node_types
    ) | link_intersect_df.to_node_type.isin(control_node_types)
    links_without_control = link_intersect_df[~link_with_control_node].index.to_list()

    if links_without_control:
        raise ValueError(
            f"Found intersecting links without control node (!): {links_without_control}. Fix supply area or specify these links in `ignore_intersecting_links` list"
        )

    # finding outflow nodes (outbound the supply area)
    outflow_links = link_intersect_df[link_intersect_df.outflow]
    outflow_nodes = sorted(
        outflow_links.loc[outflow_links.from_node_type.isin(control_node_types), "from_node_id"].to_list()
        + outflow_links.loc[outflow_links.to_node_type.isin(control_node_types), "to_node_id"].to_list()
    )

    # finding inflow nodes (inbound the supply area)
    inflow_links = link_intersect_df[~link_intersect_df.outflow]
    inflow_nodes = sorted(
        inflow_links.loc[inflow_links.from_node_type.isin(control_node_types), "from_node_id"].to_list()
        + inflow_links.loc[inflow_links.to_node_type.isin(control_node_types), "to_node_id"].to_list()
    )

    internal_nodes = node_df[node_df.node_type.isin(control_node_types) & node_df.within(polygon)]
    internal_nodes = [i for i in internal_nodes.index if i not in inflow_nodes + outflow_nodes]

    return outflow_nodes, inflow_nodes, internal_nodes


def get_drain_nodes(model: Model, control_nodes_df: pd.DataFrame, supply_bool_col: str = "supply_node") -> list:
    """Extract drain nodes (only discharging a supply area) from a dataframe of control_nodes.

    Parameters
    ----------
    model : Model
        Ribasim Model
    control_nodes_df : pd.DataFrame
        Nodes with control and an indiction of supply-type
    supply_bool_col : str, optional
        Column in control_nodes_df with indiction of supply, by default "supply_node"

    Returns
    -------
    list
        list of nodes draining the area

    Raises
    ------
    ValueError
        Connector nodes not marked as supply nor drain that are defined in opposite direction between two basins (one must be drain, the other supply)
    """
    # from_node_id and to_nodes to table
    from_node_id = model.link.df.set_index("to_node_id")["from_node_id"]
    to_node_id = model.link.df.set_index("from_node_id")["to_node_id"]

    # alle connector-nodes
    control_nodes_df["from_node_id"] = [from_node_id[i] for i in control_nodes_df.index]
    control_nodes_df["to_node_id"] = [to_node_id[i] for i in control_nodes_df.index]

    # filter all supply_nodes
    supply_nodes_df = control_nodes_df[control_nodes_df[supply_bool_col]]

    # extract drain_nodes
    drain_nodes = [
        i.Index
        for i in control_nodes_df[~control_nodes_df[supply_bool_col]].itertuples()
        if ((supply_nodes_df.from_node_id == i.to_node_id) & (supply_nodes_df.to_node_id == i.from_node_id)).any()
    ]

    # check if control nodes exist for which one or more should have been labelled as supply-node
    check_nodes_df = control_nodes_df[~control_nodes_df[supply_bool_col] & ~control_nodes_df.index.isin(drain_nodes)]

    drain_supply_nodes = [
        i.Index
        for i in check_nodes_df.itertuples()
        if ((check_nodes_df.from_node_id == i.to_node_id) & (check_nodes_df.to_node_id == i.from_node_id)).any()
    ]

    if drain_supply_nodes:
        raise ValueError(
            f"Connector-nodes in opposite direction: {drain_supply_nodes}. One or more has to be labelled with `{supply_bool_col}` == True"
        )

    return drain_nodes


def add_controllers_to_drain_nodes(
    model,
    drain_nodes,
    target_level_offset: float = 0.0,
    target_level_column: str = "meta_streefpeil",
    x_offset: float = 10,
    y_offset: float = 0,
):
    for node_id in drain_nodes:
        # get targed_level
        us_basin = model.upstream_node_id(node_id=node_id)
        target_level = model.basin.area.df.set_index("node_id").at[us_basin, target_level_column]

        if us_basin is None or target_level is None:
            msg = f"Drain node {node_id}: target_level missing in upstream basin {us_basin}"
            raise ValueError(msg)

        target_level += float(target_level_offset)

        # update static table
        node_type = model.get_node_type(node_id=node_id)
        control_state = ["aanvoer", "afvoer"]
        original_max_flow_rate = (
            getattr(model, pascal_to_snake_case(node_type))
            .static.df.set_index("node_id")
            .loc[[node_id], "max_flow_rate"]
            .max()
        )  # extract flow_rate from existing static-table
        static_table = getattr(nodes, pascal_to_snake_case(node_type)).Static
        model.update_node(
            node_id,
            node_type,
            [
                static_table(
                    min_upstream_level=[target_level, target_level],
                    flow_rate=[0, 100],
                    max_flow_rate=[0, original_max_flow_rate],
                    control_state=control_state,
                )
            ],
        )

        # add control_node
        tables = [
            discrete_control.Variable(
                compound_variable_id=1,
                listen_node_id=us_basin,
                variable=["level"],
                weight=[1],
            ),
            discrete_control.Condition(
                compound_variable_id=[1],
                condition_id=[1],
                threshold_high=[target_level],
                threshold_low=[target_level],
            ),
            discrete_control.Logic(truth_state=["F", "T"], control_state=control_state),
        ]

        connector_node = model.get_node(node_id)
        control_node = model.discrete_control.add(
            _offset_new_node(connector_node, x_offset=x_offset, y_offset=y_offset), tables
        )

        model.link.add(control_node, connector_node)

        print(f"[OK] drain {node_id}: upstream basin={us_basin} min_upstream_level={target_level:.3f} ")

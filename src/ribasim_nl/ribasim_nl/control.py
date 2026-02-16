# %%

import math
from typing import Literal

import geopandas as gpd
import pandas as pd
from ribasim import Node, nodes
from ribasim.nodes import discrete_control, flow_demand
from shapely.geometry import Point, Polygon

from ribasim_nl import Model
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.downstream import downstream_nodes


class InvalidTable(Exception):
    """Basin.Area table is invalid."""

    def __init__(self, message: str):
        super().__init__(message)


class MissingTable(Exception):
    """Table is missing."""

    def __init__(self, table: str):
        super().__init__(f"{table} table is missing!")


def _read_node_table(model: Model) -> gpd.GeoDataFrame:
    """Read node_table model from model."""
    df = model.node.df
    if df is None:
        raise MissingTable(table="Node table")
    return df


def _read_link_table(model: Model, link_type: Literal["flow", "control"] = "flow") -> gpd.GeoDataFrame:
    """Read link_table model from model."""
    df = model.link.df
    if df is None:
        raise MissingTable(table="Link table")
    return df[df.link_type == link_type]


def _offset_new_node(node: Node, offset: float = 10, angle: int = 90, **kwargs) -> Node:
    """Create a new Ribasim.Node with from offset (distance) and angle (degrees, North = 0)"""
    theta = math.radians(angle)

    x_offset = offset * math.sin(theta)
    y_offset = offset * math.cos(theta)

    return Node(geometry=Point(node.geometry.x + x_offset, node.geometry.y + y_offset), **kwargs)


def _recursive_search_over_junctions(
    model: Model,
    node_id: int,
    direction: Literal["upstream", "downstream"],
    connector_node_id: int | None = None,
):
    """Recursive downstream basin search over Junctions"""
    # define connector_node_id so we can raise Exception on connector-Node
    if connector_node_id is None:
        connector_node_id = node_id
    dir_node_id = getattr(model, f"{direction}_node_id")(node_id=node_id)

    # raise missing downstream basin
    if dir_node_id is None:
        raise ValueError(f"Connector-Node {connector_node_id} does not have a {direction} Node")

    dir_node_type = model.get_node_type(node_id=dir_node_id)
    if dir_node_type != "Junction":
        return dir_node_id, dir_node_type

    # recursieve stap
    return _recursive_search_over_junctions(
        model=model, node_id=dir_node_id, direction=direction, connector_node_id=connector_node_id
    )


def _target_level(
    model: Model, node_types: pd.Series, node_id: int, target_level_column: str, allow_missing: bool = False
) -> float | None:
    """Get target-level of a Basin Node"""
    node_type = node_types[node_id]

    # Find a target level if node is a Basin. Raise Exception if not found and not allowed
    if node_type not in ["Basin", "LevelBoundary"]:
        if allow_missing:
            return None
        else:
            msg = f"Listen node: {node_id} ({node_type}) not of type Basin or LevelBoundary"
            raise ValueError(msg)
    else:
        if node_type == "Basin":
            if node_id not in model.basin.area.df["node_id"].values:  # node_id missing in Basin.Area table
                if allow_missing:
                    return None
                else:
                    msg = f"Listen node: {node_id} not found in Basin.Area table"
                    raise ValueError(msg)
            else:
                target_level = model.basin.area.df.set_index("node_id").at[node_id, target_level_column]
        else:  # node type is LevelBoundary. We get the min-value from time-table or static-table if available
            target_level = None

            # read from time-table table exist and node_id is in it
            if model.level_boundary.time.df is not None:
                if node_id in model.level_boundary.time.df["node_id"].values:
                    target_level = model.level_boundary.time.df.set_index("node_id").loc[[node_id], "level"].min()

            # read from static-table exist and node_id is in it
            elif model.level_boundary.static.df is not None:
                if node_id in model.level_boundary.static.df["node_id"].values:
                    target_level = model.level_boundary.static.df.set_index("node_id").loc[[node_id], "level"].min()

            if (not allow_missing) and (target_level is None):
                msg = f"Listen node: {node_id} not found in LevelBoundary.Time or LevelBoundary.Static table"
                raise ValueError(msg)

    # Return target_level or raise Exception if missing and not allowed
    if target_level is None:
        if allow_missing:
            return None
        else:
            msg = f"Target level missing in Basin.Area table for node_id={node_id} in column '{target_level_column}'"
            raise ValueError(msg)
    else:
        return target_level


def _update_meta_info(model: Model, nodes_df: gpd.GeoDataFrame, supply: bool = True, drain: bool = True):
    """Update meta-info for masking Basins and Connnector nodes."""
    for col in ["meta_func_aanvoer", "meta_func_afvoer"]:
        if col not in model.pump.static.df.columns:
            model.pump.static.df[col] = 0
    if "meta_aanvoer" not in model.outlet.static.df.columns:
        model.outlet.static.df["meta_aanvoer"] = 0
    if supply and ("meta_aanvoer" not in model.basin.area.df.columns):
        model.basin.area.df["meta_aanvoer"] = False

    # update values
    if supply:
        model.pump.static.df.loc[model.pump.static.df.node_id.isin(nodes_df.index.values), "meta_func_aanvoer"] = 1
        model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(nodes_df.index.values), "meta_aanvoer"] = 1

        ds_node_ids = nodes_df.to_node_id.values
        model.basin.area.df.loc[model.basin.area.df.node_id.isin(ds_node_ids), "meta_aanvoer"] = True

    if drain:
        model.pump.static.df.loc[model.pump.static.df.node_id.isin(nodes_df.index.values), "meta_func_afvoer"] = 1


def validate_nodes_on_reversed_direction(
    nodes_df: gpd.GeoDataFrame, node_function: Literal["flushing", "drain", "supply", "flow_control"] = "drain"
):
    """Check if nodes_df has connector-nodes has nodes in reversed-direction

    Parameters
    ----------
    nodes_df : gpd.GeoDataFrame
        Node table with from_node_id and to_node_id. Can be generated with `get_node_table_with_from_to_node_ids`
    node_function : Literal["flushing", "drain", "supply", "flow_control"], optional
        Node category that is analyzed, used in generating the VallueError only, by default "drain"
    """

    def reversed_node(nodes_df, row):
        mask = (nodes_df[nodes_df.index != row.Index].from_node_id == row.to_node_id) & (
            nodes_df[nodes_df.index != row.Index].to_node_id == row.from_node_id
        )
        if mask.any():
            return {row.Index: int(nodes_df[nodes_df.index != row.Index][mask].index[0])}

    reversed_nodes = [i for i in [reversed_node(nodes_df, row) for row in nodes_df.itertuples()] if i is not None]

    if len(reversed_nodes) > 0:
        """remove reverses and order on key"""
        seen = set()
        result = []
        for d in reversed_nodes:
            ((k, v),) = d.items()
            pair = tuple(sorted((k, v)))
            if pair not in seen:
                seen.add(pair)
                result.append({k: v})
        raise ValueError(
            f"Found {len(result)} connector-node pairs with reversed flow-directions: {reversed_nodes} in set marked as category {node_function}"
        )


def discrete_control_tables_single_basin(listen_node_id: int, control_state: list[str, str], threshold: float) -> list:
    """Create discrete_control tables for a single-basin (upstream or downstream) control-node: drain and supply.

    Parameters
    ----------
    listen_node_id : int
        listen_node_id voor discrete_control.Variable
    control_state : list[str, str]
        control_state voor discrete_control.Logic
    threshold : float
        Threshold for discrete_control.Condition

    Returns
    -------
    list
        tables for discrete_control.add() function
    """
    return [
        discrete_control.Variable(
            compound_variable_id=1,
            listen_node_id=listen_node_id,
            variable=["level"],
            weight=[1],
        ),
        discrete_control.Condition(
            compound_variable_id=[1],
            condition_id=[1],
            threshold_high=[threshold],
            threshold_low=[threshold],
        ),
        discrete_control.Logic(truth_state=["F", "T"], control_state=control_state),
    ]


def add_and_connect_discrete_control_node(
    model: Model, node_id: int, offset: float, angle: int, tables: list, **kwargs
):
    """Add and connect a DiscreteControl Node to an existing node.

    Parameters
    ----------
    model : Model
        Ribasim Model
    node_id : int
        model node_id to add node to
    offset : float
        Offset control-node with respect to connector-node.
    angle : int
        Clock-wise (0 degrees is North) angle control-node with respect to connector-node
    tables : list
        Control-tables, e.g. generated by `discrete_control_tables_single_basin`
    **kwargs:
        keyword:arguments added to ribasim.Node
    """
    connector_node = model.get_node(node_id)
    control_node = model.discrete_control.add(
        _offset_new_node(connector_node, offset=offset, angle=angle, **kwargs), tables
    )
    model.link.add(control_node, connector_node)


def get_node_table_with_from_to_node_ids(
    model: Model,
    node_ids: list[int] | None = None,
    node_types: list[Literal["Pump", "Outlet", "ManningResistance", "TabulatedRatingCurve", "LinearResistance"]] = [
        "Outlet",
        "Pump",
    ],
    max_iter=20,
) -> gpd.GeoDataFrame:
    """Get a node_df from selected connector-nodes, including upstream and downstream non-Junction type node_ids.

    `Connector-nodes` are "flow-type" connecting a Basin with another Basin, LevelBoundary or FlowBoundary

    Upstream/downstream search works recursively, skipping Junction nodes.

    Parameters
    ----------
    model : Model
        Ribasim Model
    node_ids : list[int] | None, optional
        List with connector-nodes ids to select. If None, all nodes of type in `node_types` are selected, by default None
    node_types : list[Literal["Pump", "Outlet", "ManningResistance", "TabulatedRatingCurve", "LinearResistance"]], optional
        Node_types considered to be control nodes, by default ["Outlet", "Pump"]
    max_iter : int, optional
        Max iters to search upstream/downstream over junctions, by default 20

    Returns
    -------
    pd.GeoDataFrame
        GeoDataFrame with node info including from_node_id and to_node_id
    """
    # add from_node_id and to_node_id to node_select_df
    _df = _read_link_table(model=model, link_type="flow")
    _from_node_id = _df.set_index("to_node_id")["from_node_id"]
    _to_node_id = _df.set_index("from_node_id")["to_node_id"]

    # select nodes from model node_table
    node_df = _read_node_table(model=model)
    _node_type = node_df["node_type"]
    selected_node_df = node_df.loc[node_df.node_type.isin(node_types)]

    # filter on node_ids if provided
    if node_ids is not None:
        missing_node_ids = [i for i in node_ids if i not in selected_node_df.index]
        if missing_node_ids:
            raise ValueError(f"Node_ids not in model or of type {node_types}: {missing_node_ids}")
        selected_node_df = selected_node_df.loc[node_ids]

    # find from node_id over junctions
    selected_node_df.loc[:, ["from_node_id"]] = [_from_node_id[i] for i in selected_node_df.index]
    unresolved_node_idx = selected_node_df[
        [_node_type[i] == "Junction" for i in selected_node_df["from_node_id"]]
    ].index

    for _ in range(max_iter):
        selected_node_df.loc[unresolved_node_idx, ["from_node_id"]] = [
            _from_node_id[i] for i in selected_node_df.loc[unresolved_node_idx].from_node_id
        ]
        unresolved_node_idx = selected_node_df.loc[unresolved_node_idx][
            [_node_type[i] == "Junction" for i in selected_node_df.loc[unresolved_node_idx, "from_node_id"]]
        ].index
        if unresolved_node_idx.empty:
            break

    if not unresolved_node_idx.empty:
        raise ValueError(
            f"Max iterations reached ({max_iter}) when searching upstream nodes over junctions for nodes: {unresolved_node_idx.to_list()}"
        )

    # find to_node_id over junctions
    selected_node_df.loc[:, ["to_node_id"]] = [_to_node_id[i] for i in selected_node_df.index]
    unresolved_node_idx = selected_node_df[[_node_type[i] == "Junction" for i in selected_node_df["to_node_id"]]].index

    for _ in range(max_iter):
        selected_node_df.loc[unresolved_node_idx, ["to_node_id"]] = [
            _to_node_id[i] for i in selected_node_df.loc[unresolved_node_idx].to_node_id
        ]
        unresolved_node_idx = selected_node_df.loc[unresolved_node_idx][
            [_node_type[i] == "Junction" for i in selected_node_df.loc[unresolved_node_idx, "to_node_id"]]
        ].index
        if unresolved_node_idx.empty:
            break

    if not unresolved_node_idx.empty:
        raise ValueError(
            f"Max iterations reached ({max_iter}) when searching downstream nodes over junctions for nodes: {unresolved_node_idx.to_list()}"
        )

    return selected_node_df


def add_control_functions_to_connector_nodes(
    model: Model,
    node_positions: pd.Series,
    drain_nodes: list[int],
    supply_nodes: list[int],
    flow_control_nodes: list[int],
    flushing_nodes: dict[int, float],
    is_supply_node_column: str = "meta_supply_node",
) -> gpd.GeoDataFrame:
    """Add control functions `drain`, `supply`, `flusing` or `flow_control` to nodes

    Parameters
    ----------
    model : Model
        Ribasim Nodes
    node_positions : pd.Series
        Series with node_ids (index) and functions (value). Where functions can be `outflow`, `inflow` or `internal`
    drain_nodes : list[int]
        List of node_ids that is forced to the function `drain`
    supply_nodes : list[int]
        List of node_ids that is forced to the function `supply`
    flow_control_nodes: list[int]
        List of node_ids that will be forced to the function `flow_control`
    flushing_nodes : dict[int, float]
        Flushing nodes with their demands in the form of {node_id:demand}
    is_supply_node_column : str, optional
        Column in model.pump.node.df and model.outlet.node.df indicates if node is a supply-node, by default "meta_supply_node"

    Returns
    -------
    gpd.GeoDataFrame
        Table with columns `node_id`, `from_node_id`, `to_node_id`, `function` and `demand` that can be used in `add_controllers_to_connector_nodes` function
    """
    # get a node_table with node_type, from_node_id, to_node_id, function (drain, supply, flow_control or flushing) and demand_flow_rate (if flushing)

    # Step: check if we miss any user-defined supply, drain or flushing nodes
    # aggregate all nodes and see if we miss any supply-drain or flushing nodes
    all_nodes = node_positions.index.to_list()
    inflow_nodes = node_positions[node_positions == "inflow"].index.to_list()
    outflow_nodes = node_positions[node_positions == "outflow"].index.to_list()

    # check on missing supply nodes
    missing_supply_nodes = [i for i in supply_nodes if i not in all_nodes]
    if missing_supply_nodes:
        raise ValueError(
            f"user-defined `supply_nodes` not found in outflow+inflow+internal nodes: {missing_supply_nodes}"
        )

    # check on missing drain nodes
    missing_drain_nodes = [i for i in drain_nodes if i not in all_nodes]
    if missing_drain_nodes:
        raise ValueError(
            f"user-defined `drain_nodes` not found in outflow+inflow+internal nodes: {missing_drain_nodes}"
        )

    # check on missing flow_control nodes
    missing_flow_control_nodes = [i for i in flow_control_nodes if i not in all_nodes]
    if missing_flow_control_nodes:
        raise ValueError(
            f"user-defined `flow_control_nodes` not found in outflow+inflow+internal nodes: {missing_flow_control_nodes}"
        )

    # check on flushing nodes
    missing_flushing_nodes = [i for i in flushing_nodes.keys() if i not in all_nodes]
    if missing_flushing_nodes:
        raise ValueError(
            f"user-defined `flushing_nodes.keys()` not found in outflow+inflow+internal nodes: {missing_flushing_nodes}"
        )

    # step: selected_nodes_df with `from_node_id` and `to_node_id` columns
    selected_nodes_df = get_node_table_with_from_to_node_ids(model, node_ids=all_nodes, max_iter=20)

    # Step: determine function of each node: supply, drain, flow_control or flushing

    # expand user_defined supply_nodes with specified in node_table, and inflow_nodes
    if is_supply_node_column not in selected_nodes_df.columns:
        selected_nodes_df[is_supply_node_column] = False
    selected_nodes_df.loc[supply_nodes, is_supply_node_column] = True
    inflow_nodes = [i for i in inflow_nodes if i not in drain_nodes + list(flushing_nodes.keys()) + flow_control_nodes]
    supply_nodes = sorted(
        set(selected_nodes_df[selected_nodes_df[is_supply_node_column]].index.to_list() + inflow_nodes)
    )

    # expand user_defined drain_nodes with outflow_nodes
    drain_nodes = sorted(set(drain_nodes + outflow_nodes))

    # expand drain_nodes based on reverse connections to supply nodes
    candidates = [i for i in all_nodes if i not in supply_nodes + drain_nodes + flow_control_nodes]
    supply_nodes_df = selected_nodes_df.loc[supply_nodes]
    drain_nodes = sorted(
        [
            i.Index
            for i in selected_nodes_df.loc[candidates].itertuples()
            if ((supply_nodes_df.from_node_id == i.to_node_id) & (supply_nodes_df.to_node_id == i.from_node_id)).any()
        ]
        + drain_nodes
    )

    # list all flow_control_nodes
    graph = model.graph
    skip_nodes = drain_nodes + supply_nodes + list(flushing_nodes.keys())
    _all_downstream_nodes: list[int] = []
    for node_id in supply_nodes:
        _downstream_nodes = downstream_nodes(graph=graph, node_id=node_id, stop_at_node_ids=drain_nodes)
        _all_downstream_nodes += _downstream_nodes
        flow_control_nodes += [i for i in _downstream_nodes if (i in all_nodes) and (i not in skip_nodes)]

    flow_control_nodes = sorted(set(flow_control_nodes))

    # list all drain_nodes again
    skip_nodes = flow_control_nodes + supply_nodes + list(flushing_nodes.keys())
    drain_nodes = sorted([i for i in all_nodes if i not in skip_nodes])

    # Step: populate function column
    selected_nodes_df["function"] = pd.Series(dtype="string")
    for node_ids, function in [
        (supply_nodes, "supply"),
        (drain_nodes, "drain"),
        (flow_control_nodes, "flow_control"),
        (list(flushing_nodes.keys()), "flushing"),
    ]:
        selected_nodes_df.loc[node_ids, "function"] = function

    # Step: add demand_flow_rate for flushing nodes
    selected_nodes_df["demand_flow_rate"] = pd.Series(dtype="float")
    selected_nodes_df.loc[list(flushing_nodes.keys()), "demand_flow_rate"] = list(flushing_nodes.values())

    # Last check
    if len(all_nodes) != len(selected_nodes_df):
        raise ValueError(f"len(node_position) != len(node_functions): {len(all_nodes)} != {len(selected_nodes_df)}")

    return selected_nodes_df


def get_control_nodes_position_from_supply_area(
    model: Model,
    polygon: Polygon,
    exclude_nodes: list[int] = [],
    control_node_types: list[Literal["Outlet", "Pump"]] = ["Outlet", "Pump"],
    ignore_intersecting_links: list[int] = [],
) -> gpd.GeoDataFrame:
    """Get control nodes with nodes position relative to a supply area defined by a polygon.

    Column `position` indicates 'inflow', 'outflow' or `internal` relative to the supply area:
    - outflow: control node with a link directed out of the supply area
    - inflow: control node with a link directed into the supply area
    - internal: control node within the supply area

    Position is determined by GIS topology of links intersecting, or within, the supply area polygon.

    Parameters
    ----------
    model : Model
        Ribasim Model
    polygon : Polygon
        Polygon containing supply area
    exclude_nodes: list[int], optional
        Nodes to exclude from final result
    control_node_types : list[str], optional
        Node_types considered to be control nodes , by default ["Outlet", "Pump"]
    ignore_intersecting_links : list[int], optional
        Optional list of links that can be ignored in producing outflow or inflow control nodes.
        Be cautious (!), only add id's to this list if you are sure it won't affect supply, by default []

    Returns
    -------
    gpd.GeoDataFrame
        node_table with control nodes with a column `position` indicating 'inflow', 'outflow' or `internal` relative to the supply area

    Raises
    ------
    ValueError
        In case intersecting links are found that are not managed by a node_type defined in `control_node_types`
    """
    # fix polygon, get exterior and polygonize so we won't miss anythin within area
    exterior = polygon.exterior
    polygon = Polygon(exterior)

    # read node_table for further use
    node_df = _read_node_table(model=model)

    # intersecting links and direction (if not outflow, then inward)
    link_df = _read_link_table(model=model, link_type="flow")
    link_intersect_df = link_df[link_df.intersects(exterior)]
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
            f"Found intersecting links without node of type {control_node_types} (!): {links_without_control}. Fix supply area or specify these links in `ignore_intersecting_links` list"
        )

    # finding outflow nodes (outbound the supply area)
    outflow_links = link_intersect_df[link_intersect_df.outflow]
    outflow_nodes = sorted(
        set(
            outflow_links.loc[outflow_links.from_node_type.isin(control_node_types), "from_node_id"].to_list()
            + outflow_links.loc[outflow_links.to_node_type.isin(control_node_types), "to_node_id"].to_list()
        )
    )

    # finding inflow nodes (inbound the supply area)
    inflow_links = link_intersect_df[~link_intersect_df.outflow]
    inflow_nodes = sorted(
        set(
            inflow_links.loc[inflow_links.from_node_type.isin(control_node_types), "from_node_id"].to_list()
            + inflow_links.loc[inflow_links.to_node_type.isin(control_node_types), "to_node_id"].to_list()
        )
    )

    internal_nodes = node_df[node_df.node_type.isin(control_node_types) & node_df.within(polygon)]
    internal_nodes = sorted(i for i in internal_nodes.index if i not in inflow_nodes + outflow_nodes)

    # making a node_table with position relative to supply area
    all_nodes = outflow_nodes + inflow_nodes + internal_nodes
    node_df = node_df.loc[all_nodes]
    node_df["position"] = pd.Series(dtype="string")
    node_df.loc[outflow_nodes, "position"] = "outflow"
    node_df.loc[inflow_nodes, "position"] = "inflow"

    # exclude manually excluded nodes
    node_df = node_df[~node_df.index.isin(exclude_nodes)]

    # warn for duplicated node ids (having multiple positions)
    if node_df.index.duplicated().any():
        duplicated_node_ids = node_df[node_df.index.duplicated()].index.to_list()
        print(
            f"WARNING: ambiguous position of nodes {duplicated_node_ids}. Define function manually in `supply_nodes` or `drain_nodes`"
        )
    node_df = node_df[~node_df.index.duplicated()]

    # check if nodes al ready have control
    control_link_df = _read_link_table(model=model, link_type="control")
    controlled_nodes = control_link_df.to_node_id.values
    has_control = [i for i in node_df.index.values if i in controlled_nodes]
    if has_control:
        print(f"WARNING connector-nodes {has_control} in supply-area are already controlled. Nodes will be skipped")
    node_df = node_df[~node_df.isin(has_control)]

    return node_df


def add_controllers_to_drain_nodes(
    model: Model,
    drain_nodes_df: gpd.GeoDataFrame,
    target_level_column: str = "meta_streefpeil",
    control_node_offset: float = 10,
    control_node_angle: int = 90,
    name: str = "uitlaat",
    update_meta_info: bool = True,
):
    """Add control nodes to connector nodes draining a system/supply-area

    Control-nodes draining a system only maintain upstream water-levels and reduce to 0 m3/s if control-state is aanvoer

    Parameters
    ----------
    model : Model
        Ribasim Model
    drain_nodes_df : gpd.GeoDataFrame
        GeoDataFrame of connector nodes having a drain function, including from_node_id and to_node_id
    target_level_column : str, optional
        Column in Basin.Area table that contains target level, by default "meta_streefpeil"
    control_node_offset : float, optional
        Offset control-node with respect to connector-node, by default 10
    control_node_angle : int, optional
        Clock-wise (0 degrees is North) angle control-node with respect to connector-node, by default 90
    name : str, optional
        Name assigned to control-nodes, by default "uitlaat"
    update_meta_info: bool, optional
        Update `meta_func_aanvoer` column in Pump.Static table. Default is True

    Raises
    ------
    ValueError
        Target level in upstream basin cannot be found
    """
    # validate if drain_nodes_df does not contain reversed flow directions
    validate_nodes_on_reversed_direction(drain_nodes_df, node_function="drain")

    # update_meta_info we can use in masking control_nodes
    if update_meta_info:
        _update_meta_info(model=model, nodes_df=drain_nodes_df, supply=False, drain=True)

    node_types = _read_node_table(model=model)["node_type"]

    for connector_node in drain_nodes_df.itertuples():
        node_id = connector_node.Index
        node_type = connector_node.node_type
        # get targed_level and define min_upstream_level; [target_level, target_level]
        us_node_id = connector_node.from_node_id
        us_target_level = _target_level(
            model=model,
            node_types=node_types,
            node_id=us_node_id,
            target_level_column=target_level_column,
            allow_missing=True,
        )
        if us_target_level is None:
            raise ValueError(f"Drain node {node_id}: target_level missing in upstream node: {us_node_id}")
        min_upstream_level = [us_target_level] * 2

        # Print so we can see what happens
        print(f"Adding drain node {node_id}: us_basin={us_node_id} | us_target_level={us_target_level}")

        # update static table
        control_state = ["aanvoer", "afvoer"]
        original_max_flow_rate = (
            getattr(model, pascal_to_snake_case(node_type))
            .static.df.set_index("node_id")
            .loc[[node_id], "flow_rate"]
            .max()
        )  # extract flow_rate from existing static-table
        static_table = getattr(nodes, pascal_to_snake_case(node_type)).Static
        model.update_node(
            node_id,
            node_type,
            [
                static_table(
                    min_upstream_level=min_upstream_level,
                    flow_rate=[0, 100],
                    max_flow_rate=[0, original_max_flow_rate],
                    control_state=control_state,
                )
            ],
        )

        # add control_node
        tables = discrete_control_tables_single_basin(
            listen_node_id=us_node_id, control_state=control_state, threshold=us_target_level
        )

        add_and_connect_discrete_control_node(
            model=model,
            node_id=node_id,
            offset=control_node_offset,
            angle=control_node_angle,
            tables=tables,
            name=f"{name}: {us_target_level:.2f} [m+NAP]",
        )


def add_controllers_to_supply_nodes(
    model: Model,
    supply_nodes_df: gpd.GeoDataFrame,
    us_target_level_offset_supply: float = -0.04,
    target_level_column: str = "meta_streefpeil",
    control_node_offset: float = 10,
    control_node_angle: int = 90,
    name: str = "inlaat",
    update_meta_info: bool = True,
):
    """Add control nodes to connector nodes supplying a system/supply-area

    Parameters
    ----------
    model : Model
        Ribasim Model
    supply_nodes_df : gpd.GeoDataFrame
        GeoDataFrame of connector nodes having a supply function, including from_node_id and to_node_id
    us_target_level_offset_supply : float, optional
        Lowering upstream target levels in supply situation, by default -0.04
    target_level_column : str, optional
        Column in Basin.Area table that contains target level, by default "meta_streefpeil"
    control_node_offset : float, optional
        Offset control-node with respect to connector-node, by default 10
    control_node_angle : int, optional
        Clock-wise (0 degrees is North) angle control-node with respect to connector-node, by default 90
    name : str, optional
        Name assigned to control-nodes, by default "inlaat"
    update_meta_info: bool, optional
        Update `meta_func_afvoer` column in Pump.Static table, `meta_afvoer` in Outlet.Static and Basin.Area tables.
        Default is True
    """
    # update_meta_info we can use in masking control_nodes
    if update_meta_info:
        _update_meta_info(model=model, nodes_df=supply_nodes_df, supply=True, drain=False)

    node_types = _read_node_table(model=model)["node_type"]
    for connector_node in supply_nodes_df.itertuples():
        node_id = connector_node.Index
        node_type = connector_node.node_type
        # get downstream target level, cannot be None (!)
        ds_node_id = connector_node.to_node_id
        ds_target_level = _target_level(
            model=model,
            node_types=node_types,
            node_id=ds_node_id,
            target_level_column=target_level_column,
            allow_missing=False,
        )

        max_downstream_level = [ds_target_level, float("nan")]

        # get upstream target_level and define min_upstream_level;
        us_node_id = connector_node.from_node_id
        us_target_level = _target_level(
            model=model,
            node_types=node_types,
            node_id=us_node_id,
            target_level_column=target_level_column,
            allow_missing=True,
        )
        min_upstream_level = (
            None if us_target_level is None else [us_target_level + us_target_level_offset_supply, us_target_level]
        )

        # Print so we can see what happens
        print(
            f"Adding supply Node {node_id}: ds_basin={ds_node_id} | ds_target_level={ds_target_level} | us_basin={us_node_id} | us_target_level={us_target_level}"
        )

        # update static table
        node_id = connector_node.Index
        node_type = connector_node.node_type
        control_state = ["aanvoer", "afvoer"]
        original_max_flow_rate = (
            getattr(model, pascal_to_snake_case(node_type))
            .static.df.set_index("node_id")
            .loc[[node_id], "flow_rate"]
            .max()
        )  # extract flow_rate from existing static-table
        static_table = getattr(nodes, pascal_to_snake_case(node_type)).Static
        model.update_node(
            node_id,
            node_type,
            [
                static_table(
                    min_upstream_level=min_upstream_level,
                    max_downstream_level=max_downstream_level,
                    flow_rate=[20, 0],
                    max_flow_rate=[original_max_flow_rate, 0],
                    control_state=control_state,
                )
            ],
        )

        # add control_node
        tables = discrete_control_tables_single_basin(
            listen_node_id=ds_node_id, control_state=control_state, threshold=ds_target_level
        )
        if us_target_level is None:
            label = f"{name}: {ds_target_level:.2f} [m+NAP]"
        else:
            label = f"{name}: {us_target_level:.2f}/{ds_target_level:.2f} [m+NAP]"
        add_and_connect_discrete_control_node(
            model=model,
            node_id=node_id,
            offset=control_node_offset,
            angle=control_node_angle,
            tables=tables,
            name=label,
        )


def add_controllers_to_flow_control_nodes(
    model: Model,
    flow_control_nodes_df: gpd.GeoDataFrame,
    us_threshold_offset: float,
    us_target_level_offset_supply: float = -0.04,
    target_level_column: str = "meta_streefpeil",
    control_node_offset: float = 10,
    control_node_angle: int = 90,
    name: str = "doorlaat",
    update_meta_info: bool = True,
):
    """Add control nodes to connector nodes controlling flows and water-levels in a system/supply-area

    Parameters
    ----------
    model : Model
        Ribasim Model
    flow_control_nodes_df : gpd.GeoDataFrame
        GeoDataFrame of connector nodes having a flow_control function, including from_node_id and to_node_id
    us_threshold_offset : float
        Level offset of discrete-control to trigger flow. Should be => model.solver.level_difference_threshold
    us_target_level_offset_supply : float, optional
        Lowering upstream target levels in supply situation, by default -0.04
    target_level_column : str, optional
        Column in Basin.Area table that contains target level, by default "meta_streefpeil"
    control_node_offset : float, optional
        Offset control-node with respect to connector-node, by default 10
    control_node_angle : int, optional
        Clock-wise (0 degrees is North) angle control-node with respect to connector-node, by default 90
    name : str, optional
        Name assigned to control-nodes, by default "doorlaat"
    update_meta_info: bool, optional
        Update `meta_func_afvoer` column in Pump.Static table, `meta_afvoer` in Outlet.Static and Basin.Area tables.
        Default is True
    """
    # update_meta_info we can use in masking control_nodes
    if update_meta_info:
        _update_meta_info(model=model, nodes_df=flow_control_nodes_df, supply=True, drain=True)

    # validate if drain_nodes_df does not contain reversed flow directions
    validate_nodes_on_reversed_direction(flow_control_nodes_df, node_function="flow_control")

    node_types = _read_node_table(model=model)["node_type"]
    for connector_node in flow_control_nodes_df.itertuples():
        node_id = connector_node.Index
        node_type = connector_node.node_type
        # get downstream target level, cannot be None (!)
        ds_node_id = connector_node.to_node_id
        ds_target_level = _target_level(
            model=model,
            node_types=node_types,
            node_id=ds_node_id,
            target_level_column=target_level_column,
            allow_missing=False,
        )
        max_downstream_level = [ds_target_level, 9999]

        # get upstream target_level and define min_upstream_level;
        # None if LevelBoundary, else [us_target_level + target_level_offset_supply, us_target_level]
        us_node_id = connector_node.from_node_id
        us_target_level = _target_level(
            model=model,
            node_types=node_types,
            node_id=us_node_id,
            target_level_column=target_level_column,
            allow_missing=False,
        )
        min_upstream_level = [us_target_level + us_target_level_offset_supply, us_target_level]

        print(
            f"Adding control Node {node_id}: ds_basin={ds_node_id} | ds_target_level={ds_target_level} | us_basin={us_node_id} | us_target_level={us_target_level}"
        )

        # update static table
        control_state = ["aanvoer", "afvoer"]
        original_max_flow_rate = (
            getattr(model, pascal_to_snake_case(node_type))
            .static.df.set_index("node_id")
            .loc[[node_id], "flow_rate"]
            .max()
        )  # extract flow_rate from existing static-table
        static_table = getattr(nodes, pascal_to_snake_case(node_type)).Static
        model.update_node(
            node_id,
            node_type,
            [
                static_table(
                    min_upstream_level=min_upstream_level,
                    max_downstream_level=max_downstream_level,
                    flow_rate=[20, 20],
                    max_flow_rate=[original_max_flow_rate] * 2,
                    control_state=control_state,
                )
            ],
        )

        # add control_node
        thresholds = [us_target_level, us_target_level + us_threshold_offset, -ds_target_level]
        tables = [
            discrete_control.Variable(
                compound_variable_id=[1, 2],
                listen_node_id=[us_node_id, ds_node_id],
                variable=["level", "level"],
                weight=[1, -1],
            ),
            discrete_control.Condition(
                compound_variable_id=[1, 1, 2],
                condition_id=[1, 2, 3],
                threshold_high=thresholds,
                threshold_low=thresholds,
            ),
            discrete_control.Logic(
                truth_state=["FFF", "FFT", "TFF", "TFT", "TTF", "TTT"],
                control_state=["aanvoer", "aanvoer", "afvoer", "aanvoer", "afvoer", "afvoer"],
            ),
        ]

        add_and_connect_discrete_control_node(
            model=model,
            node_id=node_id,
            offset=control_node_offset,
            angle=control_node_angle,
            tables=tables,
            name=f"{name}: {us_target_level:.2f}/{ds_target_level:.2f} [m+NAP]",
        )


def add_controllers_and_demand_to_flushing_nodes(
    model: Model,
    flushing_nodes_df: gpd.GeoDataFrame,
    us_threshold_offset: float,
    demand_threshold_offset: float = 0.001,
    us_target_level_offset_supply: float = -0.04,
    target_level_column: str = "meta_streefpeil",
    supply_season_start: pd.Timestamp | str = "2020-04-01",
    drain_season_start: pd.Timestamp | str = "2019-10-01",
    new_nodes_offset: float = 10,
    control_node_angle: int = 90,
    demand_node_angle: int = 45,
    name: str = "uitlaat",
    demand_name_prefix: str = "doorspoeling",
    flushing_seasonal: bool = False,
):
    """Add control nodes to connector nodes draining a system/supply-area having a certain flow-demand (flushing_nodes)

    Parameters
    ----------
    model : Model
        Ribasim Model
    flushing_nodes_df : gpd.GeoDataFrame
        GeoDataFrame of connector nodes having a flusing function, including from_node_id and to_node_id, and demand_flow_rate column
    us_threshold_offset : float
        Level offset of discrete-control to trigger flow. Should be => model.solver.level_difference_threshold
    demand_threshold_offset : float, optional
        Flow offset of discrete-control to trigger flow., by default 0.001
    us_target_level_offset_supply : float, optional
        Lowering upstream target levels in supply situation, by default -0.04
    target_level_column : str, optional
        Column in Basin.Area table that contains target level, by default "meta_streefpeil"
    supply_season_start : pd.Timestamp | str, optional
        Start of supply-season to populate time-table, by default "2020-04-01"
    drain_season_start : pd.Timestamp | str, optional
        Start of drain-season to populate time-table. Should be < supply_season, by default "2019-10-01"
    new_nodes_offset : float, optional
        Offset control-node and demand-node with respect to connector-node, by default 10
    control_node_angle : int, optional
        Clock-wise (0 degrees is North) angle control-node with respect to connector-node, by default 90
    demand_node_angle : int, optional
        Clock-wise (0 degrees is North) angle demand-node with respect to connector-node, by default 45
    name : str, optional
        Name assigned to control-node, by default "uitlaat"
    demand_name_prefix: str, optional
        Prefix assigned to name in demand-node, by default "doorspoeling
    flushing_seasonal: bool, optional
        If True, flushing will be applied in summer season (April - October) only.
    """
    node_types = _read_node_table(model=model)["node_type"]
    for connector_node in flushing_nodes_df.itertuples():
        node_id = connector_node.Index
        node_type = connector_node.node_type
        demand_flow_rate = connector_node.demand_flow_rate
        # get upstream target_level and define min_upstream_level;
        us_node_id = connector_node.from_node_id
        us_target_level = _target_level(
            model=model,
            node_id=us_node_id,
            target_level_column=target_level_column,
            node_types=node_types,
            allow_missing=False,
        )
        min_upstream_level = [us_target_level + us_target_level_offset_supply, us_target_level]

        # Print so we can see what happens
        print(
            f"Adding flusing Node {node_id}: us_basin={us_node_id} | us_target_level={us_target_level} | demand={demand_flow_rate}"
        )

        # update static table
        control_state = ["aanvoer", "afvoer"]
        original_max_flow_rate = (
            getattr(model, pascal_to_snake_case(node_type))
            .static.df.set_index("node_id")
            .loc[[node_id], "flow_rate"]
            .max()
        )  # extract flow_rate from existing static-table
        static_table = getattr(nodes, pascal_to_snake_case(node_type)).Static
        model.update_node(
            node_id,
            node_type,
            [
                static_table(
                    min_upstream_level=min_upstream_level,
                    flow_rate=[0, 20],
                    max_flow_rate=[float("nan"), original_max_flow_rate],
                    control_state=control_state,
                )
            ],
        )

        # add discrete control
        thresholds = [
            us_target_level + us_threshold_offset,
            -(demand_flow_rate + demand_threshold_offset),
        ]

        supply_season_start = pd.to_datetime(supply_season_start)
        drain_season_start = pd.to_datetime(drain_season_start)
        tables = [
            discrete_control.Variable(
                compound_variable_id=[1, 2],
                listen_node_id=[us_node_id, node_id],
                variable=["level", "flow_rate"],
                weight=[1, -1],
            ),
            discrete_control.Condition(
                compound_variable_id=[1, 2],
                condition_id=[1, 2],
                threshold_high=thresholds,
                threshold_low=thresholds,
            ),
            discrete_control.Logic(
                truth_state=["FF", "FT", "TF", "TT"],
                control_state=["afvoer", "aanvoer", "afvoer", "afvoer"],
            ),
        ]

        add_and_connect_discrete_control_node(
            model=model,
            node_id=node_id,
            offset=new_nodes_offset,
            angle=control_node_angle,
            tables=tables,
            name=f"{name}: {us_target_level:.2f} [m+NAP]",
        )

        # add demand:
        # - seasonal: Apr tot Oct aan (cyclic Time-table)
        # - non-seasonal: altijd aan (Static, jaar-onafhankelijk)

        supply_season_start = pd.to_datetime(supply_season_start)
        year = supply_season_start.year

        t0 = pd.Timestamp(year, 1, 1)
        t_on = pd.Timestamp(year, 4, 1)  # aan vanaf 1 april
        t_off = pd.Timestamp(year, 10, 1)  # uit vanaf 1 oktober

        if flushing_seasonal:
            demand_tables = [
                flow_demand.Time(
                    time=[t0, t_on, t_off],
                    demand=[0.0, float(demand_flow_rate), 0.0],
                    demand_priority=[1, 1, 1],
                )
            ]
            cyclic = True
        else:
            demand_tables = [
                flow_demand.Static(
                    demand=[float(demand_flow_rate)],
                    demand_priority=[1],
                )
            ]
            cyclic = False  # bij Static niet essentieel, maar netjes

        node = model.get_node(node_id=node_id)

        demand_node = model.flow_demand.add(
            _offset_new_node(
                node=node,
                offset=new_nodes_offset,
                angle=demand_node_angle,
                name=f"{demand_name_prefix} {demand_flow_rate} [m3/s]",
                cyclic_time=cyclic,
            ),
            tables=demand_tables,
        )
        model.link.add(demand_node, node)


def add_controllers_to_connector_nodes(
    model: Model,
    node_functions_df: gpd.GeoDataFrame,
    level_difference_threshold: float,
    target_level_column: str = "meta_streefpeil",
    flushing_seasonal: bool = False,
):
    """Add controllers to connector nodes per function

    The `function` column in `node_functions_df` can have 4 values with Dutch explanations:
    - `drain`: uitlaat
    - `supply`: inlet
    - `flow_control`: doorlaat
    - `flushing`: doorspoeling (uitlaat waarbij een minimale afvoer wordt gerealiseerd)

    The column `demand` in `node_functions_df` only needs a capacity at nodes of type `flusing`. For other nodes this can be NaN and will be ignored.

    Explanation of a node's `function` in terms of upstream/downstream levels, supply (aanvoer) or drain (afvoer) state:

    | function      | state determined by               | flow_rate @ supply-state | flow_rate @ drain-state | Nodes added                  |
    |---------------|-----------------------------------|--------------------------|-------------------------|------------------------------|
    | supply        | downstream level                  | capacity [m3/s]          | 0 [m3/s]                | DiscreteControl              |
    | drain         | upstream level                    | 0 [m3/s]                 | capacity [m3/s]         | DiscreteControl              |
    | flow control  | upstream & downstream level       | capacity [m3/s]          | capacity [m3/s]         | DiscreteControl              |
    | flushing      | upstream level & node flow_rate   | demand [m3/s]            | capacity [m3/s]         | DiscreteControl + FlowDemand |

    Parameters
    ----------
    model : Model
        Ribasim model
    node_functions_df : gpd.GeoDataFrame
        Table with columns `node_id`, `from_node_id`, `to_node_id`, `function` and `demand`
    level_difference_threshold : float
        Level offset of discrete-control to trigger flow. Should be => model.solver.level_difference_threshold
    target_level_column : str, optional
        Column in Basin.Area table to read target_level, by default "meta_streefpeil"
    flushing_seasonal: bool, optional
        If True, flushing will be applied in summer season (April - October) only.
    """
    # make sure add-api will not duplicate node-ids
    model._update_used_ids()

    # add supply nodes
    supply_nodes_df = node_functions_df[node_functions_df["function"] == "supply"]
    if not supply_nodes_df.empty:
        add_controllers_to_supply_nodes(
            model=model,
            us_target_level_offset_supply=-0.04,
            supply_nodes_df=supply_nodes_df,
        )

    # add drain nodes
    drain_nodes_df = node_functions_df[node_functions_df["function"] == "drain"]
    if not drain_nodes_df.empty:
        add_controllers_to_drain_nodes(model=model, drain_nodes_df=drain_nodes_df)

    # add flow control nodes
    flow_control_nodes_df = node_functions_df[node_functions_df["function"] == "flow_control"]
    if not flow_control_nodes_df.empty:
        add_controllers_to_flow_control_nodes(
            model=model,
            flow_control_nodes_df=flow_control_nodes_df,
            us_threshold_offset=level_difference_threshold,
        )

    # add flusing_nodes_df
    flushing_nodes_df = node_functions_df[node_functions_df["function"] == "flushing"]
    if not flushing_nodes_df.empty:
        add_controllers_and_demand_to_flushing_nodes(
            model=model,
            flushing_nodes_df=flushing_nodes_df,
            us_threshold_offset=level_difference_threshold,
            target_level_column=target_level_column,
            flushing_seasonal=flushing_seasonal,
        )


def add_controllers_to_supply_area(
    model: Model,
    polygon: Polygon,
    ignore_intersecting_links: list[int],
    drain_nodes: list[int],
    flushing_nodes: dict[int, float],
    supply_nodes: list[int],
    level_difference_threshold: float,
    flow_control_nodes: list[int] | None = None,
    exclude_nodes: list[int] | None = None,
    control_node_types: list[Literal["Pump", "Outlet"]] = ["Pump", "Outlet"],
    is_supply_node_column: str = "meta_supply_node",
    target_level_column: str = "meta_streefpeil",
    flushing_seasonal: bool = False,
) -> gpd.GeoDataFrame:
    """Add all controllers to supply area

    The resulting `function` column can have 4 values with Dutch explanations:
    - `drain`: uitlaat
    - `supply`: inlet
    - `flow_control`: doorlaat
    - `flushing`: doorspoeling (uitlaat waarbij een minimale afvoer wordt gerealiseerd)

    The column `demand` only needs a capacity at nodes of type `flusing`. For other nodes this can be NaN and will be ignored.

    Explanation of a node's `function` in terms of upstream/downstream levels, supply (aanvoer) or drain (afvoer) state:
    | function      | state determined by               | flow_rate @ supply-state | flow_rate @ drain-state | Nodes added                  |
    |---------------|-----------------------------------|--------------------------|-------------------------|------------------------------|
    | supply        | downstream level                  | capacity [m3/s]          | 0 [m3/s]                | DiscreteControl              |
    | drain         | upstream level                    | 0 [m3/s]                 | capacity [m3/s]         | DiscreteControl              |
    | flow control  | upstream & downstream level       | capacity [m3/s]          | capacity [m3/s]         | DiscreteControl              |
    | flushing      | upstream level & node flow_rate   | demand [m3/s]            | capacity [m3/s]         | DiscreteControl + FlowDemand |

    Parameters
    ----------
    model : Model
        Ribasim Model
    polygon : Polygon
        Polygon of supply area
    ignore_intersecting_links : list[int]
        Optional list of links that can be ignored in producing outflow or inflow control nodes.
        Be cautious (!), only add id's to this list if you are sure it won't affect supply, by default []
    drain_nodes : list[int]
        List of node_ids that will be forced to drain
    flushing_nodes : dict[int, float]
        Flushing nodes with their demands in the form of {node_id:demand}
    supply_nodes : list[int]
        List of node_ids that will be forced to supply
    flow_control_nodes: list[int], optional
        List of node_ids that will be forced to flow_control. By default None
    level_difference_threshold : float
        Level offset of discrete-control to trigger flow. Should be => model.solver.level_difference_threshold
    exclude_nodes : list[int], optional
        List of node_ids that are within the supply area, but will be ignored, by default None
    control_node_types : list[str], optional
        Node_types considered to be control nodes , by default ["Outlet", "Pump"]
    is_supply_node_column : str, optional
        Column in model.pump.node.df and model.outlet.node.df indicates if node is a supply-node, by default "meta_supply_node"
    target_level_column : str, optional
        Column in Basin.Area table to read target_level, by default "meta_streefpeil"

    Returns
    -------
    gpd.GeoDataFrame
        Table with columns `node_id`, `from_node_id`, `to_node_id`, `function` and `demand` for verification
    """
    flow_control_nodes = flow_control_nodes or []
    exclude_nodes = exclude_nodes or []

    node_positions_df = get_control_nodes_position_from_supply_area(
        model=model,
        polygon=polygon,
        exclude_nodes=exclude_nodes,
        control_node_types=control_node_types,
        ignore_intersecting_links=ignore_intersecting_links,
    )

    # 2. determine node functions (drain, supply, flow_control and flushing)
    node_functions_df = add_control_functions_to_connector_nodes(
        model=model,
        node_positions=node_positions_df["position"],
        supply_nodes=supply_nodes,
        drain_nodes=drain_nodes,
        flushing_nodes=flushing_nodes,
        flow_control_nodes=flow_control_nodes,
        is_supply_node_column=is_supply_node_column,
    )

    # 3. add controllers to all nodes
    add_controllers_to_connector_nodes(
        model=model,
        node_functions_df=node_functions_df,
        level_difference_threshold=level_difference_threshold,
        target_level_column=target_level_column,
        flushing_seasonal=flushing_seasonal,
    )

    return node_functions_df


def add_controllers_to_uncontrolled_connector_nodes(
    model: "Model",
    us_threshold_offset: float,
    exclude_nodes: list[int] | None = None,
    supply_nodes: list[int] | None = None,
    drain_nodes: list[int] | None = None,
    flow_control_nodes: list[int] | None = None,
    flushing_nodes: dict[int, float] | None = None,
    control_node_types: list[Literal["Pump", "Outlet"]] | None = None,
    us_target_level_offset_supply: float = -0.04,
    level_difference_threshold: float | None = None,
    flushing_seasonal: bool = False,
):
    """
    Voeg controllers toe aan ALLE connector nodes (Pump/Outlet) die nog géén control-link hebben.

    Regels (simpel en voorspelbaar):
    1) Alleen nodes zonder bestaande control-link én niet in exclude_nodes worden meegenomen.
    2) Functie-toewijzing met prioriteit:
       - flow_control_nodes  (hoogste prioriteit)
       - drain_nodes
       - supply_nodes + meta_supply_node (laagste prioriteit)
       - alles wat overblijft -> drain
    3) Nodes die al controlled zijn worden genegeerd (dus geen dubbele control-innneighbors).

    Parameters
    ----------
    model : Model
    us_threshold_offset : float
        Offset voor flow-control discrete control (moet matchen met solver threshold).
    exclude_nodes : list[int]
        Connector-nodes die je niet wilt aansturen.
    supply_nodes : list[int]
        Nodes die je expliciet als supply wilt (alleen als nog uncontrolled).
    drain_nodes : list[int]
        Nodes die je expliciet als drain wilt (alleen als nog uncontrolled).
    flow_control_nodes : list[int]
        Nodes die je expliciet als flow_control wilt (alleen als nog uncontrolled).
    flushing_nodes : dict[int, float]
        Flushing nodes with their demands in the form of {node_id:demand} (onnly if still uncontrolled)
    control_node_types : list[Literal["Pump","Outlet"]]
        Welke connector node types meegenomen worden.
    us_target_level_offset_supply : float
        Offset voor supply controls.
    """
    # make sure add-api will not duplicate node-ids
    model._update_used_ids()

    # --- defaults veilig maken (nooit [] als default-arg) ---
    exclude_nodes = exclude_nodes or []
    supply_nodes = supply_nodes or []
    drain_nodes = drain_nodes or []
    flushing_nodes = flushing_nodes or {}
    flow_control_nodes = flow_control_nodes or []
    control_node_types = control_node_types or ["Pump", "Outlet"]

    # --- 1) bepaal welke connector nodes al controlled zijn ---
    control_links = _read_link_table(model=model, link_type="control")
    controlled = set(control_links.to_node_id.to_list())

    # --- 2) haal connector nodes + from/to ---
    node_df = get_node_table_with_from_to_node_ids(model=model)
    connector_df = node_df[node_df.node_type.isin(control_node_types)].copy()

    # zorg dat meta_supply_node bestaat
    if "meta_supply_node" not in connector_df.columns:
        connector_df["meta_supply_node"] = False
    connector_df["meta_supply_node"] = connector_df["meta_supply_node"].fillna(False).astype(bool)

    # --- 3) bepaal welke nodes we überhaupt mogen aanpassen ---
    eligible = set(connector_df.index) - controlled - set(exclude_nodes)
    if not eligible:
        return  # niets te doen

    # --- 4) clip handmatige lijsten naar eligible (zo voorkom je dubbele control) ---
    flushing_set = set(flushing_nodes.keys()) & eligible
    flow_control_set = set(flow_control_nodes) & eligible
    drain_set = set(drain_nodes) & eligible
    supply_set_manual = set(supply_nodes) & eligible

    # automatische supply op basis van meta_supply_node (maar alleen eligible)
    supply_set_auto = set(connector_df.index[connector_df.meta_supply_node]) & eligible

    # --- 5) prioriteiten afdwingen: flushing_set > flow > drain > supply ---
    # (dus: als iets in flusing_set zit, haal het uit flow/drain/supply. Als iets in flow zit, haal het uit drain/supply; als iets in drain zit, haal uit supply)
    flow_control_set -= flushing_set
    drain_set -= flushing_set | flow_control_set
    supply_set_manual -= flushing_set | flow_control_set | drain_set
    supply_set_auto -= flushing_set | flow_control_set | drain_set

    supply_set = supply_set_manual | supply_set_auto

    # --- 6) alles wat overblijft -> drain ---
    used = flushing_set | flow_control_set | drain_set | supply_set
    remaining = eligible - used
    drain_set = drain_set | remaining

    # --- 7) uitvoer: controllers toevoegen ---
    # Flushing
    if flushing_set:
        node_ids = sorted(flushing_set)
        flushing_nodes_df = connector_df.loc[node_ids].copy()

        # demand_flow_rate kolom vullen (alleen voor deze flushing nodes)
        flushing_nodes_df["demand_flow_rate"] = pd.Series(index=flushing_nodes_df.index, dtype="float")
        flushing_nodes_df.loc[node_ids, "demand_flow_rate"] = [flushing_nodes[n] for n in node_ids]

        level_difference_threshold = level_difference_threshold or model.solver.level_difference_threshold
        add_controllers_and_demand_to_flushing_nodes(
            model=model,
            flushing_nodes_df=flushing_nodes_df,
            us_threshold_offset=level_difference_threshold,
            flushing_seasonal=flushing_seasonal,
        )

    # Supply
    if supply_set:
        supply_df = connector_df.loc[sorted(supply_set)]
        add_controllers_to_supply_nodes(
            model=model,
            us_target_level_offset_supply=us_target_level_offset_supply,
            supply_nodes_df=supply_df,
        )

    # Flow control
    if flow_control_set:
        flow_df = connector_df.loc[sorted(flow_control_set)]
        add_controllers_to_flow_control_nodes(
            model=model,
            flow_control_nodes_df=flow_df,
            us_threshold_offset=us_threshold_offset,
        )

    # Drain
    if drain_set:
        drain_df = connector_df.loc[sorted(drain_set)]
        add_controllers_to_drain_nodes(model=model, drain_nodes_df=drain_df)

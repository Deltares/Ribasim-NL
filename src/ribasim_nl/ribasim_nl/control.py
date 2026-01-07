# %%

import pandas as pd
from ribasim import Node, nodes
from ribasim.nodes import discrete_control, flow_demand
from shapely.geometry import Point, Polygon

from ribasim_nl import Model
from ribasim_nl.case_conversions import pascal_to_snake_case


def _offset_new_node(node: Node, x_offset: float = 10, y_offset: float = 0, **kwargs) -> Node:
    """Create a new Ribasim.Node with x_offset and y_offset of existing node."""
    return Node(geometry=Point(node.geometry.x + x_offset, node.geometry.y + y_offset), **kwargs)


def _upstream_target_level(
    model: Model, node_id: int, target_level_column: str, allow_missing: bool = True
) -> tuple[int, float | None]:
    """Get upstream node_id and target-level of a control-node"""
    us_node_id = model.upstream_node_id(node_id=node_id)

    # without an upstream node, there is no upstream target-level
    if us_node_id is None:
        raise ValueError(f"Control Node {node_id} does not have an upstream node")

    us_node_type = model.get_node_type(node_id=us_node_id)

    # if Basin, we read the target_level from the basin.Area
    if us_node_type == "Basin":
        us_target_level = model.basin.area.df.set_index("node_id").at[us_node_id, target_level_column]
        if us_node_id is None or us_target_level is None:
            msg = f"Control Node {node_id}: target_level missing in upstream basin {us_node_id}"
            raise ValueError(msg)
    # if LevelBoundary, we don't read target_level yet
    elif us_node_type == "LevelBoundary":
        us_target_level = None

    # Cannot imagine another node_type, but just in case
    else:
        msg = f"Control Node {node_id}: upstream node {us_node_id} ({us_node_type}) not of type Basin or LevelBoundary"
        raise ValueError(msg)

    if not allow_missing and us_target_level is None:
        raise ValueError(
            f"Control Node {node_id}: no target_level found in upstream node {us_node_id} ({us_node_type}). Set `allow_missing=True` if this is intended"
        )

    return us_node_id, us_target_level


def _downstream_target_level(model: Model, node_id: int, target_level_column: str) -> tuple[int, float]:
    """Get downstream node_id and target-level of a control-node"""
    ds_node_id = model.downstream_node_id(node_id=node_id)

    # without an downstream node, there is no upstream target-level
    if ds_node_id is None:
        raise ValueError(f"Control Node {node_id} does not have a downstream node")

    ds_node_type = model.get_node_type(node_id=ds_node_id)

    # downstream_target_levels are always managed for basins
    if ds_node_type != "Basin":
        msg = f"Control Node {node_id}: upstream node {ds_node_id} ({ds_node_type}) not of type Basin"
        raise ValueError(msg)

    ds_target_level = model.basin.area.df.set_index("node_id").at[ds_node_id, target_level_column]

    if ds_target_level is None:
        msg = f"Drain node {node_id}: target_level missing in downstream basin {ds_node_id}"
        raise ValueError(msg)

    return ds_node_id, ds_target_level


def discrete_control_tables_single_basin(listen_node_id: int, control_state: list[str, str], theshold: float) -> list:
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
            threshold_high=[theshold],
            threshold_low=[theshold],
        ),
        discrete_control.Logic(truth_state=["F", "T"], control_state=control_state),
    ]


def add_and_connect_discrete_control_node(
    model: Model, node_id: int, x_offset: float, y_offset: float, tables=list, **kwargs
):
    """Add and connect a DiscreteControl Node to an existing node."""
    connector_node = model.get_node(node_id)
    control_node = model.discrete_control.add(
        _offset_new_node(connector_node, x_offset=x_offset, y_offset=y_offset, **kwargs), tables
    )
    model.link.add(control_node, connector_node)


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
    model: Model,
    drain_nodes: list[int],
    target_level_column: str = "meta_streefpeil",
    x_offset: float = 10,
    y_offset: float = 0,
    name: str = "uitlaat",
):
    """Add control nodes to connector nodes draining a system/supply-area

    Control-nodes draining a system only maintain upstream water-levels and reduce to 0 m3/s if control-state is aanvoer

    Parameters
    ----------
    model : Model
        Ribasim Model
    drain_nodes : list[int]
        List of connector node_ids having a draining function
    target_level_column : str, optional
        Column in Basin.Area table that contains target level, by default "meta_streefpeil"
    x_offset : float, optional
        x_offset control-node with respect to drain node, by default 10
    y_offset : float, optional
        y_offset control-node with respect to drain node, by default 0

    Raises
    ------
    ValueError
        Target level in upstream basin cannot be found
    """
    for node_id in drain_nodes:
        # get targed_level and define min_upstream_level; [target_level, target_level]
        us_node_id, us_target_level = _upstream_target_level(
            model=model, node_id=node_id, target_level_column=target_level_column
        )
        if us_target_level is None:
            raise ValueError(f"Drain node {node_id}: target_level missing in upstream basin {us_node_id}")
        min_upstream_level = [us_target_level] * 2

        # Print so we can see what happens
        print(f"Adding drain node {node_id}: us_basin={us_node_id} | us_target_level={us_target_level:.3f}")

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
                    min_upstream_level=min_upstream_level,
                    flow_rate=[0, 100],
                    max_flow_rate=[0, original_max_flow_rate],
                    control_state=control_state,
                )
            ],
        )

        # add control_node
        tables = discrete_control_tables_single_basin(
            listen_node_id=us_node_id, control_state=control_state, theshold=us_target_level
        )

        add_and_connect_discrete_control_node(
            model=model, node_id=node_id, x_offset=x_offset, y_offset=y_offset, tables=tables, name=name
        )


def add_controllers_to_supply_nodes(
    model: Model,
    supply_nodes: list[int],
    us_target_level_offset_supply: float = -0.04,
    target_level_column: str = "meta_streefpeil",
    x_offset: float = 10,
    y_offset: float = 0,
    name: str = "inlaat",
):
    """Add control nodes to connector nodes supplying a system/supply-area

    Parameters
    ----------
    model : Model
        Ribasim Model
    supply_nodes : list[int]
        List of connector node_ids having a supply function
    us_target_level_offset_supply : float, optional
        Lowering upstream target levels in supply situation, by default -0.04
    target_level_column : str, optional
        Column in Basin.Area table that contains target level, by default "meta_streefpeil"
    x_offset : float, optional
        x_offset control-node with respect to drain node, by default 10
    y_offset : float, optional
        y_offset control-node with respect to drain node, by default 0
    """
    for node_id in supply_nodes:
        # get downstream target level, cannot be None (!)
        ds_node_id, ds_target_level = _downstream_target_level(
            model=model, node_id=node_id, target_level_column=target_level_column
        )
        max_downstream_level = [ds_target_level, float("nan")]

        # get upstream target_level and define min_upstream_level;
        # None if LevelBoundary, else [us_target_level + target_level_offset_supply, us_target_level]
        us_node_id, us_target_level = _upstream_target_level(
            model=model, node_id=node_id, target_level_column=target_level_column
        )
        min_upstream_level = (
            None if us_target_level is None else [us_target_level + us_target_level_offset_supply, us_target_level]
        )

        # Print so we can see what happens
        print(
            f"Adding supply Node {node_id}: ds_basin={ds_node_id} | ds_target_level={ds_target_level} | us_basin={us_node_id} | us_target_level={us_target_level}"
        )

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
            listen_node_id=ds_node_id, control_state=control_state, theshold=ds_target_level
        )
        add_and_connect_discrete_control_node(
            model=model, node_id=node_id, x_offset=x_offset, y_offset=y_offset, tables=tables, name=name
        )


def add_controllers_to_flow_control_nodes(
    model: Model,
    flow_control_nodes: list[int],
    us_threshold_offset: float,
    us_target_level_offset_supply: float = -0.04,
    target_level_column: str = "meta_streefpeil",
    x_offset: float = 10,
    y_offset: float = 0,
    name: str = "doorlaat",
):
    for node_id in flow_control_nodes:
        # get downstream target level, cannot be None (!)
        ds_node_id, ds_target_level = _downstream_target_level(
            model=model, node_id=node_id, target_level_column=target_level_column
        )
        max_downstream_level = [ds_target_level, 9999]

        # get upstream target_level and define min_upstream_level;
        # None if LevelBoundary, else [us_target_level + target_level_offset_supply, us_target_level]
        us_node_id, us_target_level = _upstream_target_level(
            model=model, node_id=node_id, target_level_column=target_level_column, allow_missing=False
        )
        min_upstream_level = [us_target_level + us_target_level_offset_supply, us_target_level]

        print(
            f"Adding control Node {node_id}: ds_basin={ds_node_id} | ds_target_level={ds_target_level} | us_basin={us_node_id} | us_target_level={us_target_level}"
        )

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
            model=model, node_id=node_id, x_offset=x_offset, y_offset=y_offset, tables=tables, name=name
        )


def add_controllers_and_demand_to_flushing_nodes(
    model: Model,
    flushing_nodes: dict[int, float],
    us_threshold_offset: float,
    demand_threshold_offset: float = 0.001,
    us_target_level_offset_supply: float = -0.04,
    target_level_column: str = "meta_streefpeil",
    supply_season_start: pd.Timestamp | str = "2020-04-01",
    drain_season_start: pd.Timestamp | str = "2019-10-01",
    x_offset_control_node: float = 10,
    y_offset_control_node: float = 0,
    x_offset_demand_node: float = 10,
    y_offset_demand_node: float = 10,
):
    for node_id, demand_flow_rate in flushing_nodes.items():
        # get upstream target_level and define min_upstream_level;
        # None if LevelBoundary, else [us_target_level + target_level_offset_supply, us_target_level]
        us_node_id, us_target_level = _upstream_target_level(
            model=model, node_id=node_id, target_level_column=target_level_column, allow_missing=False
        )
        min_upstream_level = [us_target_level + us_target_level_offset_supply, us_target_level]

        # Print so we can see what happens
        print(
            f"Adding flusing Node {node_id}: us_basin={us_node_id} | us_target_level={us_target_level} | demand={demand_flow_rate}"
        )

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
                    min_upstream_level=min_upstream_level,
                    flow_rate=[0, 20],
                    max_flow_rate=[float("nan"), original_max_flow_rate],
                    control_state=control_state,
                )
            ],
        )

        # add discrete control
        # thresholds = [
        #     -9999,
        #     us_target_level + us_threshold_offset,
        #     -9999,
        #     -(demand_flow_rate + demand_threshold_offset),
        #     -(demand_flow_rate + demand_threshold_offset),
        # ]
        thresholds = [
            us_target_level + us_threshold_offset,
            -(demand_flow_rate + demand_threshold_offset),
        ]

        supply_season_start = pd.to_datetime(supply_season_start)
        drain_season_start = pd.to_datetime(drain_season_start)
        # time = [
        #     drain_season_start,
        #     supply_season_start,
        #     drain_season_start + pd.DateOffset(years=1),
        #     drain_season_start,
        #     drain_season_start + pd.DateOffset(years=1),
        # ]
        tables = [
            discrete_control.Variable(
                compound_variable_id=[1, 2],
                listen_node_id=[us_node_id, node_id],
                variable=["level", "flow_rate"],
                weight=[1, -1],
            ),
            discrete_control.Condition(
                # compound_variable_id=[1, 1, 1, 2, 2],
                # condition_id=[1, 1, 1, 2, 2],
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
            x_offset=x_offset_control_node,
            y_offset=y_offset_control_node,
            tables=tables,
            # cyclic_time=True,
        )

        # add demand
        tables = [
            # flow_demand.Time(time=time[:3], demand=[0, demand_flow_rate, 0], demand_priority=[1] * 3),
            flow_demand.Static(demand=[demand_flow_rate], demand_priority=[1])
        ]
        connector_node = model.get_node(node_id=node_id)
        demand_node = model.flow_demand.add(
            _offset_new_node(
                node=connector_node,
                x_offset=x_offset_demand_node,
                y_offset=y_offset_demand_node,
                name=f"demand {demand_flow_rate} m3/s",
            ),
            tables=tables,
        )
        model.link.add(demand_node, connector_node)

import pandas as pd

from ribasim_nl import Model


def upstream_target_levels(
    model: Model, node_ids: list[int], target_level_column: str = "meta_streefpeil"
) -> pd.Series:
    """Return upstream target_levels for a list of node_ids if these node_ids are present in model.basin.area.df

    Args:
        model (Model): model (Model): Ribasim model
        node_ids (list[int]): Ribasim node_ids for connector Nodes
        target_level_column (str, optional): column in basin.area.df containing target-level. Defaults to "meta_streefpeil".

    Returns
    -------
        pd.Series: float-type series with target-levels
    """
    # get upstream node_id from edge_table
    df = model.edge.df[model.edge.df["to_node_id"].isin(node_ids)][["to_node_id", "from_node_id"]]
    df.set_index("to_node_id", inplace=True)
    df.index.name = "node_id"
    df.columns = ["upstream_node_id"]

    # get in_basin_mask; we can only get target_levels if upstream_node_id is present as node_id in model.basin.area.df
    df = df.reset_index().set_index("upstream_node_id")
    in_basin_mask = df.index.isin(model.basin.area.df.node_id)

    # get upstream_target_level from basin.area.df
    df.loc[in_basin_mask, "upstream_target_level"] = model.basin.area.df.set_index("node_id").loc[
        df[in_basin_mask].index, target_level_column
    ]

    # sanitize df and only return upstream_target_level_series
    series = df.reset_index().set_index("node_id").sort_index()["upstream_target_level"]

    return series


def downstream_target_levels(
    model: Model, node_ids: list[int], target_level_column: str = "meta_streefpeil"
) -> pd.Series:
    """Return downstream target_levels for a list of node_ids if these node_ids are present in model.basin.area.df

    Args:
        model (Model): model (Model): Ribasim model
        node_ids (list[int]): Ribasim node_ids for connector Nodes
        target_level_column (str, optional): column in basin.area.df containing target-level. Defaults to "meta_streefpeil".

    Returns
    -------
        pd.Series: float-type series with target-levels
    """
    # get downstream node_id from edge_table
    df = model.edge.df[model.edge.df["from_node_id"].isin(node_ids)][["from_node_id", "to_node_id"]]
    df.set_index("from_node_id", inplace=True)
    df.index.name = "node_id"
    df.columns = ["downstream_node_id"]

    # get in_basin_mask; we can only get target_levels if downstream_node_id is present as node_id in model.basin.area.df
    df = df.reset_index().set_index("downstream_node_id")
    in_basin_mask = df.index.isin(model.basin.area.df.node_id)

    # get downstream_target_level from basin.area.df
    df.loc[in_basin_mask, "downstream_target_level"] = model.basin.area.df.set_index("node_id").loc[
        df[in_basin_mask].index, target_level_column
    ]

    # sanitize df and only return downstream_target_level_series
    series = df.reset_index().set_index("node_id").sort_index()["downstream_target_level"]

    return series

import numpy as np
import pandas as pd

from ribasim_nl import Model
from ribasim_nl.parametrization.conversions import round_to_precision

valid_node_types = ["Outlet", "TabulatedRatingCurve", "Pump"]


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


def upstream_target_level(
    model: Model, node_id: int, target_level_column: str = "meta_streefpeil", precision: int | float = 0.01
) -> float:
    f"""Return the upstream target-level of a connector Node if it is provided in as column-value in a the basin.area

    Args:
        model (Model): Ribasim model
        node_id (int): node_id of connector Node
        target_level_column (str, optional): column in basin.area that holds target_levels. Defaults to "meta_streefpeil".
        precision (float): The rounding precision (e.g., 10, 100, 0.1). Default = 0.01

    Raises:
        ValueError: if node_id does not belong to a connector Node with type {valid_node_types}

    Returns:
        float: upstream target-level
    """
    # check if we have a valid node to get upstream_level from
    node_type = model.get_node_type(node_id=node_id)
    if node_type not in valid_node_types:
        raise ValueError(f"node_type of node_id {node_id} not valid: {node_type} not in {valid_node_types}")

    us_node_id = model.upstream_node_id(node_id=node_id)
    return get_target_level(
        model=model, node_id=us_node_id, target_level_column=target_level_column, precision=precision
    )


def downstream_target_level(
    model: Model, node_id: int, target_level_column: str = "meta_streefpeil", precision: int | float = 0.01
) -> float:
    f"""Return the downstream target-level of a connector Node if it is provided in as column-value in a the basin.area

    Args:
        model (Model): Ribasim model
        node_id (int): node_id of connector Node
        target_level_column (str, optional): column in basin.area that holds target_levels. Defaults to "meta_streefpeil".
        precision (float): The rounding precision (e.g., 10, 100, 0.1). Default = 0.01

    Raises:
        ValueError: if node_id does not belong to a connector Node with type {valid_node_types}

    Returns:
        float: donwstream target-level
    """
    # check if we have a valid node to get upstream_level from
    node_type = model.get_node_type(node_id=node_id)
    if node_type not in valid_node_types:
        raise ValueError(f"node_type of node_id {node_id} not valid: {node_type} not in {valid_node_types}")

    ds_node_id = model.downstream_node_id(node_id=node_id)
    return get_target_level(
        model=model, node_id=ds_node_id, target_level_column=target_level_column, precision=precision
    )


def get_target_level(
    model: Model, node_id: int, target_level_column: str = "meta_streefpeil", precision: int | float = 0.01
) -> float:
    """Return the target-level of a basin if it is provided in as column-value in a the basin.area

    Args:
        model (Model): Ribasim model
        node_id (int): node_id of basin Node
        target_level_column (str, optional): _description_. Defaults to "meta_streefpeil".
        precision (float): The rounding precision (e.g., 10, 100, 0.1). Default = 0.01

    Returns
    -------
        float: _description_
    """
    # if us_node_id is None, we return NA
    if node_id is None:
        return np.nan
    else:  # if us_node_type != Basin, we don't have a target_level
        us_node_type = model.get_node_type(node_id=node_id)
        if us_node_type != "Basin":
            return np.nan
        else:
            if node_id in model.basin.area.df.node_id.to_numpy():
                return round_to_precision(
                    number=model.basin.area.df.set_index("node_id").at[node_id, target_level_column],
                    precision=precision,
                )
            else:
                return np.nan

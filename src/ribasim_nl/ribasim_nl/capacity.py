"""Level limits of the DiscreteControl-controlled Outlets and Pumps."""

import logging

import numpy as np
import pandas as pd

from ribasim_nl.model import Model

LOG = logging.getLogger(__name__)

CONTROLLED_NODE_TYPES = ("Outlet", "Pump")


def _static_dfs(model: Model) -> list[pd.DataFrame]:
    """Get the static tables of the controlled node types that have one."""
    static_dfs = [model.get_component(node_type).static.df for node_type in CONTROLLED_NODE_TYPES]
    return [static_df for static_df in static_dfs if static_df is not None]


def _controllers(model: Model) -> pd.Series:
    """Get the DiscreteControl node IDs of each controlled node."""
    link_df = model.link.df
    control_link_df = link_df[link_df["link_type"] == "control"]
    discrete_control_ids = model.filter_nodes("DiscreteControl").index
    control_link_df = control_link_df[control_link_df["from_node_id"].isin(discrete_control_ids)]
    return control_link_df.groupby("to_node_id")["from_node_id"].apply(set)


def _listened_nodes(model: Model) -> dict[int, set[int]]:
    """Get the node IDs that the DiscreteControl nodes of each controlled node listen to."""
    variable_df = model.discrete_control.variable.df
    if variable_df is None:
        return {}
    listened = variable_df.groupby("node_id")["listen_node_id"].apply(set)
    controllers = _controllers(model)
    return {
        int(node_id): set().union(*(listened.get(c, set()) for c in controller_ids))
        for node_id, controller_ids in zip(controllers.index.to_numpy(), controllers, strict=True)
    }


def _flow_link_df(model: Model) -> pd.DataFrame:
    return model.link.df[model.link.df["link_type"] == "flow"]


def _downstream_neighbor(model: Model) -> pd.Series:
    """Get the downstream flow neighbor of each controlled node."""
    flow_link_df = _flow_link_df(model)
    flow_link_df = flow_link_df[flow_link_df["from_node_id"].isin(_controllers(model).index)]
    assert not flow_link_df["from_node_id"].duplicated().any(), "Expected one downstream flow link per controlled node"
    return flow_link_df.set_index("from_node_id")["to_node_id"]


def bound_supply_inflows(model: Model, target_level_column: str = "meta_streefpeil") -> list[int]:
    """Limit the `aanvoer` control state of inflows into a controlled Basin to the target level of that Basin.

    A supplying Outlet or Pump without a `max_downstream_level` can fill the Basin it supplies far above its target
    level, as long as its DiscreteControl does not switch, which is up to a day with a daily control interval.

    :param model: model with DiscreteControl-controlled Outlets and Pumps
    :param target_level_column: column of the Basin / area table with the target level
    :return: the node IDs that got a `max_downstream_level`
    """
    assert model.basin.area.df is not None
    target_levels = model.basin.area.df.groupby("node_id")[target_level_column].first()
    basin_ids = model.filter_nodes("Basin").index
    listened = _listened_nodes(model)
    downstream = _downstream_neighbor(model)
    bounded = []
    for static_df in _static_dfs(model):
        max_downstream_level = static_df["max_downstream_level"].astype(float)
        capacity = static_df[[c for c in ("flow_rate", "max_flow_rate") if c in static_df.columns]].max(axis=1)
        to_node_id = static_df["node_id"].map(downstream)
        is_listened_basin = [
            to_id in basin_ids and to_id in listened.get(node_id, set())
            for node_id, to_id in zip(static_df["node_id"], to_node_id, strict=True)
        ]
        rows = (
            static_df["control_state"].eq("aanvoer")
            & capacity.gt(0)
            & (max_downstream_level.isna() | np.isinf(max_downstream_level))
            & pd.Series(is_listened_basin, index=static_df.index)
        )
        levels = to_node_id[rows].map(target_levels)
        assert levels.notna().all(), f"Missing target levels of Basins {to_node_id[rows][levels.isna()].tolist()}"
        static_df.loc[rows, "max_downstream_level"] = levels
        bounded.extend(static_df.loc[rows, "node_id"].tolist())
    LOG.info(f"Limited the supply of {len(bounded)} nodes to the target level of the Basin they supply")
    return bounded

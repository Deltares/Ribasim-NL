# %%
import pandas as pd
from ribasim import Model


def reset_index(model: Model, node_start=1):
    # make sure column node_id == index
    node_ids = model.network.node.df.index
    model.network.node.df.loc[:, "node_id"] = node_ids

    # create a new index for re-indexing all tables
    index = pd.Series(
        data=[i + node_start for i in range(len(node_ids))], index=node_ids
    )

    # re-index node_id and fid
    model.network.node.df.index = model.network.node.df["node_id"].apply(
        lambda x: index.loc[x]
    )
    model.network.node.df.loc[:, "node_id"] = model.network.node.df.index
    model.network.node.df.index.name = "fid"

    # renumber edges
    model.network.edge.df.loc[:, ["from_node_id"]] = model.network.edge.df[
        "from_node_id"
    ].apply(lambda x: index.loc[x])

    model.network.edge.df.loc[:, ["to_node_id"]] = model.network.edge.df[
        "to_node_id"
    ].apply(lambda x: index.loc[x])

    # renumber tables
    for df in [
        model.basin.static.df,
        model.basin.profile.df,
        model.basin.state.df,
        model.linear_resistance.static.df,
        model.manning_resistance.static.df,
        model.pump.static.df,
        model.outlet.static.df,
        model.terminal.static.df,
        model.flow_boundary.static.df,
        model.level_boundary.static.df,
        model.fractional_flow.static.df,
        model.tabulated_rating_curve.static.df,
    ]:
        if df is not None:
            df.loc[:, ["node_id"]] = df["node_id"].apply(lambda x: index.loc[x])

    return model


# %%

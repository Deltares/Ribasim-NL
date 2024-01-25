import pandas as pd
from ribasim import Model


def reset_index(model: Model, node_start=1):
    node_ids = model.network.node.df.index
    index = pd.Series(
        data=[i + node_start for i in range(len(node_ids))], index=node_ids
    )

    # renumber nodes
    model.network.node.df.loc[:, ["node_id"]] = model.network.node.df["node_id"].apply(
        lambda x: index.loc[x]
    )

    # renumber edges
    model.network.edge.df.loc[:, ["from_node_id"]] = model.network.edge.df[
        "from_node_id"
    ].apply(lambda x: index.loc[x])

    model.network.edge.df.loc[:, ["to_node_id"]] = model.network.edge.df[
        "to_node_id"
    ].apply(lambda x: index.loc[x])

    # renumber basins
    model.basin.static.df.loc[:, ["node_id"]] = model.basin.static.df["node_id"].apply(
        lambda x: index.loc[x]
    )

    model.basin.profile.df.loc[:, ["node_id"]] = model.basin.profile.df[
        "node_id"
    ].apply(lambda x: index.loc[x])

    # renumber resistances
    model.linear_resistance.static.df.loc[
        :, ["node_id"]
    ] = model.linear_resistance.static.df["node_id"].apply(lambda x: index.loc[x])

    # renumber flow_boundaries
    model.flow_boundary.static.df.loc[:, ["node_id"]] = model.flow_boundary.static.df[
        "node_id"
    ].apply(lambda x: index.loc[x])

    return model

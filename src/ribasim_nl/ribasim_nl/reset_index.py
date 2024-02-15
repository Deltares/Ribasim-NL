# %%
import pandas as pd
from ribasim import Model

from ribasim_nl.model import TABLES, get_table


def reset_index(model: Model, node_start=1):
    # only reset if we have to
    fid_min = model.network.node.df.index.min()
    fid_max = model.network.node.df.index.max()
    expected_length = fid_max - fid_min + 1
    if not (
        (node_start == fid_min) and (expected_length == len(model.network.node.df))
    ):
        # make sure column node_id == index
        node_ids = model.network.node.df.index
        model.network.node.df.loc[:, "fid"] = node_ids

        # create a new index for re-indexing all tables
        index = pd.Series(
            data=[i + node_start for i in range(len(node_ids))], index=node_ids
        )

        # re-index node_id and fid
        model.network.node.df.index = model.network.node.df["fid"].apply(
            lambda x: index.loc[x]
        )
        model.network.node.df.index.name = "fid"
        model.network.node.df.drop(columns=["fid"], inplace=True)

        # renumber edges
        model.network.edge.df.loc[:, ["from_node_id"]] = model.network.edge.df[
            "from_node_id"
        ].apply(lambda x: index.loc[x])

        model.network.edge.df.loc[:, ["to_node_id"]] = model.network.edge.df[
            "to_node_id"
        ].apply(lambda x: index.loc[x])

        # renumber tables
        for table in TABLES:
            df = get_table(model, table)
            if df is not None:
                df.loc[:, ["node_id"]] = df["node_id"].apply(lambda x: index.loc[x])

    return model


# %%

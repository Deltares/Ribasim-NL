# %%
import pandas as pd
from ribasim import Model

from ribasim_nl.case_conversions import pascal_to_snake_case


def reset_index(model: Model, node_start=1):
    # only reset if we have to
    node_id_min = model.node_table().df.node_id.min()
    node_id_max = model.node_table().df.node_id.max()
    expected_length = node_id_max - node_id_min + 1
    if not ((node_start == node_id_min) and (expected_length == len(model.node_table().df))):
        # make sure column node_id == index
        node_ids = model.node_table().df.node_id

        # create a new index for re-indexing all tables
        index = pd.Series(data=[i + node_start for i in range(len(node_ids))], index=node_ids).astype("int32")

        # # re-index node_id and fid
        # model.network.node.df.index = model.network.node.df["fid"].apply(
        #     lambda x: index.loc[x]
        # )
        # model.network.node.df.index.name = "fid"
        # model.network.node.df.drop(columns=["fid"], inplace=True)
        # model.network.node.df.loc[:, "node_id"] = model.network.node.df.index

        # renumber edges
        model.edge.df.loc[:, ["from_node_id"]] = model.edge.df["from_node_id"].apply(lambda x: index.loc[x])

        model.edge.df.loc[:, ["to_node_id"]] = model.edge.df["to_node_id"].apply(lambda x: index.loc[x])

        # renumber tables
        for node_type in model.node_table().df.node_type.unique():
            ribasim_node = getattr(model, pascal_to_snake_case(node_type))
            for attr in ribasim_node.model_fields.keys():
                table = getattr(ribasim_node, attr)
                if table.df is not None:
                    table.df.loc[:, "node_id"] = table.df["node_id"].apply(lambda x: index.loc[x])

    return model


# %%

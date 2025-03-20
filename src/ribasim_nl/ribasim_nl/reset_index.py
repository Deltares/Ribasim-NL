# %%
import pandas as pd

from ribasim_nl import Model
from ribasim_nl.case_conversions import pascal_to_snake_case


def reindex_nodes(model: Model, node_index: pd.Series, original_index_postfix: str | None = "waterbeheerder"):
    """Reindex all model-nodes to a new node_index series"""
    # re-number from_node_id and to_node_id
    model.edge.df.loc[:, ["from_node_id"]] = model.edge.df["from_node_id"].apply(lambda x: node_index[x])
    model.edge.df.loc[:, ["to_node_id"]] = model.edge.df["to_node_id"].apply(lambda x: node_index[x])

    # renumber all node-tables (node, static, area, ...)
    for node_type in model.node_table().df.node_type.unique():
        ribasim_node = getattr(model, pascal_to_snake_case(node_type))
        for attr in ribasim_node.model_fields.keys():
            table = getattr(ribasim_node, attr)
            try:
                if table.df is not None:
                    if "node_id" in table.df.columns:
                        table.df.loc[:, "node_id"] = table.df["node_id"].apply(lambda x: node_index[x])
                        table.df.index += 1
                    if "listen_node_id" in table.df.columns:
                        table.df.loc[:, "listen_node_id"] = table.df["listen_node_id"].apply(lambda x: node_index[x])
                    if table.df.index.name == "node_id":
                        if original_index_postfix is not None:
                            table.df.loc[:, f"meta_node_id_{original_index_postfix}"] = table.df.index.astype("int32")
                        table.df.index = pd.Index(
                            table.df.reset_index("node_id")["node_id"].apply(lambda x: node_index[x]).to_list(),
                            name="node_id",
                        )
            except KeyError as e:
                raise KeyError(f"node_id {e} in table {node_type} / {attr} not a node_id node-table")
    return model


def prefix_index(
    model: Model, prefix_id: int, max_digits: int = 4, original_index_postfix: str | None = "waterbeheerder"
):
    """Reindex node-tables and edges with a prefix and a max number of digits

    Args:
        model (Model): ribasim.Model to be reindexed
        prefix_id (int): integer used as prefix
        max_digits (int, optional): max digits in original node_ids. Defaults to 4.
        original_index_postfix (str | None, optional): if provided the original index will be stored in a meta-column. Defaults to "waterbeheerder".

    Returns
    -------
        ribasim.Model: reindexed model
    """
    node_ids = model.node_table().df.index

    # create a node_index and reindex nodes
    node_index = pd.Series(
        data=[int(f"{prefix_id}{node_id:0>{max_digits}}") for node_id in node_ids], index=node_ids
    ).astype("int32")
    model = reindex_nodes(model=model, node_index=node_index, original_index_postfix=original_index_postfix)

    # create an edge_index and reindex edges
    edge_ids = model.edge.df.index
    model.edge.df.index = pd.Index(
        [int(f"{prefix_id}{edge_id:0>{max_digits}}") for edge_id in edge_ids], name="edge_id"
    )

    # keep original index if
    if original_index_postfix is not None:
        model.edge.df.loc[:, f"meta_edge_id_{original_index_postfix}"] = edge_ids.astype("int32")

    return model


def reset_index(model: Model, node_start=1, edge_start=1, original_index_postfix: str | None = "waterbeheerder"):
    """Reset a model index to a given node_start and edge_start number. Will result in sub-sequent node_ids and edge_ids from node_start and edge_start.

    Args:
        model (Model): ribasim.Model to be reindexed
        node_start (int, optional): start node_id. Defaults to 1.
        edge_start (int, optional): start edge_id. Defaults to 1.
        original_index_postfix (str | None, optional): if provided the original index will be stored in a meta-column. Defaults to "waterbeheerder".

    Returns
    -------
        ribasim.Model: reindexed model
    """
    # only reset nodes if we have to
    node_ids = model.node_table().df.index
    node_id_min = node_ids.min()
    node_id_max = node_ids.max()
    expected_length = node_id_max - node_id_min + 1
    if not ((node_start == node_id_min) and (expected_length == len(model.node_table().df))):
        # create a re-index for nodes
        node_index = pd.Series(data=[i + node_start for i in range(len(node_ids))], index=node_ids).astype("int32")
        model = reindex_nodes(model=model, node_index=node_index, original_index_postfix=original_index_postfix)

        # only reset nodes if we have to
        edge_ids = model.edge.df.index
        edge_id_min = edge_ids.min()
        edge_id_max = edge_ids.max()
        expected_length = edge_id_max - edge_id_min + 1

        if not ((edge_start == edge_id_min) and (expected_length == len(model.edge.df))):
            # create a re-index for edges
            model.edge.df.index = pd.Index([i + node_start for i in range(len(edge_ids))], name="edge_id")

        # keep original index if
        if original_index_postfix is not None:
            model.edge.df.loc[:, f"meta_edge_id_{original_index_postfix}"] = edge_ids
    return model

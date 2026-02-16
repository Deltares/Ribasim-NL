import pandas as pd

from ribasim_nl.model import Model


def check_basin_level(
    node_id: int, model: Model, node_df: pd.Series, check_column: str = "meta_code_waterbeheerder"
) -> bool:
    """Check if the basin_level should be checked. Basin is upstream of a node that has a non NoData check_column value

    Args:
        node_id (int): basin node_id
        model (Model): Ribasim-nl Model
        node_df (pd.Series): DataFrame with model-nodes
        check_column (str): column to check not NoData

    Returns
    -------
        bool: _description_
    """
    ds_node_id = model.downstream_node_id(node_id)

    if isinstance(ds_node_id, pd.Series):
        ds_node_ids = ds_node_id.to_numpy()
    else:
        ds_node_ids = [ds_node_id]

    return any((not pd.isna(node_df.at[i, check_column])) for i in ds_node_ids)


def add_check_basin_level(model: Model, check_column: str = "meta_code_waterbeheerder"):
    """Add a column "meta_check_basin_level" to the model basin-table

    Args:
        model (Model): Ribasim-nl Model
        check_column (str): column to check not NoData
    """
    # read the node_table first as we suspect that is faster than doing it at every iter
    node_df = model.node.df

    # apply check_basin_level function to all basin node_ids
    model.basin.node.df.loc[:, "meta_check_basin_level"] = [
        check_basin_level(i, model=model, node_df=node_df, check_column=check_column) for i in model.basin.node.df.index
    ]

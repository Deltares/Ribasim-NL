import pandas as pd

from ribasim_nl.model import Model


def check_basin_level(
    node_id: int, model: Model, node_df: pd.DataFrame, check_column: str = "meta_code_waterbeheerder"
) -> bool:
    """Check if the basin_level should be checked. Basin is upstream of a node that has a non NoData check_column value

    Args:
        node_id (int): basin node_id
        model (Model): Ribasim-nl Model
        node_df (pd.DataFrame): DataFrame with model-nodes
        check_column (str): column to check not NoData

    Returns
    -------
        bool: _description_
    """
    ds_node_id = model.downstream_node_id(node_id)

    ds_node_ids = ds_node_id.to_list() if isinstance(ds_node_id, pd.Series) else [ds_node_id]

    return any((not pd.isna(node_df.at[i, check_column])) for i in ds_node_ids)


def add_check_basin_level(model: Model, check_column: str = "meta_code_waterbeheerder") -> None:
    """Add a column "meta_check_basin_level" to the model basin-table

    Args:
        model (Model): Ribasim-nl Model
        check_column (str): column to check not NoData
    """
    # read the node_table first as we suspect that is faster than doing it at every iter
    assert model.node.df is not None
    node_df = model.node.df

    # apply check_basin_level function to all basin node_ids
    assert model.basin.node is not None
    assert model.basin.node.df is not None
    basin_ids = model.basin.node.df.index
    model.node.df["meta_check_basin_level"] = pd.Series(
        [check_basin_level(i, model=model, node_df=node_df, check_column=check_column) for i in basin_ids],
        index=basin_ids,
        dtype=bool,
    )

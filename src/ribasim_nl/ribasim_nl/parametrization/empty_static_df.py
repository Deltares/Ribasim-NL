import pandas as pd
from ribasim import nodes

from ribasim_nl import Model
from ribasim_nl.case_conversions import pascal_to_snake_case


def empty_static_df(model: Model, node_type: str, meta_columns: list[str] = []) -> pd.DataFrame:
    """Return an empty DataFrame for a Static-table.

    This allows the user to generate a DataFrame for a Static table with correct dtypes yet empty fields when not allowed by schema

    Args:
        model (Model): ribasim Model
        node_type (str): node_type to get DataFrame for
        meta_columns (list, optional): meta-columns from Node-table to append to DataFrame. Defaults to [].

    Returns
    -------
        pd.DataFrame: DataFrame for static table
    """
    # read node from model
    node = getattr(model, pascal_to_snake_case(node_type))
    meta_columns = [i for i in meta_columns if i in node.node.df.columns]

    # get correct dtypes
    dtypes = getattr(nodes, pascal_to_snake_case(node_type)).Static(**{k: [] for k in node.static.columns()}).df.dtypes

    # populate dataframe
    df = node.node.df.reset_index()[["node_id"] + meta_columns]

    for column, dtype in dtypes.to_dict().items():
        if column in df.columns:
            df.loc[:, column] = df[column].astype(dtype)
        else:
            df[column] = pd.Series(dtype=dtype)

    return df[dtypes.index.to_list() + meta_columns]

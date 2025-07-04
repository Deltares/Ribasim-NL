from typing import Literal

import pandas as pd
from ribasim import nodes

from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.model import Model


def empty_table_df(
    model: Model,
    node_type: str,
    table_type: Literal["Static", "Profile"],
    meta_columns: list[str] = [],
    fill_value: float | None = None,
) -> pd.DataFrame:
    """Return an empty DataFrame for a Static-table.

    This allows the user to generate a DataFrame for a Static table with correct dtypes yet empty fields when not allowed by schema

    Args:
        model (Model): ribasim Model
        node_type (str): node_type to get DataFrame for
        meta_columns (list, optional): meta-columns from Node-table to append to DataFrame. Defaults to [].
        fill_value (float, optional): float value to fill all numeric non-meta and not node_id columns with

    Returns
    -------
        pd.DataFrame: DataFrame for static table
    """
    # read node from model
    node = getattr(model, pascal_to_snake_case(node_type))
    meta_columns = [i for i in meta_columns if i in node.node.df.columns]

    # get correct dtypes
    dtypes = getattr(getattr(nodes, pascal_to_snake_case(node_type)), table_type)(
        **{k: [] for k in getattr(node, pascal_to_snake_case(table_type)).columns()}
    ).df.dtypes

    # populate dataframe
    df = node.node.df.reset_index()[["node_id"] + meta_columns]

    for column, dtype in dtypes.to_dict().items():
        if column in df.columns:
            df.loc[:, column] = df[column].astype(dtype)
        else:
            df[column] = pd.Series(dtype=dtype)

        if (fill_value is not None) and pd.api.types.is_numeric_dtype(dtype) and (column != "node_id"):
            df.loc[:, column] = fill_value

    return df[dtypes.index.to_list() + meta_columns]

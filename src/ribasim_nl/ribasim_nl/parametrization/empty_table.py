# %%
from typing import Literal

import pandas as pd
from ribasim import nodes

from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.model import Model


def get_dtypes(model: Model, node_type: str, table_type: Literal["Static", "Profile", "Time"]):
    """Get the datetypes of a a Ribasim Table

    Parameters
    ----------
    model : Model
        Ribasim model instance from which the schema and node information
        are derived.
    node_type : str
        Node type for which the table should be created.
    table_type : Literal["Static", "Profile", "Time"]
        Type of table to generate.

    Returns
    -------
    pd.Series: dtype per column
    """
    node = getattr(model, pascal_to_snake_case(node_type))
    return getattr(getattr(nodes, pascal_to_snake_case(node_type)), table_type)(
        **{k: [] for k in getattr(node, pascal_to_snake_case(table_type)).columns()}
    ).df.dtypes


def empty_table_df(
    model: Model,
    node_type: str,
    table_type: Literal["Static", "Profile", "Time"],
    meta_columns: list[str] = [],
    fill_value: float | None = None,
) -> pd.DataFrame:
    """
    Generate an empty DataFrame for a Ribasim node table with the correct schema and dtypes.

    This utility creates a DataFrame that conforms to the schema of a Ribasim
    node table (e.g. Static, Profile, or Time), while leaving fields empty when
    required by the schema. Optionally, metadata columns from the Node table can
    be included and numeric fields can be initialized with a default value.

    Parameters
    ----------
    model : Model
        Ribasim model instance from which the schema and node information
        are derived.
    node_type : str
        Node type for which the table should be created.
    table_type : Literal["Static", "Profile", "Time"]
        Type of table to generate.
    meta_columns : list, optional
        Metadata columns from the Node table to append to the DataFrame.
        Defaults to an empty list.
    fill_value : float, optional
        Value used to fill all numeric columns except `node_id` and metadata
        columns. If None, numeric fields remain empty.
    add_nodes : bool, optional
        If True, the returned DataFrame will contain rows for all nodes of the
        specified type. If False, the DataFrame will be empty. Defaults to False.

    Returns
    -------
    pd.DataFrame
        DataFrame with the correct schema and dtypes for the specified
        node table.
    """
    # read node from model
    node = getattr(model, pascal_to_snake_case(node_type))
    meta_columns = [i for i in meta_columns if i in node.node.df.columns]

    # get correct dtypes
    dtypes = get_dtypes(model, node_type, table_type)

    # populate dataframe
    df = node.node.df.reset_index()[["node_id", *meta_columns]]

    for column, dtype in dtypes.to_dict().items():
        if column in df.columns:
            df.loc[:, column] = df[column].astype(dtype)
        else:
            df[column] = pd.Series(dtype=dtype)

        if (fill_value is not None) and pd.api.types.is_numeric_dtype(dtype) and (column != "node_id"):
            df.loc[:, column] = fill_value

    return df[dtypes.index.to_list() + meta_columns]

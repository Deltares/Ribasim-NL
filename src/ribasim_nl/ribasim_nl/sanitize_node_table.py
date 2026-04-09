# function for sanitizing the node_table of Sweco build models

import pandas as pd

from ribasim_nl.model import Model


def sanitize_node_table(
    model: Model,
    meta_columns: list[str] = [],
    copy_map: list[dict[str, object]] = [],
    names: "pd.Series[str] | None" = None,
) -> None:
    """Clean all node-tables to their expected columns + (optionally) meta_columns."""
    assert model.basin.node is not None
    node_columns = model.basin.node.columns() + meta_columns

    # update values per node_type directly on model.node.df
    assert model.node.df is not None
    for node_type in model.node.df.node_type.unique():
        mask = model.node.df["node_type"] == node_type

        # copy data from one column to the other (or overwrite with default)
        copy_columns: dict[str, str] | None = next(
            (i["columns"] for i in copy_map if node_type in i["node_types"]),
            None,
        )
        if copy_columns is not None:
            for from_col, to_col in copy_columns.items():
                if to_col not in node_columns:  # if not in columns, we interpret to_col as new value for from_col
                    model.node.df.loc[mask, from_col] = to_col
                else:
                    model.node.df.loc[mask, to_col] = model.node.df.loc[mask, from_col]

        # add name from code-column
        if (
            ("meta_code_waterbeheerder" in model.node.df.columns)
            & (names is not None)
            & ((copy_columns is None) or ("name" not in copy_columns.values()))
        ):
            assert names is not None
            codes = model.node.df.loc[mask, "meta_code_waterbeheerder"]
            # deduplicate names index to avoid InvalidIndexError in pandas 3
            unique_names = names[~names.index.duplicated(keep="first")]
            # look up each code in the names Series, drop codes with no match or NaN values
            resolved = codes.map(unique_names).dropna()
            # filter out non-scalar results (duplicate codes returning a Series)
            resolved = resolved[resolved.apply(pd.api.types.is_scalar)].astype(str)
            # fill unmatched codes with ""
            model.node.df.loc[mask, "name"] = resolved.reindex(codes.index, fill_value="")

    # drop all columns not in node_columns
    columns = [col for col in model.node.df.columns if (col in node_columns) or (col == "node_type")]
    model.node.df = model.node.df[columns]

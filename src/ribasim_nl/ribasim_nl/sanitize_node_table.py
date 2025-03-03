# function for sanitizing the node_table of Sweco build models

from pandas import Series

from ribasim_nl import Model
from ribasim_nl.case_conversions import pascal_to_snake_case


def sanitize_node_table(
    model: Model,
    meta_columns: list[str] = [],
    copy_map: dict = {},
    names: Series | None = None,
):
    """Clean all node-tables to their expected columns + (optionally) meta_columns."""
    node_columns = model.basin.node.columns() + meta_columns

    # remove names and clean columns
    for node_type in model.node_table().df.node_type.unique():
        table = getattr(model, pascal_to_snake_case(node_type))

        # copy data from one column to the other (or overwrite with default)
        copy_columns = next((i["columns"] for i in copy_map if node_type in i["node_types"]), None)
        if copy_columns is not None:
            for from_col, to_col in copy_columns.items():
                if to_col not in node_columns:  # if not in columns, we interpret to_col as new value for from_col
                    table.node.df.loc[:, from_col] = to_col
                else:
                    table.node.df.loc[:, to_col] = table.node.df.loc[:, from_col]

        # add name from code-column
        if (
            ("meta_code_waterbeheerder" in table.node.df.columns)
            & (names is not None)
            & ("name" not in copy_columns.values())
        ):
            table.node.df.loc[:, "name"] = table.node.df["meta_code_waterbeheerder"].apply(
                lambda x: names[x] if x in names.index.to_numpy() else ""
            )

        # drop all columns not in node_columns
        columns = [col for col in table.node.df.columns if col in node_columns]
        table.node.df = table.node.df[columns]

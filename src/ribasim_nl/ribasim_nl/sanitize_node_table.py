# function for sanitizing the node_table of Sweco build models

from pandas import Series

from ribasim_nl import Model
from ribasim_nl.case_conversions import pascal_to_snake_case


def stanitize_node_table(
    model: Model,
    meta_columns: list[str] = [],
    copy_columns: dict[str] = {},
    copy_in_tables: list[str] = [],
    names: Series | None = None,
):
    """Clean all node-tables to their expected columns + (optionally) meta_columns."""
    node_columns = model.basin.node.columns() + meta_columns

    # remove names and clean columns
    for node_type in model.node_table().df.node_type.unique():
        table = getattr(model, pascal_to_snake_case(node_type))

        if node_type in copy_in_tables:
            for from_col, to_col in copy_columns.items():
                table.node.df.loc[:, to_col] = table.node.df.loc[:, from_col]

        if ("meta_code_waterbeheerder" in table.node.df.columns) & (names is not None):
            table.node.df.loc[:, "name"] = table.node.df["meta_code_waterbeheerder"].apply(
                lambda x: names[x] if x in names.index.to_numpy() else ""
            )
        elif not ("name" in copy_columns.values()) & (node_type in copy_in_tables):
            table.node.df.name = ""
        columns = [col for col in table.node.df.columns if col in node_columns]
        table.node.df = table.node.df[columns]

from pathlib import Path
from typing import Literal

import pandas as pd

from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.model import Model


def populate_function_column(model: Model, node_type: Literal["Pump", "Outlet"], static_data_xlsx: Path) -> Model:
    """Add function-column 'meta_function' to ribasim.Model node table using an Excel spreadsheet with data.

    Args:
        model (Model): Ribasim model
        node_type (Literal["Pump", "Outlet"]): Either "Pump" or "Outlet".
        static_data_xlsx (Path): Excel spreadsheet with node_types

    Returns
    -------
        Model: updated model
    """
    static_data_sheets = pd.ExcelFile(static_data_xlsx).sheet_names
    # update function - column
    if node_type in static_data_sheets:
        table = getattr(model, pascal_to_snake_case(node_type)).node
        # add node_id to static_data
        static_data = pd.read_excel(static_data_xlsx, sheet_name=node_type).set_index("code")
        static_data = static_data[static_data.index.isin(table.df["meta_code_waterbeheerder"])]
        static_data.loc[:, "node_id"] = (
            table.df.reset_index().set_index("meta_code_waterbeheerder").loc[static_data.index].node_id
        )
        static_data.set_index("node_id", inplace=True)

        # add function to node via categorie
        defaults_df = pd.read_excel(static_data_xlsx, sheet_name="defaults", index_col=0)
        for row in defaults_df.itertuples():
            category = row.Index
            function = row.function
            static_data.loc[static_data["categorie"] == category, "meta_function"] = function

        table.df.loc[static_data.index, "meta_function"] = static_data["meta_function"]

    return model

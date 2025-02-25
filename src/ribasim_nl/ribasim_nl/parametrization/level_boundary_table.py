# %%
from pathlib import Path

import pandas as pd

from ribasim_nl.model import Model
from ribasim_nl.parametrization.conversions import round_to_precision
from ribasim_nl.parametrization.empty_table import empty_table_df


def update_level_boundary_static(
    model: Model,
    static_data_xlsx: Path | None = None,
    code_column: str = "meta_code_waterbeheerder",
):
    """Update LevelBoundary table

    Args:
        model (Model): Ribasim model
        static_data_xlsx (Path): Excel spreadsheet with node_types
        code_column: (str) column in node_table corresponding with code column in static_data_xlsx

    Returns
    -------
        pd.DataFrame DataFrame in format of static table (ignoring NoData)
    """
    # start with an empty static_df with the correct columns and meta_code_waterbeheerder
    static_df = empty_table_df(model=model, node_type="LevelBoundary", table_type="Static", meta_columns=[code_column])

    # update data with static data in Excel
    if static_data_xlsx is not None:
        node_type = "LevelBoundary"
        static_data_sheets = pd.ExcelFile(static_data_xlsx).sheet_names
        if node_type in static_data_sheets:
            static_data = pd.read_excel(static_data_xlsx, sheet_name=node_type).set_index("code")

            # in case there is more defined in static_data than in the model
            static_data = static_data[static_data.index.isin(static_df[code_column])]

            # update-function from static_data in Excel
            static_data.loc[:, "node_id"] = static_df.set_index(code_column).loc[static_data.index].node_id
            static_data.columns = [i if i in static_df.columns else f"meta_{i}" for i in static_data.columns]
            static_data = static_data.set_index("node_id")

            static_df.set_index("node_id", inplace=True)
            for col in static_data.columns:
                series = static_data[static_data[col].notna()][col]
                if col == "flow_rate":
                    series = series.apply(round_to_precision, args=(0.01,))
                static_df.loc[series.index.to_numpy(), col] = series
            static_df.reset_index(inplace=True)

    # make sure inlets/outlets work
    mask = static_df.level.isna()
    if mask.any():
        for row in static_df[mask].itertuples():
            node_id = row.node_id
            us_node_id = model.upstream_node_id(node_id)
            if us_node_id is not None:  # its an outlet
                if isinstance(us_node_id, pd.Series):
                    us_basins = {model.upstream_node_id(i) for i in us_node_id}
                else:
                    us_basins = [model.upstream_node_id(us_node_id)]
                us_basins = [i for i in us_basins if i is not None]
                us_basins = [i for i in us_basins if model.get_node_type(i) == "Basin"]
                if len(us_basins) == 0:
                    raise ValueError(f"node_id {node_id} does not have an upstream basin")
                level = model.basin.area.df.set_index("node_id").loc[us_basins]["meta_streefpeil"].min()
            else:  # its an inlet
                ds_node_id = model.downstream_node_id(node_id)
                if isinstance(ds_node_id, pd.Series):
                    ds_basins = list({model.downstream_node_id(i) for i in ds_node_id})
                else:
                    ds_basins = [model.downstream_node_id(ds_node_id)]
                ds_basins = [i for i in ds_basins if model.get_node_type(i) == "Basin"]
                if len(ds_basins) == 0:
                    raise ValueError(f"node_id {node_id} does not have a downstream basin")
                level = model.basin.area.df.set_index("node_id").loc[ds_basins]["meta_streefpeil"].min()
            static_df.loc[row.Index, "level"] = level

    model.level_boundary.static.df = static_df

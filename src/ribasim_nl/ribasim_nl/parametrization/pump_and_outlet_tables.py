from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.model import Model
from ribasim_nl.parametrization.conversions import round_to_significant_digits
from ribasim_nl.parametrization.empty_table import empty_table_df
from ribasim_nl.parametrization.target_level import downstream_target_levels, upstream_target_levels


def create_static_df(
    model: Model,
    node_type: Literal["Pump", "Outlet"],
    static_data_xlsx: Path | None = None,
    code_column: str = "meta_code_waterbeheerder",
) -> pd.DataFrame:
    """Create static df and update from Excel spreadsheet.

    Args:
        model (Model): Ribasim model
        node_type (Literal["Pump", "Outlet"]): Either "Pump" or "Outlet".
        static_data_xlsx (Path): Excel spreadsheet with node_types

    Returns
    -------
        pd.DataFrame DataFrame in format of static table (ignoring NoData)
    """
    # start with an empty static_df with the correct columns and meta_code_waterbeheerder
    static_df = empty_table_df(model=model, node_type=node_type, table_type="Static", meta_columns=[code_column])

    # update data with static data in Excel
    if static_data_xlsx is not None:
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
                    series = series.apply(round_to_significant_digits)
                static_df.loc[series.index.to_numpy(), col] = series
            static_df.reset_index(inplace=True)

    return static_df


def defaults_to_static_df(model: Model, static_df: pd.DataFrame, static_data_xlsx: Path) -> pd.DataFrame:
    """Fill nodata in static table with defaults.

    Args:
    model (Model): Ribasim model
        static_df (pd.DataFrame): DataFrame in format of static table (with nodata)
        static_data_xlsx (Path): Excel containing defaults table

    Returns
    -------
        pd.DataFrame: DataFrame in format of static table
    """
    # update-function from defaults
    defaults_df = pd.read_excel(static_data_xlsx, sheet_name="defaults", index_col=0)
    for row in defaults_df.itertuples():
        category = row.Index
        mask = static_df["meta_categorie"] == category

        # flow_rate; all in sub_mask == True needs to be filled
        sub_mask = static_df[mask]["flow_rate"].isna()
        indices = sub_mask.index.to_numpy()
        if sub_mask.any():
            # fill nan with flow_rate_mm_day if provided

            if not pd.isna(row.flow_rate_mm_per_day):
                unit_conversion = row.flow_rate_mm_per_day / 1000 / 86400
                if row.function == "outlet":
                    flow_rate = np.array(
                        [
                            round_to_significant_digits(
                                model.get_upstream_basins(node_id, stop_at_inlet=True).area.sum() * unit_conversion
                            )
                            for node_id in static_df[mask][sub_mask].node_id
                        ],
                        dtype=float,
                    )
                elif row.function == "inlet":
                    flow_rate = np.array(
                        [
                            round_to_significant_digits(
                                model.get_downstream_basins(node_id, stop_at_outlet=True).area.sum() * unit_conversion
                            )
                            for node_id in static_df[mask][sub_mask].node_id
                        ],
                        dtype=float,
                    )
                static_df.loc[indices, "flow_rate"] = flow_rate
            elif not pd.isna(row.flow_rate):
                static_df.loc[indices, "flow_rate"] = row.flow_rate
            else:
                raise ValueError(f"Can't set flow_rate for node_ids {static_df.loc[indices, 'node_id'].to_numpy()}")

        # min_upstream_level
        sub_mask = static_df[mask]["min_upstream_level"].isna()
        if sub_mask.any():
            # calculate upstream levels
            upstream_level_offset = row.upstream_level_offset
            upstream_levels = upstream_target_levels(model=model, node_ids=static_df[mask][sub_mask].node_id)

            # assign upstream levels to static_df
            static_df.set_index("node_id", inplace=True)
            static_df.loc[upstream_levels.index, "min_upstream_level"] = (
                upstream_levels - upstream_level_offset
            ).round(2)
            static_df.reset_index(inplace=True)
        sub_mask = static_df[mask]["max_downstream_level"].isna()
        if sub_mask.any():
            # calculate downstream_levels
            downstream_level_offset = row.downstream_level_offset
            downstream_levels = downstream_target_levels(model=model, node_ids=static_df[mask][sub_mask].node_id)

            # assign upstream levels to static_df
            static_df.set_index("node_id", inplace=True)
            static_df.loc[downstream_levels.index, "max_downstream_level"] = (
                downstream_levels + downstream_level_offset
            ).round(2)
            static_df.reset_index(inplace=True)

    return static_df


def update_pump_outlet_static(
    model: Model,
    node_type: Literal["Pump", "Outlet"],
    static_data_xlsx: Path | None = None,
    code_column: str = "meta_code_waterbeheerder",
):
    # init static_table with static_data_xlsx
    static_df = create_static_df(
        model=model,
        node_type=node_type,
        static_data_xlsx=static_data_xlsx,
        code_column=code_column,
    )
    # fill with defaults
    static_df = defaults_to_static_df(model=model, static_df=static_df, static_data_xlsx=static_data_xlsx)
    # sanitize df and update model
    static_df.drop(columns=["meta_code_waterbeheerder"], inplace=True)
    getattr(model, pascal_to_snake_case(node_type)).static.df = static_df

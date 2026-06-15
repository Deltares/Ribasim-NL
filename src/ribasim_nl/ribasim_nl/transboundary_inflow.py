import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from ribasim_nl.model import Model

logger = logging.getLogger(__name__)


def import_transboundary_inflow(
    transboundary_data_path: Path,
    start_time: pd.Timestamp | datetime,
    stop_time: pd.Timestamp | datetime,
    model: Model,
) -> dict[str, pd.DataFrame]:
    """Import transboundary inflow data from an Excel file.

    Filters to the requested date range, computes daily averages, interpolates
    missing values, and couples locations to flow boundary nodes in the model.

    Parameters
    ----------
    transboundary_data_path : Path
        Path to the Excel file with transboundary inflow data.
    start_time : pd.Timestamp
        Model start date.
    stop_time : pd.Timestamp
        Model end date.
    model : Model
        Model with ``flow_boundary.node.df`` for node coupling.

    Returns
    -------
    dict[str, pd.DataFrame]
        Per-location DataFrames with columns 'time', 'flow_rate' and 'node_id'.
    """
    flow_boundary_df = model.flow_boundary.node.df.reset_index(drop=False)  # pyrefly: ignore[missing-attribute]
    node_ids_by_name = flow_boundary_df.set_index("name")["node_id"]
    relevant_locations = set(node_ids_by_name.index)

    xls = pd.ExcelFile(transboundary_data_path)
    df_raw_data = []

    for sheet in xls.sheet_names:
        df = pd.read_excel(
            xls,
            sheet_name=sheet,
            usecols=lambda col: col == "Datum" or col in relevant_locations,
        )
        if "Datum" in df.columns:
            value_columns = [col for col in df.columns if col != "Datum"]
            if not value_columns:
                continue

            logger.info(f"Importeer de data voor: {', '.join(value_columns)}")

            if not pd.api.types.is_datetime64_any_dtype(df["Datum"]):
                raise ValueError(f"Expected datetime64 'Datum' column from Excel, got dtype: {df['Datum'].dtype}")
            df = df.dropna(subset=["Datum"]).set_index("Datum")
            if not df.index.is_monotonic_increasing:
                raise ValueError(f"Sheet '{sheet}': Datum column is not sorted by time")
            df = df.loc[(df.index >= start_time) & (df.index <= stop_time), value_columns]

            if df.empty:
                continue

            df_numeric = df.apply(pd.to_numeric, errors="coerce")
            df_daily = df_numeric.resample("D").mean()
            if not df_daily.empty:
                df_raw_data.append(df_daily)
        else:
            logger.warning(f"Sheet without Datum column skipped: {sheet}")

    if not df_raw_data:
        logger.warning("No transboundary inflow data found for the selected period and model boundaries.")
        return {}

    # Combine data
    df_combined = pd.concat(df_raw_data, axis=0)
    df_combined = df_combined.groupby(level=0).mean(numeric_only=True).sort_index()
    df_inflow = df_combined.copy()
    df_inflow.index.name = "time"

    # Interpolate missing values within the time series
    df_inflow = df_inflow.interpolate(method="time", limit_area="inside")

    # Fill NaN's at the edges with the mean flow rate at that location
    mean_flow = df_inflow.mean()
    df_inflow = df_inflow.fillna(mean_flow)

    # If no data exists in the modelled period: set flow to 0
    cols_without_measurements = mean_flow.index[mean_flow.isna()]
    for col in cols_without_measurements:
        logger.warning(f"Location '{col}' has no measurements; filling NaNs with 0.")
    if len(cols_without_measurements) > 0:
        df_inflow.loc[:, cols_without_measurements] = df_inflow.loc[:, cols_without_measurements].fillna(0)

    # Negative inflows are not allowed; clip them to zero and warn per location.
    cols_with_negative_flow = df_inflow.columns[(df_inflow < 0).any()]
    for col in cols_with_negative_flow:
        n_negative = int((df_inflow[col] < 0).sum())
        logger.warning(f"Location '{col}' contains {n_negative} negative flow_rate value(s); setting them to 0.")
    df_inflow = df_inflow.clip(lower=0)

    # Check for remaining NaN values
    assert not df_inflow.isna().any().any(), "There are NaN values remaining!"

    # Convert to dictionary
    dict_flow = {
        loc: pd.DataFrame(
            {
                "time": df_inflow.index,
                "flow_rate": df_inflow[loc].to_numpy(),
                "node_id": node_ids_by_name.at[loc],
            }
        )
        for loc in df_inflow.columns
    }

    logger.info(f"Transboundary inflow dictionary created: {dict_flow}")
    return dict_flow


def add_transboundary_inflow(model: Model, dict_flow: dict[str, pd.DataFrame]) -> None:
    """Add transboundary inflow time series to the model as flow boundary time data.

    Replaces existing static and time entries for the affected node_ids.
    Modifies the model in-place.

    Parameters
    ----------
    model : Model
        The Ribasim model to update.
    dict_flow : dict[str, pd.DataFrame]
        Per-location DataFrames with columns 'time', 'flow_rate' and 'node_id'.
    """
    if not dict_flow:
        logger.info("No transboundary inflow data matched for this model; skipping.")
        return

    flowboundaries = model.flow_boundary.node.df.name  # pyrefly: ignore[missing-attribute]
    df_flowboundaries_time = pd.concat(dict_flow.values(), axis=0)

    included_node_ids = df_flowboundaries_time.node_id.unique()
    included_names = flowboundaries[flowboundaries.index.isin(included_node_ids)].tolist()
    logger.info(f"Flowboundaries included in data: {', '.join(included_names)}")

    # remove the static flow rates when we have timeseries
    if model.flow_boundary.static.df is not None:
        model.flow_boundary.static.df = model.flow_boundary.static.df[
            ~model.flow_boundary.static.df["node_id"].isin(included_node_ids)
        ]
    # remove possible existing timeseries with the same node ID
    if model.flow_boundary.time.df is not None:
        model.flow_boundary.time.df = model.flow_boundary.time.df[
            ~model.flow_boundary.time.df["node_id"].isin(included_node_ids)
        ]
    # add the rows of df_flowboundaries_time to the existing df
    if model.flow_boundary.time.df is None:
        model.flow_boundary.time.df = df_flowboundaries_time  # pyrefly: ignore[bad-assignment]
    else:
        model.flow_boundary.time.df = pd.concat([model.flow_boundary.time.df, df_flowboundaries_time])  # pyrefly: ignore[bad-assignment]

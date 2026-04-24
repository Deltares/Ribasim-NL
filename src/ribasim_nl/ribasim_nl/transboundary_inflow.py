import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from ribasim_nl.model import Model

logger = logging.getLogger(__name__)


def fix_date_string(date_str: str | pd.Timestamp) -> pd.Timestamp:
    """Fix dates containing '24:00' and parse day-first formatted date strings.

    Parameters
    ----------
    date_str : str or pd.Timestamp
        Date as string or timestamp, e.g. '23-06-2017 24:00'.

    Returns
    -------
    pd.Timestamp or pd.NaT
        A valid datetime value. Returns NaT if parsing fails.
    """
    try:
        if isinstance(date_str, str) and "24:00" in date_str:
            # Extract the date part
            base_date = datetime.strptime(date_str.split(" ")[0], "%d-%m-%Y")
            # Add one day, time will be 00:00 of the next day
            return pd.Timestamp(base_date + timedelta(days=1))
        # Use dayfirst=True to parse dates like "23-06-2017"
        return pd.to_datetime(date_str, dayfirst=True)
    except Exception as e:
        logger.warning(f"Date parse error: '{date_str}' -> {e}")
        return pd.NaT  # pyrefly: ignore[bad-return]


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
    flowboundaries = model.flow_boundary.node.df.name  # pyrefly: ignore[missing-attribute]
    xls = pd.ExcelFile(transboundary_data_path)
    sheet_names = xls.sheet_names
    print("sheet_names", sheet_names)
    df_raw_data = []

    for sheet in sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        if "Datum" in df.columns:
            logger.info(f"Importeer de data voor: {', '.join(df.columns[1:])}")
            df["Datum"] = df["Datum"].apply(fix_date_string)
            df = df.dropna(subset=["Datum"])
            df.set_index("Datum", inplace=True)
            df_filtered = df[(df.index >= start_time) & (df.index <= stop_time)].copy()
            df_numeric = df_filtered.apply(pd.to_numeric, errors="coerce")
            df_daily = df_numeric.resample("D").mean()
            df_raw_data.append(df_daily)
        else:
            print("sheet without datum:", sheet)

    # Combine data
    df_combined = pd.concat(df_raw_data, axis=0)
    df_combined = df_combined.groupby("Datum").mean(numeric_only=True)

    # Filter columns that exist in flowboundaries
    available_columns = [col for col in df_combined.columns if col in flowboundaries.values]
    df_inflow = df_combined[available_columns].copy()
    df_inflow.index.name = "time"

    # Interpolate missing values within the time series
    df_inflow = df_inflow.interpolate(method="time", limit_area="inside")

    # Fill NaN's at the edges with the mean flow rate at that location
    for col in df_inflow.columns:
        mean_value = df_inflow[col].mean()
        df_inflow[col] = df_inflow[col].fillna(mean_value)

    # If no data exists in the modelled period: set flow to 0
    cols_with_nan = df_inflow.columns[df_inflow.isna().any()]
    for col in cols_with_nan:
        mean_value = df_inflow[col].mean()
        if pd.isna(mean_value):  # entire column is empty
            logger.warning(f"Location '{col}' has no measurements; filling NaNs with 0.")
            df_inflow[col] = df_inflow[col].fillna(0)
        else:
            logger.warning(
                f"Location '{col}' has no measurements during the modelled period; "
                f"filling NaNs with mean value ({mean_value:.2f})."
            )
            df_inflow[col] = df_inflow[col].fillna(mean_value)

    # Check for remaining NaN values
    assert not df_inflow.isna().any().any(), "There are NaN values remaining!"

    # Convert to dictionary
    dict_flow = {}
    for loc in df_inflow.columns:
        df_single = df_inflow[[loc]].copy()
        df_single.columns = ["flow_rate"]
        dict_flow[loc] = df_single.reset_index()

    # Add node_id to each location and filter out locations not found in model
    locations_to_remove = []
    for loc in dict_flow:
        try:
            node_id = model.flow_boundary.node.df.reset_index(drop=False).set_index("name").at[loc, "node_id"]  # pyrefly: ignore[missing-attribute]
            dict_flow[loc]["node_id"] = node_id
        except KeyError:
            logger.warning(
                f"Warning: '{loc}' not found in model.flow_boundary.node.df. Will be removed from dict_flow."
            )
            locations_to_remove.append(loc)

    # Remove locations that were not found in the model
    for loc in locations_to_remove:
        dict_flow.pop(loc, None)

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

# %%

import geopandas as gpd
import numpy as np
import pandas as pd
import tqdm
from shapely.geometry import box
from shapely.prepared import prep
from xarray.core.dataset import Dataset

from ribasim_nl import CloudStorage


def SyncWiwbMeteoFromCloud(cloud):
    """
    Synchronize Meteo information from cloud to local directory

    Function that syncs the LHM Precipitation and Makkink Evaporation (Meteobase) from the Deltares GoodCloud
    storage to a local directory (if not available yet)

    Parameters
    ----------
    cloud (CloudStorage)
        A cloud storage object that is used to connect to the Cloud facilities for Ribasim
    """
    WIWB_dir = cloud.joinpath("Basisgegevens", "WIWB")
    WIWB_Precip_path = WIWB_dir / "Meteobase.Precipitation.nc"
    WIWB_Evap_path = WIWB_dir / "Meteobase.Evaporation.Makkink.nc"

    cloud.synchronize(filepaths=[WIWB_Precip_path, WIWB_Evap_path])

    print(f"WIWB Meteo data synced from cloud. Available in {WIWB_dir}")


def SyncModelFromCloud(cloud: CloudStorage, authority: str = "Rijkswaterstaat", model: str = "lhm_coupled_2025_5_0"):
    """
    Synchronize desired model from the cloud to local directory

    Function that syncs a required model from the Deltares GoodCloud storage to a local directory (if not available yet)

    Parameters
    ----------
    cloud (CloudStorage)
        A cloud storage object that is used to connect to the Cloud facilities for Ribasim
    authority (str)
        The water authority that a model is requested for
    model (str)
        The model version that is chosen from the specific authority
    """
    model_dir = cloud.joinpath(authority, "modellen", model)

    cloud.synchronize(filepaths=[model_dir])
    print("Done syncing model dirs from the cloud")


def GetFractionalGridPerBasin(xll_coords: np.ndarray, yll_coords: np.ndarray, basins: gpd.GeoDataFrame):
    """
    Get the meteo grid coverage per basin, expressed as a fraction for each of the covered cells

    Calculates the fractional overlap of supplied basins given a set of x and y coordinates.
    It returns a dictionary with the touching cells (indices) for each basin, and the fractional coverage.

    Parameters
    ----------
    xll_coords (np.ndarray)
        An array of x-coordinates of a grid (lower left corner).
    yll_coords (np.ndarray)
        An array of y-coordinates of a grid (lower left corner)
    basins (gpd.GeoDataFrame)
        A GeoDataFrame with the basins as used in the Ribasim model.

    """
    # Get the cell area
    cell_area = abs((xll_coords[1] - xll_coords[0]) * (yll_coords[1] - yll_coords[0]))

    # Read in the modelbasins and prepare the geometries for spatial intersection
    nodeids = basins["node_id"].tolist()
    prepared_geoms = [(nodeids[i], prep(geom)) for i, geom in enumerate(basins.geometry)]
    node_geoms = dict(zip(nodeids, basins.geometry))  # For actual intersection area calc

    # Get the height and the width of the grid
    height = len(yll_coords)
    width = len(xll_coords)

    # Initialize an empty dictionary
    fraction_map = {}

    for node_id, prep_geom in tqdm.tqdm(prepared_geoms, desc="Overlapping polygons with rasters"):
        geom = node_geoms[node_id]
        xl, yl, xr, yu = geom.bounds  # minx, miny, maxx, maxy

        # Convert bounds to raster row/col indices
        xmin_diff = xl - xll_coords
        xmax_diff = xr - xll_coords
        ymin_diff = yl - yll_coords
        ymax_diff = yu - yll_coords

        col_start = np.where(xmin_diff > 0)[0][np.argmin(xmin_diff[xmin_diff > 0])] if any(xmin_diff > 0) else 0
        col_end = np.where(xmax_diff > 0)[0][np.argmin(xmax_diff[xmax_diff > 0])] if any(xmax_diff > 0) else 0
        row_end = (
            np.where(ymin_diff > 0)[0][np.argmin(ymin_diff[ymin_diff > 0])] if any(ymin_diff > 0) else 0
        )  # Y defined from top down
        row_start = (
            np.where(ymax_diff > 0)[0][np.argmin(ymax_diff[ymax_diff > 0])] if any(ymax_diff > 0) else 0
        )  # Y defined from top down

        fraction_map[node_id] = []

        # If the geometry is fully outside of the raster, get the nearest cell
        if row_start >= row_end or row_end <= 0 or col_start >= col_end or col_end <= 0:
            closest_row = min(max(0, row_start), height - 1)
            closest_col = min(max(0, col_start), width - 1)
            fraction_map[node_id].append((closest_row, closest_col, 1))

        else:  # If the geometry has valid rows and cols
            for row in range(row_start, row_end):
                for col in range(col_start, col_end):
                    cell_poly = box(xll_coords[col], yll_coords[row], xll_coords[col + 1], yll_coords[row + 1])

                    if prep_geom.intersects(cell_poly):
                        intersection = cell_poly.intersection(geom)
                        if not intersection.is_empty:
                            frac = intersection.area / cell_area
                            if frac > 0:
                                fraction_map[node_id].append((row, col, frac))

    return fraction_map


def GetMeteoPerBasin(startdate: str, enddate: str, precip: Dataset, evp: Dataset, fraction_map: dict):
    """
    Get dynamic meteo per basin

    Function takes meteo information and extracts the required timeseries per basin, taking into account which
    cells are covered by the basin using the fraction_map argument.

    Parameters
    ----------
    startdate (str)
        Startdate from where the meteo information is extracted. Format: YYYY-MM-DD
    enddate (str)
        Enddate until when the meteo information is extracted. Format: YYYY-MM-DD
    precip (Dataset)
        Xarray object containing variables x, y, time and precipitation (in mm/d)
    evp (Dataset)
        Xarray object containing variables x, y, time and evaporation (Makkink, in mm/d)
    fraction_map (dict)
        Dictionary with the cells covered (including fraction of coverage) for each basin, based on ID

    """
    # Select the right time indices
    time = precip["time"].values
    startdate = np.datetime64(startdate)
    enddate = np.datetime64(enddate)
    mask = (time >= startdate) & (time <= enddate)
    time_indices = np.where(mask)[0]

    # Load the time slice into memory
    precip_data = precip["P"].isel(time=slice(time_indices[0], time_indices[-1] + 1)).load().data
    evp_data = evp["Evaporation"].isel(time=slice(time_indices[0], time_indices[-1] + 1)).load().data

    means = {}
    for node_id, pixels in tqdm.tqdm(fraction_map.items(), desc="Extracting meteo per basin"):
        means[node_id] = {}

        # If no pixels are found that overlap, add NaN
        if len(pixels) == 0:
            means[node_id]["prec"] = [np.nan] * len(time_indices)
            means[node_id]["evp"] = [np.nan] * len(time_indices)
            continue

        # Extract the meteo information for each basin
        values_P = np.stack([precip_data[:, row, col] for row, col, _ in pixels], axis=1)
        values_ET = np.stack([evp_data[:, row, col] for row, col, _ in pixels], axis=1)

        weights = np.array([frac for _, _, frac in pixels])

        # Calculate the weighted average per basin
        averaged_P_mmd = np.average(values_P, axis=1, weights=weights)
        averaged_ET_mmd = np.average(values_ET, axis=1, weights=weights)

        # Convert the precipitation units
        averaged_P_ms = averaged_P_mmd / 86400 / 1000

        # Convert the ET units and convert to open water ET using a factor of 1.26
        averaged_ET_ms = averaged_ET_mmd / 86400 / 1000 * 1.26

        means[node_id]["prec"] = averaged_P_ms.tolist()
        means[node_id]["evp"] = averaged_ET_ms.tolist()

    # Convert into the right DataFrame format to add to the model
    full_time_df = CombineMeteoIntoDF(means, startdate, enddate)
    print("Converted the meteo data to a pd.DataFrame")
    return full_time_df


def CombineMeteoIntoDF(meteo_per_node: dict, start_date: str, end_date: str):
    """
    Convert a dict with meteo info to a pd.Dataframe

    Function converts a given dictionary with timeseries per basin to a proper dataframe that can be used
    as input to Ribasim models.

    Parameters
    ----------
    meteo_per_node (dict)
        Dictionary with per node_id (basin) a list of precipitation and evaporation values between the start and end date
    start_date (str)
        Start date of the timeseries of meteo information
    end_date (str)
        End date of the timeseries of meteo information

    Returns
    -------
    Returns a pd.DataFrame with 4 columns; node_id, time, precipitation and potential_evaporation.
    """
    # Convert the information to a dataframe
    meteo_dataframe = {nodeid: pd.DataFrame(v) for nodeid, v in meteo_per_node.items()}

    list_of_dfs = []
    # Add the timestamp and id per node
    for nodeid, df in meteo_dataframe.items():
        df["time"] = pd.date_range(start_date, end_date)  # .dt.floor('ms')
        df["node_id"] = nodeid
        list_of_dfs.append(df)

    # Combine all the node dataframes
    full_time_df = pd.concat(list_of_dfs, ignore_index=True)

    # Rename the original columns to the necessary column headers for Ribasim
    full_time_df.rename(columns={"evp": "potential_evaporation", "prec": "precipitation"}, inplace=True)

    return full_time_df


def AddMeteoModel(meteo_means: pd.DataFrame, model, start_date, end_date):
    """
    Add dynamic meteo information to an existing Ribasim model

    Add the meteo information (dynamic) to the model. If a basin.time.df is already specified, the precipitation and evaporation values are replaced.
    If a mode.basin.time.df is not present, it is created and the drainage and infiltration fluxes are set to 0

    Parameters
    ----------
    meteo_means
        Dataframe with the meteo information per timestep per node
    model
        Ribasim model object to which the information is added
    start_date
        String representation of the start date of the model
    end_date
        String representation of the end date of the model

    Returns
    -------
    The function returns a model with the right dynamic meteo information incorporated
    """
    if model.basin.time.df is None:
        if model.basin.static.df is not None:
            final_time_df = meteo_means.merge(
                model.basin.static.df[["node_id", "drainage", "infiltration"]], on="node_id", how="left"
            )
        else:
            meteo_means["drainage"] = 0
            meteo_means["infiltration"] = 0
            final_time_df = meteo_means.copy()

        model.basin.time.df = final_time_df
        model.basin.time.df.fillna(0, inplace=True)

    else:
        current_df = model.basin.time.df
        current_df["conv_time"] = pd.to_datetime(current_df["time"])
        current_df = current_df.merge(
            meteo_means,
            left_on=["node_id", "conv_time"],
            right_on=["node_id", "time"],
            how="left",
            suffixes=("_existing", "_new"),
        )

        current_df.rename(
            columns={
                "time_existing": "time",
                "potential_evaporation_new": "potential_evaporation",
                "precipitation_new": "precipitation",
            },
            inplace=True,
        )
        current_df.drop(
            columns=["time_new", "conv_time", "potential_evaporation_existing", "precipitation_existing"], inplace=True
        )

        model.basin.time.df = current_df
        model.basin.time.df.fillna(0, inplace=True)
    # Reset the static information
    model.basin.static.df = None

    # Set the start and end date of the model
    model.starttime = start_date
    model.endtime = end_date
    print("Dynamic meteo added to model")
    return model


# %%

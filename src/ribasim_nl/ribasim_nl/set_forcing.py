# copied from notebooks\meteo\add_meteo.py

import geopandas as gpd
import numpy as np
import pandas as pd
import tqdm
import xarray as xr
from shapely.geometry import box
from shapely.prepared import prep

from ribasim_nl import CloudStorage, Model


class SetDynamicForcing:
    def __init__(
        self,
        model: Model,
        cloud: CloudStorage,
        startdate: str,
        enddate: str,
    ):
        self.model = model
        self.cloud = cloud
        self.startdate = startdate
        self.enddate = enddate

    def add(self):
        ############# SET THE DESIRED MODEL AND TIME PERIOD ###################
        # authority = "Rijkswaterstaat"  # Water authority folder that is used on the Cloud Storage
        # model = "lhm_coupled_2025_5_0"  # Model that is selected on the Cloud Storage
        # startdate = "2017-01-01"  # Startdate of the modelrun
        # enddate = "2017-12-31"  # Enddate of the modelrun
        ######################################################################

        # Load the precipitation, evaporation and basins
        precip = xr.open_dataset(self.cloud.joinpath("Basisgegevens/WIWB/Meteobase.Precipitation.nc"))
        evp = xr.open_dataset(self.cloud.joinpath("Basisgegevens/WIWB/Meteobase.Evaporation.Makkink.nc"))

        # Load the basins from the model
        basins = self.model.basin.area.df

        # Extract arrays of x and y coordinates from the meteo grids
        xll_coords = precip["x"].values
        yll_coords = precip["y"].values

        # Get a dictionary with fractional coverage for each basin
        fraction_map = self._get_fractional_grid_per_basin(xll_coords, yll_coords, basins)

        # Get the meteo input per basin as pd.DataFrame
        meteo_time_df = self._get_meteo_per_basin(self.startdate, self.enddate, precip, evp, fraction_map)

        # Add the meteo information to the selected model
        new_model = self._add_meteo_to_model(meteo_time_df)

        # return the new model with the meteo information added
        return new_model

    def _sync_meteo_from_cloud(self):
        """
        Synchronize Meteo information from cloud to local directory

        Function that syncs the LHM Precipitation and Makkink Evaporation (Meteobase) from the Deltares GoodCloud
        storage to a local directory (if not available yet)
        """
        WIWB_dir = self.cloud.joinpath("Basisgegevens/WIWB")
        WIWB_Precip_path = WIWB_dir / "Meteobase.Precipitation.nc"
        WIWB_Evap_path = WIWB_dir / "Meteobase.Evaporation.Makkink.nc"
        self.cloud.synchronize(filepaths=[WIWB_Precip_path, WIWB_Evap_path])
        print(f"WIWB Meteo data synced from cloud. Available in {WIWB_dir}")

    def _get_fractional_grid_per_basin(self, xll_coords: np.ndarray, yll_coords: np.ndarray, basins: gpd.GeoDataFrame):
        """
        Get the meteo grid coverage per basin, expressed as a fraction for each of the covered cells

        Calculates the fractional overlap of supplied basins given a set of x and y coordinates.
        It returns a dictionary with the touching cells (indices) for each basin, and the fractional coverage.
        """
        cell_area = abs((xll_coords[1] - xll_coords[0]) * (yll_coords[1] - yll_coords[0]))
        nodeids = basins["node_id"].tolist()
        prepared_geoms = [(nodeids[i], prep(geom)) for i, geom in enumerate(basins.geometry)]
        node_geoms = dict(zip(nodeids, basins.geometry))

        height = len(yll_coords)
        width = len(xll_coords)
        fraction_map = {}

        for node_id, prep_geom in tqdm.tqdm(prepared_geoms, desc="Overlapping polygons with rasters"):
            geom = node_geoms[node_id]
            xl, yl, xr, yu = geom.bounds
            xmin_diff = xl - xll_coords
            xmax_diff = xr - xll_coords
            ymin_diff = yl - yll_coords
            ymax_diff = yu - yll_coords

            col_start = np.where(xmin_diff > 0)[0][np.argmin(xmin_diff[xmin_diff > 0])] if any(xmin_diff > 0) else 0
            col_end = np.where(xmax_diff > 0)[0][np.argmin(xmax_diff[xmax_diff > 0])] if any(xmax_diff > 0) else 0
            row_end = np.where(ymin_diff > 0)[0][np.argmin(ymin_diff[ymin_diff > 0])] if any(ymin_diff > 0) else 0
            row_start = np.where(ymax_diff > 0)[0][np.argmin(ymax_diff[ymax_diff > 0])] if any(ymax_diff > 0) else 0

            fraction_map[node_id] = []
            if row_start >= row_end or row_end <= 0 or col_start >= col_end or col_end <= 0:
                closest_row = min(max(0, row_start), height - 1)
                closest_col = min(max(0, col_start), width - 1)
                fraction_map[node_id].append((closest_row, closest_col, 1))
            else:
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

    def _get_meteo_per_basin(
        self, startdate: str, enddate: str, precip: xr.Dataset, evp: xr.Dataset, fraction_map: dict
    ):
        """
        Get dynamic meteo per basin

        Function takes meteo information and extracts the required timeseries per basin, taking into account which
        cells are covered by the basin using the fraction_map argument.
        """
        time = precip["time"].values
        startdate = np.datetime64(startdate)
        enddate = np.datetime64(enddate)
        mask = (time >= startdate) & (time <= enddate)
        time_indices = np.where(mask)[0]

        precip_data = precip["P"].isel(time=slice(time_indices[0], time_indices[-1] + 1)).load().data
        evp_data = evp["Evaporation"].isel(time=slice(time_indices[0], time_indices[-1] + 1)).load().data

        means = {}
        for node_id, pixels in tqdm.tqdm(fraction_map.items(), desc="Extracting meteo per basin"):
            means[node_id] = {}
            if len(pixels) == 0:
                means[node_id]["prec"] = [np.nan] * len(time_indices)
                means[node_id]["evp"] = [np.nan] * len(time_indices)
                continue
            values_P = np.stack([precip_data[:, row, col] for row, col, _ in pixels], axis=1)
            values_ET = np.stack([evp_data[:, row, col] for row, col, _ in pixels], axis=1)
            weights = np.array([frac for _, _, frac in pixels])

            averaged_P_ms = np.average(values_P, axis=1, weights=weights) / 86400 / 1000
            averaged_ET_ms = np.average(values_ET, axis=1, weights=weights) / 86400 / 1000 * 1.26

            means[node_id]["prec"] = averaged_P_ms.tolist()
            means[node_id]["evp"] = averaged_ET_ms.tolist()

        # Convert into the right DataFrame format to add to the model
        full_time_df = self._combine_meteo_into_df(means, startdate, enddate)
        print("Converted the meteo data to a pd.DataFrame")
        return full_time_df

    def _combine_meteo_into_df(self, meteo_per_node: dict, start_date: str, end_date: str):
        """
        Convert a dict with meteo info to a pd.Dataframe

        Function converts a given dictionary with timeseries per basin to a proper dataframe that can be used
        as input to Ribasim models.
        """
        meteo_dataframe = {nodeid: pd.DataFrame(v) for nodeid, v in meteo_per_node.items()}
        list_of_dfs = []
        for nodeid, df in meteo_dataframe.items():
            df["time"] = pd.date_range(start_date, end_date)
            df["node_id"] = nodeid
            list_of_dfs.append(df)
        full_time_df = pd.concat(list_of_dfs, ignore_index=True)
        full_time_df.rename(columns={"evp": "potential_evaporation", "prec": "precipitation"}, inplace=True)
        return full_time_df

    def _add_meteo_to_model(self, meteo_means: pd.DataFrame):
        """
        Add dynamic meteo information to an existing Ribasim model

        Add the meteo information (dynamic) to the model. If a basin.time.df is already specified, the precipitation and evaporation values are replaced.
        If a mode.basin.time.df is not present, it is created and the drainage and infiltration fluxes are set to 0
        """
        model = self.model
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
                columns=["time_new", "conv_time", "potential_evaporation_existing", "precipitation_existing"],
                inplace=True,
            )
            model.basin.time.df = current_df
            model.basin.time.df.fillna(0, inplace=True)

        # Reset the static information
        model.basin.static.df = None

        # Set the start and end date of the model
        model.starttime = self.startdate
        model.endtime = self.enddate
        print("Dynamic meteo added to model")
        return model

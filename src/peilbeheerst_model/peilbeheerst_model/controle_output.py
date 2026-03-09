import logging
import os
import shutil
from pathlib import Path

import geopandas as gpd
import pandas as pd
import xarray as xr

from ribasim_nl import Model


def model_loaded(func: callable) -> callable:
    """Wrapper-function to assert that the model data is loaded before using methods that analyse this data.

    :param func: class-method
    :type func: callable

    :return: wrapper
    :rtype: callable
    """

    def wrapper(*args, **kwargs) -> dict:
        """Wrapper-function to assert that the model is loaded before using methods that analyse the data.

        :param args: positional arguments
        :param kwargs: optional arguments

        :return: control dictionary
        :rtype: dict
        """
        co = args[0]
        assert isinstance(co, Control)
        assert all(getattr(co, k) is not None for k in ("ds_basin", "ds_link", "model"))
        return func(*args, **kwargs)

    return wrapper


class Control:
    ds_basin: xr.Dataset = None
    ds_link: xr.Dataset = None
    model: Model = None

    def __init__(self, qlr_path=None, work_dir=None, ribasim_toml=None):
        if (work_dir is None) and (ribasim_toml is None):
            raise ValueError("provide either work_dir or ribasim_toml")
        else:
            if ribasim_toml is not None:
                self.path_ribasim_toml = ribasim_toml
                self.work_dir = Path(ribasim_toml).parent
            else:
                self.path_ribasim_toml = os.path.join(work_dir, "ribasim.toml")
                self.work_dir = work_dir

        if qlr_path is None:
            self.qlr_path = Path(__file__).parent.joinpath("data", "output_controle.qlr")
        else:
            self.qlr_path = qlr_path
        self.path_basin_output = os.path.join(self.work_dir, "results", "basin.nc")
        self.path_link_output = os.path.join(self.work_dir, "results", "flow.nc")
        self.path_control_dict_path = os.path.join(self.work_dir, "results", "output_controle")

    def read_model_output(self):
        ds_basin = xr.open_dataset(self.path_basin_output)
        ds_link = xr.open_dataset(self.path_link_output)
        model = Model.read(self.path_ribasim_toml)

        self.ds_basin = ds_basin
        self.ds_link = ds_link
        self.model = model
        control_dict = {}

        return control_dict

    @staticmethod
    def _to_bool_series(series: pd.Series) -> pd.Series:
        """Convert mixed True/False representations to booleans without pandas downcasting warnings."""
        normalized = series.astype("string").str.strip().str.lower()
        return normalized.map({"true": True, "false": False, "1": True, "0": False}).fillna(False).astype(bool)

    @model_loaded
    def initial_final_level(self, control_dict):
        initial_level = self.ds_basin["level"].isel(time=0).to_series().rename("initial_level")
        final_level = self.ds_basin["level"].isel(time=-1).to_series().rename("final_level")
        initial_final_level_df = pd.concat([initial_level, final_level], axis=1).reset_index()
        initial_final_level_df["difference_level"] = (
            initial_final_level_df["initial_level"] - initial_final_level_df["final_level"]
        )

        # Retrieve the geometries
        initial_final_level_df = initial_final_level_df.merge(
            self.model.basin.node.df, on="node_id", suffixes=("", "model_")
        )
        initial_final_level_df = initial_final_level_df.set_geometry("geometry")

        initial_final_level_df = gpd.GeoDataFrame(initial_final_level_df, geometry="geometry")

        # add to the dictionary
        control_dict["initial_final_level"] = initial_final_level_df

        return control_dict

    @model_loaded
    def min_max_level(self, control_dict):
        basin_level = self.ds_basin["level"]
        min_basin_level = pd.concat(
            [
                basin_level.min(dim="time").to_series().rename("min_level"),
                basin_level.idxmin(dim="time").to_series().rename("min_level_time"),
            ],
            axis=1,
        ).reset_index()

        # retrieve the geometries
        min_basin_level = min_basin_level.merge(
            self.model.basin.node.df[["geometry"]], left_on="node_id", right_index=True, suffixes=("", "model_")
        )

        max_basin_level = pd.concat(
            [
                basin_level.max(dim="time").to_series().rename("max_level"),
                basin_level.idxmax(dim="time").to_series().rename("max_level_time"),
            ],
            axis=1,
        ).reset_index()

        # retrieve the geometries
        max_basin_level = max_basin_level.merge(
            self.model.basin.node.df[["geometry"]], left_on="node_id", right_index=True, suffixes=("", "model_")
        )

        # convert to geopandas
        min_basin_level = gpd.GeoDataFrame(min_basin_level, geometry="geometry")
        max_basin_level = gpd.GeoDataFrame(max_basin_level, geometry="geometry")

        control_dict["min_level"] = min_basin_level
        control_dict["max_level"] = max_basin_level

        return control_dict

    @model_loaded
    def error(self, control_dict):
        relative_error = self.ds_basin["relative_error"]
        abs_relative_error = abs(relative_error)

        error_gdf = pd.concat(
            [
                abs_relative_error.max(dim="time").to_series().rename("max_abs_error"),
                abs_relative_error.idxmax(dim="time").to_series().rename("max_abs_error_time"),
                abs_relative_error.sum(dim="time").to_series().rename("max_abs_sum_error"),
                abs_relative_error.mean(dim="time").to_series().rename("max_abs_average_error"),
                relative_error.sum(dim="time").to_series().rename("summed_error"),
            ],
            axis=1,
        ).reset_index()

        error_gdf = error_gdf.merge(self.model.basin.node.df, on="node_id", suffixes=("", "model_"))
        error_gdf = error_gdf.set_geometry("geometry")

        error_gdf = gpd.GeoDataFrame(error_gdf, geometry="geometry")

        control_dict["error"] = error_gdf

        return control_dict

    @model_loaded
    def stationary(self, control_dict):
        time_index = pd.DatetimeIndex(self.ds_basin["time"].values)
        last_time = time_index[-1]
        time_window_start = last_time - pd.Timedelta(hours=24)

        level = self.ds_basin["level"]
        average_last_values = level.sel(time=slice(time_window_start, last_time)).mean(dim="time")
        actual_last_value = level.sel(time=last_time)
        stationary = (abs(actual_last_value - average_last_values) <= 0.01).to_series().rename("stationary")
        stationary_gdf = stationary.reset_index().dropna()

        # Retrieve the geometries
        stationary_gdf["geometry"] = stationary_gdf.merge(
            self.model.basin.node.df, on="node_id", suffixes=("", "model_")
        )["geometry"]

        control_dict["stationary"] = gpd.GeoDataFrame(stationary_gdf, geometry="geometry")

        return control_dict

    @model_loaded
    def find_mean_flow(self, control_dict):
        df_link_avg = pd.concat(
            [
                abs(self.ds_link["flow_rate"]).mean(dim="time").to_series().rename("flow_rate"),
                self.ds_link["from_node_id"].to_series().rename("from_node_id"),
                self.ds_link["to_node_id"].to_series().rename("to_node_id"),
            ],
            axis=1,
        ).reset_index()
        df_link_avg["time"] = pd.Timestamp(self.ds_link["time"].values[0])
        print("MAX VALUE DF_LINK", df_link_avg["flow_rate"].max())

        # Merge the geometry from links_ribasim to df_link_avg to retrieve the geometries
        links_ribasim = self.model.link.df.copy()
        df_link_avg = df_link_avg.merge(
            right=links_ribasim[["from_node_id", "to_node_id", "geometry"]],
            on=["from_node_id", "to_node_id"],
            how="left",
        )

        df_link_avg = gpd.GeoDataFrame(df_link_avg).set_crs(crs="EPSG:28992")
        control_dict["flow"] = df_link_avg

        return control_dict

    @model_loaded
    def water_aanvoer_areas(self, control_dict):
        """Retrieve the areas (polygons) of the wateraanvoer gebieden."""
        aanvoer_areas = self.model.basin.area.df.copy(deep=True)
        aanvoer_areas["meta_aanvoer"] = self._to_bool_series(aanvoer_areas["meta_aanvoer"])

        aanvoer_areas = aanvoer_areas.loc[
            aanvoer_areas["meta_aanvoer"]
        ]  # only select the rows where meta_aanvoer == True

        aanvoer_areas = gpd.GeoDataFrame(aanvoer_areas[["node_id", "geometry"]], crs="EPSG:28992")
        control_dict["aanvoer_areas"] = aanvoer_areas

        return control_dict

    @model_loaded
    def water_aanvoer_afvoer_basin_nodes(self, control_dict):
        """Retrieve the nodes (points) of the wateraanvoer gebieden basin, as well as afvoer basin nodes"""
        aanvoer_areas = self.model.basin.area.df.copy(deep=True)
        basin_nodes = self.model.basin.node.df.copy(deep=True).reset_index()
        basin_nodes = basin_nodes.merge(right=aanvoer_areas[["node_id", "meta_aanvoer"]], how="left", on="node_id")

        basin_nodes["meta_aanvoer"] = self._to_bool_series(basin_nodes["meta_aanvoer"])

        basin_aanvoer_nodes = basin_nodes.loc[
            basin_nodes["meta_aanvoer"]
        ]  # only select the rows where meta_aanvoer == True
        basin_afvoer_nodes = basin_nodes.loc[
            ~basin_nodes["meta_aanvoer"]
        ]  # only select the rows where meta_aanvoer != True

        basin_aanvoer_nodes = gpd.GeoDataFrame(basin_aanvoer_nodes[["node_id", "geometry"]], crs="EPSG:28992")
        basin_afvoer_nodes = gpd.GeoDataFrame(basin_afvoer_nodes[["node_id", "geometry"]], crs="EPSG:28992")

        control_dict["aanvoer_basin_nodes"] = basin_aanvoer_nodes
        control_dict["afvoer_basin_nodes"] = basin_afvoer_nodes

        return control_dict

    @model_loaded
    def water_aanvoer_afvoer_pumps(self, control_dict):
        """Retrieve the nodes (points) of the wateraan- and afvoer pumps"""
        aanvoer_afvoer_pumps = self.model.pump.static.df.copy(deep=True)
        aanvoer_afvoer_pumps_nodes = self.model.pump.node.df.copy(deep=True).reset_index()
        aanvoer_afvoer_pumps = aanvoer_afvoer_pumps.merge(
            aanvoer_afvoer_pumps_nodes[["node_id", "geometry"]], on="node_id", how="left"
        )

        aanvoer_afvoer_pumps = gpd.GeoDataFrame(
            aanvoer_afvoer_pumps[["node_id", "meta_func_afvoer", "meta_func_aanvoer", "geometry"]], crs="EPSG:28992"
        )
        afvoer_pumps = aanvoer_afvoer_pumps.loc[aanvoer_afvoer_pumps.meta_func_afvoer == 1]
        aanvoer_pumps = aanvoer_afvoer_pumps.loc[aanvoer_afvoer_pumps.meta_func_aanvoer == 1]
        AanAfvoer_pumps = aanvoer_afvoer_pumps.loc[
            (aanvoer_afvoer_pumps.meta_func_aanvoer == 1) & (aanvoer_afvoer_pumps.meta_func_afvoer == 1)
        ]

        control_dict["afvoer_pumps"] = afvoer_pumps
        control_dict["aanvoer_pumps"] = aanvoer_pumps
        control_dict["AanAfvoer_pumps"] = AanAfvoer_pumps

        return control_dict

    @model_loaded
    def water_aanvoer_outlets(self, control_dict):
        """Retrieve the nodes (points) of the wateraan outlets"""
        aanvoer_outlets = self.model.outlet.static.df.copy(deep=True)
        outlet_nodes = self.model.outlet.node.df.copy(deep=True).reset_index()
        aanvoer_outlets = aanvoer_outlets.merge(
            outlet_nodes[["node_id", "geometry"]], on="node_id", how="left"
        )  # merge to retrieve the geoms
        aanvoer_outlets = aanvoer_outlets.loc[
            aanvoer_outlets.meta_aanvoer == 1
        ].reset_index()  # only select the aanvoer nodes
        aanvoer_outlets = gpd.GeoDataFrame(aanvoer_outlets[["node_id", "geometry"]], crs="EPSG:28992")
        control_dict["aanvoer_outlets"] = aanvoer_outlets

        return control_dict

    @model_loaded
    def mask_basins(self, control_dict):
        if "meta_gestuwd" in self.model.basin.node.df.columns:
            control_dict["mask_afvoer"] = self.model.basin.node.df[
                self.model.basin.node.df["meta_gestuwd"] == "False"
            ].reset_index()[["node_id", "geometry"]]

        return control_dict

    @model_loaded
    def flow_rate(self, control_dict):
        flow_rate = self.ds_link["flow_rate"].isel(time=-1).to_dataframe().reset_index()
        link_df = self.model.link.df.reset_index()
        link_df = link_df.merge(flow_rate[["link_id", "flow_rate"]], on="link_id", how="left").set_index("link_id")
        control_dict["flow_rate"] = link_df

        return control_dict

    @model_loaded
    def store_data(self, data, output_path):
        """Store the control_dict"""
        for key in data.keys():
            data[str(key)].to_file(output_path + ".gpkg", layer=str(key), driver="GPKG", mode="w")

        # copy checks_symbology file from old dir to new dir
        # delete old .qlr file (overwriting does apparently not work due to permission rights)
        if os.path.exists(os.path.join(self.work_dir, "results", "output_controle.qlr")):
            os.remove(os.path.join(self.work_dir, "results", "output_controle.qlr"))

        # copy .qlr file
        shutil.copy(src=self.qlr_path, dst=os.path.join(self.work_dir, "results", "output_controle.qlr"))

        return

    def run_all(self):
        control_dict = self.read_model_output()
        control_dict = self.initial_final_level(control_dict)
        control_dict = self.min_max_level(control_dict)
        control_dict = self.error(control_dict)
        control_dict = self.stationary(control_dict)
        control_dict = self.find_mean_flow(control_dict)
        control_dict = self.water_aanvoer_areas(control_dict)
        control_dict = self.water_aanvoer_afvoer_basin_nodes(control_dict)
        control_dict = self.water_aanvoer_afvoer_pumps(control_dict)
        control_dict = self.water_aanvoer_outlets(control_dict)
        control_dict = self.mask_basins(control_dict)

        self.store_data(data=control_dict, output_path=self.path_control_dict_path)

        return control_dict

    def run_afvoer(self):
        control_dict = self.read_model_output()
        control_dict = self.initial_final_level(control_dict)
        control_dict = self.min_max_level(control_dict)
        control_dict = self.error(control_dict)
        control_dict = self.stationary(control_dict)
        control_dict = self.find_mean_flow(control_dict)
        control_dict = self.mask_basins(control_dict)
        control_dict = self.flow_rate(control_dict)

        self.store_data(data=control_dict, output_path=self.path_control_dict_path)

        return control_dict

    @model_loaded
    def water_level_bounds(self, control_dict: dict, skip_time_steps: int = 0) -> dict:
        """Determine the minimum and maximum water levels within the basins occurring over time.

        As there might be some water level differences related to the initialisation, the bounds are analysed after a
        number of time steps, which is provided using the `skip_time_steps`-argument. This optional argument defaults to
        zero (0), which implies that the whole time-series of basin water levels is used in the analysis by default.

        :param control_dict: analysed data collector
        :param skip_time_steps: number of time steps skipped before determining the water level bounds, defaults to 0

        :type control_dict: dict
        :type skip_time_steps: int, optional

        :return: updated analysed data collector
        :rtype: dict
        """
        start_time = pd.Timestamp(self.ds_basin["time"].values[skip_time_steps])
        level = self.ds_basin["level"].sel(time=slice(start_time, None))

        # minimum water level
        min_basin_level = level.min(dim="time").to_series().rename("level").reset_index(drop=False)

        # maximum water level
        max_basin_level = level.max(dim="time").to_series().rename("level").reset_index(drop=False)

        # collect analysed data in GeoDataFrame
        gdf_min_basin_level = min_basin_level.merge(
            self.model.basin.node.df, on="node_id", suffixes=("", "model_")
        ).set_geometry("geometry")
        gdf_max_basin_level = max_basin_level.merge(
            self.model.basin.node.df, on="node_id", suffixes=("", "model_")
        ).set_geometry("geometry")
        control_dict.update(
            {
                "min_basin_level": gdf_min_basin_level,
                "max_basin_level": gdf_max_basin_level,
            }
        )

        # return updated analysed data collector
        return control_dict

    @model_loaded
    def error_bounds(self, control_dict: dict, autofill_missing_data: bool = True) -> dict:
        """Determine the minimum and maximum basin water level error occurring over time.

        Prior to calculating the error bounds, the water level bounds must be determined. If this is not done, the
        method `.water_level_bounds()` will be called within this method-call if `autofill_missing_data=True`. This
        auto-call will give the water level bounds using the default settings. This functionality can be disabled by
        setting `autofill_missing_data=False`.

        :param control_dict: analysed data collector
        :param autofill_missing_data: autofill water level bounds if missing, defaults to True

        :type control_dict: dict
        :type autofill_missing_data: bool, optional

        :return: updated analysed data collector
        :rtype: dict

        :raise ValueError: if data is missing in the collector and not auto-filled.
        """
        # validate available analysed data
        _keys = "min_basin_level", "max_basin_level"
        if not all(k in control_dict for k in _keys):
            logging.info(f"Water level bounds not yet determined and `autofill_missing_data={autofill_missing_data}`:")
            if autofill_missing_data:
                logging.info("Water level bounds auto-filled: Default settings used.")
                control_dict = self.water_level_bounds(control_dict)
            else:
                _data = {k: control_dict.get(k) for k in _keys}
                msg = f"Not all required data in `control_dict` and autofill disabled: {_data}"
                raise ValueError(msg)

        # get water level bounds data
        min_basin_level = control_dict["min_basin_level"]
        max_basin_level = control_dict["max_basin_level"]

        # initial water level is considered the target level
        initial_basin_level = self.ds_basin["level"].isel(time=0).to_series().rename("level").reset_index()

        # water level differences
        min_difference_level = (
            (min_basin_level.set_index("node_id")["level"] - initial_basin_level.set_index("node_id")["level"])
            .reset_index(drop=False)
            .rename(columns={"level": "level_difference"})
        )
        max_difference_level = (
            (max_basin_level.set_index("node_id")["level"] - initial_basin_level.set_index("node_id")["level"])
            .reset_index(drop=False)
            .rename(columns={"level": "level_difference"})
        )

        # collect analysed data in GeoDataFrame
        gdf_min_basin_level = min_difference_level.merge(
            self.model.basin.node.df, on="node_id", suffixes=("", "model_")
        ).set_geometry("geometry")
        gdf_max_basin_level = max_difference_level.merge(
            self.model.basin.node.df, on="node_id", suffixes=("", "model_")
        ).set_geometry("geometry")
        control_dict.update(
            {
                "error_min_basin_level": gdf_min_basin_level,
                "error_max_basin_level": gdf_max_basin_level,
            }
        )

        # return updated analysed data collector
        return control_dict

    def run_dynamic_forcing(self, **kwargs) -> dict:
        """Run the output control formatting for varying forcing conditions.

        :param kwargs: optional arguments, which are passed on to the various method-calls within this collective data
            analysis call

        :key autofill_missing_data: autofill water level bounds if missing, defaults to False
        :key skip_time_steps: number of time-steps considered as spin-up time and so skipped in analysis, defaults to 0
        :key suppress_file_warning: suppress warning for potentially incompatible *.qlr-file, defaults to False

        :return: analysed data collector
        :rtype: dict
        """
        # optional arguments
        autofill_missing_data: bool = kwargs.get("autofill_missing_data", False)
        skip_time_steps: int = kwargs.get("skip_time_steps", 0)
        suppress_file_warning: bool = kwargs.get("suppress_file_warning", False)

        # analyse output data
        control_dict = self.read_model_output()
        control_dict = self.water_level_bounds(control_dict, skip_time_steps=skip_time_steps)
        control_dict = self.error_bounds(control_dict, autofill_missing_data=autofill_missing_data)
        control_dict = self.water_aanvoer_areas(control_dict)
        control_dict = self.water_aanvoer_afvoer_basin_nodes(control_dict)
        control_dict = self.water_aanvoer_afvoer_pumps(control_dict)
        control_dict = self.water_aanvoer_outlets(control_dict)
        control_dict = self.find_mean_flow(control_dict)

        # check for dynamic forcing specific *.qlr
        filename_cc_qlr = "output_controle_cc.qlr"
        if not suppress_file_warning and not str(self.qlr_path).endswith(filename_cc_qlr):
            logging.warning(f"*.qlr-file is different from default for dynamic forcing: {filename_cc_qlr}")
            logging.warning(f"*.qlr-file may not be compatible with dynamic forcing: {self.qlr_path}")

        # export analysed data
        self.store_data(control_dict, self.path_control_dict_path)

        # return analysed data collector
        return control_dict

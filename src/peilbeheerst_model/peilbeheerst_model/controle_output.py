# %%
import os
import shutil
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ribasim_nl import Model


class Control:
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
        self.path_basin_output = os.path.join(self.work_dir, "results", "basin.arrow")
        self.path_link_output = os.path.join(self.work_dir, "results", "flow.arrow")
        self.path_control_dict_path = os.path.join(self.work_dir, "results", "output_controle")

    def read_model_output(self):
        df_basin = pd.read_feather(self.path_basin_output)
        df_link = pd.read_feather(self.path_link_output)
        model = Model(filepath=self.path_ribasim_toml)

        self.df_basin = df_basin
        self.df_link = df_link
        self.model = model
        control_dict = {}

        return control_dict

    def initial_final_level(self, control_dict):
        basin_level = self.df_basin.sort_values(
            by=["time", "node_id"], ascending=True
        ).copy()  # sort all node_id's based on time
        initial_final_level_df = gpd.GeoDataFrame()

        ini_basin_level = basin_level.drop_duplicates(subset="node_id", keep="first").reset_index(
            drop=True
        )  # pick the FIRST timestamp, which are the initial water levels
        initial_final_level_df["node_id"] = ini_basin_level["node_id"]
        initial_final_level_df["initial_level"] = ini_basin_level["level"]

        final_basin_level = basin_level.drop_duplicates(subset="node_id", keep="last").reset_index(
            drop=True
        )  # pick the LAST timestamp, which are the final water levels
        initial_final_level_df["final_level"] = final_basin_level["level"]
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

    def min_max_level(self, control_dict):
        basin_level = self.df_basin.sort_values(
            by=["level", "node_id"], ascending=True
        ).copy()  # sort all node_id's based on the level

        min_basin_level = basin_level.drop_duplicates(subset="node_id", keep="first").reset_index(
            drop=True
        )  # pick the FIRST sample, which are the minimum water levels
        min_basin_level = min_basin_level[["node_id", "level", "time"]]
        min_basin_level = min_basin_level.rename(columns={"level": "min_level", "time": "min_level_time"})

        # retrieve the geometries
        min_basin_level = min_basin_level.merge(
            self.model.basin.node.df[["geometry"]], left_on="node_id", right_index=True, suffixes=("", "model_")
        )

        max_basin_level = basin_level.drop_duplicates(subset="node_id", keep="last").reset_index(
            drop=True
        )  # pick the LAST sample, which are the maximum water levels
        max_basin_level = max_basin_level[["node_id", "level", "time"]]
        max_basin_level["max_level"] = max_basin_level["level"]
        max_basin_level["max_level_time"] = max_basin_level["time"]

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

    def error(self, control_dict):
        error_gdf = gpd.GeoDataFrame()
        basin_error = self.df_basin.copy()
        basin_error.relative_error = abs(basin_error.relative_error)
        basin_error = basin_error.sort_values(
            by=["node_id", "relative_error"], ascending=True
        )  # sort all node_id's based on the level

        max_error = self.df_basin.drop_duplicates(subset="node_id", keep="last").reset_index(
            drop=True
        )  # pick the LAST sample, which are the largest errors in the water balance
        error_gdf["node_id"] = max_error["node_id"]
        error_gdf["max_abs_error"] = max_error["relative_error"]
        error_gdf["max_abs_error_time"] = max_error["time"]

        relative_error_sum = basin_error.groupby("node_id")["relative_error"].sum().reset_index()
        error_gdf["max_abs_sum_error"] = relative_error_sum["relative_error"]

        relative_error_average = basin_error.groupby("node_id")["relative_error"].mean().reset_index()
        error_gdf["max_abs_average_error"] = relative_error_average["relative_error"]

        # retrieve the total error (not absolute!)
        basin_error = self.df_basin.copy()
        relative_error_sum = basin_error.groupby("node_id")["relative_error"].sum().reset_index()
        error_gdf["summed_error"] = relative_error_sum["relative_error"]

        error_gdf = error_gdf.merge(self.model.basin.node.df, on="node_id", suffixes=("", "model_"))
        error_gdf = error_gdf.set_geometry("geometry")

        error_gdf = gpd.GeoDataFrame(error_gdf, geometry="geometry")

        control_dict["error"] = error_gdf

        return control_dict

    def stationary(self, control_dict):
        def is_stationary(group):
            group = group.sort_values(by="time")

            # Define the time window for the last 24 hours
            last_time = group["time"].iloc[-1]
            time_window_start = last_time - pd.Timedelta(hours=24)

            # Extract levels within the last 24 hours
            last_24_hours = group[group["time"] >= time_window_start]
            average_last_values = last_24_hours["level"].mean()
            actual_last_value = group["level"].iloc[-1]

            # Calculate the deviation
            deviation = abs(actual_last_value - average_last_values)

            # Determine if it's stationary (deviation <= 1 cm)
            stationary = deviation <= 0.01
            return stationary

        stationary_gdf = gpd.GeoDataFrame()
        stationary_gdf["node_id"] = self.df_basin["node_id"]
        stationary_gdf["stationary"] = (
            self.df_basin.groupby("node_id").apply(is_stationary).reset_index(level=0, drop=True)
        )

        stationary_gdf = stationary_gdf.dropna()

        # Retrieve the geometries
        stationary_gdf["geometry"] = stationary_gdf.merge(
            self.model.basin.node.df, on="node_id", suffixes=("", "model_")
        )["geometry"]

        control_dict["stationary"] = gpd.GeoDataFrame(stationary_gdf, geometry="geometry")

        return control_dict

    def find_stationary_flow(self, control_dict, n_hours_mean=24):
        df_link = self.df_link.copy()
        df_link["time"] = pd.to_datetime(df_link["time"])  # convert to time column

        if "link_id" in df_link.columns:
            df_link = df_link.sort_values(by=["time", "link_id"], ascending=True).copy()  # sort values, just in case
        else:
            df_link = df_link.sort_values(by=["time", "link_id"], ascending=True).copy()
        last_time = df_link["time"].max()  # retireve max time value
        time_threshold = last_time - pd.Timedelta(hours=n_hours_mean)  # determine the time threshold, likely 24 hours

        df_link_24h = df_link[df_link["time"] >= time_threshold].copy()  # seelct above the threshold

        # Group by 'link_id' and calculate the average flow rate over the last 24 hours
        if "link_id" in df_link_24h.columns:
            grouper = df_link_24h.groupby("link_id", as_index=False)
        else:
            grouper = df_link_24h.groupby("link_id", as_index=False)
        df_link_avg = grouper.agg(
            {
                "flow_rate": "mean",  # take the mean, as the pumps may not show stationairy results in one timestep
                "from_node_id": "first",  # remains the same for each timestep
                "to_node_id": "first",  # remains the same for each timestep
                "time": "first",  # remains the same for each timestep
            }
        )

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

    def water_aanvoer_areas(self, control_dict):
        """Retrieve the areas (polygons) of the wateraanvoer gebieden."""
        aanvoer_areas = self.model.basin.area.df.copy(deep=True)
        aanvoer_areas["meta_aanvoer"] = (
            aanvoer_areas["meta_aanvoer"].astype(str).replace({"True": True, "False": False}).fillna(False).astype(bool)
        )  # convert all strings to booleans

        aanvoer_areas = aanvoer_areas.loc[
            aanvoer_areas["meta_aanvoer"]
        ]  # only select the rows where meta_aanvoer == True

        aanvoer_areas = gpd.GeoDataFrame(aanvoer_areas[["node_id", "geometry"]], crs="EPSG:28992")
        control_dict["aanvoer_areas"] = aanvoer_areas

        return control_dict

    def water_aanvoer_afvoer_basin_nodes(self, control_dict):
        """Retrieve the nodes (points) of the wateraanvoer gebieden basin, as well as afvoer basin nodes"""
        aanvoer_areas = self.model.basin.area.df.copy(deep=True)
        basin_nodes = self.model.basin.node.df.copy(deep=True).reset_index()
        basin_nodes = basin_nodes.merge(right=aanvoer_areas[["node_id", "meta_aanvoer"]], how="left", on="node_id")

        basin_nodes["meta_aanvoer"] = (
            basin_nodes["meta_aanvoer"].astype(str).replace({"True": True, "False": False}).fillna(False).astype(bool)
        )  # convert all strings to booleans

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

    def mask_basins(self, control_dict):
        if "meta_check_basin_level" in self.model.basin.node.df.columns:
            control_dict["mask_afvoer"] = self.model.basin.node.df[
                self.model.basin.node.df["meta_check_basin_level"] == "False"
            ].reset_index()[["node_id", "geometry"]]

        return control_dict

    def flow_rate(self, control_dict):
        time_stamp = self.model.flow_results.df.index.max()
        flow_rate = self.model.flow_results.df.loc[time_stamp].reset_index().set_index("link_id").flow_rate
        link_df = self.model.link.df.copy()
        link_df.loc[flow_rate.index, "flow_rate"] = flow_rate
        control_dict["flow_rate"] = link_df

        return control_dict

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
        control_dict = self.find_stationary_flow(control_dict)
        control_dict = self.water_aanvoer_areas(control_dict)
        control_dict = self.water_aanvoer_afvoer_basin_nodes(control_dict)
        control_dict = self.water_aanvoer_afvoer_pumps(control_dict)
        control_dict = self.water_aanvoer_outlets(control_dict)

        self.store_data(data=control_dict, output_path=self.path_control_dict_path)

        return control_dict

    def run_afvoer(self):
        control_dict = self.read_model_output()
        control_dict = self.initial_final_level(control_dict)
        control_dict = self.min_max_level(control_dict)
        control_dict = self.error(control_dict)
        control_dict = self.stationary(control_dict)
        control_dict = self.find_stationary_flow(control_dict)
        control_dict = self.mask_basins(control_dict)
        control_dict = self.flow_rate(control_dict)

        self.store_data(data=control_dict, output_path=self.path_control_dict_path)

        return control_dict

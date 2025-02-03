import os
import shutil

import geopandas as gpd
import pandas as pd
import ribasim


class Control:
    def __init__(self, work_dir):
        self.work_dir = work_dir
        self.path_basin_output = os.path.join(work_dir, "results", "basin.arrow")
        self.path_edge_output = os.path.join(work_dir, "results", "flow.arrow")
        self.path_ribasim_toml = os.path.join(work_dir, "ribasim.toml")
        self.path_control_dict_path = os.path.join(work_dir, "results", "output_controle")

    def read_model_output(self):
        df_basin = pd.read_feather(self.path_basin_output)
        df_edge = pd.read_feather(self.path_edge_output)
        model = ribasim.model.Model(filepath=self.path_ribasim_toml)

        self.df_basin = df_basin
        self.df_edge = df_edge
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

        # initial_final_level_df["final_level_within_target"] = (
        #     True  # final level within target level (deviate max 20 cm from streefpeil) is default True ...
        # )
        # initial_final_level_df.loc[
        #     (initial_final_level_df["difference_level"] > 0.2) | (initial_final_level_df["difference_level"] < -0.2),
        #     "final_level_within_target",
        # ] = False  # ... but set to False if the criteria is not met

        # retrieve the geometries
        initial_final_level_df["geometry"] = initial_final_level_df.merge(
            self.model.basin.node.df, on="node_id", suffixes=("", "model_")
        )["geometry"]

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

        # retrieve the geometries
        error_gdf["geometry"] = error_gdf.merge(self.model.basin.node.df, on="node_id", suffixes=("", "model_"))[
            "geometry"
        ]

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
        df_edge = self.df_edge.copy()
        df_edge["time"] = pd.to_datetime(df_edge["time"])  # convert to time column

        df_edge = df_edge.sort_values(by=["time", "edge_id"], ascending=True).copy()  # sort values, just in case
        last_time = df_edge["time"].max()  # retireve max time value
        time_threshold = last_time - pd.Timedelta(hours=n_hours_mean)  # determine the time threshold, likely 24 hours

        df_edge_24h = df_edge[df_edge["time"] >= time_threshold].copy()  # seelct above the threshold

        # Group by 'edge_id' and calculate the average flow rate over the last 24 hours
        df_edge_avg = df_edge_24h.groupby("edge_id", as_index=False).agg(
            {
                "flow_rate": "mean",  # take the mean, as the pumps may not show stationairy results in one timestep
                "from_node_id": "first",  # remains the same for each timestep
                "to_node_id": "first",  # remains the same for each timestep
                "time": "first",  # remains the same for each timestep
            }
        )

        # Merge the geometry from edges_ribasim to df_edge_avg to retrieve the geometries
        edges_ribasim = self.model.edge.df.copy()
        df_edge_avg = df_edge_avg.merge(
            right=edges_ribasim[["from_node_id", "to_node_id", "geometry"]],
            on=["from_node_id", "to_node_id"],
            how="left",
        )

        df_edge_avg = gpd.GeoDataFrame(df_edge_avg).set_crs(crs="EPSG:28992")
        control_dict["flow"] = df_edge_avg

        return control_dict

    def flow_situation(self, control_dict: dict, n_last_hours: int = 10) -> dict:
        """Determine whether the flow conditions are representative for drought-conditions, or for
        flood-conditions. Drought-conditions would see an inflow from the main water system into
        the polders; flood-conditions would see an outflow from the polders into the main water
        system.

        :param control_dict: colection of geopandas-dataframes containing control data.
        :param n_last_hours: number of hours to average over, defaults to 10.

        :type control_dict: dict
        :type n_last_hours: int, optional

        :return: updated collection of geopandas-dataframes
        :rtype: dict
        """
        # create a copy of edge-data to prevent overwriting original data
        df_edge = self.df_edge.copy(deep=True)

        # sort data based on time to select last `n_last_hours` for further processing
        df_edge["time"] = pd.to_datetime(df_edge["time"])
        tc = df_edge["time"].max() - pd.Timedelta(hours=n_last_hours)
        df_edge.sort_values(by=["time", "edge_id"], ascending=True, inplace=True)
        df_edge = df_edge[df_edge["time"] >= tc]

        # group by `edge_id` and calculate the average flow rate over the last `n_last_hours` hours
        grouped = df_edge.groupby("edge_id", as_index=False).agg(
            {
                "flow_rate": "mean",
                "from_node_id": "first",
                "to_node_id": "first",
                "time": "first",
            }
        )

        # merge geometries of edges from Ribasim-model
        rib_edge = self.model.edge.df.copy(deep=True)
        grouped = grouped.merge(
            right=rib_edge[["from_node_id", "to_node_id", "geometry"]], on=["from_node_id", "to_node_id"], how="left"
        )

        # set coordinate system
        grouped = gpd.GeoDataFrame(grouped).set_crs(crs="EPSG:28992")

        # update `control_dict`
        control_dict["inout"] = grouped.copy(deep=True)
        return control_dict

    def flow_main_water_system(self, control_dict: dict, n_last_hours: int = 10) -> dict:
        """Highlight the edges that can be considered part of the main water system and mark them
        as outflow ('afvoer') or inflow ('aanvoer').

        :param control_dict: colection of geopandas-dataframes containing control data.
        :param n_last_hours: number of hours to average over, defaults to 10.

        :type control_dict: dict
        :type n_last_hours: int, optional

        :return: updated collection of geopandas-dataframes
        :rtype: dict
        """
        # create a copy of edge-data to prevent overwriting of original data
        df_edge = self.df_edge.copy(deep=True)

        # select relevant edges
        df_edge = df_edge[
            (df_edge["meta_categorie"] == "hoofdwater")
            | (df_edge["meta_from_node_type"] == "LevelBoundary")
            | (df_edge["meta_to_node_type"] == "LevelBoundary")
        ]

        # sort data based on time to select last `n_last_hours` hours for further processing
        df_edge["time"] = pd.to_datetime(df_edge["time"])
        tc = df_edge["time"].max() - pd.Timedelta(hours=n_last_hours)
        df_edge.sort_values(by=["time", "edge_id"], ascending=True, inplace=True)
        df_edge = df_edge[df_edge["time"] >= tc]

        # group by `edge_id` and calculate the average flow rate over the last `n_last_hours` hours
        grouped = df_edge.groupby("edge_id", as_index=True).agg(
            {
                "flow_rate": "mean",
                "from_node_id": "first",
                "to_node_id": "first",
                "time": "first",
            }
        )

        # merge geometries of edges from Ribasim-model
        rib_edge = self.model.edge.df.copy(deep=True)
        grouped = grouped.merge(
            right=rib_edge[["from_node_id", "to_node_id", "geometry"]], on=["from_node_id", "to_node_id"], how="left"
        )

        # set coordinate system
        grouped = gpd.GeoDataFrame(grouped).set_crs(crs="EPSG:28992")

        # update `control_dict`
        control_dict["inout"] = grouped.copy(deep=True)
        return control_dict

    def store_data(self, data, output_path):
        """Store the control_dict"""
        for key in data.keys():
            data[str(key)].to_file(output_path + ".gpkg", layer=str(key), driver="GPKG", mode="w")

        # copy checks_symbology file from old dir to new dir
        # define path
        output_controle_qlr_path = r"../../../../../Data_overig/QGIS_qlr/output_controle.qlr"

        # delete old .qlr file (overwriting does apparently not work due to permission rights)
        if os.path.exists(os.path.join(self.work_dir, "results", "output_controle.qlr")):
            os.remove(os.path.join(self.work_dir, "results", "output_controle.qlr"))

        # copy .qlr file
        shutil.copy(src=output_controle_qlr_path, dst=os.path.join(self.work_dir, "results", "output_controle.qlr"))

        return

    def run_all(self):
        control_dict = self.read_model_output()
        control_dict = self.initial_final_level(control_dict)
        # control_dict = self.min_max_level(control_dict)
        control_dict = self.error(control_dict)
        control_dict = self.stationary(control_dict)
        # control_dict = self.find_stationary_flow(control_dict)
        self.store_data(data=control_dict, output_path=self.path_control_dict_path)

        return control_dict

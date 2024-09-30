import os
import shutil

import geopandas as gpd
import pandas as pd
import ribasim

class Control:
    def __init__(self, work_dir):
        self.work_dir = work_dir
        self.path_basin_output = os.path.join(work_dir, "results", "basin.arrow")
        self.path_ribasim_toml = os.path.join(work_dir, "ribasim.toml")
        self.path_control_dict_path = os.path.join(work_dir, "results", "output_controle")

    def read_model_output(self):
        df_basin = pd.read_feather(self.path_basin_output)
        model = ribasim.model.Model(filepath=self.path_ribasim_toml)

        self.df_basin = df_basin
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

        initial_final_level_df[
            "final_level_within_target"
        ] = True  # final level within target level (deviate max 20 cm from streefpeil) is default True ...
        initial_final_level_df.loc[
            (initial_final_level_df["difference_level"] > 0.2) | (initial_final_level_df["difference_level"] < -0.2),
            "final_level_within_target",
        ] = False  # ... but set to False if the criteria is not met

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

        min_basin_level_df = basin_level.drop_duplicates(subset="node_id", keep="first").reset_index(
            drop=True
        )  # pick the FIRST sample, which are the minimum water levels
        min_max_basin_level_df = min_basin_level_df[["node_id", "level", "time"]]
        min_max_basin_level_df = min_max_basin_level_df.rename(columns={"level": "min_level", "time": "min_level_time"})

        max_basin_level_df = basin_level.drop_duplicates(subset="node_id", keep="last").reset_index(
            drop=True
        )  # pick the LAST sample, which are the maximum water levels
        max_basin_level_df = max_basin_level_df[["level", "time"]]
        min_max_basin_level_df["max_level"] = max_basin_level_df["level"]
        min_max_basin_level_df["max_level_time"] = max_basin_level_df["time"]

        # retrieve the geometries
        min_max_basin_level_df["geometry"] = min_max_basin_level_df.merge(
            self.model.basin.node.df, on="node_id", suffixes=("", "model_")
        )["geometry"]

        min_max_basin_level_df = gpd.GeoDataFrame(min_max_basin_level_df, geometry="geometry")

        control_dict["min_max_level"] = min_max_basin_level_df

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

    # def stationary(self, control_dict):
    #     def is_stationary(group):
    #         group = group.sort_values(by='time')
    #         #extract the required levels of the -4:-1 timestamps
    #         last_values = group['level'].iloc[-4:-1]
    #         average_last_values = last_values.mean()
    #         actual_last_value = group['level'].iloc[-1]

    #         #calculate the deviation
    #         deviation = abs((actual_last_value - average_last_values) / average_last_values)

    #         #determine if it's stationary
    #         stationary = deviation <= 0.02
    #         return stationary

    #     stationary_gdf = gpd.GeoDataFrame()
    #     stationary_gdf['node_id'] = self.df_basin.node_id
    #     stationary_gdf['stationary'] = self.df_basin.groupby('node_id').apply(is_stationary).reset_index(level=0, drop=True)
    #     stationary_gdf = stationary_gdf.dropna()
    #     #retrieve the geometries
    #     stationary_gdf['geometry'] = pd.merge(left = stationary_gdf,
    #                                           right = self.model.basin.node.df,
    #                                           on = 'node_id',
    #                                           suffixes = ('', 'model_'))['geometry']

    #     control_dict['stationary'] = gpd.GeoDataFrame(stationary_gdf, geometry = 'geometry')

    #     return control_dict

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

            # Determine if it's stationary (deviation <= .11 cm)
            stationary = deviation <= 0.001
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

    # def inspect_individual_basins(self, data):

    def store_data(self, data, output_path):
        """Store the control_dict"""
        for key in data.keys():
            print(key)
            data[str(key)].to_file(output_path + ".gpkg", layer=str(key), driver="GPKG")

        # copy checks_symbology file from old dir to new dir
        output_controle_qlr_path = r"../../../../../Data_overig/QGIS_qlr/output_controle.qlr"
        shutil.copy(src=output_controle_qlr_path, dst=os.path.join(self.work_dir, "results", "output_controle.qlr"))

        return

    def run_all(self):
        control_dict = self.read_model_output()
        control_dict = self.initial_final_level(control_dict)
        control_dict = self.min_max_level(control_dict)
        control_dict = self.error(control_dict)
        control_dict = self.stationary(control_dict)
        self.store_data(data=control_dict, output_path=self.path_control_dict_path)

        return control_dict

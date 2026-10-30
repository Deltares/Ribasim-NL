import logging
import shutil
from pathlib import Path
from typing import cast

import geopandas as gpd
import pandas as pd
import xarray as xr

from ribasim_nl import Model
from ribasim_nl.steady_state_regression import (
    steady_state_results,
)

logger = logging.getLogger(__name__)

LEVEL_METRICS = frozenset({"initial_final_level", "min_max_level", "error", "steady_state"})
SUPPLY_METRICS = frozenset({"aanvoer_areas", "aanvoer_afvoer_basin_nodes", "aanvoer_afvoer_pumps", "aanvoer_outlets"})
FLOW_METRICS = frozenset({"mean_flow"})

STATIC_FORCING_METRICS = LEVEL_METRICS | SUPPLY_METRICS | FLOW_METRICS | {"afvoer_mask"}
AFVOER_METRICS = LEVEL_METRICS | FLOW_METRICS | {"afvoer_mask", "flow_rate"}
DYNAMIC_FORCING_METRICS = SUPPLY_METRICS | FLOW_METRICS | {"water_level_bounds", "error_bounds"}


class Control:
    """Export model-result metrics for QGIS and validate computed conditions.

    Use :meth:`run` with one of the metric sets defined in this module, or a
    custom set of metric names, to write selected metrics to
    ``results/output_controle.gpkg``.
    """

    ds_basin: xr.Dataset | None = None
    ds_link: xr.Dataset | None = None
    model: Model | None = None

    def __init__(
        self,
        qlr_path: str | Path | None = None,
        work_dir: str | Path | None = None,
        ribasim_toml: str | Path | None = None,
    ) -> None:
        """Configure paths to a model's results and optional QGIS layer definition."""
        if ribasim_toml is not None:
            self.path_ribasim_toml = ribasim_toml
            self.work_dir = Path(ribasim_toml).parent
        elif work_dir is not None:
            self.path_ribasim_toml = Path(work_dir) / "ribasim.toml"
            self.work_dir = work_dir
        else:
            raise ValueError("provide either work_dir or ribasim_toml")

        if qlr_path is None:
            self.qlr_path = Path(__file__).parent.joinpath("data", "output_controle.qlr")
        else:
            self.qlr_path = qlr_path
        self.path_basin_output = Path(self.work_dir) / "results" / "basin.nc"
        self.path_link_output = Path(self.work_dir) / "results" / "flow.nc"
        self.path_control_dict_path = Path(self.work_dir) / "results" / "output_controle"

    def read_model_output(self):
        """Load Basin and flow output together with the corresponding model."""
        ds_basin = xr.open_dataset(self.path_basin_output)
        ds_link = xr.open_dataset(self.path_link_output)
        model = Model.read(self.path_ribasim_toml)

        self.ds_basin = ds_basin
        self.ds_link = ds_link
        self.model = model
        control_dict = {}

        return control_dict

    def _loaded(self) -> tuple[xr.Dataset, xr.Dataset, Model]:
        """Return the Basin and flow output and the model, asserting that `read_model_output` loaded them."""
        assert self.ds_basin is not None and self.ds_link is not None and self.model is not None, (
            "Call `read_model_output` first"
        )
        return self.ds_basin, self.ds_link, self.model

    def _basin_node_df(self) -> gpd.GeoDataFrame:
        """Return the Basin node table of the loaded model."""
        _, _, model = self._loaded()
        return model.basin.node.df

    @staticmethod
    def _to_bool_series(series: pd.Series) -> pd.Series:
        """Convert mixed True/False representations to booleans without pandas downcasting warnings."""
        normalized = series.astype("string").str.strip().str.lower()
        return normalized.map({"true": True, "false": False, "1": True, "0": False}).fillna(False).astype(bool)

    def initial_final_level(self, control_dict):
        ds_basin, _, _ = self._loaded()
        initial_level = ds_basin["level"].isel(time=0).to_series().rename("initial_level")
        final_level = ds_basin["level"].isel(time=-1).to_series().rename("final_level")
        initial_final_level_df = pd.concat([initial_level, final_level], axis=1).reset_index()
        initial_final_level_df["difference_level"] = (
            initial_final_level_df["initial_level"] - initial_final_level_df["final_level"]
        )

        # Retrieve the geometries
        initial_final_level_df = initial_final_level_df.merge(
            self._basin_node_df(), on="node_id", suffixes=("", "model_")
        )

        initial_final_level_df = gpd.GeoDataFrame(initial_final_level_df, geometry="geometry")

        # add to the dictionary
        control_dict["initial_final_level"] = initial_final_level_df

        return control_dict

    def min_max_level(self, control_dict):
        ds_basin, _, _ = self._loaded()
        basin_level = ds_basin["level"]
        min_basin_level = pd.concat(
            [
                basin_level.min(dim="time").to_series().rename("min_level"),
                basin_level.idxmin(dim="time").to_series().rename("min_level_time"),
            ],
            axis=1,
        ).reset_index()

        # retrieve the geometries
        min_basin_level = min_basin_level.merge(
            self._basin_node_df()[["geometry"]], left_on="node_id", right_index=True, suffixes=("", "model_")
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
            self._basin_node_df()[["geometry"]], left_on="node_id", right_index=True, suffixes=("", "model_")
        )

        # convert to geopandas
        min_basin_level = gpd.GeoDataFrame(min_basin_level, geometry="geometry")
        max_basin_level = gpd.GeoDataFrame(max_basin_level, geometry="geometry")

        control_dict["min_level"] = min_basin_level
        control_dict["max_level"] = max_basin_level

        return control_dict

    def error(self, control_dict):
        ds_basin, _, _ = self._loaded()
        relative_error = ds_basin["relative_error"]
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

        error_gdf = error_gdf.merge(self._basin_node_df(), on="node_id", suffixes=("", "model_"))

        error_gdf = gpd.GeoDataFrame(error_gdf, geometry="geometry")

        control_dict["error"] = error_gdf

        return control_dict

    def steady_state(self, control_dict):
        ds_basin, _, _ = self._loaded()
        steady_state_gdf = steady_state_results(ds_basin["level"], ds_basin["storage"])

        # Retrieve the geometries
        steady_state_gdf["geometry"] = steady_state_gdf.merge(
            self._basin_node_df(), on="node_id", suffixes=("", "model_")
        )["geometry"]

        control_dict["steady_state"] = gpd.GeoDataFrame(steady_state_gdf, geometry="geometry")

        return control_dict

    def validate_result_conditions(self, *, allowed_non_steady_state_node_ids: set[int] | None = None) -> None:
        """Validate Basin result conditions after exporting output-control data.

        All detected violations are logged with their Basin node IDs before one
        exception is raised, so a single run reports every condition to fix.
        """
        ds_basin, _, _ = self._loaded()
        result = steady_state_results(ds_basin["level"], ds_basin["storage"]).set_index("node_id")
        allowed_non_steady_state_node_ids = allowed_non_steady_state_node_ids or set()
        issues: list[str] = []

        missing_allowed_node_ids = allowed_non_steady_state_node_ids.difference(result.index)
        issues.extend(
            f"allowed non-steady-state Basin node_id={node_id} is missing from output"
            for node_id in sorted(missing_allowed_node_ids)
        )
        issues.extend(
            f"Basin node_id={node_id} reached zero storage" for node_id in result.index[~result["has_positive_storage"]]
        )

        non_steady_state = result.loc[
            ~result["level_steady_state"] & ~result.index.isin(list(allowed_non_steady_state_node_ids))
        ]
        for node_id, row in non_steady_state.iterrows():
            issues.append(f"Basin node_id={node_id} is not in steady state (deviation={row.level_deviation:.6g} m)")

        for issue in issues:
            logger.error(issue)
        if issues:
            raise AssertionError(f"Model result conditions failed for {len(issues)} Basin(s); see logged issues.")

    def find_mean_flow(self, control_dict):
        _, ds_link, model = self._loaded()
        df_link_avg = pd.concat(
            [
                abs(ds_link["flow_rate"]).mean(dim="time").to_series().rename("flow_rate"),
                ds_link["from_node_id"].to_series().rename("from_node_id"),
                ds_link["to_node_id"].to_series().rename("to_node_id"),
            ],
            axis=1,
        ).reset_index()
        df_link_avg["time"] = pd.Timestamp(ds_link["time"].values[0])
        print("MAX VALUE DF_LINK", df_link_avg["flow_rate"].max())

        # Merge the geometry from links_ribasim to df_link_avg to retrieve the geometries
        links_ribasim = model.link.df.copy()
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
        _, _, model = self._loaded()
        assert model.basin.area.df is not None
        aanvoer_areas = model.basin.area.df.copy(deep=True)
        aanvoer_areas["meta_aanvoer"] = self._to_bool_series(aanvoer_areas["meta_aanvoer"])

        aanvoer_areas = aanvoer_areas.loc[
            aanvoer_areas["meta_aanvoer"]
        ]  # only select the rows where meta_aanvoer == True

        aanvoer_areas = gpd.GeoDataFrame(aanvoer_areas[["node_id", "geometry"]], crs="EPSG:28992")
        control_dict["aanvoer_areas"] = aanvoer_areas

        return control_dict

    def water_aanvoer_afvoer_basin_nodes(self, control_dict):
        """Retrieve the nodes (points) of the wateraanvoer gebieden basin, as well as afvoer basin nodes"""
        _, _, model = self._loaded()
        assert model.basin.area.df is not None
        aanvoer_areas = model.basin.area.df.copy(deep=True)
        basin_nodes = self._basin_node_df().copy(deep=True).reset_index()
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

    def water_aanvoer_afvoer_pumps(self, control_dict):
        """Retrieve the nodes (points) of the wateraan- and afvoer pumps"""
        _, _, model = self._loaded()
        assert model.pump.static.df is not None
        aanvoer_afvoer_pumps = model.pump.static.df.copy(deep=True)
        aanvoer_afvoer_pumps_nodes = model.pump.node.df.copy(deep=True).reset_index()
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
        _, _, model = self._loaded()
        assert model.outlet.static.df is not None
        aanvoer_outlets = model.outlet.static.df.copy(deep=True)
        outlet_nodes = model.outlet.node.df.copy(deep=True).reset_index()
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
        if "meta_gestuwd" in self._basin_node_df().columns:
            control_dict["mask_afvoer"] = self._basin_node_df()[
                self._basin_node_df()["meta_gestuwd"] == "False"
            ].reset_index()[["node_id", "geometry"]]

        return control_dict

    def flow_rate(self, control_dict):
        _, ds_link, model = self._loaded()
        flow_rate = ds_link["flow_rate"].isel(time=-1).to_dataframe().reset_index()
        link_df = model.link.df.reset_index()
        link_df = link_df.merge(flow_rate[["link_id", "flow_rate"]], on="link_id", how="left").set_index("link_id")
        control_dict["flow_rate"] = link_df

        return control_dict

    def store_data(self, data, output_path) -> None:
        """Store the control_dict"""
        for key in data:
            data[str(key)].to_file(Path(output_path).with_suffix(".gpkg"), layer=str(key), driver="GPKG", mode="w")

        # copy checks_symbology file from old dir to new dir
        # delete old .qlr file (overwriting does apparently not work due to permission rights)
        qlr_dst = Path(self.work_dir) / "results" / "output_controle.qlr"
        if qlr_dst.exists():
            qlr_dst.unlink()

        # copy .qlr file
        shutil.copy(src=self.qlr_path, dst=qlr_dst)

        return

    def run(
        self,
        *,
        metrics: set[str] | frozenset[str] = DYNAMIC_FORCING_METRICS,
        skip_time_steps: int = 0,
        autofill_missing_data: bool = False,
    ) -> dict[str, object]:
        """Compute selected metrics, export QGIS data, and return the collected results.

        Parameters
        ----------
        metrics
            Metric names to compute. Use ``STATIC_FORCING_METRICS``,
            ``AFVOER_METRICS``, or ``DYNAMIC_FORCING_METRICS`` for the
            established workflows.
        skip_time_steps
            Dynamic-forcing spin-up time steps excluded from level bounds.
        autofill_missing_data
            Permit ``error_bounds`` to calculate missing water-level bounds.
        """
        unknown_metrics = metrics.difference(STATIC_FORCING_METRICS | AFVOER_METRICS | DYNAMIC_FORCING_METRICS)
        if unknown_metrics:
            msg = f"Unknown output-control metrics: {sorted(unknown_metrics)}."
            raise ValueError(msg)

        control_dict = self.read_model_output()
        if "initial_final_level" in metrics:
            control_dict = self.initial_final_level(control_dict)
        if "min_max_level" in metrics:
            control_dict = self.min_max_level(control_dict)
        if "error" in metrics:
            control_dict = self.error(control_dict)
        if "steady_state" in metrics:
            control_dict = self.steady_state(control_dict)
        if "water_level_bounds" in metrics:
            control_dict = self.water_level_bounds(control_dict, skip_time_steps=skip_time_steps)
        if "error_bounds" in metrics:
            control_dict = self.error_bounds(control_dict, autofill_missing_data=autofill_missing_data)
        if "mean_flow" in metrics:
            control_dict = self.find_mean_flow(control_dict)
        if "aanvoer_areas" in metrics:
            control_dict = self.water_aanvoer_areas(control_dict)
        if "aanvoer_afvoer_basin_nodes" in metrics:
            control_dict = self.water_aanvoer_afvoer_basin_nodes(control_dict)
        if "aanvoer_afvoer_pumps" in metrics:
            control_dict = self.water_aanvoer_afvoer_pumps(control_dict)
        if "aanvoer_outlets" in metrics:
            control_dict = self.water_aanvoer_outlets(control_dict)
        if "afvoer_mask" in metrics:
            control_dict = self.mask_basins(control_dict)
        if "flow_rate" in metrics:
            control_dict = self.flow_rate(control_dict)

        self.store_data(data=control_dict, output_path=self.path_control_dict_path)
        return control_dict

    def water_level_bounds(self, control_dict: dict[str, object], skip_time_steps: int = 0) -> dict[str, object]:
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
        ds_basin, _, _ = self._loaded()
        start_time = pd.Timestamp(ds_basin["time"].values[skip_time_steps])
        level = ds_basin["level"].sel(time=slice(start_time, None))

        # minimum water level
        min_basin_level = level.min(dim="time").to_series().rename("level").reset_index(drop=False)

        # maximum water level
        max_basin_level = level.max(dim="time").to_series().rename("level").reset_index(drop=False)

        # collect analysed data in GeoDataFrame
        gdf_min_basin_level = gpd.GeoDataFrame(
            min_basin_level.merge(self._basin_node_df(), on="node_id", suffixes=("", "model_")), geometry="geometry"
        )
        gdf_max_basin_level = gpd.GeoDataFrame(
            max_basin_level.merge(self._basin_node_df(), on="node_id", suffixes=("", "model_")), geometry="geometry"
        )
        control_dict.update(
            {
                "min_basin_level": gdf_min_basin_level,
                "max_basin_level": gdf_max_basin_level,
            }
        )

        # return updated analysed data collector
        return control_dict

    def error_bounds(self, control_dict: dict[str, object], autofill_missing_data: bool = True) -> dict[str, object]:
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
        ds_basin, _, _ = self._loaded()
        # validate available analysed data
        _keys = "min_basin_level", "max_basin_level"
        if not all(k in control_dict for k in _keys):
            logger.info(f"Water level bounds not yet determined and `autofill_missing_data={autofill_missing_data}`:")
            if autofill_missing_data:
                logger.info("Water level bounds auto-filled: Default settings used.")
                control_dict = self.water_level_bounds(control_dict)
            else:
                _data = {k: control_dict.get(k) for k in _keys}
                msg = f"Not all required data in `control_dict` and autofill disabled: {_data}"
                raise ValueError(msg)

        # get water level bounds data
        min_basin_level = cast(gpd.GeoDataFrame, control_dict["min_basin_level"])
        max_basin_level = cast(gpd.GeoDataFrame, control_dict["max_basin_level"])

        # initial water level is considered the target level
        initial_basin_level = ds_basin["level"].isel(time=0).to_series().rename("level").reset_index()

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
        gdf_min_basin_level = gpd.GeoDataFrame(
            min_difference_level.merge(self._basin_node_df(), on="node_id", suffixes=("", "model_")),
            geometry="geometry",
        )
        gdf_max_basin_level = gpd.GeoDataFrame(
            max_difference_level.merge(self._basin_node_df(), on="node_id", suffixes=("", "model_")),
            geometry="geometry",
        )
        control_dict.update(
            {
                "error_min_basin_level": gdf_min_basin_level,
                "error_max_basin_level": gdf_max_basin_level,
            }
        )

        # return updated analysed data collector
        return control_dict

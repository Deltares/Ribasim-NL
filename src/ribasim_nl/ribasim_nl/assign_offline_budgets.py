"""Assign offline MODFLOW-MetaSWAP budgets (LHM zarr or local IDF files) to Ribasim Basin nodes."""

from pathlib import Path

import geopandas as gpd
import imod
import numpy as np
import pandas as pd
import shapely
import xarray as xr
from ribasim import Model
from tqdm import tqdm
from xarray.core.dataarray import DataArray
from xarray.core.dataset import Dataset

from ribasim_nl.assign_fractions_from_budgets import assign_fractions_from_budgets


def _crop_to_gdf(da: "xr.DataArray | xr.Dataset", gdf: gpd.GeoDataFrame) -> DataArray | Dataset:
    """Crop a DataArray or Dataset to a gdf.extent (total_bounds)

    Why? As LHM covers NL and we often compute budgets for 1 authority only.

    """
    xmin, ymin, xmax, ymax = gdf.total_bounds

    dx = abs(float(da.x[1] - da.x[0]))
    dy = abs(float(da.y[1] - da.y[0]))

    xmin, ymin, xmax, ymax = xmin - dx, ymin - dy, xmax + dx, ymax + dy

    return da.sel(
        x=slice(xmin, xmax) if da.x[0] < da.x[-1] else slice(xmax, xmin),
        y=slice(ymin, ymax) if da.y[0] < da.y[-1] else slice(ymax, ymin),
    )


def _compute_budgets_per_basin(budgets: xr.Dataset, basin_mask: xr.DataArray, nodata=-999) -> pd.DataFrame:
    """Sum all modflow budgets per basin_id over a basin_mask."""
    print(f"sum budgets {list(budgets.data_vars)} rasters to basins")

    if basin_mask.dims != ("x", "y"):
        basin_mask = basin_mask.transpose("x", "y")

    var_names = list(budgets.data_vars)
    times = budgets.time.values
    nt = len(times)
    nv = len(var_names)

    mask = basin_mask.values.reshape(-1)
    valid = np.isfinite(mask) & (mask != nodata)

    ids = mask[valid].astype(np.int64)
    unique_ids, inv = np.unique(ids, return_inverse=True)
    nb = len(unique_ids)

    result = np.zeros((nt, nb, nv), dtype=np.float64)

    for v, var_name in enumerate(tqdm(var_names, desc="Variables")):
        data = budgets[var_name]

        if data.dims != ("time", "x", "y"):
            data = data.transpose("time", "x", "y")

        arr = data.values.reshape(nt, -1)[:, valid]
        arr = np.where(np.isfinite(arr), arr, 0.0)

        for t in range(nt):
            result[t, :, v] = np.bincount(inv, weights=arr[t], minlength=nb)

    index = pd.MultiIndex.from_product([unique_ids, times], names=["node_id", "time"])

    df = pd.DataFrame(
        result.transpose(1, 0, 2).reshape(nb * nt, nv),
        index=index,
        columns=var_names,
    ).sort_index()

    return df


class AssignOfflineBudgets:
    def __init__(
        self,
        budgets: Path | str | xr.Dataset,
    ) -> None:
        """Assign offline budgets from MODFLOW-MetaSWAP budget files.

        Parameters
        ----------
        budgets : Path | str | xr.Dataset
            Zarr store directory with MODFLOW-MetaSWAP budgets, or an xarray Dataset
        """
        if not isinstance(budgets, xr.Dataset):
            budgets = Path(budgets)
            if not budgets.exists():
                raise FileNotFoundError(
                    f"You can't compute budgets if you don't have the budgets in a Zarr store: {budgets}"
                    "Download a copy"
                    "Alternatively go to: https://github.com/Deltares/Ribasim-NL/blob/main/scripts/add_lhm_budgets/get_data_LHM_run.py to see how you can create one."
                )
        self.budgets = budgets
        self.surface_runoff_budget_keys: set[str] = {"bdgqrun_m3d"}
        self.secondary_budget_keys: set[str] = {
            "bdgriv_sys2",
            "bdgriv_sys3",
            "bdgriv_sys6",
            "bdgdrn_sys1",
            "bdgdrn_sys2",
            "bdgdrn_sys3",
            "bdgpssw_m3d",
        }
        self.primary_budget_keys: set[str] = {"bdgriv_sys1", "bdgriv_sys4", "bdgriv_sys5"}
        self.secondary_labels: set[str] = {"bergend"}
        self.primary_labels: set[str] = {"hoofdwater", "doorgaand"}

    def compute_budgets(
        self,
        model: Model | Path | str,
        basin_split: str = "area",
        basin_subtype: str = "state",
        basin_metacol: str = "meta_categorie",
        primary_values: set[str] | None = None,
        secondary_values: set[str] | None = None,
        primary_budgets: set[str] | None = None,
        secondary_budgets: set[str] | None = None,
        surface_runoff_budgets: set[str] | None = None,
        assign_fractions: bool = False,
        fraction_prefix: str | None = None,
    ) -> tuple[Model, pd.DataFrame]:
        """Compute budgets for Ribasim model.

        MODFLOW/MetaSWAP budgets for LHM are computed in the following scheme and are expected in unit [m3/day]

        RIV-package
        sys1: primary system
        sys2: secondary system
        sys3: tertiary system
        sys4: main system; layer 1
        sys5: main system; layer 2
        sys6: boil's / well's

        DRN-package
        sys1: tube drainage
        sys2: ditch drainage
        sys3: OLF

        MetaSWAP budgets
        qrunm3: OLF via MetaSWAP
        psswm3: irrigation from surface water

        For the Ribasim schematization we distinguish:
          - Primary system for all basins
          - Secondary system in basins other than the main river system

        For drainage an infiltration input based on LHM-output budgets, we distubute the LHM-systems in the following matter:
         - Primary system (drainage/infiltration) -> RIV-sys 1 + 4 + 5
         - Secondary system (drainage/infiltration) -> RIV-sys 2 + 3 + 6, DRN-sys 1 + 2 + 3, psswm3
         - Secondary system (surface_runoff) -> qrunm3

        Parameters
        ----------
        model : Model | Path | str
            Ribasim Model
        basin_split : str, optional
            Table to split basins, by default "area"
        basin_subtype : str, optional
            optional table to find basin_metacol if not in node-table, by default "state"
        basin_metacol : str, optional
            colomn to contain primary and secondary values, by default "meta_categorie"
        primary_values: set[str]
            set of values in basin_metacol that represent primary basins, default = {'hoofdwater', 'doorgaand`}
        secondary_values: set[str]
            set of values in basin_metacol that represent secondary basins, default = {`bergend`}
        primary_budgets : set[str], optional
            set of budgets that are to be summed to primary drainage/infiltration,
             by default {"bdgriv_sys1", "bdgriv_sys4", "bdgriv_sys5"}
        secondary_budgets : set[str], optional
            set of budgets that are to be summed to secondary drainage/infiltration,
             by default {"bdgriv_sys2", "bdgriv_sys3", "bdgriv_sys6", "bdgdrn_sys1", "bdgdrn_sys2", "bdgdrn_sys3", "bdgpssw_m3d"}
        surface_runoff_budgets: set[str], optional
            set of budgets that are to be summed to secondary surface_runoff
             by default {"bdgqrun_m3d"}
        assign_fractions: bool, optional
             if True, fractions from budgets will be calculated and assigned to model.basin.concentration.df, default False
        fraction_prefix: str, optional
             if assign_fractions, then user is to define a fraction prefix here, else it kan be kept None. default None

        Returns
        -------
        Model, pd.DataFrame
            Model and with MODFLOW-MetaSWAP budgets per node_id and timestamp. These can be used for verification and/or to compute fraction tracer/concentrations
        """
        # Synchronize LHM budget and model files
        if surface_runoff_budgets is not None:
            self.surface_runoff_budget_keys = surface_runoff_budgets
        if secondary_budgets is not None:
            self.secondary_budget_keys = secondary_budgets
        if primary_budgets is not None:
            self.primary_budget_keys = primary_budgets
        if secondary_values is not None:
            self.secondary_labels = secondary_values
        if primary_values is not None:
            self.primary_labels = primary_values
        print("read and validate budgets and model")
        budgets, model = self._sync_files(model)  # read model and budgets form zarr-store
        self._validate_budgets(
            budgets, self.primary_budget_keys, self.secondary_budget_keys, self.surface_runoff_budget_keys
        )  # check if all data-variables are present

        # Split into primary and secondary basin definition
        print("split basins into primary and secondary")
        primary_basin_definition, secondary_basin_definition = self._split_basin_definitions(
            model,
            basin_split=basin_split,
            basin_subtype=basin_subtype,
            basin_metacol=basin_metacol,
            primary_values=primary_values,
            secondary_values=secondary_values,
        )
        print("rasterize basins to masks")
        primary_basin_mask = imod.prepare.rasterize(
            primary_basin_definition,
            column="node_id",
            like=_crop_to_gdf(budgets["bdgriv_sys1"].isel(time=0, drop=True), primary_basin_definition),
            fill=-999,
            dtype=np.int32,
        )
        secondary_basin_mask = imod.prepare.rasterize(
            secondary_basin_definition,
            column="node_id",
            like=_crop_to_gdf(budgets["bdgriv_sys1"].isel(time=0, drop=True), secondary_basin_definition),
            fill=-999,
            dtype=np.int32,
        )
        print("compute budgets per basin")
        primary_budgets_df = (
            _compute_budgets_per_basin(
                _crop_to_gdf(budgets[list(self.primary_budget_keys)], primary_basin_definition),
                primary_basin_mask,
            )
            / 86400
        )

        secondary_budgets_df = (
            _compute_budgets_per_basin(
                _crop_to_gdf(
                    budgets[list(self.secondary_budget_keys | self.surface_runoff_budget_keys)],
                    secondary_basin_definition,
                ),
                secondary_basin_mask,
            )
            / 86400
        )

        print("add budgets to drainage/infiltration and surface_runoff columns")
        # concat all budgets so we can return those for verification
        budgets_df = pd.concat([primary_budgets_df, secondary_budgets_df]).sort_index()

        # sum all budgets (columns) and create drainage and infiltration series
        # Group-sum to ensure unique (node_id, time) index before mapping
        summed_budgets = pd.Series(
            budgets_df[list(self.primary_budget_keys | self.secondary_budget_keys)]
            .groupby(level=["node_id", "time"])
            .sum()
            .sum(axis=1)
        )
        drainage = summed_budgets.clip(
            upper=0
        ).abs()  # all <0 is drainage. Take absolute its a positive term in RIBASIM
        infiltration = summed_budgets.clip(
            lower=0
        )  # alles > 0 (infiltratie is in modflow, ontrekking uit ribasim, maar in ribasim positief teken)
        surface_runoff = (
            budgets_df[list(self.surface_runoff_budget_keys)]
            .groupby(level=["node_id", "time"])
            .sum()
            .sum(axis=1)
            .clip(upper=0)
            .abs()
        )  # assume surface_runoff can't be <0 in RIBASIM. And negative budgets in MODFLOW-MetaSWAP are positive terms in Ribasim

        # update basin drainage and infiltration
        assert model.basin.time.df is not None
        idx = pd.MultiIndex.from_frame(model.basin.time.df[["node_id", "time"]])
        model.basin.time.df["drainage"] = idx.map(drainage)  # pyrefly: ignore[bad-argument-type]
        model.basin.time.df["infiltration"] = idx.map(infiltration)  # pyrefly: ignore[bad-argument-type]
        model.basin.time.df["surface_runoff"] = idx.map(surface_runoff)  # pyrefly: ignore[bad-argument-type]

        # assign fractions from budgets if user wants to
        if assign_fractions:
            secondary_basin_ids = secondary_basin_definition.node_id.to_numpy()
            primary_basin_ids = primary_basin_definition.node_id.to_numpy()
            if fraction_prefix is None:
                raise ValueError("fraction_prefix can't be None if assign_fractions is True")
            assign_fractions_from_budgets(
                model=model,
                budgets_df=budgets_df,
                primary_budgets=self.primary_budget_keys,
                secondary_budgets=self.secondary_budget_keys,
                surface_runoff_budgets=self.surface_runoff_budget_keys,
                primary_basin_ids=primary_basin_ids,
                secondary_basin_ids=secondary_basin_ids,
                prefix=fraction_prefix,
            )

        return model, budgets_df

    def _sync_files(
        self,
        model: Model | Path | str,
    ) -> tuple[xr.Dataset, Model]:
        """Synchronize files from the CloudStorage. Note, this is Ribasim-NL only and requires the ribasim-nl module

        Parameters
        ----------
        model : Model | Path | str
            Ribasim model or path

        Returns
        -------
        tuple[xr.Dataset, Model]
            Budgets and Ribasim model
        """
        # Read the ribasim model if needed
        if not isinstance(model, Model):
            model = Model.read(model)

        if isinstance(self.budgets, xr.Dataset):
            budgets = self.budgets
        else:
            try:
                budgets = xr.open_zarr(str(self.budgets)).sel(time=slice(model.starttime, model.endtime))
            except Exception as e:
                print("ERROR: you have to process your budgets to a zarr-storage first!")
                print(
                    "GoTo: https://github.com/Deltares/Ribasim-NL/blob/main/scripts/add_lhm_budgets/get_data_LHM_run.py to see how you can create one."
                )
                raise (e)

        return budgets, model

    def _validate_budgets(self, budgets, primary_budgets, secondary_budgets, surface_runoff_budgets) -> None:
        """Validate if all budgets are available as data vars in budgets-file"""
        expected = primary_budgets | secondary_budgets | surface_runoff_budgets
        missing = expected - set(budgets.data_vars)

        if missing:
            raise ValueError(
                f"budgets {missing} not supplied in budgets-file. Please check {self.budgets}",
            )

    def _transpose_basin_definition_polygons(
        self, basin_definition_primary: gpd.GeoDataFrame, basin_definition_secundary: gpd.GeoDataFrame
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Returns basin_difinition_out with index of basin_definition_in that intersect the basin_definition_out polygons

        Args:
            basin_definition_primary (gpd.GeoDataFrame): Basin definition with (multi) polygons
            basin_definition_secundary (gpd.GeoDataFrame): Basin definition with (multi) polygons

        Returns
        -------
            tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]: basin_definition_secundary geometry with primary based index, Basin definition with
            polygons without any intersection for primary and secundary basins
        """
        tree = shapely.STRtree(basin_definition_secundary["geometry"])
        index_in, index_out = tree.query(basin_definition_primary.representative_point(), predicate="intersects")
        index_in = basin_definition_primary.index[index_in]
        index_out = basin_definition_secundary.index[index_out]
        # evaluate non-matching items
        # secondary basins with non matching primary basins
        index_undefined_secundary = basin_definition_secundary.index[
            ~np.isin(basin_definition_secundary.index, index_out)
        ]
        undefined_secondary_basins = basin_definition_secundary.loc[index_undefined_secundary]
        # primary basins with non matching secundary basins
        # strategy: add to output df (fall true logic), and add to undefined output argument
        index_undefined_primary = basin_definition_primary.index[~np.isin(basin_definition_primary.index, index_in)]
        undefined_primary_basins = basin_definition_primary.loc[index_undefined_primary]
        # prepare main output df
        basin_definition_secundary = basin_definition_secundary.loc[index_out]
        basin_definition_secundary = basin_definition_secundary.set_index([index_in])
        basin_definition_primary_out = pd.concat(
            [basin_definition_secundary, basin_definition_primary.loc[index_undefined_primary]]
        )
        return basin_definition_primary_out, undefined_primary_basins, undefined_secondary_basins

    def _fill_basin_definition_from_points(
        self,
        basin_definition: gpd.GeoDataFrame,
        nodes: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """
        Returns basin_definition with indices of basins located within each polygon.

        Parameters
        ----------
        basin_definition : gpd.GeoDataFrame
            Basin definition with (multi)polygons.
        nodes : gpd.GeoDataFrame
            Ribasim basin nodes.

        Returns
        -------
        gpd.GeoDataFrame
            basin_definition with indices derived from the underlying Ribasim basins.
        """
        tree = shapely.STRtree(basin_definition["geometry"])
        (
            index_nodes,
            index_basin_definition,
        ) = tree.query(nodes["geometry"], predicate="within")  #'overlaps', 'within'
        index_basin_definition = basin_definition.index[index_basin_definition]
        index_nodes = nodes.index[index_nodes]
        basin_definition = basin_definition.loc[index_basin_definition]
        return basin_definition.set_index(index_nodes)

    def _split_basin_definition(
        self,
        basin_definition: gpd.GeoDataFrame,
        nodes: gpd.GeoDataFrame,
        basin_metacol: str,
        primary_values: set[str],
        secondary_values: set[str],
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Split a basin definition into primary and secondary basins based on a metadata column.

        The classification is derived from values in the specified metadata column of the
        Ribasim basin nodes. Basins are assigned to either the primary or secondary group
        depending on whether their metadata value matches the provided sets.

        Parameters
        ----------
        basin_definition : gpd.GeoDataFrame
            GeoDataFrame containing basin geometries (polygons or multipolygons).
        nodes : gpd.GeoDataFrame
            GeoDataFrame containing Ribasim basin nodes with metadata attributes.
        basin_metacol : str
            Name of the column in ``nodes`` that contains the classification values.
        primary_values : set[str]
            Set of values that define primary basins.
        secondary_values : set[str]
            Set of values that define secondary basins.

        Returns
        -------
        tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]
            Two GeoDataFrames:
            - primary basin geometries
            - secondary basin geometries

        Raises
        ------
        ValueError
            If values are found in ``basin_metacol`` that are not included in either
            ``primary_values`` or ``secondary_values``.
        """
        # validate if all nodes are convered by primary and secondary values
        all_known = primary_values | secondary_values
        unique_values = set(nodes[basin_metacol].dropna().unique())
        unknown_values = unique_values - all_known
        if unknown_values:
            raise ValueError(f"Unknown values in {basin_metacol}: {unknown_values} (all known: {all_known})")

        secondary_nodes = nodes[nodes[basin_metacol].isin(secondary_values)]
        primary_nodes = nodes[nodes[basin_metacol].isin(primary_values)]
        basin_definition = basin_definition.set_index("node_id", drop=True)
        secondary_mask = np.isin(secondary_nodes["node_id"], basin_definition.index)
        primary_mask = np.isin(primary_nodes["node_id"], basin_definition.index)
        if not secondary_mask.all():
            popped = secondary_nodes["node_id"][~secondary_mask]
            print(f"poped following secondary nodes: {popped}")
        if not primary_mask.all():
            popped = primary_nodes["node_id"][~primary_mask]
            print(f"poped following primary nodes: {popped}")
        basin_definition_primair = basin_definition.loc[primary_nodes["node_id"][primary_mask]]
        basin_definition_secondair = basin_definition.loc[secondary_nodes["node_id"][secondary_mask]]

        return basin_definition_primair, basin_definition_secondair

    def _split_basin_definitions(
        self,
        ribasim_model: Model,
        basin_split: str = "area",
        basin_subtype: str = "state",
        basin_metacol: str = "meta_categorie",
        primary_values: set[str] | None = None,
        secondary_values: set[str] | None = None,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Split basin areas into primary and secondary categories

        Parameters
        ----------
        ribasim_model : Model
            Ribasim Model
        basin_split : str, optional
            Table to be splitted, by default "area"
        basin_subtype : str, optional
            subtype to optionally read basin_metacol from, by default "state"
        basin_metacol : str, optional
            column with category, by default "meta_categorie"
        basin_primary : str, optional
            Not (?) primary value in metacolumn, by default "bergend"

        Returns
        -------
        tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]
            primary and secondary basins
        """
        # optionally get basin_metacol from other basin_subtype
        if secondary_values is None:
            secondary_values = self.secondary_labels
        if primary_values is None:
            primary_values = self.primary_labels
        assert ribasim_model.basin.node is not None
        assert ribasim_model.basin.node.df is not None
        if basin_metacol in ribasim_model.basin.node.df.columns:
            nodes = ribasim_model.basin.node.df[[basin_metacol, "geometry"]].copy().reset_index(drop=False)
        else:
            df_cat = getattr(ribasim_model.basin, basin_subtype).df.copy()
            if basin_metacol not in df_cat:
                raise ValueError(
                    f"category column {basin_metacol} not in basin.node or basin.{basin_subtype} tables. Provide column or another `basin_subtype` value"
                )
            nodes = ribasim_model.basin.node.df[["geometry"]].copy()
            if "node_id" in df_cat.columns:
                df_cat = df_cat[["node_id", basin_metacol]].set_index("node_id")
            else:
                # assume node_id is already the index
                df_cat = df_cat[[basin_metacol]]

            nodes = nodes.join(df_cat, how="left").reset_index(drop=False)

        self._validate_meta_basin_column(nodes, basin_metacol, expected_values=primary_values | secondary_values)

        # split based on meta_label in Ribasim model definition
        basin_definition = getattr(ribasim_model.basin, basin_split).df.copy()
        basin_definition_primair, basin_definition_secondair = self._split_basin_definition(
            basin_definition=basin_definition,
            nodes=nodes,
            basin_metacol=basin_metacol,
            primary_values=primary_values,
            secondary_values=secondary_values,
        )

        # transpose primary basins to secondary basin definition to get rid of the narrow polygons
        basin_definition_primair_polygon, basin_definition_undefined_primair, basin_definition_undefined_secundair = (
            self._transpose_basin_definition_polygons(basin_definition_primair, basin_definition_secondair)
        )
        if not basin_definition_undefined_primair.empty:
            # Add undefined primary elements to secondary basins so secondary budgets will also be assigned
            basin_definition_secondair = pd.concat([basin_definition_secondair, basin_definition_undefined_primair])

        # fill empty basins based on pip for secondary nodes
        basin_definition_primair_points = self._fill_basin_definition_from_points(
            basin_definition_undefined_secundair, nodes[nodes[basin_metacol].isin(primary_values)]
        )
        if not basin_definition_primair_points.empty:
            basin_definition_primair = pd.concat([basin_definition_primair_polygon, basin_definition_primair_points])
        basin_definition_primair = basin_definition_primair.reset_index(names="node_id")
        basin_definition_secondair = basin_definition_secondair.reset_index()

        return basin_definition_primair, basin_definition_secondair

    def _validate_meta_basin_column(self, df: pd.DataFrame, basin_metacol: str, expected_values: set[str]) -> None:
        """Validate if all values as expected are present in basin_metacol"""
        exception = ""
        if df[basin_metacol].isna().any():
            exception += " contains missings;"

        unexpected = [i for i in df[basin_metacol].unique() if i not in expected_values]
        if unexpected:
            exception += f" contains unexpeced values {unexpected}, check `primary_values` and `secondary_values` input"

        if exception:
            raise ValueError(f"{basin_metacol}{exception}")

    def plot_assign_validation(self, model: Model | Path | str, path: Path | str | None = None):
        """Validate if all budget cells are assigned to a basin and plot the assignment

        Parameters
        ----------
        model : Model | Path | str
            Ribasim model or path to ribasim.toml
        path : Path | str | None, optional
            Directory path to save the control plot. If None, no plot is saved.
        """
        budgets, model = self._sync_files(model=model)  # read model and budgets from zarr-store
        if path is not None:
            path = Path(path)
        primary_basin_definition, secondary_basin_definition = self._split_basin_definitions(ribasim_model=model)
        primary_basin_mask = imod.prepare.rasterize(
            primary_basin_definition,
            column="node_id",
            like=_crop_to_gdf(budgets["bdgriv_sys1"].isel(time=0, drop=True), primary_basin_definition),
            fill=-999,
            dtype=np.int32,
        )
        secondary_basin_mask = imod.prepare.rasterize(
            secondary_basin_definition,
            column="node_id",
            like=_crop_to_gdf(budgets["bdgriv_sys1"].isel(time=0, drop=True), secondary_basin_definition),
            fill=-999,
            dtype=np.int32,
        )
        basin_definition = gpd.GeoDataFrame(pd.concat([primary_basin_definition, secondary_basin_definition]))
        # split basin_definition by meta_categorie of Ribasim model itself, to keep original nod_id's
        assert model.basin.node is not None
        assert model.basin.node.df is not None
        nodes = gpd.GeoDataFrame(
            {
                "node_id": model.basin.node.df.index,
                "geometry": model.basin.node.df["geometry"],
                "meta_categorie": model.basin.node.df["meta_categorie"],
            }
        ).set_index("node_id", drop=True)
        basin_definition = basin_definition.set_index("node_id", drop=False)
        basin_bergend = basin_definition.loc[nodes["meta_categorie"] == "bergend"]
        basin_rest = basin_definition.loc[nodes["meta_categorie"] != "bergend"]

        self._plot_assign_validation(
            budgets=budgets,
            basin_definition=basin_definition,
            basin_rest=basin_rest,
            basin_bergend=basin_bergend,
            primary_basin_definition=primary_basin_definition,
            secondary_basin_definition=secondary_basin_definition,
            primary_basin_mask=primary_basin_mask,
            secondary_basin_mask=secondary_basin_mask,
            path=path,
        )

    def _plot_assign_validation(
        self,
        budgets,
        basin_definition: gpd.GeoDataFrame,
        basin_rest: gpd.GeoDataFrame,
        basin_bergend: gpd.GeoDataFrame,
        primary_basin_definition: gpd.GeoDataFrame,
        secondary_basin_definition: gpd.GeoDataFrame,
        primary_basin_mask,
        secondary_basin_mask,
        path: Path | None = None,
    ):
        # plot imports
        import matplotlib.pyplot as plt
        from matplotlib.colors import BoundaryNorm, ListedColormap
        from matplotlib.patches import Patch

        xmin, ymin, xmax, ymax = primary_basin_definition.total_bounds
        # sum over time to get all active nodes
        primary_sum = sum(
            budgets[v].sel(x=slice(xmin, xmax), y=slice(ymax, ymin)).sum("time").load()
            for v in self.primary_budget_keys
        )
        secondary_sum = sum(
            budgets[v].sel(x=slice(xmin, xmax), y=slice(ymax, ymin)).sum("time").load()
            for v in self.secondary_budget_keys
        )
        runoff_sum = sum(
            budgets[v].sel(x=slice(xmin, xmax), y=slice(ymax, ymin)).sum("time").load()
            for v in self.surface_runoff_budget_keys
        )
        # map to node_id of basins — crop masks to the same bbox first so all arrays share coordinates
        primary_mask_cropped = primary_basin_mask.where(primary_basin_mask != -999).sel(
            x=slice(xmin, xmax), y=slice(ymax, ymin)
        )
        secondary_mask_cropped = secondary_basin_mask.where(secondary_basin_mask != -999).sel(
            x=slice(xmin, xmax), y=slice(ymax, ymin)
        )
        primary_node_id = primary_mask_cropped * xr.where(primary_sum != 0.0, 1, np.nan)
        secondary_node_id = secondary_mask_cropped * xr.where(secondary_sum != 0.0, 1, np.nan)
        runoff_node_id = secondary_mask_cropped * xr.where(runoff_sum != 0.0, 1, np.nan)

        # check for unassigned budget cells: cells where budget is active but mask is nodata (-999)
        missed_primary = xr.where(
            (primary_basin_mask.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)) == -999) & (primary_sum != 0.0),
            1,
            np.nan,
        )
        missed_secondary = xr.where(
            (secondary_basin_mask.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)) == -999) & (secondary_sum != 0.0),
            1,
            np.nan,
        )
        missed_runoff = xr.where(
            (secondary_basin_mask.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)) == -999) & (runoff_sum != 0.0),
            1,
            np.nan,
        )

        # make red/green grids based solely on missed_* and active budget cells
        # green (1) = budget active and assigned to a basin
        # red   (2) = budget active but outside any basin (missed)
        # NaN       = no budget activity
        active_primary = xr.where(primary_sum != 0.0, 1.0, np.nan)
        active_secondary = xr.where(secondary_sum != 0.0, 1.0, np.nan)
        active_runoff = xr.where(runoff_sum != 0.0, 1.0, np.nan)

        combined_primary = xr.where(missed_primary == 1, 2.0, xr.where(active_primary == 1, 1.0, np.nan))
        combined_secondary = xr.where(missed_secondary == 1, 2.0, xr.where(active_secondary == 1, 1.0, np.nan))
        combined_runoff = xr.where(missed_runoff == 1, 2.0, xr.where(active_runoff == 1, 1.0, np.nan))

        # plot
        _, axes = plt.subplots(2, 5, figsize=(30, 12))
        # shared color mapping based on unique node_ids
        all_node_ids = np.sort(basin_definition["node_id"].unique())
        n = len(all_node_ids)
        # Build a colormap with exactly one unique color per node_id
        base_colors = plt.colormaps["hsv"](np.linspace(0, 1, n, endpoint=False))
        node_cmap = ListedColormap(base_colors)
        node_norm = BoundaryNorm(np.append(all_node_ids - 0.5, all_node_ids[-1] + 0.5), node_cmap.N)
        # plot the first four plots; from polygon definition to gridded primary + secondary masks
        for gdf, ax, title in zip(
            [
                basin_rest,
                basin_bergend,
                primary_basin_definition,
                secondary_basin_definition,
            ],
            [axes[0, 0], axes[0, 1], axes[1, 0], axes[1, 1]],
            [
                "Primair basin polygon",
                "Secondary basin polygon",
                "Gridded primary mask",
                "Gridded secondary mask",
            ],
            strict=True,
        ):
            gdf.plot(
                column="node_id",
                ax=ax,
                cmap=node_cmap,
                norm=node_norm,
                legend=False,
            )
            # Overlay basin boundaries
            basin_definition.boundary.plot(ax=ax, color="grey", linewidth=0.7)
            ax.set_title(f"{title} - node_id")
        # Row 0 cols 2-4: budget grids mapped to node_id
        for da, ax, title in zip(
            [primary_node_id, secondary_node_id, runoff_node_id],
            [axes[0, 2], axes[0, 3], axes[0, 4]],
            ["Primary LHM budgets - node_id", "Secondary LHM budgets - node_id", "Runoff LHM budgets - node_id"],
            strict=True,
        ):
            da.plot(ax=ax, cmap=node_cmap, norm=node_norm, add_colorbar=False)
            basin_definition.boundary.plot(ax=ax, color="grey", linewidth=0.7)
            ax.set_title(title)
            ax.set_aspect("equal")
        # red-green control grids
        cmap_combined = ListedColormap(
            [(0.0, 0.5, 0.0, 1.0), (1.0, 0.0, 0.0, 1.0), (0.0, 0.0, 0.0, 0.0)]
        )  # green=OK, red=missed, transparent=inactive
        cmap_combined.set_bad(color=(0, 0, 0, 0))  # NaN as transparent
        norm_combined = BoundaryNorm([0.5, 1.5, 2.5, 3.5], cmap_combined.N)
        legend_handles = [
            Patch(facecolor="green", edgecolor="k", label="assigned (OK)"),
            Patch(facecolor="red", edgecolor="k", label="missed (unassigned)"),
        ]
        # Row 1 cols 2-4: assignment check grids
        for da, ax, title in zip(
            [combined_primary, combined_secondary, combined_runoff],
            [axes[1, 2], axes[1, 3], axes[1, 4]],
            ["Primary: green=OK, red=missed", "Secondary: green=OK, red=missed", "Runoff: green=OK, red=missed"],
            strict=True,
        ):
            da.plot(ax=ax, cmap=cmap_combined, norm=norm_combined, add_colorbar=False)
            basin_definition.boundary.plot(ax=ax, color="grey", linewidth=0.7)
            ax.set_title(title)
            ax.set_aspect("equal")
            ax.legend(handles=legend_handles, loc="upper right")
        plt.tight_layout()
        # Add group labels above the two sets of plots
        # Left group: columns 0-1 (basin polygon definitions), right group: columns 2-4 (gridded budget checks)
        fig = plt.gcf()
        fig.text(
            0.21,
            1.01,
            "Basin polygon definitions",
            ha="center",
            va="bottom",
            fontsize=13,
            fontweight="bold",
            bbox={"boxstyle": "round,pad=0.3", "facecolor": "lightyellow", "edgecolor": "grey"},
        )
        fig.text(
            0.68,
            1.01,
            "Gridded LHM budget assignment check",
            ha="center",
            va="bottom",
            fontsize=13,
            fontweight="bold",
            bbox={"boxstyle": "round,pad=0.3", "facecolor": "lightblue", "edgecolor": "grey"},
        )
        if path is not None:
            plt.savefig(path, bbox_inches="tight")
        plt.close()

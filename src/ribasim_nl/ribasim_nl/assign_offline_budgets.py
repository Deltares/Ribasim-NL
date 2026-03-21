# %%
# original from https://github.com/Deltares/Ribasim-NL/blob/main/src/ribasim_nl/ribasim_nl/assign_offline_budgets.py
# Modified read differend IMODFLOW models
# Tested on GRAM Aa en Maas

from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd
import imod
import numpy as np
import pandas as pd
import shapely
import xarray as xr
from ribasim import Model
from tqdm import tqdm

try:
    from ribasim_nl import CloudStorage
except ImportError:
    CloudStorage = None

if TYPE_CHECKING:
    from ribasim_nl import CloudStorage as _CloudStorage


def budgets_from_dir(
    modflow_budgets_path: Path,
    metaswap_budgets_path: Path,
    starttime: pd.Timestamp | None = None,
    endtime: pd.Timestamp | None = None,
) -> xr.Dataset:
    """Reading budgets from MODFLOW-MetaSWAP

    Parameters
    ----------
    modflow_budgets_path : Path
        Path to Modflow bdgriv and bdgdrn
    metaswap_budgets_path : Path
        Path to Metaswap bdgPssw and bdgQrun
    starttime : pd.Timestamp | None, optional
        starttime to slice data, by default None
    endtime : pd.Timestamp | None, optional
        endtime to slice data, by default None

    Returns
    -------
    xr.Dataset
        DataSet with budgets
    """
    # time_slice, None if start_time/end_time are None
    time_slice = (
        slice(None)
        if starttime is None and endtime is None
        else slice(
            pd.Timestamp(starttime) if starttime is not None else None,
            pd.Timestamp(endtime) if endtime is not None else None,
        )
    )
    print(f"reading MODFLOW budgets from: {modflow_budgets_path}")
    print("reading riv-budgets for sys 1")
    ar = (
        imod.idf.open(modflow_budgets_path / "bdgriv/bdgriv_sys1_*_l*.IDF")
        .sum(dim="layer")
        .drop_vars(["dy", "dx"])
        .sel(time=time_slice)
    )

    ar.name = "bdgriv_sys1"
    ds = ar.to_dataset()

    for isys in range(2, 7):
        print(f"reading riv-budgets for sys {isys}")
        try:
            ds[f"bdgriv_sys{isys}"] = (
                imod.idf.open(modflow_budgets_path / f"bdgriv/bdgriv_sys{isys}_*_l*.IDF")
                .sum(dim="layer")
                .drop_vars(["dy", "dx"])
                .sel(time=time_slice)
            )
        except FileNotFoundError:
            print("No budget-files for this layer")

    for isys in range(1, 4):
        print(f"reading drn-budgets for sys {isys}")
        try:
            ds[f"bdgdrn_sys{isys}"] = (
                imod.idf.open(modflow_budgets_path / f"bdgdrn/bdgdrn_sys{isys}_*_l*.IDF")
                .sum(dim="layer")
                .drop_vars(["dy", "dx"])
                .sel(time=time_slice)
            )
        except FileNotFoundError:
            print("No budget-files for this layer")

    print(f"reading MetaSWAP budgets from: {metaswap_budgets_path}")
    bdgsw = imod.idf.open(metaswap_budgets_path / "bdgPssw/bdgPssw_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
    bdgqr = imod.idf.open(metaswap_budgets_path / "bdgQrun/bdgQrun_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
    ds["bdgpssw"] = bdgsw.resample(time="1D").bfill().sel(time=time_slice)
    ds["bdgqrun"] = bdgqr.resample(time="1D").bfill().sel(time=time_slice)

    return ds


def _crop_to_gdf(da, gdf):
    xmin, ymin, xmax, ymax = gdf.total_bounds

    dx = abs(float(da.x[1] - da.x[0]))
    dy = abs(float(da.y[1] - da.y[0]))

    xmin, ymin, xmax, ymax = xmin - dx, ymin - dy, xmax + dx, ymax + dy

    return da.sel(
        x=slice(xmin, xmax) if da.x[0] < da.x[-1] else slice(xmax, xmin),
        y=slice(ymin, ymax) if da.y[0] < da.y[-1] else slice(ymax, ymin),
    )


def _compute_budgets_per_basin(budgets, basin_mask, nodata=-999):
    """Faster version: process a full variable block at once."""
    print(f"∑ budgets {list(budgets.data_vars)} rasters to basins")

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
        zipped_budgets_path: Path | str | None = "Basisgegevens/LHM/4.3/results/LHM_433_budget.zip",
        modflow_budgets_path: Path | str | None = None,
        metaswap_budgets_path: Path | str | None = None,
    ):
        """Assign offline budgets from  MODFLOW MetaSWAP budget files.

        These files are either supplied as LHM zipped budgets path in Zarr-format (default) that is stored in the Deltares Cloud Storage.

        Alternatively, when local MODFLOW-MetaSWAP models are to be coupled, one can supply the modflow_budgets_path and metaswap_budgets_path containing IDF-files:
            - The expected file-pattern relative to modflow_budgets path is bdgdrn/bdgdrn_sys<system>_<yyyymmdd>_l<layer>.idf and bdgriv/bdgriv_sys<system>_<yyyymmdd>_l<layer>.idf
            - The expected file-pattern relative to metaswap_budgets_path is bdgpssw/bdgpssw_<yyyymmdd_l<layer>.idf and bdgqrun/bdgqrun_<yyyymmdd_l<layer>.idf

        Parameters
        ----------
        zipped_budgets_path : Path | str | None, optional
            Zip-file with zar-storage, by default "Basisgegevens/LHM/4.3/results/LHM_433_budget.zip"
        modflow_budgets_path : Path | str | None, optional
            MODFLOW budgets in IDF format, by default None
        metaswap_budgets_path : Path | str | None, optional
            MetaSWAP budgets in IDF forma by default None
        """
        self.cloud: _CloudStorage | None = None
        # If you don't provide modflow_budgets_path and metaswap_budgets_path you will use LHM budgets from Ribasim-NL
        # In that case you'll need access to Deltares CloudStorage
        if (modflow_budgets_path is None) | (metaswap_budgets_path is None):
            self.cloud = CloudStorage()
            self.zipped_budgets_path = self.cloud.joinpath(zipped_budgets_path)
            self.modflow_budgets_path = None
            self.metaswap_budgets_path = None
        else:
            self.zipped_budgets_path = None
            self.modflow_budgets_path = Path(modflow_budgets_path)
            self.metaswap_budgets_path = Path(metaswap_budgets_path)

    def compute_budgets(
        self,
        model: Model | Path | str,
        basin_split: str = "area",
        basin_subtype: str = "state",
        basin_metacol: str = "meta_categorie",
        primary_values: set[str] = {"hoofdwater", "doorgaand"},
        secondary_values: set[str] = {"bergend"},
        primary_budgets: set[str] = {"bdgriv_sys1", "bdgriv_sys4", "bdgriv_sys5"},
        secondary_budgets: set[str] = {
            "bdgriv_sys2",
            "bdgriv_sys3",
            "bdgriv_sys6",
            "bdgdrn_sys1",
            "bdgdrn_sys2",
            "bdgdrn_sys3",
            "bdgpssw",
        },
        surface_runoff_budgets: set[str] = {"bdgqrun"},
    ) -> tuple[Model, pd.DataFrame]:
        """Compute budgets for Ribasim model

        MODFLOW/MetaSWAP budgets for LHM are computed in the following scheme
        RIV-package
        sys1: primary system
        sys2: secondary system
        sys3: tertiary system
        sys4: main system; layer 1
        sys5: main system; layer 2
        sys6: boil's / well's

        DRN-package
        sys1: tube drainage
        sys2: ditch dranage
        sys3: OLF

        MetaSWAP budgets
        qrun: OLF via MetaSWAP
        pssw: irrigation from surface water
        TODO: evaluate if we need to add urban runoff

        For the Ribasim schematization we distinguish:
          - Primary system for all basins
          - Secondary system in basins other than the main river system

        For drainage an infiltration input based on LHM-output budgets, we distubute the LHM-systems in the following matter:
         - Primary system   -> RIV-sys 1 + 4 + 5
         - Secondary system -> RIV-sys 2 + 3 + 6, DRN-sys 1 + 2 + 3, qrun + pssw

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
            set of values in basin_metacol that represent primary basins
        secondary_values: set[str]
            set of values in basin_metacol that represent secondary basins
        primary_budgets : set[str], optional
            set of budgets that are to be summed to primary drainage/infiltration,
             by default ["bdgriv_sys1", "bdgriv_sys4", "bdgriv_sys5"]
        secondary_budgets : set[str], optional
            set of budgets that are to be summed to secondary drainage/infiltration,
             by default ["bdgriv_sys2", "bdgriv_sys3", "bdgriv_sys6", "bdgdrn_sys1", "bdgdrn_sys2", "bdgdrn_sys3", "bdgpssw"]
        surface_runoff_budgets: set[str], optional
            set of budgets that are to be summed to secondary surface_runoff
             by default ["bdgqrun"]

        Returns
        -------
        Model, pd.DataFrame
            Model and with MODFLOW-MetaSWAP budgets per node_id and timestamp. These can be used for verification and/or to compute fraction tracer/concentrations
        """
        # Synchronize LHM budget and model files
        print("📖 read and validate budgets")
        budgets, model = self._sync_files(model)

        # Split into primary and secondary basin definition
        print("🪓 split basins into primary and secondary")
        primary_basin_definition, secondary_basin_definition = self._split_basin_definitions(
            model,
            basin_split=basin_split,
            basin_subtype=basin_subtype,
            basin_metacol=basin_metacol,
            primary_values=primary_values,
            secondary_values=secondary_values,
        )

        print("▦ rasterize basins to masks")
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

        print("⚙️ compute budgets per basin")
        primary_budgets_df = (
            _compute_budgets_per_basin(
                _crop_to_gdf(budgets[list(primary_budgets)], primary_basin_definition),
                primary_basin_mask,
            )
            / 86400
        )

        secondary_budgets_df = (
            _compute_budgets_per_basin(
                _crop_to_gdf(budgets[list(secondary_budgets | surface_runoff_budgets)], secondary_basin_definition),
                secondary_basin_mask,
            )
            / 86400
        )

        print("📈 add budgets to drainage/infiltration and surface_runoff columns")
        # concat all budgets so we can return those for verification
        budgets_df = pd.concat([primary_budgets_df, secondary_budgets_df]).sort_index()

        # sum all budgets (columns) and create drainage and infiltration series
        summed_budgets = pd.Series(budgets_df[list(primary_budgets | secondary_budgets)].sum(axis=1))
        drainage = summed_budgets.clip(
            upper=0
        ).abs()  # all <0 is drainage. Take absolute its a positive term in RIBASIM
        infiltration = summed_budgets.clip(
            lower=0
        )  # alles > 0 (infiltratie is in modflow, ontrekking uit ribasim, maar in ribasim positief teken)
        surface_runoff = pd.Series(budgets_df[list(surface_runoff_budgets)].sum(axis=1)).clip(
            lower=0
        )  # assume surface_runoff can't be <0 in RIBASIM

        # update basin drainage and infiltration
        idx = pd.MultiIndex.from_frame(model.basin.time.df[["node_id", "time"]])
        model.basin.time.df["drainage"] = idx.map(drainage)
        model.basin.time.df["infiltration"] = idx.map(infiltration)
        model.basin.time.df["surface_runoff"] = idx.map(surface_runoff)

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
        # Synchronize files from cloud (LHM-case)
        filepaths = [self.zipped_budgets_path]
        if self.cloud is not None:
            if not isinstance(model, Model):
                filepaths.append(Path(model))
            self.cloud.synchronize(filepaths=filepaths)

        # Read the ribasim model if needed
        if not isinstance(model, Model):
            model = Model.read(model)

        # Open the budget-file as zarr if zip-file
        if self.zipped_budgets_path is None:
            budgets = budgets_from_dir(
                modflow_budgets_path=self.modflow_budgets_path,
                metaswap_budgets_path=self.metaswap_budgets_path,
                starttime=model.starttime,
                endtime=model.endtime,
            )
        elif self.zipped_budgets_path.is_dir() & (self.zipped_budgets_path.suffix == ".zip"):
            budgets = xr.open_zarr(str(self.zipped_budgets_path)).sel(time=slice(model.starttime, model.endtime))
        else:
            raise ValueError(
                f"{self.zipped_budgets_path} does not seem to be a zarr store or modflow budgets directory so can't be opened"
            )

        return budgets, model

    def _transpose_basin_definition_polygons(
        self,
        basin_definition_in: gpd.GeoDataFrame,
        basin_definition_out: gpd.GeoDataFrame,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Retruns basin_difinition_out with index of basin_definition_in that intersect the basin_definition_out polygons

        Args:
            basin_definition_in (gpd.GeoDataFrame): Basin definition with (multi) polygons
            basin_definition_out (gpd.GeoDataFrame): Basin definition with (multi) polygons

        Returns
        -------
            tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: Basin definition with new index, Basin definition with
            polygons without any intersection
        """
        tree = shapely.STRtree(basin_definition_out["geometry"])
        index_in, index_out = tree.query(basin_definition_in.representative_point(), predicate="intersects")
        index_in = basin_definition_in.index[index_in]
        index_out = basin_definition_out.index[index_out]
        index_undifined = basin_definition_out.index[~np.isin(basin_definition_out.index, index_out)]
        basin_definition_undifined = basin_definition_out.loc[index_undifined]
        basin_definition_out = basin_definition_out.loc[index_out]
        return basin_definition_out.set_index([index_in]), basin_definition_undifined

    def _fill_basin_definition_from_points(
        self,
        basin_definition: gpd.GeoDataFrame,
        nodes: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """
        Retuns basin_definition filled with index of basins within polygon definition

        Args:
            basin_definition (gpd.GeoDataFrame): Basin definition with (multi) polygons
            nodes (gpd.GeoDataFrame): Ribasim Basin nodes

        Returns
        -------
            gpd.GeoDataFrame: basin_definition with index from underlying Ribasim Basins
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
        primary_values: set[str] = {"hoofdwater", "doorgaand"},
        secondary_values: set[str] = {"bergend"},
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
        if "meta_categorie" in ribasim_model.basin.node.df.columns:
            nodes = ribasim_model.basin.node.df[["meta_categorie", "geometry"]].copy().reset_index(drop=False)
        else:
            nodes = ribasim_model.basin.node.df[["geometry"]].copy()

            df_cat = getattr(ribasim_model.basin, basin_subtype).df.copy()
            if "node_id" in df_cat.columns:
                df_cat = df_cat[["node_id", basin_metacol]].set_index("node_id")
            else:
                # assume node_id is already the index
                df_cat = df_cat[[basin_metacol]]

            nodes = nodes.join(df_cat, how="left").reset_index(drop=False)

        # split based on meta_label in Ribasim model definition
        basin_definition = getattr(ribasim_model.basin, basin_split).df.copy()
        basin_definition_primair, basin_definition_secondair = self._split_basin_definition(
            basin_definition=basin_definition,
            nodes=nodes,
            basin_metacol=basin_metacol,
            primary_values=primary_values,
            secondary_values=secondary_values,
        )

        # transpose primairy basins to secondary basin definition to get rid of the narrow polygons
        basin_definition_primair_polygon, basin_definition_undifined = self._transpose_basin_definition_polygons(
            basin_definition_primair, basin_definition_secondair
        )

        # fill empty basins based on pip for secondary nodes
        basin_definition_primair_points = self._fill_basin_definition_from_points(
            basin_definition_undifined, nodes[nodes[basin_metacol].isin(primary_values)]
        )
        if not basin_definition_primair_points.empty:
            basin_definition_primair = pd.concat([basin_definition_primair_polygon, basin_definition_primair_points])
        basin_definition_primair = basin_definition_primair.reset_index(names="node_id")
        basin_definition_secondair = basin_definition_secondair.reset_index()

        return basin_definition_primair, basin_definition_secondair

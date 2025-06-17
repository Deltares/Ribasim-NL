from pathlib import Path

import geopandas as gpd
import imod
import numpy as np
import pandas as pd
import shapely
import xarray as xr
from ribasim import Model

from ribasim_nl import CloudStorage
from ribasim_nl import Model as ModelNL


class AssignOfflineBudgets:
    def __init__(
        self,
        lhm_budget: Path | str = "Basisgegevens/LHM/4.3/results/LHM_budgets.zip",
    ):
        self.cloud = CloudStorage()
        self.lhm_budget = self.cloud.joinpath(lhm_budget)

    def compute_budgets(
        self,
        model: ModelNL | Model | Path | str,
        basin_split: str = "area",
        basin_subtype: str = "state",
        basin_metacol: str = "meta_categorie",
    ) -> ModelNL | Model:
        # Synchronize LHM budget and model files
        budgets, model = self._sync_files(model)

        # Split into primary and secondary basin definition
        primary_basin_definition, secondary_basin_definition = self.split_basin_definitions(
            model,
            basin_split=basin_split,
            basin_subtype=basin_subtype,
            basin_metacol=basin_metacol,
        )

        # create masks
        array = budgets["bdgriv_sys1"].isel(time=0, drop=True)
        primary_basin_mask = imod.prepare.rasterize(
            primary_basin_definition, column="node_id", like=array, fill=-999, dtype=np.int32
        )
        secondary_basin_mask = imod.prepare.rasterize(
            secondary_basin_definition, column="node_id", like=array, fill=-999, dtype=np.int32
        )

        # compute budgets
        budgets_per_node_id = self._compute_budgets_per_node_id(budgets, primary_basin_mask, secondary_basin_mask)

        # Align model
        budgets_per_node_id.columns += model.starttime - budgets_per_node_id.columns.min()

        # split to drainage and infiltration budgets
        # negative budgets means drainage from the groundwatermodel
        drainage_per_node_id = budgets_per_node_id[budgets_per_node_id.lt(0.0)].abs().fillna(0.0)
        infiltration_per_node_id = budgets_per_node_id[budgets_per_node_id.gt(0.0)].fillna(0.0)

        # Fill missing node-ids with zeros
        all_nodeids = model.basin.node.df.index.unique()
        missing_nodeids = all_nodeids[~all_nodeids.isin(budgets_per_node_id.index)]
        missing_df = pd.DataFrame(0.0, index=missing_nodeids, columns=budgets_per_node_id.columns)
        drainage_per_node_id = pd.concat([drainage_per_node_id, missing_df], ignore_index=False)
        infiltration_per_node_id = pd.concat([infiltration_per_node_id, missing_df], ignore_index=False)

        drainage_per_node_id = drainage_per_node_id.unstack().to_frame("drainage")
        infiltration_per_node_id = infiltration_per_node_id.unstack().to_frame("infiltration")
        basin_time = drainage_per_node_id.join(infiltration_per_node_id).reset_index()

        # Fill remaining columns with 0
        missing_cols = model.basin.time.df.columns[~model.basin.time.df.columns.isin(basin_time.columns)]
        basin_time[missing_cols] = 0.0

        # set basin.time
        model.basin.time.df = basin_time

        return model

    def _sync_files(
        self,
        model: ModelNL | Model | Path | str,
    ) -> tuple[xr.Dataset, ModelNL | Model]:
        # Synchronize LHM budget and model files
        filepaths = [self.lhm_budget]
        if not (isinstance(model, ModelNL) or isinstance(model, Model)):
            filepaths.append(Path(model))
        self.cloud.synchronize(filepaths=filepaths)

        # Read the ribasim model
        if not (isinstance(model, ModelNL) or isinstance(model, Model)):
            model = Model.read(model)

        # Open the LHM budget file
        budgets = xr.open_zarr(str(self.lhm_budget))

        return budgets, model

    def _compute_budgets_per_node_id(
        self,
        budgets: xr.Dataset,
        primary_basin_mask: xr.DataArray,
        secondary_basin_mask: xr.DataArray,
    ) -> pd.DataFrame:
        # compute budgets
        # LHM-budget output naming convention

        # RIV-package
        # sys1: primary system
        # sys2: secondary system
        # sys3: tertiary system
        # sys4: main system; layer 1
        # sys5: main system; layer 2
        # sys6: boil's / well's

        # DRN-package
        # sys1: tube drainage
        # sys2: ditch dranage
        # sys3: OLF

        # For the Ribasim schematization we distinguish:
        #   - Primary system for all basins
        #   - Secondary system in basins other than the main river system

        # For drainage an infiltration input based on LHM-output budgets, we distubute the LHM-systems in the following matter:
        #  - Primary system   -> RIV-sys 1 + 4 + 5
        #  - Secondary system -> RIV-sys 2 + 3 + 6, DRN-sys 1 + 2 + 3

        # sum primairy systems
        primary_summed_budgets = budgets["bdgriv_sys1"]
        primary_summed_budgets = primary_summed_budgets.rename("primair")
        for sys in [4, 5]:
            primary_summed_budgets += budgets[f"bdgriv_sys{sys}"]

        # sum secondary systems
        secondary_summed_budgets = budgets["bdgriv_sys2"]
        secondary_summed_budgets = secondary_summed_budgets.rename("secondair")
        for sys, package in zip([3, 6, 1, 2, 3], ["riv", "riv", "drn", "drn", "drn"]):
            secondary_summed_budgets += budgets[f"bdg{package}_sys{sys}"]

        # sum per system and node_id
        primary_budgets_per_node_id = (
            primary_summed_budgets.groupby(primary_basin_mask)
            .sum(dim="stacked_y_x")
            .to_dataframe()
            .unstack(1)
            .transpose()
        )
        primary_budgets_per_node_id.index = primary_budgets_per_node_id.index.droplevel(0)
        primary_budgets_per_node_id = primary_budgets_per_node_id.loc[
            primary_budgets_per_node_id.index != -999, :
        ]  # remove non overlapping budgets

        secundary_budgets_per_node_id = (
            secondary_summed_budgets.groupby(secondary_basin_mask)
            .sum(dim="stacked_y_x")
            .to_dataframe()
            .unstack(1)
            .transpose()
        )
        secundary_budgets_per_node_id.index = secundary_budgets_per_node_id.index.droplevel(0)
        secundary_budgets_per_node_id = secundary_budgets_per_node_id.loc[
            secundary_budgets_per_node_id.index != -999, :
        ]  # remove non overlapping budgets

        # combine dataframe's based on node_id
        budgets_per_node_id = pd.concat([primary_budgets_per_node_id, secundary_budgets_per_node_id])
        budgets_per_node_id.index.name = "node_id"

        return budgets_per_node_id

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
        metacol: str,
        basin_primary: str,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Splits basin definition based on 'meta_categorie' in Ribasim Basin nodes

        Args:
            basin_definition (gpd.GeoDataFrame): Basin definition with (multi) polygons
            nodes (gpd.GeoDataFrame): Ribasim Basin nodes

        Returns
        -------
            tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: Basin definition with (multi) polygons for primary ans secondary Basins
        """
        secondary_nodes = nodes[nodes[metacol] == basin_primary]
        primary_nodes = nodes[nodes[metacol] != basin_primary]
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

    def split_basin_definitions(
        self,
        ribasim_model: Model | ModelNL,
        basin_split: str = "area",
        basin_subtype: str = "state",
        basin_metacol: str = "meta_categorie",
        basin_primary: str = "bergend",
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        df_cat = getattr(ribasim_model.basin, basin_subtype).df.copy()
        if "node_id" in df_cat.columns:
            df_cat = df_cat[["node_id", basin_metacol]].set_index("node_id")
        else:
            # assume node_id is already the index
            df_cat = df_cat[[basin_metacol]]
        nodes = ribasim_model.basin.node.df[["geometry"]].copy()
        nodes = nodes.join(df_cat, how="left").reset_index(drop=False)

        # split based on meta_label in Ribasim model definition
        basin_definition = getattr(ribasim_model.basin, basin_split).df.copy()
        basin_definition_primair, basin_definition_secondair = self._split_basin_definition(
            basin_definition, nodes, basin_metacol, basin_primary
        )

        # transpose primairy basins to secondary basin definition to get rid of the narrow polygons
        basin_definition_primair_polygon, basin_definition_undifined = self._transpose_basin_definition_polygons(
            basin_definition_primair, basin_definition_secondair
        )

        # fill empty basins based on pip for secondary nodes
        basin_definition_primair_points = self._fill_basin_definition_from_points(
            basin_definition_undifined, nodes[nodes[basin_metacol] != basin_primary]
        )
        basin_definition_primair = pd.concat([basin_definition_primair_polygon, basin_definition_primair_points])
        basin_definition_primair = basin_definition_primair.reset_index(names="node_id")
        basin_definition_secondair = basin_definition_secondair.reset_index()

        return basin_definition_primair, basin_definition_secondair

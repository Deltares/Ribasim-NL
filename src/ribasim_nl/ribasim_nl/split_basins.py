"""Split basins in the Ribasim model based on linestrings"""

# https://github.com/Deltares/Ribasim-NL/issues/554

import typing

import geopandas as gpd
import pandas as pd
from ribasim import Node
from shapely.geometry.base import BaseGeometry

from ribasim_nl import Model


class SplitBasins:
    def __init__(
        self,
        model: Model,
        splitted_basin_gdf: gpd.GeoDataFrame,
        basin_node_id_to_split: int,
        # hydroobjects: gpd.GeoDataFrame,
    ):
        self.model = model
        self.splitted_basin_gdf = splitted_basin_gdf
        self.basin_node_id_to_split = basin_node_id_to_split
        # self.hydroobjects = hydroobjects

    def run(self):

        # implement the newly splitted basins in the model
        test_model, newly_created_basins = self.create_new_basins(
            model=self.model,
            splitted_basin_gdf=self.splitted_basin_gdf,
            basin_node_id_to_split=self.basin_node_id_to_split,
        )

        # redirect the connector nodes to the new basins
        test_model = self.redirect_connectors_to_new_basins(
            model=test_model,
            basin_node_id_to_split=self.basin_node_id_to_split,
            newly_created_basins=newly_created_basins,
        )

        return test_model

    def redirect_connectors_to_new_basins(
        self, model: Model, basin_node_id_to_split: int, newly_created_basins: list[int]
    ):

        # find all connectors that are connected to the original basin
        assert model.link.df is not None
        connector_node_ids_from_original_basin = model.link.df.loc[
            model.link.df.from_node_id == basin_node_id_to_split, "to_node_id"
        ]
        print(connector_node_ids_from_original_basin)

        pass

    def create_new_basins(self, model: Model, splitted_basin_gdf: gpd.GeoDataFrame, basin_node_id_to_split: int):

        newly_created_basins = []
        # loop through the splitted basin gdf and create new basins in the model for each geometry (which has been splitted)
        for new_basin in splitted_basin_gdf.itertuples():
            geometry = typing.cast(BaseGeometry, new_basin.geometry)  # avoid pyrefly error
            new_node_id = model.next_node_id
            model.basin.add(
                Node(node_id=new_node_id, geometry=geometry.representative_point()),
                tables=[],
            )

            # copy all tables (including its metadata) to the new basin. Replace the basin geometry in the next step.
            self._copy_basin_tables(
                original_node_id=basin_node_id_to_split,
                new_node_id=new_node_id,
            )

            # replace the new basin geometry in the area table
            assert self.model.basin.area.df is not None
            self.model.basin.area.df.loc[self.model.basin.area.df.node_id == new_node_id, "geometry"] = geometry  # pyrefly: ignore[unsupported-operation]
            newly_created_basins.append(new_node_id)

        return self.model, newly_created_basins

    def _copy_basin_tables(self, original_node_id: int, new_node_id: int):
        for table in [
            self.model.basin.profile,
            self.model.basin.static,
            self.model.basin.time,
            self.model.basin.state,
            self.model.basin.area,
        ]:
            # skip if table does not exist
            if table.df is None:
                continue

            rows = table.df.loc[table.df.node_id == original_node_id].copy()
            rows["node_id"] = new_node_id
            table.df = pd.concat([table.df, rows], ignore_index=True)  # pyrefly: ignore[bad-assignment]

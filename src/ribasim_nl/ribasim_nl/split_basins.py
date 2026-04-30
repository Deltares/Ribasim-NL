"""Split basins in the Ribasim model based on linestrings"""

# https://github.com/Deltares/Ribasim-NL/issues/554

import typing
from itertools import combinations

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import manning_resistance
from shapely.geometry import LineString, Point
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

        # implement the newly splitted basins in the model as new basins, without any connection (yet)
        test_model, newly_created_basins = self.create_new_basins(
            model=self.model,
            splitted_basin_gdf=self.splitted_basin_gdf,
            basin_node_id_to_split=self.basin_node_id_to_split,
        )

        # redirect the connector nodes to the splitted basins
        test_model = self.redirect_connectors_to_new_basins(
            model=test_model,
            basin_node_id_to_split=self.basin_node_id_to_split,
            newly_created_basins=newly_created_basins,
        )

        # connect the newly created basins with each other using MR
        test_model = self.add_manning_nodes_between_splitted_basins(
            model=test_model,
            basin_node_id_to_split=self.basin_node_id_to_split,
            newly_created_basins=newly_created_basins,
        )

        # delete the original basin from the model
        test_model.remove_node(self.basin_node_id_to_split, False)
        return test_model

    def add_manning_nodes_between_splitted_basins(
        self, model: Model, basin_node_id_to_split: int, newly_created_basins: list[int]
    ):

        # only manning nodes have to be added between basins which touch each other
        # find the touching parts, determine the node_ids of the basins that touch each other and determine the centroid of the touching part where the manning node will be added
        assert model.basin.area.df is not None

        basin_area = model.basin.area.df.loc[model.basin.area.df.node_id.isin(newly_created_basins)]
        basin_area = basin_area.dissolve(by="node_id")

        manning_data = manning_resistance.Static(length=[1000], manning_n=[0.04], profile_width=[10], profile_slope=[3])

        for from_basin_id, to_basin_id in combinations(newly_created_basins, 2):
            from_basin_geometry = typing.cast(BaseGeometry, basin_area.at[from_basin_id, "geometry"])
            to_basin_geometry = typing.cast(BaseGeometry, basin_area.at[to_basin_id, "geometry"])

            if not from_basin_geometry.touches(to_basin_geometry):
                continue

            touching_geometry = from_basin_geometry.boundary.intersection(to_basin_geometry.boundary)
            if touching_geometry.is_empty:
                continue

            # create Manning node at the centroid of the touching geometry and connect it to the two basins
            manning_node = model.manning_resistance.add(
                Node(geometry=touching_geometry.centroid),
                tables=[manning_data],
            )
            model.link.add(model.basin[from_basin_id], manning_node)
            model.link.add(manning_node, model.basin[to_basin_id])

        return model

    def redirect_connectors_to_new_basins(
        self, model: Model, basin_node_id_to_split: int, newly_created_basins: list[int]
    ):

        # find all connectors that are connected to the original basin
        assert model.link.df is not None
        assert model.node.df is not None
        assert model.basin.area.df is not None

        for connector_column, basin_column in [("to_node_id", "from_node_id"), ("from_node_id", "to_node_id")]:
            connector_node_ids_from_original_basin = model.link.df.loc[
                model.link.df[basin_column] == basin_node_id_to_split, connector_column
            ].dropna()

            # determine closest distance from the connector nodes to the new basins and redirect the connectors to the closest new basin
            for connector_node_id in connector_node_ids_from_original_basin:
                connector_node_id = int(connector_node_id)
                connector_node_geometry = typing.cast(
                    BaseGeometry, model.node.df.at[connector_node_id, "geometry"]
                )  # pyrefly

                closest_basin_node_id = None
                closest_distance = float("inf")

                # loop through each splitted basin part to determine the shortest distance
                for new_basin_node_id in newly_created_basins:
                    new_basin_geometry = model.basin.area.df.loc[
                        model.basin.area.df.node_id == new_basin_node_id,
                        "geometry",
                    ].iloc[0]
                    new_basin_geometry = typing.cast(BaseGeometry, new_basin_geometry)  # pyrefly

                    distance = connector_node_geometry.distance(new_basin_geometry)

                    # overwrite the closest distance and basin node_id
                    if distance < closest_distance:
                        closest_distance = distance
                        closest_basin_node_id = new_basin_node_id

                print(f"Redirecting connector node {connector_node_id} to basin node {closest_basin_node_id}")
                if closest_basin_node_id is None:
                    raise ValueError(f"No closest basin found for connector node {connector_node_id}.")

                # redirect the connector node administratively to the closest new basin
                mask = (model.link.df[basin_column] == basin_node_id_to_split) & (
                    model.link.df[connector_column] == connector_node_id
                )
                model.link.df.loc[mask, basin_column] = closest_basin_node_id

                # redirect the connector node spatially to the closest new basin by changing the geometry of the connector node to the representative point of the closest new basin
                # retrieve the point geomeries to create a straight line between them
                geometry_connector_node = typing.cast(Point, model.node.df.at[connector_node_id, "geometry"])  # pyrefly
                geometry_basin_node = typing.cast(Point, model.node.df.at[closest_basin_node_id, "geometry"])  # pyrefly

                # create a straight line between the connector node and the closest new basin
                line_points = (
                    [geometry_basin_node, geometry_connector_node]
                    if basin_column == "from_node_id"
                    else [geometry_connector_node, geometry_basin_node]
                )
                new_geometry = LineString(line_points)
                model.link.df.loc[mask, "geometry"] = new_geometry  # pyrefly: ignore[unsupported-operation]
        return model

    def create_new_basins(self, model: Model, splitted_basin_gdf: gpd.GeoDataFrame, basin_node_id_to_split: int):

        # store the newly created basins for later use
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
            assert self.model.basin.area.df is not None  # pyrefly
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

"""Split basins in a Ribasim model based on pre-split basin polygons."""

import typing
from itertools import combinations
from pathlib import Path

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import manning_resistance
from shapely.geometry import GeometryCollection, LineString, MultiLineString, Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import linemerge, unary_union

from ribasim_nl import Model

# https://github.com/Deltares/Ribasim-NL/issues/554


class SplitBasins:
    """Replace one basin by pre-split basin polygons and reconnect its links."""

    def __init__(
        self,
        model: Model,
        splitted_basin_path: str | Path,
        basin_node_id_to_split: int,
        geometry_tolerance: float = 1.0,
        printing: bool = False,
    ):
        """Initialize splitter from a path to a polygon layer with the split basin parts."""
        self.model = model
        self.splitted_basin_gdf = gpd.read_file(splitted_basin_path)
        self.basin_node_id_to_split = basin_node_id_to_split
        self.geometry_tolerance = geometry_tolerance
        self.printing = printing

    def run(self):
        """Run the full split workflow and return the updated model."""
        # implement the newly splitted basins in the model as new basins, without any connection (yet)
        newly_created_basins = self.create_new_basins()

        # redirect the connector nodes to the splitted basins
        self.redirect_connectors_to_new_basins(newly_created_basins=newly_created_basins)

        # connect the newly created basins with each other using MR
        self.add_manning_nodes_between_splitted_basins(newly_created_basins=newly_created_basins)

        # delete the original basin from the model
        self.model.remove_node(self.basin_node_id_to_split, False)
        return self.model

    def add_manning_nodes_between_splitted_basins(self, newly_created_basins: list[int]):
        """Add ManningResistance nodes between split basin polygons that share a boundary face. A ManningResistance node is added at the centroid of the shared boundary face and connected to the two basins with links. This ensures that water can flow between the newly created basins. They are added on each face. This may be redundant, but this will improve recognizability of the model and is not expected to cause performance issues, since the number of basins that are split and the number of shared faces is expected to be low."""
        # only manning nodes have to be added between basins which touch each other
        # find the touching parts, determine the node_ids of the basins that touch each other and determine the centroid of the touching part where the manning node will be added
        assert self.model.basin.area.df is not None

        basin_area = self.model.basin.area.df.loc[self.model.basin.area.df.node_id.isin(newly_created_basins)]
        basin_area = basin_area.dissolve(by="node_id")

        manning_data = manning_resistance.Static(length=[1000], manning_n=[0.04], profile_width=[10], profile_slope=[3])

        if self.printing:
            print(
                f"Split basin {self.basin_node_id_to_split}: "
                f"checking ManningResistance connections for {newly_created_basins}"
            )
        for from_basin_id, to_basin_id in combinations(newly_created_basins, 2):
            from_basin_geometry = typing.cast(BaseGeometry, basin_area.at[from_basin_id, "geometry"])
            to_basin_geometry = typing.cast(BaseGeometry, basin_area.at[to_basin_id, "geometry"])

            touches = from_basin_geometry.touches(to_basin_geometry)
            touching_geometry = from_basin_geometry.boundary.intersection(to_basin_geometry.boundary)
            touching_faces = self._touching_faces(touching_geometry)
            shared_face_length = sum(touching_face.length for touching_face in touching_faces)
            overlap_area = from_basin_geometry.intersection(to_basin_geometry).area
            # ``touches`` can be False for geometries that share linework but have tiny topology artefacts.
            # Accept those cases when there is a real shared face and the area overlap is negligible.
            within_tolerance = (
                shared_face_length > self.geometry_tolerance
                and overlap_area <= self.geometry_tolerance * self.geometry_tolerance
            )

            if self.printing:
                print(
                    f"Split basin {self.basin_node_id_to_split}: pair {from_basin_id}-{to_basin_id}: "
                    f"touches={touches}, distance={from_basin_geometry.distance(to_basin_geometry):.6f}, "
                    f"relate={from_basin_geometry.relate(to_basin_geometry)}, "
                    f"boundary_intersection={touching_geometry.geom_type}, "
                    f"empty={touching_geometry.is_empty}, length={touching_geometry.length:.6f}, "
                    f"faces={len(touching_faces)}, overlap_area={overlap_area:.6f}, "
                    f"within_tolerance={within_tolerance}"
                )

            if not touches and not within_tolerance:
                if self.printing and not touching_geometry.is_empty:
                    print(
                        f"Split basin {self.basin_node_id_to_split}: skipping pair {from_basin_id}-{to_basin_id} "
                        "because touches=False and the shared boundary is outside the geometry tolerance."
                    )
                continue
            if self.printing and not touches and within_tolerance:
                print(
                    f"Split basin {self.basin_node_id_to_split}: accepting pair {from_basin_id}-{to_basin_id} "
                    f"using geometry_tolerance={self.geometry_tolerance}."
                )

            if touching_geometry.is_empty:
                if self.printing:
                    print(
                        f"Split basin {self.basin_node_id_to_split}: skipping pair {from_basin_id}-{to_basin_id} "
                        "because the boundary intersection is empty."
                    )
                continue

            # create Manning node at the centroid of each touching face and connect it to the two basins
            if not touching_faces:
                if self.printing:
                    print(
                        f"Split basin {self.basin_node_id_to_split}: skipping pair {from_basin_id}-{to_basin_id} "
                        f"because boundary intersection type {touching_geometry.geom_type} is not handled."
                    )
                continue

            for touching_face in touching_faces:
                manning_node = self.model.manning_resistance.add(
                    Node(geometry=touching_face.centroid),
                    tables=[manning_data],
                )
                self.model.link.add(self.model.basin[from_basin_id], manning_node)
                self.model.link.add(manning_node, self.model.basin[to_basin_id])
                if self.printing:
                    print(
                        f"Split basin {self.basin_node_id_to_split}: added ManningResistance "
                        f"between {from_basin_id}-{to_basin_id} at {touching_face.centroid.wkt}"
                    )

        return self.model

    def _touching_faces(self, geometry: BaseGeometry) -> list[LineString]:
        """Return line geometries representing shared basin boundary faces."""
        # Boundary intersections can contain empty lines or non-line geometry; only linework can become a face.
        line_geometries = [
            line_geometry for line_geometry in self._line_geometries(geometry) if line_geometry.length > 0
        ]
        if not line_geometries:
            return []

        if len(line_geometries) == 1:
            return line_geometries

        # Merge connected line fragments so one continuous face gets one ManningResistance node,
        # while clearly separated faces remain separate.
        # This avoids adding multiple ManningResistance nodes on the same face in case of minor topology artefacts that cause linework to be split into multiple fragments.
        unioned_geometry = unary_union(line_geometries)
        if isinstance(unioned_geometry, LineString):
            merged_geometry = unioned_geometry
        else:
            merged_geometry = linemerge(MultiLineString(self._line_geometries(unioned_geometry)))
        return self._line_geometries(merged_geometry)

    def _line_geometries(self, geometry: BaseGeometry) -> list[LineString]:
        """Return all line geometries contained in a geometry."""
        if geometry.is_empty:
            return []

        if isinstance(geometry, LineString):
            return [geometry]

        if isinstance(geometry, MultiLineString | GeometryCollection):
            return [
                line_geometry
                for sub_geometry in geometry.geoms
                for line_geometry in self._line_geometries(typing.cast(BaseGeometry, sub_geometry))
            ]

        return []

    def redirect_connectors_to_new_basins(self, newly_created_basins: list[int]):
        """Redirect links connected to the original basin to the nearest new split basin."""
        # find all connectors that are connected to the original basin
        assert self.model.link.df is not None
        assert self.model.node.df is not None
        assert self.model.basin.area.df is not None

        for connector_column, basin_column in [("to_node_id", "from_node_id"), ("from_node_id", "to_node_id")]:
            connector_node_ids_from_original_basin = self.model.link.df.loc[
                self.model.link.df[basin_column] == self.basin_node_id_to_split, connector_column
            ].dropna()

            # determine closest distance from the connector nodes to the new basins and redirect the connectors to the closest new basin
            for connector_node_id in connector_node_ids_from_original_basin:
                connector_node_id = int(connector_node_id)
                connector_node_geometry = typing.cast(
                    BaseGeometry, self.model.node.df.at[connector_node_id, "geometry"]
                )  # pyrefly

                closest_basin_node_id = None
                closest_distance = float("inf")

                # loop through each splitted basin part to determine the shortest distance
                for new_basin_node_id in newly_created_basins:
                    new_basin_geometry = self.model.basin.area.df.loc[
                        self.model.basin.area.df.node_id == new_basin_node_id,
                        "geometry",
                    ].iloc[0]
                    new_basin_geometry = typing.cast(BaseGeometry, new_basin_geometry)  # pyrefly

                    distance = connector_node_geometry.distance(new_basin_geometry)

                    # overwrite the closest distance and basin node_id
                    if distance < closest_distance:
                        closest_distance = distance
                        closest_basin_node_id = new_basin_node_id

                if self.printing:
                    print(f"Redirecting connector node {connector_node_id} to basin node {closest_basin_node_id}")
                if closest_basin_node_id is None:
                    raise ValueError(f"No closest basin found for connector node {connector_node_id}.")

                # redirect the connector node administratively to the closest new basin
                mask = (self.model.link.df[basin_column] == self.basin_node_id_to_split) & (
                    self.model.link.df[connector_column] == connector_node_id
                )
                self.model.link.df.loc[mask, basin_column] = closest_basin_node_id

                # redirect the connector node spatially to the closest new basin by changing the geometry of the connector node to the representative point of the closest new basin
                # retrieve the point geomeries to create a straight line between them
                geometry_connector_node = typing.cast(
                    Point, self.model.node.df.at[connector_node_id, "geometry"]
                )  # pyrefly
                geometry_basin_node = typing.cast(
                    Point, self.model.node.df.at[closest_basin_node_id, "geometry"]
                )  # pyrefly

                # create a straight line between the connector node and the closest new basin
                line_points = (
                    [geometry_basin_node, geometry_connector_node]
                    if basin_column == "from_node_id"
                    else [geometry_connector_node, geometry_basin_node]
                )
                new_geometry = LineString(line_points)
                self.model.link.df.loc[mask, "geometry"] = new_geometry  # pyrefly: ignore[unsupported-operation]
        return self.model

    def create_new_basins(self):
        """Create basin nodes and basin tables for each supplied split basin polygon."""
        # store the newly created basins for later use
        newly_created_basins = []

        # loop through the splitted basin gdf and create new basins in the model for each geometry (which has been splitted)
        for new_basin in self.splitted_basin_gdf.itertuples():
            geometry = typing.cast(BaseGeometry, new_basin.geometry)  # avoid pyrefly error
            new_node_id = self.model.next_node_id
            self.model.basin.add(
                Node(node_id=new_node_id, geometry=geometry.representative_point()),
                tables=[],
            )

            # copy all tables (including its metadata) to the new basin. Replace the basin geometry in the next step.
            self._copy_basin_tables(
                original_node_id=self.basin_node_id_to_split,
                new_node_id=new_node_id,
            )

            # replace the new basin geometry in the area table
            assert self.model.basin.area.df is not None  # pyrefly
            self.model.basin.area.df.loc[self.model.basin.area.df.node_id == new_node_id, "geometry"] = geometry  # pyrefly: ignore[unsupported-operation]

            newly_created_basins.append(new_node_id)
            if self.printing:
                print(
                    f"Split basin {self.basin_node_id_to_split}: created basin {new_node_id} "
                    f"from split feature node_id={getattr(new_basin, 'node_id', None)}, "
                    f"meta_node_id={getattr(new_basin, 'meta_node_id', None)}"
                )

        return newly_created_basins

    def _copy_basin_tables(self, original_node_id: int, new_node_id: int):
        """Copy all existing basin table rows from the original basin to a new basin node id."""
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


class NodeMetaCache:
    """Caching of 'meta_categorie'-data"""

    def __init__(self, model: Model):
        """Store the 'meta_categorie' as a series (node ID as index)."""
        self.meta_category = self.get_meta_category(model)

    @staticmethod
    def get_meta_category(model: Model) -> pd.Series:
        """Get the 'meta_categorie'-data."""
        assert model.node.df is not None
        nodes = model.node.df.copy(deep=True)
        return nodes["meta_categorie"]

    def set_meta_category(self, model: Model, fill_nan: bool = True) -> Model:
        """(Re)set the 'meta_category'-data.

        Optionally set all non-cached 'meta_categorie'-data to 'hoofdwater' (default).
        """
        assert model.node.df is not None
        nodes = model.node.df
        nodes["meta_categorie"] = self.meta_category.copy(deep=True)
        if fill_nan:
            nodes.loc[nodes["node_type"] == "Basin", "meta_categorie"] = nodes.loc[
                nodes["node_type"] == "Basin", "meta_categorie"
            ].fillna("hoofdwater")
        model.node.df = nodes.copy()  # pyrefly: ignore[bad-assignment]
        return model

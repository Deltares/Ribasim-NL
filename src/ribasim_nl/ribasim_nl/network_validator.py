from dataclasses import dataclass

import geopandas as gpd
from ribasim import Model


def within_distance(row, gdf, tolerance=1.0) -> bool:
    distance = gdf[gdf.index != row.name].distance(row.geometry)
    return (distance < tolerance).any()


def check_node_connectivity(row, node_df, tolerance=1.0) -> bool:
    invalid = True

    # handle zero-length edges
    if row.geometry.length == 0:
        point_from = point_to = row.geometry.centroid
    else:
        point_from, point_to = row.geometry.boundary.geoms

    # check if from_node_id is valid
    if row.from_node_id in node_df.index:
        distance = point_from.distance(node_df.at[row.from_node_id, "geometry"])
        invalid = distance > tolerance

    # if valid, check if to_node_id is valid
    if (not invalid) and (row.to_node_id in node_df.index):
        distance = point_to.distance(node_df.at[row.to_node_id, "geometry"])
        invalid = distance > tolerance

    return invalid


def check_internal_basin(row, edge_df) -> bool:
    if row.node_type == "Basin":
        return row.name not in edge_df.from_node_id.to_numpy()
    else:
        return False


@dataclass
class NetworkValidator:
    """Contains some methods for validating a RIBASIM-model

    Parameters
    ----------
    model: ribasim.Model
        Ribasim model
    tolerance: float (default=1)
        Tolerance to use for snapping. Should be in the units of the model.crs

    """

    model: Model
    tolerance: float = 1

    @property
    def node_df(self):
        return self.model.node_table().df

    @property
    def edge_df(self):
        return self.model.edge.df

    def node_overlapping(self):
        """Check if the node-geometry overlaps another node within tolerance (default=1m)"""
        return self.node_df[self.node_df.apply(lambda row: within_distance(row, self.node_df, self.tolerance), axis=1)]

    def node_duplicated(self):
        """Check if node_id is duplicated"""
        return self.node_df[self.node_df.node_id.duplicated()]

    def node_internal_basin(self):
        """Check if a Node with node_type Basin is not connected to another node"""
        mask = self.node_df.apply(lambda row: check_internal_basin(row, self.edge_df), axis=1)
        return self.node_df[mask]

    def node_invalid_connectivity(self, tolerance: float = 1.0):
        """Check if node_from and node_to are correct on edge"""
        node_df = self.node_df
        invalid_edges_df = self.edge_incorrect_connectivity()
        invalid_nodes = []
        for row in invalid_edges_df.itertuples():
            geoms = row.geometry.boundary.geoms

            for idx, attr in ((0, "from_node_id"), (1, "to_node_id")):
                node_id = getattr(row, attr)
                point = geoms[idx]
                if node_id in node_df.index:
                    if point.distance(node_df.at[node_id, "geometry"]) > tolerance:
                        invalid_nodes += [{"node_id": node_id, "geometry": point}]
                else:
                    invalid_nodes += [{"node_id": node_id, "geometry": point}]

        if invalid_nodes:
            df = gpd.GeoDataFrame(invalid_nodes, crs=node_df.crs)
            df.drop_duplicates(inplace=True)
        else:
            df = gpd.GeoDataFrame({"node_id": []}, geometry=gpd.GeoSeries(), crs=node_df.crs)

        return df

    def edge_duplicated(self):
        """Check if the `from_node_id` and `to_node_id` in the edge-table is duplicated"""
        return self.edge_df[self.edge_df.duplicated(subset=["from_node_id", "to_node_id"], keep=False)]

    def edge_missing_nodes(self):
        """Check if the `from_node_id` and `to_node_id` in the edge-table are both as node-id in the node-table"""
        mask = ~(
            self.edge_df.from_node_id.isin(self.node_df.node_id) & self.edge_df.to_node_id.isin(self.node_df.node_id)
        )
        return self.edge_df[mask]

    def edge_incorrect_from_node(self):
        """Check if the `from_node_type` in edge-table in matches the `node_type` of the corresponding node in the node-table"""
        node_df = self.node_df
        mask = ~self.edge_df.apply(
            lambda row: node_df.at[row["from_node_id"], "node_type"] == row["from_node_type"]
            if row["from_node_id"] in node_df.index
            else False,
            axis=1,
        )
        return self.edge_df[mask]

    def edge_incorrect_to_node(self):
        """Check if the `to_node_type` in edge-table in matches the `node_type` of the corresponding node in the node-table"""
        node_df = self.node_df
        mask = ~self.edge_df.apply(
            lambda row: node_df.at[row["to_node_id"], "node_type"] == row["to_node_type"]
            if row["to_node_id"] in node_df.index
            else False,
            axis=1,
        )
        return self.edge_df[mask]

    def edge_incorrect_connectivity(self):
        """Check if the geometries of the `from_node_id` and `to_node_id` are on the start and end vertices of the edge-geometry within tolerance (default=1m)"""
        node_df = self.node_df
        mask = self.edge_df.apply(
            lambda row: check_node_connectivity(row=row, node_df=node_df, tolerance=self.tolerance), axis=1
        )

        return self.edge_df[mask]

    def edge_incorrect_type_connectivity(self, from_node_type="ManningResistance", to_node_type="LevelBoundary"):
        """Check edges that contain wrong connectivity"""
        mask = (self.model.edge_from_node_type == from_node_type) & (self.model.edge_to_node_type == to_node_type)
        return self.edge_df[mask]

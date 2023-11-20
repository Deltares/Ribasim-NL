from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
from geopandas import GeoDataFrame
from networkx import Graph

"""
TODO: dissolve nodes within tolerance https://gis.stackexchange.com/questions/271733/geopandas-dissolve-overlapping-polygons
TODO: split line where node touches edge
"""


@dataclass
class Network:
    """Create a network from a GeoDataFrame with lines.

    Attributes
    ----------
    lines_gdf : GeoDataFrame
        GeoDataFrame with lines

    Methods
    -------
    nodes
        returns GeoDataFrame with the nodes in the network
    links
        returns GeoDataFrame with the links in the network
    graph
        returns networkx.Graph with nodes and links
    to_file(path)
        write nodes and links to a (multi-layer) file, e.g. GeoPackage
    """

    lines_gdf: GeoDataFrame

    _graph: Graph | None = None
    _nodes_gdf: GeoDataFrame | None = None
    _links_gdf: GeoDataFrame | None = None

    @property
    def nodes(self) -> GeoDataFrame:
        if self._nodes_gdf is None:
            self._nodes_gdf = gpd.GeoDataFrame(
                geometry=self.lines_gdf.boundary.explode(index_parts=True).unique(),
                crs=self.lines_gdf.crs,
            )
            self._nodes_gdf.index += 1
            self._nodes_gdf.index.name = "node_id"
        return self._nodes_gdf

    @property
    def links(self) -> GeoDataFrame:
        if self._links_gdf is None:
            self._links_gdf = GeoDataFrame(self.lines_gdf["geometry"])
            # self._links_gdf["node_from"] = None
            # self._links_gdf["node_to"] = None
        return self._links_gdf

    @property
    def graph(self) -> Graph:
        if self._graph is None:
            self._graph = Graph()

            # add nodes to graph
            for row in self.nodes.itertuples():
                self._graph.add_node(row.Index, geometry=row.geometry)

            for row in self.links.itertuples():
                # select nodes of interest
                nodes_select = self.nodes.iloc[
                    self.nodes.sindex.intersection(row.geometry.bounds)
                ]
                # get closest nodes
                point_from, point_to = row.geometry.boundary.geoms
                node_from = nodes_select.distance(point_from).sort_values().index[0]
                node_to = nodes_select.distance(point_to).sort_values().index[0]

                # add edge to graph
                self._graph.add_edge(
                    node_from,
                    node_to,
                    index=row.Index,
                    length=row.geometry.length,
                    geometry=row.geometry,
                )

                # add node_from, node_to to links
                self._links_gdf.loc[row.Index, ["node_from", "node_to"]] = (
                    node_from,
                    node_to,
                )
        return self._graph

    def to_file(self, path: str | Path):
        path = Path(path)
        # make sure graph is created
        _ = self.graph
        # write nodes and links
        self.nodes.to_file(path, layer="nodes")
        self.links.to_file(path, layer="links")

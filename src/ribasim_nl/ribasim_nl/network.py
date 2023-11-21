from dataclasses import dataclass
from pathlib import Path

from geopandas import GeoDataFrame, GeoSeries
from networkx import Graph
from shapely.geometry import LineString, box

"""
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
    tolerance: float | None = None

    _graph: Graph | None = None
    _nodes_gdf: GeoDataFrame | None = None
    _links_gdf: GeoDataFrame | None = None

    @property
    def nodes(self) -> GeoDataFrame:
        if self._nodes_gdf is None:
            geoseries = self.lines_gdf.boundary.explode(index_parts=True).unique()

            # snap nodes within tolerance if it's set. We:
            #  1. create polygons using buffer
            #  2. dissolving to a multipolygon using unary_union
            #  3. explode to individual polygons
            #  4. convert to points taking the centroid
            if self.tolerance is not None:
                geoseries = (
                    GeoSeries([geoseries.buffer(self.tolerance).unary_union()])
                    .explode(index_parts=False)
                    .reset_index(drop=True)
                    .centroid
                )

            # make it a GeoDataFrame
            self._nodes_gdf = GeoDataFrame(geometry=geoseries, crs=self.lines_gdf.crs)
            # let's start index at 1 and name it node_id
            self._nodes_gdf.index += 1
            self._nodes_gdf.index.name = "node_id"
        return self._nodes_gdf

    @property
    def links(self) -> GeoDataFrame:
        if self._links_gdf is None:
            self._links_gdf = GeoDataFrame(
                self.lines_gdf["geometry"], crs=self.lines_gdf.crs
            )
            _ = self.graph  # trigger links by creating a graph
        return self._links_gdf

    @property
    def graph(self) -> Graph:
        if self._graph is None:
            self._graph = Graph()

            # add nodes to graph
            for row in (
                self.nodes.itertuples()
            ):  # TODO: use self._graph.add_nodes_from may be even faster
                self._graph.add_node(row.Index, geometry=row.geometry)

            for row in self._links_gdf.itertuples():
                # select nodes of interest
                if self.tolerance:
                    bounds = box(*row.geometry.bounds).buffer(self.tolerance).bounds
                else:
                    bounds = row.geometry.bounds
                nodes_select = self.nodes.iloc[self.nodes.sindex.intersection(bounds)]
                # get closest nodes
                point_from, point_to = row.geometry.boundary.geoms
                node_from = nodes_select.distance(point_from).sort_values().index[0]
                node_to = nodes_select.distance(point_to).sort_values().index[0]

                # get geometry and (potentially) fix it if we use tolerance
                geometry = row.geometry
                # place first and last point if we have set tolerance

                if self.tolerance is not None:
                    point_from = nodes_select.loc[node_from].geometry
                    point_to = nodes_select.loc[node_to].geometry
                    geometry = LineString(
                        [(point_from.x, point_from.y)]
                        + geometry.coords[1:-1]
                        + [(point_to.x, point_to.y)]
                    )

                # add edge to graph
                self._graph.add_edge(
                    node_from,
                    node_to,
                    index=row.Index,
                    length=row.geometry.length,
                    geometry=geometry,
                )

                # add node_from, node_to to links
                self._links_gdf.loc[row.Index, ["node_from", "node_to", "geometry"]] = (
                    node_from,
                    node_to,
                    geometry,
                )
        return self._graph

    def reset(self):
        self._graph = None
        self._nodes_gdf = None
        self._links_gdf = None

    def to_file(self, path: str | Path):
        path = Path(path)
        # make sure graph is created
        _ = self.graph
        # write nodes and links
        self.nodes.to_file(path, layer="nodes")
        self.links.to_file(path, layer="links")

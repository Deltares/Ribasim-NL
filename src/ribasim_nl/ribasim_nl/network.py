from dataclasses import dataclass
from pathlib import Path

from geopandas import GeoDataFrame, GeoSeries
from networkx import Graph
from shapely.geometry import LineString, box
from shapely.ops import snap, split

GEOMETRIES_ALLOWED = ["LineString", "MultiLineString"]


@dataclass
class Network:
    """Create a network from a GeoDataFrame with lines.

    When creating nodes and links the following fixes aremade:
    - All nodes are snapped/dissolved within tolerance if set
    - Lines shorter than tolerance are dissolved into one nodes
    - Gaps between lines, within tolerance, are filled
    - If link-segments intersect nodes, these segments are split

    Attributes
    ----------
    lines_gdf : GeoDataFrame
        GeoDataFrame with LineStrings

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

    def __post_init__(self):
        # check if lines_gdf only contains allowed geometries
        geom_types = self.lines_gdf.geom_type.unique()
        if not all(i in GEOMETRIES_ALLOWED for i in geom_types):
            raise ValueError(
                f"Only geom_types {GEOMETRIES_ALLOWED} are allowed. Got {geom_types}"
            )
        # explode to LineString
        elif "MultiLineString" in geom_types:
            self.lines_gdf = self.lines_gdf.explode(index_parts=False)

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
                    GeoSeries([geoseries.buffer(self.tolerance / 2).unary_union()])
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
    def snap_tolerance(self):
        if self.tolerance:
            return self.tolerance
        else:
            return 0.01

    @property
    def links(self) -> GeoDataFrame:
        if self._links_gdf is None:
            _ = self.graph
        return self._links_gdf

    @property
    def graph(self) -> Graph:
        if self._graph is None:
            links_data = []

            # place first and last point if we have set tolerance
            def add_link(
                node_from, point_from, node_to, point_to, geometry, links_data
            ):
                if self.tolerance is not None:
                    geometry = LineString(
                        [(point_from.x, point_from.y)]
                        + geometry.coords[1:-1]
                        + [(point_to.x, point_to.y)]
                    )

                # add edge to graph
                self._graph.add_edge(
                    node_from,
                    node_to,
                    length=geometry.length,
                    geometry=geometry,
                )

                # add node_from, node_to to links
                links_data += [
                    {"node_from": node_from, "node_to": node_to, "geometry": geometry}
                ]

            self._graph = Graph()

            # add nodes to graph
            for row in self.nodes.itertuples():  # TODO: use feeding self.nodes as dict using self._graph.add_nodes_from may be faster
                self._graph.add_node(row.Index, geometry=row.geometry)

            for row in self.lines_gdf.itertuples():
                geometry = row.geometry

                # select nodes of interest
                if self.tolerance:
                    bounds = box(*geometry.bounds).buffer(self.tolerance).bounds
                else:
                    bounds = row.geometry.bounds
                nodes_select = self.nodes.iloc[self.nodes.sindex.intersection(bounds)]
                if self.tolerance is None:
                    nodes_select = nodes_select[nodes_select.distance(geometry) == 0]
                else:
                    nodes_select = nodes_select[
                        nodes_select.distance(geometry) <= self.tolerance
                    ]

                # Only one node. Skip edge. The geometry.length < self.tolerance, so start/end nodes have been dissolved
                if len(nodes_select) == 1:
                    continue

                # More than one node. We order selected nodes by distance from start_node
                nodes_select["distance"] = nodes_select.distance(
                    geometry.boundary.geoms[0]
                )
                nodes_select.sort_values("distance", inplace=True)

                # More than one node. We select start_node and point-geometry
                node_from = nodes_select.index[0]
                point_from = nodes_select.loc[node_from].geometry

                # More than two nodes. Line should be split into parts. We create one extra edge for every extra node
                if len(nodes_select) > 2:
                    for node in nodes_select[1:-1].itertuples():
                        node_to = node.Index
                        point_to = nodes_select.loc[node_to].geometry
                        edge_geometry, geometry = split(
                            snap(geometry, point_to, self.snap_tolerance), point_to
                        ).geoms
                        add_link(
                            node_from,
                            point_from,
                            node_to,
                            point_to,
                            edge_geometry,
                            links_data,
                        )
                        node_from = node_to
                        point_from = point_to

                # More than one node. We finish the (last) edge
                node_to = nodes_select.index[-1]
                point_to = nodes_select.loc[node_to].geometry
                add_link(
                    node_from,
                    point_from,
                    node_to,
                    point_to,
                    geometry,
                    links_data,
                )

            self._links_gdf = GeoDataFrame(links_data, crs=self.lines_gdf.crs)
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

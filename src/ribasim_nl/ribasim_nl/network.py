import logging
from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries
from networkx import DiGraph, Graph
from shapely.geometry import LineString, box
from shapely.ops import snap, split

from ribasim_nl.styles import add_styles_to_geopackage

logger = logging.getLogger(__name__)

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
    tolerance : float | None
        tolerance for snapping nodes and filling gaps. Defaults to None
    name_col: str
        column in lines_gdf to preserve as 'name' column in network-links. Defaults to 'name'
    id_col: str
        column in lines_gdf to preserve as 'id' column in network-links. Defaults to 'id'

    Methods
    -------
    from_lines_gpkg(gpkg_file, layer=None, **kwargs)
        returns a Network from a layer with LineStrings within a GeoPackage.
        Optionally the user specifies a layer in a multilayer-GeoPackage.
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
    name_col: str | None = None
    id_col: str | None = None
    tolerance: float | None = None

    _graph: DiGraph | None = field(default=None, repr=False)
    _graph_undirected: Graph | None = field(default=None, repr=False)

    def __post_init__(self):
        self.validate_inputs()

    def validate_inputs(self):
        """Validate if inputs are good-to-go for generating a graph"""
        # check if name_col and id_col are valid values
        for col in [self.name_col, self.id_col]:
            if (col is not None) & (col not in self.lines_gdf.columns):
                logger.warn(
                    f"{col} not a column in lines_gdf, input will be set to None"
                )
                col = None

        # check if lines_gdf only contains allowed geometries
        geom_types = self.lines_gdf.geom_type.unique()
        if not all(i in GEOMETRIES_ALLOWED for i in geom_types):
            raise ValueError(
                f"Only geom_types {GEOMETRIES_ALLOWED} are allowed. Got {geom_types}"
            )

        # explode to LineString
        elif "MultiLineString" in geom_types:
            self.lines_gdf = self.lines_gdf.explode(index_parts=False)

    @classmethod
    def from_lines_gpkg(cls, gpkg_file: str | Path, layer: str | None = None, **kwargs):
        """Instantiate class from a lines_gpkg"""
        lines_gdf = gpd.read_file(gpkg_file, layer=layer)
        return cls(lines_gdf, **kwargs)

    @classmethod
    def from_network_gpkg(cls, gpkg_file: str | Path, **kwargs):
        """Instantiate class from a network gpkg"""
        nodes_gdf = gpd.read_file(gpkg_file, layer="nodes", engine="pyogrio").set_index(
            "node_id"
        )
        links_gdf = gpd.read_file(gpkg_file, layer="links", engine="pyogrio").set_index(
            ["node_from", "node_to"]
        )
        graph = DiGraph()
        graph.add_nodes_from(nodes_gdf.to_dict(orient="index").items())
        graph.add_edges_from(
            [(k[0], k[1], v) for k, v in links_gdf.to_dict(orient="index").items()]
        )

        result = cls(links_gdf, **kwargs)
        result.set_graph(graph)
        return result

    @property
    def snap_tolerance(self):
        """Snap tolerance for shapely.ops.snap"""
        if self.tolerance:
            return self.tolerance
        else:
            return 0.01

    @property
    def links(self) -> GeoDataFrame:
        """Return graph links as GeoDataFrame"""
        # make sure we have a graph
        _ = self.graph
        return GeoDataFrame(
            [
                {"node_from": i[0], "node_to": i[1], **i[2]}
                for i in self.graph.edges.data()
            ],
            crs=self.lines_gdf.crs,
        )

    @property
    def nodes(self) -> GeoDataFrame:
        """Return graph nodes as GeoDataFrame"""
        # make sure we have a graph
        _ = self.graph
        gdf = GeoDataFrame.from_dict(
            {i[0]: i[1] for i in self.graph.nodes.data()},
            orient="index",
            crs=self.lines_gdf.crs,
        )
        gdf.index.name = "node_id"
        return gdf

    @property
    def graph_undirected(self) -> Graph:
        if self._graph_undirected is None:
            self._graph_undirected = Graph(self.graph)
        return self._graph_undirected

    @property
    def graph(self) -> DiGraph:
        if self._graph is None:
            # see if input is valid
            self.validate_inputs()

            # place first and last point if we have set tolerance
            def add_link(
                node_from, point_from, node_to, point_to, geometry, id=None, name=None
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
                    name=name,
                    id=id,
                    length=geometry.length,
                    geometry=geometry,
                )

            self._graph = DiGraph()

            # add nodes to graph
            nodes_gdf = self.get_nodes()
            for row in nodes_gdf.itertuples():
                self._graph.add_node(row.Index, geometry=row.geometry)

            # add edges using link_def
            link_def = {}
            for row in self.lines_gdf.itertuples():
                geometry = row.geometry

                # provide id and name attributes if any
                if self.id_col is not None:
                    link_def["id"] = getattr(row, self.id_col)
                if self.name_col is not None:
                    link_def["name"] = getattr(row, self.name_col)

                # select nodes of interest
                if self.tolerance:
                    bounds = box(*geometry.bounds).buffer(self.tolerance).bounds
                else:
                    bounds = row.geometry.bounds
                nodes_select = nodes_gdf.iloc[nodes_gdf.sindex.intersection(bounds)]
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
                nodes_select["distance"] = nodes_select.geometry.apply(
                    lambda x: geometry.project(x)
                )
                nodes_select.sort_values("distance", inplace=True)

                # More than one node. We select start_node and point-geometry
                link_def["node_from"] = nodes_select.index[0]
                link_def["point_from"] = nodes_select.loc[
                    link_def["node_from"]
                ].geometry

                # More than two nodes. Line should be split into parts. We create one extra edge for every extra node
                if len(nodes_select) > 2:
                    for node in nodes_select[1:-1].itertuples():
                        link_def["node_to"] = node.Index
                        link_def["point_to"] = nodes_select.loc[
                            link_def["node_to"]
                        ].geometry
                        edge_geometry, geometry = split(
                            snap(geometry, link_def["point_to"], self.snap_tolerance),
                            link_def["point_to"],
                        ).geoms
                        link_def["geometry"] = edge_geometry
                        add_link(**link_def)
                        link_def["node_from"] = link_def["node_to"]
                        link_def["point_from"] = link_def["point_to"]

                # More than one node. We finish the (last) edge
                link_def["node_to"] = nodes_select.index[-1]
                link_def["point_to"] = nodes_select.loc[link_def["node_to"]].geometry
                link_def["geometry"] = geometry
                add_link(**link_def)

            # Set all node-types
            self.set_node_types()

        return self._graph

    def reset(self):
        self._graph = None

    def set_graph(self, graph: DiGraph):
        """Set graph directly"""
        self._graph = graph

    def get_nodes(self) -> GeoDataFrame:
        """Get nodes from lines_gdf

        Approach if tolerance is set:
        1. create polygons using buffer (tolerance/2)
        2. dissolving to a multipolygon using unary_union
        3. explode to individual polygons
        4. convert to points taking the centroid

        Returns
        -------
        GeoDataFrame
            GeoDataFrame with nodes and index
        """
        geoseries = self.lines_gdf.boundary.explode(index_parts=True).unique()

        # snap nodes within tolerance if it's set. We:

        if self.tolerance is not None:
            geoseries = (
                GeoSeries([geoseries.buffer(self.tolerance / 2).unary_union()])
                .explode(index_parts=False)
                .reset_index(drop=True)
                .centroid
            )

        # make it a GeoDataFrame
        nodes_gdf = GeoDataFrame(geometry=geoseries, crs=self.lines_gdf.crs)
        # let's start index at 1 and name it node_id
        nodes_gdf.index += 1
        nodes_gdf.index.name = "node_id"
        return nodes_gdf

    def set_node_types(self):
        """Node types to seperate boundaries from connections"""
        from_nodes = {i[0] for i in self.graph.edges}
        to_nodes = {i[1] for i in self.graph.edges}
        us_boundaries = [i for i in from_nodes if i not in to_nodes]
        ds_boundaries = [i for i in to_nodes if i not in from_nodes]
        for node_id in self._graph.nodes:
            if node_id in us_boundaries:
                self._graph.nodes[node_id]["type"] = "upstream_boundary"
            elif node_id in ds_boundaries:
                self._graph.nodes[node_id]["type"] = "downstream_boundary"
            else:
                self._graph.nodes[node_id]["type"] = "connection"

    def to_file(self, path: str | Path):
        """Write output to geopackage"""
        path = Path(path)
        if path.suffix.lower() != ".gpkg":
            raise ValueError(
                f"{path} is not a GeoPackage, please provide a file with extention 'gpkg'"
            )

        # write nodes and links
        self.nodes.to_file(path, layer="nodes", engine="pyogrio")
        self.links.to_file(path, layer="links", engine="pyogrio")
        # add styles
        add_styles_to_geopackage(path)

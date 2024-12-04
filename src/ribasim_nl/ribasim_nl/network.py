import logging
from collections import Counter
from dataclasses import dataclass, field
from itertools import chain, product
from pathlib import Path

import geopandas as gpd
import networkx as nx
import pandas as pd
from geopandas import GeoDataFrame, GeoSeries
from networkx import DiGraph, Graph, NetworkXNoPath, shortest_path, traversal
from shapely import force_2d
from shapely.geometry import LineString, Point, box
from shapely.ops import snap, split

from ribasim_nl.styles import add_styles_to_geopackage

logger = logging.getLogger(__name__)

GEOMETRIES_ALLOWED = ["LineString", "MultiLineString"]


def stop_iter(first_value, second_value):
    # stop iteration if neither value is None and values are unequal
    if all((pd.notna(first_value), pd.notna(second_value))):
        return first_value != second_value
    else:
        return False


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
                logger.warn(f"{col} not a column in lines_gdf, input will be set to None")
                col = None

        # check if lines_gdf only contains allowed geometries
        geom_types = self.lines_gdf.geom_type.unique()
        if not all(i in GEOMETRIES_ALLOWED for i in geom_types):
            raise ValueError(f"Only geom_types {GEOMETRIES_ALLOWED} are allowed. Got {geom_types}")

        # explode to LineString
        elif "MultiLineString" in geom_types:
            self.lines_gdf = self.lines_gdf.explode(index_parts=False)

        # remove z-coordinates
        self.lines_gdf.loc[:, "geometry"] = gpd.GeoSeries(
            force_2d(self.lines_gdf.geometry.array), crs=self.lines_gdf.crs
        )

    @classmethod
    def from_lines_gpkg(cls, gpkg_file: str | Path, layer: str | None = None, **kwargs):
        """Instantiate class from a lines_gpkg"""
        lines_gdf = gpd.read_file(gpkg_file, layer=layer)
        return cls(lines_gdf, **kwargs)

    @classmethod
    def from_network_gpkg(cls, gpkg_file: str | Path, **kwargs):
        """Instantiate class from a network gpkg"""
        nodes_gdf = gpd.read_file(gpkg_file, layer="nodes", engine="pyogrio").set_index("node_id")
        links_gdf = gpd.read_file(gpkg_file, layer="links", engine="pyogrio").set_index(["node_from", "node_to"])
        graph = DiGraph()
        graph.add_nodes_from(nodes_gdf.to_dict(orient="index").items())
        graph.add_edges_from([(k[0], k[1], v) for k, v in links_gdf.to_dict(orient="index").items()])

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
            [{"node_from": i[0], "node_to": i[1], **i[2]} for i in self.graph.edges.data()],
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
                    nodes_select = nodes_select[nodes_select.distance(geometry) <= self.tolerance]

                # Only one node. Skip edge. The geometry.length < self.tolerance, so start/end nodes have been dissolved
                if len(nodes_select) == 1:
                    continue

                # More than one node. We order selected nodes by distance from start_node
                nodes_select["distance"] = nodes_select.geometry.apply(lambda x: geometry.project(x))
                nodes_select.sort_values("distance", inplace=True)

                # More than one node. We select start_node and point-geometry
                link_def["node_from"] = nodes_select.index[0]
                link_def["point_from"] = nodes_select.loc[link_def["node_from"]].geometry

                # More than two nodes. Line should be split into parts. We create one extra edge for every extra node
                if len(nodes_select) > 2:
                    for node in nodes_select[1:-1].itertuples():
                        link_def["node_to"] = node.Index
                        link_def["point_to"] = nodes_select.loc[link_def["node_to"]].geometry
                        edge_geometry, geometry = split(
                            snap(geometry, link_def["point_to"], self.snap_tolerance),
                            link_def["point_to"],
                        ).geoms
                        link_def["geometry"] = edge_geometry
                        self.add_link(**link_def)
                        link_def["node_from"] = link_def["node_to"]
                        link_def["point_from"] = link_def["point_to"]

                # More than one node. We finish the (last) edge
                link_def["node_to"] = nodes_select.index[-1]
                link_def["point_to"] = nodes_select.loc[link_def["node_to"]].geometry
                link_def["geometry"] = geometry
                self.add_link(**link_def)

            # Set all node-types
            self.set_node_types()

        return self._graph

    def add_link(
        self,
        node_from,
        node_to,
        geometry,
        point_from=None,
        point_to=None,
        id=None,
        name=None,
    ):
        """Add a link (edge) to the network"""
        if self.tolerance is not None:
            geometry = LineString([(point_from.x, point_from.y)] + geometry.coords[1:-1] + [(point_to.x, point_to.y)])

        # add edge to graph
        self._graph.add_edge(
            node_from,
            node_to,
            name=name,
            id=id,
            length=geometry.length,
            geometry=geometry,
        )

    def overlay(self, gdf):
        cols = ["node_id"] + [i for i in gdf.columns if i != "geometry"]
        gdf_overlay = gpd.overlay(self.nodes.reset_index(), gdf, how="intersection")[cols]
        gdf_overlay = gdf_overlay[~gdf_overlay.duplicated(subset="node_id")]

        for row in gdf_overlay.itertuples():
            attrs = {k: v for k, v in row._asdict().items() if k not in ["Index", "node_id"]}
            for k, v in attrs.items():
                self._graph.nodes[row.node_id][k] = v

        self._graph_undirected = None

    def upstream_nodes(self, node_id):
        return [n for n in traversal.bfs_tree(self._graph, node_id, reverse=True) if n != node_id]

    def downstream_nodes(self, node_id):
        return [n for n in traversal.bfs_tree(self._graph, node_id) if n != node_id]

    def has_upstream_nodes(self, node_id):
        return len(self.upstream_nodes(node_id)) > 0

    def has_downstream_nodes(self, node_id):
        return len(self.downstream_nodes(node_id)) > 0

    def find_upstream(self, node_id, attribute, max_iters=10, include_node_id=False):
        upstream_nodes = self.upstream_nodes(node_id)
        max_iters = min(max_iters, len(upstream_nodes))
        value = None
        for idx in range(max_iters):
            node = self._graph.nodes[upstream_nodes[idx]]
            if attribute in node.keys():
                if pd.notna(node[attribute]):
                    value = node[attribute]
                    break
        if include_node_id:
            return value, upstream_nodes[idx]
        else:
            return value

    def find_downstream(self, node_id, attribute, max_iters=10, include_node_id=False):
        downstream_nodes = self.downstream_nodes(node_id)
        max_iters = min(max_iters, len(downstream_nodes))
        value = None
        for idx in range(max_iters):
            node = self._graph.nodes[downstream_nodes[idx]]
            if attribute in node.keys():
                if pd.notna(node[attribute]):
                    value = node[attribute]
                    break
        if include_node_id:
            return value, downstream_nodes[idx]
        else:
            return value

    def get_downstream(self, node_id, attribute, max_iters=5):
        downstream_nodes = self.downstream_nodes(node_id)

        # get max_iters, as we search downstream, our list should be even and double max_iters
        nbr_nodes = len(downstream_nodes)
        if nbr_nodes % 2 == 1:
            nbr_nodes -= 1
        max_iters = min(nbr_nodes, max_iters * 2)

        first_value = None
        second_value = None

        for idx in range(0, max_iters, 2):
            first_node = self._graph.nodes[downstream_nodes[idx]]
            second_node = self._graph.nodes[downstream_nodes[idx + 1]]

            if attribute in first_node.keys():
                if pd.notna(first_node[attribute]):
                    first_value = first_node[attribute]
                if stop_iter(first_value, second_value):
                    break

            if attribute in second_node.keys():
                if pd.notna(pd.notna(second_node[attribute])):
                    second_value = second_node[attribute]
                if stop_iter(first_value, second_value):
                    break

        return first_value, second_value

    def get_upstream_downstream(self, node_id, attribute, max_iters=5, max_length=2000):
        # determine upstream and downstream nodes
        upstream_nodes = self.upstream_nodes(node_id)
        downstream_nodes = self.downstream_nodes(node_id)

        max_iters = min(max_iters, len(upstream_nodes), len(downstream_nodes))

        upstream_value = None
        downstream_value = None

        for idx in range(max_iters):
            if (
                nx.shortest_path_length(
                    self.graph,
                    upstream_nodes[idx],
                    downstream_nodes[idx],
                    weight="length",
                )
                > max_length
            ):
                break
            us_node = self._graph.nodes[upstream_nodes[idx]]
            ds_node = self._graph.nodes[downstream_nodes[idx]]

            if attribute in us_node.keys():
                if pd.notna(us_node[attribute]):
                    upstream_value = us_node[attribute]
                if stop_iter(upstream_value, downstream_value):
                    break

            if attribute in ds_node.keys():
                if pd.notna(pd.notna(ds_node[attribute])):
                    downstream_value = ds_node[attribute]
                if stop_iter(upstream_value, downstream_value):
                    break

        if upstream_value == downstream_value:
            return None, None
        else:
            return upstream_value, downstream_value

    def move_node(
        self,
        point: Point,
        max_distance: float,
        align_distance: float,
        node_types=["connection", "upstream_boundary", "downstream_boundary"],
    ):
        """Move network nodes and edges to new location

        Parameters
        ----------
        point : Point
            Point to move node to
        max_distance : float
            Max distance to find closes node
        align_distance : float
            Distance over edge, from node, where vertices will be removed to align adjacent edges with Point
        """
        # take links and nodes as gdf
        nodes_gdf = self.nodes
        links_gdf = self.links

        # get closest connection-node
        distances = nodes_gdf[nodes_gdf["type"].isin(node_types)].distance(point).sort_values()
        node_id = distances.index[0]
        node_distance = distances.iloc[0]

        # check if node is within max_distance
        if node_distance <= max_distance:
            # update graph node
            self.graph.nodes[node_id]["geometry"] = point

            # update start-node of edges
            edges_from = links_gdf[links_gdf.node_from == node_id]
            for edge in edges_from.itertuples():
                geometry = edge.geometry

                # take first node from point
                coords = list(point.coords)

                # take all in between boundaries only if > REMOVE_VERT_DIST
                for coord in list(self.graph.edges[(edge.node_from, edge.node_to)]["geometry"].coords)[1:-1]:
                    if geometry.project(Point(coord)) > align_distance:
                        coords += [coord]

                # take the last from original geometry
                coords += [geometry.coords[-1]]

                self.graph.edges[(edge.node_from, edge.node_to)]["geometry"] = LineString(coords)

            # update end-node of edges
            edges_from = links_gdf[links_gdf.node_to == node_id]
            for edge in edges_from.itertuples():
                geometry = edge.geometry

                # take first from original geometry
                coords = [geometry.coords[0]]

                # take all in between boundaries only if > REMOVE_VERT_DIST
                geometry = geometry.reverse()
                for coord in list(self.graph.edges[(edge.node_from, edge.node_to)]["geometry"].coords)[1:-1]:
                    if geometry.project(Point(coord)) > align_distance:
                        coords += [coord]

                # take the last from point
                coords += [(point.x, point.y)]

                self.graph.edges[(edge.node_from, edge.node_to)]["geometry"] = LineString(coords)
            return node_id
        else:
            logger.warning(
                f"No Node moved. Closest node: {node_id}, distance > max_distance ({node_distance} > {max_distance})"
            )
            return None

    def add_node(self, point: Point, max_distance: float, align_distance: float = 100):
        # set _graph undirected to None
        self._graph_undirected = None

        # get links
        links_gdf = self.links

        # get closest edge and distances
        distances = links_gdf.distance(point).sort_values()
        edge_id = distances.index[0]
        edge_distance = distances.iloc[0]
        edge_geometry = links_gdf.at[edge_id, "geometry"]  # noqa: PD008
        node_from = links_gdf.at[edge_id, "node_from"]  # noqa: PD008
        node_to = links_gdf.at[edge_id, "node_to"]  # noqa: PD008

        if edge_distance <= max_distance:
            # add node
            node_id = max(self.graph.nodes) + 1
            node_geometry = edge_geometry.interpolate(edge_geometry.project(point))
            self.graph.add_node(node_id, geometry=node_geometry, type="connection")

            # add edges
            self.graph.remove_edge(node_from, node_to)
            us_geometry, ds_geometry = split(snap(edge_geometry, node_geometry, 0.01), node_geometry).geoms
            self.add_link(node_from, node_id, us_geometry)
            self.add_link(node_id, node_to, ds_geometry)

            return self.move_node(point, max_distance=max_distance, align_distance=align_distance)
        else:
            logger.warning(
                f"No Node added. Closest edge: {edge_id}, distance > max_distance ({edge_distance} > {max_distance})"
            )
            return None

    def reset(self):
        self._graph = None

    def set_graph(self, graph: DiGraph):
        """Set graph directly"""
        self._graph = graph

    def get_path(self, node_from, node_to, directed=True, weight="length"):
        if directed:
            try:
                return shortest_path(self.graph, node_from, node_to, weight=weight)
            except NetworkXNoPath:
                print(f"search path undirected between {node_from} and {node_to}")
                return shortest_path(self.graph_undirected, node_from, node_to, weight=weight)
        else:
            return shortest_path(self.graph_undirected, node_from, node_to, weight=weight)

    def get_links(self, node_from, node_to, directed=True, weight="length"):
        # get path and edges on path
        path = self.get_path(node_from, node_to, directed=directed, weight=weight)
        edges_on_path = list(zip(path[:-1], path[1:]))

        try:
            return self.links.set_index(["node_from", "node_to"]).loc[edges_on_path]
        except KeyError:  # if path only undirected we need to fix edges_on_path
            idx = self.links.set_index(["node_from", "node_to"]).index
            edges_on_path = [i if i in idx else (i[1], i[0]) for i in edges_on_path]
            return self.links.set_index(["node_from", "node_to"]).loc[edges_on_path]

    def subset_links(self, nodes_from, nodes_to):
        gdf = pd.concat([self.get_links(node_from, node_to) for node_from, node_to in product(nodes_from, nodes_to)])
        gdf = gdf.reset_index().drop_duplicates(["node_from", "node_to"]).reset_index()
        return gdf

    def subset_nodes(
        self,
        nodes_from,
        nodes_to,
        inclusive=True,
        directed=True,
        duplicated_nodes=True,
        weight="length",
        ignore_links=[],
    ):
        def find_duplicates(lst, counts):
            counter = Counter(lst)
            duplicates = [item for item, count in counter.items() if count == counts]
            return duplicates

        paths = [
            self.get_path(node_from, node_to, directed, weight)
            for node_from, node_to in product(nodes_from, nodes_to)
            if (node_from, node_to) not in ignore_links
        ]

        node_ids = list(chain(*paths))
        if duplicated_nodes:
            node_ids = find_duplicates(node_ids, len(paths))
        else:
            node_ids = list(set(chain(*paths)))
        if not inclusive:
            exclude_nodes = nodes_from + nodes_to
            node_ids = [i for i in node_ids if i not in exclude_nodes]
        return self.nodes.loc[node_ids]

    def _get_coordinates(self, node_from, node_to):
        # get geometries from links
        reverse = False
        links = self.links
        geometries = links.loc[(links.node_from == node_from) & (links.node_to == node_to), ["geometry"]]
        if geometries.empty:
            geometries = links.loc[
                (links.node_from == node_to) & (links.node_to == node_from),
                ["geometry"],
            ]
            if not geometries.empty:
                reverse = True
            else:
                raise ValueError(f"{node_from}, {node_to} not valid start and end nodes in the network")

        # select geometry
        if len(geometries) > 1:
            idx = geometries.length.sort_values(ascending=False).index[0]
            geometry = geometries.loc[idx].geometry
        elif len(geometries) == 1:
            geometry = geometries.iloc[0].geometry

        # invert geometry
        if reverse:
            geometry = geometry.reverse()

        return list(geometry.coords)

    def path_to_line(self, path):
        coords = []
        for node_from, node_to in zip(path[0:-1], path[1:]):
            coords += self._get_coordinates(node_from, node_to)
        return LineString(coords)

    def add_weight(self, attribute, value, weight_value=1000000):
        nodes = self.nodes[self.nodes[attribute] == value].index
        links = self.links.set_index(["node_from", "node_to"], drop=False)[["node_from", "node_to", "length"]]
        mask = ~(links.node_from.isin(nodes) & links.node_to.isin(nodes))
        weight = links.length
        weight.loc[mask] = weight[mask] * weight_value
        weight_values = weight.to_dict()
        nx.set_edge_attributes(self._graph, weight_values, "weight")

        self._graph_undirected = None

    def get_line(self, node_from, node_to, directed=True, weight="length"):
        path = self.get_path(node_from, node_to, directed, weight)
        return self.path_to_line(path)

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
            raise ValueError(f"{path} is not a GeoPackage, please provide a file with extention 'gpkg'")

        # write nodes and links
        self.nodes.to_file(path, layer="nodes", engine="pyogrio")
        self.links.to_file(path, layer="links", engine="pyogrio")
        # add styles
        add_styles_to_geopackage(path)

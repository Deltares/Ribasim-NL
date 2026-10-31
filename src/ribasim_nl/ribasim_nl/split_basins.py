"""Split basins in a Ribasim model, based on pre-split basin polygons, or along a route."""

import math
import typing
from collections.abc import Sequence
from itertools import combinations
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import shapely.affinity
from pandera.typing.geopandas import GeoDataFrame
from ribasim import Node
from ribasim.geometry.node import NodeSchema
from ribasim.nodes import basin, manning_resistance
from shapely.geometry import GeometryCollection, LineString, MultiLineString, MultiPolygon, Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import linemerge, split, substring, unary_union

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
                )

                closest_basin_node_id = None
                closest_distance = float("inf")

                # loop through each splitted basin part to determine the shortest distance
                for new_basin_node_id in newly_created_basins:
                    new_basin_geometry = self.model.basin.area.df.loc[
                        self.model.basin.area.df.node_id == new_basin_node_id,
                        "geometry",
                    ].iloc[0]
                    new_basin_geometry = typing.cast(BaseGeometry, new_basin_geometry)

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
                geometry_connector_node = typing.cast(Point, self.model.node.df.at[connector_node_id, "geometry"])
                geometry_basin_node = typing.cast(Point, self.model.node.df.at[closest_basin_node_id, "geometry"])

                # create a straight line between the connector node and the closest new basin
                line_points = (
                    [geometry_basin_node, geometry_connector_node]
                    if basin_column == "from_node_id"
                    else [geometry_connector_node, geometry_basin_node]
                )
                new_geometry = LineString(line_points)
                self.model.link.df.loc[mask, "geometry"] = new_geometry  # ty: ignore[invalid-assignment]
        return self.model

    def create_new_basins(self):
        """Create basin nodes and basin tables for each supplied split basin polygon."""
        # store the newly created basins for later use
        newly_created_basins = []

        # loop through the splitted basin gdf and create new basins in the model for each geometry (which has been splitted)
        for new_basin in self.splitted_basin_gdf.itertuples():
            geometry = typing.cast(BaseGeometry, new_basin.geometry)
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
            assert self.model.basin.area.df is not None
            self.model.basin.area.df.loc[self.model.basin.area.df.node_id == new_node_id, "geometry"] = geometry  # ty: ignore[invalid-assignment]

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
            table.df = pd.concat([table.df, rows], ignore_index=True)  # ty: ignore[invalid-assignment]


class NodeMetaCache:
    """Caching of 'meta_categorie'-data"""

    def __init__(self, model: Model):
        """Store the 'meta_categorie' as a series (node ID as index)."""
        self.meta_category = self.get_meta_category(model)

    @staticmethod
    def get_meta_category(model: Model) -> pd.Series:
        """Get the 'meta_categorie'-data."""
        nodes = model.node.df.copy(deep=True)
        return nodes["meta_categorie"]

    def set_meta_category(self, model: Model, fill_nan: bool = True) -> Model:
        """(Re)set the 'meta_category'-data.

        Optionally set all non-cached 'meta_categorie'-data to 'hoofdwater' (default).
        """
        nodes = model.node.df
        nodes["meta_categorie"] = self.meta_category.copy(deep=True)
        if fill_nan:
            nodes.loc[nodes["node_type"] == "Basin", "meta_categorie"] = nodes.loc[
                nodes["node_type"] == "Basin", "meta_categorie"
            ].fillna("hoofdwater")
        model.node.df = GeoDataFrame[NodeSchema](nodes.copy())
        return model


def _cross_section(route: LineString, distance: float, polygon: BaseGeometry, length: float) -> LineString:
    """The line across `polygon`, perpendicular to `route` at `distance` along it, from boundary to boundary."""
    point = route.interpolate(distance)
    a, b = route.interpolate(max(distance - 10.0, 0.0)), route.interpolate(min(distance + 10.0, route.length))
    norm = math.hypot(b.x - a.x, b.y - a.y)
    assert norm > 0, f"The route has no direction at {point}"
    across = ((a.y - b.y) / norm * length / 2, (b.x - a.x) / norm * length / 2)
    line = LineString([(point.x - across[0], point.y - across[1]), (point.x + across[0], point.y + across[1])])
    parts = [g for g in shapely.get_parts(line.intersection(polygon)) if g.distance(point) < 1e-6]
    if len(parts) != 1 or not isinstance(parts[0], LineString):
        raise ValueError(f"Cannot cut the Basin area across the route at {point}")
    return parts[0]


def _split_across(
    geometry: BaseGeometry, cut: LineString, route: LineString, distance: float
) -> tuple[BaseGeometry, BaseGeometry]:
    """Split `geometry` with `cut` in the parts before and after `distance` along `route`.

    Parts of `geometry` that the route does not pass, such as harbours, join the nearest of both.
    """
    # extend the cut by a meter at both ends, so it crosses the boundary
    extended = shapely.affinity.scale(cut, xfact=1 + 2 / cut.length, yfact=1 + 2 / cut.length, origin="center")
    pieces = typing.cast(list[BaseGeometry], list(shapely.get_parts(split(geometry, extended))))
    before_point, after_point = route.interpolate(distance - 1.0), route.interpolate(distance + 1.0)
    (before,) = [p for p in pieces if p.contains(before_point)]
    (after,) = [p for p in pieces if p.contains(after_point)]
    assert before is not after, f"The cut at {cut.centroid} does not split the Basin area"
    groups: tuple[list[BaseGeometry], list[BaseGeometry]] = ([before], [after])
    for piece in pieces:
        if piece is not before and piece is not after:
            groups[piece.distance(after) < piece.distance(before)].append(piece)
    return shapely.union_all(groups[0]), shapely.union_all(groups[1])


def _join(geometry: LineString, route: LineString, from_connector: bool, tolerance: float = 1.0) -> tuple[float, int]:
    """Where a flow link of a Basin joins `route`: its first vertex from the connector within `tolerance` of it.

    :param geometry: geometry of the link
    :param from_connector: whether the link flows from the connector into the Basin, so the connector is at its start
    :return: the distance along `route` and the index of the vertex, or of the connector if no vertex is close enough
    """
    points = [Point(c) for c in geometry.coords]
    indices = range(len(points)) if from_connector else range(len(points) - 1, -1, -1)
    index = next((i for i in indices if route.distance(points[i]) <= tolerance), indices[0])
    return route.project(points[index]), index


def _along_route(route: LineString, start: float, end: float) -> list[tuple[float, ...]]:
    """The coordinates of `route` from `start` to `end` along it, also when `end` lies before `start`."""
    return list(substring(route, start, end).coords)


def _join_line(coords: list[tuple[float, ...]]) -> LineString:
    """A line through `coords`, without repeated points."""
    return LineString([c for i, c in enumerate(coords) if i == 0 or c != coords[i - 1]])


def split_basin_along_route(
    model: Model,
    basin_id: int,
    route: LineString,
    split_points: Sequence[Point],
    basin_names: Sequence[str],
    manning_names: Sequence[str],
    *,
    manning_n: float,
    profile_slope: float,
    cross_section_length: float = 5000.0,
) -> tuple[list[int], list[int]]:
    """Split a Basin in sections along `route`, connected in the direction of `route` by ManningResistances.

    `route` runs through the Basin area in the direction of flow, for instance along the links of its main channel.
    The ManningResistances lie on it at the points nearest to `split_points`, where the Basin area is cut across it.
    The section that contains the Basin node keeps it; the others get a new Basin node on the route, halfway between
    where their inflows and outflows join it, so their links do not run back along the route. Each section gets the
    profile of the Basin with its area scaled by the share of the section in the length of the route within the
    Basin, so the volume is divided in proportion, and copies of the other tables of the Basin. The flow links of the
    Basin, which keep their IDs, move to the section where they join the route, and run along the route to its node.
    The ManningResistances get `manning_n`, `profile_slope`, the width of the Basin area across the route, and the
    length of their links, which run along the route from Basin node to Basin node.

    :param model: model to update in place
    :param basin_id: Basin to split, with a single area
    :param route: line through the Basin area in the direction of flow
    :param split_points: where to split the Basin, in the order of the route
    :param basin_names: names of the sections, one more than `split_points`
    :param manning_names: names of the ManningResistances, one per split point
    :param manning_n: Manning roughness of the ManningResistances [s/m^(1/3)]
    :param profile_slope: profile slope of the ManningResistances [-]
    :param cross_section_length: maximum width of the Basin area across the route [m]
    :return: the Basin node IDs of the sections and the ManningResistance node IDs, in the order of the route
    """
    assert model.basin.area.df is not None and model.basin.profile.df is not None
    if model.get_node_type(basin_id) != "Basin":
        raise ValueError(f"Node {basin_id} is not a Basin")
    if not split_points or len(basin_names) != len(split_points) + 1 or len(manning_names) != len(split_points):
        raise ValueError("Expected split points, one more Basin name and one ManningResistance name per split point")
    (area_fid,) = model.basin.area.df.index[model.basin.area.df.node_id == basin_id]
    polygon = typing.cast(BaseGeometry, model.basin.area.df.at[area_fid, "geometry"])
    cuts = [route.project(point) for point in split_points]
    if not (np.diff(cuts) > 0).all():
        raise ValueError(f"The split points of Basin {basin_id} are not in the order of the route")
    for cut in cuts:
        if not polygon.contains(route.interpolate(cut)):
            raise ValueError(f"The split point {route.interpolate(cut)} lies outside Basin {basin_id}")

    # the sections, between the cuts across the route
    cross_sections = [_cross_section(route, cut, polygon, cross_section_length) for cut in cuts]
    sections: list[BaseGeometry] = []
    remaining = polygon
    for cut, cross_section in zip(cuts, cross_sections, strict=True):
        before, remaining = _split_across(remaining, cross_section, route, cut)
        sections.append(before)
    sections.append(remaining)
    assert math.isclose(sum(s.area for s in sections), polygon.area, rel_tol=1e-6), "The sections lose area"
    inside = [g for g in shapely.get_parts(route.intersection(polygon)) if isinstance(g, LineString)]
    bounds = [
        min(route.project(Point(g.coords[0])) for g in inside),
        *cuts,
        max(route.project(Point(g.coords[-1])) for g in inside),
    ]
    lengths = np.array([route.intersection(section).length for section in sections])
    assert (lengths > 0).all(), f"The route does not pass all sections of Basin {basin_id}"
    fractions = lengths / lengths.sum()

    # the flow links of the Basin, by the section in which they join the route
    link_df = model.link.df
    flow = link_df[
        (link_df.link_type == "flow") & ((link_df.from_node_id == basin_id) | (link_df.to_node_id == basin_id))
    ]
    joins = {}
    for link_id, row in flow.iterrows():
        inflow = row.to_node_id == basin_id
        distance, vertex = _join(typing.cast(LineString, row.geometry), route, from_connector=inflow)
        section = int(np.searchsorted(cuts, distance))
        joins[link_id] = (inflow, distance, vertex, section)

    # the Basin node of each section
    basin_point = typing.cast(Point, model.node.df.at[basin_id, "geometry"])
    (kept,) = [i for i, section in enumerate(sections) if section.buffer(1e-6).contains(basin_point)]
    distances = []
    for i in range(len(sections)):
        if i == kept:
            distances.append(route.project(basin_point))
            continue
        inflows = [d for inflow, d, _, s in joins.values() if s == i and inflow]
        outflows = [d for inflow, d, _, s in joins.values() if s == i and not inflow]
        low, high = max([bounds[i], *inflows]), min([bounds[i + 1], *outflows])
        if high <= low:
            low, high = bounds[i], bounds[i + 1]
        distances.append((low + high) / 2)
    if not (np.diff(distances) > 0).all() or any(not bounds[i] < d < bounds[i + 1] for i, d in enumerate(distances)):
        raise ValueError(f"The Basin nodes of the sections of Basin {basin_id} are not in their sections")

    # copy the Basin to the new sections, with the profile scaled by their length
    node_row = model.node.df.loc[basin_id]
    meta = {str(c): v for c, v in node_row.items() if str(c).startswith("meta_") and pd.notna(v)}
    profile = model.basin.profile.df[model.basin.profile.df.node_id == basin_id]
    other_tables = [t for t in model.basin._tables() if t.tablename() not in ("Basin / profile", "Basin / area")]
    basin_ids = []
    for i, (section, fraction, name) in enumerate(zip(sections, fractions, basin_names, strict=True)):
        if i == kept:
            node_id = basin_id
        else:
            node_id = model.next_node_id
            model.basin.add(
                Node(node_id, route.interpolate(distances[i]), name=name, **meta),
                [
                    basin.Profile(level=profile.level.to_list(), area=(profile.area * fraction).to_list()),
                    basin.Area(geometry=[MultiPolygon(shapely.get_parts(section))]),
                ],
            )
            for table in other_tables:
                df = table.df
                if df is None or "node_id" not in df.columns or not (df.node_id == basin_id).any():
                    continue
                if table.tablename().startswith("Basin / subgrid"):
                    raise NotImplementedError(f"Basin {basin_id} has a subgrid, which cannot be split")
                table.df = pd.concat([df, df[df.node_id == basin_id].assign(node_id=node_id)], ignore_index=True)
        basin_ids.append(node_id)
    # adding Basins replaces the tables, so they are updated after
    model.node.df.loc[basin_id, "name"] = basin_names[kept]
    area_df = model.basin.area.df
    area_df.loc[[area_fid], "geometry"] = gpd.GeoSeries(
        [MultiPolygon(shapely.get_parts(sections[kept]))], index=[area_fid], crs=area_df.crs
    )
    model.basin.profile.df.loc[profile.index, "area"] = profile.area * fractions[kept]

    # move the flow links to the section where they join the route
    for link_id, (inflow, distance, vertex, section) in joins.items():
        if section == kept:
            continue
        coords = list(typing.cast(LineString, link_df.at[link_id, "geometry"]).coords)
        if inflow:
            geometry = _join_line(coords[: vertex + 1] + _along_route(route, distance, distances[section]))
            link_df.loc[link_id, "to_node_id"] = basin_ids[section]
        else:
            geometry = _join_line(_along_route(route, distances[section], distance) + coords[vertex:])
            link_df.loc[link_id, "from_node_id"] = basin_ids[section]
        # the stubs of pandas do not allow geometries as values
        typing.cast(typing.Any, link_df.loc)[link_id, "geometry"] = geometry

    # connect the sections with ManningResistances
    manning_ids = []
    for i, (cut, cross_section, name) in enumerate(zip(cuts, cross_sections, manning_names, strict=True)):
        upstream = LineString(_along_route(route, distances[i], cut))
        downstream = LineString(_along_route(route, cut, distances[i + 1]))
        node = model.manning_resistance.add(
            Node(model.next_node_id, route.interpolate(cut), name=name),
            [
                manning_resistance.Static(
                    length=[upstream.length + downstream.length],
                    manning_n=[manning_n],
                    profile_width=[cross_section.length],
                    profile_slope=[profile_slope],
                )
            ],
        )
        model.link.add(model.basin[basin_ids[i]], node, geometry=upstream, name=basin_names[i])
        model.link.add(node, model.basin[basin_ids[i + 1]], geometry=downstream, name=basin_names[i + 1])
        manning_ids.append(node.node_id)
    return basin_ids, manning_ids

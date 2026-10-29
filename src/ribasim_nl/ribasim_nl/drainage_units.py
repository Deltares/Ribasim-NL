"""Derive Basin areas from drainage units (afwateringseenheden) computed from the AHN, such as those of waterlagen.

A drainage unit is the area that drains to one waterway segment. Each segment is assigned to the Basin whose current
area contains most of its length, and each drainage unit to the Basin of its segment. The new area of a Basin is the
union of its drainage units, within the boundary of the water authority. Parts of the current areas that no drainage
unit covers keep their Basin.

See https://d2hydro.github.io/waterlagen/bewerkte-lagen/afwateringseenheden/
"""

import geopandas as gpd
import pandas as pd
import shapely
from geopandas import GeoDataFrame
from pandera.typing.geopandas import GeoDataFrame as SchemaGeoDataFrame
from ribasim.geometry.area import BasinAreaSchema
from shapely.geometry.base import BaseGeometry

from ribasim_nl.model import Model


def assign_segments_to_basins(segments: GeoDataFrame, basin_areas: GeoDataFrame) -> pd.Series:
    """Return the Basin of each segment: the one whose area contains most of its length.

    :param segments: waterway segments, with a unique `segment_id`
    :param basin_areas: Basin areas, with a `node_id`
    :return: `node_id` by `segment_id`, for the segments that lie at least partly in a Basin area
    """
    assert segments.segment_id.is_unique, "segment_id is not unique"
    pieces = gpd.overlay(
        segments[["segment_id", "geometry"]],
        basin_areas[["node_id", "geometry"]],
        how="intersection",
        keep_geom_type=True,
    )
    pieces["length"] = pieces.length
    longest = pieces.loc[pieces.groupby("segment_id")["length"].idxmax()]
    return longest.set_index("segment_id")["node_id"]


def assign_drainage_units_to_basins(
    drainage_units: GeoDataFrame, segments: GeoDataFrame, basin_areas: GeoDataFrame
) -> GeoDataFrame:
    """Assign each drainage unit to the Basin of its segment, or else to the Basin area it overlaps most.

    :param drainage_units: drainage units, with the `segment_id` they drain to
    :param segments: waterway segments, with a unique `segment_id`
    :param basin_areas: Basin areas, with a `node_id`
    :return: the drainage units with a `node_id`, and an `assigned_by` of "segment" or "overlap"; units that neither
        reaches a Basin are dropped
    """
    units = drainage_units[["segment_id", "geometry"]].copy()
    units["node_id"] = units.segment_id.map(assign_segments_to_basins(segments, basin_areas))
    units["assigned_by"] = "segment"

    unassigned = units[units.node_id.isna()]
    if not unassigned.empty:
        overlap = gpd.overlay(
            unassigned.reset_index(names="unit")[["unit", "geometry"]],
            basin_areas[["node_id", "geometry"]],
            how="intersection",
            keep_geom_type=True,
        )
        overlap["area"] = overlap.area
        largest = overlap.loc[overlap.groupby("unit")["area"].idxmax()].set_index("unit")["node_id"]
        units.loc[largest.index, "node_id"] = largest
        units.loc[largest.index, "assigned_by"] = "overlap"

    units = units[units.node_id.notna()].copy()
    units["node_id"] = units.node_id.astype("int32")
    return units


def basin_areas_from_drainage_units(
    basin_areas: GeoDataFrame,
    drainage_units: GeoDataFrame,
    segments: GeoDataFrame,
    boundary: BaseGeometry,
    min_part_area: float = 10_000.0,
) -> GeoDataFrame:
    """Return new Basin areas: the drainage units of each Basin, plus the parts of its current area they leave.

    :param basin_areas: current Basin areas, one per `node_id`
    :param drainage_units: drainage units, with the `segment_id` they drain to
    :param segments: waterway segments, with a unique `segment_id`
    :param boundary: area of the water authority, outside which no Basin area is added
    :param min_part_area: polygons smaller than this, in m², are dropped, except the largest of each Basin; these are
        mostly slivers between the drainage units and the current areas
    :return: one area per `node_id` of `basin_areas`, with a MultiPolygon geometry
    """
    assert basin_areas.node_id.is_unique, "Basin areas are not unique per node_id"
    assert basin_areas.crs is not None and basin_areas.crs.equals(drainage_units.crs), "CRS of inputs differ"

    units = assign_drainage_units_to_basins(drainage_units, segments, basin_areas)
    units["geometry"] = units.intersection(boundary)
    units = units[~units.is_empty]
    from_units = units.dissolve(by="node_id")["geometry"]
    covered = gpd.GeoSeries(from_units).union_all()

    # parts of the current areas that no drainage unit covers keep their Basin
    rest = basin_areas.set_index("node_id")["geometry"].difference(covered)
    combined = pd.concat([from_units, rest[~rest.is_empty]])
    combined = combined.groupby(level=0).agg(lambda geoms: shapely.union_all(geoms.values))
    geometry = gpd.GeoSeries(combined, crs=basin_areas.crs).make_valid()
    geometry = gpd.GeoSeries([_polygonal(g, min_part_area) for g in geometry], index=geometry.index, crs=geometry.crs)

    result = gpd.GeoDataFrame(
        {"node_id": geometry.index.astype("int32")}, geometry=geometry.values, crs=basin_areas.crs
    )
    result = result[~result.is_empty]
    missing = set(basin_areas.node_id) - set(result.node_id)
    assert not missing, f"Basins without area: {sorted(missing)}"
    return result


def _polygonal(geometry: BaseGeometry, min_part_area: float) -> shapely.MultiPolygon:
    """Keep the polygons of a geometry of at least `min_part_area`, and at least the largest, as a MultiPolygon."""
    parts = shapely.get_parts(geometry)
    polygons = [p for p in parts if isinstance(p, shapely.Polygon)]
    polygons += [q for p in parts if isinstance(p, shapely.MultiPolygon) for q in p.geoms]
    if not polygons:
        return shapely.MultiPolygon()
    largest = max(polygons, key=lambda p: p.area)
    return shapely.MultiPolygon([p for p in polygons if p is largest or p.area >= min_part_area])


def replace_basin_areas(model: Model, drainage_units: GeoDataFrame, segments: GeoDataFrame, boundary: BaseGeometry):
    """Replace the Basin areas of the model by those from drainage units, see `basin_areas_from_drainage_units`.

    :param model: model with one Basin area per Basin
    :param drainage_units: drainage units, with the `segment_id` they drain to
    :param segments: waterway segments, with a unique `segment_id`
    :param boundary: area of the water authority
    """
    area_df = model.basin.area.df
    assert area_df is not None
    current = area_df[["node_id", "geometry"]].dissolve(by="node_id").reset_index()
    areas = basin_areas_from_drainage_units(current, drainage_units, segments, boundary)
    meta_columns = [c for c in area_df.columns if c.startswith("meta_")]
    meta = area_df[["node_id", *meta_columns]].drop_duplicates("node_id")
    areas = areas.merge(meta, on="node_id", how="left")
    areas.index.name = "fid"
    old, new = area_df.area.sum() / 1e6, areas.area.sum() / 1e6
    print(f"Basin areas from drainage units: {old:.1f} km² before, {new:.1f} km² after")
    model.basin.area.df = SchemaGeoDataFrame[BasinAreaSchema](areas)

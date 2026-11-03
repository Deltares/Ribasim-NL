"""The waterways of North Rhine-Westphalia, for routes through Germany that the Dutch datasets do not cover.

GSK3E is the Gewässerstationierungskarte of LANUK, see
https://www.opengeodata.nrw.de/produkte/umwelt_klima/wasser/oberflaechengewaesser/gsk3e/
"""

import typing

import geopandas as gpd
import pandas as pd
import shapely
import shapely.ops

from ribasim_nl.settings import settings

GSK3E_ZIP = settings.ribasim_nl_data_dir / "Basisgegevens/GSK3E/gsk3e_EPSG25832_Shape.zip"
# the named waterways, with a code, and all other waterways
NAMED_WATERWAYS = "gsk3e_gewkz_line_name"
OTHER_WATERWAYS = "gsk3e_sonstige_line_basisdlm"


def read_layer(layer: str, *, mask: shapely.Geometry | None = None, where: str | None = None) -> gpd.GeoDataFrame:
    """Read a layer of GSK3E, in the CRS of the models.

    :param layer: name of the shapefile in the archive, without extension
    :param mask: area to read, in the CRS of the models, defaults to None (everything)
    :param where: SQL filter on the attributes, defaults to None
    :return: the features of the layer
    """
    assert GSK3E_ZIP.exists(), f"{GSK3E_ZIP} not found, pull it with DVC"
    mask_series = None if mask is None else gpd.GeoSeries([mask], crs="EPSG:28992")
    gdf = gpd.read_file(f"zip://{GSK3E_ZIP}!Eingangsdaten/shapefile/{layer}.shp", mask=mask_series, where=where)
    return gdf.to_crs("EPSG:28992")


def read_waterways(mask: shapely.Geometry) -> gpd.GeoDataFrame:
    """Read all waterways that intersect a mask, for `extra_waterways` of `ribasim_nl.network_snapping`.

    :param mask: area to read
    :return: single-part 2D lines
    """
    frames = [read_layer(layer, mask=mask)[["geometry"]] for layer in (NAMED_WATERWAYS, OTHER_WATERWAYS)]
    lines = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs).explode(ignore_index=True)
    lines = lines[lines.geom_type == "LineString"].reset_index(drop=True)
    return lines.set_geometry(shapely.force_2d(lines.geometry.to_numpy()), crs=lines.crs)


def read_reach(code: str, name: str) -> shapely.LineString:
    """Read a reach of a named waterway, drawn in the direction of the flow.

    The waterways of GSK3E are stationed from their mouth, so they are drawn in upstream direction.

    :param code: code of the waterway, GEWKZ3E
    :param name: name of the reach, ABSNAME
    :return: line of the reach
    """
    reaches = read_layer(NAMED_WATERWAYS, where=f"GEWKZ3E = '{code}' AND ABSNAME = '{name}'")
    assert len(reaches) == 1, f"Expected one reach {name} of waterway {code}, found {len(reaches)}"
    line = shapely.force_2d(reaches.geometry.iloc[0])
    return typing.cast(shapely.LineString, line.reverse())


def line_between(line: shapely.LineString, start: shapely.Point, end: shapely.Point) -> shapely.LineString:
    """The part of a line between the points nearest to two points, connected to these points.

    :param line: line, drawn from `start` to `end`
    :param start: first point
    :param end: last point
    :return: line that starts at `start` and ends at `end`
    """
    from_distance, to_distance = line.project(start), line.project(end)
    assert from_distance < to_distance, f"{start} must lie before {end} along the line"
    part = typing.cast(shapely.LineString, shapely.ops.substring(line, from_distance, to_distance))
    return shapely.LineString([start, *part.coords[1:-1], end])

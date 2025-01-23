# %%
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiPolygon, Polygon

from ribasim_nl import CloudStorage
from ribasim_nl.geometry import split_basin, split_basin_multi_polygon

authority = "BrabantseDelta"
cloud = CloudStorage()

area_df = gpd.read_file(cloud.joinpath(authority, "verwerkt", "basin_gaten.gpkg"), fid_as_index=True)
cut_lines_df = gpd.read_file(cloud.joinpath(authority, "verwerkt", "knip_basin_gaten.gpkg"), fid_as_index=True)


new_area_df = gpd.GeoSeries([], crs=area_df.crs)

for area_fid, cut_lines_select_df in cut_lines_df.groupby("poly_fid"):
    # iterate trough cut_lines per area creating a series with geometries
    series = gpd.GeoSeries([area_df.at[area_fid, "geometry"]], crs=area_df.crs)

    for row in cut_lines_select_df.itertuples():
        if not len(series[series.intersects(row.geometry)]) == 1:
            raise ValueError(f"line with fid {row.Index} intersects basin with fid {area_fid} more than once")

        # split polygon into two polygons
        fid = series[series.intersects(row.geometry)].index[0]
        area_poly = series.loc[fid]
        if isinstance(area_poly, MultiPolygon):
            try:
                result = split_basin_multi_polygon(series.loc[fid], line=row.geometry)
            except ValueError:
                result = split_basin_multi_polygon(series.loc[fid], line=LineString(row.geometry.boundary.geoms))
        elif isinstance(area_poly, Polygon):
            result = split_basin(area_poly, line=row.geometry)
            result = list(result.geoms)

        # original geometry gets first polygon
        series.loc[fid] = result[0]

        # we add the second polygon to the series
        series = pd.concat([series, gpd.GeoSeries([result[1]], crs=series.crs)], ignore_index=True)

    new_basin_area_df = pd.concat([new_area_df, series], ignore_index=True)

new_basin_area_df.to_file(cloud.joinpath(authority, "verwerkt", "basin_gaten_geknipt.gpkg"))

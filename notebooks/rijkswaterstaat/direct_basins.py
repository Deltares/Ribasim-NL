# %%
import geopandas as gpd
from ribasim_nl import CloudStorage

cloud = CloudStorage()


# %% Create file with direction basins
# Load GeoPackage files with explicit geometry column name

poly_column = "owmident"
line_column = "Name"

poly_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken.gpkg")
)

lines_gdf = gpd.read_file(
    cloud.joinpath("Basisgegevens", "lsm3-j18_5v6", "shapes", "network_Branches.shp")
)
lines_gdf.set_crs(28992, inplace=True)

# %%
data = []
# poly_row = poly_gdf.set_index("owmident").loc["NL94_1"]
# poly_ident = "NL94_1"
for poly_row in poly_gdf.itertuples():
    poly_ident = getattr(poly_row, poly_column)

    ## select intersecting lines
    intersecting_lines_gdf = lines_gdf.iloc[
        lines_gdf.sindex.intersection(poly_row.geometry.bounds)
    ]
    intersecting_lines_gdf = intersecting_lines_gdf[
        intersecting_lines_gdf["geometry"].intersects(poly_row.geometry.boundary)
    ]

    def find_intersecting_basins(line):
        intersecting_poly_gdf = poly_gdf.iloc[poly_gdf.sindex.intersection(line.bounds)]
        intersecting_poly_gdf = intersecting_poly_gdf[
            intersecting_poly_gdf["geometry"].intersects(line)
        ]
        return intersecting_poly_gdf[poly_column]

    def find_ident(point):
        # filter by spatial index
        containing_poly_gdf = poly_gdf.iloc[poly_gdf.sindex.intersection(point.bounds)]

        # filter by contain and take first row
        containing_poly_gdf = containing_poly_gdf[
            containing_poly_gdf["geometry"].contains(point)
        ]

        if containing_poly_gdf.empty:
            return None
        elif len(containing_poly_gdf) == 1:
            return getattr(containing_poly_gdf.iloc[0], poly_column)
        else:
            print(containing_poly_gdf)

    # line_row = intersecting_lines_gdf.loc[75]
    for line_row in intersecting_lines_gdf.itertuples():
        us_point, ds_point = line_row.geometry.boundary.geoms

        us_ident = find_ident(us_point)
        ds_ident = find_ident(ds_point)

        if ((us_ident is not None) and (ds_ident is not None)) and (
            us_ident != ds_ident
        ):
            line_ident = getattr(line_row, line_column)

            if poly_ident not in [us_ident, ds_ident]:
                data += [
                    {
                        "line_ident": line_ident,
                        "us_basin": us_ident,
                        "ds_basin": poly_ident,
                        "geometry": line_row.geometry,
                    },
                    {
                        "line_ident": line_ident,
                        "us_basin": poly_ident,
                        "ds_basin": ds_ident,
                        "geometry": line_row.geometry,
                    },
                ]
            elif len(find_intersecting_basins(line_row.geometry)) == 2:
                data += [
                    {
                        "line_ident": line_ident,
                        "us_basin": us_ident,
                        "ds_basin": ds_ident,
                        "geometry": line_row.geometry,
                    }
                ]
            else:
                print(f"we miss a case {line_ident}")

poly_directions_gdf = gpd.GeoDataFrame(data, crs=28992)

poly_directions_gdf.drop_duplicates(
    ["us_basin", "ds_basin"], keep="first", inplace=True
)
poly_directions_gdf.to_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_verbindingen.gpkg")
)

# %%

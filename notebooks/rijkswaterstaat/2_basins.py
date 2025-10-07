# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl.geodataframe import split_basins
from ribasim_nl.raster import sample_level_area
from shapely.geometry import MultiLineString, MultiPolygon, Polygon

from ribasim_nl import CloudStorage

cloud = CloudStorage()

# %%

DEFAULT_PROFILE = pd.DataFrame(
    data={
        "area": [0.01, 1000.0],
        "level": [0.0, 1.0],
    }
)

# input
raster_path = cloud.joinpath("Rijkswaterstaat/verwerkt/bathymetrie/bathymetrie-merged.tif")
watervlak_gpkg = cloud.joinpath("Rijkswaterstaat/verwerkt/categorie_oppervlaktewater.gpkg")
basins_user_data_gpkg = cloud.joinpath("Rijkswaterstaat/verwerkt/basins_user_data.gpkg")
osm_basins_path = cloud.joinpath("Rijkswaterstaat/verwerkt/oppervlaktewater_belgie.gpkg")

cloud.synchronize(filepaths=[raster_path, watervlak_gpkg, basins_user_data_gpkg, osm_basins_path])

# output
basins_gpkg = cloud.joinpath("Rijkswaterstaat/verwerkt/basins.gpkg")
basin_profile_path = cloud.joinpath("Rijkswaterstaat/verwerkt/basins_level_area.csv")

watervlak_diss_gdf = gpd.read_file(watervlak_gpkg, layer="watervlak", fid_as_index=True)
cut_lines_gdf = gpd.read_file(basins_user_data_gpkg, layer="cut_lines", fid_as_index=True)
merge_lines_gdf = gpd.read_file(basins_user_data_gpkg, layer="merge_lines", fid_as_index=True)
add_basins_gdf = gpd.read_file(basins_user_data_gpkg, layer="add_basins", fid_as_index=True)

osm_basins_gdf = gpd.read_file(
    osm_basins_path,
    fid_as_index=True,
)

# %%
print("dissolve")

add_basins_gdf.loc[:, ["categorie"]] = "nationaal hoofdwater"
osm_basins_gdf.loc[:, ["categorie"]] = "nationaal hoofdwater"
watervlak_diss_gdf = pd.concat([watervlak_diss_gdf, osm_basins_gdf, add_basins_gdf], ignore_index=True)

data = {"naam": [], "geometry": []}
for name, df in watervlak_diss_gdf[watervlak_diss_gdf.categorie == "nationaal hoofdwater"].groupby(by="naam"):
    # dissolve touching polygons (magic!)
    geometry = df.geometry.buffer(0.1).union_all().buffer(-0.1)
    # make sure we have a list of single polygons
    if isinstance(geometry, MultiPolygon):
        geometries = list(geometry.geoms)
    else:
        geometries = [geometry]  # that is 1 Polygon
    # add to data
    data["geometry"] += geometries
    data["naam"] += [name] * len(geometries)

basins_gdf = gpd.GeoDataFrame(data, crs=28992)
basins_gdf.index += 1
basins_gdf.name = "fid"

basins_gdf.to_file(basins_gpkg, layer="dissolved_basins")

# %%
print("split basins")
basins_gdf = split_basins(basins_gdf, cut_lines_gdf)
basins_gdf.to_file(basins_gpkg, layer="split_basins")

# %%
print("merge basins")

for line in merge_lines_gdf.itertuples():
    point_from, point_to = line.geometry.boundary.geoms
    try:
        idx_from = basins_gdf[basins_gdf.contains(point_from)].index[0]
    except IndexError:
        raise ValueError(f"line with index {line.Index} does not start in a polygon")
    try:
        idx_to = basins_gdf[basins_gdf.contains(point_to)].index[0]
    except IndexError:
        raise ValueError(f"line with index {line.Index} does not end in a polygon")
    if idx_from == idx_to:
        print(f"line with index {line.Index} is contained within a polygon")
    basins_gdf.loc[idx_to, ["geometry"]] = basins_gdf.loc[[idx_from, idx_to]].union_all()
    basins_gdf = basins_gdf[basins_gdf.index != idx_from]

basins_gdf.to_file(basins_gpkg, layer="merged_basins")

# %%
min_area = 350000
ignore_basins = [
    "Noordervaart",
    "Kanaal Briegden - Neerharen",
    "Zuid-Willemsvaart",
    "Twentekanaal (Kanaal Zutphen-Enschede)",
    "Wilhelminakanaal",
    "Máximakanaal",
    "Julianakanaal",
]
large_basins_mask = basins_gdf.area > min_area
large_basins_mask = large_basins_mask | basins_gdf.naam.isin(ignore_basins)
large_basins_gdf = basins_gdf[large_basins_mask]
snap_distance = 0.1


for basin in basins_gdf[~large_basins_mask].itertuples():
    basin_candidates_gdf = basins_gdf[basins_gdf.distance(basin.geometry) < snap_distance]
    if basin_candidates_gdf.empty:
        raise ValueError(
            f"basin met naam {basin.naam} kan niet auto gemerged worden. Zorg dat er een polygon naast ligt"
        )
    else:
        idx = basin_candidates_gdf.area.sort_values(ascending=False).index[0]
        geom = basin_candidates_gdf.at[idx, "geometry"].union(basin.geometry)
        basins_gdf.loc[idx, "geometry"] = geom

basins_gdf = basins_gdf[large_basins_mask]
basins_gdf.to_file(basins_gpkg)

# %% for ribasim
print("for ribasim")


# gaten uit basins verwijderen
def exterior_polygon(geometry):
    boundary = geometry.boundary
    if isinstance(boundary, MultiLineString):
        return MultiPolygon([Polygon(i) for i in boundary.geoms])
    else:
        return Polygon(boundary)


basins_gdf.loc[:, ["geometry"]] = basins_gdf.geometry.apply(lambda x: exterior_polygon(x))

# remove "internal boundaries"
basins_gdf.loc[:, ["geometry"]] = basins_gdf.buffer(0.1).buffer(-0.1)
basins_gdf = basins_gdf[basins_gdf.geometry.area > 1]
# # re-index and add basin_id
basins_gdf.reset_index(inplace=True, drop=True)
basins_gdf.loc[:, ["basin_id"]] = basins_gdf.index + 1
basins_gdf.to_file(basins_gpkg, layer="ribasim_basins")
# %% sample profiles

elevation_basins_gdf = gpd.read_file(basins_user_data_gpkg, layer="hoogtes", fid_as_index=True)

elevation_basins_gdf = elevation_basins_gdf.dropna()
dfs = []
for row in basins_gdf.itertuples():
    invert_area = row.geometry.area
    try:
        df = sample_level_area(raster_path, row.geometry, ident=row.basin_id)
        # if we don't cover 55% we better use an elevation point
        if df.area.max() < invert_area * 0.55:
            print(f"{df.area.max()}")
            raise IndexError("area is too small")
        elif df.area.max() < invert_area * 0.95:
            # we make sure we extend our profile to the polygon area
            df.loc[df.index.max() + 1] = {
                "area": invert_area,
                "level": df.level.max() + 0.1,
            }
        dfs += [df]
    except Exception:  # handle missing bathymetry (need new!)
        elevation_df = elevation_basins_gdf[elevation_basins_gdf.within(row.geometry)]
        if not elevation_df.empty:
            print(f"WARNING: profile elevation points for {row.basin_id}")
            invert_level = elevation_df.streefpeil.max()
            bottom_level = elevation_df.bodemhoogte.min()
            if bottom_level > invert_level:
                raise ValueError(f"bottom_level > invert_level: {bottom_level} > {invert_level}")
            bottom_area = row.geometry.buffer(-((invert_level - bottom_level) * 2)).area

            df = pd.DataFrame(
                data={
                    "area": [bottom_area, invert_area],
                    "level": [bottom_level, invert_level],
                }
            )
        else:
            print(f"WARNING: default-profile for {row.basin_id}. Check data!")
            df = DEFAULT_PROFILE.copy()

        df["id"] = row.basin_id
        dfs += [df]

df = pd.concat(dfs)

df.to_csv(basin_profile_path)

# %%

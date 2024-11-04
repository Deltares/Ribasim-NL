# %%
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage

cloud = CloudStorage()

basins_osm_gpkg = cloud.joinpath("Basisgegevens", "OSM", "Belgie", "water_areas.gpkg")

basins_user_data_gpkg = cloud.joinpath("Rijkswaterstaat", "verwerkt", "basins_user_data.gpkg")

rivers_osm_gpkg = cloud.joinpath("Basisgegevens", "OSM", "Nederland_Belgie", "waterway_river.gpkg")
canal_osm_gpkg = cloud.joinpath("Basisgegevens", "OSM", "Nederland_Belgie", "waterway_canal.gpkg")

# %%
clip_poly = gpd.read_file(
    basins_user_data_gpkg, layer="clip_osm_basins", engine="pyogrio", fid_as_index=True
).unary_union

basins_osm_gdf = (
    gpd.read_file(basins_osm_gpkg, engine="pyogrio", fid_as_index=True).clip(clip_poly).explode(ignore_index=True)
)

canal_osm_gdf = gpd.read_file(canal_osm_gpkg, engine="pyogrio", fid_as_index=True)
rivers_osm_gdf = gpd.read_file(rivers_osm_gpkg, engine="pyogrio", fid_as_index=True)


water_lines = pd.concat([canal_osm_gdf, rivers_osm_gdf])


for row in basins_osm_gdf.itertuples():
    df = water_lines[water_lines.intersects(row.geometry)].clip(row.geometry)
    df = df[df.name.notna()]
    df["length"] = df.geometry.length
    df.sort_values("length", ascending=False, inplace=True)
    if not df.empty:
        basins_osm_gdf.loc[row.Index, ["naam"]] = df["name"].to_list()[0]

basins_osm_gdf.to_file(
    cloud.joinpath("Rijkswaterstaat", "Verwerkt", "oppervlaktewater_belgie.gpkg"),
    engine="pyogrio",
)

# %%

# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage, Network

cloud = CloudStorage()


# %% Create file with direction basins
# Load GeoPackage files with explicit geometry column name

poly_column = "owmident"
line_column = "Name"


kunstwerken_gdf = gpd.read_file(
    cloud.joinpath(
        "Rijkswaterstaat", "aangeleverd", "kunstwerken_primaire_waterkeringen.gpkg"
    )
)


# %%
# Load the original geopackage

# Select values
rws_kunstwerken_gdf = kunstwerken_gdf[kunstwerken_gdf["rws"] == "JA"]
kunstwerken_primair_gdf = kunstwerken_gdf[
    (
        kunstwerken_gdf["kd_type_om"].isin(
            [
                "gemaal",
                "stuw",
                "inlaatsluis",
                "keersluis",
                "spuisluis",
                "hevel",
                "duiker",
                "schutsluis",
            ]
        )
    )
]
overige_kunstwerken_primair_gdf = kunstwerken_gdf[
    (
        ~kunstwerken_gdf["kd_type_om"].isin(
            [
                "gemaal",
                "stuw",
                "inlaatsluis",
                "keersluis",
                "spuisluis",
                "hevel",
                "duiker",
                "schutsluis",
            ]
        )
    )
]

kunstwerken_primair_gdf.to_file(
    cloud.joinpath(
        "Rijkswaterstaat", "verwerkt", "kunstwerken_primaire_waterkeringen.gpkg"
    )
)
overige_kunstwerken_primair_gdf.to_file(
    cloud.joinpath(
        "Rijkswaterstaat", "verwerkt", "overige_kunstwerken_primaire_waterkeringen.gpkg"
    )
)
rws_kunstwerken_gdf.to_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "rws_kunstwerken.gpkg")
)

# %% Merge network


# Read GeoPackage files
krw_basins_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken.gpkg")
)
vaarwegen_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "aangeleverd", "nwb_vaarwegvakken.gpkg")
)

print("read rivers")
river_osm_gdf = gpd.read_file(
    cloud.joinpath("OSM", "aangeleverd", "waterway_river_the_netherlands.gpkg")
)

print("read canals")
canal_osm_gdf = gpd.read_file(
    cloud.joinpath("OSM", "aangeleverd", "waterway_canals_the_netherlands.gpkg")
)

# Filter 'krw_basins_gdf' based on the specified attribute values
attribute_values = [
    "NL92_KETELMEER_VOSSEMEER",
    "NL92_ZWARTEMEER",
    "NL92_RANDMEREN_OOST",
    "NL92_RANDMEREN_ZUID",
    "NL95_1A",
    "NL89_westsde",
    "NL89_oostsde",
    "NL89_zoommedt",
    "NL89_volkerak",
    "NL89_grevlemr",
    "NL94_11",
    "Haringvliet-oost",
]

attribute_values_extra = [
    "NL92_MARKERMEER",
    "NL92_IJSSELMEER",
]
filtered_vaarwegen_basins_gdf = krw_basins_gdf[
    krw_basins_gdf["owmident"].isin(attribute_values)
]
filtered_osm_basins_gdf = krw_basins_gdf[
    ~krw_basins_gdf["owmident"].isin(attribute_values)
]


# Clip GeoDataFrames to the extent of krw_basins_gdf
print("clip rivers")
river_osm_gdf_clipped = gpd.clip(
    river_osm_gdf, filtered_osm_basins_gdf.geometry.unary_union
)
print("clip canals")
canal_osm_gdf_clipped = gpd.clip(
    canal_osm_gdf, filtered_osm_basins_gdf.geometry.unary_union
)
print("clip vaarwegen")
vaarwegen_gdf_clipped = gpd.clip(
    vaarwegen_gdf, filtered_vaarwegen_basins_gdf.geometry.unary_union
)

# Concatenate GeoDataFrames
print("merge")
network_osm_gdf = pd.concat(
    [river_osm_gdf_clipped, canal_osm_gdf_clipped, vaarwegen_gdf_clipped],
    ignore_index=True,
)
print("write merge")
network_osm_gdf.to_file(cloud.joinpath("OSM", "verwerkt", "network_osm.gpkg"))
# %% Write the merged GeoDataFrame to a new GeoPackage file

print("read network")
network_osm_gdf = gpd.read_file(cloud.joinpath("OSM", "verwerkt", "network_osm.gpkg"))

print("create network")
network = Network(network_osm_gdf)

print("write network")
network.to_file(cloud.joinpath("OSM", "verwerkt", "network.gpkg"))

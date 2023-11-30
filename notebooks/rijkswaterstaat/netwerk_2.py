# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage, Network
from shapely.geometry import Polygon
from shapely.ops import snap, split

cloud = CloudStorage()

# %% read files

print("read basins")
krw_basins_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken.gpkg"),
    engine="pyogrio",
)

print("read rijkspolygonen")
rws_opp_poly_gdf = gpd.read_file(
    cloud.joinpath(
        "Rijkswaterstaat", "aangeleverd", "oppervlaktewaterlichamen_rijk.gpkg"
    )
)


print("read vaarwegen")
vaarwegen_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "aangeleverd", "nwb_vaarwegvakken.gpkg"),
    engine="pyogrio",
)

print("read osm river")
river_osm_gdf = gpd.read_file(
    cloud.joinpath("basisgegevens", "OSM", "waterway_river_the_netherlands.gpkg"),
    engine="pyogrio",
)

print("read osm canals")
canal_osm_gdf = gpd.read_file(
    cloud.joinpath("basisgegevens", "OSM", "waterway_canals_the_netherlands.gpkg"),
    engine="pyogrio",
)

print("read extra lijnen")
extra_lines_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    layer="extra_netwerk_lijnen",
    engine="pyogrio",
)

print("read verwijder lijnen")
remove_lines_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    layer="verwijder lijn",
    engine="pyogrio",
)

print("read toevoegen knoop")
add_nodes_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    layer="toevoegen knoop",
    engine="pyogrio",
)


# %% Create vaarwegen and osm basins filters and masks
vaarwegen_basins = [
    "NL92_KETELMEER_VOSSEMEER",
    "NL92_ZWARTEMEER",
    "NL92_RANDMEREN_OOST",
    "NL92_RANDMEREN_ZUID",
    "NL95_1A",
    "NL89_westsde",
    "NL89_oostsde",
    "NL89_zoommedt",
    "NL89_grevlemr",
    "NL89_veersmr",
    "NL94_11",
    "Haringvliet-oost",
    "NL89_volkerak",
]

ijsselmeer_basins = [
    "NL92_MARKERMEER",
    "NL92_IJSSELMEER",
]

rijks_waterlichamen = ["Maximakanaal"]

exclude_osm_basins = vaarwegen_basins + ijsselmeer_basins

print("create vaarwegen_filter")
filtered_vaarwegen_basins_gdf = krw_basins_gdf[
    krw_basins_gdf["owmident"].isin(vaarwegen_basins)
]

print("create osm mask polygon")
filtered_osm_basins_gdf = krw_basins_gdf[
    ~krw_basins_gdf["owmident"].isin(exclude_osm_basins)
]

osm_basins_mask = (
    filtered_osm_basins_gdf.explode(index_parts=False)
    .geometry.exterior.apply(Polygon)
    .unary_union
)

rws_opp_poly_mask = rws_opp_poly_gdf[
    rws_opp_poly_gdf.waterlichaam.isin(rijks_waterlichamen)
].unary_union

osm_mask = osm_basins_mask.union(rws_opp_poly_mask)


# %% Overlay lines with krw-basins

print("extra lines basin overlay")
extra_basin_gdf = gpd.overlay(extra_lines_gdf, krw_basins_gdf, how="union")

print("river osm basin overlay")
river_osm_gdf = river_osm_gdf[river_osm_gdf.intersects(osm_mask)]
river_osm_basin_gdf = gpd.overlay(river_osm_gdf, filtered_osm_basins_gdf, how="union")

print("canal osm basin overlay")
canal_osm_gdf = canal_osm_gdf[canal_osm_gdf.intersects(osm_mask)]
canal_osm_basin_gdf = gpd.overlay(canal_osm_gdf, filtered_osm_basins_gdf, how="union")

print("vaarwegen basin overlay")
vaarwegen_basin_gdf = gpd.overlay(
    vaarwegen_gdf, filtered_vaarwegen_basins_gdf, how="intersection"
)

# %% Samenvoegen tot 1 lijnenbestand
river_osm_basin_gdf.rename(columns={"osm_id": "id"}, inplace=True)
canal_osm_basin_gdf.rename(columns={"osm_id": "id"}, inplace=True)
vaarwegen_basin_gdf.rename(columns={"vwk_id": "id", "vwg_naam": "name"}, inplace=True)
extra_basin_gdf.rename(columns={"naam": "name"}, inplace=True)
# Concatenate GeoDataFrames
print("concat")
network_lines_gdf = pd.concat(
    [
        river_osm_basin_gdf,
        canal_osm_basin_gdf,
        vaarwegen_basin_gdf,
    ],
    ignore_index=True,
)

remove_indices = []
for geometry in remove_lines_gdf.geometry:
    remove_indices += network_lines_gdf.loc[
        network_lines_gdf.geometry.within(geometry.buffer(0.1))
    ].index.to_list()
# if len(remove_indices) != len(remove_lines_gdf):
#     raise Exception(f"{len(remove_indices)} != {len(remove_lines_gdf)}")
# else:
network_lines_gdf = network_lines_gdf[~network_lines_gdf.index.isin(remove_indices)]

data = []
for geometry in add_nodes_gdf.geometry:
    lines_select_gdf = network_lines_gdf[
        network_lines_gdf.geometry.buffer(0.01).intersects(geometry.buffer(0.01))
    ]
    for idx, row in lines_select_gdf.iterrows():
        feature = row.to_dict()
        us_geometry, ds_geometry = split(
            snap(row.geometry, geometry, 0.01), geometry
        ).geoms
        network_lines_gdf.loc[idx, "geometry"] = us_geometry
        feature["geometry"] = ds_geometry
        data += [feature]

network_lines_gdf = pd.concat(
    [
        network_lines_gdf,
        extra_basin_gdf,
        gpd.GeoDataFrame(data, crs=network_lines_gdf.crs),
    ],
    ignore_index=True,
)

# %% wegschrijven als netwerk

print("create network")
network = Network(network_lines_gdf, tolerance=10, id_col="id", name_col="name")

print("write network")
network.to_file(cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk_2.gpkg"))

# %%

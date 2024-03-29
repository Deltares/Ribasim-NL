# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim_nl import CloudStorage, Network
from shapely.geometry import LineString, Point, Polygon
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

print("read osm fairway")
fairway_osm_gdf = gpd.read_file(
    cloud.joinpath("basisgegevens", "OSM", "waterway_fairway_the_netherlands.gpkg"),
    engine="pyogrio",
)

print("read osm river")
river_osm_gdf = gpd.read_file(
    cloud.joinpath("basisgegevens", "OSM", "waterway_river_the_netherlands.gpkg"),
    engine="pyogrio",
)

print("read osm canals")
canal_osm_gdf = gpd.read_file(
    cloud.joinpath("basisgegevens", "OSM", "waterway_canal_the_netherlands.gpkg"),
    engine="pyogrio",
)

print("read extra lijnen")
extra_lines_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    layer="extra_netwerk_lijnen_2",
    engine="pyogrio",
)

print("read verwijder lijnen")
remove_lines_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    layer="verwijder_lijn_2",
    engine="pyogrio",
)

print("read toevoegen knoop")
add_nodes_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    layer="toevoegen knoop",
    engine="pyogrio",
)


# %% Create vaarwegen and osm basins filters and masks

ijsselmeer_basins = [
    "NL92_MARKERMEER",
    "NL92_IJSSELMEER",
]
# KRW dekt sommige kunstwerken niet geheel waardoor rijks_waterlichamen zijn toegevoegd
rijks_waterlichamen = [
    "Maximakanaal",
    "Kanaal Wessem-Nederweert",
    "Noordzeekanaal",
    "Buiten-IJ",
    "Buitenhaven van IJmuiden",
]

exclude_osm_basins = ijsselmeer_basins

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
rws_opp_poly_mask_gdf = gpd.GeoDataFrame(geometry=[rws_opp_poly_mask])
osm_basins_mask_gdf = gpd.GeoDataFrame(geometry=[osm_basins_mask])

# Create a GeoDataFrame from the union result
osm_mask_gdf = gpd.GeoDataFrame(geometry=[osm_mask])


# %% Overlay lines with krw-basins

print("extra lines basin overlay")
extra_basin_gdf = gpd.overlay(extra_lines_gdf, krw_basins_gdf, how="union")

print("river osm basin overlay")
river_osm_gdf = river_osm_gdf[river_osm_gdf.intersects(osm_mask)]
river_osm_basin_gdf = gpd.overlay(river_osm_gdf, filtered_osm_basins_gdf, how="union")

print("canal osm basin overlay")
canal_osm_gdf = canal_osm_gdf[canal_osm_gdf.intersects(osm_mask)]
canal_osm_basin_gdf = gpd.overlay(canal_osm_gdf, filtered_osm_basins_gdf, how="union")

print("canal osm fairway overlay")
fairway_osm_gdf = fairway_osm_gdf[fairway_osm_gdf.intersects(osm_mask)]
fairway_osm_basin_gdf = gpd.overlay(
    fairway_osm_gdf, filtered_osm_basins_gdf, how="union"
)


# %% Samenvoegen tot 1 lijnenbestand
river_osm_basin_gdf.rename(columns={"osm_id": "id"}, inplace=True)
canal_osm_basin_gdf.rename(columns={"osm_id": "id"}, inplace=True)
fairway_osm_basin_gdf.rename(columns={"osm_id": "id"}, inplace=True)
extra_basin_gdf.rename(columns={"naam": "name"}, inplace=True)
# Concatenate GeoDataFrames
print("concat")
network_lines_gdf = pd.concat(
    [
        river_osm_basin_gdf,
        canal_osm_basin_gdf,
        fairway_osm_basin_gdf,
    ],
    ignore_index=True,
)

remove_indices = []
for geometry in remove_lines_gdf.geometry:
    remove_indices += network_lines_gdf.loc[
        network_lines_gdf.geometry.within(geometry.buffer(0.1))
    ].index.to_list()
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


def subdivide_line(line, max_length):
    total_length = line.length

    num_segments = int(np.ceil(total_length / max_length))

    if num_segments == 1:
        return [line]

    segments = []
    for i in range(num_segments):
        start_frac = i / num_segments
        end_frac = (i + 1) / num_segments
        start_point = line.interpolate(start_frac, normalized=True)
        start_dist = line.project(start_point)
        end_point = line.interpolate(end_frac, normalized=True)
        end_dist = line.project(end_point)

        points = (
            [start_point]
            + [
                Point(i)
                for i in line.coords
                if (line.project(Point(i)) > start_dist)
                and (line.project(Point(i)) < end_dist)
            ]
            + [end_point]
        )
        segment = LineString(points)
        segments.append(segment)

    return segments


def subdivide_geodataframe(gdf, max_length):
    data = []

    for row in gdf.explode().itertuples():
        row_dict = row._asdict()
        row_dict.pop("geometry")
        lines = subdivide_line(row.geometry, max_length)
        data += [{**row_dict, "geometry": line} for line in lines]

    return gpd.GeoDataFrame(data=data, crs=gdf.crs)


# Assuming network_lines_gdf is defined somewhere before this point
network_lines_gdf = network_lines_gdf[
    ~network_lines_gdf["name"].isin(["Geul", "Derde Diem"])
]  # brute verwijdering wegens sifon onder Julianakanaal

network = Network(network_lines_gdf, tolerance=1, id_col="id", name_col="name")

print("write to hydamo")
lines = network.links
lines.rename(columns={"name": "naam"}, inplace=True)
lines.to_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "hydamo.gpkg"),
    layer="hydroobject",
    engine="pyogrio",
)


gdf_subdivided = subdivide_geodataframe(network_lines_gdf, max_length=450)

# %%
network = Network(gdf_subdivided, tolerance=1, id_col="id", name_col="name")

print("write network")
network.to_file(cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg"))

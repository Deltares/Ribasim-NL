# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point

from ribasim_nl import CloudStorage, Network

cloud = CloudStorage()

# input on cloud
# TODO fairway not on cloud, stream is, but not used
fairway_osm_path = cloud.joinpath("Basisgegevens/OSM/waterway_fairway.gpkg")
river_osm_path = cloud.joinpath("Basisgegevens/OSM/waterway_river.gpkg")
canal_osm_path = cloud.joinpath("Basisgegevens/OSM/waterway_canal.gpkg")
model_user_data_path = cloud.joinpath("Rijkswaterstaat/verwerkt/model_user_data.gpkg")

cloud.synchronize(filepaths=[fairway_osm_path, river_osm_path, canal_osm_path, model_user_data_path])

# input from previous step
basins_path = cloud.joinpath("Rijkswaterstaat/verwerkt/basins.gpkg")

# output
hydamo_path = cloud.joinpath("Rijkswaterstaat/verwerkt/hydamo.gpkg")
network_path = cloud.joinpath("Rijkswaterstaat/verwerkt/netwerk.gpkg")


# %% read files

print("read basins")
basins_gdf = gpd.read_file(basins_path, layer="ribasim_basins")

print("read osm fairway")
fairway_osm_gdf = gpd.read_file(fairway_osm_path)

print("read osm river")
river_osm_gdf = gpd.read_file(river_osm_path)

print("read osm canals")
canal_osm_gdf = gpd.read_file(canal_osm_path)

print("read extra lijnen")
extra_lines_gdf = gpd.read_file(model_user_data_path, layer="extra_netwerk_lijnen_2")

print("read verwijder lijnen")
remove_lines_gdf = gpd.read_file(
    model_user_data_path,
    layer="verwijder_lijn_2",
)


# %% aanmaken masks

# osm_basins voor het filteren van osm_lijnen
print("samenstellen clip-polygonen")
ijsselmeer_basins = [
    "Markermeer",
    "Gouwzee",
    "IJmeer",
    "IJsselmeer",
]  # ijsselmeer komt uit extra lijnen
osm_basins_gdf = basins_gdf[~basins_gdf["naam"].isin(ijsselmeer_basins)]
ijsselmeer_poly = basins_gdf[basins_gdf["naam"].isin(ijsselmeer_basins)].union_all()

# samenvoegen van alle OSM lijnen
network_lines_gdf = pd.concat(
    [
        river_osm_gdf,
        canal_osm_gdf,
        fairway_osm_gdf,
    ],
    ignore_index=True,
)
network_lines_gdf.loc[:, ["original_index"]] = network_lines_gdf.index + 1

print("osm clippen op polygonen")
data = []
for row in osm_basins_gdf.itertuples():
    idx = network_lines_gdf.sindex.intersection(row.geometry.bounds)
    select_gdf = network_lines_gdf.iloc[idx][network_lines_gdf.iloc[idx].intersects(row.geometry)]
    data += [select_gdf]

network_lines_gdf = pd.concat(data, ignore_index=True)
network_lines_gdf.drop_duplicates("original_index", inplace=True)


# %%
print("osm lijnen samenvoegen met extra lijnen")
extra_lines_gdf.rename(columns={"naam": "name"}, inplace=True)
network_lines_gdf.rename(columns={"osm_id": "id"}, inplace=True)
network_lines_gdf = pd.concat(
    [
        network_lines_gdf,
        extra_lines_gdf,
    ],
    ignore_index=True,
)

# %%
print("lijnen overlayen met basins: split lijnen op basin-randen")
network_lines_gdf = gpd.overlay(network_lines_gdf, basins_gdf, how="union")

# %% Samenvoegen tot 1 lijnenbestand

print("lijnen opschonen met handmatig")
remove_indices = []
for geometry in remove_lines_gdf.geometry:
    remove_indices += network_lines_gdf.loc[network_lines_gdf.geometry.within(geometry.buffer(0.1))].index.to_list()
network_lines_gdf = network_lines_gdf[~network_lines_gdf.index.isin(remove_indices)]


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
                if (line.project(Point(i)) > start_dist) and (line.project(Point(i)) < end_dist)
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

network_lines_gdf = network_lines_gdf[network_lines_gdf.length > 0.5]
network = Network(network_lines_gdf, tolerance=1, id_col="id", name_col="name")

print("write to hydamo")
lines = network.links
lines.rename(columns={"name": "naam"}, inplace=True)
lines.to_file(hydamo_path, layer="hydroobject")

# %%
print("write network")
gdf_subdivided = subdivide_geodataframe(network_lines_gdf, max_length=450)
network = Network(gdf_subdivided, tolerance=1, id_col="id", name_col="name")
network.to_file(network_path)

# %%

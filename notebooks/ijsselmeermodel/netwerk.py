# %%
import os
from pathlib import Path

import geopandas as gpd
import networkx as nx
import pandas as pd
from hydamo import HyDAMO
from shapely.geometry import LineString
from shapely.ops import snap, split

DATA_DIR = Path(os.getenv("RIBASIM_NL_DATA_DIR"))
MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"
MODEL_DATA_GPKG = Path(MODEL_DIR) / "model_data.gpkg"

zuiderzeeland_hydamo_gpkg = DATA_DIR.joinpath("Zuiderzeeland", "overig", "hydamo.gpkg")

hydamo = HyDAMO.from_geopackage(file_path=zuiderzeeland_hydamo_gpkg, version="2.2.1")
# %%

tolerance = 1.0
gdf = hydamo.hydroobject
gdf["node_from"] = None
gdf["node_to"] = None
sindex = gdf.sindex

nodes_gdf = gpd.GeoDataFrame(geometry=gdf.boundary.explode().unique(), crs=28992)
nodes_gdf.to_file(MODEL_DATA_GPKG, layer="node")

# %% create network


graph = nx.Graph()
for row in nodes_gdf.itertuples():
    graph.add_node(row.Index, geometry=row.geometry)

for row in gdf.itertuples():
    row_index = row.Index
    nodes_select = nodes_gdf.loc[nodes_gdf.sindex.intersection(row.geometry.bounds)]
    point_from, point_to = row.geometry.boundary.geoms

    node_from = nodes_select.distance(point_from).sort_values().index[0]
    node_to = nodes_select.distance(point_to).sort_values().index[0]
    graph.add_edge(
        node_from,
        node_to,
        index=row.Index,
        length=row.geometry.length,
        geometry=row.geometry,
    )
    gdf.loc[row.Index, ["node_from", "node_to"]] = node_from, node_to


# %%
def get_path(from_point, to_point):
    # determine source, target node and get path
    source = nodes_gdf.distance(from_point).sort_values().index[0]
    target = nodes_gdf.distance(to_point).sort_values().index[0]
    shortest_path = nx.shortest_path(graph, source=source, target=target, weight="length", method="dijkstra")

    # create points from source and target
    source_point = graph.nodes[shortest_path[0]]["geometry"]
    target_point = graph.nodes[shortest_path[-1]]["geometry"]

    # convert path to LineString
    path = list(from_point.coords)

    if from_point != source_point:
        path += list(source_point.coords)  # add first node for linestring
    for node_from, node_to in zip(shortest_path[0:-1], shortest_path[1:]):
        # extract the line_string from the edges
        line = graph.get_edge_data(node_from, node_to)["geometry"]
        # if the upstream vertex of the linestring is not equal to the node_from.point, reverse the line
        if line.boundary.geoms[0] != graph.nodes[node_from]["geometry"]:
            line = line.reverse()
        path += line.coords[1:] + list(graph.nodes[node_to]["geometry"].coords)

    if not to_point == target_point:
        path += list(to_point.coords)

    return LineString(path)


# %%
basin_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="basin").set_index("user_id")
basin_area_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="basin_area").set_index("user_id")
data = []
# %% edges for pumps
pump_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="pump")
pump_tuples = pump_gdf.itertuples()
type = "pump"
for row in pump_tuples:
    basin_point = basin_gdf.loc[row.id_from].geometry
    kwk_point = row.geometry
    geometry = get_path(basin_point, kwk_point)
    data += [
        {
            "user_id_from": row.id_from,
            "user_id_to": row.user_id,
            "geometry": geometry,
            "type": type,
        }
    ]
    rws_to_user_id = basin_area_gdf[basin_area_gdf.rijkswater == row.id_to].distance(kwk_point).sort_values().index[0]
    geometry = LineString((kwk_point, basin_gdf.loc[rws_to_user_id].geometry))
    data += [
        {
            "user_id_from": row.user_id,
            "user_id_to": rws_to_user_id,
            "geometry": geometry,
            "type": type,
        }
    ]

# %% edges for outlets
outlet_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="outlet")
outlet_tuples = outlet_gdf.itertuples()
type = "outlet"
for row in outlet_tuples:
    basin_point = basin_gdf.loc[row.id_to].geometry
    kwk_point = row.geometry
    rws_to_user_id = basin_area_gdf[basin_area_gdf.rijkswater == row.id_from].distance(kwk_point).sort_values().index[0]
    rws_point = basin_gdf.loc[rws_to_user_id].geometry
    geometry = LineString((rws_point, kwk_point))
    data += [
        {
            "user_id_from": rws_to_user_id,
            "user_id_to": row.user_id,
            "geometry": geometry,
            "type": type,
        }
    ]

    if row.user_id in [
        "NL.WBHCODE.64.sluis.loc=143820,143820",
        "NL.WBHCODE.37.sluis.IW4570",
    ]:
        geometry = get_path(basin_point, kwk_point)
    else:
        geometry = LineString((basin_point, kwk_point))
    data += [
        {
            "user_id_from": row.user_id,
            "user_id_to": row.id_to,
            "geometry": geometry,
            "type": type,
        }
    ]


# %% edges for sluice-type resistance nodes
hydamo_gpkg = DATA_DIR.joinpath("ijsselmeergebied", "hydamo.gpkg")
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject")
resistance_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="resistance")
level_boundary_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="level_boundary")
type = "sluice"
hydroobject_indices = []


def get_path_from_hydroobject(from_point, to_point):
    row = hydroobject_gdf.loc[
        hydroobject_gdf.geometry.boundary.apply(
            lambda x: x.geoms[0].distance(from_point) + x.geoms[1].distance(to_point)
        )
        .sort_values()
        .index[0]
    ]

    # geometry = LineString(list(basin_from_point.coords) + geometry.coords[1:-1] + list(kwk_point.coords))

    return row.name, row.geometry


for row in resistance_gdf.itertuples():
    # specify structure point
    kwk_point = row.geometry
    kwk_id = row.user_id

    # specify basin_from_id and point
    basin_from_id = basin_area_gdf[basin_area_gdf.rijkswater == row.id_from].distance(kwk_point).sort_values().index[0]
    basin_from_point = basin_gdf.loc[basin_from_id].geometry

    # specify basin_to_id and point
    if (
        row.id_to != "WADDENZEE"
    ):  # TODO: make this generic (keep all in basins and turn basins to level-boundaries later)
        basin_to_id = basin_area_gdf[basin_area_gdf.rijkswater == row.id_to].distance(kwk_point).sort_values().index[0]
        basin_to_point = basin_gdf.loc[basin_to_id].geometry
    else:
        basin_to_id = "NL.WBHCODE.80.basin.WADDENZEE"
        basin_to_point = level_boundary_gdf.iloc[0].geometry

    index, geometry = get_path_from_hydroobject(basin_from_point, kwk_point)
    hydroobject_indices += [index]

    data += [
        {
            "user_id_from": basin_from_id,
            "user_id_to": kwk_id,
            "geometry": geometry,
            "type": type,
        }
    ]

    index, geometry = get_path_from_hydroobject(kwk_point, basin_to_point)
    hydroobject_indices += [index]

    data += [
        {
            "user_id_from": kwk_id,
            "user_id_to": basin_to_id,
            "geometry": geometry,
            "type": type,
        }
    ]

# %% edges for inter-basin type resistance nodes
type = "inter-basin"
resistance_data = []


for row in hydroobject_gdf[~hydroobject_gdf.index.isin(hydroobject_indices)].itertuples():
    geometry = row.geometry
    basin_from_id = basin_area_gdf.loc[basin_area_gdf.contains(row.geometry.boundary.geoms[0])].index[0]
    basin_to_id = basin_area_gdf.loc[basin_area_gdf.contains(row.geometry.boundary.geoms[1])].index[0]
    resistance_id = f"{basin_from_id}-{basin_to_id}"
    resistance_point = geometry.interpolate(0.5, normalized=True)
    resistance_data += [{"user_id": resistance_id, "resistance": 1.0, "geometry": resistance_point}]
    us_geometry, ds_geometry = split(snap(geometry, resistance_point, 0.01), resistance_point).geoms
    data += [
        {
            "user_id_from": basin_from_id,
            "user_id_to": resistance_id,
            "geometry": us_geometry,
            "type": type,
        },
        {
            "user_id_from": resistance_id,
            "user_id_to": basin_to_id,
            "geometry": ds_geometry,
            "type": type,
        },
    ]

resistance_gdf = pd.concat([resistance_gdf, gpd.GeoDataFrame(resistance_data, crs=28992)])
resistance_gdf.to_file(MODEL_DATA_GPKG, layer="resistance")
# %% write result
gpd.GeoDataFrame(data, crs=28992).to_file(MODEL_DATA_GPKG, layer="edge")
# %% get path between nodes

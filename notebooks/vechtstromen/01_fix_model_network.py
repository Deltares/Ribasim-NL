# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.geometry import edge, split_basin, split_basin_multi_polygon
from shapely.geometry import LineString, MultiPolygon, Point
from shapely.ops import nearest_points

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Vechtstromen", "modellen", "Vechtstromen_2024_6_3", "vechtstromen.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")
hydroobject_gdf = gpd.read_file(
    cloud.joinpath("Vechtstromen", "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="hydroobject", fid_as_index=True
)

split_line_gdf = gpd.read_file(
    cloud.joinpath("Vechtstromen", "verwerkt", "fix_user_data.gpkg"), layer="split_basins", fid_as_index=True
)

level_boundary_gdf = gpd.read_file(
    cloud.joinpath("Vechtstromen", "verwerkt", "fix_user_data.gpkg"), layer="level_boundary", fid_as_index=True
)

# %% read model
model = Model.read(ribasim_toml)
ribasim_toml = cloud.joinpath("Vechtstromen", "modellen", "Vechtstromen_fix_model_network", "vechtstromen.toml")
network_validator = NetworkValidator(model)

# %% some stuff we'll need again
manning_data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])
level_data = level_boundary.Static(level=[0])

basin_data = [
    basin.Profile(level=[0.0, 1.0], area=[0.01, 1000.0]),
    basin.Static(
        drainage=[0.0],
        potential_evaporation=[0.001 / 86400],
        infiltration=[0.0],
        precipitation=[0.005 / 86400],
    ),
    basin.State(level=[0]),
]
outlet_data = outlet.Static(flow_rate=[100])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385111465

# Verwijderen duplicate edges

model.edge.df.drop_duplicates(inplace=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2352686763

# Toevoegen benedenstroomse randvoorwaarden Beneden Dinkel

# verander basin met node_id 2250 naar type level_boundary
model.update_node(2250, "LevelBoundary", data=[level_data])


# verplaats basin 1375 naar het hydroobject
node_id = 1375

model.basin.node.df.loc[node_id, "geometry"] = hydroobject_gdf.at[3135, "geometry"].interpolate(0.5, normalized=True)
edge_ids = model.edge.df[
    (model.edge.df.from_node_id == node_id) | (model.edge.df.to_node_id == node_id)
].index.to_list()
model.reset_edge_geometry(edge_ids=edge_ids)

# verplaats basin 1375 naar het hydroobject


# verbind basins met level_boundaries
for fid, node_id in [(1, 1375), (2, 1624)]:
    boundary_node_geometry = level_boundary_gdf.at[fid, "geometry"]

    # line for interpolation
    basin_node_geometry = Point(
        model.basin.node.df.at[node_id, "geometry"].x, model.basin.node.df.at[node_id, "geometry"].y
    )
    line_geometry = LineString((basin_node_geometry, boundary_node_geometry))

    # define level_boundary_node
    boundary_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])
    level_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])

    # define manning_resistance_node
    outlet_node_geometry = line_geometry.interpolate(line_geometry.length - 20)
    outlet_node = model.outlet.add(Node(geometry=outlet_node_geometry), tables=[outlet_data])

    from_node_id = model.basin[node_id].node_id
    to_node_id = outlet_node.node_id

    # draw edges
    # FIXME: we force edges to be z-less untill this is solved: https://github.com/Deltares/Ribasim/issues/1854
    model.edge.add(
        model.basin[node_id], outlet_node, geometry=edge(model.basin[node_id].geometry, outlet_node.geometry)
    )
    model.edge.add(outlet_node, boundary_node, geometry=edge(outlet_node.geometry, boundary_node.geometry))

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382565944

# Verwijderen Twentekanaal (zit al bij RWS-HWS)
remove_node_ids = [1562, 1568, 1801, 1804, 1810, 1900, 2114, 2118, 2119, 32]

# remove by edge so we also remove all resistance nodes in between
edge_df = model.edge.df[
    model.edge.df.from_node_id.isin(remove_node_ids) | model.edge.df.to_node_id.isin(remove_node_ids)
][["from_node_id", "to_node_id"]]

for row in edge_df.itertuples():
    model.remove_edge(from_node_id=row.from_node_id, to_node_id=row.to_node_id, remove_disconnected_nodes=True)

# add level_boundaries at twentekanaal for later coupling
hws_model = Model.read(cloud.joinpath("Rijkswaterstaat", "modellen", "hws", "hws.toml"))
basin_ids = hws_model.node_table().df[hws_model.node_table().df.name.str.contains("Twentekanaal")].index.to_list()
twentekanaal_poly = hws_model.basin.area.df[hws_model.basin.area.df.node_id.isin(basin_ids)].union_all()

connect_node_ids = [
    i for i in set(edge_df[["from_node_id", "to_node_id"]].to_numpy().flatten()) if i in model._used_node_ids
]

for node_id in connect_node_ids:
    node = model.get_node(node_id=node_id)

    # update node to Outlet if it's a manning resistance
    if node.node_type == "ManningResistance":
        model.update_node(node.node_id, "Outlet", data=[outlet_data])
        node = model.get_node(node_id=node_id)

    _, boundary_node_geometry = nearest_points(node.geometry, twentekanaal_poly.boundary)

    boundary_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])

    # draw edge in the correct direction
    if model.edge.df.from_node_id.isin([node_id]).any():  # supply
        model.edge.add(boundary_node, node, geometry=edge(boundary_node.geometry, node.geometry))
    else:
        model.edge.add(node, boundary_node, geometry=edge(node.geometry, boundary_node.geometry))

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385525533

# Opruimen situatie rondom gemaal Oude Drostendiep

# pumps met node_id 639, 608 en 603 op te heffen (1 gemaal ipv 3)
remove_node_ids = [639, 608, 603]

for node_id in remove_node_ids:
    model.remove_node(node_id, remove_edges=True)

# remove by edge so we also remove all resistance nodes in between
edge_df = model.edge.df[
    model.edge.df.from_node_id.isin(remove_node_ids) | model.edge.df.to_node_id.isin(remove_node_ids)
][["from_node_id", "to_node_id"]]

for row in edge_df.itertuples():
    model.remove_edge(from_node_id=row.from_node_id, to_node_id=row.to_node_id, remove_disconnected_nodes=True)

# basin met node_id 1436 te verplaatsen naar locatie basin node_id 2259
basin_id = 1436
model.basin.node.df.loc[basin_id, "geometry"] = model.basin[2259].geometry
edge_ids = model.edge.df[
    (model.edge.df.from_node_id == basin_id) | (model.edge.df.to_node_id == basin_id)
].index.to_list()

model.reset_edge_geometry(edge_ids=edge_ids)

# basin met node_id 2259 opheffen (klein niets-zeggend bakje)
model.remove_node(2259, remove_edges=True)

# stuw ST05005 (node_id 361) verbinden met basin met node_id 1436
model.edge.add(model.tabulated_rating_curve[361], model.basin[basin_id])
model.edge.add(model.basin[basin_id], model.pump[635])

# basin met node_id 2250 verplaatsen naar logische plek bovenstrooms ST05005 en bendenstrooms ST02886 op hydroobjec
basin_id = 2255
model.basin.node.df.loc[basin_id, ["geometry"]] = hydroobject_gdf.at[6444, "geometry"].interpolate(0.5, normalized=True)

edge_ids = model.edge.df[
    (model.edge.df.from_node_id == basin_id) | (model.edge.df.to_node_id == basin_id)
].index.to_list()

model.reset_edge_geometry(edge_ids=edge_ids)

model.split_basin(split_line_gdf.at[9, "geometry"])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385409772

incorrect_edges_df = network_validator.edge_incorrect_connectivity()
false_basin_ids = [1356, 1357, 1358, 1359, 1360, 1361, 1362, 1363, 1364, 1365, 1366, 1367, 1368, 1369, 1370]

for false_basin_id in false_basin_ids:
    basin_geom = (
        incorrect_edges_df[incorrect_edges_df.from_node_id == false_basin_id].iloc[0].geometry.boundary.geoms[0]
    )
    basin_node = model.basin.add(Node(geometry=basin_geom), tables=basin_data)

    # fix edge topology
    model.edge.df.loc[
        incorrect_edges_df[incorrect_edges_df.from_node_id == false_basin_id].index.to_list(), ["from_node_id"]
    ] = basin_node.node_id

    model.edge.df.loc[
        incorrect_edges_df[incorrect_edges_df.to_node_id == false_basin_id].index.to_list(), ["to_node_id"]
    ] = basin_node.node_id

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2386671759


# basin 2224 en manning_resistance 898 (DK28491) opheffen
# tabulated_rating_cuves 336 (ST03745) en 238 (ST03744) opheffen
remove_node_ids = [2224, 898, 336, 238]

for node_id in remove_node_ids:
    model.remove_node(node_id, remove_edges=True)

# pump 667 (GM00088) verbinden met basin 1495
model.edge.add(model.pump[667], model.basin[1495])

# model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387026622

# opruimen basin at Amsterdamscheveld
model.remove_node(1683, remove_edges=True)

# verbinden basin node_id 1680 met tabulated_rating_curve node_id 101 en 125
model.edge.add(model.basin[1680], model.tabulated_rating_curve[101])
model.edge.add(model.basin[1680], model.tabulated_rating_curve[125])

# verbinden pump node_id 622 met basin node_id 1680
model.edge.add(model.pump[622], model.basin[1680])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387056481

# Fix stieltjeskanaal

# split basin_area bij manning_resistance node_id 1365

line = split_line_gdf.at[1, "geometry"]

basin_polygon = model.basin.area.df.at[8, "geometry"].geoms[0]
basin_polygons = split_basin(basin_polygon, line)

model.basin.area.df.loc[8, ["geometry"]] = MultiPolygon([basin_polygons.geoms[0]])
model.basin.area.df.loc[model.basin.area.df.index.max() + 1, ["geometry"]] = MultiPolygon([basin_polygons.geoms[1]])

# hef basin node_id 1901 op
model.remove_node(1901, remove_edges=True)

# hef pump node_id 574 (GM00246) en node_id 638 (GM00249) op
model.remove_node(574, remove_edges=True)
model.remove_node(638, remove_edges=True)

# verbind basin node_id 1876 met pump node_ids 626 (GM00248) en 654 (GM00247)
model.edge.add(model.basin[1876], model.pump[626])
model.edge.add(model.basin[1876], model.pump[654])
model.edge.add(model.tabulated_rating_curve[113], model.basin[1876])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387815013

# opruimen Zwinderskanaal


# split basin_area bij rode lijn
line = split_line_gdf.at[2, "geometry"]
basin_polygon = model.basin.area.df.at[65, "geometry"].geoms[0]
basin_polygons = split_basin(basin_polygon, line)

model.basin.area.df.loc[65, ["geometry"]] = MultiPolygon([basin_polygons.geoms[0]])
model.basin.area.df.loc[model.basin.area.df.index.max() + 1, ["geometry"]] = MultiPolygon([basin_polygons.geoms[1]])

# verwijderen basin 2226, 2258, 2242 en manning_resistance 1366
for node_id in [2226, 2258, 2242, 1366, 1350]:
    model.remove_node(node_id, remove_edges=True)

# verbinden tabulated_rating_curves 327 (ST03499) en 510 (ST03198) met basin 1897
model.edge.add(model.basin[1897], model.tabulated_rating_curve[327])
model.edge.add(model.tabulated_rating_curve[510], model.basin[1897])

# verbinden basin 1897 met tabulated_rating_curve 279 (ST03138)
model.edge.add(model.basin[1897], model.tabulated_rating_curve[279])

# verbinden basin 1897 met manning_resistance 1351 en 1352
model.edge.add(model.basin[1897], model.manning_resistance[1351])
model.edge.add(model.basin[1897], model.manning_resistance[1352])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387888742

# Oplossen situatie Van Echtenskanaal/Scholtenskanaal Klazinaveen

# verplaatsen level_boundary 33 naar splitsing scholtenskanaal/echtenskanaal en omzetten naar basin
outlet_node_geometry = model.level_boundary[33].geometry
model.update_node(33, "Basin", data=basin_data)
model.move_node(node_id=33, geometry=hydroobject_gdf.loc[2679].geometry.boundary.geoms[1])

# plaatsen outlet bij oorspronkelijke plaats level_boundary 33
outlet_node = model.outlet.add(Node(geometry=outlet_node_geometry), tables=[outlet_data])

# plaatsen nieuwe level_boundary op scholtenskanaal aan H&A zijde scholtenskanaal van outlet
boundary_node = model.level_boundary.add(Node(geometry=level_boundary_gdf.at[3, "geometry"]), tables=[level_data])

# toevoegen edges vanaf nieuwe basin 33 naar nieuwe outlet naar nieuwe boundary
model.edge.add(model.basin[33], outlet_node, geometry=edge(model.basin[33].geometry, outlet_node.geometry))
model.edge.add(outlet_node, boundary_node)

# opheffen manning_resistance 1330 bij GM00213
model.remove_node(1330, remove_edges=True)

# verbinden nieuwe basin met outlet en oorspronkijke manning_knopen en pompen in oorspronkelijke richting
for edge_id in [2711, 2712, 2713, 2714, 2708]:
    model.reverse_edge(edge_id=edge_id)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2388009499

# basin met node_id 1873 gaat richting geesburg
model.move_node(node_id=1873, geometry=hydroobject_gdf.loc[6616].geometry.boundary.geoms[1])
model.basin.area.df.loc[model.basin.area.df.node_id == 1873, ["node_id"]] = pd.NA
# ege 2700, 2701, 2702 worden opgeheven
model.edge.df = model.edge.df[~model.edge.df.index.isin([2700, 2701, 2702])]

# basin 1873 wordt verbonden met manning_resistance 1054
model.edge.add(
    model.basin[1873],
    model.manning_resistance[1054],
    geometry=edge(model.basin[1873].geometry, model.manning_resistance[1054].geometry),
)

# manning_resistance 1308 en 1331 worden verbonden met basin 1873
model.edge.add(
    model.manning_resistance[1308],
    model.basin[1873],
    geometry=edge(model.manning_resistance[1308].geometry, model.basin[1873].geometry),
)
model.edge.add(
    model.basin[1873],
    model.manning_resistance[1331],
    geometry=edge(model.basin[1873].geometry, model.manning_resistance[1331].geometry),
)

# level_boundary 26 wordt een outlet
model.update_node(26, "Outlet", data=[outlet_data])

# nieuwe level_boundary benedenstrooms nieuwe outlet 26
boundary_node = model.level_boundary.add(Node(geometry=level_boundary_gdf.at[4, "geometry"]), tables=[level_data])


# basin 1873 wordt verbonden met outlet en outlet met level_boundary
model.edge.add(
    model.outlet[26],
    model.basin[1873],
    geometry=edge(model.outlet[26].geometry, model.basin[1873].geometry),
)

model.edge.add(boundary_node, model.outlet[26])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2388334544

# Kruising Dinkel/Kanaal Almelo Nordhorn corrigeren

# ege 2700, 2701, 2702 worden opgeheven
model.edge.df = model.edge.df[~model.edge.df.index.isin([2690, 2691, 2692, 2693, 2694, 2695, 2696])]

# basin / area splitten bij rode lijn in twee vlakken
line = split_line_gdf.at[3, "geometry"]

total_basin_polygon = model.basin.area.df.at[544, "geometry"]
basin_polygon = [i for i in model.basin.area.df.at[544, "geometry"].geoms if i.intersects(line)][0]
basin_polygons = split_basin(basin_polygon, line)
model.basin.area.df.loc[544, ["geometry"]] = MultiPolygon(
    [i for i in model.basin.area.df.at[544, "geometry"].geoms if not i.intersects(line)] + [basin_polygons.geoms[0]]
)
model.basin.area.df.loc[model.basin.area.df.index.max() + 1, ["geometry"]] = MultiPolygon([basin_polygons.geoms[1]])

# basin op dinkel bovenstrooms kanaal
dinkel_basin_node = model.basin.add(
    Node(geometry=hydroobject_gdf.loc[2966].geometry.boundary.geoms[1]), tables=basin_data
)

# basin in kanaal
kanaal_basin_node = model.basin.add(
    Node(geometry=hydroobject_gdf.loc[7720].geometry.boundary.geoms[1]), tables=basin_data
)

# edges v.a. tabulated_rating_curve 298 (ST01865) en 448 (ST01666) naar dinkel-basin
model.edge.add(
    model.tabulated_rating_curve[298],
    dinkel_basin_node,
    geometry=edge(model.tabulated_rating_curve[298].geometry, dinkel_basin_node.geometry),
)

model.edge.add(
    model.tabulated_rating_curve[448],
    dinkel_basin_node,
    geometry=edge(model.tabulated_rating_curve[448].geometry, dinkel_basin_node.geometry),
)

# edge v.a. manning_resistance 915 naar dinkel basin
model.edge.add(
    model.manning_resistance[915],
    dinkel_basin_node,
    geometry=edge(model.manning_resistance[915].geometry, dinkel_basin_node.geometry),
)

# edges v.a. dinkel basin naar tabulate_rating_curves 132 (ST02129) en 474 (ST02130)
model.edge.add(
    dinkel_basin_node,
    model.tabulated_rating_curve[132],
    geometry=edge(dinkel_basin_node.geometry, model.tabulated_rating_curve[132].geometry),
)

model.edge.add(
    dinkel_basin_node,
    model.tabulated_rating_curve[474],
    geometry=edge(dinkel_basin_node.geometry, model.tabulated_rating_curve[474].geometry),
)

# nieuwe manning_resistance in nieuwe dinkel-basin bovenstrooms kanaal
manning_node = model.manning_resistance.add(
    Node(geometry=hydroobject_gdf.at[7721, "geometry"].interpolate(0.5, normalized=True)), tables=[manning_data]
)

# nieuwe basin verbinden met nieuwe manning_resistance en nieuw kanaal basin
model.edge.add(
    dinkel_basin_node,
    manning_node,
    geometry=edge(dinkel_basin_node.geometry, manning_node.geometry),
)

model.edge.add(
    manning_node,
    kanaal_basin_node,
    geometry=edge(manning_node.geometry, kanaal_basin_node.geometry),
)

# nieuw kanaal-basin vervinden met tabulated_rating_curve 471 (ST01051)
model.edge.add(
    kanaal_basin_node,
    model.tabulated_rating_curve[471],
    geometry=edge(kanaal_basin_node.geometry, model.tabulated_rating_curve[471].geometry),
)

# nieuw kanaal-basin vervinden met manning_resistance 1346
model.edge.add(
    kanaal_basin_node,
    model.manning_resistance[1346],
    geometry=edge(kanaal_basin_node.geometry, model.manning_resistance[1346].geometry),
)

# nieuwe outletlet bij grensduiker kanaal
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[7746, "geometry"].boundary.geoms[0]), tables=[outlet_data]
)

# nieuwe basin verbinden met outlet verbinden met level_boundary 21
model.edge.add(
    outlet_node,
    kanaal_basin_node,
    geometry=edge(outlet_node.geometry, kanaal_basin_node.geometry),
)

model.edge.add(
    model.level_boundary[21],
    outlet_node,
    geometry=edge(model.level_boundary[21].geometry, outlet_node.geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2389192454
model.reverse_edge(edge_id=2685)
model.remove_node(node_id=2229, remove_edges=True)
model.edge.add(
    model.basin[1778],
    model.outlet[1080],
    geometry=edge(model.basin[1778].geometry, model.outlet[1080].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2389198178
model.reverse_edge(edge_id=2715)
model.reverse_edge(edge_id=2720)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390712613

# Oplossen toplogische situatie kanaal Coevorden

# opheffen basin 2243 en basin 2182
model.remove_node(2243, remove_edges=True)
model.remove_node(2182, remove_edges=True)
model.remove_node(1351, remove_edges=True)
model.remove_node(1268, remove_edges=True)
model.remove_node(1265, remove_edges=True)

# onknippen basin bij rode lijn
line = split_line_gdf.at[4, "geometry"]
basin_area_row = model.basin.area.df[model.basin.area.df.contains(line.centroid)].iloc[0]
basin_area_index = basin_area_row.name
basin_polygon = basin_area_row.geometry.geoms[0]
basin_polygons = split_basin(basin_polygon, line)

model.basin.area.df.loc[basin_area_index, ["geometry"]] = MultiPolygon([basin_polygons.geoms[1]])
model.basin.area.df.loc[model.basin.area.df.index.max() + 1, ["geometry"]] = MultiPolygon([basin_polygons.geoms[0]])

# # verplaatsen basin 1678 naar kruising waterlopen
model.move_node(node_id=1678, geometry=hydroobject_gdf.loc[6594].geometry.boundary.geoms[1])

# verwijderen edges 809, 814, 807, 810, 1293, 2772
model.edge.df = model.edge.df[~model.edge.df.index.isin([809, 814, 807, 810, 887])]


# verbinden manning 1270, 1127 en pumps 644, 579 en 649 met basin 1678
for node_id in [1270, 1127]:
    model.edge.add(
        model.manning_resistance[node_id],
        model.basin[1678],
        geometry=edge(model.manning_resistance[node_id].geometry, model.basin[1678].geometry),
    )

for node_id in [644, 579, 649]:
    model.edge.add(
        model.pump[node_id],
        model.basin[1678],
        geometry=edge(model.pump[node_id].geometry, model.basin[1678].geometry),
    )

# verplaatsen manning 1267 naar basin-edge tussen 1678 en 1678
model.move_node(node_id=1267, geometry=hydroobject_gdf.loc[6609].geometry.boundary.geoms[1])

# maak nieuwe manning-node tussen 1678 en 1897
manning_node = model.manning_resistance.add(
    Node(geometry=hydroobject_gdf.loc[6596].geometry.interpolate(0.5, normalized=True)), tables=[manning_data]
)

# verbinden basin 1897 met manning-node
model.edge.add(
    model.basin[1897],
    manning_node,
    geometry=edge(model.basin[1897].geometry, manning_node.geometry),
)

model.edge.add(
    manning_node,
    model.basin[1678],
    geometry=edge(manning_node.geometry, model.basin[1678].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390952469

# Schoonebekerdiep v.a. Twist Bült

# verplaatsen basin 1909 nabij tabulated_rating_curve 383 (ST03607)
model.move_node(1909, geometry=hydroobject_gdf.loc[6865].geometry.boundary.geoms[1])

# verwijderen edges 780 en 778
model.edge.df = model.edge.df[~model.edge.df.index.isin([780, 778])]

# toevoegen edge tussen tabulated_rating_curve 383 en basin 1909
model.edge.add(
    model.tabulated_rating_curve[383],
    model.basin[1909],
    geometry=edge(model.tabulated_rating_curve[383].geometry, model.basin[1909].geometry),
)

# toevoegen edge tussen manning_resistance 851 en basin 1909
model.edge.add(
    model.manning_resistance[851],
    model.basin[1909],
    geometry=edge(model.manning_resistance[851].geometry, model.basin[1909].geometry),
)

# opknippen basin 1538 nabij 1909 en verbinden basin 1909 met 1539 via nieuwe manning_knoop
line = split_line_gdf.at[5, "geometry"]
model.split_basin(line=line)
manning_node = model.manning_resistance.add(
    Node(geometry=line.intersection(hydroobject_gdf.at[6866, "geometry"])), tables=[manning_data]
)

model.edge.add(model.basin[1909], manning_node)
model.edge.add(manning_node, model.basin[1539], geometry=edge(manning_node.geometry, model.basin[1539].geometry))

# verwijderen edge 2716,2718,2718,2719
model.edge.df = model.edge.df[~model.edge.df.index.isin([2716, 2717, 2718, 2719])]

# opknippen basin 2181 nabij 1881 en verbinden basin 1881 met 2181 via nieuwe manning_knoop
model.move_node(1881, geometry=hydroobject_gdf.loc[6919].geometry.boundary.geoms[1])
line = split_line_gdf.at[6, "geometry"]
model.split_basin(line=line)
manning_node = model.manning_resistance.add(
    Node(geometry=line.intersection(hydroobject_gdf.at[6879, "geometry"])), tables=[manning_data]
)

model.edge.add(model.basin[1881], manning_node)
model.edge.add(manning_node, model.basin[2181], geometry=edge(manning_node.geometry, model.basin[2181].geometry))

for node_id in [139, 251, 267, 205]:
    model.edge.add(
        model.tabulated_rating_curve[node_id],
        model.basin[1881],
        geometry=edge(model.tabulated_rating_curve[node_id].geometry, model.basin[1881].geometry),
    )

model.move_node(1269, hydroobject_gdf.at[7749, "geometry"].interpolate(0.5, normalized=True))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391168839

# Molengoot Hardenberg

# opheffen basin 1903
model.remove_node(1903, remove_edges=True)

# verbinden basin 1433 met pump 621
model.edge.add(
    model.basin[1433],
    model.pump[621],
    geometry=edge(model.basin[1433].geometry, model.pump[621].geometry),
)

# verbinden tabulated_rating_curves 99 en 283 met basin 1433
for node_id in [99, 283]:
    model.edge.add(
        model.tabulated_rating_curve[node_id],
        model.basin[1433],
        geometry=edge(model.tabulated_rating_curve[node_id].geometry, model.basin[1433].geometry),
    )

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390898004

model.remove_node(1131, remove_edges=True)
model.remove_node(1757, remove_edges=True)
model.edge.add(
    model.basin[1588],
    model.tabulated_rating_curve[112],
    geometry=edge(model.basin[1588].geometry, model.tabulated_rating_curve[112].geometry),
)
model.edge.add(
    model.basin[1588],
    model.manning_resistance[57],
    geometry=edge(model.basin[1588].geometry, model.manning_resistance[57].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391191673

# verwijderen basin 1905
model.remove_node(1905, remove_edges=True)

# verbinden manning_resistance 995 met basin 2148
model.edge.add(
    model.basin[2148],
    model.manning_resistance[995],
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391460899

# Samenvoegen basin-knopen Overijsselse Vecht & Coevorden Vechtkanaal
for basin_id in [1845, 2244, 2006, 1846]:
    model.merge_basins(basin_id=basin_id, to_basin_id=2222)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391666745

# Opruimen basins Nieuw-Amsterdam

# opknippen basin 1611 bij rode lijn, area mergen met basin 1879
basin_polygon = model.basin.area.df.set_index("node_id").at[1611, "geometry"]
left_poly, right_poly = split_basin_multi_polygon(basin_polygon, split_line_gdf.at[8, "geometry"])
model.basin.area.df.loc[model.basin.area.df.node_id == 1611, ["geometry"]] = right_poly

left_poly = model.basin.area.df.set_index("node_id").at[1879, "geometry"].union(left_poly)
model.basin.area.df.loc[model.basin.area.df.node_id == 1879, ["geometry"]] = MultiPolygon([left_poly])

# merge basins 2186, 2173, 2022, 1611, 2185 in basin 1902
for basin_id in [2186, 2173, 2022, 1611, 2185]:
    model.merge_basins(basin_id=basin_id, to_basin_id=1902)

# verplaats 1902 iets bovenstrooms
model.move_node(1902, hydroobject_gdf.at[6615, "geometry"].boundary.geoms[1])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391686269
model.remove_node(2198, remove_edges=True)
model.remove_node(2200, remove_edges=True)
model.edge.add(model.basin[2111], model.pump[671])
model.edge.add(model.tabulated_rating_curve[542], model.basin[2111])
model.edge.add(model.pump[671], model.basin[2316])
model.edge.add(model.basin[2316], model.tabulated_rating_curve[542])

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391710413

model.remove_node(2202, remove_edges=True)
model.edge.add(model.basin[1590], model.pump[657])
model.edge.add(model.manning_resistance[1058], model.basin[1590])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391672700

# Merge basin 2176 in 1605
model.merge_basins(basin_id=2176, to_basin_id=1605)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391726774
# Merge basins 2206 in 1518
model.merge_basins(basin_id=2206, to_basin_id=1518, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391734144
# dood takje uit Overijsselse Vecht
model.remove_node(2210, remove_edges=True)
model.remove_node(1294, remove_edges=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391740603

# Merge basin 2225 met 2304
model.merge_basins(2225, 2304)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391815016

# Wetteringe als laterale inflow
model.merge_basins(2231, 1853)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391750536

# Rondom SL00010 opruimen
model.remove_node(2230, remove_edges=True)
model.remove_node(2251, remove_edges=True)
model.edge.add(model.outlet[41], model.level_boundary[15])
model.edge.add(model.basin[1442], model.pump[664])
model.edge.add(model.basin[1442], model.pump[665])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391820198

# Merge basin 2232 in 1591
model.merge_basins(basin_id=2232, to_basin_id=1591)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391825301

# Basin 2236 naar LevelBoundary
model.update_node(2236, "LevelBoundary", data=[level_data])

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391829471

# Merge basin 2246 en 1419
model.merge_basins(basin_id=2246, to_basin_id=1419)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391946915

# Opruimen Elsbeek

# Basin 2256 verplaatsen naar punt
model.move_node(node_id=2256, geometry=hydroobject_gdf.loc[2896].geometry.boundary.geoms[1])

# Basin knippen over lijn
model.basin.area.df.loc[model.basin.area.df.node_id == 2256, ["node_id"]] = pd.NA
model.split_basin(split_line_gdf.at[10, "geometry"])

# Edges 446, 1516, 443 en 444 verwijderen
model.edge.df = model.edge.df[~model.edge.df.index.isin([446, 1516, 443, 444])]

# tabulated_rating_curves 202 en 230 verbinden met basin 2256
model.edge.add(
    model.tabulated_rating_curve[202],
    model.basin[2256],
    geometry=edge(model.tabulated_rating_curve[202].geometry, model.basin[2256].geometry),
)
model.edge.add(
    model.tabulated_rating_curve[230],
    model.basin[2256],
    geometry=edge(model.tabulated_rating_curve[230].geometry, model.basin[2256].geometry),
)

# resistance 954 verbinden met basin 2256
model.edge.add(
    model.manning_resistance[954],
    model.basin[2256],
    geometry=edge(model.manning_resistance[954].geometry, model.basin[2256].geometry),
)

# basin 2256 verbinden met resistance 1106
model.edge.add(
    model.basin[2256],
    model.manning_resistance[1106],
    geometry=edge(model.basin[2256].geometry, model.manning_resistance[1106].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391984234

# Merge basin 2261 in basin 1698
model.merge_basins(2261, 1698)
model.remove_node(390, remove_edges=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391995841

# Merge basin 2260 met basin 1645
model.merge_basins(2260, 1645)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392010526
# Merge basin 2220 met basin 1371
model.merge_basins(2220, 1371, are_connected=False)


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392017041

# Kanaal Almelo Nordhorn bij Almelo
model.merge_basins(2219, 1583, are_connected=False)
model.merge_basins(2209, 1583, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392022887

# Merge basin 2203 met 2227
model.merge_basins(2203, 2227, are_connected=False)
model.remove_node(1219, remove_edges=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392026739

# Merge basin 2014 met 2144
model.merge_basins(2014, 2144)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392030268

# Merge basin 1696 met 1411
model.merge_basins(1696, 1411)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392037263

# Merge basin 2264 met 1459
model.merge_basins(2264, 1459)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392043973

# Merge basin 2212 en 2310
model.merge_basins(2212, 2310)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392048684

# Merge basin 2253 in basin 2228
model.merge_basins(2253, 2228)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392052379

# Merge basin 2221 in basin 1634
model.merge_basins(2221, 1634)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392076634

# Verbinding rondwaterleiding / Lennelwaterleiding herstellen
model.merge_basins(1859, 2235, are_connected=False)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382572457

# Administratie basin node_id in node_table en Basin / Area correct maken
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest", distance=100)
model.fix_unassigned_basin_area()

model.unassigned_basin_area.to_file("unassigned_basins.gpkg")
model.basin.area.df = model.basin.area.df[~model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]


# %%
# corrigeren knoop-topologie

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.edge_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.edge_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet", data=[outlet_data])

# %%
# basin-profielen updaten

df = pd.DataFrame(
    {
        "node_id": np.repeat(model.basin.node.df.index.to_numpy(), 2),
        "level": [0.0, 1.0] * len(model.basin.node.df),
        "area": [0.01, 1000.0] * len(model.basin.node.df),
    }
)
df.index.name = "fid"
model.basin.profile.df = df

df = model.basin.profile.df.groupby("node_id")[["level"]].max().reset_index()
df.index.name = "fid"
model.basin.state.df = df

# %%
# tabulated_rating_curves updaten
df = pd.DataFrame(
    {
        "node_id": np.repeat(model.tabulated_rating_curve.node.df.index.to_numpy(), 2),
        "level": [0.0, 5] * len(model.tabulated_rating_curve.node.df),
        "flow_rate": [0, 0.1] * len(model.tabulated_rating_curve.node.df),
    }
)
df.index.name = "fid"
model.tabulated_rating_curve.static.df = df


# %%

# level_boundaries updaten
df = pd.DataFrame(
    {
        "node_id": model.level_boundary.node.df.index.to_list(),
        "level": [0.0] * len(model.level_boundary.node.df),
    }
)
df.index.name = "fid"
model.level_boundary.static.df = df

# %%
# manning_resistance updaten
length = len(model.manning_resistance.node.df)
df = pd.DataFrame(
    {
        "node_id": model.manning_resistance.node.df.index.to_list(),
        "length": [100.0] * length,
        "manning_n": [100.0] * length,
        "profile_width": [100.0] * length,
        "profile_slope": [100.0] * length,
    }
)
df.index.name = "fid"
model.manning_resistance.static.df = df

#  %% write model

model.basin.area.df.loc[:, ["meta_area"]] = model.basin.area.df.area
model.basin.node.df[~model.basin.node.df.index.isin(model.basin.area.df.node_id)].to_file("missing_areas.gpkg")


# model.use_validation = True
model.write(ribasim_toml)

# %%
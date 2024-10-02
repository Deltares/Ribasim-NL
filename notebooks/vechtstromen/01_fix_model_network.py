# %%
import geopandas as gpd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.geometry import edge, split_basin
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

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382572457

# Administratie basin node_id in node_table en Basin / Area correct maken
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest", distance=100)
model.fix_unassigned_basin_area()

model.unassigned_basin_area.to_file("unassigned_basins.gpkg")

#  %% write model
model.use_validation = False
ribasim_toml = cloud.joinpath("Vechtstromen", "modellen", "Vechtstromen_fix_model_network", "vechtstromen.toml")
model.write(ribasim_toml)
# %%

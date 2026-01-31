# %%
import inspect

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.geometry import drop_z, link, split_basin, split_basin_multi_polygon
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.ops import nearest_points

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()
authority = "Vechtstromen"
name = "vechtstromen"
run_model = False

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
fix_user_data_gpkg = cloud.joinpath(authority, "verwerkt/fix_user_data.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
ribasim_areas_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/areas.gpkg")
hws_model_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_transient/hws.toml")

cloud.synchronize(filepaths=[ribasim_dir, fix_user_data_gpkg, model_edits_gpkg, hydamo_gpkg, ribasim_areas_gpkg])

# %%
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
split_line_gdf = gpd.read_file(fix_user_data_gpkg, layer="split_basins", fid_as_index=True)
level_boundary_gdf = gpd.read_file(fix_user_data_gpkg, layer="level_boundary", fid_as_index=True)
ribasim_areas_gdf = gpd.read_file(ribasim_areas_gpkg, fid_as_index=True, layer="areas")
drainage_areas_df = gpd.read_file(cloud.joinpath("Vechtstromen/verwerkt/4_ribasim/areas.gpkg"), layer="drainage_areas")

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


# # drop z in basin.nodes, zodat we hieronder geen crashes meer krijgen.
model.basin.node.df.loc[:, "geometry"] = model.basin.node.df.geometry.apply(drop_z)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385111465

# Verwijderen duplicate links

model.link.df.drop_duplicates(inplace=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2352686763

# Toevoegen benedenstroomse randvoorwaarden Beneden Dinkel

# verander basin met node_id 2250 naar type level_boundary
model.update_node(2250, "LevelBoundary", data=[level_data])


# verplaats basin 1375 naar het hydroobject
node_id = 1375

model.basin.node.df.loc[node_id, "geometry"] = hydroobject_gdf.at[3135, "geometry"].interpolate(0.5, normalized=True)
link_ids = model.link.df[
    (model.link.df.from_node_id == node_id) | (model.link.df.to_node_id == node_id)
].index.to_list()
model.reset_link_geometry(link_ids=link_ids)

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

    # draw links
    # FIXME: we force links to be z-less untill this is solved: https://github.com/Deltares/Ribasim/issues/1854
    model.link.add(
        model.basin[node_id], outlet_node, geometry=link(model.basin[node_id].geometry, outlet_node.geometry)
    )
    model.link.add(outlet_node, boundary_node, geometry=link(outlet_node.geometry, boundary_node.geometry))

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382565944

# Verwijderen Twentekanaal (zit al bij RWS-HWS)
remove_node_ids = [1562, 1568, 1801, 1804, 1810, 1900, 2114, 2118, 2119, 32]

# remove by link so we also remove all resistance nodes in between
link_df = model.link.df[
    model.link.df.from_node_id.isin(remove_node_ids) | model.link.df.to_node_id.isin(remove_node_ids)
][["from_node_id", "to_node_id"]]

for row in link_df.itertuples():
    model.remove_link(from_node_id=row.from_node_id, to_node_id=row.to_node_id, remove_disconnected_nodes=True)

# add level_boundaries at twentekanaal for later coupling
hws_model = Model.read(hws_model_toml)
basin_ids = hws_model.node_table().df[hws_model.node_table().df.name.str.contains("Twentekanaal")].index.to_list()
twentekanaal_poly = hws_model.basin.area.df[hws_model.basin.area.df.node_id.isin(basin_ids)].union_all()

connect_node_ids = [
    i for i in set(link_df[["from_node_id", "to_node_id"]].to_numpy().flatten()) if i in model._used_node_ids
]

for node_id in connect_node_ids:
    node = model.get_node(node_id=node_id)

    # update node to Outlet if it's a manning resistance
    if node.node_type == "ManningResistance":
        model.update_node(node.node_id, "Outlet", data=[outlet_data])
        node = model.get_node(node_id=node_id)

    _, boundary_node_geometry = nearest_points(node.geometry, twentekanaal_poly.boundary)

    boundary_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])

    # draw link in the correct direction
    if model.link.df.from_node_id.isin([node_id]).any():  # supply
        model.link.add(boundary_node, node, geometry=link(boundary_node.geometry, node.geometry))
    else:
        model.link.add(node, boundary_node, geometry=link(node.geometry, boundary_node.geometry))

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385525533

# Opruimen situatie rondom gemaal Oude Drostendiep

# pumps met node_id 639, 608 en 603 op te heffen (1 gemaal ipv 3)
remove_node_ids = [639, 608, 603]

for node_id in remove_node_ids:
    model.remove_node(node_id, remove_links=True)

# remove by link so we also remove all resistance nodes in between
link_df = model.link.df[
    model.link.df.from_node_id.isin(remove_node_ids) | model.link.df.to_node_id.isin(remove_node_ids)
][["from_node_id", "to_node_id"]]

for row in link_df.itertuples():
    model.remove_link(from_node_id=row.from_node_id, to_node_id=row.to_node_id, remove_disconnected_nodes=True)

# basin met node_id 1436 te verplaatsen naar locatie basin node_id 2259
basin_id = 1436
model.basin.node.df.loc[basin_id, "geometry"] = model.basin[2259].geometry
link_ids = model.link.df[
    (model.link.df.from_node_id == basin_id) | (model.link.df.to_node_id == basin_id)
].index.to_list()

model.reset_link_geometry(link_ids=link_ids)

# basin met node_id 2259 opheffen (klein niets-zeggend bakje)
model.remove_node(2259, remove_links=True)

# stuw ST05005 (node_id 361) verbinden met basin met node_id 1436
model.link.add(model.tabulated_rating_curve[361], model.basin[basin_id])
model.link.add(model.basin[basin_id], model.pump[635])

# basin met node_id 2250 verplaatsen naar logische plek bovenstrooms ST05005 en bendenstrooms ST02886 op hydroobjec
basin_id = 2255
model.basin.node.df.loc[basin_id, ["geometry"]] = hydroobject_gdf.at[6444, "geometry"].interpolate(0.5, normalized=True)

link_ids = model.link.df[
    (model.link.df.from_node_id == basin_id) | (model.link.df.to_node_id == basin_id)
].index.to_list()

model.reset_link_geometry(link_ids=link_ids)

model.split_basin(split_line_gdf.at[9, "geometry"])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385409772

incorrect_links_df = network_validator.link_incorrect_connectivity()
false_basin_ids = [1356, 1357, 1358, 1359, 1360, 1361, 1362, 1363, 1364, 1365, 1366, 1367, 1368, 1369, 1370]

for false_basin_id in false_basin_ids:
    basin_geom = (
        incorrect_links_df[incorrect_links_df.from_node_id == false_basin_id].iloc[0].geometry.boundary.geoms[0]
    )
    basin_node = model.basin.add(Node(geometry=basin_geom), tables=basin_data)

    # fix link topology
    model.link.df.loc[
        incorrect_links_df[incorrect_links_df.from_node_id == false_basin_id].index.to_list(), ["from_node_id"]
    ] = basin_node.node_id

    model.link.df.loc[
        incorrect_links_df[incorrect_links_df.to_node_id == false_basin_id].index.to_list(), ["to_node_id"]
    ] = basin_node.node_id

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2386671759


# basin 2224 en manning_resistance 898 (DK28491) opheffen
# tabulated_rating_cuves 336 (ST03745) en 238 (ST03744) opheffen
remove_node_ids = [2224, 898, 336, 238]

for node_id in remove_node_ids:
    model.remove_node(node_id, remove_links=True)

# pump 667 (GM00088) verbinden met basin 1495
model.link.add(model.pump[667], model.basin[1495])

# model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387026622

# opruimen basin at Amsterdamscheveld
model.remove_node(1683, remove_links=True)

# verbinden basin node_id 1680 met tabulated_rating_curve node_id 101 en 125
model.link.add(model.basin[1680], model.tabulated_rating_curve[101])
model.link.add(model.basin[1680], model.tabulated_rating_curve[125])

# verbinden pump node_id 622 met basin node_id 1680
model.link.add(model.pump[622], model.basin[1680])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387056481

# Fix stieltjeskanaal

# split basin_area bij manning_resistance node_id 1365

line = split_line_gdf.at[1, "geometry"]

basin_polygon = model.basin.area.df.at[8, "geometry"].geoms[0]
basin_polygons = split_basin(basin_polygon, line)

model.basin.area.df.loc[8, ["geometry"]] = MultiPolygon([basin_polygons.geoms[0]])
model.basin.area.df.loc[model.basin.area.df.index.max() + 1, ["geometry"]] = MultiPolygon([basin_polygons.geoms[1]])

# hef basin node_id 1901 op
model.remove_node(1901, remove_links=True)

# hef pump node_id 574 (GM00246) en node_id 638 (GM00249) op
model.remove_node(574, remove_links=True)
model.remove_node(638, remove_links=True)

# verbind basin node_id 1876 met pump node_ids 626 (GM00248) en 654 (GM00247)
model.link.add(model.basin[1876], model.pump[626])
model.link.add(model.basin[1876], model.pump[654])
model.link.add(model.tabulated_rating_curve[113], model.basin[1876])

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
    model.remove_node(node_id, remove_links=True)

# verbinden tabulated_rating_curves 327 (ST03499) en 510 (ST03198) met basin 1897
model.link.add(model.basin[1897], model.tabulated_rating_curve[327])
model.link.add(model.tabulated_rating_curve[510], model.basin[1897])

# verbinden basin 1897 met tabulated_rating_curve 279 (ST03138)
model.link.add(model.basin[1897], model.tabulated_rating_curve[279])

# verbinden basin 1897 met manning_resistance 1351 en 1352
model.link.add(model.basin[1897], model.manning_resistance[1351])
model.link.add(model.basin[1897], model.manning_resistance[1352])

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

# toevoegen links vanaf nieuwe basin 33 naar nieuwe outlet naar nieuwe boundary
model.link.add(model.basin[33], outlet_node, geometry=link(model.basin[33].geometry, outlet_node.geometry))
model.link.add(outlet_node, boundary_node)

# opheffen manning_resistance 1330 bij GM00213
model.remove_node(1330, remove_links=True)

# verbinden nieuwe basin met outlet en oorspronkijke manning_knopen en pompen in oorspronkelijke richting
for link_id in [2711, 2712, 2713, 2714, 2708]:
    model.reverse_link(link_id=link_id)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2388009499

# basin met node_id 1873 gaat richting geesburg
model.move_node(node_id=1873, geometry=hydroobject_gdf.loc[6616].geometry.boundary.geoms[1])
model.basin.area.df.loc[model.basin.area.df.node_id == 1873, ["node_id"]] = pd.NA
# ege 2700, 2701, 2702 worden opgeheven
model.link.df = model.link.df[~model.link.df.index.isin([2700, 2701, 2702])]

# basin 1873 wordt verbonden met manning_resistance 1054
model.link.add(
    model.basin[1873],
    model.manning_resistance[1054],
    geometry=link(model.basin[1873].geometry, model.manning_resistance[1054].geometry),
)

# manning_resistance 1308 en 1331 worden verbonden met basin 1873
model.link.add(
    model.manning_resistance[1308],
    model.basin[1873],
    geometry=link(model.manning_resistance[1308].geometry, model.basin[1873].geometry),
)
model.link.add(
    model.basin[1873],
    model.manning_resistance[1331],
    geometry=link(model.basin[1873].geometry, model.manning_resistance[1331].geometry),
)

# level_boundary 26 wordt een outlet
model.update_node(26, "Outlet", data=[outlet_data])

# nieuwe level_boundary benedenstrooms nieuwe outlet 26
boundary_node = model.level_boundary.add(Node(geometry=level_boundary_gdf.at[4, "geometry"]), tables=[level_data])


# basin 1873 wordt verbonden met outlet en outlet met level_boundary
model.link.add(
    model.outlet[26],
    model.basin[1873],
    geometry=link(model.outlet[26].geometry, model.basin[1873].geometry),
)

model.link.add(boundary_node, model.outlet[26])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2388334544

# Kruising Dinkel/Kanaal Almelo Nordhorn corrigeren

# ege 2700, 2701, 2702 worden opgeheven
model.link.df = model.link.df[~model.link.df.index.isin([2690, 2691, 2692, 2693, 2694, 2695, 2696])]

# basin / area splitten bij rode lijn in twee vlakken
line = split_line_gdf.at[3, "geometry"]

total_basin_polygon = model.basin.area.df.at[544, "geometry"]
basin_polygon = next(i for i in model.basin.area.df.at[544, "geometry"].geoms if i.intersects(line))
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

# links v.a. tabulated_rating_curve 298 (ST01865) en 448 (ST01666) naar dinkel-basin
model.link.add(
    model.tabulated_rating_curve[298],
    dinkel_basin_node,
    geometry=link(model.tabulated_rating_curve[298].geometry, dinkel_basin_node.geometry),
)

model.link.add(
    model.tabulated_rating_curve[448],
    dinkel_basin_node,
    geometry=link(model.tabulated_rating_curve[448].geometry, dinkel_basin_node.geometry),
)

# link v.a. manning_resistance 915 naar dinkel basin
model.link.add(
    model.manning_resistance[915],
    dinkel_basin_node,
    geometry=link(model.manning_resistance[915].geometry, dinkel_basin_node.geometry),
)

# links v.a. dinkel basin naar tabulate_rating_curves 132 (ST02129) en 474 (ST02130)
model.link.add(
    dinkel_basin_node,
    model.tabulated_rating_curve[132],
    geometry=link(dinkel_basin_node.geometry, model.tabulated_rating_curve[132].geometry),
)

model.link.add(
    dinkel_basin_node,
    model.tabulated_rating_curve[474],
    geometry=link(dinkel_basin_node.geometry, model.tabulated_rating_curve[474].geometry),
)

# nieuwe manning_resistance in nieuwe dinkel-basin bovenstrooms kanaal
manning_node = model.manning_resistance.add(
    Node(geometry=hydroobject_gdf.at[7721, "geometry"].interpolate(0.5, normalized=True)), tables=[manning_data]
)

# nieuwe basin verbinden met nieuwe manning_resistance en nieuw kanaal basin
model.link.add(
    dinkel_basin_node,
    manning_node,
    geometry=link(dinkel_basin_node.geometry, manning_node.geometry),
)

model.link.add(
    manning_node,
    kanaal_basin_node,
    geometry=link(manning_node.geometry, kanaal_basin_node.geometry),
)

# nieuw kanaal-basin vervinden met tabulated_rating_curve 471 (ST01051)
model.link.add(
    kanaal_basin_node,
    model.tabulated_rating_curve[471],
    geometry=link(kanaal_basin_node.geometry, model.tabulated_rating_curve[471].geometry),
)

# nieuw kanaal-basin vervinden met manning_resistance 1346
model.link.add(
    kanaal_basin_node,
    model.manning_resistance[1346],
    geometry=link(kanaal_basin_node.geometry, model.manning_resistance[1346].geometry),
)

# nieuwe outletlet bij grensduiker kanaal
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[7746, "geometry"].boundary.geoms[0]), tables=[outlet_data]
)

# nieuwe basin verbinden met outlet verbinden met level_boundary 21
model.link.add(
    outlet_node,
    kanaal_basin_node,
    geometry=link(outlet_node.geometry, kanaal_basin_node.geometry),
)

model.link.add(
    model.level_boundary[21],
    outlet_node,
    geometry=link(model.level_boundary[21].geometry, outlet_node.geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2389192454
model.reverse_link(link_id=2685)
model.remove_node(node_id=2229, remove_links=True)
model.link.add(
    model.basin[1778],
    model.outlet[1080],
    geometry=link(model.basin[1778].geometry, model.outlet[1080].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2389198178
model.reverse_link(link_id=2715)
model.reverse_link(link_id=2720)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390712613

# Oplossen toplogische situatie kanaal Coevorden

# opheffen basin 2243 en basin 2182
model.remove_node(2243, remove_links=True)
model.remove_node(2182, remove_links=True)
model.remove_node(1351, remove_links=True)
model.remove_node(1268, remove_links=True)
model.remove_node(1265, remove_links=True)

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

# verwijderen links 809, 814, 807, 810, 1293, 2772
model.link.df = model.link.df[~model.link.df.index.isin([809, 814, 807, 810, 887])]


# verbinden manning 1270, 1127 en pumps 644, 579 en 649 met basin 1678
for node_id in [1270, 1127]:
    model.link.add(
        model.manning_resistance[node_id],
        model.basin[1678],
        geometry=link(model.manning_resistance[node_id].geometry, model.basin[1678].geometry),
    )

for node_id in [644, 579, 649]:
    model.link.add(
        model.pump[node_id],
        model.basin[1678],
        geometry=link(model.pump[node_id].geometry, model.basin[1678].geometry),
    )

# verplaatsen manning 1267 naar basin-link tussen 1678 en 1678
model.move_node(node_id=1267, geometry=hydroobject_gdf.loc[6609].geometry.boundary.geoms[1])

# maak nieuwe manning-node tussen 1678 en 1897
manning_node = model.manning_resistance.add(
    Node(geometry=hydroobject_gdf.loc[6596].geometry.interpolate(0.5, normalized=True)), tables=[manning_data]
)

# verbinden basin 1897 met manning-node
model.link.add(
    model.basin[1897],
    manning_node,
    geometry=link(model.basin[1897].geometry, manning_node.geometry),
)

model.link.add(
    manning_node,
    model.basin[1678],
    geometry=link(manning_node.geometry, model.basin[1678].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390952469

# Schoonebekerdiep v.a. Twist Bült

# verplaatsen basin 1909 nabij tabulated_rating_curve 383 (ST03607)
model.move_node(1909, geometry=hydroobject_gdf.loc[6865].geometry.boundary.geoms[1])

# verwijderen links 780 en 778
model.link.df = model.link.df[~model.link.df.index.isin([780, 778])]

# toevoegen link tussen tabulated_rating_curve 383 en basin 1909
model.link.add(
    model.tabulated_rating_curve[383],
    model.basin[1909],
    geometry=link(model.tabulated_rating_curve[383].geometry, model.basin[1909].geometry),
)

# toevoegen link tussen manning_resistance 851 en basin 1909
model.link.add(
    model.manning_resistance[851],
    model.basin[1909],
    geometry=link(model.manning_resistance[851].geometry, model.basin[1909].geometry),
)

# opknippen basin 1538 nabij 1909 en verbinden basin 1909 met 1539 via nieuwe manning_knoop
line = split_line_gdf.at[5, "geometry"]
model.split_basin(line=line)
manning_node = model.manning_resistance.add(
    Node(geometry=drop_z(line.intersection(hydroobject_gdf.at[6866, "geometry"]))), tables=[manning_data]
)

model.basin.node.df.loc[1909, "geometry"] = drop_z(model.basin[1909].geometry)
model.link.add(model.basin[1909], manning_node)
model.link.add(manning_node, model.basin[1539], geometry=link(manning_node.geometry, model.basin[1539].geometry))

# verwijderen link 2716,2718,2718,2719
model.link.df = model.link.df[~model.link.df.index.isin([2716, 2717, 2718, 2719])]

# opknippen basin 2181 nabij 1881 en verbinden basin 1881 met 2181 via nieuwe manning_knoop
model.move_node(1881, geometry=hydroobject_gdf.loc[6919].geometry.boundary.geoms[1])
line = split_line_gdf.at[6, "geometry"]
model.split_basin(line=line)
manning_node = model.manning_resistance.add(
    Node(geometry=line.intersection(hydroobject_gdf.at[6879, "geometry"])), tables=[manning_data]
)
model.basin.node.df.loc[1881, "geometry"] = drop_z(model.basin[1881].geometry)
model.link.add(model.basin[1881], manning_node)
model.link.add(manning_node, model.basin[2181], geometry=link(manning_node.geometry, model.basin[2181].geometry))

for node_id in [139, 251, 267, 205]:
    model.link.add(
        model.tabulated_rating_curve[node_id],
        model.basin[1881],
        geometry=link(model.tabulated_rating_curve[node_id].geometry, model.basin[1881].geometry),
    )

model.move_node(1269, hydroobject_gdf.at[7749, "geometry"].interpolate(0.5, normalized=True))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391168839

# Molengoot Hardenberg

# opheffen basin 1903
model.remove_node(1903, remove_links=True)

# verbinden basin 1433 met pump 621
model.link.add(
    model.basin[1433],
    model.pump[621],
    geometry=link(model.basin[1433].geometry, model.pump[621].geometry),
)

# verbinden tabulated_rating_curves 99 en 283 met basin 1433
for node_id in [99, 283]:
    model.link.add(
        model.tabulated_rating_curve[node_id],
        model.basin[1433],
        geometry=link(model.tabulated_rating_curve[node_id].geometry, model.basin[1433].geometry),
    )

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390898004

model.remove_node(1131, remove_links=True)
model.remove_node(1757, remove_links=True)
model.link.add(
    model.basin[1588],
    model.tabulated_rating_curve[112],
    geometry=link(model.basin[1588].geometry, model.tabulated_rating_curve[112].geometry),
)
model.link.add(
    model.basin[1588],
    model.manning_resistance[57],
    geometry=link(model.basin[1588].geometry, model.manning_resistance[57].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391191673

# verwijderen basin 1905
model.remove_node(1905, remove_links=True)

# verbinden manning_resistance 995 met basin 2148
model.link.add(
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
model.remove_node(2198, remove_links=True)
model.remove_node(2200, remove_links=True)
model.link.add(model.basin[2111], model.pump[671])
model.link.add(model.tabulated_rating_curve[542], model.basin[2111])
model.link.add(model.pump[671], model.basin[2316])
model.link.add(model.basin[2316], model.tabulated_rating_curve[542])

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391710413

model.remove_node(2202, remove_links=True)
model.link.add(model.basin[1590], model.pump[657])
model.link.add(model.manning_resistance[1058], model.basin[1590])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391672700

# Merge basin 2176 in 1605
model.merge_basins(basin_id=2176, to_basin_id=1605)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391726774
# Merge basins 2206 in 1518
model.merge_basins(basin_id=2206, to_basin_id=1518, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391734144
# dood takje uit Overijsselse Vecht
model.remove_node(2210, remove_links=True)
model.remove_node(1294, remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391740603

# Merge basin 2225 met 2304
model.merge_basins(basin_id=2225, to_node_id=2304)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391815016

# Wetteringe als laterale inflow
model.merge_basins(basin_id=2231, to_node_id=1853)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391750536

# Rondom SL00010 opruimen
model.remove_node(2230, remove_links=True)
model.remove_node(2251, remove_links=True)
model.link.add(model.outlet[41], model.level_boundary[15])
model.link.add(model.basin[1442], model.pump[664])
model.link.add(model.basin[1442], model.pump[665])


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

# Links 446, 1516, 443 en 444 verwijderen
model.link.df = model.link.df[~model.link.df.index.isin([446, 1516, 443, 444])]

# tabulated_rating_curves 202 en 230 verbinden met basin 2256
model.link.add(
    model.tabulated_rating_curve[202],
    model.basin[2256],
    geometry=link(model.tabulated_rating_curve[202].geometry, model.basin[2256].geometry),
)
model.link.add(
    model.tabulated_rating_curve[230],
    model.basin[2256],
    geometry=link(model.tabulated_rating_curve[230].geometry, model.basin[2256].geometry),
)

# resistance 954 verbinden met basin 2256
model.link.add(
    model.manning_resistance[954],
    model.basin[2256],
    geometry=link(model.manning_resistance[954].geometry, model.basin[2256].geometry),
)

# basin 2256 verbinden met resistance 1106
model.link.add(
    model.basin[2256],
    model.manning_resistance[1106],
    geometry=link(model.basin[2256].geometry, model.manning_resistance[1106].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391984234

# Merge basin 2261 in basin 1698
model.merge_basins(basin_id=2261, to_node_id=1698)
# model.remove_node(390, remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391995841

# Merge basin 2260 met basin 1645
model.merge_basins(basin_id=2260, to_node_id=1645)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392010526
# Merge basin 2220 met basin 1371
model.merge_basins(basin_id=2220, to_node_id=1371, are_connected=False)


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392017041

# Kanaal Almelo Nordhorn bij Almelo
model.merge_basins(basin_id=2219, to_node_id=1583, are_connected=False)
model.merge_basins(basin_id=2209, to_node_id=1583, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392022887

# Merge basin 2203 met 2227
model.merge_basins(basin_id=2203, to_node_id=2227, are_connected=False)
model.remove_node(1219, remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392026739

# Merge basin 2014 met 2144
model.merge_basins(basin_id=2014, to_node_id=2144)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392030268

# Merge basin 1696 met 1411
model.merge_basins(basin_id=1696, to_node_id=1411)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392037263

# Merge basin 2264 met 1459
model.merge_basins(basin_id=2264, to_node_id=1459)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392043973

# Merge basin 2212 en 2310
model.merge_basins(basin_id=2212, to_node_id=2310)
poly = model.basin.area.df.at[59, "geometry"].union(model.basin.area.df.set_index("node_id").at[2310, "geometry"])
model.basin.area.df.loc[model.basin.area.df.node_id == 2310, ["geometry"]] = MultiPolygon([poly])

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392048684

# Merge basin 2253 in basin 2228
model.merge_basins(basin_id=2253, to_node_id=2228)


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392052379

# Merge basin 2221 in basin 1634
model.merge_basins(basin_id=2221, to_node_id=1634)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392076634

# Verbinding rondwaterleiding / Lennelwaterleiding herstellen
model.merge_basins(basin_id=1859, to_node_id=2235, are_connected=False)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382572457

# Administratie basin node_id in node_table en Basin / Area correct maken
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest", distance=100)
model.fix_unassigned_basin_area()
model.basin.area.df = model.basin.area.df[~model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]

# %%
# corrigeren knoop-topologie

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet", data=[outlet_data])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382578661

# opvullen gaten
basin_polygon = model.basin.area.df.union_all()
# holes = [Polygon(interior) for polygon in basin_polygon.buffer(10).buffer(-10).geoms for interior in polygon.interiors]
holes = [Polygon(interior) for interior in basin_polygon.buffer(10).buffer(-10).interiors]
holes_df = gpd.GeoSeries(holes, crs=28992)
holes_df.index = holes_df.index + 1

# splitsen Alemelo-Nordhorn / Overijsselskanaal. Overijsselskanaal zit in HWS
line = split_line_gdf.at[12, "geometry"]
idx = holes_df[holes_df.intersects(line)].index[0]
poly = split_basin(holes_df[holes_df.intersects(line)].iloc[0], line).geoms[0]
poly = model.basin.area.df.set_index("node_id").at[1583, "geometry"].union(poly)
model.basin.area.df.loc[model.basin.area.df.node_id == 1583, ["geometry"]] = MultiPolygon([poly])

# Split Overijsselskanaal bij Zwolsekanaal
line = split_line_gdf.at[13, "geometry"]
poly1, poly2 = split_basin(holes_df[holes_df.intersects(line)].iloc[0], line).geoms

poly1 = model.basin.area.df.set_index("node_id").at[2116, "geometry"].union(poly1)
poly1 = MultiPolygon([i for i in poly1.geoms if i.geom_type == "Polygon"])
model.basin.area.df.loc[model.basin.area.df.node_id == 2116, ["geometry"]] = poly1

poly2 = model.basin.area.df.set_index("node_id").at[2115, "geometry"].union(poly2)
poly2 = MultiPolygon([i for i in poly2.geoms if i.geom_type == "Polygon"] + [holes_df.loc[38], holes_df.loc[29]])
model.basin.area.df.loc[model.basin.area.df.node_id == 2115, ["geometry"]] = poly2

# de rest gaan we automatisch vullen
holes_df = holes_df[~holes_df.index.isin([10, 22, 29, 32, 38, 39, 41])]

drainage_areas_df = drainage_areas_df[drainage_areas_df.buffer(-10).intersects(basin_polygon)]

for idx, geometry in enumerate(holes_df):
    # select drainage-area
    drainage_area_select = drainage_areas_df[drainage_areas_df.contains(geometry.buffer(-10))]
    if not drainage_area_select.empty:
        if not len(drainage_area_select) == 1:
            raise ValueError("hole contained by multiple drainage areas, can't fix that yet")

        drainage_area = drainage_area_select.iloc[0].geometry

        # find basin_id to merge to
        selected_basins_df = model.basin.area.df[
            model.basin.area.df.node_id.isin(model.basin.node.df[model.basin.node.df.within(drainage_area)].index)
        ].set_index("node_id")
        if selected_basins_df.empty:
            selected_basins_df = model.basin.area.df[
                model.basin.area.df.buffer(-10).intersects(drainage_area)
            ].set_index("node_id")

        assigned_basin_id = selected_basins_df.intersection(geometry.buffer(10)).area.idxmax()

        # clip and merge geometry
        geometry = geometry.buffer(10).difference(basin_polygon)
        geometry = (
            model.basin.area.df.set_index("node_id")
            .at[assigned_basin_id, "geometry"]
            .union(geometry)
            .buffer(0.1)
            .buffer(-0.1)
        )

        if isinstance(geometry, Polygon):
            geometry = MultiPolygon([geometry])
        model.basin.area.df.loc[model.basin.area.df.node_id == assigned_basin_id, "geometry"] = geometry
# buffer out small slivers
model.basin.area.df.loc[:, ["geometry"]] = (
    model.basin.area.df.buffer(0.1)
    .buffer(-0.1)
    .apply(lambda x: x if x.geom_type == "MultiPolygon" else MultiPolygon([x]))
)
# %%
# Fix small nodata holes (slivers in the basins: https://github.com/Deltares/Ribasim-NL/issues/316 )
ribasim_areas_gdf = ribasim_areas_gdf.to_crs(model.basin.area.df.crs)
ribasim_areas_gdf.loc[:, "geometry"] = ribasim_areas_gdf.buffer(-0.01).buffer(0.01)

# Exclude Twentekanaal by setting their geometry to NaN
codes_to_exclude = [
    "AFW_E/20",
    "AFW_E/TWK1600/30",
    "AFW_E/20/005",
    "AFW_E/TWK1600/20",
    "AFW_E/20/10",
    "AFW_E/TWK2500/10",
    "AFW_E/ENSCHEDE 3",
    "AFW_E/20/010",
    "AFW_E/20/030",
    "AFW_E/24/010",
]
ribasim_areas_gdf.loc[ribasim_areas_gdf["code"].isin(codes_to_exclude), "geometry"] = pd.NA

# Process basin area
processed_basin_area_df = model.basin.area.df.copy()
processed_basin_area_df = processed_basin_area_df.dissolve(by="node_id").reset_index()
processed_basin_area_df = processed_basin_area_df[processed_basin_area_df["geometry"].notna()]
processed_basin_area_df = processed_basin_area_df[processed_basin_area_df.geometry.area > 0]
processed_basin_area_df = processed_basin_area_df[~processed_basin_area_df.geometry.is_empty]

# Combine all geometries into a single polygon
combined_geometry = processed_basin_area_df.geometry.union_all()
combined_basin_area_gdf = gpd.GeoDataFrame(geometry=[combined_geometry], crs=processed_basin_area_df.crs)

# Get the bounding box and calculate internal NoData areas
bounding_box = combined_basin_area_gdf.geometry.union_all().envelope
internal_no_data_areas = bounding_box.difference(combined_geometry)
internal_no_data_gdf = gpd.GeoDataFrame(geometry=[internal_no_data_areas], crs=combined_basin_area_gdf.crs)
exploded_internal_no_data_gdf = internal_no_data_gdf.explode(index_parts=True).reset_index(drop=True)

# Apply area threshold for sliver removal
threshold_area = 10
exploded_internal_no_data_gdf = exploded_internal_no_data_gdf[
    exploded_internal_no_data_gdf.geometry.area > threshold_area
]

# Clip to remove areas where ribasim_areas_gdf is NoData (NaN geometries)
ribasim_not_na_gdf = ribasim_areas_gdf[~ribasim_areas_gdf.geometry.isna()]
exploded_internal_no_data_gdf = gpd.overlay(
    exploded_internal_no_data_gdf, ribasim_not_na_gdf, how="intersection", keep_geom_type=True
)
unique_codes = exploded_internal_no_data_gdf["code"].unique()
filtered_ribasim_areas_gdf = ribasim_areas_gdf[ribasim_areas_gdf["code"].isin(unique_codes)]
combined_basin_areas_gdf = gpd.overlay(
    filtered_ribasim_areas_gdf, model.basin.area.df, how="union", keep_geom_type=True
).explode()
combined_basin_areas_gdf["area"] = combined_basin_areas_gdf.geometry.area
non_null_basin_areas_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()]
largest_area_node_ids = non_null_basin_areas_gdf.loc[
    non_null_basin_areas_gdf.groupby("code")["area"].idxmax(), ["code", "node_id"]
]
combined_basin_areas_gdf = combined_basin_areas_gdf.merge(
    largest_area_node_ids, on="code", how="left", suffixes=("", "_largest")
)

# Fill missing node_id with the largest_area node_id
combined_basin_areas_gdf["node_id"] = combined_basin_areas_gdf["node_id"].fillna(
    combined_basin_areas_gdf["node_id_largest"]
)

combined_basin_areas_gdf.drop(columns=["node_id_largest"], inplace=True)
combined_basin_areas_gdf = combined_basin_areas_gdf.drop_duplicates()
combined_basin_areas_gdf = combined_basin_areas_gdf.dissolve(by="node_id").reset_index()
combined_basin_areas_gdf = combined_basin_areas_gdf[["node_id", "geometry"]]
combined_basin_areas_gdf.index.name = "fid"
model.basin.area.df = combined_basin_areas_gdf

# buffer out small slivers
model.basin.area.df.loc[:, ["geometry"]] = (
    model.basin.area.df.buffer(0.1)
    .buffer(-0.1)
    .apply(lambda x: x if x.geom_type == "MultiPolygon" else MultiPolygon([x]))
)
# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)


# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(authority=authority, layers=["gemaal", "stuw", "sluis"])
df.set_index("code", inplace=True)
names = df["naam"]

# set meta_gestuwd in basins
model.basin.node.df["meta_gestuwd"] = False
model.outlet.node.df["meta_gestuwd"] = False
model.pump.node.df["meta_gestuwd"] = True

# set stuwen als gestuwd

model.outlet.node.df.loc[model.outlet.node.df["meta_object_type"].isin(["stuw"]), "meta_gestuwd"] = True

# set bovenstroomse basins als gestuwd
node_df = model.node_table().df
node_df = node_df[(node_df["meta_gestuwd"] == True) & node_df["node_type"].isin(["Outlet", "Pump"])]  # noqa: E712

upstream_node_ids = [model.upstream_node_id(i) for i in node_df.index]
basin_mask = model.basin.node.df.index.isin(upstream_node_ids)
model.basin.node.df.loc[basin_mask, "meta_gestuwd"] = True

# set álle benedenstroomse outlets van gestuwde basins als gestuwd (dus ook duikers en andere objecten)
downstream_node_ids = (
    pd.Series([model.downstream_node_id(i) for i in model.basin.node.df[basin_mask].index]).explode().to_numpy()
)
model.outlet.node.df.loc[model.outlet.node.df.index.isin(downstream_node_ids), "meta_gestuwd"] = True


sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_object_type", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump", "TabulatedRatingCurve"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "ManningResistance"], "columns": {"name": ""}},
        {"node_types": ["LevelBoundary", "FlowBoundary"], "columns": {"meta_name": "name"}},
    ],
    names=names,
)

# %%
actions = [
    "remove_basin_area",
    "remove_node",
    "remove_link",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_link",
    "connect_basins",
    "move_node",
    "update_node",
    "redirect_link",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_gpkg).name.to_list()]
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    df = gpd.read_file(model_edits_gpkg, layer=action, fid_as_index=True)
    if "order" in df.columns:
        df.sort_values("order", inplace=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = row._asdict()
        if inspect.getfullargspec(method).varkw != "kwargs":
            keywords = inspect.getfullargspec(method).args
            kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# remove unassigned basin area
model.remove_unassigned_basin_area()
# %% some customs
# remove unassigned basin area
model.redirect_link(link_id=89, to_node_id=1561)
model.redirect_link(link_id=1989, from_node_id=2333)
model.redirect_link(link_id=1990, from_node_id=2333)
model.merge_basins(basin_id=2115, to_node_id=1405)
model.merge_basins(basin_id=1378, to_node_id=1431)
model.merge_basins(basin_id=2211, to_node_id=1727)
model.merge_basins(basin_id=1538, to_node_id=33)
model.merge_basins(basin_id=1963, to_node_id=1518)
model.merge_basins(basin_id=2245, to_node_id=1818)
model.merge_basins(basin_id=2026, to_node_id=1818)
model.merge_basins(basin_id=1412, to_node_id=2107)
model.merge_basins(basin_id=1592, to_node_id=1765)
model.merge_basins(basin_id=1765, to_node_id=1817)
model.merge_basins(basin_id=2159, to_node_id=1890)
model.merge_basins(basin_id=1654, to_node_id=2163)
model.merge_basins(basin_id=2254, to_node_id=1493, are_connected=False)
# model.merge_basins(basin_id=1604, to_node_id=1890)
model.merge_basins(basin_id=1628, to_node_id=2143)
model.merge_basins(basin_id=1821, to_node_id=2143)
model.merge_basins(basin_id=2144, to_node_id=2143)
model.merge_basins(basin_id=2116, to_node_id=1730)
model.merge_basins(basin_id=2177, to_node_id=1730)
model.remove_node(node_id=619, remove_links=True)
model.remove_node(node_id=660, remove_links=True)
model.remove_node(node_id=698, remove_links=True)
model.remove_node(node_id=1243, remove_links=True)
model.remove_node(node_id=1242, remove_links=True)
model.remove_node(node_id=1252, remove_links=True)
model.remove_node(node_id=836, remove_links=True)
model.remove_node(node_id=80, remove_links=True)
model.remove_node(node_id=265, remove_links=True)
model.remove_node(node_id=126, remove_links=True)
model.remove_node(166, remove_links=True)
model.remove_node(313, remove_links=True)
model.remove_node(393, remove_links=True)
model.remove_node(146, remove_links=True)
model.remove_node(835, remove_links=True)
model.remove_node(1370, remove_links=True)
model.remove_node(358, remove_links=True)
model.remove_node(188, remove_links=True)
model.remove_node(219, remove_links=True)
model.remove_node(345, remove_links=True)
model.remove_node(654, remove_links=True)
model.remove_node(1045, remove_links=True)
model.remove_node(125, remove_links=True)
model.remove_node(601, remove_links=True)
model.remove_node(121, remove_links=True)
model.remove_node(590, remove_links=True)
model.remove_node(624, remove_links=True)
model.remove_node(573, remove_links=True)
model.remove_node(570, remove_links=True)
model.remove_node(657, remove_links=True)
model.remove_node(652, remove_links=True)
model.remove_node(633, remove_links=True)
model.remove_node(178, remove_links=True)
model.remove_node(319, remove_links=True)
model.remove_node(395, remove_links=True)
model.remove_node(114, remove_links=True)
model.remove_node(442, remove_links=True)
model.remove_node(562, remove_links=True)
model.remove_node(438, remove_links=True)
model.remove_node(167, remove_links=True)
model.remove_node(561, remove_links=True)
model.remove_node(456, remove_links=True)
model.remove_node(673, remove_links=True)
model.remove_node(1128, remove_links=True)
model.merge_basins(basin_id=1488, to_basin_id=1834)
model.merge_basins(basin_id=1848, to_basin_id=2158)
model.merge_basins(basin_id=1891, to_basin_id=2158)
model.merge_basins(basin_id=1994, to_basin_id=2158)
model.merge_basins(basin_id=1648, to_basin_id=1826)
model.merge_basins(basin_id=1826, to_node_id=1843)
model.merge_basins(basin_id=1646, to_basin_id=1513)
model.merge_basins(basin_id=1897, to_basin_id=1678)
model.merge_basins(basin_id=2181, to_basin_id=1678)
model.merge_basins(basin_id=1678, to_basin_id=1700)
model.merge_basins(basin_id=1876, to_basin_id=1633)
model.merge_basins(basin_id=2187, to_basin_id=1873)
model.merge_basins(basin_id=2174, to_basin_id=1873)
model.merge_basins(basin_id=1875, to_basin_id=1879)
model.merge_basins(basin_id=1908, to_basin_id=1879)
model.merge_basins(basin_id=1902, to_basin_id=1879)
model.merge_basins(basin_id=1441, to_basin_id=1879)
model.merge_basins(basin_id=1957, to_basin_id=1879)
model.merge_basins(basin_id=1717, to_basin_id=1528)
model.merge_basins(basin_id=1995, to_basin_id=1442)
model.merge_basins(basin_id=2262, to_basin_id=1431)
model.merge_basins(basin_id=1853, to_basin_id=1852)
model.merge_basins(basin_id=2137, to_basin_id=2138)
model.merge_basins(basin_id=1819, to_basin_id=2138)
model.merge_basins(basin_id=33, to_basin_id=1377)
model.merge_basins(basin_id=1560, to_basin_id=1478)
model.merge_basins(basin_id=2228, to_basin_id=2121)
model.merge_basins(basin_id=2050, to_basin_id=1812)
model.merge_basins(basin_id=1811, to_basin_id=1812)
model.merge_basins(basin_id=1386, to_basin_id=2157)
model.merge_basins(basin_id=2307, to_basin_id=2157)
model.merge_basins(basin_id=1610, to_basin_id=1454)
model.merge_basins(basin_id=1664, to_basin_id=1588)
model.merge_basins(basin_id=2234, to_basin_id=1418)
model.merge_basins(basin_id=2317, to_basin_id=1700)
model.merge_basins(basin_id=1982, to_basin_id=1431)
model.merge_basins(basin_id=1589, to_basin_id=1431)

model.remove_node(413, remove_links=True)
model.remove_node(842, remove_links=True)
model.remove_node(463, remove_links=True)
model.remove_node(341, remove_links=True)
model.remove_node(235, remove_links=True)
model.merge_basins(basin_id=1584, to_basin_id=1817)
model.merge_basins(basin_id=1817, to_basin_id=2135)
model.merge_basins(basin_id=1588, to_basin_id=1561)
# %%

# sanitize node-table
for node_id in model.tabulated_rating_curve.node.df.index:
    model.update_node(node_id=node_id, node_type="Outlet")

# ManningResistance that are duikersifonhevel to outlet
for node_id in model.manning_resistance.node.df[
    model.manning_resistance.node.df["meta_object_type"] == "duikersifonhevel"
].index:
    model.update_node(node_id=node_id, node_type="Outlet")

# nodes we've added do not have category, we fill with hoofdwater
for node_type in model.node_table().df.node_type.unique():
    table = getattr(model, pascal_to_snake_case(node_type)).node
    table.df.loc[table.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# %%
model.flow_boundary.node.df["meta_categorie"] = "buitenlandse aanvoer"

# set meta_gestuwd in basins
model.basin.node.df["meta_gestuwd"] = False
model.outlet.node.df["meta_gestuwd"] = False
model.pump.node.df["meta_gestuwd"] = True

# set stuwen als gestuwd

model.outlet.node.df.loc[model.outlet.node.df["meta_object_type"].isin(["stuw"]), "meta_gestuwd"] = True

# set bovenstroomse basins als gestuwd
node_df = model.node_table().df
node_df = node_df[(node_df["meta_gestuwd"] == True) & node_df["node_type"].isin(["Outlet", "Pump"])]  # noqa: E712

upstream_node_ids = [model.upstream_node_id(i) for i in node_df.index]
basin_mask = model.basin.node.df.index.isin(upstream_node_ids)
model.basin.node.df.loc[basin_mask, "meta_gestuwd"] = True

# set álle benedenstroomse outlets van gestuwde basins als gestuwd (dus ook duikers en andere objecten)
downstream_node_ids = (
    pd.Series([model.downstream_node_id(i) for i in model.basin.node.df[basin_mask].index]).explode().to_numpy()
)
model.outlet.node.df.loc[model.outlet.node.df.index.isin(downstream_node_ids), "meta_gestuwd"] = True

#  %% write model
model.basin.area.df.loc[:, ["meta_area"]] = model.basin.area.df.area
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%
if run_model:
    result = model.run()
    assert result.exit_code == 0

# %%
import inspect

import geopandas as gpd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.geometry import split_basin_multi_polygon
from ribasim_nl.reset_static_tables import reset_static_tables

cloud = CloudStorage()

authority = "DrentsOverijsselseDelta"
short_name = "dod"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{short_name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")
hydroobject_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="hydroobject", fid_as_index=True
)
duikersifonhevel_gdf = gpd.read_file(
    cloud.joinpath(authority, "aangeleverd", "Aanlevering_202311", "HyDAMO_WM_20231117.gpkg"),
    fid_as_index=True,
    layer="duikersifonhevel",
)

split_line_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "fix_user_data.gpkg"), layer="split_basins", fid_as_index=True
)

# Load node edit data
model_edits_url = cloud.joinurl(authority, "verwerkt", "model_edits.gpkg")
model_edits_path = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")
if not model_edits_path.exists():
    cloud.download_file(model_edits_url)


# level_boundary_gdf = gpd.read_file(
#     cloud.joinpath(authority, "verwerkt", "fix_user_data.gpkg"), layer="level_boundary", fid_as_index=True
# )

# %% read model
model = Model.read(ribasim_toml)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_network", f"{short_name}.toml")
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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393424844
# Verwijderen duplicate edges

model.edge.df.drop_duplicates(inplace=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393458802

# Toevoegen ontbrekende basins (oplossen topologie)
model.remove_node(7, remove_edges=True)
model.remove_node(84, remove_edges=True)
basin_edges_df = network_validator.edge_incorrect_connectivity()
basin_nodes_df = network_validator.node_invalid_connectivity()

for row in basin_nodes_df.itertuples():
    # maak basin-node
    basin_node = model.basin.add(Node(geometry=row.geometry), tables=basin_data)

    # update edge_table
    model.edge.df.loc[basin_edges_df[basin_edges_df.from_node_id == row.node_id].index, ["from_node_id"]] = (
        basin_node.node_id
    )
    model.edge.df.loc[basin_edges_df[basin_edges_df.to_node_id == row.node_id].index, ["to_node_id"]] = (
        basin_node.node_id
    )

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393672367

# Omdraaien edge-richting rondom outlets (inlaten/uitlaten)
# for edge_id in [2282, ]

# https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393731749

# Opruimen Reeve

# basin 2484 wordt LevelBoundary (IJssel)
model.update_node(2484, "LevelBoundary", data=[level_data])

# nodes 1536, 762, 761, 1486 + aangesloten edges gooien we weg
for node_id in [1536, 762, 761, 1486]:
    model.remove_node(node_id, remove_edges=True)

# edges 2841, 2842, 2843, 2846 gooien we weg
model.remove_edges([2841, 2842, 2843, 2846])

# duiker 286309 voegen we toe
kdu = duikersifonhevel_gdf.loc[9063]
outlet_node = model.outlet.add(
    Node(geometry=kdu.geometry.interpolate(0.5, normalized=True), name=f"duikersifonhevel.{kdu.objectid}"),
    tables=[outlet_data],
)

model.edge.add(model.level_boundary[10], outlet_node)
model.edge.add(outlet_node, model.basin[2240])
model.edge.add(model.manning_resistance[849], model.basin[2240])
model.edge.add(model.manning_resistance[760], model.basin[2240])
model.edge.add(model.tabulated_rating_curve[187], model.basin[2240])
model.edge.add(model.basin[2240], model.pump[1100])

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393871075

# Ramsgeul bij Ramspol
for node_id in [81, 839]:
    model.remove_node(node_id, remove_edges=True)

model.update_node(83, "Basin", data=basin_data)
model.move_node(83, hydroobject_gdf.at[21045, "geometry"].boundary.geoms[0])
model.reverse_edge(edge_id=3013)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399104407

# Deelstroomgebied Frysland

# Nieuwe Kanaal / Tussen Linde voorzien van basin nabij gemaal.20 (node_id 701)
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[19671, "geometry"].boundary.geoms[1]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[19671, "geometry"].interpolate(0.5, normalized=True)),
    tables=[outlet_data],
)

# basin 1623 verbinden met inlaatduikers (3x) en gemaal 1623; overige verbindingen verwijderen.
model.remove_edges([3038, 3040, 3037, 3041, 3039])

# nw basin verbinden met gemaal 20, level boundary 94 en alle inlaatduikers
model.reverse_edge(edge_id=2282)
model.edge.add(outlet_node, model.level_boundary[94])
model.edge.add(basin_node, outlet_node)
model.edge.add(basin_node, model.manning_resistance[1182])
model.edge.add(basin_node, model.manning_resistance[969])
model.edge.add(basin_node, model.manning_resistance[1050])
model.edge.add(basin_node, model.outlet[539])
model.edge.add(model.pump[701], basin_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399209787

# Aansluiten NW boezem op Fryslan

# basin /area 1681 op te knippen nabij basin 1717 (rode lijn)
model.split_basin(geometry=split_line_gdf.at[14, "geometry"])
model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != 1717]

# basin 1682 te veranderen in een LevelBoundary
model.update_node(1682, "LevelBoundary", [level_data])

# Alle edges die nu naar basin 1717 lopen naar LevelBoundary 1682 of opheffen
model.remove_node(27, remove_edges=True)
model.remove_node(1556, remove_edges=True)
model.remove_edges([945, 2537, 2536])

boundary_node = model.level_boundary.add(Node(geometry=hydroobject_gdf.at[7778, "geometry"].boundary.geoms[0]))

model.edge.add(model.pump[642], boundary_node)
model.update_node(1202, "Outlet", data=[outlet_data])
model.edge.add(boundary_node, model.outlet[1202])
model.update_node(1203, "Outlet", data=[outlet_data])
model.edge.add(boundary_node, model.outlet[1203])

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399328441

# Misc pump benedenstroomse edges
for edge_id in [2862, 3006, 3049]:
    model.reverse_edge(edge_id=edge_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399355028

# Misc tabulated_rating_curve (stuwen) stroomrichting
for edge_id in [1884, 2197]:
    model.reverse_edge(edge_id=edge_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399382478

# Misc manning_resistance (duikers) stroomrichting
for edge_id in [1081, 518]:
    model.reverse_edge(edge_id=edge_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399425885
# Opknippen NW boezem

poly1, poly2 = split_basin_multi_polygon(model.basin.area.df.at[598, "geometry"], split_line_gdf.at[15, "geometry"])
model.basin.area.df.loc[model.basin.area.df.node_id == 1681, ["geometry"]] = poly1

poly1, poly2 = split_basin_multi_polygon(poly2, split_line_gdf.at[16, "geometry"])
model.basin.area.df.loc[598] = {"node_id": 1686, "geometry": poly1}

poly1, poly2 = split_basin_multi_polygon(poly2, split_line_gdf.at[17, "geometry"])
model.basin.area.df.loc[model.basin.area.df.index.max() + 1] = {"geometry": poly1, "node_id": 1695}
model.basin.area.df.crs = model.crs

tables = basin_data + [basin.Area(geometry=[poly2])]
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[19608, "geometry"].boundary.geoms[1]), tables=tables)


model.move_node(1686, hydroobject_gdf.at[19566, "geometry"].boundary.geoms[1])
model.merge_basins(basin_id=2426, to_basin_id=1696, are_connected=True)
model.merge_basins(basin_id=2460, to_basin_id=1696, are_connected=True)
model.merge_basins(basin_id=1648, to_basin_id=1696, are_connected=True)

model.merge_basins(basin_id=1696, to_basin_id=2453, are_connected=True)

model.merge_basins(basin_id=2453, to_basin_id=1686, are_connected=True)
model.merge_basins(basin_id=1719, to_basin_id=1686, are_connected=True)
model.merge_basins(basin_id=1858, to_basin_id=1686, are_connected=True)

model.remove_node(1532, remove_edges=True)
model.remove_node(722, remove_edges=True)
model.remove_node(536, remove_edges=True)
model.remove_node(2506, remove_edges=True)

edge_ids = [
    2866,
    2867,
    2868,
    2869,
    2870,
    2871,
    2872,
    2873,
    2875,
    2876,
    2877,
    2878,
    2879,
    2880,
    2881,
    2883,
    2885,
    2886,
    2889,
    2890,
    2891,
    2895,
    2897,
    2899,
    2901,
    2902,
    2903,
    2905,
    2906,
    2907,
    2908,
    2910,
    2911,
    2912,
    2913,
    2915,
    2916,
    2918,
]

for edge_id in edge_ids:
    model.redirect_edge(edge_id, to_node_id=basin_node.node_id)

model.remove_edges([2887, 2892])
model.edge.add(basin_node, model.pump[547])
model.edge.add(basin_node, model.outlet[540])

for edge_id in [2914, 2894]:
    model.redirect_edge(edge_id, to_node_id=31)

model.redirect_edge(461, to_node_id=1585)

model.basin.area.df.loc[model.basin.area.df.node_id == 1545, ["node_id"]] = 1585

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2400050483

# Ontbrekende basin beneden-Vecht
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[12057, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[12057, "geometry"].interpolate(0.5, normalized=True)), tables=[outlet_data]
)

for edge_id in [2956, 2957, 2958, 2959, 2960, 2961]:
    model.redirect_edge(edge_id, to_node_id=basin_node.node_id)

model.remove_node(76, remove_edges=True)
model.edge.add(basin_node, model.pump[598])
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[50])


# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399931763

# Samenvoegen Westerveldse Aa
model.merge_basins(basin_id=1592, to_basin_id=1645, are_connected=True)
model.merge_basins(basin_id=1593, to_basin_id=1645, are_connected=True)

model.merge_basins(basin_id=1645, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2567, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2303, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2549, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2568, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2572, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2374, to_basin_id=1585, are_connected=True)

model.merge_basins(basin_id=2559, to_basin_id=2337, are_connected=False)


# %%
# corrigeren knoop-topologie

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.edge_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.edge_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")

# %%
model.explode_basin_area()  # all multipolygons to singles

# update from layers
actions = [
    "remove_basin_area",
    "split_basin",
    "merge_basins",
    "add_basin",
    "update_node",
    "add_basin_area",
    "update_basin_area",
    "redirect_edge",
    "reverse_edge",
    "deactivate_node",
    "move_node",
    "remove_node",
    "connect_basins",
]

actions = [i for i in actions if i in gpd.list_layers(model_edits_path).name.to_list()]
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_path, layer=action, fid_as_index=True)
    if "order" in df.columns:
        df.sort_values("order", inplace=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# remove unassigned basin area
model.fix_unassigned_basin_area()

model = reset_static_tables(model)
#  %% write model
model.use_validation = True
model.write(ribasim_toml)

model.invalid_topology_at_node().to_file(ribasim_toml.with_name("invalid_topology_at_connector_nodes.gpkg"))
model.report_basin_area()
model.report_internal_basins()
# %%

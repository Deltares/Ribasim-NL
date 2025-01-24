# %%

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.reset_static_tables import reset_static_tables

# Initialize cloud storage and set authority/model parameters

cloud = CloudStorage()

authority = "Limburg"
name = "limburg"

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")

cloud.synchronize(filepaths=[ribasim_dir, ribasim_toml, database_gpkg, hydamo_gpkg, model_edits_gpkg])

# %% read model

model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)

hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
duiker_gdf = gpd.read_file(hydamo_gpkg, layer="duikersifonhevel", fid_as_index=True)
basin_node_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="unassigned_basin_node")
rename_basin_area_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="rename_basin_area")
add_basin_area_gdf = gpd.read_file(model_edits_gpkg, layer="add_basin_area")
connect_basins_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="connect_basins")
reverse_edge_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="reverse_edge")
add_basin_outlet_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="add_basin_outlet")


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


# HIER KOMEN ISSUES

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426118811

# Verwijderen edge met 0m lengte
model.remove_node(2434, remove_edges=True)
model.remove_node(1308, remove_edges=True)
model.merge_basins(basin_id=2396, to_basin_id=1669, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426151899

# Corrigeren ontbrekende basins en outlets nabij modelrand
# geen kdu bij 2202
geometry = hydroobject_gdf.at[3099, "geometry"]
basin_node = model.basin.add(Node(geometry=geometry.boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(Node(geometry=geometry.interpolate(271)), tables=[outlet_data])
model.redirect_edge(edge_id=2202, from_node_id=outlet_node.node_id)
model.edge.add(basin_node, outlet_node)

for fid, edge_id, boundary_node_id in ((2054, 2244, 63), (9794, 2295, 103), (9260, 2297, 105), (3307, 2305, 113)):
    kdu = duiker_gdf.loc[fid]
    basin_node = model.basin.add(
        Node(geometry=model.edge.df.loc[edge_id, "geometry"].boundary.geoms[0]), tables=basin_data
    )
    outlet_node = model.outlet.add(
        Node(
            name=kdu.code, geometry=kdu.geometry.interpolate(0.5, normalized=True), meta_object_type="duikersifonhevel"
        ),
        tables=[outlet_data],
    )
    model.redirect_edge(edge_id=edge_id, from_node_id=outlet_node.node_id)
    model.edge.add(basin_node, outlet_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426258242

# Corrigeren netwerk bij Jeker
for node_id in [276, 2003, 990, 2395, 989]:
    model.remove_node(node_id, remove_edges=True)

basin_node = model.basin.add(Node(geometry=model.edge.df.at[2257, "geometry"].boundary.geoms[1]))
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[2099, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)
model.redirect_edge(edge_id=2257, to_node_id=basin_node.node_id)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[82])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426373368

# Corrigeren Snelle Loop bij Defensiekanaal
outlet_node = model.outlet.add(
    Node(geometry=model.edge.df.at[2357, "geometry"].boundary.geoms[1]), tables=[outlet_data]
)
model.redirect_edge(edge_id=2357, to_node_id=outlet_node.node_id)
model.edge.add(outlet_node, model.basin[1452])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426401489

# Corrigeren Panheelsebeek
model.remove_node(node_id=940, remove_edges=True)
model.reverse_edge(edge_id=211)
model.merge_basins(basin_id=2465, to_basin_id=1340, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426443778

# Toevoegen Wellse Molenbeek
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[3100, "geometry"].boundary.geoms[1]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[687, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)

model.redirect_edge(edge_id=2240, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2239, from_node_id=basin_node.node_id, to_node_id=outlet_node.node_id)
model.edge.add(basin_node, model.manning_resistance[425])
model.edge.add(outlet_node, model.level_boundary[59])


# %%
model.remove_node(node_id=1036, remove_edges=True)
kdu = duiker_gdf.loc[4664]
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[477, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(name=kdu.code, geometry=kdu.geometry.interpolate(0.5, normalized=True), meta_object_type="duikersifonhevel"),
    tables=[outlet_data],
)
model.edge.add(outlet_node, model.basin[1389])
model.edge.add(basin_node, outlet_node)

kdu = duiker_gdf.loc[3709]
outlet_node = model.outlet.add(
    Node(name=kdu.code, geometry=kdu.geometry.interpolate(0.5, normalized=True), meta_object_type="duikersifonhevel"),
    tables=[outlet_data],
)

model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[39])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426653554

# Correctie Panheelderbeek bij kanaal Wessem-Nederweert
model.remove_edges(edge_ids=[2316, 2309, 2307, 2308, 2310, 2312, 2315, 2317])
model.remove_node(114, remove_edges=True)
model.reverse_edge(edge_id=1999)
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[1649, "geometry"].boundary.geoms[1]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[4110, "geometry"].boundary.geoms[1]), tables=[outlet_data]
)

model.edge.add(model.tabulated_rating_curve[270], basin_node)
model.edge.add(basin_node, model.manning_resistance[1316])
model.edge.add(basin_node, model.manning_resistance[1315])
model.edge.add(basin_node, model.manning_resistance[1130])
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[115])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426706167

# Correctie edge-richting bij Ijsselsteinseweg

model.reverse_edge(edge_id=2332)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426763136

# Opname Helenavaart
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[112, "geometry"].boundary.geoms[0]), tables=basin_data)
model.redirect_edge(edge_id=2329, to_node_id=basin_node.node_id)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[112, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[123])

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[1702, "geometry"].interpolate(0.1, normalized=True)), tables=[outlet_data]
)
model.redirect_edge(edge_id=2328, to_node_id=outlet_node.node_id)
model.edge.add(outlet_node, basin_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426789675

# Opname Oude Helenavaart/kanaal van Deurne
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[1565, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[3277, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)

model.redirect_edge(edge_id=2327, from_node_id=outlet_node.node_id)
model.edge.add(basin_node, outlet_node)

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[1565, "geometry"].interpolate(0.98, normalized=True)), tables=[outlet_data]
)
model.redirect_edge(edge_id=2323, from_node_id=outlet_node.node_id)
model.edge.add(basin_node, outlet_node)

model.redirect_edge(edge_id=2326, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2325, to_node_id=basin_node.node_id)

model.edge.add(model.tabulated_rating_curve[238], basin_node)

# 2 edges die afwateren op Oude Helenavaart
model.remove_edges(edge_ids=[2322, 2324])


# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426816843

# Verwijderen afwaterende basisn Mooks kanaal
model.remove_node(34, remove_edges=True)

# EINDE ISSUES


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


# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)
model.fix_unassigned_basin_area()
# %%
# rename node-ids
df = rename_basin_area_gdf.set_index("ribasim_fid")
model.basin.area.df.loc[df.index, ["node_id"]] = df["to_node_id"].astype("int32")

# %%add basin area
for row in add_basin_area_gdf.itertuples():
    model.add_basin_area(node_id=row.node_id, geometry=row.geometry)

# %% merge_basins

# merge basins
selection_df = basin_node_edits_gdf[basin_node_edits_gdf["to_node_id"].notna()]
for row in selection_df.itertuples():
    if pd.isna(row.connected):
        are_connected = True
    else:
        are_connected = row.connected
    model.merge_basins(basin_id=row.node_id, to_basin_id=row.to_node_id, are_connected=are_connected)

# %% reverse edges

# reverse edges
for edge_id in reverse_edge_gdf.edge_id:
    model.reverse_edge(edge_id=edge_id)


# %% change node_type

# change node type
selection_df = basin_node_edits_gdf[basin_node_edits_gdf["change_node_type"].notna()]
for row in basin_node_edits_gdf[basin_node_edits_gdf["change_node_type"].notna()].itertuples():
    if row.change_node_type:
        model.update_node(node_id=row.node_id, node_type=row.change_node_type)

# %% remove nodes

# remove nodes
for node_id in basin_node_edits_gdf[basin_node_edits_gdf["remove_node_id"].notna()].node_id:
    model.remove_node(node_id=node_id, remove_edges=True)

# %% add and connect basins

# add and connect basins
for row in connect_basins_gdf.itertuples():
    from_basin_id = row.node_id
    to_basin_id = row.to_node_id
    if row.add_object == "duikersifonhevel":
        node_type = "TabulatedRatingCurve"
    model.add_and_connect_node(
        from_basin_id, int(to_basin_id), geometry=row.geometry, node_type=node_type, name=row.add_object_name
    )

# %% add basin outlet

# add basin outlet
for row in add_basin_outlet_gdf.itertuples():
    basin_id = row.node_id
    if row.add_object == "duikersifonhevel":
        node_type = "TabulatedRatingCurve"
    model.add_basin_outlet(basin_id, geometry=row.geometry, node_type=node_type, name=row.add_object_name)

# %% weggooien basin areas zonder node

df = model.basin.area.df[~model.basin.area.df.index.isin(model.unassigned_basin_area.index)]
df = df.dissolve(by="node_id").reset_index()
df.index.name = "fid"
model.basin.area.df = df


# %% corrigeren knoop-topologie
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.edge_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.edge_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")

model.remove_unassigned_basin_area()

#  %% write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %% Test run model
result = model.run()
assert result == 0
# %%

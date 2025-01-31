# %%

import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table

cloud = CloudStorage()

authority = "AaenMaas"
name = "aam"

# %% Check if model exist, otherwise download
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg")
afwateringseenheden_shp = cloud.joinpath(
    authority,
    "verwerkt",
    "1_ontvangen_data",
    "Na_levering_202404",
    "afwateringseenheden_WAM",
    "Afwateringseenheden.shp",
)
af_aanvoergebied_shp = cloud.joinpath(authority, "aangeleverd", "Eerste_levering", "AfvoergebiedAanvoergebied.shp")
ribasim_areas_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "areas.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")

cloud.synchronize(
    filepaths=[
        ribasim_dir,
        ribasim_areas_gpkg,
        afwateringseenheden_shp,
        hydamo_gpkg,
        af_aanvoergebied_shp,
        model_edits_gpkg,
    ]
)
# %%
model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)

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

# %% https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421617819

# Verwijderen duplicate edges
model.edge.df.drop_duplicates(inplace=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421959240

# Verwijderen edge met 0m lengte
model.remove_node(34, remove_edges=True)
model.update_node(1568, "LevelBoundary", data=[level_data], node_properties={"name": ""})


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421946693
# toevoegen ontbrekende basins

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

# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421991252

# Corrigeren netwerk Den Bosch

# Binnenstad
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[2788, "geometry"].boundary.geoms[0]), tables=basin_data)

model.reverse_edge(edge_id=2077)
model.redirect_edge(edge_id=2077, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2078, from_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2079, from_node_id=basin_node.node_id)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[4999, "geometry"].interpolate(0.5, normalized=True)), tables=[outlet_data]
)
model.edge.add(model.level_boundary[46], outlet_node)
model.edge.add(outlet_node, basin_node)

# Dommel
basin_node = model.basin.add(
    Node(geometry=hydroobject_gdf.at[9055, "geometry"].interpolate(0.5, normalized=True)), tables=basin_data
)
model.redirect_edge(edge_id=2082, from_node_id=basin_node.node_id)

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[9055, "geometry"].interpolate(0.1, normalized=True)), tables=[outlet_data]
)

model.edge.add(model.level_boundary[49], outlet_node)
model.edge.add(outlet_node, basin_node)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422078500

# Corrigeren netwerk bij sluis Empel
for node_id in [729, 730, 1990, 1962]:
    model.remove_node(node_id, remove_edges=True)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2431933060
# Omkeren edgerichting
for edge_id in [131, 398, 407, 495, 513, 515, 894]:
    model.reverse_edge(edge_id=edge_id)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422164355

# Corrigeren netwerk bij Spuisluis Crèvecoeur
model.remove_node(411, remove_edges=True)
model.remove_node(4, remove_edges=True)
model.redirect_edge(edge_id=2018, to_node_id=1950)

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[4825, "geometry"].boundary.geoms[0], name="AKW839"),
    tables=[outlet_data],
)

model.edge.add(outlet_node, model.level_boundary[5])
model.edge.add(model.basin[1627], outlet_node)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422373708

# Corrigeren Afleidingskanaal bij Holthees

# nabij grens
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[8868, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[4680, "geometry"].interpolate(0.1, normalized=True)), tables=[outlet_data]
)

model.redirect_edge(edge_id=2091, from_node_id=56, to_node_id=outlet_node.node_id)
model.redirect_edge(edge_id=2092, from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2093, from_node_id=653, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2094, from_node_id=basin_node.node_id, to_node_id=82)

# nabij afleidingskanaal
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[7466, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[8456, "geometry"].interpolate(0.1, normalized=True)), tables=[outlet_data]
)
model.redirect_edge(edge_id=2089, to_node_id=outlet_node.node_id)
model.redirect_edge(edge_id=2088, from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)
model.edge.add(model.tabulated_rating_curve[82], basin_node)
model.edge.add(basin_node, model.tabulated_rating_curve[853])

# nabij Maas
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[2385, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[2539, "geometry"].boundary.geoms[0]), tables=[outlet_data]
)

model.redirect_edge(edge_id=2054, to_node_id=basin_node.node_id)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[26])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422452167

# Toevoegen basin bij Oude Zuid-Willemsvaart
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[3174, "geometry"].boundary.geoms[0], name="20301"), tables=[outlet_data]
)
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[6499, "geometry"].boundary.geoms[0]), tables=basin_data)
model.redirect_edge(edge_id=2102, to_node_id=outlet_node.node_id)
model.edge.add(outlet_node, basin_node)
model.redirect_edge(edge_id=2106, to_node_id=2026)

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[646, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[66])

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


## UPDATEN STATIC TABLES

# %%
# basin-profielen/state updaten
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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2431933060
node_ids = [280, 335, 373, 879]
model.tabulated_rating_curve.static.df.loc[
    model.tabulated_rating_curve.static.df.node_id.isin([280, 335, 373, 879]), "active"
] = False


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

# %%
# flow boundaries updaten
length = len(model.flow_boundary.node.df)
df = pd.DataFrame(
    {
        "node_id": model.flow_boundary.node.df.index.to_list(),
        "flow_rate": [0.0] * length,
    }
)
df.index.name = "fid"
model.flow_boundary.static.df = df

# %% read
drainage_units_gdf = gpd.read_file(
    afwateringseenheden_shp, fid_as_index=True
)  # Deze afwateringseenheden wordt gebruikt voor vullen gaten
drainage_units_johnny_gdf = gpd.read_file(
    af_aanvoergebied_shp, fid_as_index=True
)  # Indien gaten niet met bovenstaande data kunnen worden gevuld dan proberen met deze
ribasim_areas_gdf = gpd.read_file(ribasim_areas_gpkg, fid_as_index=True, layer="areas")

basin_node_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="unassigned_basin_node")
basin_area_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="unassigned_basin_area")
internal_basin_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="internal_basins")
model.basin.area.df = model.basin.area.df[~model.basin.area.df.index.isin(model.unassigned_basin_area.index)]

df = basin_area_edits_gdf[basin_area_edits_gdf["to_node_id"].notna()]
df.loc[:, ["node_id"]] = df["to_node_id"].astype("int32")
model.basin.area.df = pd.concat([model.basin.area.df, df[["node_id", "geometry"]]])

# %% Assign Ribasim model ID's (dissolved areas) to the model basin areas (original areas with code) by overlapping the Ribasim area file baed on largest overlap
# then assign Ribasim node-ID's to areas with the same area code. Many nodata areas disappear by this method
combined_basin_areas_gdf = gpd.overlay(
    ribasim_areas_gdf, model.basin.area.df, how="union", keep_geom_type=True
).explode()

combined_basin_areas_gdf["geometry"] = combined_basin_areas_gdf["geometry"].apply(lambda x: x if x.has_z else x)
combined_basin_areas_gdf["area"] = combined_basin_areas_gdf.geometry.area
non_null_basin_areas_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()]

largest_area_node_ids = non_null_basin_areas_gdf.loc[
    non_null_basin_areas_gdf.groupby("code")["area"].idxmax(), ["code", "node_id"]
].reset_index(drop=True)

combined_basin_areas_gdf = combined_basin_areas_gdf.merge(largest_area_node_ids, on="code", how="left")
combined_basin_areas_gdf["node_id"] = combined_basin_areas_gdf["node_id_y"]
combined_basin_areas_gdf.drop(columns=["node_id_x", "node_id_y"], inplace=True)
combined_basin_areas_gdf = combined_basin_areas_gdf.drop_duplicates(keep="first")
combined_basin_areas_gdf = combined_basin_areas_gdf.dissolve(by="code").reset_index()

# %% The Ribasim model basins that have still nodata are being checked if they have overlap with aftwareringseenheden.shp.
# If overlap, they get ther Ribasim node-id where they have the largest overlap with
filtered_drainage_units_gdf = drainage_units_johnny_gdf[
    drainage_units_johnny_gdf["SOORTAFVOE"] != "Deelstroomgebied"
].copy()
filtered_drainage_units_gdf["geometry"] = filtered_drainage_units_gdf["geometry"].apply(lambda x: x if x.has_z else x)

filtered_drainage_units_gdf = filtered_drainage_units_gdf.to_crs(combined_basin_areas_gdf.crs)

# Overlay filtered drainage units and updated basin areas
combined_basin_areas_johnny_gdf = gpd.overlay(
    filtered_drainage_units_gdf, combined_basin_areas_gdf, how="union", keep_geom_type=True
).explode()
combined_basin_areas_johnny_gdf = combined_basin_areas_johnny_gdf.dissolve(by="CODE")

# Step 1: Separate unassigned from assigned units
unassigned_units_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].isnull()].copy()
unassigned_units_gdf["geometry"] = unassigned_units_gdf["geometry"].apply(lambda x: x if x.has_z else x)
assigned_units_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()].copy()
assigned_units_gdf["geometry"] = assigned_units_gdf["geometry"].apply(lambda x: x if x.has_z else x)

# Step 2: Calculate intersection areas between unassigned units and Johnny basins
overlap_gdf = gpd.overlay(combined_basin_areas_johnny_gdf, unassigned_units_gdf, how="union", keep_geom_type=True)
# Step 3: Add overlap area for each polygon
overlap_gdf["overlap_area"] = overlap_gdf.geometry.area

# Step 4: Select the largest overlap per code to assign `node_id`
largest_area_node_ids = overlap_gdf.loc[
    overlap_gdf.groupby("OBJECTID_2")["overlap_area"].idxmax(), ["node_id_1", "OBJECTID_2"]
].reset_index(drop=True)

# Step 5: Merge largest node_id from overlaps back to unassigned units
unassigned_units_gdf = unassigned_units_gdf.merge(
    largest_area_node_ids, left_on=["OBJECTID"], right_on=["OBJECTID_2"], how="outer"
)
unassigned_units_gdf["node_id"] = unassigned_units_gdf["node_id_1"]
unassigned_units_gdf.drop(columns=["node_id_1"], inplace=True)

# Step 6: Merge unassigned node_ids back into the main dataset
basin_area_update = combined_basin_areas_gdf.merge(
    unassigned_units_gdf[["OBJECTID", "node_id"]],
    on="OBJECTID",
    how="left",
    suffixes=("", "_unassigned"),
)
# Step 7: Finalize missing `node_id` values from unassigned units
basin_area_update.loc[:, ["node_id"]] = basin_area_update["node_id"].fillna(basin_area_update["node_id_unassigned"])
basin_area_update.drop(columns=["node_id_unassigned"], inplace=True)

# %% If there are still nodata basins they are removed by assigning Nearest Basin ID
null_node_rows = basin_area_update[basin_area_update["node_id"].isnull()]

if not null_node_rows.empty:
    null_node_rows = null_node_rows.set_geometry(null_node_rows.geometry.centroid)
    basin_area_update_centroid = basin_area_update.set_geometry(basin_area_update.geometry.centroid)

    nearest_basin = gpd.sjoin_nearest(
        null_node_rows,
        basin_area_update_centroid[basin_area_update_centroid["node_id"].notna()][["geometry", "node_id"]],
        how="left",
        distance_col="distance",
    )
    basin_area_update.loc[basin_area_update["node_id"].isnull(), "node_id"] = nearest_basin["node_id_right"]
# basin_area_update.to_file("basin_area_update.gpkg", layer="basin_area_update")
# %% Based on basin_node_edits.gpkg, areas are assigned the Ribasim node_id that is in the file
basin_node_edits_notnull_gdf = basin_node_edits_gdf[basin_node_edits_gdf["to_area_code"].notna()]
merged_gdf = basin_area_update.merge(
    basin_node_edits_notnull_gdf[["to_area_code", "node_id"]],
    how="left",
    left_on="code",
    right_on="to_area_code",
)
merged_gdf["node_id"] = merged_gdf["node_id_y"].combine_first(merged_gdf["node_id_x"])
merged_gdf.drop(columns=["node_id_x", "node_id_y"], inplace=True)

# Dissolve geometries by `node_id` and save final GeoDataFrame
final_basins_gdf = merged_gdf.set_index("node_id").dissolve(by="node_id").reset_index()
# final_basins_gdf.to_file("basins_noholes.gpkg", layer="basins_noholes")

final_basins_gdf.index.name = "fid"
model.basin.area.df = final_basins_gdf[["node_id", "geometry"]]

# %% Check Differences in Node_ID Between Initial and Final Models
final_node_ids = final_basins_gdf["node_id"]
model_node_ids = model.basin.area.df["node_id"]
missing_in_model = final_basins_gdf[~final_basins_gdf["node_id"].isin(model_node_ids)]
missing_in_final = model.basin.area.df[~model.basin.area.df["node_id"].isin(final_node_ids)]
missing_gdf = pd.concat([missing_in_model, missing_in_final])

if "fid" in missing_gdf.columns:
    missing_gdf = missing_gdf.rename(columns={"fid": "new_fid_name"})

# %% merge_basins
for row in basin_node_edits_gdf[basin_node_edits_gdf["to_node_id"].notna()].itertuples():
    if pd.isna(row.connected):
        are_connected = True
    else:
        are_connected = row.connected
    model.merge_basins(basin_id=row.node_id, to_basin_id=row.to_node_id, are_connected=are_connected)

mask = internal_basin_edits_gdf["to_node_id"].notna() & internal_basin_edits_gdf["add_object"].isna()
for row in internal_basin_edits_gdf[mask].itertuples():
    if pd.isna(row.connected):
        are_connected = True
    else:
        are_connected = row.connected
    model.merge_basins(basin_id=row.node_id, to_basin_id=row.to_node_id, are_connected=are_connected)

# %% add and connect nodes
for row in internal_basin_edits_gdf[internal_basin_edits_gdf.add_object.notna()].itertuples():
    from_basin_id = row.node_id
    to_basin_id = row.to_node_id
    if row.add_object == "stuw":
        node_type = "TabulatedRatingCurve"
    model.add_and_connect_node(
        from_basin_id, int(to_basin_id), geometry=row.geometry, node_type=node_type, name=row.add_object_name
    )

# %% reverse direction at node
for row in internal_basin_edits_gdf[internal_basin_edits_gdf["reverse_direction"]].itertuples():
    model.reverse_direction_at_node(node_id=row.node_id)

# %% change node_type
for row in basin_node_edits_gdf[basin_node_edits_gdf["change_to_node_type"].notna()].itertuples():
    if row.change_to_node_type:
        model.update_node(row.node_id, row.change_to_node_type, data=[level_boundary.Static(level=[0])])


# %% corrigeren knoop-topologie
outlet_data = outlet.Static(flow_rate=[100])
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.edge_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.edge_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet", data=[outlet_data])

# %% sanitize node-table
# TabulatedRatingCurve to Outlet
for row in model.node_table().df[model.node_table().df.node_type == "TabulatedRatingCurve"].itertuples():
    node_id = row.Index
    model.update_node(node_id=node_id, node_type="Outlet")

# basins and outlets we've added do not have category, we fill with hoofdwater
model.basin.node.df.loc[model.basin.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"
model.outlet.node.df.loc[model.outlet.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# somehow Sluis Engelen (beheerregister AAM) has been named Henriettesluis
model.outlet.node.df.loc[model.outlet.node.df.name == "Henriëttesluis", "name"] = "AKW855"

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(authority=authority, layers=["gemaal", "stuw", "sluis"])
df.set_index("code", inplace=True)
names = df["naam"]

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["LevelBoundary", "FlowBoundary"], "columns": {"meta_name": "name"}},
        {"node_types": ["Basin", "ManningResistance"], "columns": {"name": ""}},
    ],
    names=names,
)


# %%
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model = reset_static_tables(model)
model.use_validation = True
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %% Test run model
result = model.run()
assert result == 0

# %%

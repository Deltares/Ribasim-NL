# %%
import inspect

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "RijnenIJssel"
name = "wrij"
run_model = False
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
ribasim_areas_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/areas.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")

cloud.synchronize(filepaths=[ribasim_dir, ribasim_areas_gpkg, hydamo_gpkg, model_edits_gpkg])
# %% read model
model = Model.read(ribasim_toml)

network_validator = NetworkValidator(model)

ribasim_areas_gdf = gpd.read_file(ribasim_areas_gpkg, fid_as_index=True, layer="areas")
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
duiker_gdf = gpd.read_file(hydamo_gpkg, layer="duikersifonhevel", fid_as_index=True)

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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419605149
# Verwijderen duplicate links

model.link.df.drop_duplicates(inplace=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419620184
# toevoegen ontbrekende basins

basin_links_df = network_validator.link_incorrect_connectivity()
basin_nodes_df = network_validator.node_invalid_connectivity()

for row in basin_nodes_df.itertuples():
    # maak basin-node
    basin_node = model.basin.add(Node(geometry=row.geometry), tables=basin_data)

    # update link_table
    model.link.df.loc[basin_links_df[basin_links_df.from_node_id == row.node_id].index, ["from_node_id"]] = (
        basin_node.node_id
    )
    model.link.df.loc[basin_links_df[basin_links_df.to_node_id == row.node_id].index, ["to_node_id"]] = (
        basin_node.node_id
    )

# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419649171
# update link administratie

model.link.df.loc[516, "from_node_id"] = 666
model.link.df.loc[520, "from_node_id"] = 667
model.link.df.loc[954, "to_node_id"] = 652
model.link.df.loc[1271, "to_node_id"] = 662
model.link.df.loc[1281, "to_node_id"] = 667

# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419747636

# fix link_richting

# verplaatsen van `LevelBoundary` 47 binnen de basin, updaten naar `Basin` en reversen van `Link` 1370
model.move_node(47, hydroobject_gdf.at[8781, "geometry"].boundary.geoms[0])
model.update_node(47, "Basin", data=basin_data)
model.reverse_link(link_id=1370)

# omdraaien richting van `Link` 196
for link_id in [196, 188, 472, 513, 560, 391, 566]:
    model.reverse_link(link_id=link_id)

# opruimen basin Arnhem nabij Lauwersgracht
model.remove_node(514, remove_links=True)
model.remove_node(1101, remove_links=True)
model.remove_links([1364, 1363])

kdu = duiker_gdf.loc[548]
outlet_node = model.outlet.add(
    Node(name=kdu.code, geometry=kdu.geometry.interpolate(0.5, normalized=True), meta_object_type="duikersifonhevel"),
    tables=[outlet_data],
)
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[9528, "geometry"].boundary.geoms[0]))
model.link.add(model.tabulated_rating_curve[265], basin_node)
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[43])
model.link.add(basin_node, model.pump[264])
model.link.add(model.pump[264], model.level_boundary[44])

# %% see https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2422536079

# corrigeren ontbrekende outlets nabij Rijkswateren
for fid, link_id, boundary_node_id in ((14276, 1331, 19), (14259, 1337, 25), (14683, 1339, 27), (3294, 1355, 38)):
    kdu = duiker_gdf.loc[fid]
    outlet_node = model.outlet.add(
        Node(
            name=kdu.code, geometry=kdu.geometry.interpolate(0.5, normalized=True), meta_object_type="duikersifonhevel"
        ),
        tables=[outlet_data],
    )
    model.redirect_link(link_id=link_id, to_node_id=outlet_node.node_id)
    model.link.add(outlet_node, model.level_boundary[boundary_node_id])

# 1349 heeft geen duiker
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[10080, "geometry"].interpolate(0.5, normalized=True)),
    tables=[outlet_data],
)
model.redirect_link(link_id=1349, to_node_id=outlet_node.node_id)
model.link.add(outlet_node, model.level_boundary[33])

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


# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)

# %%
actions = [
    "remove_node",
    "remove_link",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_link",
    "update_node",
    "move_node",
    "connect_basins",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_gpkg).name.to_list()]
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action if "edge" not in action else action.replace("edge", "link"))
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_gpkg, layer=action, fid_as_index=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {
            k.replace("edge", "link"): v for k, v in row._asdict().items() if k.replace("edge", "link") in keywords
        }
        method(**kwargs)

# remove unassigned basin area
model.remove_unassigned_basin_area()


# %% Assign Ribasim model ID's (dissolved areas) to the model basin areas (original areas with code) by overlapping the Ribasim area file baed on largest overlap
# then assign Ribasim node-ID's to areas with the same area code. Many nodata areas disappear by this method
# Create the overlay of areas
ribasim_areas_gdf = ribasim_areas_gdf.to_crs(model.basin.area.df.crs)
combined_basin_areas_gdf = gpd.overlay(ribasim_areas_gdf, model.basin.area.df, how="union").explode()
combined_basin_areas_gdf["geometry"] = combined_basin_areas_gdf["geometry"].apply(lambda x: x if x.has_z else x)

# Calculate area for each geometry
combined_basin_areas_gdf["area"] = combined_basin_areas_gdf.geometry.area

# Separate rows with and without node_id
non_null_basin_areas_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()]

# Find largest area node_ids for each code
largest_area_node_ids = non_null_basin_areas_gdf.loc[
    non_null_basin_areas_gdf.groupby("code")["area"].idxmax(), ["code", "node_id"]
]

# Merge largest area node_ids back into the combined DataFrame
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

# %%
model.basin.area.df = combined_basin_areas_gdf


#  %% write model
# model.use_validation = False
model.fix_unassigned_basin_area()


# %% merge basins
model.merge_basins(basin_id=944, to_basin_id=1028)
model.merge_basins(basin_id=788, to_basin_id=1150)
# %%
# Sanitize node_table
for node_id in model.tabulated_rating_curve.node.df.index:
    model.update_node(node_id=node_id, node_type="Outlet")

# ManningResistance that are duikersifonhevel to outlet
for node_id in model.manning_resistance.node.df[
    model.manning_resistance.node.df["meta_object_type"] == "duikersifonhevel"
].index:
    model.update_node(node_id=node_id, node_type="Outlet")

# basins and outlets we've added do not have category, we fill with hoofdwater
model.basin.node.df.loc[model.basin.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"
model.outlet.node.df.loc[model.outlet.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"
model.pump.node.df.loc[model.pump.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# name-column contains the code we want to keep, meta_name the name we want to have
df = pd.concat(
    [
        gpd.read_file(hydamo_gpkg, layer="sluis"),
        gpd.read_file(hydamo_gpkg, layer="gemaal"),
        gpd.read_file(hydamo_gpkg, layer="stuw"),
    ],
    ignore_index=True,
)
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
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "ManningResistance", "LevelBoundary"], "columns": {"name": ""}},
        {"node_types": ["FlowBoundary"], "columns": {"meta_name": "name"}},
    ],
    names=names,
)

# %%
model.flow_boundary.node.df["meta_categorie"] = "buitenlandse aanvoer"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%
if run_model:
    result = model.run()
    assert result.exit_code == 0

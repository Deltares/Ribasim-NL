# %%
import inspect
from pathlib import Path

import geopandas as gpd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.geometry import drop_z
from ribasim_nl.reset_static_tables import reset_static_tables

cloud = CloudStorage()

authority = "BrabantseDelta"
name = "wbd"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")

ribasim_areas_path = cloud.joinpath(authority, "verwerkt", "4_ribasim", "areas.gpkg")
ribasim_areas_gdf = gpd.read_file(ribasim_areas_path, fid_as_index=True, layer="areas")

# Load node edit data
model_edits_url = cloud.joinurl(authority, "verwerkt", "model_edits.gpkg")
model_edits_path = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")
if not model_edits_path.exists():
    cloud.download_file(model_edits_url)

# %% read model and hydroobject
model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)

hydroobject_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="hydroobject", fid_as_index=True
)

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
# %% https://github.com/Deltares/Ribasim-NL/issues/152#issuecomment-2427492528

# Herstellen verbinding Schelde-Rijnverbinding met KDU02582
model.remove_node(2288, remove_edges=True)
model.redirect_edge(edge_id=2450, to_node_id=955)

# %% https://github.com/Deltares/Ribasim-NL/issues/152#issue-2535747701
# Omkeren edges
edge_ids = [2470, 2468, 2469, 2465, 748, 2476, 2489, 697, 2500, 2487, 2440]

for edge_id in edge_ids:
    model.reverse_edge(edge_id=edge_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/152#issuecomment-2428677846
# Toevoegen Donge
basin_node = model.basin.add(
    Node(geometry=drop_z(hydroobject_gdf.at[13091, "geometry"].boundary.geoms[0])), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=drop_z(hydroobject_gdf.at[13136, "geometry"].boundary.geoms[0])), tables=[outlet_data]
)
model.redirect_edge(edge_id=2477, from_node_id=basin_node.node_id, to_node_id=973)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[31])

outlet_node = model.outlet.add(
    Node(geometry=drop_z(hydroobject_gdf.at[13088, "geometry"].boundary.geoms[0])), tables=[outlet_data]
)
model.redirect_edge(edge_id=2497, to_node_id=outlet_node.node_id)
model.redirect_edge(edge_id=2498, from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)

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

# %%
for action in gpd.list_layers(model_edits_path).name:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_path, layer=action, fid_as_index=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# %%
# remove unassigned basin area
model.remove_unassigned_basin_area()

# %% corrigeren knoop-topologie
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.edge_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.edge_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")

# %% Assign Ribasim model ID's (dissolved areas) to the model basin areas (original areas with code)
# by overlapping the Ribasim area file baed on largest overlap
# then assign Ribasim node-ID's to areas with the same area code.
# Many nodata areas are removed by this method
combined_basin_areas_gdf = gpd.overlay(
    ribasim_areas_gdf, model.basin.area.df, how="union", keep_geom_type=True
).explode()
combined_basin_areas_gdf["geometry"] = combined_basin_areas_gdf["geometry"].apply(lambda x: x if x.has_z else x)
combined_basin_areas_gdf["area"] = combined_basin_areas_gdf.geometry.area
non_null_basin_areas_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()]

# Find largest area node_ids for each code
largest_area_node_ids = non_null_basin_areas_gdf.loc[
    non_null_basin_areas_gdf.groupby("code")["area"].idxmax(), ["code", "node_id"]
]

# Merge largest area node_ids
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

#  %% write model
model.use_validation = True
ribasim_fix_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%#
# Test run model

model = Model.read(ribasim_toml)
model.run(ribasim_exe=Path("c:\\ribasim_dev\\ribasim.exe"))

# %%

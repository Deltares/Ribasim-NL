# %%
import inspect
import os

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.geometry import drop_z
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "BrabantseDelta"
short_name = "wbd"
run_model = False
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg")

ribasim_areas_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "areas.gpkg")
ribasim_areas_bewerkt_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "areas_bewerkt.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")

cloud.synchronize(filepaths=[ribasim_dir, ribasim_areas_gpkg, hydamo_gpkg, model_edits_gpkg])

# %% read model and hydroobject
model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)

hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)


ribasim_areas_gdf = gpd.read_file(ribasim_areas_gpkg, fid_as_index=True, layer="areas")

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
model.redirect_edge(link_id=2450, to_node_id=955)

# %% https://github.com/Deltares/Ribasim-NL/issues/152#issue-2535747701
# Omkeren edges
edge_ids = [2470, 2468, 2469, 2465, 748, 2476, 2489, 697, 2500, 2487, 2440]

for link_id in edge_ids:
    model.reverse_edge(link_id=link_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/152#issuecomment-2428677846
# Toevoegen Donge
basin_node = model.basin.add(
    Node(geometry=drop_z(hydroobject_gdf.at[13091, "geometry"].boundary.geoms[0])), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=drop_z(hydroobject_gdf.at[13136, "geometry"].boundary.geoms[0])), tables=[outlet_data]
)
model.redirect_edge(link_id=2477, from_node_id=basin_node.node_id, to_node_id=973)
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[31])

outlet_node = model.outlet.add(
    Node(geometry=drop_z(hydroobject_gdf.at[13088, "geometry"].boundary.geoms[0])), tables=[outlet_data]
)
model.redirect_edge(link_id=2497, to_node_id=outlet_node.node_id)
model.redirect_edge(link_id=2498, from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)

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


# %%
for action in gpd.list_layers(model_edits_gpkg).name:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_gpkg, layer=action, fid_as_index=True)
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
# by overlapping the Ribasim area file based on largest overlap
# then assign Ribasim node-ID's to areas with the same area code.
# Many nodata areas are removed by this method

if os.path.exists(ribasim_areas_bewerkt_gpkg):
    # Load precomputed result
    combined_basin_areas_gdf = gpd.read_file(ribasim_areas_bewerkt_gpkg)
else:
    # Step 1: Clean geometries
    ribasim_areas_gdf["geometry"] = ribasim_areas_gdf.buffer(-0.01).buffer(0.01)

    # Step 2: Overlay
    combined_basin_areas_gdf = gpd.overlay(
        ribasim_areas_gdf, model.basin.area.df, how="union", keep_geom_type=True
    ).explode(index_parts=False)

    # Step 3: Handle Z-coordinates and calculate area
    combined_basin_areas_gdf["geometry"] = combined_basin_areas_gdf["geometry"].apply(lambda x: x if x.has_z else x)
    combined_basin_areas_gdf["area"] = combined_basin_areas_gdf.geometry.area

    # Step 4: Find and assign node_id
    non_null = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()]
    largest = non_null.loc[non_null.groupby("code")["area"].idxmax(), ["code", "node_id"]]

    combined_basin_areas_gdf = combined_basin_areas_gdf.merge(largest, on="code", how="left", suffixes=("", "_largest"))
    combined_basin_areas_gdf["node_id"] = combined_basin_areas_gdf["node_id"].fillna(
        combined_basin_areas_gdf["node_id_largest"]
    )
    combined_basin_areas_gdf.drop(columns=["node_id_largest"], inplace=True)

    # Step 5: Final processing
    combined_basin_areas_gdf = combined_basin_areas_gdf.drop_duplicates()
    combined_basin_areas_gdf = combined_basin_areas_gdf.dissolve(by="node_id").reset_index()
    combined_basin_areas_gdf = combined_basin_areas_gdf[["node_id", "geometry"]]
    combined_basin_areas_gdf.index.name = "fid"

    # Save for future use
    combined_basin_areas_gdf.to_file(ribasim_areas_bewerkt_gpkg, driver="GPKG")

# Assign to model
model.basin.area.df = combined_basin_areas_gdf
# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)

# Sanitize node_table
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

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(layers=["sluis", "gemaal", "stuw"], authority=authority)
df.set_index("code", inplace=True)
names = df["naam"]

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "ManningResistance"], "columns": {"name": ""}},
        {"node_types": ["FlowBoundary", "LevelBoundary"], "columns": {"meta_name": "name"}},
    ],
    names=names,
)

# label flow-boundaries to buitenlandse-aanvoer
model.flow_boundary.node.df["meta_categorie"] = "buitenlandse aanvoer"


# %%

# init gestuwd voor basins, pumps en outlets
model.basin.node.df["meta_gestuwd"] = False
model.outlet.node.df["meta_gestuwd"] = False
model.pump.node.df["meta_gestuwd"] = True

#
node_ids = (
    model.node_table()
    .df[
        model.node_table().df["meta_code_waterbeheerder"].str.startswith("KST")
        | model.node_table().df["meta_code_waterbeheerder"].str.startswith("GEM_")
    ]
    .index
)

upstream_node_ids = [model.upstream_node_id(i) for i in node_ids]

basin_mask = model.basin.node.df.index.isin(upstream_node_ids)
model.basin.node.df.loc[basin_mask, "meta_gestuwd"] = True

downstream_node_ids = (
    pd.Series([model.downstream_node_id(i) for i in model.basin.node.df[basin_mask].index]).explode().to_numpy()
)
model.outlet.node.df.loc[model.outlet.node.df.index.isin(downstream_node_ids), "meta_gestuwd"] = True


#  %% write model
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{short_name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%#
# Test run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

# %%

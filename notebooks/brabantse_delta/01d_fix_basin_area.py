# %% Import Libraries and Initialize Variables

import inspect

import geopandas as gpd

from ribasim_nl import CloudStorage, Model, NetworkValidator

# Initialize cloud storage and set authority/model parameters
cloud_storage = CloudStorage()
authority_name = "BrabantseDelta"
model_short_name = "wbd"

# Define the path to the Ribasim model configuration file
ribasim_model_dir = cloud_storage.joinpath(authority_name, "modellen", f"{authority_name}_fix_model_network")
ribasim_model_path = ribasim_model_dir / f"{model_short_name}.toml"
model = Model.read(ribasim_model_path)
network_validator = NetworkValidator(model)

# Load node edit data
model_edits_url = cloud_storage.joinurl(authority_name, "verwerkt", "model_edits.gpkg")
model_edits_path = cloud_storage.joinpath(authority_name, "verwerkt", "model_edits.gpkg")
if not model_edits_path.exists():
    cloud_storage.download_file(model_edits_url)

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

# %%
model.use_validation = True
model.write(ribasim_model_dir.with_stem(f"{authority_name}_fix_model_area") / f"{model_short_name}.toml")
model.report_basin_area()
model.report_internal_basins()

# %%

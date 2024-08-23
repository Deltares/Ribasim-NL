# %%

import geopandas as gpd
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_fix", "model.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")


for layer in ["Node", "Basin / area"]:
    df = gpd.read_file(database_gpkg, layer=layer, engine="pyogrio", fid_as_index=True)
    df.set_index("node_id", inplace=True)
    df.index.name = "fid"
    df.to_file(database_gpkg, layer=layer, engine="pyogrio")

model = Model.read(ribasim_toml)

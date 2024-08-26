# %%

import geopandas as gpd
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_fix", "model.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")


for layer in ["Node", "Basin / area"]:
    # read GeoDataFrame and set index
    df = gpd.read_file(database_gpkg, layer=layer, engine="pyogrio", fid_as_index=True)
    df.set_index("node_id", inplace=True)
    df.index.name = "node_id"

    df.to_file(
        database_gpkg,
        layer=layer,
        driver="GPKG",
        index=True,
        fid=df.index.name,
    )

# %%
layer = "Edge"
df = gpd.read_file(database_gpkg, layer=layer, engine="pyogrio", fid_as_index=True)
df.reset_index(drop=True, inplace=True)
df.index += 1
df.index.name = "edge_id"
df.drop(columns=["from_node_type", "to_node_type"], inplace=True)
df.to_file(
    database_gpkg,
    layer=layer,
    driver="GPKG",
    index=True,
    fid=df.index.name,
)

model = Model.read(ribasim_toml)

# %%

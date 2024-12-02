# %% Import Libraries and Initialize Variables

import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model

# Initialize cloud storage and set authority/model parameters
cloud_storage = CloudStorage()
authority_name = "Noorderzijlvest"
model_short_name = "nzv"

# Define the path to the Ribasim model configuration file
ribasim_model_dir = cloud_storage.joinpath(authority_name, "modellen", f"{authority_name}_fix_model_network")
ribasim_model_path = ribasim_model_dir / f"{model_short_name}.toml"
model = Model.read(ribasim_model_path)
node_df = model.node_table().df
# read hydrologische eenheden
he_df = gpd.read_file(
    cloud_storage.joinpath(authority_name, "verwerkt", "1_ontvangen_data", "20241113", "HydrologischeEenheden_v45.shp")
)
he_df.loc[:, "node_id"] = pd.Series()

he_snap_df = gpd.read_file(
    cloud_storage.joinpath(authority_name, "verwerkt", "1_ontvangen_data", "20241113", "HE_v45_snappingpoints.shp")
)


# %% assign basin-id if we can find KWKuit in node_table
def find_basin_id(kwk_code):
    kwk_node_id = node_df[node_df.node_type != "Basin"].reset_index().set_index("name").at[kwk_code, "node_id"]
    basin_node_id = model.upstream_node_id(kwk_node_id)
    return basin_node_id


# only works if names are not duplicated
mask = node_df[node_df["name"].isin(he_df["KWKuit"])].duplicated()
if mask.any():
    raise ValueError(f"kwk codes duplicated in node table {node_df[mask].name.to_list()}")

mask = he_df["KWKuit"].isin(node_df.name)

he_df.loc[mask, "node_id"] = he_df[mask]["KWKuit"].apply(lambda x: find_basin_id(x))

# %% assign basin-id if we can find KWKuit in node_table
point, df = next(
    i
    for i in iter(he_snap_df[he_snap_df.duplicated("geometry", keep=False)].groupby("geometry"))
    if i[1].Kunstwerk.notna().any()
)


# %% dissolve geometry and take min summer target level as streefpeil
data = []
for node_id, df in he_df[he_df["node_id"].notna()].groupby("node_id"):
    geometry = df.union_all()
    streefpeil = df["OPVAFWZP"].min()

    data += [{"node_id": node_id, "meta_streefpeil": streefpeil, "geometry": geometry}]

# %%
# add new basins to model
df = gpd.GeoDataFrame(data, crs=model.crs)
df.loc[:, "geometry"] = df.buffer(0.1).buffer(-0.1)
df.index.name = "fid"
model.basin.area.df = df

# %%
model.write(ribasim_model_dir.with_stem(f"{authority_name}_fix_model_area") / f"{model_short_name}.toml")
model.report_basin_area()
# %%

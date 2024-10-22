# %%
import geopandas as gpd

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

authority = "AaenMaas"
short_name = "aam"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_network", f"{short_name}.toml")
model = Model.read(ribasim_toml)

afwateringseenheden_gdf = gpd.read_file(
    cloud.joinpath(
        authority,
        "verwerkt",
        "1_ontvangen_data",
        "Na_levering_202404",
        "afwateringseenheden_WAM",
        "Afwateringseenheden.shp",
    ),
    fid_as_index=True,
)

# %%

# Perform the overlay (including polygons with node_id = null)
area_df = gpd.overlay(model.basin.area.df, afwateringseenheden_gdf, how="union")
area_df.to_file("overlay.gpkg", layer="overlay")

# Calculate the area of each polygon and store it in a new column
area_df["area"] = area_df.geometry.area


# Custom function to select the 'node_id' from the largest area within each group
def select_largest_area(group):
    # Find the index of the row with the largest area
    largest_area_idx = group["area"].idxmax()
    # Return the 'node_id' of the polygon with the largest area
    return group.loc[largest_area_idx, "node_id"]


# Group the data by 'Id' and select the 'node_id' of the largest area for each group
largest_node_ids = area_df.groupby("Id").apply(select_largest_area).reset_index(name="node_id")

# Now dissolve the polygons by 'Id'
area_df = area_df.dissolve(by="Id")

# Merge the dissolved polygons with the node_id from the largest area
area_df = area_df.merge(largest_node_ids, on="Id")

# Save the dissolved result with the correct 'node_id'
area_df.to_file("overlay.gpkg", layer="dissolve_id")

# %%

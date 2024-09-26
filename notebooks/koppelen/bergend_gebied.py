# %%
import geopandas as gpd
from ribasim_nl import CloudStorage, Model
from ribasim_nl.geodataframe import split_basins

cloud = CloudStorage()

# %%

# %% RWS-HWS
model_path = cloud.joinpath("Rijkswaterstaat", "modellen", "hws")
toml_file = model_path / "hws.toml"
rws_model = Model.read(toml_file)

# %% DeDommel
model_path = cloud.joinpath("DeDommel", "modellen", "DeDommel")
toml_file = model_path / "model.toml"
model = Model.read(toml_file)
basin_polygon = model.basin.area.df[model.basin.area.df.node_id != 1228].union_all()

drainage_area = gpd.read_file(
    cloud.joinpath("DeDommel", "verwerkt", "4_ribasim", "areas.gpkg"), layer="drainage_areas"
).union_all()


rws_selected_areas_df = rws_model.basin.area.df[rws_model.basin.area.df.intersects(drainage_area.buffer(-10))]
rws_selected_areas = rws_selected_areas_df.union_all()

poly = (
    rws_model.basin.area.df[rws_model.basin.area.df.intersects(drainage_area.buffer(-10))]
    .buffer(0.1)
    .union_all()
    .buffer(3000)
)
poly = poly.difference(basin_polygon).intersection(drainage_area)

berging_basins_df = gpd.GeoDataFrame(geometry=gpd.GeoSeries(poly.geoms, crs=28992))

berging_basins_df = berging_basins_df[berging_basins_df.geom_type == "Polygon"]
berging_basins_df = berging_basins_df[berging_basins_df.intersects(rws_selected_areas)]
berging_basins_df = berging_basins_df[berging_basins_df.area > 50]

cut_lines_df = gpd.read_file(cloud.joinpath("Rijkswaterstaat", "verwerkt", "couple_user_data.gpkg"), layer="cut_lines")

berging_basins_df = split_basins(berging_basins_df, cut_lines_df)
berging_basins_df = berging_basins_df[berging_basins_df.intersects(rws_selected_areas)]


rws_selected_basins_df = rws_model.basin.node.df[rws_model.basin.node.df.index.isin(rws_selected_areas_df.node_id)]

berging_basins_df.loc[:, "node_id"] = berging_basins_df.geometry.apply(
    lambda x: rws_selected_basins_df.distance(x).idxmin()
)

berging_basins_df.to_file(cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergende_basins_rws.gpkg"))


# %%

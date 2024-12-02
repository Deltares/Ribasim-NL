# %% Check for basin-holes
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon

from ribasim_nl import CloudStorage, Model

# Initialize cloud storage and set authority/model parameters
cloud = CloudStorage()
authority = "RijnenIJssel"
short_name = "wrij"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_area", f"{short_name}.toml")
model = Model.read(ribasim_toml)

afwateringseenheden_df = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="afvoergebiedaanvoergebied"
)
afwateringseenheden_poly = afwateringseenheden_df.buffer(0.01).buffer(-0.01).union_all()
waterschap_poly = MultiPolygon([Polygon(i.exterior) for i in afwateringseenheden_poly.geoms])

basin_polygon = model.basin.area.df.union_all()
holes_poly = waterschap_poly.difference(basin_polygon)
holes_df = gpd.GeoSeries(holes_poly.geoms, crs=model.basin.area.df.crs)
holes_df.to_file(cloud.joinpath(authority, "verwerkt", "basin_gaten.gpkg"))

# %%

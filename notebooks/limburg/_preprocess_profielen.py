# %%
import geopandas as gpd
from shapely.geometry import LineString

from ribasim_nl import CloudStorage

cloud = CloudStorage()
authority = "Limburg"

hydamo_gpkg = cloud.joinpath(
    authority, "verwerkt/1_ontvangen_data/HyDAMO_2_2_Limburg_met_wasmachine_levering20230406.gpkg"
)

profielpunt_df = gpd.read_file(hydamo_gpkg, layer="Profielpunt")
dwarsprofielen_gpkg = cloud.joinpath(authority, "verwerkt/profielen.gpkg")


data = []
for profiellijnid, df in profielpunt_df.groupby("profiellijnid"):
    data += [
        {
            "globalid": profiellijnid,
            "geometry": LineString(df.sort_values("codevolgnummer")["geometry"].iloc[[0, -1]].to_numpy()),
        }
    ]
gpd.GeoDataFrame(data, crs=profielpunt_df.crs).to_file(dwarsprofielen_gpkg, layer="profiellijn")
profielpunt_df.to_file(dwarsprofielen_gpkg, layer="profielpunt")

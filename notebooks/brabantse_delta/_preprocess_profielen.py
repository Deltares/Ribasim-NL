# %%
import geopandas as gpd
from shapely.geometry import LineString

from ribasim_nl import CloudStorage

cloud = CloudStorage()
authority = "BrabantseDelta"

dwarsprofielen_gml = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/GML/dwarsprofiel.gml")
dwarsprofielen_gpkg = cloud.joinpath(authority, "verwerkt/profielen.gpkg")

cloud.synchronize([dwarsprofielen_gml])

dwarsprofielen_df = gpd.read_file(dwarsprofielen_gml)

data = []
for profiellijnid, df in dwarsprofielen_df.groupby("profielcode"):
    data += [
        {
            "code": profiellijnid,
            "geometry": LineString(df.sort_values("codevolgnummer")["geometry"].iloc[[0, -1]].to_numpy()),
        }
    ]
gpd.GeoDataFrame(data, crs=dwarsprofielen_df.crs).to_file(dwarsprofielen_gpkg, layer="profiellijn")
dwarsprofielen_df.rename(columns={"profielcode": "profiellijnid"}, inplace=True)
dwarsprofielen_df.to_file(dwarsprofielen_gpkg, layer="profielpunt")

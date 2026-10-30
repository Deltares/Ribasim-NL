# %%
import geopandas as gpd
from ribasim_nl.parametrization.damo_profiles import profile_lines_from_points

from ribasim_nl import CloudStorage

cloud = CloudStorage()
authority = "BrabantseDelta"

dwarsprofielen_gml = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/GML/dwarsprofiel.gml")
dwarsprofielen_gpkg = cloud.joinpath(authority, "verwerkt/profielen.gpkg")

dwarsprofielen_df = gpd.read_file(dwarsprofielen_gml)

profile_lines_from_points(dwarsprofielen_df, profile_id_col="profielcode", line_id_col="code").to_file(
    dwarsprofielen_gpkg, layer="profiellijn"
)
dwarsprofielen_df.rename(columns={"profielcode": "profiellijnid"}, inplace=True)
dwarsprofielen_df.to_file(dwarsprofielen_gpkg, layer="profielpunt")

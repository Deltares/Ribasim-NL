# %%
import geopandas as gpd
from ribasim_nl.parametrization.damo_profiles import profile_lines_from_points

from ribasim_nl import CloudStorage

cloud = CloudStorage()
authority = "Limburg"

hydamo_gpkg = cloud.joinpath(
    authority, "verwerkt/1_ontvangen_data/HyDAMO_2_2_Limburg_met_wasmachine_levering20230406.gpkg"
)
dwarsprofielen_gpkg = cloud.joinpath(authority, "verwerkt/profielen.gpkg")

profielpunt_df = gpd.read_file(hydamo_gpkg, layer="Profielpunt")

profile_lines_from_points(profielpunt_df, profile_id_col="profiellijnid", line_id_col="globalid").to_file(
    dwarsprofielen_gpkg, layer="profiellijn"
)
profielpunt_df.to_file(dwarsprofielen_gpkg, layer="profielpunt")

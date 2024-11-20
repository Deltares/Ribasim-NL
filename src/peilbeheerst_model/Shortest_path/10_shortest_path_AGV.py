# The AGV shortest path code is identical to the other water boards.
# Below the content of shortest_path_waterschappen is spelled out to be able to
# point to the right data from the cloud storage.

import fiona
import geopandas as gpd
import pandas as pd
from shapely.wkt import dumps

from peilbeheerst_model.shortest_path import shortest_path
from ribasim_nl import CloudStorage

waterschap = "AmstelGooienVecht"

cloud = CloudStorage()
# cloud.download_verwerkt(waterschap)
# cloud.download_verwerkt("Rijkswaterstaat")
# cloud.download_basisgegevens()

# %%
verwerkt_dir = cloud.joinpath(waterschap, "verwerkt")

# Load Data
# Define crossings file path
data_path = verwerkt_dir / "crossings.gpkg"

# Load crossings file
DATA = {L: gpd.read_file(data_path, layer=L) for L in fiona.listlayers(data_path)}

# Select rhws

# Select RHWS peilgebeied & calculate representative point
gdf_rhws = DATA["peilgebied"].loc[DATA["peilgebied"]["peilgebied_cat"] == 1].copy()
gdf_rhws["representative_point"] = gdf_rhws.representative_point()

# Apply aggregation level based filter
gdf_cross = (
    DATA["crossings_hydroobject_filtered"].loc[DATA["crossings_hydroobject_filtered"]["agg_links_in_use"]].copy()
)  # filter aggregation level

gdf_crossings_out = shortest_path(waterschap, DATA, gdf_cross, gdf_rhws)
# Write final output
gdf_out = gpd.GeoDataFrame(pd.concat(gdf_crossings_out))
gdf_out["shortest_path"] = gdf_out["shortest_path"].apply(lambda geom: dumps(geom) if geom is not None else None)
gdf_out.to_file(verwerkt_dir / "shortest_path.gpkg", driver="GPKG")

# cloud.upload_verwerkt(waterschap)

# %%

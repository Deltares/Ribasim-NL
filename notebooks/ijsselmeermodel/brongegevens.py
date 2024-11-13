import os
from pathlib import Path

import geopandas as gpd

DATA_DIR = os.getenv("RIBASIM_NL_DATA_DIR")

# file-paths
kunstwerken_gpkg = Path(DATA_DIR) / "nl_kunstwerken.gpkg"

kunstwerken_gdf = gpd.read_file(kunstwerken_gpkg)

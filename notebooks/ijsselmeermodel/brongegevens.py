from pathlib import Path

import geopandas as gpd

from ribasim_nl import settings

DATA_DIR = settings.ribasim_nl_data_dir

# file-paths
kunstwerken_gpkg = Path(DATA_DIR) / "nl_kunstwerken.gpkg"

kunstwerken_gdf = gpd.read_file(kunstwerken_gpkg)

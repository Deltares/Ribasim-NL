# %%
import os
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"


data = [
    {
        "user_id": "NL.WBHCODE.80.basin.WADDENZEE",
        "level": 0.0,
        "geometry": Point(136402.663, 563546.430),
    }
]


gpd.GeoDataFrame(data, crs=28992).to_file(
    MODEL_DIR / "model_data.gpkg", layer="level_boundary"
)
# %%

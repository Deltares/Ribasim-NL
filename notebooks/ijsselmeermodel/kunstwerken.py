# %%
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from hydamo import code_utils

DATA_DIR = Path(os.getenv("RIBASIM_NL_DATA_DIR"))
MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"
EXCEL_FILE = "uitlaten_inlaten.xlsx"
BGT_CODES = ["W0650", "P0024"]

KUNSTWERKEN_XLSX = Path(DATA_DIR) / EXCEL_FILE
MODEL_DATA_GPKG = Path(MODEL_DIR) / "model_data.gpkg"

basin_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="basin")

kunstwerken_df = pd.read_excel(KUNSTWERKEN_XLSX)
kunstwerken_df = kunstwerken_df.loc[kunstwerken_df.bgt_code.isin(BGT_CODES)]
kunstwerken_gdf = gpd.GeoDataFrame(
    kunstwerken_df,
    geometry=gpd.points_from_xy(x=kunstwerken_df.x, y=kunstwerken_df.y),
    crs=28992,
)

# %%
pump_gdf = kunstwerken_gdf[kunstwerken_gdf.dm_type == "uitlaat"][
    ["dm_capaciteit", "user_id", "peilvak", "rijkswater", "geometry"]
].copy()
pump_gdf.rename(
    columns={"dm_capaciteit": "flow_rate", "peilvak": "id_from", "rijkswater": "id_to"},
    inplace=True,
)
pump_gdf["id_from"] = pump_gdf["id_from"].apply(
    lambda x: code_utils.generate_model_id(code=x, layer="basin", bgt_code="W0650")
)
pump_gdf[pump_gdf.flow_rate.isna()]["flow_rate"] = 0
pump_gdf.to_file(MODEL_DIR / "model_data.gpkg", layer="pump")

# %%
outlet_gdf = (
    kunstwerken_gdf[kunstwerken_gdf.dm_type == "inlaat"][
        ["dm_capaciteit", "user_id", "peilvak", "rijkswater", "geometry"]
    ]
    .copy()
    .rename(columns={"dm_capaciteit": "flow_rate"})
)
outlet_gdf.rename(
    columns={"dm_capaciteit": "flow_rate", "peilvak": "id_to", "rijkswater": "id_from"},
    inplace=True,
)
outlet_gdf["id_to"] = outlet_gdf["id_to"].apply(
    lambda x: code_utils.generate_model_id(code=x, layer="basin", bgt_code="W0650")
)
outlet_gdf[outlet_gdf.flow_rate.isna()]["flow_rate"] = 0
outlet_gdf.to_file(MODEL_DIR / "model_data.gpkg", layer="outlet")

# %%

resistance_gdf = gpd.read_file(
    DATA_DIR.joinpath("ijsselmeergebied", "hydamo.gpkg"), layer="sluis"
)

resistance_gdf.rename(
    columns={"rijkswater_naar": "id_to", "rijkswater_van": "id_from"}, inplace=True
)

resistance_gdf["user_id"] = resistance_gdf["code"].apply(
    lambda x: code_utils.generate_model_id(code=x, layer="sluis", bgt_code="L0002")
)
resistance_gdf["resistance"] = 1000

resistance_gdf.to_file(MODEL_DIR / "model_data.gpkg", layer="resistance")
# %%

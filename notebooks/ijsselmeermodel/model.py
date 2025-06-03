# %%
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
import ribasim

from ribasim_nl import settings

DATA_DIR = settings.ribasim_nl_data_dir
MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"

MODEL_DATA_GPKG = Path(MODEL_DIR) / "model_data.gpkg"
PRECIPITATION = 0.005 / 86400  # m/s
EVAPORATION = 0.001 / 86400  # m/s
LEVEL = [0.0, 1.0]
AREA = [0.01, 1000.0]

# %%

basin_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="basin")
pump_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="pump")
outlet_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="outlet")
resistance_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="resistance")
level_boundary_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="level_boundary")

edge_gdf = gpd.read_file(MODEL_DATA_GPKG, layer="edge")

basin_gdf["type"] = "Basin"
pump_gdf["type"] = "Pump"
outlet_gdf["type"] = "Outlet"
resistance_gdf["type"] = "LinearResistance"
level_boundary_gdf["type"] = "LevelBoundary"

# %%
node_gdf = pd.concat(
    [basin_gdf, pump_gdf, outlet_gdf, resistance_gdf, level_boundary_gdf],
    ignore_index=True,
)

if node_gdf.user_id.isna().any():
    raise ValueError("missing user_id's in 'basin', 'pump', 'outlet', 'resistance' or 'level_boundary' layer")

if node_gdf.user_id.duplicated().any():
    raise ValueError(f"duplicated user_id's: {list(node_gdf[node_gdf.user_id.duplicated()].user_id.unique())}")

node_gdf.node_id = node_gdf.index + 1
node_gdf.set_index("user_id", drop=False, inplace=True)

edge_gdf["from_node_id"] = edge_gdf["user_id_from"].apply(lambda x: node_gdf.loc[x]["node_id"])
edge_gdf["to_node_id"] = edge_gdf["user_id_to"].apply(lambda x: node_gdf.loc[x]["node_id"])
edge_gdf["edge_type"] = "flow"
# %% add basins

basin_df = pd.DataFrame(basin_gdf["user_id"])
basin_df["node_id"] = basin_df.user_id.apply(lambda x: node_gdf.loc[x].node_id)
basin_df.rename(columns={"user_id": "remarks"}, inplace=True)
basin_static_df = basin_df.copy()
basin_static_df["drainage"] = 0.0
basin_static_df["potential_evaporation"] = EVAPORATION
basin_static_df["infiltration"] = 0.0
basin_static_df["precipitation"] = PRECIPITATION
basin_static_df["urban_runoff"] = 0.0

basin_profile_df = pd.DataFrame(
    data=[[node, area, level] for node in basin_df["node_id"] for area in AREA for level in LEVEL],
    columns=["node_id", "area", "level"],
)

basin = ribasim.Basin(profile=basin_profile_df, static=basin_static_df)

# %%
pump_df = pump_gdf[["user_id", "flow_rate"]]
pump_df["node_id"] = pump_df.user_id.apply(lambda x: node_gdf.loc[x].node_id)
pump_df.rename(columns={"user_id": "remarks"}, inplace=True)
pump = ribasim.Pump(static=pump_df)
# %%
outlet_df = outlet_gdf[["user_id", "flow_rate"]]
outlet_df["node_id"] = outlet_df.user_id.apply(lambda x: node_gdf.loc[x].node_id)
outlet_df.rename(columns={"user_id": "remarks"}, inplace=True)
outlet_df.loc[outlet_df.flow_rate.isna(), ["flow_rate"]] = 0
outlet = ribasim.Outlet(static=outlet_df)

# %%
resistance_df = resistance_gdf[["user_id", "resistance"]]

resistance_df["node_id"] = resistance_df.user_id.apply(lambda x: node_gdf.loc[x].node_id)
resistance_df.rename(columns={"user_id": "remarks"}, inplace=True)

linear_resistance = ribasim.LinearResistance(static=resistance_df)

# %%
level_boundary_df = level_boundary_gdf[["user_id", "level"]]
level_boundary_df["node_id"] = level_boundary_df.user_id.apply(lambda x: node_gdf.loc[x].node_id)
level_boundary_df.rename(columns={"user_id": "remarks"}, inplace=True)
level_boundary = ribasim.LevelBoundary(static=level_boundary_df)

# %%
node_gdf.rename(columns={"user_id": "remarks"}, inplace=True)
node_gdf.set_index("node_id", inplace=True, drop=False)
node_gdf.index.name = "fid"

node = ribasim.Node(static=node_gdf)

# %%
edge = ribasim.Edge(static=edge_gdf)

# %%
model = ribasim.Model(
    network=ribasim.Network(
        node=node,
        edge=edge,
    ),
    basin=basin,
    outlet=outlet,
    level_boundary=level_boundary,
    pump=pump,
    linear_resistance=linear_resistance,
    starttime="2020-01-01 00:00:00",
    endtime="2021-01-01 00:00:00",
)
# %%
ribasim_model_dir = MODEL_DIR / "ijsselmeermodel"
model.write(ribasim_model_dir)
# %%
# environmnt variables

RIBASIM_NL_CLOUD_PASS = os.getenv("RIBASIM_NL_CLOUD_PASS")
assert RIBASIM_NL_CLOUD_PASS is not None

RIBASIM_NL_CLOUD_USER = "nhi_api"
WEBDAV_URL = "https://deltares.thegood.cloud/remote.php/dav"
BASE_URL = f"{WEBDAV_URL}/files/{RIBASIM_NL_CLOUD_USER}/D-HYDRO modeldata"


def upload_file(url, path):
    with open(path, "rb") as f:
        r = requests.put(url, data=f, auth=(RIBASIM_NL_CLOUD_USER, RIBASIM_NL_CLOUD_PASS))
    r.raise_for_status()


for file in ribasim_model_dir.glob("*.*"):
    to_url = f"{BASE_URL}/HyDAMO_geconstrueerd/{file.name}"
    upload_file(to_url, file)

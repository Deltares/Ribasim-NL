import os
import warnings
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
from ribasim_lumping_tools.default_model import DEFAULTS, default_model

warnings.simplefilter(action="ignore", category=UserWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)


# #### Read Ribasim model


base_dir = Path("..\\Ribasim modeldata\\")


waterschappen = {
    "AaenMaas": "NL38",
    "BrabantseDelta": "NL25",
    "DeDommel": "NL27",
    "DrentsOverijsselseDelta": "NL59",
    "HunzeenAas": "NL33",
    "Limburg": "NL60",
    "Noorderzijlvest": "NL34",
    "RijnenIJssel": "NL7",
    "StichtseRijnlanden": "NL14",
    "ValleienVeluwe": "NL8",
    "Vechtstromen": "NL44",
}


new_model_dir_string = "..\\modellen\\WATERBOARD\\modellen\\WATERBOARD_2024_6_3"

for waterschap, waterschap_code in waterschappen.items():
    print(waterschap)
    new_model_dir = Path(new_model_dir_string.replace("WATERBOARD", waterschap))
    print(new_model_dir)

    if not new_model_dir.exists():
        os.makedirs(new_model_dir)

    # gpkg
    old_ribasim_model_gpkg = Path(base_dir, waterschap, "verwerkt", "4_ribasim", "ribasim_model.gpkg")
    old_krw_gpkg = Path(base_dir, waterschap, "verwerkt", "4_ribasim", "krw.gpkg")

    # read nodes
    node_df = gpd.read_file(old_ribasim_model_gpkg, layer="Node", engine="pyogrio", fid_as_index=True)
    node_df = node_df.rename(columns={"type": "node_type"})
    node_df["meta_code"] = waterschap_code

    # read links
    link_df = gpd.read_file(old_ribasim_model_gpkg, layer="Link", engine="pyogrio", fid_as_index=True)

    # read basin areas
    basin_areas = gpd.read_file(
        str(old_ribasim_model_gpkg).replace("ribasim_model.gpkg", "ribasim_network.gpkg"), layer="basin_areas"
    )
    basin_areas = basin_areas[["basin_node_id", "geometry"]].rename(columns={"basin_node_id": "node_id"})
    basin_areas.node_id = basin_areas.node_id.astype(int)

    # read krw
    krw = gpd.GeoDataFrame()
    krw_layers = fiona.listlayers(str(old_krw_gpkg))
    if "krw_line" in krw_layers:
        krw_line = gpd.read_file(str(old_krw_gpkg), layer="krw_line").explode(index_parts=True)
        krw_line.geometry = krw_line.geometry.buffer(10, join_style="bevel")
        krw = pd.concat([krw, krw_line])
    if "krw_vlak" in krw_layers:
        krw_vlak = gpd.read_file(str(old_krw_gpkg), layer="krw_vlak").explode(index_parts=True)
        krw = pd.concat([krw, krw_vlak])
    krw = krw[["owmident", "owmnaam", "owmtype", "geometry"]].reset_index(drop=True)
    krw.columns = ["meta_krw_id", "meta_krw_name", "meta_krw_type", "geometry"]

    node_df = (
        node_df.sjoin(krw, how="left").drop(columns=["index_right"]).drop_duplicates(subset="node_id", keep="first")
    )
    node_df["meta_categorie"] = "doorgaand"
    node_df.loc[~node_df.meta_krw_id.isna(), "meta_categorie"] = "hoofdwater"

    # create default model
    model = default_model(node_df, link_df, basin_areas, **DEFAULTS)

    # write model to disk
    ribasim_toml = Path(new_model_dir, "model.toml")
    model.write(str(ribasim_toml))

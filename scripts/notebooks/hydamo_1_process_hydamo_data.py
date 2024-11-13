# Verwerk wijzigingen aan HyDAMO geopackages in nieuwe HyDAMO input geopackage
#
# Contactpersoon:     Harm Nomden (Sweco)
#
# Laatste update:     15-03-2024


import warnings
from pathlib import Path

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
from hydamo_preprocessing.preprocessing import preprocess_hydamo_hydroobjects

warnings.simplefilter(action="ignore", category=UserWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)


def process_hydamo_changes(
    dir_waterschap, dir_hydamo_preprocess, dir_hydamo_changes, dir_hydamo_processed, sel_layers=None
):
    # process hydamo changes (toevoegen en verwijderen) to new hydamo geopackage
    path_hydamo_gpkg_preprocess = Path(dir_hydamo_preprocess, "hydamo.gpkg")
    path_hydamo_gpkg_processed = Path(dir_hydamo_processed, "hydamo.gpkg")
    path_hydamo_gpkg_remove = Path(dir_hydamo_changes, "hydamo_verwijderen.gpkg")
    path_hydamo_gpkg_add = Path(dir_hydamo_changes, "hydamo_toevoegen.gpkg")

    if sel_layers is None or sel_layers == []:
        sel_layers = fiona.listlayers(path_hydamo_gpkg_preprocess)
    print(sel_layers)
    for layer in sel_layers:
        if layer == "layer_styles":
            continue
        print(f" - {layer}")
        # read original hydamo gpkg (from specified region)
        gdf = gpd.read_file(str(path_hydamo_gpkg_preprocess), layer=layer, crs=28992)

        # remove objects
        if layer in fiona.listlayers(path_hydamo_gpkg_remove):
            gdf_remove = gpd.read_file(path_hydamo_gpkg_remove, layer=layer, crs=28992)
            try:
                gdf = gdf.loc[~np.isin(gdf["code"], gdf_remove["code"])]
            except KeyError:
                gdf = gdf.loc[~np.isin(gdf["globalid"], gdf_remove["globalid"])]
        # add new objects
        if layer in fiona.listlayers(path_hydamo_gpkg_add):
            gdf_add = gpd.read_file(path_hydamo_gpkg_add, layer=layer, crs=28992)
            gdf_add = gdf_add.to_crs(28992)
            gdf = gdf.to_crs(28992)
            gdf = gpd.GeoDataFrame(pd.concat([gdf, gdf_add])).reset_index()

        # save to new hydamo gpkg
        layer_options = "ASPATIAL_VARIANT=GPKG_ATTRIBUTES"
        if gdf.geometry.isnull().all():
            gdf.to_file(str(path_hydamo_gpkg_processed), layer=layer, driver="GPKG", layer_options=layer_options)
        else:
            gdf.to_file(str(path_hydamo_gpkg_processed), layer=layer, driver="GPKG")


main_dir = "..\\Ribasim modeldata"


waterschappen = [
    # "AaenMaas",
    # "BrabantseDelta",
    "DeDommel",
    # "DrentsOverijsselseDelta",
    # "HunzeenAas",
    # "Limburg",
    # "RijnenIJssel",
    # "StichtseRijnlanden",
    # "ValleienVeluwe",
    # "Vechtstromen"
]


# optional: preprocess the hydro-objects (check and adapt endpoints)


preprocess_hydroobjects = False


if preprocess_hydroobjects:
    for waterschap in waterschappen:
        dir_waterschap = main_dir / waterschap / "verwerkt"
        dir_hydamo_preprocess = dir_waterschap / "2_voorbewerking"
        dir_hydamo_processed = dir_waterschap / "4_ribasim"

        hydroobjects = gpd.read_file(Path(dir_hydamo_preprocess, "hydamo.gpkg"), layer="hydroobject")
        wfd_lines = gpd.read_file(Path(dir_hydamo_processed, "krw.gpkg"), layer="krw_line")
        wfd_polygons = gpd.read_file(Path(dir_hydamo_processed, "krw.gpkg"), layer="krw_polygon")

        hydroobject_new = preprocess_hydamo_hydroobjects(
            hydroobjects,
            wfd_lines=wfd_lines,
            wfd_polygons=wfd_polygons,
            buffer_distance_endpoints=0.5,
            wfd_id_column="owmident",
            buffer_distance_wfd=10,
            overlap_ratio_wfd=0.9,
        )

        hydroobject_new.to_file(Path(dir_hydamo_preprocess, "hydamo.gpkg"), layer="hydroobject", driver="GPKG")


sel_layers = [
    "hydroobject",
    # 'stuw',
    # 'gemaal',
    # 'afvoergebiedaanvoergebied',
    # 'pomp',
    # 'peilgebiedvigerend',
    # 'peilgebiedpraktijk',
    # 'streefpeil',
    # 'duikersifonhevel',
    # 'afsluiter',
    # 'sluis',
]


for waterschap in waterschappen:
    print(f"Waterschap {waterschap}")
    dir_waterschap = Path(main_dir, waterschap, "verwerkt")
    dir_hydamo_preprocess = Path(dir_waterschap, "2_voorbewerking")
    dir_hydamo_changes = Path(dir_waterschap, "3_input")
    dir_hydamo_processed = Path(dir_waterschap, "4_ribasim")

    process_hydamo_changes(
        dir_waterschap=dir_waterschap,
        dir_hydamo_preprocess=dir_hydamo_preprocess,
        dir_hydamo_changes=dir_hydamo_changes,
        dir_hydamo_processed=dir_hydamo_processed,
        sel_layers=sel_layers,
    )

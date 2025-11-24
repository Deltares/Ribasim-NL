# %%
import shutil
from pathlib import Path

import geopandas as gpd

from ribasim_nl import CloudStorage

cloud = CloudStorage()


SELECTION: list[str] = []


def rename_link_to_edge(gpkg: Path, replace_from: str = "edge", replace_to: str = "link", dry_run: bool = True):
    # create backup
    gpkg_bkp = gpkg.with_name(f"{gpkg.stem}_backup{gpkg.suffix}")
    gpkg_tmp = gpkg.with_name(f"{gpkg.stem}_tmp{gpkg.suffix}")

    shutil.copy2(gpkg, gpkg_bkp)

    layers = gpd.list_layers(gpkg).name

    for layer in layers:
        print(f"checking : {layer}")

        # Laag inlezen
        gdf = gpd.read_file(gpkg, layer=layer)

        # modify columns
        new_columns = {col: col.replace(replace_from, replace_to) for col in gdf.columns}
        if list(new_columns.values()) != gdf.columns.to_list():
            print(f"renaming columns to:{new_columns}")
            gdf = gdf.rename(columns=new_columns)

        # modify layer-name
        new_layer_name = layer.replace(replace_from, replace_to)
        if new_layer_name != layer:
            print(f"saving to layer: {new_layer_name}")

        # Schrijf naar nieuwe GeoPackage
        gdf.to_file(
            gpkg_tmp,
            layer=new_layer_name,
            driver="GPKG",
        )

    # copy temp to gpkg and drop temp gpkg
    if not dry_run:
        shutil.copy(gpkg_tmp, gpkg)
        gpkg_tmp.unlink()


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


if len(SELECTION) == 0:
    authorities = cloud.water_authorities
else:
    authorities = SELECTION

for authority in authorities:
    model_edits_gpkg = cloud.joinpath(f"{authority}/verwerkt/model_edits.gpkg")
    if model_edits_gpkg.exists():
        print(authority)
        rename_link_to_edge(gpkg=model_edits_gpkg, dry_run=False)
        print("uploading")
        cloud.upload_file(model_edits_gpkg)

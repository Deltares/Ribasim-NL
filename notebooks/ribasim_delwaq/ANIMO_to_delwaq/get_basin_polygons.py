# %%
# ruff: noqa: E402

import sys
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.toml"
ROOT = CONFIG_PATH.resolve().parents[2]
sys.path.insert(0, str(CONFIG_PATH.parent))
import geopandas as gpd
from config import load_delwaq_config

# %% CONFIG
# select basin polygons (or other selection) and save shapefiles
basin_types = ["bergend"]  # ,  "hoofdwater"]
# %%

delwaq_config = load_delwaq_config(CONFIG_PATH)
model_name = delwaq_config.model_path.name
model_path = delwaq_config.model_path
output_path = delwaq_config.animo_data / "aangeleverd" / "shapes" / model_name
# %%

basin_path = model_path / "input/database.gpkg"
basin = gpd.read_file(basin_path, layer="Basin / area")

nodes = gpd.read_file(basin_path, layer="Node", fid_as_index=True)
nodes.index.rename("node_id", inplace=True)
nodes = nodes.reset_index()

basin = basin.merge(nodes[["node_id", "meta_categorie"]], on="node_id", how="left")

# %%

basins_to_save = basin[basin["meta_categorie"].isin(basin_types)]
total_area_km2 = basins_to_save.geometry.area.sum() / 1e6
print(f"Total area of selected polygons: {total_area_km2:.2f} km²")

output_path.mkdir(exist_ok=True)

shp_path = output_path / f"basin_polygons_{model_name}.shp"

basins_to_save.to_file(shp_path)

# %%
# diagnostic: compare area of polygon selection to total area that is covered by LHM
# note that this is not the surface area of the Netherlands
dissolved = basin.dissolve()
total_area_dissolved_km2 = dissolved.geometry.area.iloc[0] / 1e6
print(f"total area covered by LHM: {total_area_dissolved_km2:.2f} km²")
print(f"Total area of selected polygons: {total_area_km2:.2f} km²")
print(f"Percent of LHM area accounted for by selected polygons: {total_area_km2 / total_area_dissolved_km2 * 100:.2f}%")
# %%
# more diagnostic: overlap between selected basin nodes
dissolved_sel = basins_to_save.dissolve()
total_area_dissolved_sel_km2 = dissolved_sel.geometry.area.iloc[0] / 1e6
print(f"total area covered by LHM: {total_area_dissolved_sel_km2:.2f} km²")
print(f"Total area of selected polygons: {total_area_km2:.2f} km²")
print(f"Areal coverage of selected basin polygons: {total_area_km2 / total_area_dissolved_km2 * 100:.2f}%")
print("if this exceeds 100%, there is overlap among the selected polygons")

# %%

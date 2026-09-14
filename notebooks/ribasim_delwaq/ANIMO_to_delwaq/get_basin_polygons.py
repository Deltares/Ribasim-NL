# %%
from pathlib import Path

import geopandas as gpd

model_name = "lhm_coupled_full"

script_dir = Path(__file__).resolve().parent
root_dir = script_dir.resolve().parent.parent.parent
model_path = root_dir / "data/Rijkswaterstaat/modellen" / model_name
# %%

basin_path = model_path / "input/database.gpkg"
basin = gpd.read_file(basin_path, layer="Basin / area")

nodes = gpd.read_file(basin_path, layer="Node", fid_as_index=True)
nodes.index.rename("node_id", inplace=True)
nodes = nodes.reset_index()

basin = basin.merge(nodes[["node_id", "meta_categorie"]], on="node_id", how="left")

# %%
# select basin polygons (or other selection) and save shapefiles

basin_types = ["bergend"]  # ,  "hoofdwater"]

basins_to_save = basin[basin["meta_categorie"].isin(basin_types)]
total_area_km2 = basins_to_save.geometry.area.sum() / 1e6
print(f"Total area of selected polygons: {total_area_km2:.2f} km²")

out_dir = script_dir / "output"
out_dir.mkdir(exist_ok=True)

shp_path = out_dir / f"basin_polygons_{model_name}.shp"

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

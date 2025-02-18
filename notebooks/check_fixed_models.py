# %%
import geopandas as gpd

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authorities = [
    "Noorderzijlvest",
    "HunzeenAas",
    "DrentsOverijsselseDelta",
    "AaenMaas",
    "BrabantseDelta",
    "StichtseRijnlanden",
    "ValleienVeluwe",
    "Vechtstromen",
    "RijnenIJssel",
    "DeDommel",
    "Limburg",
]

basin_file_not_exists = []
internal_basin_file_not_exists = []
unassigned_basin_node = []
unassigned_basin_area = []
internal_basin = []

for authority in authorities:
    gpkg = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", "basin_node_area_errors.gpkg")
    if not gpkg.exists():
        basin_file_not_exists += [authority]
    else:
        df = gpd.read_file(gpkg, layer="unassigned_basin_node")
        if not df.empty:
            unassigned_basin_node += [authority]
        df = gpd.read_file(gpkg, layer="unassigned_basin_area")
        if not df.empty:
            unassigned_basin_area += [authority]
    gpkg = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", "internal_basins.gpkg")
    if not gpkg.exists():
        internal_basin_file_not_exists += [authority]
    else:
        df = gpd.read_file(gpkg, layer="internal_basins")
        if not df.empty:
            internal_basin += [authority]

assert len(basin_file_not_exists) == 0
assert len(internal_basin_file_not_exists) == 0
assert len(unassigned_basin_area) == 0
assert len(unassigned_basin_node) == 0
assert len(internal_basin) == 0

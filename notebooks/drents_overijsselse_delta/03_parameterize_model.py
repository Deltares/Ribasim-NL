# %%
import time

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "DrentsOverijsselseDelta"
short_name = "dod"

run_model = False

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens\\QGIS_lyr\\output_controle_vaw_afvoer.qlr")
inlaten = cloud.joinpath(authority, "verwerkt\\1_ontvangen_data\extra data\\Duiker_inlaat_lijn\\Duiker_inlaat_lijn.shp")
inlaten_gdf = gpd.read_file(inlaten)

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
# cloud.synchronize(filepaths=[static_data_xlsx, profiles_gpkg])
# cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# read
model = Model.read(ribasim_toml)
start_time = time.time()


# %%
# parameterize

model.parameterize(
    static_data_xlsx=static_data_xlsx,
    precipitation_mm_per_day=10,
    profiles_gpkg=profiles_gpkg,
    max_pump_flow_rate=125,
)
print("Elapsed Time:", time.time() - start_time, "seconds")

# %%
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.001

# %%
# Inlaten op False zetten
outlet_node_df = model.outlet.node.df.copy()
outlet_gdf = gpd.GeoDataFrame(outlet_node_df, geometry="geometry", crs="EPSG:28992")

if inlaten_gdf.crs != outlet_gdf.crs:
    inlaten_gdf = inlaten_gdf.to_crs(outlet_gdf.crs)

outlet_gdf["geometry"] = outlet_gdf.geometry.buffer(0.1)
joined = gpd.sjoin(outlet_gdf, inlaten_gdf, how="inner", predicate="intersects")

if joined.empty:
    print("Geen intersectie gevonden tussen buffered outlet-punten en inlaten-lijnen.")
else:
    for node_id, row in joined.iterrows():
        model.outlet.static.df.loc[model.outlet.static.df.node_id == node_id, ["meta_categorie"]] = "Inlaat"

node_ids = model.outlet.static.df[model.outlet.static.df["meta_categorie"] == "Inlaat"]["node_id"].to_numpy()
model.outlet.static.df.loc[model.outlet.static.df["node_id"].isin(node_ids), "max_flow_rate"] = 0.1

# %%
model.update_node(node_id=1401, node_type="Outlet")
model.reverse_link(link_id=894)
model.reverse_link(link_id=1983)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1401, "min_upstream_level"] = -2.6
model.pump.static.df.loc[model.pump.static.df.node_id == 600, "min_upstream_level"] = 4
model.pump.static.df.loc[model.pump.static.df.node_id == 601, "min_upstream_level"] = 4
model.pump.static.df.loc[model.pump.static.df.node_id == 602, "min_upstream_level"] = 2.65
model.pump.static.df.loc[model.pump.static.df.node_id == 1095, "min_upstream_level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 739, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 749, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 841, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1724, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1257, "active"] = False
model.pump.static.df.loc[model.pump.static.df.node_id == 2594, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 980, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 980, "meta_categorie"] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1143, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1143, "meta_categorie"] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1401, "meta_categorie"] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1086, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1086, "meta_categorie"] = "Inlaat"
model.pump.static.df.loc[model.pump.static.df.node_id == 667, "flow_rate"] = 1
model.pump.static.df.loc[model.pump.static.df.node_id == 385, "flow_rate"] = 1
model.merge_basins(basin_id=1763, to_basin_id=1764, are_connected=False)
model.merge_basins(basin_id=2474, to_basin_id=2185, are_connected=False)
model.merge_basins(basin_id=2011, to_basin_id=2008, are_connected=True)
model.merge_basins(basin_id=56, to_basin_id=59, are_connected=True)
model.merge_basins(basin_id=1681, to_basin_id=1717, are_connected=True)
model.merge_basins(basin_id=2348, to_basin_id=1756, are_connected=False)
model.merge_basins(basin_id=2192, to_basin_id=2194, are_connected=False)


# %%

node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# %%

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
add_check_basin_level(model=model)
model.write(ribasim_toml)

# %%
# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

# # %%
controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
indicators = controle_output.run_afvoer()

# %%

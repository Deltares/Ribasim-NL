# %%
import time

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    sync_min_upstream_levels_with_profile_bottoms,
)
from ribasim_nl.parametrization.level_boundary_table import update_level_boundary_static
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model


# Helper
def update_nodes(model: Model, node_ids: list[int], node_type: str) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.update_node(node_id=node_id, node_type=node_type)


def remove_nodes(model: Model, node_ids: list[int]) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.remove_node(node_id, remove_links=True)


# %%
cloud = CloudStorage()
authority = "DrentsOverijsselseDelta"
short_name = "dod"

run_model = False

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")
inlaten = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/extra data/Duiker_inlaat_lijn/Duiker_inlaat_lijn.shp")
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, qlr_path, inlaten, aanvoergebieden_gpkg])

# read
inlaten_gdf = gpd.read_file(inlaten)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

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
basin_level_overrides = [
    # Paradijssluis ZP peilenkaart
    ([59, 58, 2150, 2306, 2113, 2140], -0.2),
    # Haveltersluis ZP peilenkaart
    ([2105], 1.82),
    # Ossesluis ZP peilenkaart
    ([1680], 4.8),
    # Nieuwebrugsluis ZP peilenkaart
    ([1772], 11.1),
    # Smildigersluis ZP peilenkaart
    ([1747], 13.27),
    # Zwiggeltersluissluis ZP peilenkaart
    ([1901], 14.95),
    # Overijsels kanaal (Ankersmit)
    ([1761, 2008, 2229, 2247, 2449], 5.75),
    ([1721], 8.35),
    # Rietberg
    ([2288], 1.6),
    # Peilgebied VL169
    ([1635], 4.95),
    # BoezemNW
    ([2580], -0.73),
    # Bentpolder
    ([2185], -0.45),
    ([2131], 8.4),
]
protected_basin_node_ids = apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

model.update_node(node_id=1452, node_type="Outlet")

# make outlet nodes
update_nodes(
    model,
    [1418, 1419, 1428, 1433, 1444, 1448, 1452, 1459, 1460, 1462, 1466, 1488, 1512, 1513, 1522, 1548, 1550],
    "Outlet",
)

# make manning nodes
update_nodes(model, [859, 1032, 1060, 1083], "ManningResistance")

# make pump nodes
update_nodes(model, [195, 332, 346, 414, 742, 829, 906, 1048, 1076, 1423, 1484, 1516], "Pump")


# make outlet nodes
update_nodes(
    model,
    [1418, 1419, 1428, 1433, 1444, 1448, 1452, 1459, 1460, 1462, 1466, 1488, 1512, 1513, 1522, 1548, 1550],
    "Outlet",
)

# Westerveld
update_nodes(model, [1104, 885, 1522], "ManningResistance")

# Verwijderen nutteloze kunstwerken voor LHM
remove_nodes(model, [530, 264, 849, 1174, 1229, 266, 268, 523, 275, 537, 464, 406])

# Verwijderen dubbele sluispompen. Capaciteit staat op de dichtstbijzijnde pomp per sluis.
remove_nodes(model, [706, 550, 548, 632, 686, 406, 650])


model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
sync_parameterized_manning_basin_levels(
    model=model,
    aanvoergebieden_df=aanvoergebieden_df,
    output_gpkg=cloud.joinpath(
        authority,
        "modellen",
        f"{authority}_parameterized_model",
        "manning_level_basin_updates.gpkg",
    ),
    protected_basin_node_ids=protected_basin_node_ids,
)

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
    for node_id, _row in joined.iterrows():
        model.outlet.static.df.loc[model.outlet.static.df.node_id == node_id, ["meta_categorie"]] = "Inlaat"

node_ids = model.outlet.static.df[model.outlet.static.df["meta_categorie"] == "Inlaat"]["node_id"].to_numpy()
model.outlet.static.df.loc[model.outlet.static.df["node_id"].isin(node_ids), "max_flow_rate"] = 0.1

# %%

model.outlet.static.df.loc[model.outlet.static.df.node_id == 980, "meta_categorie"] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1143, "meta_categorie"] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1401, "meta_categorie"] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1086, "meta_categorie"] = "Inlaat"

update_level_boundary_static(
    model=model,
    static_data_xlsx=static_data_xlsx,
    code_column="meta_code_waterbeheerder",
)


# %%

node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# %%

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
sync_min_upstream_levels_with_profile_bottoms(model=model)
add_check_basin_level(model=model)
model.write(ribasim_toml)

# %%
# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()

# %%

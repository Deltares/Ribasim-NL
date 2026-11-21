# %%
import time

import geopandas as gpd
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.controle_output import AFVOER_METRICS, Control
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    clear_ungated_outlet_levels,
    sync_min_upstream_levels_with_profile_bottoms,
)
from ribasim_nl.parametrization.level_boundary_table import update_level_boundary_static
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model

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

# read
inlaten_gdf = gpd.read_file(inlaten)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
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
    (
        [
            n("bnd:59"),
            n("bnd:58"),
            n("up:duikersifonhevel.134542"),
            n("up:ow:hydroobject.7406"),
            n("up:inlaat_nvl2"),
            n("up:ow:hydroobject.12715"),
        ],
        -0.2,
    ),
    # Haveltersluis ZP peilenkaart
    ([n("up:gemaal.21189")], 1.82),
    # Ossesluis ZP peilenkaart
    ([n("up:duikersifonhevel.27017")], 4.8),
    # Nieuwebrugsluis ZP peilenkaart
    ([n("up:duikersifonhevel.134226")], 11.1),
    # Smildigersluis ZP peilenkaart
    ([n("up:duikersifonhevel.13280")], 13.27),
    # Zwiggeltersluissluis ZP peilenkaart
    ([n("up:duikersifonhevel.24641")], 14.95),
    # Overijsels kanaal (Ankersmit)
    (
        [
            n("up:duikersifonhevel.284644"),
            n("up:gemaal.11472"),
            n("up:duikersifonhevel.284665"),
            n("up:duikersifonhevel.286196"),
            n("up:duikersifonhevel.422952"),
        ],
        5.75,
    ),
    ([n("up:duikersifonhevel.284649")], 8.35),
    # Rietberg
    ([n("up:duikersifonhevel.464602")], 1.6),
    # Peilgebied VL169
    ([n("up:duikersifonhevel.15979")], 4.95),
    # BoezemNW
    ([n("loc:197542,525242")], -0.73),
    # Bentpolder
    ([n("up:gemaal.8491")], -0.45),
    ([n("up:stuw.16980")], 8.4),
]
protected_basin_node_ids = apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

model.update_node(node_id=n("ow:hydroobject.236366"), node_type="Outlet")

# make outlet nodes
model.update_nodes(
    [
        n("ow:hydroobject.203309"),
        n("ow:hydroobject.200741"),
        n("ow:hydroobject.10133"),
        n("ow:hydroobject.9069"),
        n("ow:hydroobject.211426"),
        n("ow:hydroobject.200446"),
        n("ow:hydroobject.236366"),
        n("ow:hydroobject.601606"),
        n("ow:hydroobject.203937"),
        n("ow:hydroobject.201586"),
        n("ow:hydroobject.214582"),
        n("ow:hydroobject.209633"),
        n("ow:hydroobject.11593"),
        n("ow:hydroobject.200512"),
        n("ow:hydroobject.9552"),
        n("ow:hydroobject.212111"),
        n("ow:hydroobject.429079"),
    ],
    "Outlet",
)

# make manning nodes
model.update_nodes(
    [
        n("duikersifonhevel.278403"),
        n("duikersifonhevel.18955"),
        n("duikersifonhevel.18988"),
        n("duikersifonhevel.508762"),
    ],
    "ManningResistance",
)

# make pump nodes
model.update_nodes(
    [
        n("stuw.47752"),
        n("stuw.2350"),
        n("stuw.112138"),
        n("stuw.97401"),
        n("duikersifonhevel.285129"),
        n("duikersifonhevel.285046"),
        n("duikersifonhevel.464602"),
        n("duikersifonhevel.18968"),
        n("duikersifonhevel.739223"),
        n("ow:hydroobject.10384"),
        n("ow:hydroobject.308700"),
        n("ow:hydroobject.9684"),
    ],
    "Pump",
)


# make outlet nodes
model.update_nodes(
    [
        n("ow:hydroobject.203309"),
        n("ow:hydroobject.200741"),
        n("ow:hydroobject.10133"),
        n("ow:hydroobject.9069"),
        n("ow:hydroobject.211426"),
        n("ow:hydroobject.200446"),
        n("ow:hydroobject.236366"),
        n("ow:hydroobject.601606"),
        n("ow:hydroobject.203937"),
        n("ow:hydroobject.201586"),
        n("ow:hydroobject.214582"),
        n("ow:hydroobject.209633"),
        n("ow:hydroobject.11593"),
        n("ow:hydroobject.200512"),
        n("ow:hydroobject.9552"),
        n("ow:hydroobject.212111"),
        n("ow:hydroobject.429079"),
    ],
    "Outlet",
)

# Westerveld
model.update_nodes(
    [n("duikersifonhevel.1016527"), n("duikersifonhevel.275959"), n("ow:hydroobject.9552")], "ManningResistance"
)

# Verwijderen nutteloze kunstwerken voor LHM
model.remove_nodes(
    [
        n("sluis.3335"),
        n("stuw.43262"),
        n("duikersifonhevel.280149"),
        n("duikersifonhevel.284680"),
        n("duikersifonhevel.754571"),
        n("stuw.43349"),
        n("stuw.43369"),
        n("sluis.3323"),
        n("stuw.67928"),
        n("sluis.1966"),
        n("stuw.21779"),
        n("stuw.107020"),
    ],
    remove_links=True,
)

# Verwijderen dubbele sluispompen. Capaciteit staat op de dichtstbijzijnde pomp per sluis.
model.remove_nodes(
    [
        n("gemaal.24733"),
        n("gemaal.25052"),
        n("gemaal.25372"),
        n("gemaal.25692"),
        n("gemaal.25693"),
        n("gemaal.25694"),
    ],
    remove_links=True,
)


assert model.manning_resistance.static.df is not None
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
    assert outlet_gdf.crs is not None
    inlaten_gdf = inlaten_gdf.to_crs(outlet_gdf.crs)

outlet_gdf["geometry"] = outlet_gdf.geometry.buffer(0.1)
joined = gpd.sjoin(outlet_gdf, inlaten_gdf, how="inner", predicate="intersects")

assert model.outlet.static.df is not None
if joined.empty:
    print("Geen intersectie gevonden tussen buffered outlet-punten en inlaten-lijnen.")
else:
    for node_id, _row in joined.iterrows():
        model.outlet.static.df.loc[model.outlet.static.df.node_id == node_id, ["meta_categorie"]] = "Inlaat"

node_ids = model.outlet.static.df[model.outlet.static.df["meta_categorie"] == "Inlaat"]["node_id"].to_numpy()
model.outlet.static.df.loc[model.outlet.static.df["node_id"].isin(node_ids), "max_flow_rate"] = 0.1

# %%

model.outlet.static.df.loc[model.outlet.static.df.node_id == n("duikersifonhevel.11301"), "meta_categorie"] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("duikersifonhevel.12171"), "meta_categorie"] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("ow:hydroobject.497881"), "meta_categorie"] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("duikersifonhevel.13178"), "meta_categorie"] = "Inlaat"

update_level_boundary_static(
    model=model,
    static_data_xlsx=static_data_xlsx,
    code_column="meta_code_waterbeheerder",
)


# %%

clear_ungated_outlet_levels(model)

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
    indicators = controle_output.run(metrics=AFVOER_METRICS)

# %%

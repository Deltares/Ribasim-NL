# %%
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from ribasim_nl.parametrization.target_level import upstream_target_levels

cloud = CloudStorage()
authority = "Vechtstromen"
short_name = "vechtstromen"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
profielpunt_shp = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/nalevering_20240920/Meting_profielpunt_wvs.shp")
profiellijn_shp = cloud.joinpath(
    authority, "verwerkt/1_ontvangen_data/nalevering_20240920/meting_profiellijn_soort_profiel_wsv.shp"
)
# peilgebieden_path = cloud.joinpath(authority, "verwerkt/downloads/peilgebieden_voormalig_velt_en_vecht.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")
peilregister_xlsx = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/nalevering_20240920/Peilregister.xlsx")
cloud.synchronize(filepaths=[top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name

static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%
lines_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject")
network = Network(lines_gdf=gpd.read_file(hydamo_gpkg, layer="hydroobject"), tolerance=0.2)

# %%
profile_line_df = gpd.read_file(profiellijn_shp).to_crs(28992)
profile_line_df = profile_line_df[profile_line_df["SOORTPROFI"] == 7]
profile_point_df = gpd.read_file(profielpunt_shp).to_crs(28992)
profile_point_df.rename(columns={"METINGPROF": "profiellijnid", "CODEVOLGNU": "codevolgnummer"}, inplace=True)

damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=profile_line_df,
    profile_point_df=profile_point_df,
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
    profile_line_id_col="GLOBALID",
)
# %%
if not profiles_gpkg.exists():
    profiles_df = damo_profiles.process_profiles()
    profiles_df = profiles_df[profiles_df.bottom_level != 0]
    profiles_df = profiles_df[profiles_df.invert_level < 50]
    profiles_df.to_file(profiles_gpkg)
else:
    profiles_df = gpd.read_file(profiles_gpkg)
profiles_df.set_index("profiel_id", inplace=True)

# %%

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
        "meta_profielid_waterbeheerder"
    ]

else:
    fix_link_geometries(model, network, max_straight_line_ratio=5)
    add_link_profile_ids(model, profiles=damo_profiles, id_col="GLOBALID")
    model.edge.df.reset_index().to_file(link_geometries_gpkg)

# %%

# add link profiles

# %%

# add streefpeilen
# add_streefpeil(
#    model=model,
#    peilgebieden_path=peilgebieden_path,
#    layername=None,
#    target_level="GPGZMRPL",
#    code="GPGIDENT",
# )
if "meta_code_waterbeheerder" not in model.basin.area.columns:
    model.basin.area.df["meta_code_waterbeheerder"] = pd.Series(dtype=str)

if "meta_streefpeil" in model.basin.area.columns:
    model.basin.area.drop(columns="meta_streefpeil", inplace=True)
model.basin.area.df["meta_streefpeil"] = pd.Series(dtype=float)

# %%
# OUTLET

# OUTLET.min_upstream_level
# from basin streefpeil

static_data.reset_data_frame(node_type="Outlet")

min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.outlet.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Outlet", series=min_upstream_level)

# from Peilregister
peilregister_df = pd.read_excel(peilregister_xlsx, sheet_name="Blad2")
peilregister_df.drop_duplicates("KXXIDENT", inplace=True)
peilregister_df.set_index("KXXIDENT", inplace=True)
peilregister_df.index.name = "code"
min_upstream_level = peilregister_df["GGOR_ZPNAP"]
min_upstream_level.name = "min_upstream_level"
min_upstream_level = min_upstream_level[min_upstream_level != -9.99]
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# from DAMO profiles
node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.edge.df[model.edge.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
]
# levels = (
#    profiles_df.loc[profile_ids]["bottom_level"]
#    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 2
# ).to_numpy()

levels = (profiles_df.loc[profile_ids]["invert_level"] - 1).to_numpy()
min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# %%

# PUMP

# PUMP.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Pump")
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.pump.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Pump", series=min_upstream_level)

# from damo
pump_df = gpd.read_file(hydamo_gpkg, layer="gemaal").set_index("code")
min_upstream_level = pump_df["kerendehoogte"]
min_upstream_level.name = "min_upstream_level"
min_upstream_level = min_upstream_level[min_upstream_level < 50]
static_data.add_series(node_type="Pump", series=min_upstream_level, fill_na=True)

# from DAMO profiles
node_ids = static_data.pump[static_data.pump.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.edge.df[model.edge.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
]
# levels = ((profiles_df.loc[profile_ids]["invert_level"] + profiles_df.loc[profile_ids]["bottom_level"]) / 2).to_numpy()

# veel bottom levels kloppen niet dus nemen we de invert level -1
levels = (profiles_df.loc[profile_ids]["invert_level"] - 1).to_numpy()
min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Pump", series=min_upstream_level, fill_na=True)


# %%

# # BASIN
static_data.reset_data_frame(node_type="Basin")

# fill streefpeil from ds min_upstream_level
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()

ds_levels = pd.concat([static_data.basin, static_data.outlet, static_data.pump], ignore_index=True).set_index(
    "node_id"
)["min_upstream_level"]
ds_levels.dropna(inplace=True)
ds_levels = ds_levels[ds_levels.index.isin(ds_node_ids)]
ds_node_ids = ds_node_ids[ds_node_ids.isin(ds_levels.index)]

levels = ds_node_ids.apply(lambda x: ds_levels[x])
streefpeil = levels.groupby(levels.index).min()
streefpeil.name = "streefpeil"
streefpeil.index.name = "node_id"
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

# get all nodata streefpeilen with their profile_ids and levels
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
profile_ids = [damo_profiles.get_profile_id(node_id) for node_id in node_ids]
# levels = (
#    profiles_df.loc[profile_ids]["bottom_level"]
#    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 2
# ).to_numpy()

# Veel bottom_levels kloppen niet, dus we nemen de invert_level -1
levels = (profiles_df.loc[profile_ids]["invert_level"] - 1).to_numpy()

# # update static_data
profielid = pd.Series(profile_ids, index=pd.Index(node_ids, name="node_id"), name="profielid")
static_data.add_series(node_type="Basin", series=profielid, fill_na=True)
streefpeil = pd.Series(levels, index=pd.Index(node_ids, name="node_id"), name="streefpeil")
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

# # update model basin-data
model.basin.area.set_index("node_id", inplace=True)

streefpeil = static_data.basin.set_index("node_id")["streefpeil"]
model.basin.area.loc[streefpeil.index, "meta_streefpeil"] = streefpeil
profiellijnid = static_data.basin.set_index("node_id")["profielid"]
model.basin.area.loc[streefpeil.index, "meta_profiellijnid"] = profiellijnid

model.basin.area.reset_index(drop=False, inplace=True)
model.basin.area.index += 1
model.basin.area.index.name = "fid"

# some customs
model.basin.area.loc[model.basin.area.node_id == 1549]

# write
static_data.write()

model.write(ribasim_toml)

# %%

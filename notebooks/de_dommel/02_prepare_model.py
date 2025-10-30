# %%
import geopandas as gpd
import pandas as pd
from peilbeheerst_model.assign_authorities import AssignAuthorities
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData

from ribasim_nl import CloudStorage, Model, Network

cloud = CloudStorage()
authority = "DeDommel"
short_name = "dommel"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")

cloud.synchronize(filepaths=[top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name

static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%
profile_line_df = gpd.read_file(hydamo_gpkg, layer="profiellijn")
profile_line_df.rename(columns={"GLOBALID": "globalid"}, inplace=True)
profile_point_df = gpd.read_file(hydamo_gpkg, layer="profielpunt")
profile_point_df.rename(columns={"PROFIELLIJNID": "profiellijnid", "CODEVOLGNUMMER": "codevolgnummer"}, inplace=True)
network = Network(lines_gdf=gpd.read_file(hydamo_gpkg, layer="hydroobject"))
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=profile_line_df,
    profile_point_df=profile_point_df,
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
)
# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.link.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    if "meta_profielid_waterbeheerder" in link_geometries_df.columns:
        model.link.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
            "meta_profielid_waterbeheerder"
        ]
    profiles_df = gpd.read_file(profiles_gpkg)


else:
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
    add_link_profile_ids(model, profiles=damo_profiles)
    fix_link_geometries(model, network)
    model.link.df.reset_index().to_file(link_geometries_gpkg)

profiles_df.set_index("profiel_id", inplace=True)
# %%

# add link profiles

# %%
# OUTLET

# OUTLET.min_upstream_level
# from basin streefpeil
stuw_df = gpd.read_file(hydamo_gpkg, layer="stuw", engine="pyogrio")
stuw_df = stuw_df[
    stuw_df["NAAM"] != "BS1 drempel 1 vispassage Spoordonk"
]  # deze vispassage heeft code gekregen van BS1 drempel 1 voorde bovenstrooms zandvang Heilige Eik?

# from WS STREEFPEILHOOG
static_data.reset_data_frame(node_type="Outlet")
min_upstream_level = stuw_df.set_index("CODE")["WS_STREEFPEILHOOG"]
min_upstream_level = min_upstream_level[min_upstream_level != 9999]
min_upstream_level = min_upstream_level[min_upstream_level.index.notna()]
min_upstream_level.name = "min_upstream_level"
min_upstream_level.index.name = "code"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# from DAMO profiles
node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.link.df[model.link.df.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
]
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 2
).to_numpy()

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)


# %%

# PUMP
static_data.reset_data_frame(node_type="Pump")
kgm_df = gpd.read_file(hydamo_gpkg, layer="gemaal").set_index("CODE")

# Pump.min_upstream_level

# from DAMO profiles
node_ids = static_data.pump[static_data.pump.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.link.df[model.link.df.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
]
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 2
).to_numpy()

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

ds_levels = pd.concat([static_data.basin, static_data.outlet], ignore_index=True).set_index("node_id")[
    "min_upstream_level"
]
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
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 2
).to_numpy()

# # update static_data
profielid = pd.Series(profile_ids, index=pd.Index(node_ids, name="node_id"), name="profielid")
static_data.add_series(node_type="Basin", series=profielid, fill_na=True)
streefpeil = pd.Series(levels, index=pd.Index(node_ids, name="node_id"), name="streefpeil")
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

# # update model basin-data
model.basin.area.df.set_index("node_id", inplace=True)

streefpeil = static_data.basin.set_index("node_id")["streefpeil"]
model.basin.area.df.loc[streefpeil.index, "meta_streefpeil"] = streefpeil
profiellijnid = static_data.basin.set_index("node_id")["profielid"]
model.basin.area.df.loc[streefpeil.index, "meta_profiellijnid"] = profiellijnid

model.basin.area.df.reset_index(drop=False, inplace=True)
model.basin.area.df.index += 1
model.basin.area.df.index.name = "fid"


# # koppelen
ws_grenzen_path = cloud.joinpath("Basisgegevens", "RWS_waterschaps_grenzen", "waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens", "RWS_waterschaps_grenzen", "Rijkswaterstaat.gpkg")
assign = AssignAuthorities(
    ribasim_model=model,
    waterschap=authority,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={1891: "Buitenland", 1911: "Buitenland"},
)
model = assign.assign_authorities()

# %%

# write
static_data.write()

model.write(ribasim_toml)

# %%

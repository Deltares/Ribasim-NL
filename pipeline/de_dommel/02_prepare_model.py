# %%
import geopandas as gpd
from ribasim_nl.assign_authorities import AssignAuthorities
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

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
parameters_dir.mkdir(parents=True, exist_ok=True)
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens/Top10NL/top10nl_Compleet.gpkg")

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
# fix link geometries and profiles
profiles_df = damo_profiles.process_profiles()
profiles_df.to_file(profiles_gpkg)
add_link_profile_ids(model, profiles=damo_profiles)
fix_link_geometries(model, network)
assert model.link.df is not None
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
static_data.fill_min_upstream_level_from_profiles("Outlet", profiles_df)


# %%

# PUMP
static_data.reset_data_frame(node_type="Pump")
kgm_df = gpd.read_file(hydamo_gpkg, layer="gemaal").set_index("CODE")

# Pump.min_upstream_level

# from DAMO profiles
static_data.fill_min_upstream_level_from_profiles("Pump", profiles_df)

# %%

# # BASIN
static_data.reset_data_frame(node_type="Basin")

# fill streefpeil from ds min_upstream_level
static_data.fill_streefpeil_from_downstream_levels()

# get all nodata streefpeilen with their profile_ids and levels
static_data.fill_streefpeil_from_profiles(damo_profiles, profiles_df)

# # update model basin-data
static_data.update_model_basin_levels()


# # koppelen
ws_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/Rijkswaterstaat.gpkg")
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

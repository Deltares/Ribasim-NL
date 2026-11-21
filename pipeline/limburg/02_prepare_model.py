# %%
import geopandas as gpd
from ribasim_nl.assign_authorities import AssignAuthorities
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData

from ribasim_nl import CloudStorage, Model, Network

cloud = CloudStorage()
authority = "Limburg"
short_name = "limburg"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
parameters_dir.mkdir(parents=True, exist_ok=True)
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

stuwen_shp = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/20250613/stuwWL_fase3.shp")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
profielen_gpkg = cloud.joinpath(authority, "verwerkt/profielen.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens/Top10NL/top10nl_Compleet.gpkg")

# %% init things
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name

static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%
lines_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
lines_gdf = lines_gdf[lines_gdf.index != 689]
network = Network(lines_gdf=lines_gdf)
profile_line_df = gpd.read_file(profielen_gpkg, layer="profiellijn")
profile_point_df = gpd.read_file(profielen_gpkg, layer="profielpunt")
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
fix_link_geometries(model, network)
add_link_profile_ids(model, profiles=damo_profiles)
model.link.df.reset_index().to_file(link_geometries_gpkg)
profiles_df.set_index("profiel_id", inplace=True)

# %%OUTLET

# OUTLET.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Outlet")

# vanuit stuwen

stuwen_df = gpd.read_file(stuwen_shp)
stuwen_df.index = "S_" + stuwen_df["CODE"]
stuwen_df.index.name = "code"
assert static_data.outlet is not None
stuwen_df = stuwen_df[stuwen_df.index.isin(static_data.outlet["code"])]
min_upstream_level = stuwen_df["WS_STUWF_3"]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# %%
# from DAMO profiles

static_data.fill_min_upstream_level_from_profiles("Outlet", profiles_df)

# %%

# %%
# PUMP vanuit profielen

static_data.reset_data_frame(node_type="Pump")
# from DAMO profiles
static_data.fill_min_upstream_level_from_profiles("Pump", profiles_df)

# %% Bepaal de basin streefpeilen door minimale upstream level van stuwen en gemalen. Duikers in basin worden gelijk gezet aan basinpeil

static_data.reset_data_frame(node_type="Basin")
static_data.fill_streefpeil_from_downstream_structures(lambda code: code.str.startswith("S"), fill_na=True)


# %% if basin niet gestuwd, level from damo profiles

# fill streefpeil from ds min_upstream_level
static_data.fill_streefpeil_from_downstream_levels()

# get all nodata streefpeilen with their profile_ids and levels
static_data.fill_streefpeil_from_profiles(damo_profiles, profiles_df)

# Handmatige correcties streefpeilen:
forced_levels = {
    n("loc:191446,372816"): 31.0,  # Benedenstrooms stuw Katsberg
    n("up:S_97631"): 30.13,  # Benedenstrooms Grenssloot op Moostdijk en AVL Dorperpeel
    n("up:G_27616"): 30.75,  # Bovenstrooms gemaal Helenaveen
    n("up:S_97928"): 30.75,
    n("up:D_217317"): 30.75,
    n("loc:189487,383501"): 30.75,
}

assert static_data.basin is not None
mask = static_data.basin["node_id"].isin(forced_levels.keys())
static_data.basin.loc[mask, "streefpeil"] = static_data.basin.loc[mask, "node_id"].map(forced_levels)

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
    custom_nodes={
        n("bnd:8"): "Rijkswaterstaat",
        n("bnd:37"): "Rijkswaterstaat",
        n("bnd:39"): "Buitenland",
        n("bnd:87"): "Buitenland",
        n("bnd:88"): "Buitenland",
        n("bnd:102"): "Buitenland",
        n("bnd:111"): "Buitenland",
        n("bnd:131"): "Buitenland",
        n("bnd:132"): "AaenMaas",
        n("up:S_97911"): "DeDommel",
    },
)
model = assign.assign_authorities()

# %%

# write
static_data.write()

model.write(ribasim_toml)
# %%

# %%
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from ribasim_nl.parametrization.target_level import upstream_target_levels
from ribasim_nl.streefpeilen import add_streefpeil

cloud = CloudStorage()
authority = "BrabantseDelta"
short_name = "wbd"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
damo_profiles_gpkg = cloud.joinpath(authority, "verwerkt/profielen.gpkg")
peilgebieden_path = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")
network_gpkg = cloud.joinpath(authority, "verwerkt", "network.gpkg")

cloud.synchronize(filepaths=[peilgebieden_path, top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name

static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%

# prepare DAMO profiles and network
lines_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", bbox=bbox)

if not network_gpkg.exists():
    network = Network(lines_gdf=lines_gdf, tolerance=0.2)
    network.to_file(network_gpkg)
else:
    network = Network.from_network_gpkg(network_gpkg)

damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(hydamo_gpkg, layer="profiellijn", bbox=bbox),
    profile_point_df=gpd.read_file(hydamo_gpkg, layer="profielpunt", bbox=bbox),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak", bbox=bbox),
    profile_line_id_col="code",
)
if not profiles_gpkg.exists():
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
else:
    profiles_df = gpd.read_file(profiles_gpkg)


network = Network(lines_gdf=gpd.read_file(hydamo_gpkg, layer="hydroobject"))
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(damo_profiles_gpkg, layer="profiellijn"),
    profile_point_df=gpd.read_file(damo_profiles_gpkg, layer="profielpunt"),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
    profile_line_id_col="code",
)

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("edge_id")
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    if "meta_profielid_waterbeheerder" in link_geometries_df.columns:
        model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
            "meta_profielid_waterbeheerder"
        ]
    profiles_df = gpd.read_file(profiles_gpkg).set_index("profiel_id")
else:
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
    add_link_profile_ids(model, profiles=damo_profiles, id_col="code")
    fix_link_geometries(model, network)
    model.edge.df.reset_index().to_file(link_geometries_gpkg)

# %%

# add link profiles

# %%

# add streefpeilen
peilgebieden_path_editted = peilgebieden_path.parent.joinpath("peilgebieden_bewerkt.gpkg")


df = gpd.read_file(peilgebieden_path, layer="peilgebiedpraktijk")

# fill with zomerpeil
df["streefpeil"] = df["WS_ZOMERPEIL"]

# if nodata, fill with vastpeil
mask = df["streefpeil"].isna()
df.loc[mask, "streefpeil"] = df[mask]["WS_VAST_PEIL"]

# if nodata and onderpeil is nodata, fill with bovenpeil
mask = df["streefpeil"].isna()
df.loc[mask, "streefpeil"] = df[mask]["WS_MAXIMUM"]

# write
df[df["streefpeil"].notna()].to_file(peilgebieden_path_editted)


add_streefpeil(
    model=model,
    peilgebieden_path=peilgebieden_path_editted,
    layername=None,
    target_level="streefpeil",
    code="CODE",
)

# %%
# OUTLET

# OUTLET.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Outlet")
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.outlet.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Outlet", series=min_upstream_level)

# from DAMO profiles
node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.edge.df[model.edge.df.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
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

# PUMP.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Pump")
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.pump.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Pump", series=min_upstream_level)

# from DAMO profiles
node_ids = static_data.pump[static_data.pump.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.edge.df[model.edge.df.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
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


# %%

# write
static_data.write()

model.write(ribasim_toml)

# %%

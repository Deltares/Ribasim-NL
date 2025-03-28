# %%

import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from ribasim_nl.parametrization.target_level import upstream_target_levels

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name

# %%

# check files
peilgebieden_path = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/Eerste_levering/vallei_en_veluwe.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")
venv_hydamo_gpkg = cloud.joinpath(authority, "verwerkt", "2_voorbewerking", "hydamo.gpkg")

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

cloud.synchronize(filepaths=[peilgebieden_path, top10NL_gpkg])

# %%
bbox = None
# init classes
network = Network(lines_gdf=gpd.read_file(venv_hydamo_gpkg, layer="hydroobject", bbox=bbox))
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(venv_hydamo_gpkg, layer="profiellijn", bbox=bbox),
    profile_point_df=gpd.read_file(venv_hydamo_gpkg, layer="profielpunt", bbox=bbox),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak", bbox=bbox),
    profile_line_id_col="code",
)
if not profiles_gpkg.exists():
    damo_profiles.process_profiles().to_file(profiles_gpkg)
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)

# %%
# Edges
network = Network(lines_gdf=gpd.read_file(venv_hydamo_gpkg, layer="hydroobject"))
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(venv_hydamo_gpkg, layer="profiellijn"),
    profile_point_df=gpd.read_file(venv_hydamo_gpkg, layer="profielpunt"),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
    profile_line_id_col="code",
)

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    if "meta_profielid_waterbeheerder" in link_geometries_df.columns:
        model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
            "meta_profielid_waterbeheerder"
        ]
    profiles_df = gpd.read_file(profiles_gpkg)
else:
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
    fix_link_geometries(model, network)
    add_link_profile_ids(model, profiles=damo_profiles, id_col="code")
    model.edge.df.reset_index().to_file(link_geometries_gpkg)
profiles_df.set_index("profiel_id", inplace=True)

# %%

# Basins
# add streefpeilen

# add_streefpeil(
#   model=model,
#   peilgebieden_path=peilgebieden_path,
#   layername="peilgebiedpraktijk",
#   target_level="ws_min_peil",
#   code="code",
# )

# if "meta_streefpeil" not in model.basin.area.df.columns:
#    model.basin.area.df["meta_streefpeil"] = pd.NA

# Ensure required columns exist in the dataframe
if "meta_code_waterbeheerder" not in model.basin.area.df.columns:
    model.basin.area.df["meta_code_waterbeheerder"] = pd.Series(dtype=str)

if "meta_streefpeil" in model.basin.area.df.columns:
    model.basin.area.df.drop(columns="meta_streefpeil", inplace=True)
model.basin.area.df["meta_streefpeil"] = pd.Series(dtype=float)

# %%
# OUTLET, peil uit stuwbestand bepalen
static_data.reset_data_frame(node_type="Outlet")
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.outlet.node_id)
min_upstream_level = min_upstream_level[
    min_upstream_level.notna() & (min_upstream_level != 0) & (min_upstream_level < 30)
]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Outlet", series=min_upstream_level)
stuwen = gpd.read_file(venv_hydamo_gpkg, layer="stuw").set_index("code")
hoogste = stuwen["hoogstedoorstroomhoogte"].copy()
laagste = stuwen["laagstedoorstroomhoogte"].copy()
valid_data = (hoogste.notna()) & (laagste.notna()) & (hoogste != 0) & (laagste != 0)
hoogste = hoogste[valid_data]
laagste = laagste[valid_data]

min_upstream_level = laagste + (hoogste - laagste) * 0.75
min_upstream_level = min_upstream_level[
    min_upstream_level.notna() & (min_upstream_level != 0) & (min_upstream_level < 30)
]
min_upstream_level.name = "min_upstream_level"
min_upstream_level.index.name = "code"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# %%
# 🏗️ DAMO-profielen bepalen voor outlets
node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = model.edge.df.set_index("to_node_id").loc[node_ids]["meta_profielid_waterbeheerder"]
levels = ((profiles_df.loc[profile_ids]["bottom_level"] + profiles_df.loc[profile_ids]["invert_level"]) / 2).to_numpy()

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# %%
# PUMP.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Pump")
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.pump.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Pump", series=min_upstream_level)

# from DAMO profiles
node_ids = static_data.pump[static_data.pump.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = model.edge.df.set_index("to_node_id").loc[node_ids]["meta_profielid_waterbeheerder"]
levels = ((profiles_df.loc[profile_ids]["bottom_level"] + profiles_df.loc[profile_ids]["invert_level"]) / 2).to_numpy()

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Pump", series=min_upstream_level, fill_na=True)

## PUMP.flow_rate
gkw_gemaal_df = get_data_from_gkw(authority=authority, layers=["gemaal"])
flow_rate = gkw_gemaal_df.set_index("code")["maximalecapaciteit"] / 60  # m3/minuut to m3/s
flow_rate.name = "flow_rate"
static_data.add_series(node_type="Pump", series=flow_rate)

# BASIN
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
model.basin.area.df["meta_streefpeil"] = pd.to_numeric(model.basin.area.df["meta_streefpeil"], errors="coerce")

model.basin.area.df.reset_index(drop=False, inplace=True)
model.basin.area.df.index += 1
model.basin.area.df.index.name = "fid"


# %%

# build static_data.xlsx

# defaults
static_data_xlsx = cloud.joinpath(
    authority,
    "verwerkt",
    "parameters",
    "static_data_template.xlsx",
)

static_data.write()

# write model
model.write(ribasim_toml)

# %%

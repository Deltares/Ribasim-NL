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

cloud.synchronize(filepaths=[stuwen_shp, hydamo_gpkg])
cloud.synchronize(filepaths=[top10NL_gpkg], overwrite=False)

# %% init things
model = Model.read(ribasim_toml)
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


# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.link.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    model.link.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
        "meta_profielid_waterbeheerder"
    ]
    profiles_df = gpd.read_file(profiles_gpkg)
else:
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
stuwen_df = stuwen_df[stuwen_df.index.isin(static_data.outlet["code"])]
min_upstream_level = stuwen_df["WS_STUWF_3"]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# %%
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

# %%
# PUMP vanuit profielen

static_data.reset_data_frame(node_type="Pump")
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

# %% Bepaal de basin streefpeilen door minimale upstream level van stuwen en gemalen. Duikers in basin worden gelijk gezet aan basinpeil

static_data.reset_data_frame(node_type="Basin")
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
ds_node_ids = []

for node_id in node_ids:
    try:
        ds = model.downstream_node_id(int(node_id))
        if isinstance(ds, pd.Series):
            ds_node_ids.append(ds.to_list())
        else:
            ds_node_ids.append([ds])
    except KeyError:
        ds_node_ids.append([])

ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()
ds_node_ids = ds_node_ids[ds_node_ids.isin(static_data.outlet.node_id) | ds_node_ids.isin(static_data.pump.node_id)]
combined = pd.concat([static_data.outlet, static_data.pump])
combined = combined.reset_index(drop=True)

combined["source"] = combined.apply(
    lambda row: "pump" if row["node_id"] in static_data.pump.node_id.values else "outlet", axis=1
)

valid_ids = ds_node_ids[ds_node_ids.isin(combined["node_id"])]
streefpeil = combined.set_index("node_id").loc[valid_ids.to_numpy(), ["min_upstream_level", "code", "source"]]
streefpeil["basin_node_id"] = valid_ids.loc[valid_ids.isin(streefpeil.index)].index
streefpeil["node_id"] = streefpeil.index  # outlet/pump node_id
streefpeil = streefpeil.set_index("basin_node_id")
streefpeil.dropna(inplace=True)

# Sorteer per groep zodat de kleinste 'min_upstream_level' bovenaan staat
streefpeil = streefpeil.sort_index().sort_values(by="min_upstream_level", inplace=False)
non_kdu_values = streefpeil[streefpeil["code"].str.startswith("S") | (streefpeil["source"] == "pump")]
non_kdu_first = non_kdu_values.groupby(level=0).first()
all_first = streefpeil.groupby(level=0).first()

# Reset min_upstream level duikers op basis van streefpeilen
streefpeil_update = streefpeil.loc[streefpeil.index.isin(non_kdu_first.index)].copy()
streefpeil_update["min_upstream_level"] = streefpeil_update.index.map(non_kdu_first["min_upstream_level"])
min_upstream_level_outlets = streefpeil_update.set_index("node_id")["min_upstream_level"]
min_upstream_level_outlets.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=min_upstream_level_outlets, fill_na=False)

# update basin levels
streefpeil = non_kdu_first.combine_first(all_first)
streefpeil = streefpeil["min_upstream_level"]
streefpeil.index.name = "node_id"
streefpeil.name = "streefpeil"
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

# Update stuwen met nodata op basis van basin streefpeil
missing_min_level_outlets = static_data.outlet[static_data.outlet.min_upstream_level.isna()]
downstream_basin_ids = model.link.df.set_index("to_node_id").loc[missing_min_level_outlets.node_id].from_node_id
downstream_basin_levels = static_data.basin.set_index("node_id").reindex(downstream_basin_ids)["streefpeil"]
downstream_basin_levels.index = missing_min_level_outlets.node_id
downstream_basin_levels.name = "min_upstream_level"
static_data.add_series(node_type="Outlet", series=downstream_basin_levels.dropna(), fill_na=False)

# Update pumps met nodata op basis van basin streefpeil
missing_min_level_pumps = static_data.pump[static_data.pump.min_upstream_level.isna()]
downstream_basin_ids = model.link.df.set_index("to_node_id").loc[missing_min_level_pumps.node_id].from_node_id
downstream_basin_levels = static_data.basin.set_index("node_id").reindex(downstream_basin_ids)["streefpeil"]
downstream_basin_levels.index = missing_min_level_pumps.node_id
downstream_basin_levels.name = "min_upstream_level"
static_data.add_series(node_type="Pump", series=downstream_basin_levels.dropna(), fill_na=False)


# %% if basin niet gestuwd, level from damo profiles

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
model.basin.area.df.loc[streefpeil.index, "meta_streefpeil"] = streefpeil.astype(float)
profiellijnid = static_data.basin.set_index("node_id")["profielid"]
model.basin.area.df.loc[streefpeil.index, "meta_profiellijnid"] = profiellijnid

model.basin.area.df.reset_index(drop=False, inplace=True)
model.basin.area.df.index += 1
model.basin.area.df.index.name = "fid"

# # koppelen
ws_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/Rijkswaterstaat.gpkg")
assign = AssignAuthorities(
    ribasim_model=model,
    waterschap=authority,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={
        8: "Rijkswaterstaat",
        37: "Rijkswaterstaat",
        39: "Buitenland",
        87: "Buitenland",
        88: "Buitenland",
        102: "Buitenland",
        111: "Buitenland",
        131: "Buitenland",
        132: "AaenMaas",
    },
)
model = assign.assign_authorities()

# %%

# write
static_data.write()

model.write(ribasim_toml)
# %%

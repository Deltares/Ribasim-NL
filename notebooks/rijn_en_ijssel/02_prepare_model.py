# %%
import geopandas as gpd
import pandas as pd

from peilbeheerst_model.assign_authorities import AssignAuthorities
from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData

cloud = CloudStorage()
authority = "RijnenIJssel"
short_name = "wrij"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"
regelpeil_csv = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/RegelmiddelZpWp.csv")

hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
profielen_gpkg = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/wrij_profielen.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")

cloud.synchronize(filepaths=[top10NL_gpkg, profielen_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name

static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%
network = Network(lines_gdf=gpd.read_file(hydamo_gpkg, layer="hydroobject"))
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
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
        "meta_profielid_waterbeheerder"
    ]
    profiles_df = gpd.read_file(profiles_gpkg)
else:
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
    fix_link_geometries(model, network)
    add_link_profile_ids(model, profiles=damo_profiles)
    model.edge.df.reset_index().to_file(link_geometries_gpkg)
profiles_df.set_index("profiel_id", inplace=True)

# %%

# add link profiles

# %%

# add streefpeilen

# %%
# OUTLET

# OUTLET.min_upstream_level
# from basin streefpeil
# Reset de data voor Outlet nodes
static_data.reset_data_frame(node_type="Outlet")

# Lees het CSV-bestand in (met tabs als delimiter)
regelpeil_df = pd.read_csv(regelpeil_csv, delimiter="\t")

# Strip de 'Code'-waarde tot alleen het relevante ST-codenummer, zoals 'ST84450156'
# Hiervoor gebruiken we regex om de middenwaarde eruit te halen (tussen de streepjes)
regelpeil_df["code"] = regelpeil_df["Code"].str.extract(r"-(ST\d+)-")
regelpeil_df = regelpeil_df[regelpeil_df["code"].notna()]
regelpeil_df = regelpeil_df.drop_duplicates(subset="code", keep="first")

regelpeil_df = regelpeil_df.set_index("code")
regelpeil_df.index.name = "code"
min_upstream_level = regelpeil_df["Stuwpeil zomer (m NAP)"]
min_upstream_level.name = "min_upstream_level"

# Voeg deze serie toe aan static_data voor nodes van type 'Outlet'
# Alleen codes die overeenkomen worden gekoppeld
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# from DAMO profiles
node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.edge.df[model.edge.df.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
]
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 3
).to_numpy()

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"

static_data.outlet.loc[static_data.outlet.node_id == 119, "min_upstream_level"] = 10  # here profiles are messed-up
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)


# %%

# PUMP

# PUMP.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Pump")
# from DAMO profiles
node_ids = static_data.pump[static_data.pump.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.edge.df[model.edge.df.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
]
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 3
).to_numpy()

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Pump", series=min_upstream_level, fill_na=True)

# %%

# BASIN
static_data.reset_data_frame(node_type="Basin")

# Vind Basin nodes zonder streefpeil
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()

# Zoek downstream nodes per Basin
ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()

# Combineer Basin, Outlet én Pump data
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
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 3
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
    custom_nodes={837: "Buitenland"},
)
model = assign.assign_authorities()

# %%

# write
static_data.write()

model.write(ribasim_toml)


# %%

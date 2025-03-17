# %%

import geopandas as gpd

from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from ribasim_nl.streefpeilen import add_streefpeil

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

# init classes
network = Network(lines_gdf=gpd.read_file(venv_hydamo_gpkg, layer="hydroobject"))
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(venv_hydamo_gpkg, layer="profiellijn"),
    profile_point_df=gpd.read_file(venv_hydamo_gpkg, layer="profielpunt"),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
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

# Basins
# add streefpeilen

add_streefpeil(
    model=model,
    peilgebieden_path=peilgebieden_path,
    layername="peilgebiedpraktijk",
    target_level="ws_min_peil",
    code="code",
)

# model.basin.area.df.loc[model.basin.area.df.meta_streefpeil == 999, "meta_streefpeil"] = 0.0
model.basin.area.df.loc[model.basin.area.df.meta_streefpeil > 990, "meta_streefpeil"] = 0.0
area_df = model.basin.area.df
area_df.loc[area_df.node_id == 1188, "meta_streefpeil"] = -0.4
area_df.loc[area_df.node_id == 1048, "meta_streefpeil"] = 12

# %%

# build static_data.xlsx

# defaults
static_data_xlsx = cloud.joinpath(
    authority,
    "verwerkt",
    "parameters",
    "static_data_template.xlsx",
)

static_data = StaticData(model=model, xlsx_path=static_data_xlsx)
static_data.write()

# write model
model.write(ribasim_toml)

# %%

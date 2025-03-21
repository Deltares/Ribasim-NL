# %%
import geopandas as gpd

from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from ribasim_nl.parametrization.target_level import upstream_target_levels
from ribasim_nl.streefpeilen import add_streefpeil

cloud = CloudStorage()
authority = "HunzeenAas"
short_name = "hea"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
profielen_gpkg = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/20230606_HyDAMO_Onze-PROF-Tabs-Definitief.gpkg")
peilgebieden_path = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/peilgebieden.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")

cloud.synchronize(filepaths=[peilgebieden_path, top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name

static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("edge_id")
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
        "meta_profielid_waterbeheerder"
    ]
else:
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
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
    fix_link_geometries(model, network)
    add_link_profile_ids(model, profiles=damo_profiles)
    model.edge.df.reset_index().to_file(link_geometries_gpkg)

# %%

# add link profiles

# %%

# add streefpeilen


add_streefpeil(
    model=model,
    peilgebieden_path=peilgebieden_path,
    layername=None,
    target_level="gpgzmrpl",
    code="gpgident",
)

model.basin.area.df.loc[model.basin.area.df.node_id == 1583, "meta_streefpeil"] = 1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 1583, "meta_code_waterbeheerder"] = "GPG-W-01961"

# %%
# OUTLET

# OUTLET.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Outlet")
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.outlet.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Outlet", series=min_upstream_level)


# %%

# PUMP

# PUMP.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Pump")
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.pump.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Pump", series=min_upstream_level)


# %%

# # BASIN
static_data.reset_data_frame(node_type="Basin")

# %%

# write
static_data.write()

model.write(ribasim_toml)

# %%

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
authority = "DrentsOverijsselseDelta"
short_name = "dod"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

peilgebieden_path = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/extra data/Peilgebieden/Peilgebieden.shp")
hydamo_wm_gpkg = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/HyDAMO_WM_20230720.gpkg")
meppelerdiep_gpkg = cloud.joinpath(authority, "verwerkt/2_voorbewerking/meppelerdiep.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")

cloud.synchronize(filepaths=[peilgebieden_path, top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name
lines_gdf = pd.concat(
    [gpd.read_file(hydamo_wm_gpkg, layer="hydroobject"), gpd.read_file(meppelerdiep_gpkg)], ignore_index=True
)
network = Network(lines_gdf=lines_gdf)
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(hydamo_wm_gpkg, layer="profiellijn"),
    profile_point_df=gpd.read_file(hydamo_wm_gpkg, layer="profielpunt"),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
)

if not profiles_gpkg.exists():
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
else:
    profiles_df = gpd.read_file(profiles_gpkg)

profiles_df.set_index("profiel_id", inplace=True)
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
    fix_link_geometries(model, network)
    add_link_profile_ids(model, profiles=damo_profiles)
    model.edge.df.reset_index().to_file(link_geometries_gpkg)

# %%

# add link profiles

# %%

# add streefpeilen
add_streefpeil(
    model=model, peilgebieden_path=peilgebieden_path, layername=None, target_level="GPGZMRPL", code="GPGIDENT"
)

model.basin.area.df.loc[model.basin.area.df.node_id == 2190, "meta_streefpeil"] = -0.6
model.basin.area.df.loc[model.basin.area.df.node_id == 1612, "meta_streefpeil"] = model.basin.area.df.set_index(
    "node_id"
).at[1769, "meta_streefpeil"]


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

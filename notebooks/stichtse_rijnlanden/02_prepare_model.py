# %%
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from ribasim_nl.parametrization.target_level import upstream_target_levels
from ribasim_nl.streefpeilen import add_streefpeil

cloud = CloudStorage()
authority = "StichtseRijnlanden"
short_name = "hdsr"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
# profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"
hydroobject_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydroobject.gpkg")
peilgebieden_path = cloud.joinpath(authority, "verwerkt/4_ribasim/peilgebieden.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")

cloud.synchronize(filepaths=[peilgebieden_path, top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name
network = Network(lines_gdf=gpd.read_file(hydroobject_gpkg, layer="hydroobject"))
# damo_profiles = DAMOProfiles(
#     model=model,
#     network=network,
#     profile_line_df=gpd.read_file(hydamo_wm_gpkg, layer="profiellijn"),
#     profile_point_df=gpd.read_file(hydamo_wm_gpkg, layer="profielpunt"),
#     water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
# )
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("edge_id")
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    # model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
    #     "meta_profielid_waterbeheerder"
    # ]
else:
    fix_link_geometries(model, network)
    # add_link_profile_ids(model, profiles=damo_profiles)
    model.edge.df.reset_index().to_file(link_geometries_gpkg)

# %%

# add link profiles

# %%

# add streefpeilen
peilgebieden_path_editted = peilgebieden_path.with_name(f"{peilgebieden_path.stem}_bewerkt.gpkg")
if not peilgebieden_path_editted.exists():
    df = gpd.read_file(peilgebieden_path)

    # LBW_A_069 has 999 bovenpeil
    mask = df["WS_BP"] == 999
    df.loc[mask, "WS_BP"] = pd.NA

    # fill with zomerpeil
    df["streefpeil"] = df["WS_ZP"]

    # if nodata, fill with vastpeil
    mask = df["streefpeil"].isna()
    df.loc[mask, "streefpeil"] = df[mask]["WS_VP"]

    # if nodata and onderpeil is nodata, fill with bovenpeil
    mask = df["streefpeil"].isna()
    df.loc[mask, "streefpeil"] = df[mask]["WS_BP"]

    # write
    df[df["streefpeil"].notna()].to_file(peilgebieden_path_editted)


add_streefpeil(
    model=model,
    peilgebieden_path=peilgebieden_path_editted,
    layername=None,
    target_level="streefpeil",
    code="WS_PGID",
)

# migrate peilen
for basin_id, to_basin_ids in ((2016, [1963, 2015]), (1562, [1563, 1432])):
    model.basin.area.df.loc[model.basin.area.df.node_id.isin(to_basin_ids), "meta_streefpeil"] = (
        model.basin.area.df.set_index("node_id").at[basin_id, "meta_streefpeil"]
    )

    model.basin.area.df.loc[model.basin.area.df.node_id.isin(to_basin_ids), "meta_code_waterbeheerder"] = (
        model.basin.area.df.set_index("node_id").at[basin_id, "meta_code_waterbeheerder"]
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

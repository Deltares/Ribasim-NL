# %%
import geopandas as gpd
from ribasim_nl.assign_authorities import AssignAuthorities
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from ribasim_nl.parametrization.target_level import upstream_target_levels
from ribasim_nl.streefpeilen import add_streefpeil

from ribasim_nl import CloudStorage, Model, Network

cloud = CloudStorage()
authority = "AaenMaas"
short_name = "aam"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
parameters_dir.mkdir(parents=True, exist_ok=True)
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"

peilgebieden_path = cloud.joinpath(authority, "verwerkt/downloads/WS_PEILGEBIEDPolygon.shp")
stuwen_shp = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/Na_levering_20240418/stuwen.shp")
aam_data_gpkg = cloud.joinpath(authority, "verwerkt/2_voorbewerking/AanpassinghWh/20230530AaEnMaasData.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens/Top10NL/top10nl_Compleet.gpkg")
waterschap_grenzen = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
rws_waterschap_grenzen = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/Rijkswaterstaat.gpkg")
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name


##
network = Network(lines_gdf=gpd.read_file(aam_data_gpkg, layer="hydroobject", fid_as_index=True))
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(aam_data_gpkg, layer="profiellijn"),
    profile_point_df=gpd.read_file(aam_data_gpkg, layer="profielpunt"),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
)
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)

# fix link geometries and profiles
profiles_df = damo_profiles.process_profiles()
profiles_df.to_file(profiles_gpkg)
fix_link_geometries(model, network)
add_link_profile_ids(model, profiles=damo_profiles, id_col="globalid")
assert model.link.df is not None
model.link.df.reset_index().to_file(link_geometries_gpkg)
profiles_df.set_index("profiel_id", inplace=True)


# %%

# %%

# add streefpeilen
add_streefpeil(model=model, peilgebieden_path=peilgebieden_path, layername=None, target_level="ZOMERPEIL", code="CODE")

# %%
# OUTLET

# OUTLET.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Outlet")
assert static_data.outlet is not None
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.outlet.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Outlet", series=min_upstream_level)

# from DAMO WS_STREE_2
min_upstream_level = gpd.read_file(stuwen_shp).set_index("CODE")["WS_STREE_2"]
min_upstream_level = min_upstream_level[min_upstream_level != 0]
min_upstream_level.name = "min_upstream_level"
min_upstream_level.index.name = "code"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# from DAMO HOOGSTEDOO
hoogste = gpd.read_file(stuwen_shp).set_index("CODE")["HOOGSTEDOO"]
laagste = gpd.read_file(stuwen_shp).set_index("CODE")["LAAGSTEDOO"]
min_upstream_level = laagste + (hoogste - laagste) * 0.75
min_upstream_level.name = "min_upstream_level"
min_upstream_level.index.name = "code"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# from DAMO profiles
static_data.fill_min_upstream_level_from_profiles("Outlet", profiles_df)


# %%

# PUMP

# PUMP.min_upstream_level
# from basin streefpeil
static_data.reset_data_frame(node_type="Pump")
assert static_data.pump is not None
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.pump.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Pump", series=min_upstream_level)

# from DAMO profiles
static_data.fill_min_upstream_level_from_profiles("Pump", profiles_df)

## PUMP.flow_rate
gkw_gemaal_df = get_data_from_gkw(authority=authority, layers=["gemaal"])
flow_rate = gkw_gemaal_df.set_index("code")["maximalecapaciteit"] / 60  # m3/minuut to m3/s
flow_rate.name = "flow_rate"
# --- Gemaal Veluwe heeft een capaciteit van 1.5m3/s ---
static_data.reset_data_frame(node_type="Pump")  # zorgt dat static_data.pump up-to-date is
static_data.pump.loc[static_data.pump["node_id"] == 100, "flow_rate"] = 1.5

static_data.add_series(node_type="Pump", series=flow_rate)


# %%

# BASIN
static_data.reset_data_frame(node_type="Basin")

# fill streefpeil from ds min_upstream_level
static_data.fill_streefpeil_from_downstream_levels()

# get all nodata streefpeilen with their profile_ids and levels
static_data.fill_streefpeil_from_profiles(damo_profiles, profiles_df)

# handmatige correcties
forced_levels = {
    1273: 6.4,
    1849: 30.75,
    1496: 4.2,
    2006: 11.66,
    1550: 21.93,
    2011: 21.93,
    1689: 15,
    1406: 7.3,
    1149: 6.95,
    1465: 6.95,
    1517: 13,
    1190: 13.90,
    1341: 14.82,
    1922: 15,
    1275: 2.2,  ## Drongelens kanaal en Dieze op streefpeil 2.2m (WATAK)
    1743: 2.2,
    1879: 2.2,
    1801: 2.2,
    1950: 2.2,
    1394: 2.2,
    2016: 2.2,
    1627: 2.2,
    3096: 2.2,
}

assert static_data.basin is not None
mask = static_data.basin["node_id"].isin(forced_levels.keys())
static_data.basin.loc[mask, "streefpeil"] = static_data.basin.loc[mask, "node_id"].map(forced_levels)

# # update model basin-data
static_data.update_model_basin_levels()

# toevoegen aangrenzende autoriteiten
ws_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/Rijkswaterstaat.gpkg")
assign = AssignAuthorities(
    ribasim_model=model,
    waterschap=authority,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={9: "Rijkswaterstaat", 46: "DeDommel", 45: "DeDommel", 40: "Limburg"},
)
model = assign.assign_authorities()

# %%

# write
static_data.write()

model.write(ribasim_toml)

# %%

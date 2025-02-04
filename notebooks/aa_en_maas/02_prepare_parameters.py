# %%
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from ribasim_nl.parametrization.target_level import upstream_target_levels
from ribasim_nl.streefpeilen import add_streefpeil

cloud = CloudStorage()
authority = "AaenMaas"
short_name = "aam"


ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

static_data_xlsx = cloud.joinpath(
    authority,
    "verwerkt",
    "parameters",
    "static_data_template.xlsx",
)


stuwen_shp = cloud.joinpath(authority, "verwerkt", "1_ontvangen_data", "Na_levering_20240418", "stuwen.shp")
aam_data_gpkg = cloud.joinpath(authority, "verwerkt", "2_voorbewerking", "AanpassinghWh", "20230530AaEnMaasData.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")


model = Model.read(ribasim_toml)


# %%

# add streefpeilen
peilgebieden_path = cloud.joinpath(authority, "verwerkt/downloads/WS_PEILGEBIEDPolygon.shp")
cloud.synchronize(filepaths=[peilgebieden_path])

_ = add_streefpeil(
    model=model, peilgebieden_path=peilgebieden_path, layername=None, target_level="ZOMERPEIL", code="CODE"
)


# %%
# init static_data from model and defaults
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)

# %%
# init damo_profiles from a whole bunch of data
damo_profiles = DAMOProfiles(
    model=model,
    network=Network(lines_gdf=gpd.read_file(aam_data_gpkg, layer="hydroobject")),
    profile_line_df=gpd.read_file(aam_data_gpkg, layer="profiellijn"),
    profile_point_df=gpd.read_file(aam_data_gpkg, layer="profielpunt"),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
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

# from DAMO WS_STREE_2
min_upstream_level = gpd.read_file(stuwen_shp).set_index("CODE")["WS_STREE_2"]
min_upstream_level = min_upstream_level[min_upstream_level != 0]
min_upstream_level.name = "min_upstream_level"
min_upstream_level.index.name = "code"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# from DAMO HOOGSTEDOO
min_upstream_level = gpd.read_file(stuwen_shp).set_index("CODE")["HOOGSTEDOO"]
min_upstream_level.name = "min_upstream_level"
min_upstream_level.index.name = "code"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# from DAMO profiles
node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
levels = damo_profiles.upstream_levels(node_ids=node_ids)
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
levels = damo_profiles.upstream_levels(node_ids=node_ids)
min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Pump", series=min_upstream_level, fill_na=True)

## PUMP.flow_rate
gkw_gemaal_df = get_data_from_gkw(authority=authority, layers=["gemaal"])
flow_rate = gkw_gemaal_df.set_index("code")["maximalecapaciteit"] / 60  # m3/minuut to m3/s
flow_rate.name = "flow_rate"
static_data.add_series(node_type="Pump", series=flow_rate)


# %%

# write
static_data.write()
model.write(ribasim_toml)

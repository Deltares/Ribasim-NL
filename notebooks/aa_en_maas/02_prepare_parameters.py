# %%
import geopandas as gpd

from ribasim_nl import CloudStorage, Model
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
model = Model.read(ribasim_toml)


# %%

# add streefpeilen
peilgebieden_path = cloud.joinpath(authority, "verwerkt/downloads/WS_PEILGEBIEDPolygon.shp")
cloud.synchronize(filepaths=[peilgebieden_path])

_ = add_streefpeil(
    model=model, peilgebieden_path=peilgebieden_path, layername=None, target_level="ZOMERPEIL", code="CODE"
)


# %%

# build static_data.xlsx

# init static data from model and defaults
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)

# find min_upstream_level in stuwen
min_upstream_level = gpd.read_file(stuwen_shp).set_index("CODE")["WS_STREE_2"]
min_upstream_level = min_upstream_level[min_upstream_level != 0]
min_upstream_level.name = "min_upstream_level"
min_upstream_level.index.name = "code"
static_data.add_series(node_type="Outlet", series=min_upstream_level)

# find min_upstream_level in gemalen
static_data.reset_data_frame(node_type="Pump")
min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.pump.node_id)
min_upstream_level = min_upstream_level[min_upstream_level.notna()]
min_upstream_level.name = "min_upstream_level"
static_data.add_series(node_type="Pump", series=min_upstream_level)

static_data.write()
# %%

# write model

model.write(ribasim_toml)

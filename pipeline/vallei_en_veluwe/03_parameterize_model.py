# %%
import time

from ribasim_nl.controle_output import AFVOER_METRICS, Control
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    sync_min_upstream_levels_with_profile_bottoms,
)

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"
run_model = False

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

# you need the excel, but the model should be local-only by running 01_fix_model.py

# %%

# read
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
start_time = time.time()
# %%
# parameterize
series = model.basin.node.df["meta_categorie"]
uncategorized_basins = series[series.isna()].index.values
if len(uncategorized_basins) > 0:
    print(f"uncategorized basins: {uncategorized_basins}, will be set to doorgaand")
    model.node.df.loc[uncategorized_basins, "meta_categorie"] = "doorgaand"

model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")


assert model.basin.profile.df is not None
model.basin.profile.df.loc[
    (model.basin.profile.df.node_id == n("up:KDU-5871")) & (model.basin.profile.df.area > 0.1), "area"
] = 10000

basin_level_overrides = [
    ([n("up:KDU-3220")], -0.5),
    ([n("up:KGM-18")], 1.2),
    ([n("up:KST-1062")], 1.55),
    ([n("up:KGM-73")], 3.35),
]

apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

# Inlaat de Wenden
assert model.level_boundary.static.df is not None
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == n("bnd:1"), "level"] = 0.8


assert model.manning_resistance.static.df is not None
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03

# Havensluis Elburg
model.reverse_direction_at_node(n("KSL-10"))

# Inlaat Eektermerksluis
assert model.outlet.static.df is not None
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("KDU-3178"), "meta_code"] = "KSL-8"
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("KDU-3178"), "meta_name"] = "Eektermerksluis"


# %%
# Write model
assert model.basin.area.df is not None
model.basin.area.df.loc[:, "meta_area_m2"] = model.basin.area.df.area
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
sync_min_upstream_levels_with_profile_bottoms(model=model)
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run(metrics=AFVOER_METRICS)

# %%

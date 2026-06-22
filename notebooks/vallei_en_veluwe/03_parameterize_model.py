# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl.parametrization.basin_tables import sync_min_upstream_levels_with_profile_bottoms

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
cloud.synchronize(filepaths=[static_data_xlsx, qlr_path])

# %%

# read
model = Model.read(ribasim_toml)
start_time = time.time()
# %%
# parameterize
series = model.basin.node.df["meta_categorie"]
uncategorized_basins = series[series.isna()].index.values
if len(uncategorized_basins) > 0:
    print(f"uncategorized basins: {uncategorized_basins}, will be set to doorgaand")
    # pyrefly: ignore[missing-attribute]
    model.node.df.loc[uncategorized_basins, "meta_categorie"] = "doorgaand"

model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")


model.basin.profile.df.loc[(model.basin.profile.df.node_id == 1209) & (model.basin.profile.df.area > 0.1), "area"] = (
    10000
)

basin_level_overrides = [
    ([864], -0.5),
    ([1271], 1.2),
    ([786], 1.55),
    ([751], 3.35),
]

for node_ids, meta_streefpeil in basin_level_overrides:
    mask = model.basin.area.df.node_id.isin(node_ids)
    model.basin.area.df.loc[mask, "meta_streefpeil"] = meta_streefpeil

# Herbereken afgeleide tabellen na handmatige streefpeil-overrides.
model.basin.state.df = model.basin.area.df[["node_id", "meta_streefpeil"]].rename(columns={"meta_streefpeil": "level"})

# Inlaat de Wenden
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 1, "level"] = 0.8


model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03

# Havensluis Elburg
model.reverse_direction_at_node(477)

# Inlaat Eektermerksluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "meta_code"] = "KSL-8"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "meta_name"] = "Eektermerksluis"

# Inlaat gemaal de Haar
model.outlet.static.df.loc[model.outlet.static.df.node_id == 232, "meta_name"] = "Aanvoergemaal de Haar"


# %%
# Write model
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
    indicators = controle_output.run_afvoer()

# %%

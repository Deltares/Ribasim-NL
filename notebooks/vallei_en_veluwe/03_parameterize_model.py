# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"
run_model = False

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens\\QGIS_lyr\\output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"


# # you need the excel, but the model should be local-only by running 01_fix_model.py
# cloud.synchronize(filepaths=[static_data_xlsx])
# cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)


# %%

# read
model = Model.read(ribasim_toml)
start_time = time.time()

# %%


# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=10, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")

model.basin.profile.df.loc[(model.basin.profile.df.node_id == 1209) & (model.basin.profile.df.area > 0.1), "area"] = (
    10000
)
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 306, "min_upstream_level"] = 2.85
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 312, "min_upstream_level"] = 2.85
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 369, "min_upstream_level"] = 2.85
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 576, "min_upstream_level"] = 2.85
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 555, "min_upstream_level"] = 1.7
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 542, "min_upstream_level"] = 3.9
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 74, "flow_rate"] = 1
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 289, "min_upstream_level"] = 12
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "min_upstream_level"] = -0.75
# model.pump.static.df.loc[model.pump.static.df.node_id == 266, "min_upstream_level"] = 4.2
# model.basin.area.df.loc[model.basin.area.df.node_id == 1006, "meta_streefpeil"] = 3.9
# model.basin.area.df.loc[model.basin.area.df.node_id == 1028, "meta_streefpeil"] = 4.2

# %% Verbeter model

model.merge_basins(basin_id=1107, to_node_id=1134, are_connected=True)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 512, "min_upstream_level"] = -0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 512, "flow_rate"] = 0

model.outlet.static.df.loc[model.outlet.static.df.node_id == 497, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 497, ["meta_categorie"]] = "Inlaat"

model.outlet.static.df.loc[model.outlet.static.df.node_id == 67, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 67, ["meta_categorie"]] = "Inlaat"

model.outlet.static.df.loc[model.outlet.static.df.node_id == 67, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 67, ["meta_categorie"]] = "Inlaat"

model.outlet.static.df.loc[model.outlet.static.df.node_id == 512, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 512, ["meta_categorie"]] = "Inlaat"

model.outlet.static.df.loc[model.outlet.static.df.node_id == 530, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 530, ["meta_categorie"]] = "Inlaat"

model.outlet.static.df.loc[model.outlet.static.df.node_id == 381, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 381, ["meta_categorie"]] = "Inlaat"

model.outlet.static.df.loc[model.outlet.static.df.node_id == 366, "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 366, ["meta_categorie"]] = "Inlaat"

model.outlet.static.df.loc[model.outlet.static.df.node_id == 137, "min_upstream_level"] = -0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 137, "flow_rate"] = 0

model.pump.static.df.loc[model.pump.static.df.node_id == 232, ["flow_rate"]] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 232, ["meta_categorie"]] = "Inlaat"

model.pump.static.df.loc[model.pump.static.df.node_id == 466, ["flow_rate"]] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 466, ["meta_categorie"]] = "Inlaat"

model.pump.static.df.loc[model.pump.static.df.node_id == 264, ["flow_rate"]] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 264, ["meta_categorie"]] = "Inlaat"

model.outlet.static.df.loc[model.outlet.static.df.node_id == 288, ["meta_categorie"]] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 288, ["meta_name"]] = "Grebbesluis"

model.pump.static.df.loc[model.pump.static.df.node_id == 1282, ["flow_rate"]] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 1282, ["meta_categorie"]] = "Inlaat"

model.outlet.static.df.loc[model.outlet.static.df.node_id == 190, ["flow_rate"]] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 599, ["flow_rate"]] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 599, ["meta_categorie"]] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 269, ["flow_rate"]] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 269, ["meta_categorie"]] = "Inlaat"
model.pump.static.df.loc[model.pump.static.df.node_id == 268, ["flow_rate"]] = 2

model.pump.static.df.loc[model.pump.static.df.node_id == 231, ["flow_rate"]] = 5

model.outlet.static.df.loc[model.outlet.static.df.node_id == 634, ["active"]] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 634, ["meta_categorie"]] = "Inlaat"

model.manning_resistance.static.df.loc[:, "manning_n"] = 0.005
# %%

# Write model
add_check_basin_level(model=model)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    exit_code = model.run()
    assert exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()


# %%

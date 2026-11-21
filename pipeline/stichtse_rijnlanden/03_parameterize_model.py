# %%

import time

import geopandas as gpd
from ribasim_nl.controle_output import AFVOER_METRICS, Control
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    clear_ungated_outlet_levels,
    sync_min_upstream_levels_with_profile_bottoms,
)
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "StichtseRijnlanden"
short_name = "hdsr"
run_model = False
static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters/static_data.xlsx")
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")


# %%
# read
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")
start_time = time.time()

# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5)
print("Elapsed Time:", time.time() - start_time, "seconds")
assert model.manning_resistance.static.df is not None
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
# the sediment at open water connection OW575 resists more, see https://github.com/Deltares/Ribasim-NL/issues/912
(ow575,) = model.node.df.index[model.node.df["meta_code_waterbeheerder"] == "OW575"]
assert model.get_node_type(ow575) == "ManningResistance"
model.manning_resistance.static.df.loc[model.manning_resistance.static.df.node_id == ow575, "manning_n"] = 0.04
sync_parameterized_manning_basin_levels(
    model=model,
    aanvoergebieden_df=aanvoergebieden_df,
    output_gpkg=cloud.joinpath(
        authority,
        "modellen",
        f"{authority}_parameterized_model",
        "manning_level_basin_updates.gpkg",
    ),
)

# Fixes
basin_level_overrides = [
    ([n("up:OW571")], -0.4),
    ([n("loc:135314,450583")], 0.52),
    ([n("up:G4060")], -2.08),  # Wulverhorst
    ([n("up:G4032")], 0.0),
    ([n("up:H041347___1")], -0.48),
    ([n("up:H059371")], 0.52),
    ([n("up:I0543")], 0.52),
    ([n("up:H071910___7")], 0.52),
    ([n("up:I6206")], -2.22),
    ([n("up:H055539___9")], -1.55),
    ([n("up:I0002")], -1.55),
    ([n("up:H055539___23")], -1.55),
    ([n("up:H055539___34")], -1.55),
    ([n("up:H055539___41")], -1.55),
    ([n("up:OW637")], -2.22),
    ([n("up:ST0769")], -2.22),
    ([n("up:H078195")], -2.22),
    ([n("up:H058524___12")], -2.22),
    ([n("up:ST5008")], -2.1),
    ([n("up:I1111")], -0.1),  # check streefpeil!
    ([n("up:ST0441")], -2.35),  # check streefpeil!
    ([n("up:ST2072")], -0.25),  # check streefpeil!
    ([n("up:I1352")], 1.85),  # check streefpeil!
]

# stadswater Utrecht beneden peil
node_ids = [
    n("up:I6078"),
    n("up:OW546"),
    n("up:G3901"),
    n("down:OW701"),
    n("up:G1006"),
    n("up:I1580"),
    n("up:I6073"),
    n("up:OW548"),
    n("up:SL0014"),
    n("up:OW554"),
    n("up:H071436___3"),
    n("up:H072046"),
    n("up:G0009"),
    n("up:G0052"),
    n("up:H012363___0"),
    n("up:H012352___1"),
]
basin_level_overrides.append((node_ids, 0.58))
apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

# %%
# Write model
clear_ungated_outlet_levels(model)

sync_min_upstream_levels_with_profile_bottoms(model=model)
assert model.basin.area.df is not None
model.basin.area.df.loc[:, "meta_area_m2"] = model.basin.area.df.area
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run(metrics=AFVOER_METRICS)
# %%

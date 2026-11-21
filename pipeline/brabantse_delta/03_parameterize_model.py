# %%
import time

import geopandas as gpd
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.controle_output import AFVOER_METRICS, Control
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    sync_min_upstream_levels_with_profile_bottoms,
)
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "BrabantseDelta"
short_name = "wbd"

run_model = False

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py

# %%

# read
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")
start_time = time.time()

# %% fixes basins and profiles

basin_level_overrides = [
    ([n("up:KST02510")], 4.1),
    ([n("up:OVK21946")], 10),
    (
        [
            n("up:KST01228"),
            n("up:KDU08584"),
            n("up:KDU30006"),
            n("up:OVK09180___1"),
            n("up:ow:OVK07158"),
            n("up:ow:OVK09178"),
            n("up:ow:OVK11744-0"),
            n("up:ow:OVK02864"),
            n("up:ow:OVK12137"),
            n("up:KDU60008"),
            n("up:KDU02941"),
            n("up:ow:OVK03088"),
            n("up:OVK06046-0___4"),
            n("up:ow:OVK04866"),
            n("up:KDU03212"),
            n("up:GEM_KGM00389"),
            n("up:KDU10014"),
            n("up:GEM_KGM00385"),
            n("up:KST02969"),
            n("up:ow:OVK12144"),
            n("up:KDU03371"),
            n("up:KDU04571"),
            n("up:ow:OVK06098"),
            n("up:ow:OVK12132-1"),
            n("up:ow:OVK10756-0"),
            n("up:KDU09522"),
            n("up:KDU02968"),
            n("up:KDU17027"),
            n("up:KDU14657"),
            n("up:KDU60018"),
            n("up:ow:OVK09179"),
            n("up:ow:OVK09177-1"),
            n("up:ow:OVK07820"),
            n("up:KST01227"),
            n("up:ow:OVK09174"),
            n("up:ow:OVK02446"),
            n("up:KDU19784"),
            n("up:KDU03223"),
            n("up:OVK08441-0___1"),
            n("up:ow:OVK07159"),
            n("up:KDU03118"),
            n("up:KDU19775"),
            n("up:ow:OVK04862-2"),
            n("up:ow:OVK07817"),
            n("up:OVK12134"),
            n("up:KDU03252"),
            n("up:ow:OVK09397"),
            n("up:OVK04661___3"),
            n("up:ow:OVK99982"),
            n("up:OVK04863-0___4"),
            n("up:OVK04863-1___2"),
            n("up:KDU19778"),
            n("up:OVK06593-0___2"),
            n("up:ow:OVK06644-1"),
            n("up:ow:OVK07160-1"),
            n("up:KDU80099"),
            n("up:ow:OVK10756-2"),
            n("up:ow:OVK99999"),
            n("up:OVK06052"),
            n("up:OVK06601"),
            n("up:ow:OVK06592-0"),
        ],
        0.0,
    ),
]

model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")

protected_basin_node_ids = apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

assert model.manning_resistance.static.df is not None
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
sync_parameterized_manning_basin_levels(
    model=model,
    aanvoergebieden_df=aanvoergebieden_df,
    output_gpkg=cloud.joinpath(
        authority,
        "modellen",
        f"{authority}_parameterized_model",
        "manning_level_basin_updates.gpkg",
    ),
    protected_basin_node_ids=protected_basin_node_ids,
)

model.update_node(node_id=n("ow:OVK04500"), node_type="outlet")
model.update_node(node_id=n("KST02130"), node_type="pump")
sync_min_upstream_levels_with_profile_bottoms(model=model)
# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
add_check_basin_level(model=model)
assert model.basin.area.df is not None
model.basin.area.df.loc[:, "meta_area"] = model.basin.area.df.area
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

    # # %%
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run(metrics=AFVOER_METRICS)
# %%

# %%
import time

import geopandas as gpd
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.controle_output import AFVOER_METRICS, Control
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    clear_ungated_outlet_levels,
    sync_min_upstream_levels_with_profile_bottoms,
)
from ribasim_nl.parametrization.level_boundary_table import update_level_boundary_static
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "Vechtstromen"
short_name = "vechtstromen"
run_model = False
run_period = None
static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters/static_data.xlsx")
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
parameterized_model_dir = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")


# %%
# read
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id

start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=1)
print("Elapsed Time:", time.time() - start_time, "seconds")
assert model.manning_resistance.static.df is not None
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


# %%
assert model.outlet.static.df is not None
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("down:ST04531"), ["meta_categorie"]] = "Inlaat"

basin_level_overrides = [
    ([n("up:ST01973"), n("up:ow:WL01181-5"), n("up:ow:WL01052-11")], 10),
    ([n("up:ST04058")], 10.1),
    ([n("up:SY00177")], 6.8),
    ([n("up:SY00239")], 5.7),
    ([n("up:ow:WL01711-4")], 10.22),
    ([n("up:DK25523"), n("up:GM00159"), n("up:DK26830")], 5.75),
    ([n("up:ST03198"), n("up:DK17294")], 10),
    ([n("up:ST03211")], 17.7),
    ([n("up:ow:WL03325-10"), n("up:ST03360")], 10.5),
    ([n("ow:WL01863-18"), n("up:WL01959-4"), n("up:ow:WL01959-14"), n("up:GM00224")], 7.35),
    (
        [
            n("up:ow:WL02102-15"),
            n("up:WL02311"),
            n("up:ow:WL01959-75"),
            n("up:ow:WL01318-9"),
            n("up:ow:WL02056-18"),
            n("up:ow:WL02036-3"),
            n("up:ST01592"),
            n("up:ow:WL01318-10"),
            n("up:ST01689"),
        ],
        4.39,
    ),
    (
        [
            n("up:WL01698-3_4_inlaat"),
            n("up:ST04918"),
            n("up:ow:WL01863-9"),
            n("up:ST02355"),
            n("up:DK30351_BE"),
            n("up:ow:WL01698-3_2"),
        ],
        8.35,
    ),
    ([n("up:ow:WL01863-18"), n("up:DK00370"), n("up:ow:WL01803-3"), n("up:ow:WL01764-19"), n("up:ST04870")], 7.35),
    ([n("up:ow:WL01909-0"), n("up:ow:WL01909-9")], 2.65),
    ([n("up:WL01925-3"), n("up:ST00203")], 3.73),
    (
        [
            n("up:ST03541"),
            n("up:ow:WL03046-5"),
            n("up:DK17873"),
            n("up:ow:WL02747-4"),
            n("up:DK22015"),
            n("up:GM00191"),
            n("up:DK04366"),
        ],
        9.1,
    ),
    ([n("up:ow:WL02926-4"), n("up:SL00013"), n("up:ow:WL02747-10"), n("up:DK31883")], 7.1),
    ([n("up:ow:WL02747-25"), n("up:SL00018")], 5.60),
    ([n("up:SL00017")], 4.50),
    ([n("up:ow:WL02747-62"), n("up:ow:WL04956-0")], 2.65),
    ([n("up:DK21438")], 11.2),
    ([n("up:DK20656"), n("up:ST03026"), n("up:DK25702"), n("up:ow:WL03676-1")], 12.95),
    ([n("up:DK25442"), n("up:ow:WL03440-7"), n("up:DK25518"), n("up:SL00004")], 17.7),
    ([n("loc:238984,496571")], 9.7),
    ([n("up:ow:WL03774-2-0")], 17.7),
]
protected_basin_node_ids = apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

sync_parameterized_manning_basin_levels(
    model=model,
    aanvoergebieden_df=aanvoergebieden_df,
    output_gpkg=parameterized_model_dir / "manning_level_basin_updates.gpkg",
    protected_basin_node_ids=protected_basin_node_ids,
)

update_level_boundary_static(
    model=model,
    static_data_xlsx=static_data_xlsx,
    code_column="meta_code_waterbeheerder",
)

# Outlet 704 ligt benedenstrooms van basin 1545; gebruik het streefpeil van dat basin als drempelpeil.
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("DK22755"), "min_upstream_level"] = 10.5


# %%
clear_ungated_outlet_levels(model)
sync_min_upstream_levels_with_profile_bottoms(model=model)

# %%
# Write model
ribasim_toml = parameterized_model_dir / f"{short_name}.toml"
add_check_basin_level(model=model)
model.write(ribasim_toml)

# %%

# run model
if run_model:
    if run_period is not None:
        model.endtime = model.starttime + run_period
        model.write(ribasim_toml)
    result = model.run()
    assert result.exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run(metrics=AFVOER_METRICS)
# %%

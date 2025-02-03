# %%

from ribasim_nl import CloudStorage, Model
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from ribasim_nl.streefpeilen import add_streefpeil

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"


ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

model = Model.read(ribasim_toml)


# %%

# add streefpeilen
peilgebieden_path = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/Eerste_levering/vallei_en_veluwe.gpkg")
cloud.synchronize(filepaths=[peilgebieden_path])

_ = add_streefpeil(
    model=model,
    peilgebieden_path=peilgebieden_path,
    layername="peilgebiedpraktijk",
    target_level="ws_min_peil",
    code="code",
)


# %%

# build static_data.xlsx

# defaults
static_data_xlsx = cloud.joinpath(
    authority,
    "verwerkt",
    "parameters",
    "static_data_template.xlsx",
)

static_data = StaticData(model=model, xlsx_path=static_data_xlsx)
static_data.write()

# write model
model.write(ribasim_toml)

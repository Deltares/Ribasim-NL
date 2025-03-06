# %%


from ribasim_nl import CloudStorage, Model
from ribasim_nl.parametrization.static_data_xlsx import StaticData

cloud = CloudStorage()
authority = "Noorderzijlvest"
short_name = "nzv"

fix_link_geoms = False

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name


# %%
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

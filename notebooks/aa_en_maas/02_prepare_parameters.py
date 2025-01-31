# %%
import pandas as pd

from ribasim_nl import CloudStorage, Model
from ribasim_nl.streefpeilen import add_streefpeil

cloud = CloudStorage()
authority = "AaenMaas"
short_name = "aam"


ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

model = Model.read(ribasim_toml)


# %%

# add streefpeilen
peilgebieden_path = cloud.joinpath(authority, "verwerkt/downloads/WS_PEILGEBIEDPolygon.shp")
cloud.synchronize(filepaths=[peilgebieden_path])

add_streefpeil(model=model, peilgebieden_path=peilgebieden_path, layername=None, target_level="ZOMERPEIL", code="CODE")


# %%

# build static_data.xlsx

# defaults
defaults = {
    "Afvoergemaal": {
        "upstream_level_offset": 0.0,
        "downstream_level_offset": 0.2,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 15,
        "function": "outlet",
    },
    "Aanvoergemaal": {
        "upstream_level_offset": 0.2,
        "downstream_level_offset": 0.0,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 4,
        "function": "inlet",
    },
    "Uitlaat": {
        "upstream_level_offset": 0.0,
        "downstream_level_offset": 0.3,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 50,
        "function": "outlet",
    },
    "Inlaat": {
        "upstream_level_offset": 0.2,
        "downstream_level_offset": 0.0,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 4,
        "function": "inlet",
    },
}

defaults_df = pd.DataFrame.from_dict(defaults, orient="index")
defaults_df.index.name = "categorie"


static_data_xlsx = cloud.joinpath(
    authority,
    "verwerkt",
    "parameters",
    "static_data_template.xlsx",
)
static_data_xlsx.parent.mkdir(exist_ok=True)

# %%

# write

model.write(ribasim_toml)
defaults_df.to_excel(static_data_xlsx, sheet_name="defaults")

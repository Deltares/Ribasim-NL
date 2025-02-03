# %%
import pandas as pd

from ribasim_nl import CloudStorage, Model
from ribasim_nl.parametrization.empty_table import empty_table_df
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
if static_data_xlsx.exists():
    static_data_xlsx.unlink()
else:
    static_data_xlsx.parent.mkdir(exist_ok=True, parents=True)

defaults_df.to_excel(static_data_xlsx, sheet_name="defaults")

with pd.ExcelWriter(static_data_xlsx, mode="a", if_sheet_exists="replace") as xlsx_writer:
    # Pump, Outlet
    columns = ["node_id", "name", "code", "flow_rate", "min_upstream_level", "max_downstream_level"]
    extra_columns = ["categorie", "opmerking_waterbeheerder"]
    for node_type in ["Pump", "Outlet"]:
        df = empty_table_df(
            model=model, table_type="Static", node_type=node_type, meta_columns=["meta_code_waterbeheerder", "name"]
        )
        df.rename(columns={"meta_code_waterbeheerder": "code"}, inplace=True)
        if node_type == "Pump":
            df["categorie"] = "Afvoergemaal"
        if node_type == "Outlet":
            df["categorie"] = "Uitlaat"
        df["opmerking_waterbeheerder"] = ""
        df[columns + extra_columns].to_excel(xlsx_writer, sheet_name=node_type, index=False)
# %%

# write model

model.write(ribasim_toml)

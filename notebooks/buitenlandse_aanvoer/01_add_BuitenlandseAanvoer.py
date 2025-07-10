# %% Imports
import logging
from datetime import datetime, timedelta

import pandas as pd
import ribasim
from ribasim.nodes import flow_boundary

from ribasim_nl import CloudStorage, Model

# print("ribasim:", ribasim.__file__)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", force=True)

# %%
cloud = CloudStorage()
readme = f"""# Model van (deel)gebieden uit het Landelijk Hydrologisch Model inclusief Buitenlandse aanvoeren

Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""

logging.info(readme)

# %%  Laad het model in

authority = "Rijkswaterstaat"
ribasim_dir = cloud.joinpath(authority, "modellen", "lhm_vrij_coupled_2025_6_1")
ribasim_toml = ribasim_dir / "lhm.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
model = Model.read(ribasim_toml)

start_time = pd.to_datetime("2017-01-01")
stop_time = pd.to_datetime("2018-01-01")
flowboundaries = model.flow_boundary.node.df.name
logging.info(f"Alle flow boundaries in het model: {flowboundaries}")

# %%

# data_path = cloud.joinpath("Basisgegevens", "BuitenlandseAanvoer","verwerkt", "BuitenlandseAanvoer.xlsx")
# if not data_path.exists():
#     logging.info(f"Downloaden data: {data_path}")
#     url = cloud.joinurl("Basisgegevens", "BuitenlandseAanvoer","verwerkt", "BuitenlandseAanvoer.xlsx")#model_spec["authority"], "modellen", model_version.path_string)
#     cloud.download_content(url)
#     print('downloaded the data')

BA_data_path = r"c:\projects\2024\LWKM\01_data\Buitenlandse aanvoer\BuitenlandseAanvoer_V2.xlsx"


# Functie die 24:00 naar 00:00 zet
def fix_date_string(date_str):
    try:
        if isinstance(date_str, str) and "24:00" in date_str:
            base_date = datetime.strptime(date_str.split(" ")[0], "%d-%m-%Y")
            return base_date + timedelta(days=1)
        return pd.to_datetime(date_str)
    except Exception as e:
        print(f"Date parse error: {date_str} -> {e}")
        return pd.NaT


# Laad de Excel sheet in
xls = pd.ExcelFile(BA_data_path)
sheet_names = xls.sheet_names

filtered_dfs = []

for sheet in sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet)

    if "Datum" in df.columns:
        df["Datum"] = df["Datum"].apply(fix_date_string)
        df = df.dropna(subset=["Datum"])  # Drop rows with unparseable dates
        df.set_index("Datum", inplace=True)  # Make sure "Datum" is the index
        df_filtered = df[(df.index >= start_time) & (df.index <= stop_time)]
        df_daily = df_filtered.resample("D").mean()
        filtered_dfs.append(df_daily)

# Concat om één df te krijgen
combined_df = pd.concat(filtered_dfs, axis=0)
combined_df = combined_df.groupby("Datum").mean(numeric_only=True)
# buitenlandse_aanvoer_df = combined_df[combined_df.columns.intersection(flowboundaries)]
buitenlandse_aanvoer_df = combined_df.loc[:, combined_df.columns.intersection(flowboundaries)]
buitenlandse_aanvoer_df.index.name = "time"

# Interpoleer waar data mist
buitenlandse_aanvoer_df = buitenlandse_aanvoer_df.interpolate(method="time")

# Extrapoleer aan de randen
#     - forward-fill takes the last known value and projects it forward
#     - back-fill takes the first known value and projects it backward
buitenlandse_aanvoer_df = buitenlandse_aanvoer_df.ffill().bfill()

# Sanity check
assert not buitenlandse_aanvoer_df.isna().any().any(), "there are nan values!"

# Sla op in dictionary
dict_BA = {}
for loc in buitenlandse_aanvoer_df.columns:
    df_single = buitenlandse_aanvoer_df[[loc]].copy()
    df_single.columns = ["flow_rate"]
    dict_BA[loc] = df_single.reset_index()

for loc in dict_BA.keys():
    try:
        node_id = model.flow_boundary.node.df.reset_index(drop=False).set_index("name").at[loc, "node_id"]
        dict_BA[loc]["node_id"] = node_id

    except KeyError:
        print(f"Warning: '{loc}' not found in model.flow_boundary.node.df")
# BA_locaties = pd.DataFrame(rows)
# print(dict_BA)
logging.info(f"Dictionary aangemaakt met de buitenlandse aanvoeren: {dict_BA}")


# %% Voeg de flowboundaries toe aan het model
flowboundaries_time_df = pd.concat(dict_BA.values(), axis=0)
model.flow_boundary.time = flow_boundary.Time(**flowboundaries_time_df.to_dict(orient="list"))


# #TODO: moeten de fixed values van static verwijderd worden?
# model.flow_boundary.static.df = model.flow_boundary.static.df[
#     ~model.flow_boundary.static.df.node_id.isin(bc_time_df.node_id.unique())
# ]

# %% Sla de modellen op

# TODO: Bepaal de goede locaties en naamgeving voor de modellen
logging.info("Sla het model met Buitenlandse Aanvoeren op")
# model.write(ribasim_toml)
ribasim_toml = cloud.joinpath("Basisgegevens", "BuitenlandseAanvoer", "modellen", "BA_totaal_run", "BA.toml")
model.write(ribasim_toml)
cloud.upload_model("Basisgegevens/BuitenlandseAanvoer", model="BA_totaal_run")

# model.run()
# upload_model = True

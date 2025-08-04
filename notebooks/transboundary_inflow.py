# %% Imports
import logging
from datetime import datetime, timedelta

import pandas as pd
import ribasim
from ribasim.nodes import flow_boundary

from ribasim_nl import CloudStorage, Model

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", force=True)
upload_model = False

# %%
readme = f"""# Model van (deel)gebieden uit het Landelijk Hydrologisch Model inclusief Buitenlandse aanvoeren

Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""

logging.info(readme)
cloud = CloudStorage()
authority = "Rijkswaterstaat"

# Download the Ribasim model
model_name = "lhm_vrij_coupled_2025_6_1"
url = cloud.joinurl("Rijkswaterstaat", "modellen", model_name)
cloud.download_content(url)
ribasim_toml = cloud.joinpath(authority, "modellen", model_name, "lhm.toml")

database_gpkg = ribasim_toml.with_name("database.gpkg")
model = Model.read(ribasim_toml)

start_time = pd.to_datetime("2017-01-01")
stop_time = pd.to_datetime("2018-01-01")
flowboundaries = model.flow_boundary.node.df.name
logging.info(f"Alle flow boundaries in het model: {flowboundaries}")

BA_data_path = cloud.joinpath("Basisgegevens", "BuitenlandseAanvoer", "aangeleverd", "BuitenlandseAanvoer_V5.xlsx")
cloud.synchronize(filepaths=[BA_data_path])

# %%
# Importeer de door waterschappen aangeleverde buitenlandse aanvoeren
# TODO: voor de overige locaties moeten we de buitelandse aanvoeren gaan bepalen op basis van oppervlak van het grensoverschrijdende afvoergebied


# Functie die 24:00 naar 00:00 zet
def fix_date_string(date_str):
    """
    Corrigeert datums die de waarde '24:00' bevatten en parseert overige datums met dag-maand-jaar volgorde.

    Parameters
    ----------
    date_str : str of pd.Timestamp
        Datum als string of timestamp, bijvoorbeeld '23-06-2017 24:00'.

    Returns
    -------
    pd.Timestamp of pd.NaT
        Een geldige datetime-waarde. Als parsing faalt, wordt NaT teruggegeven.
    """
    try:
        if isinstance(date_str, str) and "24:00" in date_str:
            # Extract the date part
            base_date = datetime.strptime(date_str.split(" ")[0], "%d-%m-%Y")
            # Add one day, time will be 00:00 of the next day
            return pd.Timestamp(base_date + timedelta(days=1))
        # Use dayfirst=True to parse dates like "23-06-2017"
        return pd.to_datetime(date_str, dayfirst=True)
    except Exception as e:
        logging.warning(f"Date parse error: '{date_str}' -> {e}")
        return pd.NaT


def importeer_buitenlandse_aanvoer(BA_data_path, start_time, stop_time, flowboundaries, model):
    """
    Importeer buitenlandse aanvoer uit een Excelbestand.

    Filter de gewenste datums, maakt dagelijkse gemiddelden, interpoleert ontbrekende waarden
    en koppelt aan flow boundary nodes uit het model.

    Parameters
    ----------
    BA_data_path : Path naar het Excelbestand met buitenlandse aanvoer.
    start_time : Startdatum van het model (pd.Timestamp)
    stop_time : Einddatum van het modelinterval (pd.Timestamp)
    flowboundaries : Serie met namen als index en node_ids als waarden.
    model : Modelobject met `flow_boundary.node.df` attribuut voor node-koppeling.

    Returns
    -------
    dict
        Dictionary met per locatie een dataframe met tijdreeksen en node_id.
    """
    xls = pd.ExcelFile(BA_data_path)
    sheet_names = xls.sheet_names

    df_BA_raw_data = []

    for sheet in sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        if "Datum" in df.columns:
            logging.info(f"Importeer de data voor: {', '.join(df.columns[1:])}")
            df["Datum"] = df["Datum"].apply(fix_date_string)
            df = df.dropna(subset=["Datum"])
            df.set_index("Datum", inplace=True)
            df_filtered = df[(df.index >= start_time) & (df.index <= stop_time)].copy()
            df_numeric = df_filtered.apply(pd.to_numeric, errors="coerce")
            df_daily = df_numeric.resample("D").mean()
            df_BA_raw_data.append(df_daily)

    # Voeg de data samen
    df_combined_BA = pd.concat(df_BA_raw_data, axis=0)
    df_combined_BA = df_combined_BA.groupby("Datum").mean(numeric_only=True)

    # Filter columns that exist in flowboundaries
    available_columns = [col for col in df_combined_BA.columns if col in flowboundaries.values]
    df_buitenlandse_aanvoer = df_combined_BA[available_columns].copy()
    df_buitenlandse_aanvoer.index.name = "time"

    # Interpoleer om missende data te vullen
    df_buitenlandse_aanvoer = df_buitenlandse_aanvoer.interpolate(method="time")
    # Extrapoleer naar de begin en eind data
    df_buitenlandse_aanvoer = df_buitenlandse_aanvoer.ffill().bfill()

    # In het geval van geen data in de gemodelleerde periode: verander afvoer naar 0
    # TODO: netter om een gemiddelde waarde te gebruiken uit de totale data serie
    cols_with_nan = df_buitenlandse_aanvoer.columns[df_buitenlandse_aanvoer.isna().any()]
    for col in cols_with_nan:
        logging.warning(f"Column '{col}' has no measurements during the modelled interval; filling NaNs with 0.")
        df_buitenlandse_aanvoer[col].fillna(0, inplace=True)
    # Controleer op NaN values
    assert not df_buitenlandse_aanvoer.isna().any().any(), "There are NaN values remaining!"

    # Omzetten naar dictionary
    dict_BA = {}
    for loc in df_buitenlandse_aanvoer.columns:
        df_single = df_buitenlandse_aanvoer[[loc]].copy()
        df_single.columns = ["flow_rate"]
        dict_BA[loc] = df_single.reset_index()

    # Add node_id to each location and filter out locations not found in model
    locations_to_remove = []
    for loc in dict_BA.keys():
        try:
            node_id = model.flow_boundary.node.df.reset_index(drop=False).set_index("name").at[loc, "node_id"]
            dict_BA[loc]["node_id"] = node_id
        except KeyError:
            logging.warning(f"Warning: '{loc}' not found in model.flow_boundary.node.df. Will be removed from dict_BA.")
            locations_to_remove.append(loc)

    # Remove locations that were not found in the model
    for loc in locations_to_remove:
        dict_BA.pop(loc, None)

    logging.info(f"Dictionary aangemaakt met de buitenlandse aanvoeren: {dict_BA}")
    return dict_BA


dict_BA = importeer_buitenlandse_aanvoer(BA_data_path, start_time, stop_time, flowboundaries, model)

# %% Voeg de flowboundaries toe aan het model en sla het model op
df_flowboundaries_time = pd.concat(dict_BA.values(), axis=0)
# Convert to dictionary with proper type casting
flow_data_dict = df_flowboundaries_time.to_dict(orient="list")
# Ensure all values are properly typed for the flow_boundary.Time constructor
model.flow_boundary.time = flow_boundary.Time(**flow_data_dict)
included_node_ids = df_flowboundaries_time.node_id.unique()
included_names = flowboundaries[flowboundaries.index.isin(included_node_ids)].tolist()
logging.info(f"Flowboundaries included in data: {', '.join(included_names)}")

# TODO: vraag: moeten de fixed values van static verwijderd worden?
# model.flow_boundary.static.df = model.flow_boundary.static.df[
#     ~model.flow_boundary.static.df.node_id.isin(bc_time_df.node_id.unique())
# ]

# TODO: Bepaal de goede locaties en naamgeving voor de modellen
ribasim_toml = cloud.joinpath("Basisgegevens", "BuitenlandseAanvoer", "modellen", "BA_totaal_run", "BA.toml")
model.write(ribasim_toml)
if upload_model:
    logging.info("Upload het model met Buitenlandse Aanvoeren")
    cloud.upload_model("Basisgegevens/BuitenlandseAanvoer", model="BA_totaal_run")

# model.run()

# %%

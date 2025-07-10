"""
Created on Mon Apr  7 11:03:05 2025

@author: kingma
"""

# %%
import logging
import os
from collections import Counter
from datetime import datetime

import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyodbc
import ribasim
from ribasim import Model, Node
from ribasim.nodes import flow_boundary
from shapely.geometry import Point

from ribasim_nl import CloudStorage  # Model

# %%
cloud = CloudStorage()
ribasim_toml = cloud.joinpath("Basisgegevens", "RWZI", "modellen", "rwzi", "rwzi.toml")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# datafiles
model_dir = cloud.joinpath("Basisgegevens", "RWZI", "modellen")
root_path_local = cloud.joinpath("Basisgegevens", "RWZI")

ribasim_path = r"c:/projects/2024/LWKM/06_Ribasim/00_scripts/ribasim_applicatie/ribasim.exe"

zinfo_influentdebieten_path = os.path.join(
    root_path_local,
    r"aangeleverd/Z-info/metingen/zinfo_20160101_20231231_influentdebieten.csv",
)

db_file = os.path.join(root_path_local, r"aangeleverd/RwziBase/RwziBase2022_RWS_18032024.accdb")
rwzi_ligging_path = os.path.join(root_path_local, r"aangeleverd/locaties/RWZI_coordinates.geojson")

# model settings
starttime = "1991-01-01"
endtime = "2018-01-01"
time_range = pd.date_range(start=starttime, end=endtime, freq="D")
logging.info(f"Setting up Ribasim-RWZI model between {starttime} and {endtime} in {model_dir}.")

model = Model(
    starttime=starttime,
    endtime=endtime,
    crs="EPSG:28992",
)


# %% Laad Z-info data in
def process_zinfo_quantity_data(file_path, starttime, endtime):
    """
    Lees de Z-info afvoerdata in en bewaar in een dataframe.

    Parameters
    ----------
        file_path (str): csv file met de z-info influent debieten
        starttime (str or datetime): start datum
        endtime (str or datetime): eind datum

    Returns
    -------
        df_Zinfo_Q_pivot (DataFrame): dataframe met Z-info debieten per RWZI over de geselecteerde tijdspan.
        RWZI_ids_zinfo (ndarray): Unieke RWZI ID's in de Z-info dataset
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The CSV file does not exist: {file_path}")

    df = pd.read_csv(file_path, sep=",")
    df = df.drop(columns=["org", "ehd"]).rename(columns={"ist": "RWZI_ids", "d": "time", "debiet": "debiet_m3d"})
    df = df[(df["time"] >= starttime) & (df["time"] <= endtime)]
    df["time"] = pd.to_datetime(df["time"])

    duplicates = df[df.duplicated(subset=["time", "RWZI_ids"], keep=False)]
    if not duplicates.empty:
        logging.info(
            "Duplicate entries found for RWZI: %s",
            duplicates.sort_values(by=["time", "RWZI_ids"])["RWZI_ids"].unique(),
        )
        print("Duplicate entries found for RWZI:")
        print(duplicates.sort_values(by=["time", "RWZI_ids"])["RWZI_ids"].unique())

    df_summed = df.groupby(["time", "RWZI_ids"])["debiet_m3d"].sum().reset_index()
    logging.info("Duplicates are summed over the day \n")

    df_pivot = df_summed.pivot(index="time", columns="RWZI_ids", values="debiet_m3d")

    # Change unit to m3/s
    df_pivot_m3s = df_pivot / (3600 * 24)
    df_pivot_m3s.reset_index(inplace=True)

    logging.info("Zinfo data: \n %s", df_pivot_m3s.head())

    RWZI_ids = df_summed["RWZI_ids"].unique()
    return df_pivot_m3s, RWZI_ids


df_Zinfo_influentdebieten, RWZI_ids_zinfo = process_zinfo_quantity_data(zinfo_influentdebieten_path, starttime, endtime)


# %% Locaties van de RWZI's in Nederland
def load_rwzi_geodata(filepath):
    """
    Loads RWZI spatial data from a GeoJSON, Shapefile, or other supported format.

    Parameters
    ----------
        filepath (str): Path to the spatial file.

    Returns
    -------
        GeoDataFrame: Loaded RWZI spatial data.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"RWZI spatial file not found: {filepath}")

    gdf = gpd.read_file(filepath)
    logging.info(f"Loaded RWZI GeoDataFrame with {len(gdf)} records from {filepath}")
    return gdf


rwzi_gdf = load_rwzi_geodata(rwzi_ligging_path)

# Plot alle RWZI's
rwzi_gdf = rwzi_gdf.to_crs(epsg=3857)
fig, ax = plt.subplots(figsize=(5, 6))
rwzi_gdf.plot(ax=ax, color="dodgerblue", edgecolor="black", alpha=0.8, markersize=20)
ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)
ax.set_title("RWZI locaties in Nederland", fontsize=8)
ax.axis("off")

xmin, ymin, xmax, ymax = rwzi_gdf.total_bounds
xmargin = (xmax - xmin) * 0.05
ymargin = (ymax - ymin) * 0.05

ax.set_xlim(xmin - xmargin, xmax + xmargin)
ax.set_ylim(ymin - ymargin, ymax + ymargin)

plt.tight_layout()
plt.show()


# %%
def process_rwzi_zinfo_data(rwzi_gdf, RWZI_ids_zinfo, df_Zinfo_influentdebieten):
    """
    Combineer de Z-info data met de spatial data om de naam van de RWZI te achterhalen (gebaseerd op de ID)

    en om de RWZI's te vinden die niet in de Z-info database opgenomen zijn.

    Parameters
    ----------
    - rwzi_gdf: rwzi spatial data
    - RWZI_ids_zinfo: rwzi meta data
    - df_Zinfo_influentdebieten: rwzi debieten

    Returns
    -------
    - rwzi_flow_data (pd.DataFrame): Filtered DataFrame with renamed RWZI columns and time
    """
    # Extract all unique RWZI names
    rwzi_all_names = rwzi_gdf["Naam rwzi"].unique()
    logging.info(f"Total unique RWZI names found: {len(rwzi_all_names)}")

    # Split RWZI GeoDataFrame into included and excluded based on Zinfo
    gdf_rwzi_zinfo_incl = rwzi_gdf[rwzi_gdf["Codeist"].isin(RWZI_ids_zinfo)]
    gdf_rwzi_zinfo_excl = rwzi_gdf[~rwzi_gdf["Codeist"].isin(RWZI_ids_zinfo)]

    # Get included/excluded names and codes
    rwzi_names_zinfo_incl = gdf_rwzi_zinfo_incl["Naam rwzi"].unique()
    rwzi_codeist_zinfo_incl = gdf_rwzi_zinfo_incl["Codeist"].unique()
    rwzi_names_zinfo_excl = gdf_rwzi_zinfo_excl["Naam rwzi"].unique()
    rwzi_RwziCode_zinfo_excl = gdf_rwzi_zinfo_excl["RwziCode"].unique()

    logging.info(
        f"RWZIs with Zinfo data: {len(rwzi_names_zinfo_incl)} names, {len(rwzi_codeist_zinfo_incl)} Codeist IDs"
    )
    logging.info(
        f"RWZIs without Zinfo data: {len(rwzi_names_zinfo_excl)} names, {len(rwzi_RwziCode_zinfo_excl)} RwziCodes"
    )

    # Map Codeist to RWZI name for renaming columns
    codeist_to_name = dict(zip(gdf_rwzi_zinfo_incl["Codeist"], gdf_rwzi_zinfo_incl["Naam rwzi"]))

    # Rename columns
    df_Zinfo_influentdebieten_renamed = df_Zinfo_influentdebieten.rename(
        columns={col: codeist_to_name.get(col, col) for col in df_Zinfo_influentdebieten.columns if col != "time"}
    )

    # Filter columns: only "time" + RWZI names with data
    rwzi_flow_data = df_Zinfo_influentdebieten_renamed[["time"] + list(rwzi_names_zinfo_incl)]

    logging.info(
        "Zinfo DataFrame with renamed columns: %s",
        df_Zinfo_influentdebieten_renamed.head(),
    )

    logging.info("Filtered RWZI flow data with common RWZIs: %s", rwzi_flow_data.head())

    return rwzi_flow_data, gdf_rwzi_zinfo_incl, gdf_rwzi_zinfo_excl


rwzi_flow_data, gdf_rwzi_zinfo_incl, gdf_rwzi_zinfo_excl = process_rwzi_zinfo_data(
    rwzi_gdf, RWZI_ids_zinfo, df_Zinfo_influentdebieten
)


# %% RWS Jaardebieten voor overige locaties omzetten naar dagwaardes
# def process_rws_quantity_data(db_file, rwzi_codes_excl, gdf_excluded):
def process_rws_quantity_data(db_file, gdf_excluded):
    """
    Haalt jaarafvoeren (omgezet naar dagwaardes) uit de RWS database voor de RWZI's die niet in Z-info staan.

    Parameters
    ----------
        db_file (str): Path naar RwziBase2022_RWS_18032024.accdb
        rwzi_codes_excl (array-like): de lijst van RWZI's die niet in de Z-info database zitten.
        gdf_excluded (GeoDataFrame): GeoDataFrame met de metadata van deze RWZI's.

    Returns
    -------
        DataFrame: gemiddelde dagafvoeren van de RWZI's die niet in de Zinfo database zitten.
    """
    logging.info("Connecting to Access database...")
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={db_file};"
    )

    rwzi_codes_excl = gdf_excluded["RwziCode"].unique()

    conn = pyodbc.connect(conn_str)
    df_belasting = pd.read_sql("SELECT * FROM Belasting", conn)
    conn.close()
    logging.info("Database connection closed.")

    # Filter for parameter code '085' (corresponding with RWZI discharge)
    df_afvoeren = df_belasting[df_belasting["ParameterCode"] == "085"]
    df_afvoeren["RwziCode"] = df_afvoeren["RwziCode"].astype(int)
    df_afvoeren_excluded = df_afvoeren[df_afvoeren["RwziCode"].isin(rwzi_codes_excl)]

    # Merge RWZI names
    df_afvoeren_excluded = df_afvoeren_excluded.merge(
        gdf_excluded[["RwziCode", "Naam rwzi"]], on="RwziCode", how="left"
    )

    # Add 'time' column for pivoting
    df_afvoeren_excluded["time"] = df_afvoeren_excluded["Jaar"].astype(str) + "-01-01"
    df_afvoeren_pivot = df_afvoeren_excluded.pivot(index="time", columns="Naam rwzi", values="Waarde")

    df_afvoeren_pivot.reset_index(inplace=True)
    df_afvoeren_pivot["time"] = pd.to_datetime(df_afvoeren_pivot["time"])

    logging.info("Distributing annual values evenly across daily time series...")
    daily_dataframes = []

    for idx, row in df_afvoeren_pivot.iterrows():
        year = row["time"].year
        start_of_year = pd.Timestamp(f"{year}-01-01")
        end_of_year = pd.Timestamp(f"{year}-12-31")
        num_days = (end_of_year - start_of_year).days + 1
        date_range = pd.date_range(start=start_of_year, end=end_of_year, freq="D")

        daily_row_data = {"time": date_range}

        for col in row.index[1:]:  # Skip 'time'
            yearly_val = row[col]
            if pd.notna(yearly_val):
                daily_row_data[col] = [yearly_val / num_days] * num_days
            else:
                daily_row_data[col] = [np.nan] * num_days

        df_year = pd.DataFrame(daily_row_data)
        daily_dataframes.append(df_year)
        # print('check',daily_dataframes)

    df_daily_values = pd.concat(daily_dataframes, ignore_index=True)

    # Average duplicate dates, just in case
    # TODO: Check the units
    df_daily_values = df_daily_values.groupby("time").mean(numeric_only=True)
    df_m3pers = df_daily_values / (3600 * 24)
    df_m3pers = df_m3pers.reset_index()
    print(df_m3pers)
    logging.info("Finished processing excluded RWZI flow data.")
    return df_m3pers


df_rws_influentdebieten = process_rws_quantity_data(
    db_file=db_file,
    # rwzi_codes_excl=gdf_rwzi_zinfo_excl["RwziCode"].unique(),
    gdf_excluded=gdf_rwzi_zinfo_excl,
)

print(df_rws_influentdebieten.head())

# %% Combineer de Z-info data met de RWS database om een complete
rwzi_flow_data_idxed = rwzi_flow_data.set_index("time")
df_daily_values_idxed = df_rws_influentdebieten.set_index("time")
rwzi_flow_data_all = rwzi_flow_data_idxed.combine_first(df_daily_values_idxed)
rwzi_flow_data_all.reset_index(inplace=True)

rwzi_flow_data_all = rwzi_flow_data_all[
    (rwzi_flow_data_all["time"] >= starttime) & (rwzi_flow_data_all["time"] < endtime)
]

# Log result
# logging.info(
#    "Combined RWZI flow data within date range %s to %s:", starttime
# )  # .date(), endtime.date())
print(rwzi_flow_data_all.head())


# %% Define Boundary nodes
def create_flow_boundary_nodes(rwzi_gdf, rwzi_flow_data_all, model, starttime, endtime):
    """
    Zet de locaties en afvoeren van de RWZI's om in Ribasim - Flow Boundaries

    NaN data wordt overgeslagen en gelogged

    Parameters
    ----------
        rwzi_gdf (GeoDataFrame): locaties en metadata van  alle RWZI's
        rwzi_flow_data_all (DataFrame): DataFrame met alle RWZI flow data.
        model (object): Ribasim Model
        starttime (str): Start datum
        endtime (str): Eind datum

    Returns
    -------
        tuple: (flow_boundary_nodes_dict, skipped_rwzis, removed_timesteps)
    """
    flow_boundary_nodes = {}  # Dictionary to track flow boundary nodes by RWZI name
    skipped_rwzis = []  # List to track skipped RWZIs
    removed_timesteps = []  # List to track removed timesteps due to NaN values

    for index, row in rwzi_gdf.iterrows():
        rwzi_name = row["Naam rwzi"]
        rwzi_codeist = row["Codeist"]
        rwzi_code = row["RwziCode"]
        rwzi_beheerder_nr = row["Beheer_id"]
        rwzi_organisatie = row["Beheerder"]

        # RWZI coordinates (Longitude, Latitude)
        x_coord = row["RwziX_RD"]
        y_coord = row["RwziY_RD"]

        # Get the flow data for the current RWZI
        flow_rates = rwzi_flow_data_all.get(f"{rwzi_name}")

        # Skip RWZIs with no flow data
        if flow_rates is None:
            skipped_rwzis.append(rwzi_name)

            logging.info(f"Skipping RWZI '{rwzi_name}' due to missing flow data.")
            continue

        # Identify and remove NaN values, store the removed time steps
        nan_mask = flow_rates.isna()
        removed_times = rwzi_flow_data_all.time[nan_mask]
        for t in removed_times:
            removed_timesteps.append((rwzi_name, t))

        # Filter out NaN values and keep only valid flow data
        valid_mask = ~flow_rates.isna()
        valid_times = rwzi_flow_data_all.time[valid_mask]
        valid_flow_rates = flow_rates[valid_mask]

        # Skip RWZIs with no valid data after removing NaNs
        if valid_flow_rates.empty:
            skipped_rwzis.append(rwzi_name)
            logging.info(f"No valid data remaining for RWZI '{rwzi_name}'. Skipping.")
            continue

        # Normalize the time and adjust it to 12:00 PM of each day
        valid_times = valid_times.dt.normalize() + pd.Timedelta(hours=12)

        # Create the flow boundary node
        try:
            flow_boundary_node = model.flow_boundary.add(
                Node(
                    index + 1,
                    Point(x_coord, y_coord),
                    # TODO: besluit welke metadata er in het model relevant is (rwzi_code wordt gebruikt in merge)
                    name=rwzi_name,
                    meta_rwzi_codeist=rwzi_codeist,
                    meta_rwzi_code=rwzi_code,
                    meta_rwzi_beheerder_nr=rwzi_beheerder_nr,
                    meta_rwzi_organisatie=rwzi_organisatie,
                ),
                [
                    flow_boundary.Time(
                        time=valid_times,
                        # TODO: Check units
                        flow_rate=np.round(valid_flow_rates, 3),  # Convert flow rate to daily rate
                    )
                ],
            )
            flow_boundary_nodes[rwzi_name] = flow_boundary_node
            logging.info(f"Created flow boundary for RWZI '{rwzi_name}' at ({x_coord}, {y_coord})")

        except Exception as e:
            logging.error(f"Failed to create flow boundary for RWZI '{rwzi_name}': {e}")

    # Log the removed time steps due to NaN values
    removed_counts = Counter(rwzi for rwzi, _ in removed_timesteps)
    if removed_counts:
        logging.info("\nRemoved time steps due to NaN values:")
        for rwzi, count in removed_counts.items():
            logging.info(f"RWZI: {rwzi}, Skipped Time Steps: {count}")

    # Return the collected data
    return flow_boundary_nodes, skipped_rwzis, removed_timesteps


flow_boundary_nodes, skipped_rwzis, removed_timesteps = create_flow_boundary_nodes(
    rwzi_gdf=rwzi_gdf,
    rwzi_flow_data_all=rwzi_flow_data_all,
    model=model,
    starttime=starttime,
    endtime=endtime,
)


# %% Define Terminals
def create_terminal_nodes_from_gdf(rwzi_gdf, rwzi_flow_data_all, skipped_rwzis, model, start_node_id=999):
    """
    Create terminal nodes from RWZI GeoDataFrame.

    Parameters
    ----------
        rwzi_gdf (GeoDataFrame): locaties en metadata van  alle RWZI's
        rwzi_flow_data_all (DataFrame): DataFrame met alle RWZI flow data.
        skipped_rwzis (set or list): RWZI names to skip due to missing or invalid data.
        model (object): Ribasim Model
        start_node_id (int): Node ID nummer vanwaar de nodes zullen optellen

    Returns
    -------
        tuple: (terminal_nodes_dict, final_node_id)
    """
    terminal_nodes = {}
    node_id_counter = start_node_id

    for _, row in rwzi_gdf.iterrows():
        rwzi_name = row["Naam rwzi"]
        rwzi_codeist = row["Codeist"]
        rwzi_code = row["RwziCode"]
        rwzi_beheerder_nr = row["Beheer_id"]
        rwzi_organisatie = row["Beheerder"]
        x_outlet = row["LozingX_RD"]
        y_outlet = row["LozingY_RD"]
        terminal_node_name = f"{rwzi_name}_out"

        if rwzi_name in skipped_rwzis:
            logging.info(f"Skipping RWZI '{rwzi_name}' due to missing data.")
            continue

        try:
            terminal_node = model.terminal.add(
                Node(
                    node_id_counter,
                    Point(x_outlet, y_outlet),
                    name=terminal_node_name,
                    meta_rwzi_codeist=rwzi_codeist,
                    meta_rwzi_code=rwzi_code,
                    meta_rwzi_beheerder_nr=rwzi_beheerder_nr,
                    meta_rwzi_organisatie=rwzi_organisatie,
                )
            )
            terminal_nodes[terminal_node_name] = terminal_node
            logging.info(
                f"Created terminal node for outlet of RWZI '{rwzi_name}' "
                f"at ({x_outlet}, {y_outlet}) with name '{terminal_node_name}'."
            )
            node_id_counter += 1
        except Exception as e:
            logging.error(f"Error creating terminal node for RWZI '{rwzi_name}': {e}")
    return terminal_nodes, node_id_counter


terminal_nodes, node_id_counter = create_terminal_nodes_from_gdf(
    rwzi_gdf=rwzi_gdf,
    rwzi_flow_data_all=rwzi_flow_data_all,
    skipped_rwzis=skipped_rwzis,
    model=model,
    start_node_id=999,  # TODO: choose logical value
)


# %% Define Edges
def connect_flow_boundaries_to_terminal_nodes(flow_boundary_nodes, terminal_nodes, model):
    """
    Connectie tussen flow boundary nodes en terminal nodes.

    Parameters
    ----------
        flow_boundary_nodes (dict): Dictionary met flow boundary nodes
        terminal_nodes (dict): Dictionary met terminal nodes
        model (object): Ribasim Model

    Returns
    -------
        None
    """
    for rwzi_name, flow_boundary_node in flow_boundary_nodes.items():
        terminal_node_name = f"{rwzi_name}_out"

        # Check if the terminal node exists
        if terminal_node_name in terminal_nodes:
            terminal_node = terminal_nodes[terminal_node_name]

            # Connect the flow boundary to the terminal node
            model.edge.add(flow_boundary_node, terminal_node, name=f"{rwzi_name}_edge")
            logging.info(f"Connected flow boundary '{rwzi_name}' to terminal '{terminal_node_name}'")
        else:
            logging.warning(f"Terminal node '{terminal_node_name}' not found for RWZI '{rwzi_name}'.")


connect_flow_boundaries_to_terminal_nodes(flow_boundary_nodes, terminal_nodes, model)

# %% Run and Results
readme = f"""# Model met RWZI's connected aan terminals

Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""

# toml_path = base_dir / "ribasim.toml"
print("write rwzi model")
model.write(ribasim_toml)
cloud.joinpath("Basisgegevens", "RWZI", "modellen", "rwzi", "readme.md").write_text(readme)
upload_model = True

if upload_model:
    cloud.upload_model("Basisgegevens/RWZI", model="rwzi")

# %%
# result = model.run()
# result = subprocess.run([ribasim_path, ribasim_toml], capture_output=True, encoding="utf-8")
# print(result.stderr)
# result.check_returncode()

# %%
# print("write lhm model")
##ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
# model.write(ribasim_toml)

# ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "lhm.toml")
# lhm_model.write(ribasim_toml)
# cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "readme.md").write_text(readme)

# %% import modules
import os
from datetime import timedelta

import matplotlib.pyplot as plt
import pandas as pd
import requests
import ribasim

from ribasim_nl import CloudStorage

# %% Nieuwe excel inlezen

cloud = CloudStorage()
CONFIG = {
    "Venlo": {"link_id": 171},
    "BorgharenDorp": {"link_id": 20},
    "Smeermaas": {"link_id": 24},
    "Bunde": {"link_id": 317},
    "Maaseik": {"link_id": 205},
    "Eijsden": {"link_id": 159},
    "Megen": {"link_id": 177},
    "Lobith": {"link_id": 57},
    "Pannerdens_kanaal": {"link_id": 10},
    "Millingen_ad_Rijn": {"link_id": 8},
    "Driel_boven": {"link_id": 227},
    "Westervoort": {"link_id": 271},
    "Hagestein_boven": {"link_id": 213},
    "Maarssen": {"link_id": 217},
    "Weesp": {"link_id": 219},
    "Olst": {"link_id": 294},
    "Sluis13_debiet": {"link_id": 165},
    "Noordervaart": {"link_id": 380},
    "Loozen": {"link_id": 377},
    "Monsin": {"link_id": 319},
    "Kanne": {"link_id": 326},
    "Haccourt": {"link_id": 326},
}


# Read Ribasim model and data
ribasim_model_dir = cloud.joinpath("Rijkswaterstaat/modellen/hws_transient")
plots_dir = ribasim_model_dir / "plots"
ribasim_toml = ribasim_model_dir / "hws.toml"
model = ribasim.Model.read(ribasim_toml)
start_time, end_time = model.starttime + timedelta(days=40), model.endtime

plots_dir.mkdir(exist_ok=True)

flow_df = pd.read_feather(ribasim_toml.parent / "results" / "flow.arrow").set_index("time")
flow_df = flow_df[flow_df.index > start_time]
basin_df = pd.read_feather(ribasim_toml.parent / "results" / "basin.arrow").set_index("time")
basin_df = basin_df[basin_df.index > start_time]

# Read Excel file and sheet names
excel_file_path = cloud.joinpath("Rijkswaterstaat/aangeleverd/RWsOS-IWP_debieten_2023_2024.xlsx")
sheet_names = pd.ExcelFile(excel_file_path).sheet_names
found_names = set()


# Helper function to print first 30 values for Venlo
def print_venlo_data(df, sheet_name):
    if "Venlo" in df.columns:
        print(f"First 30 values for Venlo in sheet {sheet_name}:")
        print(df["Venlo"].head(30))
    else:
        print("Venlo data not found in sheet:", sheet_name)


# Process each sheet
for sheet_name in sheet_names:
    if set(CONFIG.keys()) == found_names:
        break  # Stop if all names are found

    print(f"Processing sheet: {sheet_name}")

    # Read and process headers
    meting_header_df = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=None, nrows=7)
    meting_headers = meting_header_df.iloc[5].tolist()

    available_names = [name for name in CONFIG if name in meting_headers and name not in found_names]

    if not available_names:
        continue
    # Select correct columns based on CONFIG keys
    column_indices = {name: meting_headers.index(name) for name in available_names}
    usecols = [0, *list(column_indices.values())]

    # Read data
    meting_df = pd.read_excel(
        excel_file_path,
        sheet_name=sheet_name,
        skiprows=5,
        usecols=usecols,
        index_col=0,
        parse_dates=True,
    )

    # Filter to the desired time range and print data for Venlo
    meting_df = meting_df[(meting_df.index > start_time) & (meting_df.index < end_time)]
    # Resample to daily data and fill forward to preserve existing values
    meting_df = meting_df.resample("D").mean()
    # Process each configuration item and generate plots
    for name, v in CONFIG.items():
        if name not in meting_df.columns or name in found_names:
            print(f"{name} not found in meting_df columns of sheet {sheet_name}.")
            continue

        found_names.add(name)
        try:
            if "link_id" in v:
                Q_meting = meting_df[name].rename("meting")
                Q_berekening = flow_df[flow_df["link_id"] == v["link_id"]][["flow_rate"]].rename(
                    columns={"flow_rate": "berekend"}
                )
                plot = pd.concat([Q_meting, Q_berekening], axis=1).plot(title=name, ylabel="m3/s")
                plot.get_figure().savefig(plots_dir / f"{name}_m3_s.png")
                print(f"Plot saved for {name} (link_id) in sheet {sheet_name}.")
        except KeyError:
            print(f"{name} not found in data for sheet {sheet_name}.")

print("Processing completed.")

# %% Oude excel files inlezen van IWP met afvoermetingen


cloud = CloudStorage()
CONFIG = {
    "Venlo": {"link_id": 171},
    "Heel boven": {"node_id": 8865},
    "Roermond boven": {"node_id": 9126},
    "Belfeld boven": {"node_id": 9422},
    "Bunde (Julianakanaal)": {"node_id": 7928},
    "Echt (Julianakanaal)": {"node_id": 8504},
    "Eijsden-grens": {"link_id": 159},
}

# Inlezen ribasim model
ribasim_model_dir = cloud.joinpath("Rijkswaterstaat/modellen/hws_transient")
plots_dir = ribasim_model_dir / "plots"
ribasim_toml = ribasim_model_dir / "hws.toml"
model = ribasim.Model.read(ribasim_toml)

start_time = model.starttime + timedelta(days=40)
end_time = model.endtime

ribasim_model_dir

plots_dir.mkdir(exist_ok=True)

flow_df = pd.read_feather(ribasim_toml.parent / "results" / "flow.arrow").set_index("time")
flow_df = flow_df[flow_df.index > start_time]
basin_df = pd.read_feather(ribasim_toml.parent / "results" / "basin.arrow").set_index("time")
basin_df = basin_df[basin_df.index > start_time]

meting_df = pd.read_excel(
    cloud.joinpath("Rijkswaterstaat/aangeleverd/debieten_Rijn_Maas_2023_2024.xlsx"),
    header=[0, 1, 2, 3, 4, 5, 6, 7, 8],
    index_col=[0],
)


meting_df = meting_df[(meting_df.index > start_time) & (meting_df.index < end_time)]
meting_df = meting_df.resample("D").mean()

for k, v in CONFIG.items():
    name = k
    if "link_id" in v.keys():
        Q_meting = meting_df["Debiet"]["(m3/s)"][name]
        Q_meting.columns = ["meting"]
        Q_berekening = flow_df[flow_df["link_id"] == v["link_id"]][["flow_rate"]].rename(
            columns={"flow_rate": "berekend"}
        )

        plot = pd.concat([Q_meting, Q_berekening]).plot(title=name, ylabel="m3/s")
        fig = plot.get_figure()
        fig.savefig(plots_dir / f"{name}_m3_s.png")

    if "node_id" in v.keys():
        H_meting = meting_df["Waterstand"]["(m) "][name]
        H_meting.columns = ["meting"]
        H_berekening = basin_df[basin_df["node_id"] == v["node_id"]][["level"]].rename(columns={"level": "berekend"})
        plot = pd.concat([H_meting, H_berekening]).plot(title=name, ylabel="m NAP")
        fig = plot.get_figure()
        fig.savefig(plots_dir / f"{name}_m.png")

# %% RWS csv's tijdseries inlezen en plotten


cloud = CloudStorage()
# Read the CSV file
ribasim_model_dir = cloud.joinpath("Rijkswaterstaat/aangeleverd/LWM_RWS_Waterinfo/debiet_2023-2024")
df = pd.read_csv(ribasim_model_dir / "LWM_Q_4.csv", delimiter=";", encoding="latin-1")
# Debug: Print the column names


# Debug: Print the column names to verify
print("Columns in the DataFrame:", df.columns)

# Remove leading/trailing spaces from column names if necessary
df.columns = df.columns.str.strip()

# Debug: Print the column names again after stripping
print("Stripped Columns in the DataFrame:", df.columns)

# Check if required columns exist
required_columns = [
    "WAARNEMINGDATUM",
    "WAARNEMINGTIJD (MET/CET)",
    "MONSTER_IDENTIFICATIE",
    "NUMERIEKEWAARDE",
]
missing_columns = [col for col in required_columns if col not in df.columns]

if missing_columns:
    raise KeyError(f"Missing columns in the DataFrame: {missing_columns}")


# Convert NUMERIEKEWAARDE to float (handling commas as decimal points)
def convert_numeriekewaarde(value):
    if isinstance(value, str):
        try:
            return float(value.replace(",", "."))
        except ValueError:
            return value
    elif isinstance(value, int):
        if value == 999999999:
            return float("nan")
        else:
            return float(value)
    else:
        return value  # Return as-is for other types


df["NUMERIEKEWAARDE"] = df["NUMERIEKEWAARDE"].apply(convert_numeriekewaarde)
# Convert WAARNEMINGDATUM and WAARNEMINGTIJD (MET/CET) to a single datetime column
df["WAARNEMINGDATUMTIJD"] = pd.to_datetime(
    df["WAARNEMINGDATUM"] + " " + df["WAARNEMINGTIJD (MET/CET)"],
    format="%d-%m-%Y %H:%M:%S",
)
print(df["WAARNEMINGDATUMTIJD"])
# Group by MONSTER_IDENTIFICATIE
grouped = df.groupby("MEETPUNT_IDENTIFICATIE")
print(df)

# Initialize cloud storage instance
cloud = CloudStorage()

# Create output directory for temporary storage of plots
output_dir = "plots"
os.makedirs(output_dir, exist_ok=True)

# Plot data for each MEETPUNT_IDENTIFICATIE
for monster_id, group in grouped:
    print(monster_id)
    plt.figure(figsize=(10, 6))
    plt.plot(group["WAARNEMINGDATUMTIJD"], group["NUMERIEKEWAARDE"])
    plt.xlabel("Date")
    plt.ylabel("Discharge (m3/s)")
    plt.title(f"Discharge over Time for {monster_id}")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.grid(True)

    # Save plot as PNG
    plot_file_path = os.path.join(output_dir, f"{monster_id}.png")
    plt.savefig(plot_file_path)
    plt.close()


print("Plots have been generated and uploaded to the cloud.")
# %%
# Define the API endpoint and request payload
collect_catalogus = "https://waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES_DBO/OphalenWaarnemingen"
request = {
    "Locatie": {"Code": "VLIS", "X": 541518.745919649, "Y": 5699254.96425966},
    "AquoPlusWaarnemingMetadata": {
        "AquoMetadata": {
            "Compartiment": {"Code": "OW"},
            "Grootheid": {"Code": "WATHTE"},
        },
        "WaarnemingMetadata": {"KwaliteitswaardecodeLijst": ["00", "10", "20", "25", "30", "40"]},
    },
    "Periode": {
        "Begindatumtijd": "2023-01-01T08:00:00.000+01:00",
        "Einddatumtijd": "2024-01-02T23:00:00.000+01:00",
    },
}

# Send the POST request
resp = requests.post(collect_catalogus, json=request)
elements = resp.json()


# Extract and print date and water level values
for waarnemingen in elements["WaarnemingenLijst"]:
    for lijst in waarnemingen["MetingenLijst"]:
        date = lijst["Tijdstip"]
        water_level = lijst["Meetwaarde"]["Waarde_Numeriek"]
        print(f"Date: {date}, Water Level: {water_level}")

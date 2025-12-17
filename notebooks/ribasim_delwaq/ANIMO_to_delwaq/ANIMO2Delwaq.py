"""Script to convert ANIMO output to Delwaq input

Created: 17-12-2025 by Gerard Pijcke

Notes
-----
- ANIMO test data set contains only quartelry varying values. Therefore script only writes input for 1st Jan, Apr, Jul, Sep. This should be changed later if ANIMO produces daily varying loads.
- A *.inc file is written per Delwaq segment / Ribasim basin node. For large models, this results in a large number of individual files.
- The Ribasim basin id is extracted from the csv file name. As such, the naming of the input files should follow name convention "basin_id_kg_per_day.csv" where id is the Ribasim basin id.
- It is user's responsibility that the selected Ribasim model and ANIMO files are a matching set
- Output consists of a 1.) *.inc file per Delwaq segment / Ribasim basin node containing the loads N and P for that specific segment / ribasim basin; 2.) another include file called include_listing.inc which contains a reference to all other include files. In your Delwaq input (*.inp file) it is sufficient to refer to the include_listing.inc which then refers to the files with loads per segment/basin. The user only needs to ensure that files are stored in the right location.
"""

# %% Import necessary libraries

import os
import re
from pathlib import Path

import pandas as pd

# %% User input

# model
model_name = "lhm_coupled_2025_9_0"
# wq mode (1=lwkm_np; 2=source tracking).
# In mode 1, total-n and total-p loads are distributed over inorganic and organic fractions as used for LWKM water quality simulations.
# In mode 2, total-n and total-p loads are attributed to tracers ANIMO_N and ANIMO_P to track nitrogen and phosphorous loads from agriculture and nature in Delwaq
wq_mode = 2
# data input directory
input_dir = "p:/11210327-lwkm2/00_scripts/Fatima/MethodeB/outputs/2017_per_quarter/csv/"
# output directory name
output_folder = "delwaq_wqmode2"

# %% Set model and output directories

# model directory
model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "modellen" / model_name
# output directory
output_path = model_path / output_folder

# %% Conversions

conv_kg2g = 1000
conv_day2sec = 1 / 86400

# %% Conversion ANIMO input to Delwaq wasteloads

# List all CSV files in the input directory
csv_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]
total_files = len(csv_files)

# List to store include lines
include_lines = []

# Loop through each CSV file
for idx, file in enumerate(csv_files, start=1):
    print(f"Processing file {idx} of {total_files}: {file}")

    file_path = os.path.join(input_dir, file)

    # Extract basin_id from filename
    match = re.search(r"basin_(\d+)_kg_per_day\.csv", file)
    basin_id = match.group(1) if match else "Unknown"

    # Read the CSV into a DataFrame
    df = pd.read_csv(file_path)

    # TODO: the current testing input files have the total nitrogen load in column "NO3" and total phosphorous load in column "PO4". This may change later!!!
    # Retain only the required columns
    df = df[["date", "NO3", "PO4"]]

    # Convert the 'date' column to the desired format
    # TODO: the below line may need to get reactivated later, when the lines of code below to keep only quarterly inputs have been removed.
    # df['date'] = pd.to_datetime(df['date']).dt.strftime("'%Y/%m/%d-%H:%M:%S'")

    # Convert loads from kg/d to g/s (required by Delwaq)
    df["NO3"] = df["NO3"] * conv_kg2g * conv_day2sec
    df["PO4"] = df["PO4"] * conv_kg2g * conv_day2sec

    # TODO: the below should be removed later when ANIMO data are available at daily time step. The below was merely added because the testing data set from ANIMO is based on quartely inputs.
    # keep only 1 jan / 1 apr / 1 jul / 1 oct (remove later)
    df["date"] = pd.to_datetime(df["date"])
    target_days = [(1, 1), (3, 1), (7, 1), (10, 1)]
    df = df[df["date"].apply(lambda d: (d.month, d.day) in target_days)]
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("'%Y/%m/%d-%H:%M:%S'")

    # Prepare the output content

    if wq_mode == 1:
        output_lines = [
            f"ITEM   '{basin_id}'",
            "CONCENTRATION",
            "USEFOR   'NO3'   'TotN' * 0.8",
            "USEFOR   'NH4'   'TotN' * 0.1",
            "USEFOR   'DetN'   'TotN' * 0.1",
            "USEFOR   'PO4'   'TotP' * 0.5",
            "USEFOR   'AAP'   'TotP' * 0.4",
            "USEFOR   'DetP'   'TotP' * 0.1",
            "TIME BLOCK",
            "DATA       'TotN'    'TotP'",
        ]

    elif wq_mode == 2:
        output_lines = [
            f"ITEM   '{basin_id}'",
            "CONCENTRATION",
            "USEFOR   'ANIMO_N'   'TotN'",
            "USEFOR   'ANIMO_P'   'TotP'",
            "TIME BLOCK",
            "DATA       'TotN'    'TotP'",
        ]

    for _, row in df.iterrows():
        output_lines.append(f"{row['date']}   {row['NO3']}   {row['PO4']}")

    # Write to output file
    output_filename = f"{basin_id}_animo.inc"
    output_file_path = os.path.join(output_path, output_filename)
    with open(output_file_path, "w") as f:
        f.write("\n".join(output_lines))

    # Add to include listing
    include_lines.append(f"INCLUDE   '{output_filename}'")

# %% Write include listing to file
include_listing_path = os.path.join(output_path, "include_ANIMO.inc")
with open(include_listing_path, "w") as f:
    f.write("\n".join(include_lines))

print(f"Include files for Delwaq simulation written to {output_path}")

# %%

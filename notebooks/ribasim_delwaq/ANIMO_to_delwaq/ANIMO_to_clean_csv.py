# %%

from pathlib import Path

import pandas as pd

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------

script_dir = Path(__file__).resolve().parent

input_file = script_dir / "data" / "dry_weight_loads_per_basin_kg_day.parquet"
output_file = script_dir / "output" / "ANIMO_loads_df.parquet"

output_file.parent.mkdir(exist_ok=True)

# -----------------------------------------------------------------------------
# Read
# -----------------------------------------------------------------------------

df = pd.read_parquet(input_file)

# -----------------------------------------------------------------------------
# Rename columns
# -----------------------------------------------------------------------------

df["time"] = pd.to_datetime(df.time, format="%Y")
df.rename(columns={"basin_id": "node_id"}, inplace=True)
df["node_id"] = df["node_id"].astype(int)

df["NO3"] = df["cNO3N"]
df["NH4"] = df["cNH4N"]
df["OON"] = df["cNorg"]
df["PO4"] = df["cPort"]
df["AAP"] = df["cPtot"] * 0
df["OOP"] = df["cPorg"]

# -----------------------------------------------------------------------------
# Reshape wide -> long
# -----------------------------------------------------------------------------

loads_df = df.melt(
    id_vars=["node_id", "time"],
    value_vars=["NO3", "NH4", "OON", "PO4", "AAP", "OOP"],
    var_name="substance",
    value_name="load",
)  # kg d-1, based on input filename (which is equal to the variable name in the script that converts WEnR ANIMO data)

# -----------------------------------------------------------------------------
# Save
# -----------------------------------------------------------------------------

loads_df.to_parquet(output_file, index=False)

print(f"Written: {output_file}")
print(loads_df.head())
# %%

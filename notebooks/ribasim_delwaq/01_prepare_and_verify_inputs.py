"""Prepare and verify DELWAQ input data."""

# %%
import subprocess
import sys
from pathlib import Path

import pandas as pd
import scripts.interpolate_timeseries as timeseries_helpers
import xarray as xr
from ribasim import Model
from scripts.verify_mass_loads import verify_mass_loads

# %%
ROOT = Path(__file__).resolve().parents[2]
MODEL_NAME = "lhm_coupled_full"
TOML_NAME = "lhm_coupled.toml"
model_path = ROOT / "data" / "Rijkswaterstaat" / "modellen" / MODEL_NAME
toml_path = model_path / TOML_NAME
model = Model.read(toml_path)

DELWAQ_DATA_DIR = ROOT / "data" / "Basisgegevens" / "Delwaq"
ANIMO_DATA_DIR = DELWAQ_DATA_DIR / "ANIMO"
ER_DATA_DIR = DELWAQ_DATA_DIR / "Emissieregistratie"
IM_DATA_DIR = DELWAQ_DATA_DIR / "IM"
ZINFO_DATA_DIR = DELWAQ_DATA_DIR / "Zinfo"
PREPARED_DATA_DIR = model_path / "delwaq_input"

RUN_CONVERSION_SCRIPTS = False
START_DATE = "2017-01-01"
END_DATE = "2019-12-31"


def run_conversion_script(script_path: Path, description: str) -> None:
    """Run a source-data conversion script and report its output."""
    result = subprocess.run(
        [sys.executable, str(script_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"{description} failed with exit code {result.returncode}")


def read_required_parquet(path: Path, description: str) -> pd.DataFrame:
    """Read a required parquet input or raise an informative error."""
    if not path.exists():
        raise FileNotFoundError(f"Expected {description} not found: {path}")
    return pd.read_parquet(path)


def prepare_boundary_data(boundary_data: pd.DataFrame, source_name: str) -> dict[str, pd.DataFrame]:
    """Clean, interpolate, and calculate mass loads for one boundary source."""
    concentration = boundary_data["concentration"]
    node_id = boundary_data["node_id"]

    print(f"\n{source_name} boundary input:")
    print("Rows with concentration == NA/NaN:", concentration.isna().sum(), "- excluding rows..")
    print("Rows with concentration == -999:", (concentration == -999).sum(), "- excluding rows..")
    print("Rows with concentration == 0:", (concentration == 0).sum())
    print("Rows with node_id == NA/NaN:", node_id.isna().sum(), "- excluding rows..")

    cleaned_data = boundary_data.loc[
        boundary_data["concentration"].notna()
        & boundary_data["concentration"].ne(-999)
        & boundary_data["node_id"].notna()
    ].copy()

    daily_concentrations, concentration_coverage = timeseries_helpers.interpolate_concentrations(
        cleaned_data,
        start_date=START_DATE,
        end_date=END_DATE,
    )
    print(concentration_coverage.sort_values(by="coverage_fraction"))

    daily_mass_loads = daily_concentrations.merge(
        daily_flow_dynamic[["node_id", "time", "flow_rate"]],
        on=["node_id", "time"],
        how="left",
        validate="many_to_one",
    ).merge(
        flow_static[["node_id", "flow_rate"]].rename(columns={"flow_rate": "static_flow_rate"}),
        on="node_id",
        how="left",
        validate="many_to_one",
    )

    static_flow_mask = daily_mass_loads["flow_rate"].isna() & daily_mass_loads["static_flow_rate"].notna()
    static_flow_nodes = daily_mass_loads.loc[static_flow_mask, "node_id"].nunique()
    print(f"{source_name} nodes using static flow rates: {static_flow_nodes}")

    daily_mass_loads["flow_rate"] = daily_mass_loads["flow_rate"].fillna(daily_mass_loads["static_flow_rate"])
    daily_mass_loads = daily_mass_loads.assign(load=lambda frame: frame["concentration"] * frame["flow_rate"])[
        ["node_id", "substance", "time", "load"]
    ]

    if daily_mass_loads["load"].isna().any():
        raise ValueError(f"{source_name} boundary mass loads contain missing flow matches.")

    return {
        "concentrations": cleaned_data,
        "daily_concentrations": daily_concentrations,
        "daily_mass_loads": daily_mass_loads,
        "concentration_coverage": concentration_coverage,
    }


# %% Read and verify source data
ER_loads_df_script = Path(__file__).resolve().parent / "ER_to_delwaq" / "ER_coupling_delwaq.py"
ER_loads_df_path = ER_DATA_DIR / "output" / "ER_loads_g_s_df.parquet"
if RUN_CONVERSION_SCRIPTS:
    run_conversion_script(ER_loads_df_script, "ER data coupling script")
ER_loads_df = read_required_parquet(ER_loads_df_path, "ER loads file")
verification_ER = verify_mass_loads(
    ER_loads_df,
    model=model,
    spatial_unit="basin",
    start_date="2017-01-01",
    end_date="2019-12-31",
    make_plot=True,
)

ANIMO_loads_df_script = Path(__file__).resolve().parent / "ANIMO_to_delwaq" / "ANIMO_coupling_delwaq.py"
ANIMO_loads_df_path = ANIMO_DATA_DIR / "output" / "ANIMO_loads_g_s_df.parquet"
if RUN_CONVERSION_SCRIPTS:
    run_conversion_script(ANIMO_loads_df_script, "ANIMO data coupling script")
ANIMO_loads_df = read_required_parquet(ANIMO_loads_df_path, "ANIMO loads file")
verification_ANIMO = verify_mass_loads(
    ANIMO_loads_df,
    model=model,
    spatial_unit="basin",
    start_date="2017-01-10",
    end_date="2020-01-01",
    make_plot=True,
)

boundwq_df_script = Path(__file__).resolve().parent / "IM_Zinfo_to_delwaq" / "write_boundwq_files.py"
IM_boundaries_df_path = IM_DATA_DIR / "output" / "IM_boundaries_mg_L_df.parquet"
Zinfo_boundaries_df_path = ZINFO_DATA_DIR / "output" / "Zinfo_boundaries_mg_L_df.parquet"
if RUN_CONVERSION_SCRIPTS:
    run_conversion_script(boundwq_df_script, "Boundary data coupling script")
IM_boundaries_df = read_required_parquet(IM_boundaries_df_path, "IM boundary file")
Zinfo_boundaries_df = read_required_parquet(Zinfo_boundaries_df_path, "Zinfo boundary file")

daily_flow_dynamic, flow_dynamic_coverage = timeseries_helpers.interpolate_flow_dynamic(
    model.flow_boundary.time.df,
    start_date=START_DATE,
    end_date=END_DATE,
)
flow_static = model.flow_boundary.static.df.reset_index(drop=True)
if not {"node_id", "flow_rate"}.issubset(flow_static.columns):
    raise ValueError("model.flow_boundary.static.df must contain 'node_id' and 'flow_rate' columns.")

IM_boundary_data = prepare_boundary_data(IM_boundaries_df, "IM")
Zinfo_boundary_data = prepare_boundary_data(Zinfo_boundaries_df, "Zinfo")

verification_IM_boundaries = verify_mass_loads(
    IM_boundary_data["daily_mass_loads"],
    model=model,
    spatial_unit="boundary",
    start_date="2017-01-01",
    end_date="2019-12-31",
    make_plot=True,
)
verification_Zinfo_boundaries = verify_mass_loads(
    Zinfo_boundary_data["daily_mass_loads"],
    model=model,
    spatial_unit="boundary",
    start_date="2017-01-01",
    end_date="2019-12-31",
    make_plot=True,
)

# %% Combine only after independent processing and verification
animo = ANIMO_loads_df.assign(year=ANIMO_loads_df["time"].dt.year)
er = ER_loads_df.assign(year=ER_loads_df["time"].dt.year)
result = animo.merge(
    er[["node_id", "substance", "year", "time", "load"]],
    on=["node_id", "substance", "year"],
    how="outer",
    suffixes=("_animo", "_er"),
)
result["time"] = result["time_animo"].fillna(result["time_er"])
result["load"] = result["load_animo"].fillna(0) + result["load_er"].fillna(0)
ANIMO_and_ER_loads_df = result[["node_id", "time", "substance", "load"]]

IM_and_Zinfo_df = pd.concat(
    [IM_boundary_data["concentrations"], Zinfo_boundary_data["concentrations"]],
    ignore_index=True,
)
daily_concentrations = pd.concat(
    [IM_boundary_data["daily_concentrations"], Zinfo_boundary_data["daily_concentrations"]],
    ignore_index=True,
)
daily_mass_loads_boundaries = pd.concat(
    [IM_boundary_data["daily_mass_loads"], Zinfo_boundary_data["daily_mass_loads"]],
    ignore_index=True,
)

# %% Persist the handoff for the model-building notebook
PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)
ANIMO_and_ER_loads_df.to_parquet(PREPARED_DATA_DIR / "ANIMO_and_ER_loads.parquet", index=False)
IM_and_Zinfo_df.to_parquet(PREPARED_DATA_DIR / "IM_and_Zinfo_concentrations.parquet", index=False)
daily_mass_loads_boundaries.to_parquet(PREPARED_DATA_DIR / "daily_mass_loads_boundaries.parquet", index=False)

# %% Optional interactive diagnostics
plot_node_id = 8010191
concentration_substance = "NO3"
lhm_flow = xr.open_dataset(model_path / "results" / "flow.nc").to_dataframe().reset_index()
timeseries_helpers.plot_boundary_timeseries(
    IM_and_Zinfo_df,
    daily_concentrations,
    lhm_flow,
    daily_flow_dynamic,
    plot_node_id,
    concentration_substance,
)

# %%

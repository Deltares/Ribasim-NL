"""
5-12-2025 Jesse van Leeuwen

- Test script to generate delwaq input files from Ribasim model, run delwaq simulation and check results

"""

# %%
import os
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from ribasim import Model
from ribasim.delwaq import generate, parse

# %%
ROOT = Path(__file__).resolve().parents[2]

model_name = "lhm_coupled_full"
toml_name = "lhm_coupled.toml"

model_path = ROOT / "data" / "Rijkswaterstaat" / "modellen" / model_name
toml_path = model_path / toml_name
assert toml_path.is_file()

model = Model.read(toml_path)

# %% define data dirs
DELWAQ_DATA_DIR = ROOT / "data" / "Basisgegevens" / "Delwaq"

ANIMO_DATA_DIR = DELWAQ_DATA_DIR / "ANIMO"
ER_DATA_DIR = DELWAQ_DATA_DIR / "Emissieregistratie"
IM_DATA_DIR = DELWAQ_DATA_DIR / "IM"
ZINFO_DATA_DIR = DELWAQ_DATA_DIR / "Zinfo"

RUN_CONVERSION_SCRIPTS = False


def run_conversion_script(script_path: Path, description: str) -> None:
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


# %%


def verify_mass_loads(
    loads_df,
    basin_area,
    start_date=None,
    end_date=None,
    make_plot=True,
):
    """
    Verify mass-load input for the water-quality model.

    Parameters
    ----------
    loads_df : pandas.DataFrame
        Must contain:
        - node_id
        - time
        - substance
        - load [g/s]

    basin_area : geopandas.GeoDataFrame
        Basin polygons containing node_id and geometry.

    start_date, end_date : str or datetime, optional
        Analysis period. Provide both or neither.

    make_plot : bool, default True
        Create verification plots.

    Returns
    -------
    dict
    """
    # ==============================================================
    # Structure and completeness
    # ==============================================================

    required_columns = {"node_id", "time", "substance", "load"}
    missing_columns = required_columns - set(loads_df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    if (start_date is None) != (end_date is None):
        raise ValueError("Provide both start_date and end_date, or neither.")

    df = loads_df.copy()
    df["time"] = pd.to_datetime(df["time"])
    if df["time"].isna().any():
        raise ValueError("The time column must not contain missing values.")

    if start_date is None:
        start_date = df["time"].min()
        end_date = df["time"].max()
    else:
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)

    if start_date >= end_date:
        raise ValueError("end_date must be after start_date.")

    df = df.loc[df["time"] <= end_date].copy()

    if df.empty:
        raise ValueError(f"No mass-load records found on or before {end_date.date()}.")

    # DELWAQ interprets a load as a state: it remains active until the next
    # value for the same node and substance. Include the last state before the
    # requested period so that forward-filling also works at start_date.
    state_values = (
        df.sort_values("time")
        .pivot_table(
            index="time",
            columns=["node_id", "substance"],
            values="load",
            aggfunc="last",
        )
        .sort_index()
    )
    time_index = state_values.index.union(pd.DatetimeIndex([start_date, end_date])).sort_values()
    state_values = state_values.reindex(time_index).ffill()

    missing_initial_values = state_values.loc[start_date].isna()
    if missing_initial_values.any():
        missing = [
            f"{node_id}/{substance}" for node_id, substance in missing_initial_values[missing_initial_values].index
        ]
        raise ValueError(
            "No load is available at start_date for node/substance pairs: "
            + ", ".join(missing[:10])
            + (" ..." if len(missing) > 10 else "")
        )

    period_values = state_values.loc[start_date:end_date]
    summed_loads = period_values.T.groupby(level="substance").sum().T

    def weighted_means(period_start, period_end):
        """Return node/substance means weighted over [period_start, period_end]."""
        boundaries = period_values.index.union(pd.DatetimeIndex([period_start, period_end])).sort_values()
        values = state_values.reindex(boundaries).ffill()
        if values.loc[period_start].isna().any():
            raise ValueError(f"No load is available at {period_start}.")

        weights = (values.index.to_series().shift(-1) - values.index).dt.total_seconds()
        weights = weights.clip(lower=0).fillna(0)
        valid_intervals = (values.index >= period_start) & (values.index < period_end)
        weights = weights.where(valid_intervals, 0)
        return values.mul(weights, axis=0).sum(axis=0) / weights.sum()

    # Records are retained for diagnostics; all statistics below use the
    # forward-filled state values.
    df = df.loc[df["time"].between(start_date, end_date)].copy()
    if df.empty:
        raise ValueError(f"No mass-load records found between {start_date.date()} and {end_date.date()}.")
    df["year"] = df["time"].dt.year

    print(f"Analysed period: {start_date.date()} to {end_date.date()}")

    print("Number of nodes with a mass load:", df["node_id"].nunique())

    print("Substances:", sorted(df["substance"].unique()))

    print("\nMissing values:")
    print(df.isna().sum())

    # ==============================================================
    # Annual and period statistics
    # ==============================================================

    seconds_per_average_year = 365.25 * 24 * 60 * 60
    annual_frames = []
    for year in range(start_date.year, end_date.year + 1):
        year_start = max(start_date, pd.Timestamp(year=year, month=1, day=1))
        year_end = min(end_date, pd.Timestamp(year=year + 1, month=1, day=1))
        if year_start >= year_end:
            continue
        means = weighted_means(year_start, year_end).rename("load").reset_index()
        means["year"] = year
        annual_frames.append(means)

    annual_node_loads = pd.concat(annual_frames, ignore_index=True)
    annual_distribution = annual_node_loads.groupby(
        ["year", "substance"],
        as_index=False,
    ).agg(
        total_load_g_s=("load", "sum"),
        mean_node_load_g_s=("load", "mean"),
        median_node_load_g_s=("load", "median"),
    )
    annual_distribution["total_load_kg_year"] = annual_distribution["total_load_g_s"] * seconds_per_average_year / 1000

    max_load = df.loc[df["load"].idxmax()]

    print("\nHighest individual load entry in the selected period:")
    print(max_load)

    # ==============================================================
    # Period statistics
    # ==============================================================

    nutrient_substances = {
        "N": ["NO3", "NH4", "OON"],
        "P": ["PO4", "AAP", "OOP"],
    }

    period_node_loads = weighted_means(start_date, end_date).rename("load").reset_index()
    period_substance_totals = period_node_loads.groupby("substance")["load"].sum()

    period_results = {}

    for nutrient, substances in nutrient_substances.items():
        mean_g_s = period_substance_totals.reindex(
            substances,
            fill_value=0,
        ).sum()

        period_results[f"total_{nutrient}_mean_g_s"] = mean_g_s

        period_results[f"total_{nutrient}_mean_t_year"] = mean_g_s * seconds_per_average_year / 1e6

    period_statistics = pd.Series(
        {
            "start_date": start_date,
            "end_date": end_date,
            **period_results,
        },
        name="period_statistics",
    )

    print()

    for nutrient in nutrient_substances:
        mean_g_s = period_results[f"total_{nutrient}_mean_g_s"]

        mean_t_year = period_results[f"total_{nutrient}_mean_t_year"]

        print(f"Mean total {nutrient} load: {mean_g_s:.6f} g/s ({mean_t_year:,.0f} tonnes/year)")

    # ==============================================================
    # Prepare basin maps
    # ==============================================================

    node_substance_loads = period_node_loads.copy()

    node_substance_loads["load_kg_year"] = node_substance_loads["load"] * seconds_per_average_year / 1000

    basin_area = basin_area.copy()
    basin_area["node_id"] = pd.to_numeric(basin_area["node_id"], errors="raise").astype("int64")
    node_substance_loads["node_id"] = pd.to_numeric(
        node_substance_loads["node_id"],
        errors="raise",
    ).astype("int64")

    # EPSG:28992 -> area returned in m²
    basin_area["area_ha"] = basin_area.geometry.area / 10_000
    if basin_area["area_ha"].le(0).any() or basin_area["area_ha"].isna().any():
        raise ValueError("basin_area contains missing or non-positive polygon areas.")

    basin_node_ids = set(basin_area["node_id"])
    load_node_ids = set(node_substance_loads["node_id"])
    overlap = basin_node_ids & load_node_ids
    if not overlap:
        raise ValueError(
            "No node_id values in the load data match basin_area. "
            f"Load node_id dtype: {node_substance_loads['node_id'].dtype}; "
            f"basin node_id dtype: {basin_area['node_id'].dtype}."
        )
    print(f"Nodes mapped to basin polygons: {len(overlap)} of {len(load_node_ids)}")

    nutrient_maps = {}

    for nutrient, substances in nutrient_substances.items():
        # TO PLOT A SINGLE SUBSTANCE INSTEAD:
        # substances = ["NO3"]

        nutrient_loads = (
            node_substance_loads.loc[node_substance_loads["substance"].isin(substances)]
            .groupby(
                "node_id",
                as_index=False,
            )["load_kg_year"]
            .sum()
        )

        nutrient_map = basin_area.merge(
            nutrient_loads,
            on="node_id",
            how="left",
        )

        nutrient_map["load_kg_year"] = nutrient_map["load_kg_year"].fillna(0)

        nutrient_map["load_kg_ha_year"] = nutrient_map["load_kg_year"] / nutrient_map["area_ha"]

        nutrient_maps[nutrient] = nutrient_map

    # ==============================================================
    # Plotting
    # ==============================================================

    fig = None

    if make_plot:
        fig, axes = plt.subplots(
            2,
            3,
            figsize=(22, 14),
        )

        ax_annual = axes[0, 0]
        ax_hist_n = axes[0, 1]
        ax_hist_p = axes[0, 2]
        ax_n = axes[1, 1]
        ax_p = axes[1, 2]

        # ----------------------------------------------------------
        # Forward-filled total load per substance
        # ----------------------------------------------------------

        for substance in summed_loads.columns:
            ax_annual.plot(
                summed_loads.index,
                summed_loads[substance],
                drawstyle="steps-post",
                marker="o",
                label=substance,
            )

        ax_annual.set(
            xlabel="Time",
            ylabel="Total load [g/s]",
            title="Forward-filled total load per substance",
        )

        ax_annual.grid(
            True,
            alpha=0.3,
        )

        ax_annual.legend(title="Substance")

        # ----------------------------------------------------------
        # Linear-scale distributions, excluding the highest 1 percent
        # separately for N and P so one nutrient cannot set the other's bins.
        # ----------------------------------------------------------

        for nutrient, ax in (("N", ax_hist_n), ("P", ax_hist_p)):
            values = nutrient_maps[nutrient]["load_kg_ha_year"]
            values = values.replace([float("inf"), -float("inf")], pd.NA).dropna()
            values = values[values.gt(0)]
            if values.empty:
                ax.text(
                    0.5,
                    0.5,
                    "No positive finite values",
                    ha="center",
                    va="center",
                    transform=ax.transAxes,
                )
            else:
                upper_limit = values.quantile(0.99)
                values = values[values <= upper_limit]
                if len(values) > 1 and not values.eq(values.iloc[0]).all():
                    ax.hist(
                        values, bins=30, density=True, alpha=0.75
                    )  # density should be lower for N because the bins are wider
                else:
                    ax.axvline(values.iloc[0], color="tab:blue")
            ax.set(
                xlabel="Load [kg/ha/year]",
                ylabel="Density",
                title=f"Total {nutrient} basin loads (below 99th percentile)",
            )
            ax.grid(alpha=0.3)

        # ----------------------------------------------------------
        # Total N map
        # ----------------------------------------------------------

        vmax_n = nutrient_maps["N"]["load_kg_ha_year"].replace([float("inf"), -float("inf")], pd.NA).quantile(0.99)

        nutrient_maps["N"].plot(
            column="load_kg_ha_year",
            cmap="viridis",
            legend=True,
            vmax=vmax_n,
            ax=ax_n,
        )

        ax_n.set_title("Total N load [kg/ha/year]")

        ax_n.axis("off")

        # ----------------------------------------------------------
        # Total P map
        # ----------------------------------------------------------

        vmax_p = nutrient_maps["P"]["load_kg_ha_year"].replace([float("inf"), -float("inf")], pd.NA).quantile(0.99)

        nutrient_maps["P"].plot(
            column="load_kg_ha_year",
            cmap="viridis",
            legend=True,
            vmax=vmax_p,
            ax=ax_p,
        )

        ax_p.set_title("Total P load [kg/ha/year]")

        ax_p.axis("off")
        axes[1, 0].axis("off")

        fig.tight_layout()

        plt.show()

    return {
        "filtered_loads": df,
        "annual_node_loads": annual_node_loads,
        "annual_distribution": annual_distribution,
        "forward_filled_loads": period_values,
        "summed_loads": summed_loads,
        "period_node_loads": period_node_loads,
        "period_substance_totals": period_substance_totals,
        "period_statistics": period_statistics,
        "nutrient_maps": nutrient_maps,
        "max_load": max_load,
        "figure": fig,
    }


# %%


### READ & PROCESS MASS LOAD EMISSIONS ###

# %% run ER data conversion script
# units: g/s

ER_loads_df_script = Path(__file__).resolve().parents[0] / "ER_to_delwaq" / "ER_coupling_delwaq.py"
ER_loads_df_path = ER_DATA_DIR / "output" / "ER_loads_g_s_df.parquet"

if RUN_CONVERSION_SCRIPTS:
    run_conversion_script(ER_loads_df_script, "ER data coupling script")

if ER_loads_df_path.exists():
    ER_loads_df = pd.read_parquet(ER_loads_df_path)
else:
    raise FileNotFoundError(f"Expected ER loads file not found: {ER_loads_df_path}")

# %%
verification = verify_mass_loads(
    ER_loads_df,
    basin_area=model.basin.area.df.copy(),
    start_date="2017-01-10",
    end_date="2019-12-31",
    make_plot=True,
)


# %% run ANIMO data conversion script
# units: g/s

ANIMO_loads_df_script = Path(__file__).resolve().parents[0] / "ANIMO_to_delwaq" / "ANIMO_coupling_delwaq.py"
ANIMO_loads_df_path = ANIMO_DATA_DIR / "output" / "ANIMO_loads_g_s_df.parquet"

if RUN_CONVERSION_SCRIPTS:
    run_conversion_script(ANIMO_loads_df_script, "ANIMO data coupling script")

if ANIMO_loads_df_path.exists():
    ANIMO_loads_df = pd.read_parquet(ANIMO_loads_df_path)
else:
    raise FileNotFoundError(f"Expected ANIMO loads file not found: {ANIMO_loads_df_path}")

verification = verify_mass_loads(
    ANIMO_loads_df,
    basin_area=model.basin.area.df.copy(),
    start_date="2017-01-10",
    end_date="2020-01-01",
    make_plot=True,
)

# %% run boundwq data conversion script
# units: mg/L

boundwq_df_script = Path(__file__).resolve().parents[0] / "IM_Zinfo_to_delwaq" / "write_boundwq_files.py"
IM_boundaries_df_path = IM_DATA_DIR / "output" / "IM_boundaries_mg_L_df.parquet"
Zinfo_boundaries_df_path = ZINFO_DATA_DIR / "output" / "Zinfo_boundaries_mg_L_df.parquet"

if RUN_CONVERSION_SCRIPTS:
    run_conversion_script(boundwq_df_script, "Boundary data coupling script")

# Load IM boundaries

if IM_boundaries_df_path.exists():
    IM_boundaries_df = pd.read_parquet(IM_boundaries_df_path)
else:
    raise FileNotFoundError(f"Expected Boundary loads file not found: {IM_boundaries_df_path}")

# Load Zinfo boundaries

if Zinfo_boundaries_df_path.exists():
    Zinfo_boundaries_df = pd.read_parquet(Zinfo_boundaries_df_path)
else:
    raise FileNotFoundError(f"Expected Boundary loads file not found: {Zinfo_boundaries_df_path}")


# %%
"""
COMBINING LOADS FROM ANIMO AND ER

for every ANIMO load in a specific time, add the ER load that is defined for that year
this assumes that the ER load, defined for the first day of that year, is representative for that whole year
if we end up using more detailed ER values, we might use merge_asof + direction = backward
--> this would take for every ANIMO load the most recent ER load defined either on that day or before that day
but it is more likely that we change generate.py to handle multiple emission sources together
"""

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

ANIMO_and_ER_loads_df.sort_values("load")

# %%
"""
COMBINING BOUNDARY CONCENTRATIONS FROM IM AND ZINFO

This is less complex as both data sources are linked to different boundaries: transboundary inflows and WWTPs respecitively
--> here, NA/NaN/-999 entries have to be removed to enable coupling to ribasim model --> check whether this is a problem
"""

IM_and_Zinfo_df = pd.concat([IM_boundaries_df, Zinfo_boundaries_df], ignore_index=True)

conc = IM_and_Zinfo_df["concentration"]

print("Rows with concentration == NA/NaN:", conc.isna().sum(), "- excluding rows..")
print("Rows with concentration == -999:", (conc == -999).sum(), "- excluding rows..")

nodeid = IM_and_Zinfo_df["node_id"]

print("Rows with node_id == NA/NaN:", nodeid.isna().sum(), "- excluding rows..")

IM_and_Zinfo_df = IM_and_Zinfo_df[IM_and_Zinfo_df["concentration"].notna() & (IM_and_Zinfo_df["concentration"] != -999)]

IM_and_Zinfo_df = IM_and_Zinfo_df[IM_and_Zinfo_df["node_id"].notna()]

IM_and_Zinfo_df.sort_values("node_id")


# %%

"""
ADD EMISSION DATA TO MODEL AND WRITE FOR REPRODUCIBILITY

--> The 'new' written model may be added to dvc - simply running generate based on this model should result in a properly set up delwaq simulation
--> With DVC, it could then easily be tested whether changes in generate.py still result in the same delwaq input, given that the WQ forcing remains constant
"""

# read model to add emission data to
model = Model.read(toml_path)

# mass loads
mass_loads = ANIMO_and_ER_loads_df  # or ER_loads_df or ANIMO_loads_df
model.basin.mass_load = mass_loads  # either the sum of ER and ANIMO or two separate dataframes (or another column specifying the data source, depending on what generate.py can handle easiest)

# boundary concentrations
# Add new data to existing (basic) tracers
flow_boundary_concentration = IM_and_Zinfo_df
model.flow_boundary.concentration = pd.concat(
    [model.flow_boundary.concentration.df, flow_boundary_concentration],
    ignore_index=True,
)

model.write(toml_path)

# %%
# 2. Set up DELWAQ simulation automatically using generate.py

output_folder = "delwaq_all_sources"

output_path = model_path / output_folder
generate(toml_path, output_path)

# %%
# 3. Run DELWAQ simulation

# Define path of Ribasim model again
output_folder = "delwaq_all_sources"  # change folder name with delwaq.inp modifications and added files: boundwq_rwzi.dat, boundwq_ba.dat, loadswq.id, b6_loads.inc to prevent overwriting

output_path = model_path / output_folder
assert toml_path.is_file()

# %%
# run delwaq & stream output

dimr_path = Path(os.environ["D3D_HOME"])
dimr_config_path = output_path / "dimr_config.xml"

cmd = f'"{dimr_path}" "{dimr_config_path}"'

proc = subprocess.Popen(  # noqa: S602
    cmd,
    cwd=output_path,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    shell=True,
)

with proc:
    for line in proc.stdout:
        print(line, end="")

returncode = proc.wait()

if returncode != 0:
    raise subprocess.CalledProcessError(returncode, cmd)

# %%
# 4. Parse and save simulation results

# before parsing model: include manually added substance/load
# TODO: include in generate function

# %%
# parse delwaq results
nmodel = parse(toml_path, output_folder=output_path, to_input=True)

# # %% check added loads in specified Ribasim nodes
# plot_fraction(nmodel, 700970, ["NO3"])  # node downstream of BA 700008; see lhm.toml in QGIS

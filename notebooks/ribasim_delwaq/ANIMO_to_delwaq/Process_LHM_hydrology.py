"""
Created on Wed Sep 16 10:58:38 2026

@author: janssen_gs
"""

from pathlib import Path

import imod
import pandas as pd
import xarray as xr

ROOT = Path(r"P:\11212767-lwkm2\Koppeling_ANIMO_Delwaq")

ANIMO_DATA_DIR = ROOT / "data"
ANIMO_INPUT_DIR = ANIMO_DATA_DIR / "1-external"
ANIMO_INTERIM_DIR = ANIMO_DATA_DIR / "2-interim"
ANIMO_OUTPUT_DIR = ANIMO_DATA_DIR / "5-results"
ANIMO_FIGURES_DIR = ANIMO_DATA_DIR / "6-visualization"

LHM_INPUT_DIR = ANIMO_INPUT_DIR / "LHM_fluxen" / "LHM_351"
LHM_OUTPUT_DIR = ANIMO_INTERIM_DIR / "LHM_fluxen" / "LHM_351"

for directory in [ANIMO_INPUT_DIR, ANIMO_INTERIM_DIR, ANIMO_OUTPUT_DIR, ANIMO_FIGURES_DIR, LHM_OUTPUT_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

check_mass_conservation = True  # Not standard, as it takes time


print("Reading LHM output files (fluxes)")
# Afvoeren zijn hier posititief gedefinieerd (Delwaq IN)
da_bdgqrun = -imod.idf.open(str(LHM_INPUT_DIR / "bdgqrun" / "bdgqrun_*.IDF"))
da_bdgriv_sys1 = -imod.idf.open(str(LHM_INPUT_DIR / "bdgriv" / "bdgriv_sys1*.IDF"))
da_bdgriv_sys2 = -imod.idf.open(str(LHM_INPUT_DIR / "bdgriv" / "bdgriv_sys2*.IDF"))
da_bdgriv_sys3 = -imod.idf.open(str(LHM_INPUT_DIR / "bdgriv" / "bdgriv_sys3*.IDF"))
da_bdgriv_sys4 = -imod.idf.open(str(LHM_INPUT_DIR / "bdgriv" / "bdgriv_sys4*.IDF"))
da_bdgriv_sys5 = -imod.idf.open(str(LHM_INPUT_DIR / "bdgriv" / "bdgriv_sys5*.IDF"))
da_bdgriv_sys6 = -imod.idf.open(str(LHM_INPUT_DIR / "bdgriv" / "bdgriv_sys6*.IDF"))
da_bdgdrn_sys1 = -imod.idf.open(str(LHM_INPUT_DIR / "bdgdrn" / "bdgdrn_sys1*.IDF"))
da_bdgdrn_sys2 = -imod.idf.open(str(LHM_INPUT_DIR / "bdgdrn" / "bdgdrn_sys2*.IDF"))
da_bdgdrn_sys3 = -imod.idf.open(str(LHM_INPUT_DIR / "bdgdrn" / "bdgdrn_sys3*.IDF"))
# Kwelfluxen zijn positief gedefinieerd (Delwaq IN)
da_bdgflf = imod.idf.open(str(LHM_INPUT_DIR / "bdgflf" / "bdgflf_*_L1.IDF"))
da_bdgflf_corr = imod.idf.open(str(LHM_INPUT_DIR / "bdgflf" / "diepe_slootkwel_gecorrigeerd" / "bdgflf_*_L1.IDF"))

# %%

metaswap_areaal = imod.idf.open(ANIMO_INPUT_DIR / "metaswap_areaal" / "AREA.IDF")
da_bdgqrun_m3_decade = da_bdgqrun * metaswap_areaal
da_bdgqrun_m3_decade = da_bdgqrun_m3_decade.fillna(0.0)

target_times = da_bdgqrun.time.to_index()
# Add the start of the first period
bins = pd.Index([da_bdgriv_sys1.time.to_index()[0]]).append(target_times)
period_lengths = (bins.to_series().diff().dt.days.fillna(0).astype(int) + 1).iloc[1:].to_numpy()
period_lengths = xr.DataArray(
    period_lengths,
    dims="time",
    coords={"time": da_bdgqrun.time},
)

print("Converting fluxes from m3/decade to m3/day (still on decade basis")
# Convert bdgqrun from m3/decasde to m3/day (still on decade basis)
da_bdgqrun_m3_day = da_bdgqrun_m3_decade / period_lengths
da_bdgqrun_m3_day = da_bdgqrun_m3_day.squeeze("layer")

# Convert modflow fluxes from m3/day on daily basis to m3/day on decade basis.
da_bdgriv_sys1_m3_day = (da_bdgriv_sys1.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgriv_sys2_m3_day = (da_bdgriv_sys2.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgriv_sys3_m3_day = (da_bdgriv_sys3.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgriv_sys4_m3_day = (da_bdgriv_sys4.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgriv_sys5_m3_day = (da_bdgriv_sys5.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgriv_sys6_m3_day = (da_bdgriv_sys6.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgdrn_sys1_m3_day = (da_bdgdrn_sys1.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgdrn_sys2_m3_day = (da_bdgdrn_sys2.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgdrn_sys3_m3_day = (da_bdgdrn_sys3.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgflf_m3_day = (da_bdgflf.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")
da_bdgflf_corr_m3_day = (da_bdgflf_corr.groupby_bins("time", bins=bins, include_lowest=True).sum()).squeeze("layer")


period_lengths_bins = xr.DataArray(
    period_lengths.values,
    dims="time_bins",
    coords={"time_bins": da_bdgriv_sys1_m3_day.time_bins},
)

print("Converting fluxes m3/day, from decade basis to daily basisto (still on decade basis")
da_bdgriv_sys1_m3_day = da_bdgriv_sys1_m3_day / period_lengths_bins
da_bdgriv_sys2_m3_day = da_bdgriv_sys2_m3_day / period_lengths_bins
da_bdgriv_sys3_m3_day = da_bdgriv_sys3_m3_day / period_lengths_bins
da_bdgriv_sys4_m3_day = da_bdgriv_sys4_m3_day / period_lengths_bins
da_bdgriv_sys5_m3_day = da_bdgriv_sys5_m3_day / period_lengths_bins
da_bdgriv_sys6_m3_day = da_bdgriv_sys6_m3_day / period_lengths_bins
da_bdgdrn_sys1_m3_day = da_bdgdrn_sys1_m3_day / period_lengths_bins
da_bdgdrn_sys2_m3_day = da_bdgdrn_sys2_m3_day / period_lengths_bins
da_bdgdrn_sys3_m3_day = da_bdgdrn_sys3_m3_day / period_lengths_bins
da_bdgflf_m3_day = da_bdgflf_m3_day / period_lengths_bins
da_bdgflf_corr_m3_day = da_bdgflf_corr_m3_day / period_lengths_bins

# Replace the Interval coordinate with the desired timestamps
da_bdgriv_sys1_m3_day = (
    da_bdgriv_sys1_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgriv_sys2_m3_day = (
    da_bdgriv_sys2_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgriv_sys3_m3_day = (
    da_bdgriv_sys3_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgriv_sys4_m3_day = (
    da_bdgriv_sys4_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgriv_sys5_m3_day = (
    da_bdgriv_sys5_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgriv_sys6_m3_day = (
    da_bdgriv_sys6_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgdrn_sys1_m3_day = (
    da_bdgdrn_sys1_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgdrn_sys2_m3_day = (
    da_bdgdrn_sys2_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgdrn_sys3_m3_day = (
    da_bdgdrn_sys3_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgflf_m3_day = (
    da_bdgflf_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)
da_bdgflf_corr_m3_day = (
    da_bdgflf_corr_m3_day.assign_coords(time=("time_bins", target_times))
    .swap_dims({"time_bins": "time"})
    .drop_vars("time_bins")
)

# Bereken totalen
da_afvoer_lhm_m3_day = (
    da_bdgqrun_m3_day
    + da_bdgriv_sys1_m3_day
    + da_bdgriv_sys2_m3_day
    + da_bdgriv_sys3_m3_day
    + da_bdgriv_sys4_m3_day
    + da_bdgriv_sys5_m3_day
    + da_bdgriv_sys6_m3_day
    + da_bdgdrn_sys1_m3_day
    + da_bdgdrn_sys2_m3_day
    + da_bdgdrn_sys3_m3_day
)

if check_mass_conservation:
    print("Checking mass conservation")
    total_flux_before = (
        da_bdgqrun_m3_decade.sum()
        + da_bdgriv_sys1.sum()
        + da_bdgriv_sys2.sum()
        + da_bdgriv_sys3.sum()
        + da_bdgriv_sys4.sum()
        + da_bdgriv_sys5.sum()
        + da_bdgriv_sys6.sum()
        + da_bdgdrn_sys1.sum()
        + da_bdgdrn_sys2.sum()
        + da_bdgdrn_sys3.sum()
    )
    total_flux_after = (
        da_bdgqrun_m3_day * period_lengths
        + da_bdgriv_sys1_m3_day * period_lengths
        + da_bdgriv_sys2_m3_day * period_lengths
        + da_bdgriv_sys3_m3_day * period_lengths
        + da_bdgriv_sys4_m3_day * period_lengths
        + da_bdgriv_sys5_m3_day * period_lengths
        + da_bdgriv_sys6_m3_day * period_lengths
        + da_bdgdrn_sys1_m3_day * period_lengths
        + da_bdgdrn_sys2_m3_day * period_lengths
        + da_bdgdrn_sys3_m3_day * period_lengths
    )
    total_flux_after = total_flux_after.sum()
    total_flux_lhm_summation_after = da_afvoer_lhm_m3_day * period_lengths
    total_flux_lhm_summation_after = total_flux_lhm_summation_after.sum()
    print("Total flux before temporal resolution adjustment: " + str(total_flux_before.values))
    print("Total flux after temporal resolution adjustment: " + str(total_flux_after.values))
    print(
        "Total flux after temporal resolution adjustment and summation: " + str(total_flux_lhm_summation_after.values)
    )

# Calcualte deep seepage correction term
da_correction_m3_day = da_bdgflf_m3_day - da_bdgflf_corr_m3_day

# %% Save to interim files
print("Save to interim files")
imod.idf.save(LHM_OUTPUT_DIR / "total_flux_m3_per_day.idf", da_afvoer_lhm_m3_day)
imod.idf.save(LHM_OUTPUT_DIR / "deep_seepage_correction_m3_per_day.idf", da_correction_m3_day)

from pathlib import Path

import imod
import pandas as pd
import zarr

# TODO (for LHM):
# - qrunm3 toevoegen door qrun te vermenigvuldingen met cellsize
# - psswm3 toevoegen door pssw te vermenigvuldigen met cellsize
# - metaswap resultaten (automatisch) fillen op de juiste tijdstap
# - dxy automatisch bepalen op basis van modflow-rooster
# - distance automatisch bepalen op basis van modflow-rooster (t.b.v. flexibele toepassing)
# - zarr store met compressie (wel alle budgets!) zoals besproken met @visr

# INPUT
time_slice = slice(pd.Timestamp("2013-01-01"), pd.Timestamp("2022-12-31"))  # time slice
dxy = 250  # cell-size of your raster-set
river_systems = range(2, 7)  # river systems to read (and expected in input!)
drainage_systems = range(1, 4)  # drainage systems to read (and expected in input!)
chunk_size = 100_000  # size of every chunk (reading optimization)
base_path = Path(
    r"g:\projecten\2025\release_LHM433\modelruns\runs_LWKM\LWKM_run_resultaten_totaal"
)  # Path to MODFLOW-METASWAP
modflow_budgets_path = base_path / "modflow"  # MODFLOW sub-dir
metaswap_budgets_path = base_path / "metaswap"  # MODFLOW sub-dir


# PROCESSING
print(f"reading MODFLOW budgets from: {modflow_budgets_path}")
print("reading riv-budgets for sys 1")
ar = (
    imod.idf.open(modflow_budgets_path / "bdgriv/bdgriv_sys1_*_l*.IDF")
    .sum(dim="layer")
    .drop_vars(["dy", "dx"])
    .sel(time=time_slice)
)
ar.name = "bdgriv_sys1"
ds = ar.to_dataset()

for isys in river_systems:
    print(f"reading riv-budgets for sys{isys}")
    try:
        ds[f"bdgriv_sys{isys}"] = (
            imod.idf.open(base_path / "modflow" / f"bdgriv/bdgriv_sys{isys}_*_l*.IDF")
            .sum(dim="layer")
            .drop_vars(["dy", "dx"])
            .sel(time=time_slice)
        )
    except FileNotFoundError:
        print("No budget-files for this system, please check and change `river_systems` variable")

for isys in drainage_systems:
    print(f"reading drn-budgets for sys {isys}")
    try:
        ds[f"bdgdrn_sys{isys}"] = (
            imod.idf.open(base_path / "modflow" / f"bdgdrn/bdgdrn_sys{isys}_*_l*.IDF")
            .sum(dim="layer")
            .drop_vars(["dy", "dx"])
            .sel(time=time_slice)
        )
    except FileNotFoundError:
        print("No budget-files for this system, please check and change `drainage_systems` variable")

print(f"reading MetaSWAP budgets from: {metaswap_budgets_path}")
bdgsw = imod.idf.open(metaswap_budgets_path / "bdgPssw/bdgPssw_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
bdgqr = imod.idf.open(metaswap_budgets_path / "bdgQrun/bdgQrun_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
ds["bdgpssw"] = bdgsw.resample(time="1D").bfill().sel(time=time_slice)
ds["bdgqrun"] = bdgqr.resample(time="1D").bfill().sel(time=time_slice)

n = int(chunk_size / dxy)
ds = ds.chunk(
    chunks={
        "time": 365,
        "y": n,
        "x": n,
    }
)
store = zarr.DirectoryStore("LHM_433_budget.zip")
print("writing to zarr-file")
ds.to_zarr(store=store, mode="w")
store.close()

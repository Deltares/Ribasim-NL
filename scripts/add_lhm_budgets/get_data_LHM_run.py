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

time_slice = slice(pd.Timestamp("2013-01-01"), pd.Timestamp("2022-12-31"))
dxy = 250
distance = 100_000

base_path = Path(r"g:\projecten\2025\release_LHM433\modelruns\runs_LWKM\LWKM_run_resultaten_totaal")
modflow_path = base_path / "modflow"
metaswap_path = base_path / "metaswap"

print("reading riv-budgets for sys 1")
ar = (
    imod.idf.open(modflow_path / "bdgriv/bdgriv_sys1_*_l*.IDF")
    .sum(dim="layer")
    .drop_vars(["dy", "dx"])
    .sel(time=time_slice)
)
ar.name = "bdgriv_sys1"
ds = ar.to_dataset()

for isys in range(2, 7):
    print(f"reading riv-budgets for sys{isys}")
    ds[f"bdgriv_sys{isys}"] = (
        imod.idf.open(base_path / "modflow" / f"bdgriv/bdgriv_sys{isys}_*_l*.IDF")
        .sum(dim="layer")
        .drop_vars(["dy", "dx"])
        .sel(time=time_slice)
    )

for isys in range(1, 4):
    print(f"reading drn-budgets for sys {isys}")
    ds[f"bdgdrn_sys{isys}"] = (
        imod.idf.open(base_path / "modflow" / f"bdgdrn/bdgdrn_sys{isys}_*_l*.IDF")
        .sum(dim="layer")
        .drop_vars(["dy", "dx"])
        .sel(time=time_slice)
    )

print("reading MetaSWAP budgets")
bdgsw = imod.idf.open(metaswap_path / "bdgPssw/bdgPssw_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
bdgqr = imod.idf.open(metaswap_path / "bdgQrun/bdgQrun_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
ds["bdgpssw"] = bdgsw.resample(time="1D").bfill().sel(time=time_slice)
ds["bdgqrun"] = bdgqr.resample(time="1D").bfill().sel(time=time_slice)

n = int(distance / dxy)
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

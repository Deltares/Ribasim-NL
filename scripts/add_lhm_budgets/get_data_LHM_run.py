from pathlib import Path

import imod
import numcodecs
import numpy as np
import pandas as pd
import xarray as xr
import xugrid as xu
import zarr

# script now works on older imod + xurgid versions, avalible on LHM-server
print(f"xugrid version: {xu.__version__}")
print(f"iMOD version: {imod.__version__}")

# inputs
time_slice = slice(pd.Timestamp("2013-01-01"), pd.Timestamp("2022-12-31"))  # time slice
dxy = 250  # cell-size of your raster-set
distance = 100_000  # total lateral distance in every chunk (reading optimization)
n = int(distance / dxy)  # chunk size lateral
time_chunk = 365  # chunk size temporal: one year

base_path = Path(
    r"g:\projecten\2025\release_LHM433\modelruns\runs_LWKM\LWKM_run_resultaten_totaal"
)  # Path to MODFLOW-METASWAP
modflow_budgets_path = base_path / "modflow"  # MODFLOW sub-dir
metaswap_budgets_path = base_path / "metaswap"  # MODFLOW sub-dir

precipitation_files = r"e:\LHM_4.3.3\Data\2_Model_Input\meteo\RD1\precipitation_*.asc"
makkink_files = r"e:\LHM_4.3.3\Data\2_Model_Input\meteo\EV24\evaporation_*.asc"


# definition
def resample_to_flux(arr: xr.DataArray) -> xr.DataArray:
    # get dt and padd for first dt
    dt_days = arr.time.diff("time") / np.timedelta64(1, "D")
    dt_days_full = xr.concat([dt_days.isel(time=0), dt_days], dim="time")
    dt_days_full = dt_days_full.assign_coords(time=arr.time)
    # to flux
    arr_flux = arr / dt_days_full  # to flux
    return arr_flux.resample(time="1D").bfill()  # resample to daily and backfill


# processing
print("reading riv-budgets for sys 1")
ar = (
    imod.idf.open(modflow_budgets_path / "bdgriv/bdgriv_sys1_*_l*.IDF")
    .sum(dim="layer")
    .drop_vars(["dy", "dx"])
    .sel(time=time_slice)
)
ar.name = "bdgriv_sys1"
ds = ar.to_dataset()

for isys in range(2, 7):
    print(f"reading riv-budgets for sys{isys}")
    ds[f"bdgriv_sys{isys}"] = (
        imod.idf.open(modflow_budgets_path / f"bdgriv/bdgriv_sys{isys}_*_l*.IDF")
        .sum(dim="layer")
        .drop_vars(["dy", "dx"])
        .sel(time=time_slice)
    )

for isys in range(1, 4):
    print(f"reading drn-budgets for sys {isys}")
    ds[f"bdgdrn_sys{isys}"] = (
        imod.idf.open(modflow_budgets_path / f"bdgdrn/bdgdrn_sys{isys}_*_l*.IDF")
        .sum(dim="layer")
        .drop_vars(["dy", "dx"])
        .sel(time=time_slice)
    )

print("reading MetaSWAP budgets")

bdgsw = imod.idf.open(metaswap_budgets_path / "bdgPssw/bdgPssw_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])

# get MetaSWAP area
area = imod.idf.open(metaswap_budgets_path / "bdgQrun/AREA.IDF")
bdgsw = imod.idf.open(metaswap_budgets_path / "bdgPssw/bdgPssw_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
ds["bdgpssw_m3d"] = (resample_to_flux(bdgsw) * area).sel(time=time_slice)

bdgqr = (
    imod.idf.open(metaswap_budgets_path / "bdgQrunm3_daily/bdgQrunm3_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
)
ds["bdgqrun_m3d"] = bdgqr

print("adding precipitation grids")
# voeg pp toe voor een check
pp = imod.rasterio.open(precipitation_files).sel(time=time_slice).drop_vars(["dy", "dx"])
mask = bdgsw.isel(time=0, drop=True)

regridder = imod.prepare.Regridder(method="nearest")
pp_regrid = regridder.regrid(
    source=pp,
    like=mask,
)
ds["precipitation_mmd"] = pp_regrid

print("adding MAKKINK grids")
# voeg pp toe voor een check
etref = imod.rasterio.open(makkink_files).sel(time=time_slice).drop_vars(["dy", "dx"])

regridder = imod.prepare.Regridder(method="nearest")
etref_regrid = regridder.regrid(
    source=etref,
    like=mask,
)
ds["makkink_mmd"] = etref_regrid


# rechunk and store
ds = ds.chunk(
    chunks={
        "time": time_chunk,
        "y": n,
        "x": n,
    }
)
store = zarr.DirectoryStore("LHM_433_budgets_update_makkink.zip")
print("writing to zar-file")
compressor = numcodecs.Blosc(cname="zstd", clevel=3, shuffle=2)
ds.to_zarr(
    store=store,
    mode="w",
    encoding={var: {"compressor": compressor} for var in ds.data_vars},
)
store.close()

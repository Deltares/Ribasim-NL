from pathlib import Path

import imod
import zarr

path = Path(r"g:\LHM_modelruns\model_v3.5\35-jarige_run_3.5.1\NHI3.5.1_2004-2023\modflow\results")

ar = imod.idf.open(path / "bdgriv/bdgriv_sys1_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
ar.name = "bdgriv_sys1"
ds = ar.to_dataset()

for isys in range(2, 7):
    ds[f"bdgriv_sys{isys}"] = (
        imod.idf.open(path / f"bdgriv/bdgriv_sys{isys}_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
    )

for isys in range(1, 4):
    ds[f"bdgdrn_sys{isys}"] = (
        imod.idf.open(path / f"bdgdrn/bdgdrn_sys{isys}_*_l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
    )

store = zarr.DirectoryStore("LHM_budgets.zip")
ds.to_zarr(store)
store.close()

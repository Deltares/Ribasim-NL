import geopandas as gpd
import imod
import numpy as np
import pandas as pd

# create basin_id mask array from basin definitions
# TODO: all node_id's need to be unique. Thats not the case right now!

basin1 = gpd.read_file("ontvangen/hkv_basin_areas_aug-gpkg_2024-08-21_0741/Basin_areas_aug.gpkg")
basin2 = gpd.read_file("ontvangen/export_alle_waterschappen_basin_areas.gpkg")
basin2 = basin2.rename(columns={"basin_node_id": "node_id"})
basins = pd.concat(
    [basin1, basin2],
    ignore_index=True,
)
array = imod.idf.open("model_uitvoer/bdgriv/bdgriv_sys1_20220101000000_l1.IDF").isel(time=0, layer=0, drop=True)
mask = imod.prepare.rasterize(basins, column="node_id", like=array, fill=0, dtype=np.int32)

# LHM-budget output naming convention

# RIV-package
# sys1: primary system
# sys2: secondary system
# sys3: tertiary system
# sys4: main system; layer 1
# sys5: main system; layer 2
# sys6: boil's / well's

# DRN-package
# sys1: tube drainage
# sys2: ditch dranage
# sys3: OLF

# For the Ribasim schematization we distinguish:
#   - Primary system for all basins
#   - Secondary system in basins other than the main river system

# For drainage an infiltration input based on LHM-output budgets, we distubute the LHM-systems in the following matter:
#  - Primary system   -> RIV-sys 1 + 4 + 5
#  - Secondary system -> RIV-sys 2 + 3 + 6, DRN-sys 1 + 2 + 3

# read riv-budgets, sum over yx, per basin
# sum primairy systems
budget_ar = imod.idf.open("model_uitvoer/bdgriv/bdgriv_sys1_*l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
budget = budget_ar.rename("primair").to_dataset()
for sys in [4, 5]:
    budget_ar = imod.idf.open(f"model_uitvoer/bdgriv/bdgriv_sys{sys}_*l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
    budget["primair"] += budget_ar

# sum secondary systems
budget["secondair"] = imod.idf.open("model_uitvoer/bdgriv/bdgriv_sys2_*l*.IDF").sum(dim="layer").drop_vars(["dy", "dx"])
for sys, package in zip([3, 6, 1, 2, 3], ["riv", "riv", "drn", "drn", "drn"]):
    budget_ar = (
        imod.idf.open(f"model_uitvoer/bdg{package}/bdg{package}_sys{sys}_*l*.IDF")
        .sum(dim="layer")
        .drop_vars(["dy", "dx"])
    )
    budget["secondair"] += budget_ar

# sum per node_id
# negative budgets means drainage from the groundwatermodel
budgets_per_node_id = budget.groupby(mask).sum(dim="stacked_y_x").to_dataframe().unstack(1)
budgets_per_node_id.index.name = "node_id"
budgets_per_node_id.to_csv("budgets_per_node_id.csv")

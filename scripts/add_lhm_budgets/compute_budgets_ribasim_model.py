import geopandas as gpd
import imod
import numpy as np
import pandas as pd
import ribasim
import xarray as xr
from split_basin_definition import split_basin_definitions

from ribasim_nl import CloudStorage

# %% get cloud data
cloud = CloudStorage()
filepath_lhm_budget = cloud.joinpath("Basisgegevens", "LHM", "4.3", "results", "LHM_budgets.zip")
filepath_ribasim_database = cloud.joinpath(
    "Zuiderzeeland", "modellen", "Zuiderzeeland_parameterized_2025_4_13", "database.gpkg"
)
filepath_ribasim_toml = cloud.joinpath(
    "Zuiderzeeland", "modellen", "Zuiderzeeland_parameterized_2025_4_13", "ribasim.toml"
)
cloud.synchronize(filepaths=[filepath_lhm_budget, filepath_ribasim_database, filepath_ribasim_toml])

# %% read data
budgets = xr.open_zarr(r"budgets\Basisgegevens\LHM\4.3\results\LHM_budgets.zip")
basin_definition = gpd.read_file(
    r"budgets\Zuiderzeeland\modellen\Zuiderzeeland_parameterized_2025_4_13\database.gpkg", layer="Basin / area"
)
ribasim_model = ribasim.Model.read(r"budgets\Zuiderzeeland\modellen\Zuiderzeeland_parameterized_2025_4_13\ribasim.toml")
primary_basin_definition, secondary_basin_definition = split_basin_definitions(basin_definition, ribasim_model)

# %% create masks
array = budgets["bdgriv_sys1"].isel(time=0, drop=True)
primary_basin_mask = imod.prepare.rasterize(
    primary_basin_definition, column="node_id", like=array, fill=-999, dtype=np.int32
)
secondary_basin_mask = imod.prepare.rasterize(
    secondary_basin_definition, column="node_id", like=array, fill=-999, dtype=np.int32
)

# %% compute budgets
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

# sum primairy systems
primary_summed_budgets = budgets["bdgriv_sys1"]
primary_summed_budgets = primary_summed_budgets.rename("primair")
for sys in [4, 5]:
    primary_summed_budgets += budgets[f"bdgriv_sys{sys}"]

# sum secondary systems
secondary_summed_budgets = budgets["bdgriv_sys2"]
secondary_summed_budgets = secondary_summed_budgets.rename("secondair")
for sys, package in zip([3, 6, 1, 2, 3], ["riv", "riv", "drn", "drn", "drn"]):
    secondary_summed_budgets += budgets[f"bdg{package}_sys{sys}"]

# sum per system and node_id
primary_budgets_per_node_id = (
    primary_summed_budgets.groupby(primary_basin_mask).sum(dim="stacked_y_x").to_dataframe().unstack(1).transpose()
)
primary_budgets_per_node_id.index = primary_budgets_per_node_id.index.droplevel(0)
primary_budgets_per_node_id = primary_budgets_per_node_id.loc[
    primary_budgets_per_node_id.index != -999, :
]  # remove non overlapping budgets

secundary_budgets_per_node_id = (
    secondary_summed_budgets.groupby(secondary_basin_mask).sum(dim="stacked_y_x").to_dataframe().unstack(1).transpose()
)
secundary_budgets_per_node_id.index = secundary_budgets_per_node_id.index.droplevel(0)
secundary_budgets_per_node_id = secundary_budgets_per_node_id.loc[
    secundary_budgets_per_node_id.index != -999, :
]  # remove non overlapping budgets

# combine dataframe's based on node_id
budgets_per_node_id = pd.concat([primary_budgets_per_node_id, secundary_budgets_per_node_id])
budgets_per_node_id.index.name = "node_id"

# split to drainage and infiltration budgets
# negative budgets means drainage from the groundwatermodel
drainag_per_node_id = budgets_per_node_id[budgets_per_node_id.lt(0.0)].abs().fillna(0.0)
infiltration_per_node_id = budgets_per_node_id[budgets_per_node_id.gt(0.0)].fillna(0.0)

drainag_per_node_id.to_csv("drainage.csv")
infiltration_per_node_id.to_csv("infiltration.csv")

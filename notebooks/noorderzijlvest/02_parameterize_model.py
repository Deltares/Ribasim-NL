# %%
import time

import geopandas as gpd
import numpy as np
import pandas as pd

from ribasim_nl import CloudStorage, Model
from ribasim_nl.parametrization import empty_static_df
from ribasim_nl.parametrization.target_level import (
    downstream_target_levels,
    upstream_target_levels,
)

cloud = CloudStorage()
authority = "Noorderzijlvest"
short_name = "nzv"
waterbeheercode = 34
kwk_names = {"KSL011": "R.J. Cleveringsluizen"}

parameter_defaults = {
    "Afvoergemaal": {"upstream_level_offset": 0.0, "downstream_level_offset": 0.3, "flow_rate_mm_per_day": 15},
    "Aanvoergemaal": {"upstream_level_offset": 0.2, "downstream_level_offset": 0.0, "flow_rate": 0.0},
    "Uitlaat": {"upstream_level_offset": 0.0, "downstream_level_offset": 0.3, "flow_rate_mm_per_day": 50},
    "Inlaat": {"upstream_level_offset": 0.2, "downstream_level_offset": 0.0, "flow_rate": 0.0},
}


# files
gemaal_gpkg = cloud.joinpath("Basisgegevens", "GKW", "20250116", "gemaal.gpkg")
stuw_gpkg = cloud.joinpath("Basisgegevens", "GKW", "20250116", "stuw.gpkg")
sluis_gpkg = cloud.joinpath("Basisgegevens", "GKW", "20250116", "sluis.gpkg")
static_data_xlsx = cloud.joinpath(
    "Noorderzijlvest",
    "verwerkt",
    "parameters",
    "static_data.xlsx",
)

# read files
_dfs = (gpd.read_file(i) for i in (gemaal_gpkg, stuw_gpkg, sluis_gpkg))
_dfs = (i[i.nen3610id.str.startswith(f"NL.WBHCODE.{waterbeheercode}")] for i in _dfs)
name_series = pd.concat(_dfs, ignore_index=True).drop_duplicates("code").set_index("code")["naam"]
for k, v in kwk_names.items():
    name_series[k] = v


gemaal_df = gpd.read_file(gemaal_gpkg, fid_as_index=True).nen3610id.str.startswith(f"NL.WBHCODE.{waterbeheercode}")
stuw_df = gpd.read_file(stuw_gpkg, fid_as_index=True).nen3610id.str.startswith(f"NL.WBHCODE.{waterbeheercode}")
sluis_gpkg = gpd.read_file(sluis_gpkg, fid_as_index=True).nen3610id.str.startswith(f"NL.WBHCODE.{waterbeheercode}")

gemaal_static_data = pd.read_excel(static_data_xlsx, sheet_name="Pump").set_index("code")
outlet_static_data = pd.read_excel(static_data_xlsx, sheet_name="Outlet").set_index("code")

# read model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{short_name}.toml")
model = Model.read(ribasim_toml)

# %%
start_time = time.time()
# %%
# Outlet
# %% parameterize
# set name
mask = model.outlet.node.df.meta_code_waterbeheerder.isin(name_series.index)
model.outlet.node.df.loc[~mask, "name"] = ""
model.outlet.node.df.loc[mask, "name"] = model.outlet.node.df[mask]["meta_code_waterbeheerder"].apply(
    lambda x: name_series[x]
)

# construct parameters
static_df = empty_static_df(model=model, node_type="Outlet", meta_columns=["meta_code_waterbeheerder"])

static_data = outlet_static_data.copy()

# update-function from static_data in Excel
static_data["node_id"] = static_df.set_index("meta_code_waterbeheerder").loc[static_data.index].node_id
static_data.columns = [i if i in static_df.columns else f"meta_{i}" for i in static_data.columns]
static_data = static_data.set_index("node_id")

static_df.set_index("node_id", inplace=True)
for col in static_data.columns:
    series = static_data[static_data[col].notna()]
    static_df.loc[series.index.to_numpy(), col] = series
static_df.reset_index(inplace=True)

# update-function from defaults

for category, parameters in parameter_defaults.items():
    print(category)

    mask = static_df["meta_categorie"] == category

    # flow_rate; all in sub_mask == True needs to be filled
    sub_mask = static_df[mask]["flow_rate"].isna()
    indices = sub_mask.index.to_numpy()
    if sub_mask.any():
        # fill nan with flow_rate_mm_day if provided

        if "flow_rate_mm_per_day" in parameters.keys():
            flow_rate_mm_per_day = parameters["flow_rate_mm_per_day"]
            flow_rate = np.array(
                [
                    (model.get_upstream_basins(node_id).area.sum() * flow_rate_mm_per_day / 1000) / 86400
                    for node_id in static_df[mask][sub_mask].node_id
                ],
                dtype=float,
            )
            scale = 10 ** (3 - 1 - np.where(flow_rate > 0, np.floor(np.log10(flow_rate)), 0))
            static_df.loc[indices, "flow_rate"] = np.round(flow_rate * scale) / scale
        elif "flow_rate" in parameters.keys():
            static_df.loc[indices, "flow_rate"] = parameters["flow_rate"]
        else:
            raise ValueError(f"Can't set flow_rate for node_ids {static_df.loc[indices, "node_id"].to_numpy()}")

    print("Elapsed Time:", time.time() - start_time, "seconds")
    # min_upstream_level
    sub_mask = static_df[mask]["min_upstream_level"].isna()
    if sub_mask.any():
        # calculate upstream levels
        upstream_level_offset = parameters["upstream_level_offset"]
        upstream_levels = upstream_target_levels(model=model, node_ids=static_df[mask][sub_mask].node_id)

        # assign upstream levels to static_df
        static_df.set_index("node_id", inplace=True)
        static_df.loc[upstream_levels.index, "min_upstream_level"] = (upstream_levels - upstream_level_offset).round(2)
        static_df.reset_index(inplace=True)
        print("Elapsed Time:", time.time() - start_time, "seconds")
    # max_downstream_level
    sub_mask = static_df[mask]["max_downstream_level"].isna()
    if sub_mask.any():
        # calculate downstream_levels
        downstream_level_offset = parameters["downstream_level_offset"]
        downstream_levels = downstream_target_levels(model=model, node_ids=static_df[mask][sub_mask].node_id)

        # assign upstream levels to static_df
        static_df.set_index("node_id", inplace=True)
        static_df.loc[downstream_levels.index, "max_downstream_level"] = (
            downstream_levels + downstream_level_offset
        ).round(2)
        static_df.reset_index(inplace=True)

# sanitize df
static_df.drop(columns=["meta_code_waterbeheerder"], inplace=True)
model.outlet.static.df = static_df


# %% write

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

print("Elapsed Time:", time.time() - start_time, "seconds")

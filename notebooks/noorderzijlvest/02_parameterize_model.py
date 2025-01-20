# %%
import time

import numpy as np
import pandas as pd

from ribasim_nl import CloudStorage, Model
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.parametrization import empty_static_df
from ribasim_nl.parametrization.conversions import round_to_significant_digits
from ribasim_nl.parametrization.target_level import (
    downstream_target_levels,
    upstream_target_levels,
)

cloud = CloudStorage()
authority = "Noorderzijlvest"
short_name = "nzv"


static_data_xlsx = cloud.joinpath(
    "Noorderzijlvest",
    "verwerkt",
    "parameters",
    "static_data.xlsx",
)
static_data_sheets = pd.ExcelFile(static_data_xlsx).sheet_names
defaults_df = pd.read_excel(static_data_xlsx, sheet_name="defaults", index_col=0)


# read model

# read model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{short_name}.toml")
model = Model.read(ribasim_toml)

start_time = time.time()
# %% add functions

# functions

# update function - column
for node_type in ["Pump", "Outlet"]:
    table = getattr(model, pascal_to_snake_case(node_type)).node
    if node_type in static_data_sheets:
        # add node_id to static_data
        static_data = pd.read_excel(static_data_xlsx, sheet_name=node_type).set_index("code")
        static_data = static_data[static_data.index.isin(table.df["meta_code_waterbeheerder"])]
        static_data.loc[:, "node_id"] = (
            table.df.reset_index().set_index("meta_code_waterbeheerder").loc[static_data.index].node_id
        )
        static_data.set_index("node_id", inplace=True)

        # add function to node via categorie
        for row in defaults_df.itertuples():
            category = row.Index
            function = row.function
            static_data.loc[static_data["categorie"] == category, "meta_function"] = function

        table.df.loc[static_data.index, "meta_function"] = static_data["meta_function"]


# %% parameterize
for node_type in ["Pump", "Outlet"]:
    # update on static_table
    if node_type in static_data_sheets:
        # start with an empty static_df with the correct columns and meta_code_waterbeheerder
        static_df = empty_static_df(model=model, node_type=node_type, meta_columns=["meta_code_waterbeheerder"])

        static_data = pd.read_excel(static_data_xlsx, sheet_name=node_type).set_index("code")

        # in case there is more defined in static_data than in the model
        static_data = static_data[static_data.index.isin(static_df["meta_code_waterbeheerder"])]

        # update-function from static_data in Excel
        static_data.loc[:, "node_id"] = static_df.set_index("meta_code_waterbeheerder").loc[static_data.index].node_id
        static_data.columns = [i if i in static_df.columns else f"meta_{i}" for i in static_data.columns]
        static_data = static_data.set_index("node_id")

        static_df.set_index("node_id", inplace=True)
        for col in static_data.columns:
            series = static_data[static_data[col].notna()][col]
            if col == "flow_rate":
                series = series.apply(round_to_significant_digits)
            static_df.loc[series.index.to_numpy(), col] = series
        static_df.reset_index(inplace=True)

    # update-function from defaults
    for row in defaults_df.itertuples():
        category = row.Index
        mask = static_df["meta_categorie"] == category

        # flow_rate; all in sub_mask == True needs to be filled
        sub_mask = static_df[mask]["flow_rate"].isna()
        indices = sub_mask.index.to_numpy()
        if sub_mask.any():
            # fill nan with flow_rate_mm_day if provided

            if not pd.isna(row.flow_rate_mm_per_day):
                flow_rate_mm_per_day = row.flow_rate_mm_per_day
                flow_rate = np.array(
                    [
                        round_to_significant_digits(
                            model.get_upstream_basins(node_id, stop_at_inlet=True).area.sum()
                            * flow_rate_mm_per_day
                            / 1000
                            / 86400
                        )
                        for node_id in static_df[mask][sub_mask].node_id
                    ],
                    dtype=float,
                )
                static_df.loc[indices, "flow_rate"] = flow_rate
            elif not pd.isna(row.flow_rate):
                static_df.loc[indices, "flow_rate"] = row.flow_rate
            else:
                raise ValueError(f"Can't set flow_rate for node_ids {static_df.loc[indices, "node_id"].to_numpy()}")

        # min_upstream_level
        sub_mask = static_df[mask]["min_upstream_level"].isna()
        if sub_mask.any():
            # calculate upstream levels
            upstream_level_offset = row.upstream_level_offset
            upstream_levels = upstream_target_levels(model=model, node_ids=static_df[mask][sub_mask].node_id)

            # assign upstream levels to static_df
            static_df.set_index("node_id", inplace=True)
            static_df.loc[upstream_levels.index, "min_upstream_level"] = (
                upstream_levels - upstream_level_offset
            ).round(2)
            static_df.reset_index(inplace=True)
        sub_mask = static_df[mask]["max_downstream_level"].isna()
        if sub_mask.any():
            # calculate downstream_levels
            downstream_level_offset = row.downstream_level_offset
            downstream_levels = downstream_target_levels(model=model, node_ids=static_df[mask][sub_mask].node_id)

            # assign upstream levels to static_df
            static_df.set_index("node_id", inplace=True)
            static_df.loc[downstream_levels.index, "max_downstream_level"] = (
                downstream_levels + downstream_level_offset
            ).round(2)
            static_df.reset_index(inplace=True)

    # sanitize df
    static_df.drop(columns=["meta_code_waterbeheerder"], inplace=True)
    getattr(model, pascal_to_snake_case(node_type)).static.df = static_df


# %% write

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

print("Elapsed Time:", time.time() - start_time, "seconds")

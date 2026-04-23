from pathlib import Path

import numpy as np
from ribasim import Model, Node, run_ribasim
from ribasim.nodes import *

# Load model, set allocation to true
root = Path("./scripts/allocation_testing")
basemodel = Path("./data/Rijkswaterstaat/modellen/hws_transient/hws.toml")

model = Model.read(filepath=basemodel)
model.experimental.allocation = True
model.allocation.timestep = 60
model.logging.verbosity = "debug"

# inspect static flows
print("static flows: \n", model.flow_boundary.static.df)

# inspect time-varying flows
print("time-varying flows: \n", model.flow_boundary.time.df)

# set constant boundaries Meuse and Rhine
node_id_meuse = 7168
node_id_rhine = 10191

discharge_meuse = 200
discharge_rhine = 1000


def replace_node(model, node_id, constant_flow_rate):
    # 1. Grab the existing geometry (so the node stays in place on the map)
    existing_node = model.flow_boundary[node_id]

    # 2. Replace the node by adding new node (it replaces the original node)
    model.flow_boundary.add(
        node=Node(node_id=node_id, geometry=existing_node.geometry),
        tables=[flow_boundary.Static(flow_rate=[constant_flow_rate])],
    )
    return model


model = replace_node(model, node_id=node_id_meuse, constant_flow_rate=discharge_meuse)
model = replace_node(model, node_id=node_id_rhine, constant_flow_rate=discharge_rhine)

# set lowest threshold_high values to zero
node_ids = [10723, 10724]
values_original = [685, 685]


def update_discrete_control_condition(model, node_id, column, old_value, new_value):
    mask = (model.discrete_control.condition.df["node_id"] == node_id) & (
        model.discrete_control.condition.df[column] == old_value
    )
    n_updated = mask.sum()
    model.discrete_control.condition.df.loc[mask, column] = new_value
    return n_updated


def set_basin_state_from_profile_median(model):
    profile_df = model.basin.profile.df
    state_df = model.basin.state.df

    median_level_per_node = (
        profile_df.groupby("node_id", as_index=False)["level"]
        .median()
        .rename(columns={"level": "median_profile_level"})
    )

    state_df = state_df.merge(median_level_per_node, on="node_id", how="left")
    state_df["level"] = state_df["median_profile_level"].combine_first(state_df["level"])
    state_df = state_df.drop(columns=["median_profile_level"])

    model.basin.state.df = state_df
    return model


def extend_basin_profiles_with_max_area_at_top_level(model, top_level=100.0):
    import pandas as pd
    from ribasim import Node
    from ribasim.nodes import basin

    profile_df = model.basin.profile.df
    state_df = model.basin.state.df

    for node_id in profile_df["node_id"].unique():
        existing_node = model.basin[node_id]

        # Get profile data for this basin
        node_profile = profile_df[profile_df["node_id"] == node_id].copy()

        # Get state data for this basin
        node_state = state_df[state_df["node_id"] == node_id].copy()

        # Find max area
        max_area = node_profile["area"].max()

        # Add new row at top_level with max area
        new_row = pd.DataFrame({"node_id": [node_id], "level": [top_level], "area": [max_area], "storage": [np.nan]})
        updated_profile = pd.concat([node_profile, new_row], ignore_index=True)
        print(updated_profile)
        # Prepare tables to re-add
        tables = [
            basin.Profile(
                node_id=updated_profile["node_id"].tolist(),
                level=updated_profile["level"].tolist(),
                area=updated_profile["area"].tolist(),
                storage=updated_profile["storage"].tolist(),
            )
        ]

        # Add State table if it exists for this node
        if not node_state.empty:
            tables.append(
                basin.State(
                    node_id=node_state["node_id"].tolist(),
                    level=node_state["level"].tolist(),
                )
            )

        # Re-add the basin with updated profile and state
        model.basin.add(
            node=Node(node_id=node_id, geometry=existing_node.geometry),
            tables=tables,
        )


update_discrete_control_condition(model, node_ids[0], "threshold_high", values_original[0], 0)
update_discrete_control_condition(model, node_ids[1], "threshold_high", values_original[1], 0)

# set_basin_state_from_profile_median(model)
extend_basin_profiles_with_max_area_at_top_level(model)

# only run for one week
model.starttime = "2023-02-01 00:00:00"
model.endtime = "2023-02-08 00:00:00"

updated_toml = root / "constant/hws.toml"

model.write(filepath=updated_toml)

run_ribasim(updated_toml)

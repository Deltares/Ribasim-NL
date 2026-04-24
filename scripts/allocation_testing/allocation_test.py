import shutil
from pathlib import Path

import numpy as np
import pandas as pd
from ribasim import Model, Node
from ribasim.nodes import *
from ribasim.nodes import basin, user_demand


def clear_results(model):
    results_dir = model.toml_path.parent / "results"
    if results_dir.exists():
        shutil.rmtree(results_dir)


def replace_node_flow_rate(model, node_id, constant_flow_rate):
    # 1. Grab the existing geometry (so the node stays in place on the map)
    existing_node = model.flow_boundary[node_id]

    # 2. Replace the node by adding new node (it replaces the original node)
    model.flow_boundary.add(
        node=Node(node_id=node_id, geometry=existing_node.geometry),
        tables=[flow_boundary.Static(flow_rate=[constant_flow_rate])],
    )
    return model


def update_discrete_control_condition(model, node_id, column, old_value, new_value):
    mask = (model.discrete_control.condition.df["node_id"] == node_id) & (
        model.discrete_control.condition.df[column] == old_value
    )
    n_updated = mask.sum()
    model.discrete_control.condition.df.loc[mask, column] = new_value
    return n_updated


def set_user_demand_priority(model, node_id, priority=1):
    existing_node = model.user_demand[node_id]

    if model.user_demand.static.df is not None:
        node_static = model.user_demand.static.df[model.user_demand.static.df["node_id"] == node_id].copy()
        if not node_static.empty:
            node_static["demand_priority"] = priority
            model.user_demand.add(
                node=Node(node_id=node_id, geometry=existing_node.geometry),
                tables=[
                    user_demand.Static(
                        demand_priority=node_static["demand_priority"].tolist(),
                        demand=node_static["demand"].tolist(),
                        return_factor=node_static["return_factor"].tolist(),
                        min_level=node_static["min_level"].tolist(),
                    )
                ],
            )

    if model.user_demand.time.df is not None:
        node_time = model.user_demand.time.df[model.user_demand.time.df["node_id"] == node_id].copy()
        if not node_time.empty:
            node_time["demand_priority"] = priority
            model.user_demand.add(
                node=Node(node_id=node_id, geometry=existing_node.geometry),
                tables=[
                    user_demand.Time(
                        time=node_time["time"].tolist(),
                        demand_priority=node_time["demand_priority"].tolist(),
                        demand=node_time["demand"].tolist(),
                        return_factor=node_time["return_factor"].tolist(),
                        min_level=node_time["min_level"].tolist(),
                    )
                ],
            )


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


def set_manual_basin_state(model, node_id, level):
    profile_df = model.basin.profile.df
    target_profile = profile_df[profile_df["node_id"] == node_id].copy()
    if target_profile.empty:
        raise ValueError(f"No basin profile found for node_id={node_id}")

    existing_node = model.basin[node_id]

    model.basin.add(
        node=Node(node_id=node_id, geometry=existing_node.geometry),
        tables=[
            basin.Profile(
                node_id=target_profile["node_id"].tolist(),
                level=target_profile["level"].tolist(),
                area=target_profile["area"].tolist(),
                storage=target_profile["storage"].tolist(),
            ),
            basin.State(
                node_id=[node_id],
                level=[level],
            ),
        ],
    )


def extend_basin_profiles_with_max_area_at_top_level(model, top_level=100.0):
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


# Load model, set allocation to true
root = Path("./scripts/allocation_testing")
basemodel = Path("./data/Rijkswaterstaat/modellen/hws_transient/hws.toml")

# Write base model in project folder
model = Model.read(filepath=basemodel)
model.logging.verbosity = "info"

updated_toml = root / "basemodel/hws.toml"
model.write(filepath=updated_toml)

# set lowest threshold_high values to zero
node_ids_control = [10723, 10724]
values_original = [685, 685]

update_discrete_control_condition(model, node_ids_control[0], "threshold_high", values_original[0], 0)
update_discrete_control_condition(model, node_ids_control[1], "threshold_high", values_original[1], 0)

# set_basin_state_from_profile_median(model)
extend_basin_profiles_with_max_area_at_top_level(model)

# set_manual_basin_state(model, node_id=5792, level=-3.5)  # Markermeer

# timing settings for 50 days
model.starttime = "2023-01-20 00:00:00"
model.endtime = "2023-03-11 00:00:00"

model.solver.dt = 60
model.solver.dtmin = 60
model.solver.force_dtmin = True
model.allocation.timestep = 3600
# model.solver.algorithm = "Euler"
# model.solver.algorithm = "ImplicitEuler"
model.solver.algorithm = "QNDF"

# 1. write first model without allocation
model.experimental.allocation = False

updated_toml = root / "dynamic_alloc_disabled/hws.toml"
model.write(filepath=updated_toml)
clear_results(model)

# 2. and then the model with allocation
model.experimental.allocation = True

updated_toml = root / "dynamic_alloc_enabled/hws.toml"
model.write(filepath=updated_toml)
clear_results(model)

# 3. set warmup forcing for Meuse and Rhine
node_id_meuse = 7168
node_id_rhine = 10191

discharge_meuse = 200
discharge_rhine = 1000

model = replace_node_flow_rate(model, node_id=node_id_meuse, constant_flow_rate=discharge_meuse)
model = replace_node_flow_rate(model, node_id=node_id_rhine, constant_flow_rate=discharge_rhine)

model.experimental.allocation = False

updated_toml = root / "constant_alloc_disabled/hws.toml"

model.write(filepath=updated_toml)
clear_results(model)

# 4. and then the model with allocation
model.experimental.allocation = True

updated_toml = root / "constant_alloc_enabled/hws.toml"

model.write(filepath=updated_toml)
clear_results(model)

# run_ribasim(toml_path=updated_toml)

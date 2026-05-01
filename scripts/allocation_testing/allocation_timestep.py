from pathlib import Path

import numpy as np
import pandas as pd
from ribasim import Model, Node
from ribasim.nodes import basin


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
model.logging.verbosity = "debug"

extend_basin_profiles_with_max_area_at_top_level(model)

# Enable allocation
model.experimental.allocation = True
model.allocation.timestep = 3600

updated_toml = root / "timestep/hws.toml"
model.write(filepath=updated_toml)

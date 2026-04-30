# %%
from datetime import datetime

import numpy as np
import pandas as pd
from ribasim import Model
from ribasim.schemas import BasinConcentrationSchema, FlowBoundaryConcentrationSchema, LevelBoundaryConcentrationSchema

RENAME_MAP = {"buitenland": "Buitenland", "buitenlandse aanvoer": "Buitenland"}


def sort_lhm_fractions(items: list[str]) -> list[str]:
    """
    Sort LHM fraction labels according to a custom order.

    The sorting rules are:
    - A predefined set of items is placed at the top (`top_order`)
    - A predefined set of items is placed at the bottom (`lower_order`)
    - All remaining items are placed in between, preserving their original order
    - Items not present in the input are ignored
    - Duplicate items are removed while preserving the first occurrence

    Parameters
    ----------
    items : List[str]
        List of LHM fraction labels to be sorted.

    Returns
    -------
    List[str]
        Sorted list of LHM fraction labels following the custom ordering rules.

    Notes
    -----
    The function preserves the relative order of items that are not explicitly
    defined in `top_order` or `lower_order`.
    """
    # --- Remove duplicates while preserving original order ---
    items = list(dict.fromkeys(items))

    # --- Items that should always appear at the top (in this order) ---
    top_order = ["Initial", "Drainage (primair)", "Drainage (secundair)", "RWZI"]

    # --- Items that should always appear at the bottom (in this order) ---
    lower_order = ["Buitenland", "Maaiveld", "Neerslag"]

    result: list[str] = []

    # --- Add top-priority items (only if they exist in the input) ---
    result += [x for x in top_order if x in items]

    # --- Add all remaining items that are not explicitly ordered ---
    mentioned = set(top_order + lower_order)
    result += [x for x in items if x not in mentioned]

    # --- Add bottom-priority items (only if they exist in the input) ---
    result += [x for x in lower_order if x in items]

    return result


def get_lhm_fractions(model: Model) -> list[str]:
    """
    Extract LHM fractions from different concentration tables in the model and sort for consistent plotting.

    This function collects all substances marked as LHM fractions from:
    - Basin concentrations
    - Flow boundary concentrations
    - Level boundary concentrations

    Parameters
    ----------
    model : Model
        Ribasim model object containing concentration data in different components.

    Returns
    -------
    list of str
        List of unique LHM fraction names, including the default "Initial" fraction.
    """
    # --- Initialize with default fraction ---
    fractions = ["Initial"]

    # --- Extract LHM fractions from basin concentrations ---
    basin_df = model.basin.concentration.df
    if basin_df is not None:
        basin_fractions = basin_df[basin_df.meta_lhm_fraction.astype(bool)].substance.unique()
        fractions += list(basin_fractions)

    # --- Extract LHM fractions from flow boundary concentrations ---
    flow_df = model.flow_boundary.concentration.df
    if flow_df is not None:
        flow_fractions = flow_df[flow_df.meta_lhm_fraction.astype(bool)].substance.unique()
        fractions += list(flow_fractions)

    # --- Extract LHM fractions from level boundary concentrations ---
    level_df = model.level_boundary.concentration.df
    if level_df is not None:
        level_fractions = level_df[level_df.meta_lhm_fraction.astype(bool)].substance.unique()
        fractions += list(level_fractions)

    # --- Return combined list ---
    return sort_lhm_fractions(fractions)


def assign_lhm_fractions(
    model: Model,
    secondary_values: set[str] | None = None,
    primary_values: set[str] | None = None,
    metacol: str = "meta_categorie",
    time: datetime | None = None,
) -> None:
    """Assign LHM fraction substances to basin, flow boundary, and level boundary nodes.

    The function creates concentration tables for Ribasim LHM fraction tracing.
    Basin nodes receive fractions for precipitation, primary or secondary drainage,
    and secondary surface runoff. Flow boundary and level boundary nodes receive
    a concentration of 1.0 for a substance derived from their metadata.

    Parameters
    ----------
    model : ribasim.Model
        Ribasim model to update in place.
    secondary_values : set of str, optional
        Values in `metacol` that identify secondary basin nodes.
    primary_values : set of str, optional
        Values in `metacol` that identify primary basin nodes.
    metacol : str, optional
        Column in `model.basin.node.df` used to classify basin nodes.
    time : datetime, optional
        Timestamp to use for all concentration records. If None,
        `model.starttime` is used.

    Returns
    -------
    None, all concentration tables are made in place
    """
    # get all defaults
    if secondary_values is None:
        secondary_values = {"bergend"}
    if primary_values is None:
        primary_values = {"hoofdwater", "doorgaand"}

    if time is None:
        time = model.starttime

    # ---
    # Basin fractions
    # ---
    assert model.basin.node is not None
    basin_nodes = model.basin.node.df
    assert basin_nodes is not None

    primary_basin_node_ids = basin_nodes[basin_nodes[metacol].isin(list(primary_values))].index.to_numpy()

    secondary_basin_node_ids = basin_nodes[basin_nodes[metacol].isin(list(secondary_values))].index.to_numpy()

    all_basin_node_ids = np.concatenate([primary_basin_node_ids, secondary_basin_node_ids])

    basin_tables = [
        pd.DataFrame(
            {
                "node_id": all_basin_node_ids,
                "time": [time] * len(all_basin_node_ids),
                "substance": ["Neerslag"] * len(all_basin_node_ids),
                "drainage": [0.0] * len(all_basin_node_ids),
                "precipitation": [1.0] * len(all_basin_node_ids),
                "surface_runoff": [0.0] * len(all_basin_node_ids),
            }
        ),
        pd.DataFrame(
            {
                "node_id": primary_basin_node_ids,
                "time": [time] * len(primary_basin_node_ids),
                "substance": ["Drainage (primair)"] * len(primary_basin_node_ids),
                "drainage": [1.0] * len(primary_basin_node_ids),
                "precipitation": [0.0] * len(primary_basin_node_ids),
                "surface_runoff": [0.0] * len(primary_basin_node_ids),
            }
        ),
        pd.DataFrame(
            {
                "node_id": secondary_basin_node_ids,
                "time": [time] * len(secondary_basin_node_ids),
                "substance": ["Drainage (secundair)"] * len(secondary_basin_node_ids),
                "drainage": [1.0] * len(secondary_basin_node_ids),
                "precipitation": [0.0] * len(secondary_basin_node_ids),
                "surface_runoff": [0.0] * len(secondary_basin_node_ids),
            }
        ),
        pd.DataFrame(
            {
                "node_id": secondary_basin_node_ids,
                "time": [time] * len(secondary_basin_node_ids),
                "substance": ["Maaiveld"] * len(secondary_basin_node_ids),
                "drainage": [0.0] * len(secondary_basin_node_ids),
                "precipitation": [0.0] * len(secondary_basin_node_ids),
                "surface_runoff": [1.0] * len(secondary_basin_node_ids),
            }
        ),
    ]

    basin_df = pd.concat(
        [df for df in basin_tables if not df.empty],
        ignore_index=True,
    )
    basin_df["meta_lhm_fraction"] = True

    model.basin.concentration.df = BasinConcentrationSchema.validate(basin_df)

    # ---
    # Flow boundary fractions
    # ---
    assert model.flow_boundary.node is not None
    assert model.flow_boundary.node.df is not None
    flow_boundary_df = model.flow_boundary.node.df.reset_index()[["node_id", "meta_categorie"]].rename(
        columns={"meta_categorie": "substance"}
    )

    flow_boundary_df["substance"] = flow_boundary_df["substance"].replace(RENAME_MAP)
    flow_boundary_df["concentration"] = 1.0
    flow_boundary_df["time"] = time
    flow_boundary_df["meta_lhm_fraction"] = True

    model.flow_boundary.concentration.df = FlowBoundaryConcentrationSchema.validate(flow_boundary_df)

    # ---
    # Level boundary fractions
    # ---

    assert model.level_boundary.node is not None
    assert model.level_boundary.node.df is not None
    level_boundary_df = model.level_boundary.node.df.reset_index()[["node_id", "meta_couple_authority"]].rename(
        columns={"meta_couple_authority": "substance"}
    )

    level_boundary_df["substance"] = level_boundary_df["substance"].replace(RENAME_MAP)
    level_boundary_df["concentration"] = 1.0
    level_boundary_df["time"] = time
    level_boundary_df["meta_lhm_fraction"] = True

    model.level_boundary.concentration.df = LevelBoundaryConcentrationSchema.validate(level_boundary_df)

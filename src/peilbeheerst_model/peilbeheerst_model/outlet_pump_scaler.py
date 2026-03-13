from __future__ import annotations

import pathlib
from dataclasses import dataclass, field

import pandas as pd
from ribasim import run_ribasim

from ribasim_nl import CloudStorage, Model

pd.set_option("display.max_columns", None)

__all__ = ["OutletPumpScalingConfig", "scale_outlets_pumps"]


@dataclass
class OutletPumpScalingConfig:
    ribasim_model_path: str | pathlib.Path
    ribasim_model: Model
    from_to_node_function_table: pd.DataFrame
    waterschap: str
    cloud: CloudStorage
    max_iterations: int = 12
    initial_guess_flow_rate_outlet: float = 0.1
    initial_guess_flow_rate_pump: float = 10.0
    design_precipitation_event: float = 10
    design_potential_evaporation_event: float = 1.5
    node_id_exclusion_list: list[int] = field(default_factory=list)
    printing: bool = True
    level_boundary_waterlevel_drainage_situation: float = -999
    level_boundary_waterlevel_demand_situation: float = 999
    max_deviation: float = 0.02
    max_days: int = 5
    max_scaled_flow_rate: float = 50
    add_information_to_from_to_node_function_table: bool = True
    situations: list[str] = field(default_factory=lambda: ["water_demand", "water_drainage"])
    apply_temporary_debug_changes: bool = False
    debug_outlet_max_flow_rate: float = 0.10
    RESCALE_FLOW_CAPACITIES: bool = True

    @property
    def results_path(self) -> pathlib.Path:
        return pathlib.Path(self.ribasim_model_path).parent / "results" / "basin.arrow"


def set_initial_water_levels(ribasim_model):
    """Set basin initial water levels equal to target levels.

    Parameters
    ----------
    ribasim_model : ribasim.Model
        The Ribasim model to update.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        The updated basin state dataframe with initial levels and a basin
        information dataframe containing categories and target levels.
    """
    print("Setting initial water levels based on basin area target levels.")

    # create dataframe with basin information
    target_levels = ribasim_model.basin.area.df.copy()[["node_id", "meta_streefpeil"]]
    bergend_doorgaand = ribasim_model.basin.node.df.copy()[["node_type"]]
    bergend_doorgaand = bergend_doorgaand.merge(
        ribasim_model.basin.state.df[["node_id", "meta_categorie"]], on="node_id", how="left"
    )
    basin_information = bergend_doorgaand.merge(target_levels, on="node_id", how="left")

    # streefpeil of bergende bakje may be missing, extract from nodes and edges by identifying the basin downstream of the downstream connector node
    bergende_basins = bergend_doorgaand.loc[bergend_doorgaand["meta_categorie"] == "bergend"].reset_index(
        drop=False
    )  # select bergende basins
    bergende_basins = bergende_basins.merge(
        ribasim_model.link.df[["from_node_id", "to_node_id"]], left_on="node_id", right_on="from_node_id", how="left"
    )
    bergende_basins = bergende_basins.rename(columns={"to_node_id": "bergende_connector_node_id"}).drop(
        columns=["from_node_id"]
    )
    bergende_basins = bergende_basins.merge(
        ribasim_model.link.df[["from_node_id", "to_node_id"]],
        left_on="bergende_connector_node_id",
        right_on="from_node_id",
        how="left",
    )
    bergende_basins = bergende_basins.rename(columns={"to_node_id": "doorgaande_node_id"}).drop(
        columns=["from_node_id"]
    )

    # add streefpeil to bergende basins based on the downstream doorgaande basin
    doorgaande_levels = target_levels[["node_id", "meta_streefpeil"]].rename(
        columns={"node_id": "doorgaande_node_id", "meta_streefpeil": "meta_streefpeil_doorgaand"}
    )
    bergende_basins = bergende_basins.merge(doorgaande_levels, on="doorgaande_node_id", how="left")

    # supplement missing streefpeil in basin_information using bergende -> downstream doorgaande mapping
    bergende_supplement = (
        bergende_basins[["node_id", "meta_streefpeil_doorgaand"]]
        .dropna(subset=["meta_streefpeil_doorgaand"])
        .drop_duplicates(subset=["node_id"], keep="first")
        .set_index("node_id")["meta_streefpeil_doorgaand"]
    )
    basin_information = basin_information.set_index("node_id", drop=False)
    basin_information["meta_streefpeil"] = basin_information["meta_streefpeil"].combine_first(bergende_supplement)

    # raise error when streefpeil is still missing
    if basin_information["meta_streefpeil"].isnull().any():
        missing_basins = basin_information[basin_information["meta_streefpeil"].isnull()]
        raise ValueError(f"The following basins are missing a meta_streefpeil value:\n{missing_basins}")

    initial_water_level = basin_information[["node_id", "meta_categorie", "meta_streefpeil"]]
    initial_water_level = initial_water_level.rename(columns={"meta_streefpeil": "level"})

    # avoid profile error
    basin_ids = ribasim_model.basin.node.df.loc[
        ribasim_model.basin.node.df.node_type == "Basin", "meta_node_id"
    ].to_numpy()

    ribasim_model.basin.profile = ribasim_model.basin.profile.df.loc[
        ribasim_model.basin.profile.df.node_id.isin(basin_ids)
    ].reset_index(drop=True)

    return initial_water_level, basin_information


def check_known_flow_rate_columns(ribasim_model):
    """Validate presence and completeness of `meta_known_flow_rate` columns.

    Parameters
    ----------
    ribasim_model : ribasim.Model
        The Ribasim model whose pump and outlet static tables are checked.

    Raises
    ------
    ValueError
        If the required column is missing or contains NaN values.
    """
    # check whether column meta_known_flow_rate exists in the outlet and the pump table, if not raise error
    if "meta_known_flow_rate" not in ribasim_model.outlet.static.df.columns:
        raise ValueError("Column 'meta_known_flow_rate' is missing from outlet static data.")
    if "meta_known_flow_rate" not in ribasim_model.pump.static.df.columns:
        raise ValueError("Column 'meta_known_flow_rate' is missing from pump static data.")

    # check whether there are any NaN values in the meta_known_flow_rate column, if so raise error and show which nodes are missing
    if ribasim_model.pump.static.df["meta_known_flow_rate"].isnull().any():
        missing_nodes = ribasim_model.pump.static.df[ribasim_model.pump.static.df["meta_known_flow_rate"].isnull()]
        raise ValueError(
            f"Column 'meta_known_flow_rate' in pump static data contains NaN values for the following nodes:\n{missing_nodes}"
        )
    if ribasim_model.outlet.static.df["meta_known_flow_rate"].isnull().any():
        missing_nodes = ribasim_model.outlet.static.df[ribasim_model.outlet.static.df["meta_known_flow_rate"].isnull()]
        raise ValueError(
            f"Column 'meta_known_flow_rate' in outlet static data contains NaN values for the following nodes:\n{missing_nodes}"
        )


def set_vertical_static_forcing(
    ribasim_model, situation, design_precipitation_event, design_potential_evaporation_event
):
    """Set basin forcing terms for a drainage or demand design ("maatgevende") situation.

    Parameters
    ----------
    ribasim_model : ribasim.Model
        The Ribasim model to update.
    situation : str
        Scenario name, expected to be `water_drainage` or `water_demand`.
    design_precipitation_event : float
        Design precipitation in mm/day.
    design_potential_evaporation_event : float
        Design potential evaporation in mm/day.

    Returns
    -------
    ribasim.Model
        The model with updated basin time forcing.
    """
    # percentage open water may differ. To reduce lines of code: only set drainage and infiltration fluxes, based on size of the basin
    if "meta_area" not in ribasim_model.basin.time.df.columns:
        ribasim_model.basin.area.df["meta_area"] = ribasim_model.basin.area.df.area
        ribasim_model.basin.time.df = ribasim_model.basin.time.df.merge(
            ribasim_model.basin.area.df[["node_id", "meta_area"]], left_on="node_id", right_on="node_id", how="left"
        )

    if situation == "water_drainage":
        precipitation = design_precipitation_event / 1000 / (24 * 3600)  # convert mm/day to m/s
        potential_evaporation = 0.0
    elif situation == "water_demand":
        precipitation = 0.0
        potential_evaporation = design_potential_evaporation_event / 1000 / (24 * 3600)  # convert mm/day to m/s
    else:
        raise ValueError(f"Unknown situation: {situation}. Options are 'water_drainage' and 'water_demand'.")

    # calculate fluxes based on area: m2/s to m3/s
    ribasim_model.basin.time.df["drainage"] = ribasim_model.basin.time.df["meta_area"] * precipitation
    ribasim_model.basin.time.df["infiltration"] = ribasim_model.basin.time.df["meta_area"] * potential_evaporation

    # as the drainage or infiltration is already set, set the other fluxes to zero
    ribasim_model.basin.time.df["precipitation"] = 0
    ribasim_model.basin.time.df["potential_evaporation"] = 0
    ribasim_model.basin.time.df["surface_runoff"] = 0.0

    return ribasim_model


def warn_for_to_high_flow_rates(ribasim_model):
    """Print a warning for pump or outlet nodes with very high flow rates.

    Parameters
    ----------
    ribasim_model : ribasim.Model
        The Ribasim model whose pump and outlet `max_flow_rate` values are checked.

    Notes
    -----
    A warning is printed when any node exceeds 25 m3/s.
    """
    pump_flow_rates = ribasim_model.pump.static.df[["node_id", "max_flow_rate"]]
    outlet_flow_rates = ribasim_model.outlet.static.df[["node_id", "max_flow_rate"]]
    flow_rates = pd.concat([pump_flow_rates, outlet_flow_rates], ignore_index=True)
    high_flow_rates = flow_rates[flow_rates["max_flow_rate"] > 25]
    if not high_flow_rates.empty:
        print("Warning: The following nodes have a max_flow_rate exceeding 25 m3/s:")
        print(high_flow_rates)


def update_from_to_node_function_table_with_new_flow_rate(
    from_to_node_function_table: pd.DataFrame,
    iteration: int,
    situation: str,
    column_name_direction: str,
) -> pd.DataFrame:
    """Add a new guessed flow-rate column based on earlier iteration results.

    The output column is named `new_max_flow_rates_[iteration]_[situation]`.

    Parameters
    ----------
    from_to_node_function_table : pd.DataFrame
        Connector-node table with current and historical flow-rate guesses.
    iteration : int
        Iteration number used in the output column name.
    situation : str
        Scenario name used in the output column name (water_drainage or water_demand).
    column_name_direction : str
        Column containing scaling directions such as `higher`, `lower`, or `equal`.

    Returns
    -------
    pd.DataFrame
        The input table with one additional guessed flow-rate column.
    """
    column_name_new_flow_rate = "new_max_flow_rates_" + str(iteration) + "_" + situation

    # Find all historical new_max_flow_rates columns for this situation
    history_prefix = "new_max_flow_rates_"
    history_suffix = "_" + situation
    history_columns = [
        c for c in from_to_node_function_table.columns if c.startswith(history_prefix) and c.endswith(history_suffix)
    ]

    # Sort history by iteration number in the column name
    def _iteration_from_column(col_name: str) -> int:
        return int(col_name.replace(history_prefix, "").replace(history_suffix, ""))

    history_columns = sorted(history_columns, key=_iteration_from_column)

    # Create output column for this iteration
    from_to_node_function_table[column_name_new_flow_rate] = pd.NA

    for row_idx in from_to_node_function_table.index:
        direction_value = from_to_node_function_table.at[row_idx, column_name_direction]

        # Collect non-null historical values for this row
        row_history = []
        for col in history_columns:
            val = from_to_node_function_table.at[row_idx, col]
            if pd.notna(val):
                row_history.append(float(val))

        # No history yet: fall back to current max_flow_rate if present
        if len(row_history) == 0:
            base_value = from_to_node_function_table.at[row_idx, "max_flow_rate"]
            if pd.notna(base_value):
                from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = float(base_value)
            continue

        latest_value = row_history[-1]
        preceding_values = row_history[:-1]

        # Only one known value so far: expand directly by direction
        if len(preceding_values) == 0:
            if direction_value == "higher":
                from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = latest_value * 2.0
            elif direction_value == "lower":
                from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = latest_value / 2.0
            continue

        if direction_value == "higher":
            if latest_value > max(preceding_values):
                from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = latest_value * 2.0
            else:
                values_above = [v for v in preceding_values if v > latest_value]
                if len(values_above) > 0:
                    closest_above = min(values_above)
                    from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = (
                        latest_value * closest_above
                    ) ** 0.5
                else:
                    from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = latest_value * 2.0

        elif direction_value == "lower":
            if latest_value < min(preceding_values):
                from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = latest_value / 2.0
            else:
                values_below = [v for v in preceding_values if v < latest_value]
                if len(values_below) > 0:
                    closest_below = max(values_below)
                    from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = (
                        latest_value * closest_below
                    ) ** 0.5
                else:
                    from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = latest_value / 2.0

        else:
            from_to_node_function_table.at[row_idx, column_name_new_flow_rate] = latest_value

    return from_to_node_function_table


def overwrite_demand_values_with_drainage_values(from_to_node_function_table):
    """Ensure demand guesses do not fall below the highest drainage guess.

    Parameters
    ----------
    from_to_node_function_table : pd.DataFrame
        Connector-node table with guessed flow-rate columns.

    Returns
    -------
    pd.DataFrame
        The updated table with demand values overwritten where needed.
    """
    # determine all column names for drainage
    drainage_columns = [
        c
        for c in from_to_node_function_table.columns
        if c.startswith("new_max_flow_rates_") and c.endswith("_water_drainage")
    ]

    # determine highest drainage value for each row
    from_to_node_function_table["max_drainage_value"] = from_to_node_function_table[drainage_columns].max(axis=1)

    # determine the latest column name for demand
    demand_columns = [
        c
        for c in from_to_node_function_table.columns
        if c.startswith("new_max_flow_rates_") and c.endswith("_water_demand")
    ]

    # only replace value if a demand situation has already been processed
    if len(demand_columns) > 0:
        latest_demand_column = demand_columns[-1]

        # overwrite demand values with drainage values
        from_to_node_function_table.loc[
            from_to_node_function_table[latest_demand_column] < from_to_node_function_table["max_drainage_value"],
            latest_demand_column,
        ] = from_to_node_function_table["max_drainage_value"]

    return from_to_node_function_table


def overwrite_guessed_flow_rates_if_not_allowed_to_scale(from_to_node_function_table):
    """Restore original flow rates for nodes that are marked as not scalable.

    Parameters
    ----------
    from_to_node_function_table : pd.DataFrame
        Connector-node table with guessed flow-rate columns and scaling flags.

    Returns
    -------
    pd.DataFrame
        The updated table with restricted nodes reset to `max_flow_rate`.
    """
    # retrieve the last column name starting with "new_max_flow_rates_"
    new_flow_rate_columns = [c for c in from_to_node_function_table.columns if c.startswith("new_max_flow_rates_")]
    if len(new_flow_rate_columns) == 0:
        return from_to_node_function_table

    last_new_flow_rate_column = new_flow_rate_columns[-1]

    mask_false = from_to_node_function_table["allowed_to_scale"].eq(False)
    from_to_node_function_table.loc[mask_false, last_new_flow_rate_column] = from_to_node_function_table[
        "max_flow_rate"
    ]

    return from_to_node_function_table


def cap_guessed_flow_rates_at_maximum(from_to_node_function_table, max_scaled_flow_rate):
    """Cap guessed flow rates at a defined maximum to prevent unrealistic values.

    Parameters
    ----------
    from_to_node_function_table : pd.DataFrame
        Connector-node table with guessed flow-rate columns.
    max_scaled_flow_rate : float
        The maximum allowed flow rate in m3/s.

    Returns
    -------
    pd.DataFrame
        The updated table with guessed flow rates capped at the specified maximum.
    """
    # retrieve the last column name starting with "new_max_flow_rates_"
    new_flow_rate_columns = [c for c in from_to_node_function_table.columns if c.startswith("new_max_flow_rates_")]
    if len(new_flow_rate_columns) == 0:
        return from_to_node_function_table

    last_new_flow_rate_column = new_flow_rate_columns[-1]

    # cap the guessed flow rates at the defined maximum
    from_to_node_function_table.loc[
        from_to_node_function_table[last_new_flow_rate_column] > max_scaled_flow_rate, last_new_flow_rate_column
    ] = max_scaled_flow_rate

    return from_to_node_function_table


def update_max_flow_rates_in_ribasim_model(ribasim_model, from_to_node_function_table):
    """Copy the latest guessed connector flow rates into the Ribasim model.

    Parameters
    ----------
    ribasim_model : ribasim.Model
        The model whose pump and outlet static tables are updated.
    from_to_node_function_table : pd.DataFrame
        Connector-node table containing guessed flow-rate columns.

    Returns
    -------
    ribasim.Model
        The model with updated pump and outlet `max_flow_rate` values.
    """
    # retrieve the last column name starting with "new_max_flow_rates_"
    new_flow_rate_columns = [c for c in from_to_node_function_table.columns if c.startswith("new_max_flow_rates_")]
    if len(new_flow_rate_columns) == 0:
        return ribasim_model
    last_new_flow_rate_column = new_flow_rate_columns[-1]

    # select only the relevant columns from from_to_node_function_table
    flow_rate_updates = from_to_node_function_table[["node_id", last_new_flow_rate_column]]
    flow_rate_updates = flow_rate_updates.rename(columns={last_new_flow_rate_column: "max_flow_rate"})
    flow_rate_updates = flow_rate_updates.dropna(subset=["max_flow_rate"])
    flow_rate_updates = flow_rate_updates.drop_duplicates(subset=["node_id"], keep="last")
    flow_rate_updates = flow_rate_updates.set_index("node_id")["max_flow_rate"]

    # update the max_flow_rate in the ribasim_model for the pump and outlet nodes.
    pump_df = ribasim_model.pump.static.df.copy()
    pump_df["max_flow_rate"] = pump_df["node_id"].map(flow_rate_updates).fillna(pump_df["max_flow_rate"])
    ribasim_model.pump.static.df = pump_df

    outlet_df = ribasim_model.outlet.static.df.copy()
    outlet_df["max_flow_rate"] = outlet_df["node_id"].map(flow_rate_updates).fillna(outlet_df["max_flow_rate"])
    ribasim_model.outlet.static.df = outlet_df

    return ribasim_model


def upload_from_to_node_function_table(from_to_node_function_table, waterschap):
    """Save the from_to_node_function_table with estimated flow rates to a CSV file.

    Parameters
    ----------
    from_to_node_function_table : pd.DataFrame
        Connector-node table containing estimated flow-rate columns.
    """
    cloud = CloudStorage()

    # Upload from_to_node_function_table to waterschap/verwerkt
    from_to_node_function_table_path = cloud.joinpath(
        waterschap, "verwerkt", "Parametrisatie_data", "from_to_node_function_table_scaled_max_flow_rates.csv"
    )
    from_to_node_function_table.to_csv(from_to_node_function_table_path)
    cloud.upload_file(from_to_node_function_table_path)

    print("from_to_node_function_table with estimated flow rates saved to the GoodCloud.")


class _OutletPumpScaler:
    def __init__(self, config: OutletPumpScalingConfig):
        self.config = config

    def run(self):
        config = self.config

        # load model, create df with basin information, set node_id as index
        ribasim_model = config.ribasim_model
        from_to_node_function_table = (
            config.from_to_node_function_table
        )  # table with from_node_id, to_node_id and function (drain, supply, flow_control) for each connector node
        original_meteo = ribasim_model.basin.time.df.copy()  # will be temporarily modified
        original_initial_waterlevels = ribasim_model.basin.state.df.copy()  # will be temporarily modified
        original_from_to_node_function_table = from_to_node_function_table.copy()  # will be temporarily modified
        original_level_boundary_static = ribasim_model.level_boundary.static.df.copy()
        original_level_boundary_time = ribasim_model.level_boundary.time.df.copy()

        # WEGHALEN #########################################################
        if config.apply_temporary_debug_changes:
            ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
            ribasim_model.pump.static.df["meta_known_flow_rate"] = True
            ribasim_model.pump.static.df.loc[
                ribasim_model.pump.static.df.max_flow_rate == 10 / 60, "meta_known_flow_rate"
            ] = False

            # also temp
            # ribasim_model.pump.static.df.max_flow_rate = 10
            ribasim_model.outlet.static.df.max_flow_rate = config.debug_outlet_max_flow_rate

            # if max_flow_rate is 0, change to 0.1
            ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.max_flow_rate == 0, "max_flow_rate"] = 0.1
            ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.max_flow_rate == 0, "max_flow_rate"] = 0.1

        ###########################

        situations = config.situations
        results_path = config.results_path
        max_iterations = config.max_iterations
        initial_guess_flow_rate_outlet = config.initial_guess_flow_rate_outlet
        initial_guess_flow_rate_pump = config.initial_guess_flow_rate_pump
        design_precipitation_event = config.design_precipitation_event
        design_potential_evaporation_event = config.design_potential_evaporation_event
        node_id_exclusion_list = config.node_id_exclusion_list
        printing = config.printing
        level_boundary_waterlevel_drainage_situation = config.level_boundary_waterlevel_drainage_situation
        level_boundary_waterlevel_demand_situation = config.level_boundary_waterlevel_demand_situation
        max_deviation = config.max_deviation
        max_days = config.max_days
        max_scaled_flow_rate = config.max_scaled_flow_rate
        add_information_to_from_to_node_function_table = config.add_information_to_from_to_node_function_table

        # Set initial conditions equal to the target levels
        initial_water_level, basin_information = set_initial_water_levels(ribasim_model)

        # determine two df's for the downstream + upstream connector nodes which should be used in the scaling
        outlet_nodes = ribasim_model.outlet.static.df[["node_id", "meta_known_flow_rate"]].copy()
        pump_nodes = ribasim_model.pump.static.df[["node_id", "meta_known_flow_rate"]].copy()

        # concat the pump and outlet nodes into one dataframe, and merge with the from_to_node_function_table to add the meta_known_flow_rate information to the from_to_node_function_table
        connector_nodes_to_scale = pd.concat([pump_nodes, outlet_nodes], ignore_index=True)
        from_to_node_function_table = from_to_node_function_table.merge(
            connector_nodes_to_scale, on="node_id", how="left"
        )

        # determine which nodes are allowed to be scaled
        from_to_node_function_table["allowed_to_scale"] = True
        from_to_node_function_table["max_drainage_value"] = None
        from_to_node_function_table.loc[
            from_to_node_function_table["node_id"].isin(node_id_exclusion_list), "allowed_to_scale"
        ] = False
        from_to_node_function_table.loc[from_to_node_function_table["meta_known_flow_rate"], "allowed_to_scale"] = False

        # add max_flow_rate of pump and outlet nodes to from_to_node_function_table
        max_flow_rate_df = pd.concat(
            [
                ribasim_model.pump.static.df[["node_id", "max_flow_rate"]].drop_duplicates(subset=["node_id"]),
                ribasim_model.outlet.static.df[["node_id", "max_flow_rate"]].drop_duplicates(subset=["node_id"]),
            ],
            ignore_index=True,
        )
        from_to_node_function_table = from_to_node_function_table.merge(max_flow_rate_df, on="node_id", how="left")

        for situation in situations:  # loop through drainage (afvoer) and demand (aanvoer) situation
            first_iteration = True

            # dont let gravity pose a problem for the level boundaries
            if situation == "water_drainage":
                ribasim_model.level_boundary.time.df["level"] = level_boundary_waterlevel_drainage_situation
            elif situation == "water_demand":
                ribasim_model.level_boundary.time.df["level"] = level_boundary_waterlevel_demand_situation
            ribasim_model.level_boundary.static.df = None

            # loop through each iteration
            for iteration in range(max_iterations):
                if printing:
                    print(f"Starting iteration {iteration + 1}/{max_iterations} for situation: {situation}")

                if first_iteration:
                    ribasim_model = set_vertical_static_forcing(
                        ribasim_model, situation, design_precipitation_event, design_potential_evaporation_event
                    )
                    ribasim_model.basin.state.df = initial_water_level

                    # check if known_flow_rate columns exist and filled correctly
                    check_known_flow_rate_columns(ribasim_model)

                    # Set initial guess flow_rate for outlets and pumps with unknown flow_rate
                    if printing:
                        print("Replacing initial guess flow rates for outlets and pumps with unknown flow rates.")

                    ribasim_model.outlet.static.df.loc[
                        ~ribasim_model.outlet.static.df["meta_known_flow_rate"], "max_flow_rate"
                    ] = initial_guess_flow_rate_outlet
                    ribasim_model.pump.static.df.loc[
                        ~ribasim_model.pump.static.df["meta_known_flow_rate"], "max_flow_rate"
                    ] = initial_guess_flow_rate_pump

                    # write model
                    if printing:
                        print(
                            "Writing updated Ribasim model with: \n - (temporarily) changed initial water levels \n - (temporarily) changed vertical fluxes \n - initial guessed flow rates."
                        )
                    ribasim_model.write(config.ribasim_model_path)
                    first_iteration = (
                        False  # avoid resetting initial water levels and initial guess flow rates in next iterations
                    )

                # run simulation
                if printing:
                    print(f"Running Ribasim simulation: {iteration + 1}/{max_iterations} for situation: {situation}")

                run_ribasim(toml_path=config.ribasim_model_path)

                # extract results, only select relevant columns, merge streefpeil to node_id
                ribasim_water_levels = pd.read_feather(results_path)
                ribasim_water_levels = ribasim_water_levels[["time", "node_id", "level"]]
                ribasim_water_levels = ribasim_water_levels.merge(
                    basin_information[["meta_streefpeil"]], left_on="node_id", right_index=True, how="left"
                )

                ### basin analysis ###
                # determine which basins exceed the maximum allowed deviation and duration from the streefpeil
                column_name_iteration = "exceeds_deviation_duration_iteration_" + str(iteration) + "_" + situation
                ribasim_water_levels["deviation"] = (
                    ribasim_water_levels["level"] - ribasim_water_levels["meta_streefpeil"]
                )

                if (
                    situation == "water_drainage"
                ):  # in drainage situation, check where water levels are too high, so deviation is above the max_deviation threshold
                    ribasim_water_levels[column_name_iteration] = ribasim_water_levels["deviation"] > max_deviation
                elif (
                    situation == "water_demand"
                ):  # in demand situation, check where water levels are too low, so deviation is below the negative max_deviation threshold
                    ribasim_water_levels[column_name_iteration] = ribasim_water_levels["deviation"] < -max_deviation

                # group by basin and determine for how many timesteps the deviation is exceeded
                basin_exceedance = ribasim_water_levels.groupby("node_id")[column_name_iteration].sum().reset_index()
                basin_exceedance = basin_exceedance.merge(
                    basin_information[["meta_streefpeil"]], left_on="node_id", right_index=True, how="left"
                )

                # determine which basins needs to be scaled higher or lower based on the exceeds_deviation_duration_iteration
                column_name_direction = "scale_direction_iteration_" + str(iteration) + "_" + situation
                basin_exceedance[column_name_direction] = None
                basin_exceedance.loc[basin_exceedance[column_name_iteration] > max_days, column_name_direction] = (
                    "higher"  # if the deviation is exceeded for more than max_days, the flow rate should be scaled higher
                )
                basin_exceedance.loc[basin_exceedance[column_name_iteration] <= max_days, column_name_direction] = (
                    "lower"  # if lower, then the flow rate can be set lower
                )

                ### bridge basin --> connector nodes ###
                # add the information to the from_to_node_function_table, based on the situation (drainage: checking downstream nodes, demand: checking upstream nodes)
                if situation == "water_drainage":
                    from_to_node_function_table = from_to_node_function_table.merge(
                        basin_exceedance.set_index("node_id")[[column_name_iteration, column_name_direction]],
                        left_on="from_node_id",
                        right_index=True,
                        how="left",
                    )
                elif situation == "water_demand":
                    from_to_node_function_table = from_to_node_function_table.merge(
                        basin_exceedance.set_index("node_id")[[column_name_iteration, column_name_direction]],
                        left_on="to_node_id",
                        right_index=True,
                        how="left",
                    )

                # if drainage situation, do not scale supply nodes, and vice versa.
                if situation == "water_drainage":
                    from_to_node_function_table.loc[
                        from_to_node_function_table.function.isin(["supply"]), column_name_direction
                    ] = "equal"
                elif situation == "water_demand":
                    from_to_node_function_table.loc[
                        from_to_node_function_table.function.isin(["drain"]), column_name_direction
                    ] = "equal"

                ### Bisection method ###
                from_to_node_function_table = update_from_to_node_function_table_with_new_flow_rate(
                    from_to_node_function_table=from_to_node_function_table,
                    iteration=iteration + 1,
                    situation=situation,
                    column_name_direction=column_name_direction,
                )

                # if lower values have been assigned for the water_demand situation, then overwrite the max_flow_rate with the value from water_drainage
                from_to_node_function_table = overwrite_demand_values_with_drainage_values(from_to_node_function_table)

                # if there are nodes which are not allowed to be scaled, overwrite the guessed flow rates with the original max_flow_rate from the ribasim model
                from_to_node_function_table = overwrite_guessed_flow_rates_if_not_allowed_to_scale(
                    from_to_node_function_table
                )

                # cap the max_flow_rate at the defined maximum
                from_to_node_function_table = cap_guessed_flow_rates_at_maximum(
                    from_to_node_function_table, max_scaled_flow_rate
                )

                # update the max_flow_rate in the ribasim_model based on the new flow rates in the from_to_node_function_table
                ribasim_model = update_max_flow_rates_in_ribasim_model(ribasim_model, from_to_node_function_table)

                # set flow rate equal to max flow rate
                ribasim_model.outlet.static.df.flow_rate = ribasim_model.outlet.static.df.max_flow_rate
                ribasim_model.pump.static.df.flow_rate = ribasim_model.pump.static.df.max_flow_rate

                # store model
                ribasim_model.write(config.ribasim_model_path)

        # replace the original meteo, initial water levels and boundary levels in the ribasim model
        ribasim_model.basin.time.df = original_meteo
        ribasim_model.basin.state.df = original_initial_waterlevels
        ribasim_model.level_boundary.time.df = original_level_boundary_time
        ribasim_model.level_boundary.static.df = original_level_boundary_static

        if not add_information_to_from_to_node_function_table:
            from_to_node_function_table = original_from_to_node_function_table

        # raise warning if the newest max_flow_rates exceed the 25 m3/s
        warn_for_to_high_flow_rates(ribasim_model)

        print("Done")

        # upload the from_to_node_function_table with scaled flow rates to the goodcloud
        upload_from_to_node_function_table(from_to_node_function_table, config.waterschap)

        return ribasim_model, from_to_node_function_table


def load_from_to_node_function_table_from_goodcloud(config: OutletPumpScalingConfig):
    cloud = config.cloud
    scaled_max_flow_rates_path = cloud.joinpath(
        config.waterschap, "verwerkt", "Parametrisatie_data", "from_to_node_function_table_scaled_max_flow_rates.csv"
    )
    cloud.synchronize([scaled_max_flow_rates_path])

    return pd.read_csv(scaled_max_flow_rates_path)


def scale_outlets_pumps(config: OutletPumpScalingConfig):
    """Run outlet and pump max-flow scaling using the provided configuration."""
    if config.RESCALE_FLOW_CAPACITIES:
        return _OutletPumpScaler(config).run()
    else:
        from_to_node_function_table = load_from_to_node_function_table_from_goodcloud(config)

        # update the max_flow_rate in the ribasim_model based on the new flow rates in the from_to_node_function_table
        ribasim_model = update_max_flow_rates_in_ribasim_model(config.ribasim_model, from_to_node_function_table)
        return ribasim_model, from_to_node_function_table

import pathlib

import pandas as pd
from ribasim import run_ribasim

from ribasim_nl import Model

# assumptions: meta_wateraanvoer column exists in basin area dataframe
# assumptions: meta_aanvoer, meta_func_afvoer, meta_func_aanvoer exists in outlet.static and pump.static dataframes
# assumptions: all outlets can always be used to drain water, and can only be used to supply water when meta_aanvoer == True


def set_initial_water_levels(ribasim_model):
    """Set initial water levels in the ribasim model based on target levels in basin areas.

    Parameters
    ----------
    ribasim_model : ribasim.Model
        The Ribasim model to set initial water levels for.
    max_iterations : int, optional
        The maximum number of iterations to perform, by default 10.
    """
    print("Setting initial water levels based on basin area target levels.")

    # create dataframe with basin information
    target_levels = ribasim_model.basin.area.df.copy()[["meta_streefpeil", "meta_aanvoer"]]
    bergend_doorgaand = ribasim_model.basin.node.df.copy()[["node_type", "meta_categorie"]]
    basin_information = bergend_doorgaand.merge(target_levels, left_index=True, right_index=True, how="left")

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

    # add streefpeil to bergende basins
    bergende_basins = bergende_basins.merge(
        target_levels[["meta_streefpeil"]],
        left_on="doorgaande_node_id",
        right_index=True,
        how="left",
    )

    # merge to get streefpeil of the bergende basin from the downstream doorgaande basin
    basin_information["meta_streefpeil"] = basin_information["meta_streefpeil"].combine_first(
        bergende_basins.set_index("node_id")["meta_streefpeil"]
    )

    # raise error when streefpeil is still missing
    if basin_information["meta_streefpeil"].isnull().any():
        missing_basins = basin_information[basin_information["meta_streefpeil"].isnull()]
        raise ValueError(f"The following basins are missing a meta_streefpeil value:\n{missing_basins}")

    initial_water_level = ribasim_model.basin.state.df.copy()
    initial_water_level = initial_water_level.drop(columns=["level"])
    initial_water_level = initial_water_level.merge(
        basin_information[["meta_streefpeil"]], left_on="node_id", right_index=True, how="left"
    )
    initial_water_level = initial_water_level.rename(columns={"meta_streefpeil": "level"})

    return initial_water_level, basin_information


def check_known_flow_rate_columns(ribasim_model):
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
    # if situation == 'water_drainage':
    #     precipitation = design_precipitation_event / 1000 / 24 * 3600 #convert mm/day to m/s
    #     potential_evaporation = 0.0
    # elif situation == 'water_demand':
    #     precipitation = 0.0
    #     potential_evaporation = design_potential_evaporation_event / 1000 / 24 * 3600 #convert mm/day to m/s
    # else:
    #     raise ValueError(f"Unknown situation: {situation}. Options are 'water_drainage' and 'water_demand'.")

    # percentage open water may differ. To reduce lines of code: only set drainage and infiltration fluxes, based on size of the basin
    ribasim_model.basin.area.df["meta_area"] = ribasim_model.basin.area.df.area
    ribasim_model.basin.time.df = ribasim_model.basin.time.df.merge(
        ribasim_model.basin.area.df[["node_id", "meta_area"]], left_on="node_id", right_on="node_id", how="left"
    )

    if situation == "water_drainage":
        precipitation = design_precipitation_event / 1000 / 24 * 3600  # convert mm/day to m/s
        potential_evaporation = 0.0
    elif situation == "water_demand":
        precipitation = 0.0
        potential_evaporation = design_potential_evaporation_event / 1000 / 24 * 3600  # convert mm/day to m/s
    else:
        raise ValueError(f"Unknown situation: {situation}. Options are 'water_drainage' and 'water_demand'.")

    # calculate fluxes based on area: m2/s to m3/s
    ribasim_model.basin.time.df["drainage"] = ribasim_model.basin.time.df["meta_area"] * precipitation
    ribasim_model.basin.time.df["infiltration"] = ribasim_model.basin.time.df["meta_area"] * potential_evaporation

    # as the drainage or infiltration is already set, set the other fluxes to zero
    ribasim_model.basin.time.df["precipitation"] = precipitation
    ribasim_model.basin.time.df["potential_evaporation"] = potential_evaporation
    ribasim_model.basin.time.df["surface_runoff"] = 0.0

    return ribasim_model


# paths and parameters
ribasim_model_path = r"D:\Users\Bruijns\Documents\PR4750_30\Delfland_parameterized_2026_1_1\ribasim.toml"
results_path = pathlib.Path(ribasim_model_path).parent / "results" / "basin.arrow"
max_iterations = 10
initial_guess_flow_rate = 0.1  # m3/s, will be updated iteratively
design_precipitation_event = 15  # mm/day
design_potential_evaporation_event = 5  # mm/day
printing = True

# load model, create df with basin information, set node_id as index
ribasim_model = Model.read(ribasim_model_path)
original_meteo = ribasim_model.basin.time.df.copy()  # will be temporarily modified
original_initial_waterlevels = ribasim_model.basin.state.df.copy()  # will be temporarily modified

# WEGHALEN #########################################################
ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df["meta_known_flow_rate"] = True
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.max_flow_rate == 10 / 60, "meta_known_flow_rate"] = False
###########################

situations = ["water_drainage", "water_demand"]

# Set initial conditions equal to the target levels
initial_water_level, basin_information = set_initial_water_levels(ribasim_model)

# determine two df's for the downstream + upstream connector nodes which should be used in the scaling
doorgaand_hoofdwater_basins = basin_information.loc[basin_information["meta_categorie"] != "bergend"]
outlet_nodes = ribasim_model.outlet.static.df[["node_id", "meta_aanvoer", "meta_known_flow_rate"]].copy()
pump_nodes = ribasim_model.pump.static.df[
    ["node_id", "meta_func_afvoer", "meta_func_aanvoer", "meta_known_flow_rate"]
].copy()

# change 1 to True and 0 to False in meta_aanvoer, meta_func_afvoer, meta_func_aanvoer columns
outlet_nodes = outlet_nodes.astype({"meta_aanvoer": "bool"})
pump_nodes = pump_nodes.astype({"meta_func_afvoer": "bool", "meta_func_aanvoer": "bool"})

# only select connector nodes with(out) meta_known_flow_rate == False, as only these need to be scaled
outlet_nodes_to_scale = outlet_nodes.loc[~outlet_nodes["meta_known_flow_rate"]]
pump_nodes_to_scale = pump_nodes.loc[~pump_nodes["meta_known_flow_rate"]]

# assume that all outlet nodes can always be used for drainage, and only those with meta_aanvoer == True can be used for supply
drainage_connector_nodes = pd.concat(
    [pump_nodes_to_scale.loc[pump_nodes_to_scale["meta_func_afvoer"]], outlet_nodes_to_scale]
)
supply_connector_nodes = pd.concat(
    [
        pump_nodes_to_scale.loc[pump_nodes_to_scale["meta_func_aanvoer"]],
        outlet_nodes_to_scale.loc[outlet_nodes_to_scale["meta_aanvoer"]],
    ]
)

# TO DO
# exclude connector nodes defined by de user
# run first simulation, check where waterlevels are not met
# scale only the connector nodes that influence these basins which are also in the drainage_connector_nodes or supply_connector_nodes dataframes
# use bisection method to find optimal scaling factor for these nodes
# rerun simulation with scaled nodes

for situation in situations:
    first_iteration = True

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

            ribasim_model.outlet.static.df.loc[~ribasim_model.outlet.static.df["meta_known_flow_rate"], "flow_rate"] = (
                initial_guess_flow_rate
            )
            ribasim_model.pump.static.df.loc[~ribasim_model.pump.static.df["meta_known_flow_rate"], "flow_rate"] = (
                initial_guess_flow_rate
            )
            # write model
            if printing:
                print(
                    "Writing updated Ribasim model with: \n - (temporarily) changed initial water levels \n - (temporarily) changed vertical fluxes \n - initial guess flow rates."
                )
            ribasim_model.write(ribasim_model_path)
            first_iteration = (
                False  # avoid resetting initial water levels and initial guess flow rates in next iterations
            )

        # run simulation
        if printing:
            print(f"Running Ribasim simulation: {iteration + 1}/{max_iterations} for situation: {situation}")

        run_ribasim(toml_path=ribasim_model_path)

        # extract results
        ribasim_water_levels = pd.read_feather(results_path)


#

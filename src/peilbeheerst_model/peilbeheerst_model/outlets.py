import os
import subprocess
import warnings

import pandas as pd
import yaml
from ribasim import Model, Node
from ribasim.nodes import basin, discrete_control, level_boundary, outlet, pump
from shapely.geometry import Point

warnings.filterwarnings("ignore")


class case1:
    """
    Create the first Ribasim schematisation with outlets.

    It contains the simple case of a boezem, two cascaded peilgebieden with an outlet in between,
    after which the water is pumped through a Pump node to the boezem again.
    """

    def __init__(self, case_example_name):
        """Initialize the class. Convert the forcing from mm/day to m/s."""
        case_example_path = os.path.join("../../../../Outlet_tests/json", case_example_name + ".json")

        with open(case_example_path) as file:
            characteristics = yaml.safe_load(file)

        self.characteristics = characteristics

        # add the example and case to the characteristics
        case, example = case_example_name.split("_")

        self.characteristics["case"] = case
        self.characteristics["example"] = example

        self.characteristics["evaporation"] = (
            self.characteristics["evaporation"] / 1000 / 3600 / 24
        )  # convert from mm/day to m/s
        self.characteristics["precipitation"] = (
            self.characteristics["precipitation"] / 1000 / 3600 / 24
        )  # convert from mm/day to m/s

    def create_empty_model(self):
        """Create an empty Ribasim model."""
        model = Model(
            starttime=self.characteristics["starttime"],
            endtime=self.characteristics["endtime"],
            crs=self.characteristics["crs"],
        )
        return model

    def add_boezems(self, model):
        """Add the boezems to the model, based on the pre-defined model characteristics."""
        # add the boezems
        model.level_boundary.add(
            Node(node_id=1, geometry=Point(0.0, 0.0), name="boezem1"),
            [level_boundary.Static(level=[self.characteristics["boezem1_level"]])],
        )

        model.level_boundary.add(
            Node(node_id=2, geometry=Point(6.0, 0.0), name="boezem2"),
            [level_boundary.Static(level=[self.characteristics["boezem2_level"]])],
        )
        return model

    def add_peilgebieden(self, model):
        """Add the peilgebieden to the model, based on the pre-defined model characteristics."""
        # add peilgebied1
        basin1_data = [
            basin.Profile(
                area=self.characteristics["basin1_profile_area"], level=self.characteristics["basin1_profile_level"]
            ),
            basin.Time(
                time=pd.date_range(self.characteristics["starttime"], self.characteristics["endtime"]),
                drainage=0.0,
                potential_evaporation=self.characteristics["evaporation"],
                infiltration=0.0,
                precipitation=self.characteristics["precipitation"],
            ),
            basin.State(level=self.characteristics["basin1_initial_level"]),
        ]

        model.basin.add(Node(node_id=3, geometry=Point(2.0, 0.0), name="peilgebied1"), basin1_data)

        # add peilgebied2
        basin2_data = [
            basin.Profile(
                area=self.characteristics["basin2_profile_area"], level=self.characteristics["basin2_profile_level"]
            ),
            basin.Time(
                time=pd.date_range(self.characteristics["starttime"], self.characteristics["endtime"]),
                drainage=0.0,
                potential_evaporation=self.characteristics["evaporation"],
                infiltration=0.0,
                precipitation=self.characteristics["precipitation"],
            ),
            basin.State(level=self.characteristics["basin2_initial_level"]),
        ]

        model.basin.add(Node(node_id=4, geometry=Point(4.0, 0.0), name="peilgebied2"), basin2_data)

        return model

    def add_connection_nodes(self, model):
        """Add the "connection nodes" to the model (Outlets, Pumps), based on the pre-defined model characteristics."""
        # add the connection nodes
        (
            model.outlet.add(
                Node(node_id=5, geometry=Point(1.0, 0.0), name="Outlet1"),
                [
                    outlet.Static(
                        control_state=["pass", "block"],
                        flow_rate=[self.characteristics["outlet1_flow_rate"], 0],
                        min_crest_level=self.characteristics["outlet1_min_crest_level"],
                    )
                ],
            ),
        )

        model.outlet.add(
            Node(node_id=6, geometry=Point(3.0, 0.0), name="Outlet2"),
            [
                outlet.Static(
                    control_state=["pass", "block"],
                    flow_rate=[self.characteristics["outlet2_flow_rate"], 0],
                    min_crest_level=self.characteristics["outlet2_min_crest_level"],
                )
            ],
        )

        model.pump.add(
            Node(node_id=7, geometry=Point(5.0, 0.0), name="Pump1"),
            [pump.Static(control_state=["pass", "block"], flow_rate=[self.characteristics["pump1_flow_rate"], 0])],
        )

        return model

    def add_discrete_control(self, model):
        """Add discrete control for each "connecton node", such as Pumps and Outlets."""
        # add the discrete control between the boezem and the basin
        model.discrete_control.add(
            Node(node_id=100, geometry=Point(1, 1), name="Outlet_DC_1"),
            [
                discrete_control.Variable(
                    compound_variable_id=1,
                    listen_node_id=[1, 3],
                    listen_node_type=["LevelBoundary", "Basin"],
                    variable=["level", "Level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=1,
                    greater_than=[2.95, self.characteristics["basin1_target_level"]],
                ),
                discrete_control.Logic(
                    truth_state=["FF", "FT", "TF", "TT"],
                    control_state=[
                        "block",
                        "block",
                        "pass",
                        "block",
                    ],  # block 1 & 2: block when boezem level drops below 2.95
                ),
            ],
        )

        # add the discrete control between the basins
        model.discrete_control.add(
            Node(node_id=101, geometry=Point(3, 1), name="Outlet_DC_2"),
            [
                discrete_control.Variable(
                    compound_variable_id=2,
                    listen_node_id=[3, 4],
                    listen_node_type=["Basin", "Basin"],
                    variable=["level", "level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=2,
                    greater_than=[
                        self.characteristics["basin1_target_level"] - 0.05,
                        self.characteristics["basin2_target_level"],
                    ],
                ),
                discrete_control.Logic(
                    truth_state=["FF", "FT", "TF", "TT"], control_state=["block", "block", "pass", "block"]
                ),
            ],
        )

        # add the discrete control between the basin and the boezem
        model.discrete_control.add(
            Node(node_id=102, geometry=Point(5, 1), name="Outlet_DC_1"),
            [
                discrete_control.Variable(
                    compound_variable_id=3,
                    listen_node_id=[4],
                    listen_node_type=["Basin"],
                    variable=["level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=3,
                    greater_than=[self.characteristics["basin2_target_level"]],
                ),
                discrete_control.Logic(truth_state=["F", "T"], control_state=["block", "pass"]),
            ],
        )

        return model

    def add_edges(self, model):
        """Add edges between each node."""
        # add the edges
        model.edge.add(model.level_boundary[1], model.outlet[5])
        model.edge.add(model.outlet[5], model.basin[3])
        model.edge.add(model.basin[3], model.outlet[6])
        model.edge.add(model.outlet[6], model.basin[4])
        model.edge.add(model.basin[4], model.pump[7])
        model.edge.add(model.pump[7], model.level_boundary[2])

        # add the edges for the discrete control
        model.edge.add(model.discrete_control[100], model.outlet[5])
        model.edge.add(model.discrete_control[101], model.outlet[6])
        model.edge.add(model.discrete_control[102], model.pump[7])

        return model

    def store_model(self, model):
        """Plot and store the model."""
        # apply the settings for the solver
        model.solver.saveat = self.characteristics["saveat"]

        # plot the model
        if self.characteristics["plot"]:
            model.plot()

        # toml_dir = os.path.join(
        #     self.characteristics["results_dir"], self.characteristics["case"], self.characteristics["example"]
        # )
        # create the directory where the model should be stored
        if not os.path.exists(os.path.join(self.characteristics["results_dir"], self.characteristics["case"])):
            os.makedirs(os.path.join(self.characteristics["results_dir"], self.characteristics["case"]))

        # store the model
        model.write(
            filepath=os.path.join(
                self.characteristics["results_dir"],
                self.characteristics["case"],
                self.characteristics["example"],
                "ribasim.toml",
            )
        )

    def run_model(self, model):
        """Run the created Ribasim model."""
        if self.characteristics["show_progress"]:
            # show progress of the Ribasim model
            subprocess.run(
                [
                    "ribasim",
                    os.path.join(
                        self.characteristics["results_dir"],
                        self.characteristics["case"],
                        self.characteristics["example"],
                        "ribasim.toml",
                    ),
                ],
                check=False,
            )
        else:
            subprocess.run(
                [
                    "ribasim",
                    os.path.join(
                        self.characteristics["results_dir"],
                        self.characteristics["case"],
                        self.characteristics["example"],
                        "ribasim.toml",
                    ),
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    def show_results(self, model):
        """Load and plot some results."""
        if self.characteristics["show_results"]:
            # load in the data
            df_basin = pd.read_feather(
                os.path.join(
                    self.characteristics["results_dir"],
                    self.characteristics["case"],
                    self.characteristics["example"],
                    "results",
                    "basin.arrow",
                )
            )

            # plot the levels
            df_basin_wide = df_basin.pivot_table(index="time", columns="node_id", values=["level"])
            df_basin_wide["level"].plot()

            # display(df_basin)

    def create_model(self, copy=False):
        """Create the model by running all the functions."""
        model = self.create_empty_model()
        model = self.add_boezems(model)
        model = self.add_peilgebieden(model)
        model = self.add_connection_nodes(model)
        model = self.add_discrete_control(model)
        model = self.add_edges(model)

        if copy:
            return model

        else:
            self.store_model(model)
            self.run_model(model)
            self.show_results(model)


class case2:
    """
    Create the second Ribasim schematisation case with an additional peilgebied.

    It builds upon case1 which had a simple case of a boezem, two cascaded peilgebieden with an outlet in between,
    after which the water is pumped through a Pump node to the boezem again.
    In this case, a third peilgebied is added with logical flow direction from and to the boezem and other peilgebieden.
    """

    def __init__(self, case_example_name, model):
        """Initialize the class. Convert the forcing from mm/day to m/s."""
        case_example_path = os.path.join("../../../../Outlet_tests/json", case_example_name + ".json")

        with open(case_example_path) as file:
            characteristics = yaml.safe_load(file)

        # define the characteristics and the base model
        self.characteristics = characteristics
        self.model = model

        # add the example and case to the characteristics
        case, example = case_example_name.split("_")

        self.characteristics["case"] = case
        self.characteristics["example"] = example

        self.characteristics["evaporation"] = (
            self.characteristics["evaporation"] / 1000 / 3600 / 24
        )  # convert from mm/day to m/s
        self.characteristics["precipitation"] = (
            self.characteristics["precipitation"] / 1000 / 3600 / 24
        )  # convert from mm/day to m/s

    def add_peilgebied(self, model):
        """Add the peilgebieden to the model, based on the pre-defined model characteristics."""
        # add peilgebied1
        basin3_data = [
            basin.Profile(
                area=self.characteristics["basin3_profile_area"], level=self.characteristics["basin3_profile_level"]
            ),
            basin.Time(
                time=pd.date_range(self.characteristics["starttime"], self.characteristics["endtime"]),
                drainage=0.0,
                potential_evaporation=self.characteristics["evaporation"],
                infiltration=0.0,
                precipitation=self.characteristics["precipitation"],
            ),
            basin.State(level=self.characteristics["basin3_initial_level"]),
        ]

        model.basin.add(Node(node_id=8, geometry=Point(3.0, -2.0), name="peilgebied3"), basin3_data)

        return model

    def add_connection_nodes(self, model):
        """Add the "connection nodes" to the model (Outlets, Pumps), based on the pre-defined model characteristics."""
        # add the connection nodes
        model.outlet.add(
            Node(node_id=9, geometry=Point(1.5, -1.0), name="Outlet3"),
            [
                outlet.Static(
                    control_state=["pass", "block"],
                    flow_rate=[self.characteristics["outlet4_flow_rate"], 0],
                    min_crest_level=self.characteristics["outlet4_min_crest_level"],
                )
            ],
        )

        (
            model.outlet.add(
                Node(node_id=10, geometry=Point(2.5, -1.0), name="Outlet4"),
                [
                    outlet.Static(
                        control_state=["pass", "block"],
                        flow_rate=[self.characteristics["outlet3_flow_rate"], 0],
                        min_crest_level=self.characteristics["outlet3_min_crest_level"],
                    )
                ],
            ),
        )

        model.outlet.add(
            Node(node_id=11, geometry=Point(3.5, -1.0), name="Outlet5"),
            [
                outlet.Static(
                    control_state=["pass", "block"],
                    flow_rate=[self.characteristics["outlet4_flow_rate"], 0],
                    min_crest_level=self.characteristics["outlet4_min_crest_level"],
                )
            ],
        )

        model.pump.add(
            Node(node_id=12, geometry=Point(4.5, -1.0), name="Pump2"),
            [pump.Static(control_state=["pass", "block"], flow_rate=[self.characteristics["pump2_flow_rate"], 0])],
        )

        return model

    def add_discrete_control(self, model):
        """Add discrete control for each "connecton node", such as Pumps and Outlets."""
        # add the discrete control between the boezem and the basin
        model.discrete_control.add(
            Node(node_id=103, geometry=Point(0, -2), name="Outlet_DC_4"),
            [
                discrete_control.Variable(
                    compound_variable_id=4,
                    listen_node_id=[1, 8],
                    listen_node_type=["LevelBoundary", "Basin"],
                    variable=["level", "level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=4,
                    greater_than=[2.95, self.characteristics["basin3_target_level"]],
                ),
                discrete_control.Logic(
                    truth_state=["FF", "FT", "TF", "TT"], control_state=["block", "block", "pass", "block"]
                ),
            ],
        )

        # add the discrete control between the boezem and the basin
        model.discrete_control.add(
            Node(node_id=104, geometry=Point(2, -1), name="Outlet_DC_5"),
            [
                discrete_control.Variable(
                    compound_variable_id=5,
                    listen_node_id=[8, 3],
                    listen_node_type=["Basin", "Basin"],
                    variable=["level", "level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=5,
                    greater_than=[
                        self.characteristics["basin3_target_level"] - 0.05,
                        self.characteristics["basin1_target_level"],
                    ],
                ),
                discrete_control.Logic(
                    truth_state=["FF", "FT", "TF", "TT"], control_state=["block", "block", "pass", "block"]
                ),
            ],
        )

        # add the discrete control between the basins
        model.discrete_control.add(
            Node(node_id=105, geometry=Point(4, -1), name="Outlet_DC_6"),
            [
                discrete_control.Variable(
                    compound_variable_id=6,
                    listen_node_id=[8, 4],
                    listen_node_type=["Basin", "Basin"],
                    variable=["level", "level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=6,
                    greater_than=[
                        self.characteristics["basin3_target_level"] - 0.05,
                        self.characteristics["basin2_target_level"],
                    ],
                ),
                discrete_control.Logic(
                    truth_state=["FF", "FT", "TF", "TT"], control_state=["block", "block", "pass", "block"]
                ),
            ],
        )

        # add the discrete control between the basin and the boezem
        model.discrete_control.add(
            Node(node_id=106, geometry=Point(6, -2), name="Outlet_DC_7"),
            [
                discrete_control.Variable(
                    compound_variable_id=7,
                    listen_node_id=[8, 2],
                    listen_node_type=["Basin", "LevelBoundary"],
                    variable=["level", "level"],
                ),
                discrete_control.Condition(
                    compound_variable_id=7,
                    greater_than=[self.characteristics["basin3_target_level"] - 0.05, 295],
                ),
                discrete_control.Logic(
                    truth_state=["FF", "FT", "TF", "TT"], control_state=["block", "block", "pass", "block"]
                ),
            ],
        )

        return model

    def store_model(self, model):
        """Plot and store the model."""
        # apply the settings for the solver
        model.solver.saveat = self.characteristics["saveat"]

        # plot the model
        if self.characteristics["plot"]:
            model.plot()

        # toml_dir = os.path.join(
        #     self.characteristics["results_dir"], self.characteristics["case"], self.characteristics["example"]
        # )
        # create the directory where the model should be stored
        if not os.path.exists(os.path.join(self.characteristics["results_dir"], self.characteristics["case"])):
            os.makedirs(os.path.join(self.characteristics["results_dir"], self.characteristics["case"]))

        # store the model
        model.write(
            filepath=os.path.join(
                self.characteristics["results_dir"],
                self.characteristics["case"],
                self.characteristics["example"],
                "ribasim.toml",
            )
        )

    def run_model(self, model):
        """Run the created Ribasim model."""
        if self.characteristics["show_progress"]:
            # show progress of the Ribasim model
            subprocess.run(
                [
                    "ribasim",
                    os.path.join(
                        self.characteristics["results_dir"],
                        self.characteristics["case"],
                        self.characteristics["example"],
                        "ribasim.toml",
                    ),
                ],
                check=False,
            )
        else:
            subprocess.run(
                [
                    "ribasim",
                    os.path.join(
                        self.characteristics["results_dir"],
                        self.characteristics["case"],
                        self.characteristics["example"],
                        "ribasim.toml",
                    ),
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    def show_results(self, model):
        """Load and plot some results."""
        if self.characteristics["show_results"]:
            # load in the data
            df_basin = pd.read_feather(
                os.path.join(
                    self.characteristics["results_dir"],
                    self.characteristics["case"],
                    self.characteristics["example"],
                    "results",
                    "basin.arrow",
                )
            )

            # plot the levels
            df_basin_wide = df_basin.pivot_table(index="time", columns="node_id", values=["level"])
            df_basin_wide["level"].plot()

            # display(df_basin)

    def add_edges(self, model):
        """Add edges between each node."""
        # add the edges
        model.edge.add(model.level_boundary[1], model.outlet[9])
        model.edge.add(model.outlet[9], model.basin[8])
        model.edge.add(model.basin[8], model.outlet[10])
        model.edge.add(model.outlet[10], model.basin[3])
        model.edge.add(model.basin[8], model.outlet[11])
        model.edge.add(model.outlet[11], model.basin[4])
        model.edge.add(model.basin[8], model.pump[12])
        model.edge.add(model.pump[12], model.level_boundary[2])

        # add the edges for the discrete control
        model.edge.add(model.discrete_control[103], model.outlet[9])
        model.edge.add(model.discrete_control[104], model.outlet[10])
        model.edge.add(model.discrete_control[105], model.outlet[11])
        model.edge.add(model.discrete_control[106], model.pump[12])

        return model

    def create_model(self, copy=False):
        """Create the model by running all the functions."""
        model = self.add_peilgebied(self.model)
        model = self.add_connection_nodes(model)
        model = self.add_discrete_control(model)
        model = self.add_edges(model)

        if copy:
            return model

        else:
            self.store_model(model)
            self.run_model(model)
            self.show_results(model)

import logging
from pathlib import Path

# import numpy as np
import pandas as pd
from ribasim import Model
from shapely.geometry import Point


class NoGeometryFilter(logging.Filter):
    def filter(self, record):
        return not record.getMessage().startswith("[")


class AddStorageBasin:
    def __init__(self, ribasim_toml, model_name, output_folder, include_hoofdwater=False, log=True, node_ids=None):
        """
        Initialize the AddStorageBasin class.

        :param ribasim_toml: Path to the ribasim TOML file
        :param model_name: Name of the model
        :param output_folder: Folder to output the results
        :param include_hoofdwater: Boolean flag to include hoofdwater in processing
        :param log: Boolean flag to enable logging
        :param node_ids: List of node IDs to process, if specified
        """
        # Parse input
        self.ribasim_toml = ribasim_toml
        self.model_name = model_name
        self.output_folder = output_folder
        self.include_hoofdwater = include_hoofdwater
        self.log = log
        self.node_ids = node_ids
        # Load model
        self.model = self.load_ribasim_model(ribasim_toml)
        # Set logging
        if self.log is True:
            self.log_filename = Path(output_folder) / f"{model_name}.log"
            self.setup_logging()

    def setup_logging(self):
        """Set up logging to file and console."""
        # Clear any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # Setup logging to file
        logging.basicConfig(
            filename=self.log_filename,
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # # Add console handler
        # console_handler = logging.StreamHandler()
        # console_handler.setLevel(logging.DEBUG)
        # console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        # # Add custom filter to console handler
        # console_handler.addFilter(NoGeometryFilter())

        # logging.getLogger().addHandler(console_handler)

    def load_ribasim_model(self, ribasim_toml):
        """
        Load the ribasim model from the TOML file.

        :param ribasim_toml: Path to the ribasim TOML file
        :return: Loaded ribasim model
        """
        model = Model(filepath=ribasim_toml)
        return model

    def get_current_max_nodeid(self):
        """
        Get the current maximum node ID from the model.

        :return: Maximum node ID
        """
        max_ids = []
        for k, v in self.model.__dict__.items():
            if hasattr(v, "node") and "node_id" in v.node.df.columns.tolist():
                mid = v.node.df.node_id.max()
                if not pd.isna(mid):
                    max_ids.append(int(mid))
        if len(max_ids) == 0:
            raise ValueError("No node ids found")
        max_id = max(max_ids)
        return max_id

    def add_basin_nodes_with_manning_resistance(self):
        """Add basin nodes with Manning resistance based on meta_categorie."""
        # Get the meta_categorie column from the state DataFrame
        state_df = self.model.basin.state.df

        for index, row in self.model.basin.node.df.iterrows():
            node_id = row["node_id"]

            # If node_ids is specified, only process those nodes
            if self.node_ids is not None and node_id not in self.node_ids:
                continue

            # Retrieve the corresponding meta_categorie for the current node
            meta_categorie = state_df.loc[state_df["node_id"] == node_id, "meta_categorie"].to_numpy()

            # If meta_categorie is empty, continue to the next row
            if len(meta_categorie) == 0:
                continue

            meta_categorie = meta_categorie[0]  # Get the actual value

            if self.include_hoofdwater:
                if "bergend" in meta_categorie or (
                    "hoofdwater" not in meta_categorie and "doorgaand" not in meta_categorie
                ):
                    continue
            else:
                if "bergend" in meta_categorie or "hoofdwater" in meta_categorie or "doorgaand" not in meta_categorie:
                    continue

            original_node_id = row["node_id"]
            original_geometry = row["geometry"]
            logging.info(f"Processing Basin Node ID: {original_node_id}")

            # Calculate new geometries
            manning_geometry = Point(original_geometry.x + 5, original_geometry.y)
            new_basin_geometry = Point(original_geometry.x + 10, original_geometry.y)

            # Add manning resistance node
            manning_node_id = self.add_manning_resistance_node(manning_geometry)
            if manning_node_id is not None:
                # Add new basin node and connect to manning resistance node
                new_basin_node_id = self.add_new_basin_node(new_basin_geometry)
                if new_basin_node_id is not None:
                    self.connect_nodes(new_basin_node_id, manning_node_id, original_node_id)
                else:
                    logging.error(f"Failed to add new basin node for Manning Resistance Node ID: {manning_node_id}")
            else:
                logging.error(f"Failed to add Manning Resistance node for Basin Node ID: {original_node_id}")

    def add_new_basin_node(self, geometry):
        """
        Add a new basin node at the specified geometry.

        :param geometry: Geometry of the new basin node
        :return: ID of the new basin node, or None if adding failed
        """
        try:
            max_id = self.get_current_max_nodeid()
            new_node_id = max_id + 1
            key = "basin"
            value = getattr(self.model, key, None)

            if value is not None:
                original_geometry = None
                if hasattr(value, "__dict__"):
                    # Retrieve the original geometry (MultiPolygon) from the first row of the basin area DataFrame
                    if "area" in value.__dict__ and hasattr(value.area, "df") and not value.area.df.empty:
                        original_geometry = value.area.df.iloc[0]["geometry"]

                    for sub_key, sub_value in value.__dict__.items():
                        if sub_key == "time" or sub_key == "subgrid":
                            continue
                        else:
                            sub_value = getattr(value, sub_key, None)
                            if sub_value is None or not hasattr(sub_value, "df") or sub_value.df is None:
                                logging.warning(f"Sub value for key '{sub_key}' is None or has no DataFrame")
                                continue
                            df_value = sub_value.df.copy()
                            last_row = df_value.iloc[-1].copy()
                            last_row["node_id"] = new_node_id
                            if "geometry" in last_row:
                                # Determine the geometry type based on the table type
                                if sub_key == "node":
                                    last_row["geometry"] = geometry
                                elif sub_key == "area":
                                    last_row["geometry"] = (
                                        original_geometry if original_geometry is not None else geometry
                                    )
                            for col in last_row.index:
                                if col.startswith("meta_cat"):
                                    last_row[col] = "bergend"
                            new_row_df = pd.DataFrame([last_row])
                            df_value = pd.concat([df_value, new_row_df], ignore_index=True)
                            sub_value.df = df_value

                    logging.info(f"Successfully added new basin node with Node ID: {new_node_id}")
                    return new_node_id
                else:
                    logging.error(f"Could not find value for key '{key}'")
                    return None

        except Exception as e:
            logging.error(f"An error occurred while adding new basin node: {e}")
            return None

    def add_manning_resistance_node(self, geometry):
        """
        Add a Manning resistance node at the specified geometry.

        :param geometry: Geometry of the Manning resistance node
        :return: ID of the Manning resistance node, or None if adding failed
        """
        try:
            max_id = self.get_current_max_nodeid()
            manning_node_id = max_id + 1
            key = "manning_resistance"
            value = getattr(self.model, key, None)

            if value is not None:
                if hasattr(value, "__dict__"):
                    for sub_key, sub_value in value.__dict__.items():
                        if sub_key == "time" or sub_key == "subgrid":
                            continue
                        else:
                            sub_value = getattr(value, sub_key, None)
                            if sub_value is None or not hasattr(sub_value, "df") or sub_value.df is None:
                                logging.warning(f"Sub value for key '{sub_key}' is None or has no DataFrame")
                                continue
                            df_value = sub_value.df.copy()
                            last_row = df_value.iloc[-1].copy()
                            last_row["node_id"] = manning_node_id
                            if "geometry" in last_row:
                                last_row["geometry"] = geometry
                            for col in last_row.index:
                                if col.startswith("meta_categ"):
                                    last_row[col] = "bergend"
                            new_row_df = pd.DataFrame([last_row])
                            df_value = pd.concat([df_value, new_row_df], ignore_index=True)
                            sub_value.df = df_value

                logging.info(f"Successfully added Manning Resistance node with Node ID: {manning_node_id}")
                return manning_node_id
            else:
                logging.error(f"Could not find value for key '{key}'")
                return None

        except Exception as e:
            logging.error(f"Error adding Manning Resistance node: {e}")
            return None

    def connect_nodes(self, new_basin_node_id, manning_node_id, original_node_id):
        """
        Connect the new basin node to the original basin node via the Manning resistance node.

        :param new_basin_node_id: ID of the new basin node
        :param manning_node_id: ID of the Manning resistance node
        :param original_node_id: ID of the original basin node
        """
        try:
            self.model.edge.add(self.model.basin[new_basin_node_id], self.model.manning_resistance[manning_node_id])
            self.model.edge.add(self.model.manning_resistance[manning_node_id], self.model.basin[original_node_id])
            logging.info(
                f"Connected new Basin Node ID: {new_basin_node_id} to original Basin Node ID: {original_node_id} via Manning Resistance Node ID: {manning_node_id}"
            )
        except Exception as e:
            logging.error(f"Error connecting nodes: {e}")

    def run(self):
        """Run the process of adding basin nodes with Manning resistance and writing the updated model"""
        self.add_basin_nodes_with_manning_resistance()
        # self.write_ribasim_model()
        logging.shutdown()

        return self.model

    def write_ribasim_model(self):
        """Write the updated ribasim model to the output directory"""
        outputdir = Path(self.output_folder)
        modelcase_dir = Path(f"updated_{self.model_name.lower()}")

        full_path = outputdir / modelcase_dir
        full_path.mkdir(parents=True, exist_ok=True)

        self.model.write(full_path / "ribasim.toml")


# Example usage
# ribasim_toml =  r"C:\Users\Aerts\Desktop\RIBASIM Project\Verwerken_Feedback\modellen\AmstelGooienVecht_boezemmodel_2024_6_8\ribasim.toml"
# output_folder = r"C:\Users\Aerts\Desktop\RIBASIM Project\Verwerken_Feedback\verwerkte_modellen"
# model_name = 'test_hoofdwater'
# node_ids = [1, 2, 3]  # Specify node IDs to process

# processor = AddStorageBasin(ribasim_toml, model_name, output_folder, include_hoofdwater=True, log=True, node_ids=node_ids)
# processor.run()

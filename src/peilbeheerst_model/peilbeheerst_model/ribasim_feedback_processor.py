import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import ribasim
from pyproj import Proj, Transformer
from shapely.geometry import LineString, Point

# Mapping between feedback form and model names
mapping = {
    "Basin": "basin",
    "TabulatedRatingCurve": "tabulated_rating_curve",
    "Pump": "pump",
    "Outlet": "outlet",
    "UserDemand": "user_demand",
    "LevelDemand": "level_demand",
    "FlowDemand": "flow_demand",
    "LevelBoundary": "level_boundary",
    "FlowBoundary": "flow_boundary",
    "LinearResistance": "linear_resistance",
    "ManningResistance": "manning_resistance",
    "Terminal": "terminal",
    "DiscreteControl": "discrete_control",
    "PidControl": "pid_control",
}


class RibasimFeedbackProcessor:
    _basin_aanvoer_on: tuple = None
    _basin_aanvoer_off: tuple = None
    _outlet_aanvoer_on: tuple = None
    _outlet_aanvoer_off: tuple = None

    def __init__(
        self,
        name,
        waterschap,
        versie,
        feedback_excel,
        ribasim_toml,
        output_folder,
        feedback_excel_processed=None,
        use_validation=True,
    ):
        self.name = name
        self.waterschap = waterschap
        self.versie = versie
        self.feedback_excel = feedback_excel
        self.ribasim_toml = ribasim_toml
        self.output_folder = output_folder
        self.feedback_excel_processed = feedback_excel_processed or feedback_excel.replace(".xlsx", "_processed.xlsx")
        self.use_validation = use_validation

        self.df = self.load_feedback(feedback_excel)
        self.df_node_types = self.load_node_type(feedback_excel)
        self.model = self.load_ribasim_model(ribasim_toml)
        self.log_filename = Path(output_folder) / f"{waterschap}.log"

    def setup_logging(self):
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename=self.log_filename,
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    @staticmethod
    def load_feedback(feedback_excel):
        df = pd.read_excel(feedback_excel, sheet_name="Feedback_Formulier", skiprows=7)
        df = df[df["Actie"].notna()]
        return df

    @staticmethod
    def load_node_type(feedback_excel):
        df = pd.read_excel(feedback_excel, sheet_name="Node_Data")
        df = df[df["node_id"].notna()]
        df = df.set_index("node_id")
        return df

    @staticmethod
    def load_ribasim_model(ribasim_toml):
        model = ribasim.Model(filepath=ribasim_toml)
        return model

    def get_current_max_nodeid(self):
        max_ids = []
        for k, v in self.model.__dict__.items():
            if hasattr(v, "node") and not v.node.df.index.empty:
                mid = v.node.df.index.max()
                if not np.isnan(mid):
                    max_ids.append(int(mid))

        if len(max_ids) == 0:
            raise ValueError("No node ids found")

        max_id = max(max_ids)
        return max_id

    def write_ribasim_model(self):
        outputdir = Path(self.output_folder)
        self.model.write(outputdir / "ribasim.toml")

    def update_dataframe_with_new_node_ids(self, node_id_map):
        for old_id, new_id in node_id_map.items():
            self.df.replace(old_id, new_id, inplace=True)
        return self.df

    def process_model(self):
        self.setup_logging()
        node_id_map = {}

        try:
            for index, row in self.df.iterrows():
                logging.info(f"Processing row: {index + 7}")
                try:
                    if row["Actie"] == "Verwijderen":
                        self.remove_node(row)
                    elif row["Actie"] == "Toevoegen":
                        new_node_id = self.add_node(row)
                        if new_node_id is not None:
                            node_id_map[int(row["Node ID"])] = new_node_id
                            self.df.at[index, "Verwerkt"] = new_node_id
                    elif row["Actie"] == "Aanpassen":
                        if row["Verbinding"] == "Node":
                            new_node_id = self.adjust_node(row)
                            if new_node_id is not None:
                                node_id_map[int(row["Node ID.2"])] = new_node_id
                                self.df.at[index, "Verwerkt"] = new_node_id
                        elif row["Verbinding"] in ("Edge", "Link") and row["Aanpassing"] == "Stroomrichting Omdraaien":
                            self.adjust_links(row, node_id_map)
                except Exception as e:
                    logging.error(f"Error processing {row['Actie']}, {row['Verbinding']}, at index {index}: {e}")
        finally:
            for handler in logging.root.handlers[:]:
                handler.close()
                logging.root.removeHandler(handler)

        print("Processed all actions")

    def remove_node(self, row):
        try:
            key = row["Node Type"]
            key = mapping[key]
            node_id = int(row["Node ID"])
            logging.info(f"Node ID: {node_id}")
            value = getattr(self.model, key, None)

            # Remove the Node
            if value is not None:
                if hasattr(value, "__dict__"):
                    for sub_key, sub_value in value.__dict__.items():
                        if hasattr(sub_value, "df") and sub_value.df is not None:
                            if not sub_value.df.empty:
                                if sub_key == "node":
                                    filtered_df = sub_value.df[sub_value.df.index != node_id]
                                    sub_value.df = filtered_df
                                if sub_key == "static":
                                    filtered_df = sub_value.df[sub_value.df["node_id"] != node_id]
                                    sub_value.df = filtered_df

            # Remove the Links
            rows_to_remove = self.model.link.df[
                (self.model.link.df["from_node_id"] == node_id) | (self.model.link.df["to_node_id"] == node_id)
            ].index
            self.model.link.df = self.model.link.df.drop(rows_to_remove)

            # Log status
            logging.info(f"Successfully removed node with Node ID: {node_id}, Action: Verwijderen")

            return rows_to_remove

        except Exception as e:
            logging.error(f"Error removing node {row['Node ID']}: {e}")

    def add_node(self, row):
        try:
            key = row["Node Type.1"]
            key = mapping.get(key, None)
            max_id = self.get_current_max_nodeid()
            node_id = max_id + 1
            logging.info(f"Node ID: {node_id}")
            value = getattr(self.model, key, None)

            # Add the Node
            if value is not None:
                if hasattr(value, "__dict__"):
                    for sub_key, sub_value in value.__dict__.items():
                        if sub_key == "time" or sub_key == "subgrid":
                            continue
                        else:
                            if sub_value is None or not hasattr(sub_value, "df") or sub_value.df is None:
                                logging.warning(f"Sub value for key '{sub_key}' is None or has no DataFrame")
                                continue

                            if sub_key == "node":
                                sub_value = getattr(value, sub_key, None)
                                df_value = sub_value.df.copy()
                                last_row = df_value.iloc[-1].copy()

                                last_row.name = node_id
                                if "geometry" in last_row:
                                    x_coord = row["Coordinaat X"]
                                    y_coord = row["Coordinaat Y"]
                                    last_row["geometry"] = Point(x_coord, y_coord)

                                for col in last_row.index:
                                    if col.startswith("meta_"):
                                        last_row[col] = np.nan

                                new_row_df = pd.DataFrame([last_row])
                                new_row_df["meta_node_id"] = node_id

                                df_value = pd.concat([df_value, new_row_df], ignore_index=False)
                                df_value.index.name = "node_id"
                                sub_value.df = df_value.copy()

                            if sub_key == "static":
                                sub_value = getattr(value, sub_key, None)
                                df_value_static = sub_value.df.copy()
                                last_row = df_value_static.iloc[-1].copy()

                                last_row["node_id"] = node_id
                                if "geometry" in last_row:
                                    x_coord = row["Coordinaat X"]
                                    y_coord = row["Coordinaat Y"]
                                    last_row["geometry"] = Point(x_coord, y_coord)

                                for col in last_row.index:
                                    if col.startswith("meta_"):
                                        last_row[col] = np.nan

                                new_row_df = pd.DataFrame([last_row])
                                new_row_df["meta_node_id"] = node_id
                                new_row_df.index.name = "fid"

                                # drop unused columns to avoid a warning
                                df_value_static = df_value_static.dropna(axis=1, how="all")
                                new_row_df = new_row_df.dropna(axis=1, how="all")

                                df_value_static = pd.concat([df_value_static, new_row_df], ignore_index=True)
                                df_value_static.index.name = "fid"
                                sub_value.df = df_value_static.copy()

                # Add the Links
                if key in ["level_boundary", "flow_boundary", "terminal"]:
                    new_node = getattr(self.model, key, None)[node_id]

                    if pd.notna(row["Node ID A"]):
                        node_type_a = self.df_node_types.loc[int(row["Node ID A"])].node_type
                        node_type_a = mapping[node_type_a]
                        node_a = getattr(self.model, node_type_a, None)[int(row["Node ID A"])]
                        self.model.link.add(new_node, node_a)
                    else:
                        logging.warning(f"'Node ID A' is NaN for node type {key} at index {row.name}")
                else:
                    if pd.isna(row["Node ID A"]) or pd.isna(row["Node ID B"]):
                        logging.error(f"'Node ID A' or 'Node ID B' is NaN for node type {key} at index {row.name}")
                        return None
                    new_node = getattr(self.model, key, None)[node_id]
                    node_type_a = self.df_node_types.loc[int(row["Node ID A"])].node_type
                    node_type_b = self.df_node_types.loc[int(row["Node ID B"])].node_type
                    node_type_a = mapping[node_type_a]
                    node_type_b = mapping[node_type_b]
                    node_a = getattr(self.model, node_type_a, None)[int(row["Node ID A"])]
                    node_b = getattr(self.model, node_type_b, None)[int(row["Node ID B"])]
                    self.model.link.add(node_a, new_node)
                    self.model.link.add(new_node, node_b)

                new_node_type_row = pd.DataFrame(
                    [
                        {
                            "fid": np.nan,
                            "name": np.nan,
                            "node_type": key,
                            "subnetwork_id": np.nan,
                        }
                    ],
                    index=[node_id],
                )

                self.df_node_types = pd.concat([self.df_node_types, new_node_type_row])

                logging.info(f"Successfully added node with Node ID: {node_id}, Action: Toevoegen")

        except Exception as e:
            logging.error(f"Error adding node at row {row.name}: {e}")

    def adjust_node(self, row):
        try:
            # Get the old node type and id
            key = self.df_node_types.loc[int(row["Node ID.2"])].node_type
            key = mapping[key]
            node_id = int(row["Node ID.2"])
            logging.info(f"Node ID: {node_id}")
            # node_id_old = node_id
            value = getattr(self.model, key, None)

            # Get old geometry and remove Node
            if value is not None:
                if hasattr(value, "__dict__"):
                    for sub_key, sub_value in value.__dict__.items():
                        if sub_key == "time" or sub_key == "subgrid":
                            continue
                        else:
                            if sub_value is None or not hasattr(sub_value, "df") or sub_value.df is None:
                                logging.warning(f"Sub value for key '{sub_key}' is None or has no DataFrame")
                                continue

                        if "geometry" in sub_value.df:
                            if sub_key == "node":
                                geometry_old = sub_value.df[sub_value.df.index == node_id].geometry.iloc[0]
                                filtered_df = sub_value.df[sub_value.df.index != node_id]
                                sub_value.df = filtered_df

                            if sub_key == "static":
                                geometry_old = sub_value.df[sub_value.df["node_id"] == node_id].geometry.iloc[0]
                                filtered_df = sub_value.df[sub_value.df["node_id"] != node_id]
                                sub_value.df = filtered_df

            key = row["Nieuw Node Type"]
            key = mapping.get(key, None)
            value = getattr(self.model, key, None)

            if value is not None:
                if hasattr(value, "__dict__"):
                    for sub_key, sub_value in value.__dict__.items():
                        if sub_key == "time" or sub_key == "subgrid":
                            continue
                        else:
                            if sub_value is None or not hasattr(sub_value, "df") or sub_value.df is None:
                                logging.warning(f"Sub value for key '{sub_key}' is None or has no DataFrame")
                                continue

                            if sub_key == "node":
                                sub_value = getattr(value, sub_key, None)
                                df_value = sub_value.df.copy()
                                last_row = df_value.iloc[-1].copy()

                                last_row.name = node_id
                                if "geometry" in last_row:
                                    last_row["geometry"] = geometry_old if "geometry_old" in locals() else None

                                for col in last_row.index:
                                    if col.startswith("meta_"):
                                        last_row[col] = np.nan

                                new_row_df = pd.DataFrame([last_row])
                                new_row_df["meta_node_id"] = node_id

                                df_value = pd.concat([df_value, new_row_df], ignore_index=False)
                                df_value.index.name = "node_id"
                                sub_value.df = df_value.copy()

                            if sub_key == "static":
                                sub_value = getattr(value, sub_key, None)
                                df_value_static = sub_value.df.copy()
                                last_row = df_value_static.iloc[-1].copy()

                                last_row["node_id"] = node_id
                                if "geometry" in last_row:
                                    last_row["geometry"] = geometry_old if "geometry_old" in locals() else None

                                for col in last_row.index:
                                    if col.startswith("meta_"):
                                        last_row[col] = np.nan

                                new_row_df = pd.DataFrame([last_row])
                                new_row_df["meta_node_id"] = node_id
                                new_row_df.index.name = "fid"

                                # drop unused columns to avoid a warning
                                df_value_static = df_value_static.dropna(axis=1, how="all")
                                new_row_df = new_row_df.dropna(axis=1, how="all")

                                df_value_static = pd.concat([df_value_static, new_row_df], ignore_index=True)
                                df_value_static.index.name = "fid"
                                sub_value.df = df_value_static.copy()

            # Adjust links meta_node_type
            rows_to_update = self.model.link.df[
                (self.model.link.df["from_node_id"] == node_id) | (self.model.link.df["to_node_id"] == node_id)
            ]

            for idx, link_row in rows_to_update.iterrows():
                if link_row["to_node_id"] == node_id:
                    # Update the meta_node_type for the to_node
                    self.model.link.df.at[idx, "meta_to_node_type"] = key

                if link_row["from_node_id"] == node_id:
                    # Update the meta_node_type for the from_node
                    self.model.link.df.at[idx, "meta_from_node_type"] = key

            logging.info(f"Successfully updated meta_node_type for links related to Node ID: {node_id}")

            logging.info(f"Successfully adjusted node with old Node ID: {node_id}, Action: Aanpassen")
            return node_id

        except Exception:
            logging.error(f"Error adjusting node at row: {row}", exc_info=True)
            return None

    def adjust_links(self, row, node_id_map):
        try:
            node_a = int(node_id_map.get(row["Node ID A.1"], row["Node ID A.1"]))
            node_b = int(node_id_map.get(row["Node ID B.1"], row["Node ID B.1"]))

            df_row_a_b = self.model.link.df[
                (self.model.link.df["from_node_id"] == node_a) & (self.model.link.df["to_node_id"] == node_b)
            ]
            df_row_b_a = self.model.link.df[
                (self.model.link.df["from_node_id"] == node_b) & (self.model.link.df["to_node_id"] == node_a)
            ]

            if df_row_a_b.empty and df_row_b_a.empty:
                logging.error(f"Link not found between Node A: {node_a} and Node B: {node_b} at index {row}")
                return
            if not df_row_a_b.empty:
                df_row = df_row_a_b
            else:
                df_row = df_row_b_a
            self.model.link.df.loc[df_row.index, ["from_node_id", "to_node_id"]] = self.model.link.df.loc[
                df_row.index, ["to_node_id", "from_node_id"]
            ].to_numpy()
            if "geometry" in df_row.columns:
                row_index = df_row.index[0]
                line = self.model.link.df.loc[row_index].geometry
                reversed_coords = list(line.coords)[::-1]
                reversed_line = LineString(reversed_coords)
                self.model.link.df.at[row_index, "geometry"] = reversed_line
            print(f"Swapped link direction between Node A: {node_a} and Node B: {node_b}")
            logging.info(
                f"Successfully swapped link direction between Node A: {node_a} and Node B: {node_b}, "
                f"Action: Aanpassen, Adjustment: Stroomrichting Omdraaien"
            )
        except Exception as e:
            logging.error(f"Error adjusting link: {e}")

    def special_preprocessing_for_hollandse_delta(self):
        p1 = Proj("epsg:4326")  # WGS84
        p2 = Proj("epsg:28992")  # Rijksdriehoekstelsel
        transformer = Transformer.from_proj(p1, p2)

        def clean_coordinate(coord_str):
            if pd.isna(coord_str):
                return None
            coord_str = str(coord_str).replace("°E", "").replace("°N", "").replace(",", ".")
            return float(coord_str)

        for index, row in self.df.iterrows():
            if pd.notna(row["Coordinaat X"]) and pd.notna(row["Coordinaat Y"]):
                lon_str = row["Coordinaat X"]
                lat_str = row["Coordinaat Y"]
                lon = clean_coordinate(lon_str)
                lat = clean_coordinate(lat_str)
                x, y = transformer.transform(lon, lat)
                self.df.at[index, "Coordinaat X"] = x
                self.df.at[index, "Coordinaat Y"] = y

    def save_feedback(self):
        self.df["Naam.1"] = self.name
        self.df["Datum.1"] = datetime.now().strftime("%d-%m-%Y")
        self.df["Versie"] = self.versie
        self.df.to_excel(self.feedback_excel_processed, index=False)

    def update_target_levels(self):
        # read sheet with the updated the target levels
        df_TL = pd.read_excel(self.feedback_excel, sheet_name="Streefpeilen", header=0)
        df_TL = df_TL.sort_values(by=["Basin node_id"]).reset_index(drop=True)
        if len(df_TL) > 0:  # if the sheet is filled in, proceed
            # print warning if there are non existing basins
            existing_basins = self.model.basin.node.df.index.to_numpy()
            non_existing_basins = df_TL.loc[~df_TL["Basin node_id"].isin(existing_basins)]
            if len(non_existing_basins) > 0:
                print("Warning! Following basins do not exist:\n", non_existing_basins, "\n")

            # update streefpeilen in the .state table
            self.model.basin.state.df.loc[
                self.model.basin.state.df.node_id.isin(df_TL["Basin node_id"].to_numpy()), "level"
            ] = df_TL.Streefpeil.astype(float).to_numpy()

            # update streefpeilen in the .area table
            self.model.basin.area.df.loc[
                self.model.basin.area.df.node_id.isin(df_TL["Basin node_id"].to_numpy()), "meta_streefpeil"
            ] = df_TL.Streefpeil.astype(float).to_numpy()
            print("The target levels (streefpeilen) have been updated.")

    def functie_gemalen(self):
        # read sheet with the updated the pump functions
        try:
            df_FG = pd.read_excel(self.feedback_excel, sheet_name="Functie gemalen", header=0, usecols="A:B")
        except ValueError:
            df_FG = pd.read_excel(self.feedback_excel, sheet_name="Aan_afvoer_gemalen", header=0, usecols="A:B")

        if len(df_FG) > 0:  # if the sheet is filled in, proceed
            # print warning if there are non existing pumps
            existing_pumps = self.model.pump.node.df.index.to_numpy()
            non_existing_pumps = df_FG.loc[~df_FG["Pump node_id"].isin(existing_pumps)]
            if len(non_existing_pumps) > 0:
                print("Warning! Following pumps do not exist:\n", non_existing_pumps, "\n")

            # determine the function provided in the feedback form
            aanvoer_pumps = df_FG.loc[df_FG["Aanvoer / afvoer?"].str.lower() == "aanvoer"]
            afvoer_pumps = df_FG.loc[df_FG["Aanvoer / afvoer?"].str.lower() == "afvoer"]
            allround_pumps = df_FG.loc[df_FG["Aanvoer / afvoer?"].str.lower() == "aanvoer & afvoer"]

            # extract pump IDs and make them unique
            aanvoer_pump_ids = np.unique(aanvoer_pumps["Pump node_id"].to_numpy(dtype=int))
            afvoer_pump_ids = np.unique(afvoer_pumps["Pump node_id"].to_numpy(dtype=int))
            allround_pump_ids = np.unique(allround_pumps["Pump node_id"].to_numpy(dtype=int))

            # clean up pump-IDs
            double_ids = []
            for i in aanvoer_pump_ids:
                if i in afvoer_pump_ids:
                    allround_pump_ids = np.append(allround_pump_ids, i)
                    double_ids.append(i)
            for i in double_ids:
                aanvoer_pump_ids = np.delete(aanvoer_pump_ids, aanvoer_pump_ids == i)
                afvoer_pump_ids = np.delete(afvoer_pump_ids, afvoer_pump_ids == i)

            # change the meta_func_* columns
            pumps = aanvoer_pumps, afvoer_pumps, allround_pumps
            funcs = [0, 1], [1, 0], [1, 1]
            for p, f in zip(pumps, funcs):
                self.model.pump.static.df.loc[
                    self.model.pump.static.df["node_id"].isin(p["Pump node_id"]),
                    ["meta_func_afvoer", "meta_func_aanvoer"],
                ] = f

            # logging statement
            print("The function of the pumps have been updated.")

    def run(self):
        self.process_model()
        self.save_feedback()
        if not self.use_validation:
            self.model.use_validation = self.use_validation

        self.update_target_levels()
        self.functie_gemalen()
        self.write_ribasim_model()

    def get_basin_aanvoer_corrections(self) -> None:
        """Extract corrections on basin 'aanvoer'-flagging from the feedback forms."""
        sheet_name = "Aan_afvoer_basins"
        try:
            df = pd.read_excel(self.feedback_excel, sheet_name=sheet_name, usecols="A:B")
        except ValueError:
            logging.info(f'No "{sheet_name}"-worksheet in "{self.feedback_excel}": Skipped corrections.')
            self._basin_aanvoer_on = ()
            self._basin_aanvoer_off = ()
        else:
            df.dropna(axis=0, inplace=True)
            if len(df) == 0:
                aanvoer_ids = afvoer_ids = []
            else:
                aanvoer_ids = df.loc[df["Aanvoer / afvoer?"].str.lower() == "aanvoer", "Basin ID"].to_numpy(dtype=int)
                afvoer_ids = df.loc[df["Aanvoer / afvoer?"].str.lower() == "afvoer", "Basin ID"].to_numpy(dtype=int)

            self._basin_aanvoer_on = tuple(aanvoer_ids)
            self._basin_aanvoer_off = tuple(afvoer_ids)
        finally:
            logging.warning(
                f'Catch for missing sheet-name "{sheet_name}" in "{self.feedback_excel}" will be deprecated: '
                f'Make sure that feedback forms will have a sheet-name titled "{sheet_name}".'
            )

    def get_outlet_aanvoer_corrections(self) -> None:
        """Extract corrections on outlet 'aanvoer'-flagging from the feedback forms."""
        # TODO: Remove this 'missing worksheet'-catch in the future
        sheet_name = "Aan_afvoer_outlets"
        try:
            df = pd.read_excel(self.feedback_excel, sheet_name=sheet_name, usecols="A:B")
        except ValueError:
            logging.info(f'No "{sheet_name}"-worksheet in "{self.feedback_excel}": Skipped corrections.')
            self._outlet_aanvoer_on = ()
            self._outlet_aanvoer_off = ()
        else:
            df.dropna(axis=0, inplace=True)
            if len(df) == 0:
                aanvoer_ids = afvoer_ids = []
            else:
                aanvoer_ids = df.loc[df["Aanvoer / afvoer?"].str.lower() == "aanvoer", "Outlet node_id"].to_numpy(
                    dtype=int
                )
                afvoer_ids = df.loc[df["Aanvoer / afvoer?"].str.lower() == "afvoer", "Outlet node_id"].to_numpy(
                    dtype=int
                )

            self._outlet_aanvoer_on = tuple(aanvoer_ids)
            self._outlet_aanvoer_off = tuple(afvoer_ids)
        finally:
            logging.warning(
                f'Catch for missing sheet-name "{sheet_name}" in "{self.feedback_excel}" will be deprecated: '
                f'Make sure that feedback forms will have a sheet-name titled "{sheet_name}".'
            )

    @property
    def basin_aanvoer_on(self) -> tuple:
        """Basin 'aanvoer'-flagging: True

        :return: basin-IDs
        :rtype: tuple
        """
        if self._basin_aanvoer_on is None:
            self.get_basin_aanvoer_corrections()

        return self._basin_aanvoer_on

    @property
    def basin_aanvoer_off(self) -> tuple:
        """Basin 'aanvoer'-flagging: False

        :return: basin-IDs
        :rtype: tuple
        """
        if self._basin_aanvoer_off is None:
            self.get_basin_aanvoer_corrections()

        return self._basin_aanvoer_off

    @property
    def outlet_aanvoer_on(self) -> tuple:
        """Oulet 'aanvoer'-flagging: True

        :return: outlet-IDs
        :rtype: tuple
        """
        if self._outlet_aanvoer_on is None:
            self.get_outlet_aanvoer_corrections()

        return self._outlet_aanvoer_on

    @property
    def outlet_aanvoer_off(self) -> tuple:
        """Outlet 'aanvoer'-flagging: False

        :return: outlet-IDs
        :rtype: tuple
        """
        if self._outlet_aanvoer_off is None:
            self.get_outlet_aanvoer_corrections()

        return self._outlet_aanvoer_off

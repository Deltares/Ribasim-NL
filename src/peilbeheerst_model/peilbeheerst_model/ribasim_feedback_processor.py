import logging
import pandas as pd
import numpy as np
import ribasim
from ribasim import Node
from ribasim.nodes import (
    discrete_control,
    pid_control,
)
from shapely.geometry import Point, LineString
from pathlib import Path
from datetime import datetime
from pyproj import Proj, Transformer

# Mapping between feedback form and model names
mapping = {
    'Basin': 'basin',
    'FractionalFlow': 'fractional_flow',
    'TabulatedRatingCurve': 'tabulated_rating_curve',
    'Pump': 'pump',
    'Outlet': 'outlet',
    'UserDemand': 'user_demand',
    'LevelDemand': 'level_demand',
    'FlowDemand': 'flow_demand',
    'LevelBoundary': 'level_boundary',
    'FlowBoundary': 'flow_boundary',
    'LinearResistance': 'linear_resistance',
    'ManningResistance': 'manning_resistance',
    'Terminal': 'terminal',
    'DiscreteControl': 'discrete_control',
    'PidControl': 'pid_control'
}

class RibasimFeedbackProcessor:
    def __init__(self, name, waterschap, versie, feedback_excel, ribasim_toml, output_folder, feedback_excel_processed=None):
        self.name = name
        self.waterschap = waterschap
        self.versie = versie
        self.feedback_excel = feedback_excel
        self.ribasim_toml = ribasim_toml
        self.output_folder = output_folder
        self.feedback_excel_processed = feedback_excel_processed or feedback_excel.replace('.xlsx', '_processed.xlsx')
        
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
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def load_feedback(self, feedback_excel):
        df = pd.read_excel(feedback_excel, sheet_name="Feedback_Formulier", skiprows=7)
        df = df[df['Actie'].notna()]
        return df

    def load_node_type(self, feedback_excel):
        df = pd.read_excel(feedback_excel, sheet_name="Node_Data")
        df = df[df['node_id'].notna()]
        df = df.set_index('node_id')
        return df

    def load_ribasim_model(self, ribasim_toml):
        model = ribasim.Model(filepath=ribasim_toml)
        return model

    def get_current_max_nodeid(self):
        max_ids = []
        for k, v in self.model.__dict__.items():
            if hasattr(v, 'node') and "node_id" in v.node.df.columns.tolist():
                mid = v.node.df.node_id.max()
                if not np.isnan(mid):
                    max_ids.append(int(mid))

        if len(max_ids) == 0:
            raise ValueError("No node ids found")

        max_id = max(max_ids)
        return max_id

    def write_ribasim_model(self):
        outputdir = Path(self.output_folder)
        modelcase_dir = Path(f'updated_{self.waterschap.lower()}')
        
        full_path = outputdir / modelcase_dir
        full_path.mkdir(parents=True, exist_ok=True)

        self.model.write(full_path / "ribasim.toml")

    def update_dataframe_with_new_node_ids(self, node_id_map):
        for old_id, new_id in node_id_map.items():
            self.df.replace(old_id, new_id, inplace=True)
        return self.df

    def process_model(self):
        self.setup_logging()
        node_id_map = {}

        try:
            for index, row in self.df.iterrows():
                logging.info(f'Processing row: {index+7}')
                try:
                    if row['Actie'] == 'Verwijderen':
                        self.remove_node(row)
                    elif row['Actie'] == 'Toevoegen':
                        new_node_id = self.add_node(row)
                        if new_node_id is not None:
                            node_id_map[int(row['Node ID'])] = new_node_id
                            self.df.at[index, 'Verwerkt'] = new_node_id
                    elif row['Actie'] == 'Aanpassen':
                        if row['Verbinding'] == 'Node':
                            new_node_id = self.adjust_node(row)
                            if new_node_id is not None:
                                node_id_map[int(row['Node ID.2'])] = new_node_id
                                self.df.at[index, 'Verwerkt'] = new_node_id
                        elif row['Verbinding'] == 'Edge' and row['Aanpassing'] == 'Stroomrichting Omdraaien':
                            self.adjust_edges(row, node_id_map)
                except Exception as e:
                    logging.error(f"Error processing {row['Actie']}, {row['Verbinding']}, at index {index}: {e}")
        finally:
            for handler in logging.root.handlers[:]:
                handler.close()
                logging.root.removeHandler(handler)

        print("Processed all actions")

    def remove_node(self, row):
        try:
            key = row['Node Type']
            key = mapping[key]
            node_id = int(row['Node ID'])
            logging.info(f'Node ID: {node_id}')
            value = getattr(self.model, key, None)

            # Identify the discrete control node before removing any edges
            discrete_control_id = None
            if key == 'pump':
                # Find the discrete control node connected to this pump
                for index, edge in self.model.edge.df.iterrows():
                    if edge['from_node_id'] == node_id or edge['to_node_id'] == node_id:
                        connected_node_id = edge['to_node_id'] if edge['from_node_id'] == node_id else edge['from_node_id']
                        connected_node_type = self.df_node_types.loc[connected_node_id].node_type
                        if connected_node_type == 'DiscreteControl':  # Check for DiscreteControl type

                            discrete_control_id = connected_node_id
                            break

            if value is not None:
                if hasattr(value, '__dict__'):
                    for sub_key, sub_value in value.__dict__.items():
                        if hasattr(sub_value, 'df') and sub_value.df is not None:
                            if not sub_value.df.empty:
                                filtered_df = sub_value.df[sub_value.df['node_id'] != node_id]
                                sub_value.df = filtered_df

                                logging.info(f"Removed node (and edges) with Node Type: {key} and Node ID: {node_id}")

            rows_to_remove = self.model.edge.df[(self.model.edge.df['from_node_id'] == node_id) | (self.model.edge.df['to_node_id'] == node_id)].index
            self.model.edge.df = self.model.edge.df.drop(rows_to_remove)

            if discrete_control_id is not None:
                # Remove the discrete control node
                key = 'discrete_control'
                value = getattr(self.model, key, None)
                if value is not None and hasattr(value, '__dict__'):
                    for sub_key, sub_value in value.__dict__.items():
                        if hasattr(sub_value, 'df') and sub_value.df is not None:
                            if not sub_value.df.empty:
                                filtered_df = sub_value.df[sub_value.df['node_id'] != discrete_control_id]
                                sub_value.df = filtered_df

                                logging.info(f"Removed discrete control node with Node ID: {discrete_control_id}")

                # Remove edges connected to the discrete control node
                rows_to_remove = self.model.edge.df[(self.model.edge.df['from_node_id'] == discrete_control_id) | (self.model.edge.df['to_node_id'] == discrete_control_id)].index
                self.model.edge.df = self.model.edge.df.drop(rows_to_remove)
                logging.info(f"Removed edges connected to discrete control node with Node ID: {discrete_control_id}")

            logging.info(f"Successfully removed node with Node ID: {node_id}, Action: Verwijderen")
            return rows_to_remove

        except Exception as e:
            logging.error(f"Error removing node {row['Node ID']}: {e}")

    def add_discrete_control_node_for_pump(self, pump_node_id, pump_geometry):
        logging.info(f"Adding DiscreteControl node for Pump Node ID: {pump_node_id}")

        control_states = ["off", "on"]
        dfs_pump = self.model.pump.static.df

        if "control_state" not in dfs_pump.columns.tolist() or pd.isnull(dfs_pump.control_state).all():

            dfs_pump_list = []
            for control_state in control_states:
                df_pump = dfs_pump.copy()
                df_pump["control_state"] = control_state
                if control_state == "off":
                    df_pump["flow_rate"] = 0.0
                dfs_pump_list.append(df_pump)
            dfs_pump = pd.concat(dfs_pump_list, ignore_index=True)
            self.model.pump.static.df = dfs_pump

        cur_max_nodeid = self.get_current_max_nodeid()

        if cur_max_nodeid < 90000:
            new_nodeid = 90000 + cur_max_nodeid + 1
        else:
            new_nodeid = cur_max_nodeid + 1

        basin = self.model.edge.df[
            ((self.model.edge.df['to_node_id'] == pump_node_id) | (self.model.edge.df['from_node_id'] == pump_node_id)) &
            ((self.model.edge.df['from_node_type'] == "Basin") | (self.model.edge.df['to_node_type'] == "Basin"))
        ]
        assert len(basin) >= 1
        basin = basin.iloc[0, :].copy()
        if basin['from_node_type'] == "Basin":
            compound_variable_id = basin['from_node_id']
            listen_node_id = basin['from_node_id']
        else:
            compound_variable_id = basin['to_node_id']
            listen_node_id = basin['to_node_id']

        df_streefpeilen = self.model.basin.area.df.set_index("node_id")
        assert df_streefpeilen.index.is_unique


        try:
            self.model.discrete_control.add(
                Node(new_nodeid, pump_geometry),
                [
                    discrete_control.Variable(
                        compound_variable_id=compound_variable_id,
                        listen_node_type=["Basin"],
                        listen_node_id=listen_node_id,
                        variable=["level"],
                    ),
                    discrete_control.Condition(
                        compound_variable_id=compound_variable_id,
                        greater_than=[df_streefpeilen.at[listen_node_id, 'meta_streefpeil']],
                    ),
                    discrete_control.Logic(
                        truth_state=["F", "T"],
                        control_state=control_states,
                    ),
                ],
            )
            logging.info(f"Added DiscreteControl Node with ID: {new_nodeid}")
        except Exception as e:
            logging.error(f"Error adding DiscreteControl Node: {e}")

        try:
            self.model.edge.add(self.model.discrete_control[new_nodeid], self.model.pump[pump_node_id])
            logging.info(f"Added control edge from DiscreteControl Node ID: {new_nodeid} to Pump Node ID: {pump_node_id}")
        except Exception as e:
            logging.error(f"Error adding control edge: {e}")

        new_node_type_row = pd.DataFrame([{
            'fid': np.nan,
            'name': np.nan,
            'node_type': 'discrete_control',
            'subnetwork_id': np.nan,
        }], index=[new_nodeid])

        self.df_node_types = pd.concat([self.df_node_types, new_node_type_row])

        logging.info(f"Added DiscreteControl node with Node ID: {new_nodeid} at the same location as Pump with Node ID: {pump_node_id}")
        logging.info(f"Added control edge from DiscreteControl Node ID: {new_nodeid} to Pump Node ID: {pump_node_id}")


    def add_node(self, row):
        try:
            if pd.isna(row['Node Type.1']):
                logging.warning(f"Skipping row with NaN values: {row}")
                return None

            max_id = self.get_current_max_nodeid()
            node_id = max_id + 1
            logging.info(f'Node ID: {node_id}')
            key = row['Node Type.1']
            key = mapping.get(key, None)

            value = getattr(self.model, key, None)
            if value is not None:
                pump_geometry = None
                if hasattr(value, '__dict__'):
                    for sub_key, sub_value in value.__dict__.items():
                        if sub_key == 'time' or sub_key == 'subgrid':
                            continue
                        else:
                            sub_value = getattr(value, sub_key, None)
                            if sub_value is None or not hasattr(sub_value, 'df') or sub_value.df is None:
                                logging.error(f"Sub value for key '{sub_key}' is None or has no DataFrame")
                                continue
                            df_value = sub_value.df.copy()
                            last_row = df_value.iloc[-1].copy()
                            last_row['node_id'] = node_id
                            if 'geometry' in last_row:
                                x_coord = row['Coordinaat X']
                                y_coord = row['Coordinaat Y']
                                last_row['geometry'] = Point(x_coord, y_coord)
                                pump_geometry = last_row['geometry']
                            for col in last_row.index:
                                if col.startswith('meta_'):
                                    last_row[col] = np.nan
                            new_row_df = pd.DataFrame([last_row])
                            df_value = pd.concat([df_value, new_row_df], ignore_index=True)
                            sub_value.df = df_value
                if key in ["level_boundary", "flow_boundary", "terminal"]:
                    new_node = getattr(self.model, key, None)[node_id]
                    if pd.notna(row['Node ID A']):
                        node_type_a = self.df_node_types.loc[int(row['Node ID A'])].node_type
                        node_type_a = mapping[node_type_a]
                        node_a = getattr(self.model, node_type_a, None)[int(row['Node ID A'])]
                        self.model.edge.add(new_node, node_a)
                    else:
                        logging.warning(f"'Node ID A' is NaN for node type {key} at index {row.name}")
                else:
                    if pd.isna(row['Node ID A']) or pd.isna(row['Node ID B']):
                        logging.error(f"'Node ID A' or 'Node ID B' is NaN for node type {key} at index {row.name}")
                        return None
                    new_node = getattr(self.model, key, None)[node_id]
                    node_type_a = self.df_node_types.loc[int(row['Node ID A'])].node_type
                    node_type_b = self.df_node_types.loc[int(row['Node ID B'])].node_type
                    node_type_a = mapping[node_type_a]
                    node_type_b = mapping[node_type_b]
                    node_a = getattr(self.model, node_type_a, None)[int(row['Node ID A'])]
                    node_b = getattr(self.model, node_type_b, None)[int(row['Node ID B'])]
                    self.model.edge.add(node_a, new_node)
                    self.model.edge.add(new_node, node_b)

                new_node_type_row = pd.DataFrame([{
                    'fid': np.nan,
                    'name': np.nan,
                    'node_type': key,
                    'subnetwork_id': np.nan,
                }], index=[node_id])

                self.df_node_types = pd.concat([self.df_node_types, new_node_type_row])

                logging.info(f"Successfully added node with Node ID: {node_id}, Action: Toevoegen")

                # Add a DiscreteControl node at the same location as the pump and connect it
                if key == 'pump' and pump_geometry is not None:
                    self.add_discrete_control_node_for_pump(node_id, pump_geometry)

        except Exception as e:
            logging.error(f"Error adding node at row {row.name}: {e}")

    def adjust_node(self, row):
        try:
            # Get the old node type and id
            key = self.df_node_types.loc[int(row['Node ID.2'])].node_type
            key = mapping[key]
            node_id = int(row['Node ID.2'])
            logging.info(f'Node ID: {node_id}')
            node_id_old = node_id
            value = getattr(self.model, key, None)
            pump_geometry = None

            if value is not None:
                if hasattr(value, '__dict__'):
                    for sub_key, sub_value in value.__dict__.items():
                        if hasattr(sub_value, 'df') and sub_value.df is not None:
                            if not sub_value.df.empty:
                                if 'geometry' in sub_value.df and not sub_value.df[sub_value.df['node_id'] == node_id].empty:
                                    geometry_old = sub_value.df[sub_value.df['node_id'] == node_id].geometry.iloc[0]
                                    pump_geometry = geometry_old
                                    filtered_df = sub_value.df[sub_value.df['node_id'] != node_id]
                                    sub_value.df = filtered_df
                                else:
                                    filtered_df = sub_value.df[sub_value.df['node_id'] != node_id]
                                    sub_value.df = filtered_df

            # Remove discrete control if the old node was a pump
            if key == 'pump':
                # Identify the discrete control node before removing any edges
                discrete_control_id = None
                for index, edge in self.model.edge.df.iterrows():
                    if edge['from_node_id'] == node_id or edge['to_node_id'] == node_id:
                        connected_node_id = edge['to_node_id'] if edge['from_node_id'] == node_id else edge['from_node_id']
                        connected_node_type = self.df_node_types.loc[connected_node_id].node_type
                        if connected_node_type == 'DiscreteControl':  # Check for DiscreteControl type
                            discrete_control_id = connected_node_id
                            break

                # Remove edges connected to the pump node
                rows_to_remove = self.model.edge.df[(self.model.edge.df['from_node_id'] == node_id) | (self.model.edge.df['to_node_id'] == node_id)].index
                self.model.edge.df = self.model.edge.df.drop(rows_to_remove)

                # Remove the discrete control node if it exists
                if discrete_control_id is not None:
                    key = 'discrete_control'
                    value = getattr(self.model, key, None)
                    if value is not None and hasattr(value, '__dict__'):
                        for sub_key, sub_value in value.__dict__.items():
                            if hasattr(sub_value, 'df') and sub_value.df is not None:
                                if not sub_value.df.empty:
                                    filtered_df = sub_value.df[sub_value.df['node_id'] != discrete_control_id]
                                    sub_value.df = filtered_df
                                    logging.info(f"Removed discrete control node with Node ID: {discrete_control_id}")

                    # Remove edges connected to the discrete control node
                    rows_to_remove = self.model.edge.df[(self.model.edge.df['from_node_id'] == discrete_control_id) | (self.model.edge.df['to_node_id'] == discrete_control_id)].index
                    self.model.edge.df = self.model.edge.df.drop(rows_to_remove)
                    logging.info(f"Removed edges connected to discrete control node with Node ID: {discrete_control_id}")

            # Get the new node type and add the node
            max_id = self.get_current_max_nodeid()
            node_id = max_id + 1
            key = row['Nieuw Node Type']
            key = mapping.get(key, None)
            value = getattr(self.model, key, None)

            if value is not None:
                if hasattr(value, '__dict__'):
                    for sub_key, sub_value in value.__dict__.items():
                        if sub_key == 'time':
                            continue  
                        else:
                            sub_value = getattr(value, sub_key, None)
                            if sub_value is None or not hasattr(sub_value, 'df') or sub_value.df is None:
                                continue
                            df_value = sub_value.df.copy()
                            last_row = df_value.iloc[-1].copy()
                            last_row['node_id'] = node_id
                            if 'geometry' in last_row:
                                last_row['geometry'] = geometry_old if 'geometry_old' in locals() else None
                            for col in last_row.index:
                                if col.startswith('meta_'):
                                    last_row[col] = np.nan
                            new_row_df = pd.DataFrame([last_row])
                            df_value = pd.concat([df_value, new_row_df], ignore_index=True)
                            sub_value.df = df_value

            # Adjust edges
            rows_to_remove = self.model.edge.df[(self.model.edge.df['from_node_id'] == node_id_old) | (self.model.edge.df['to_node_id'] == node_id_old)]
            if len(rows_to_remove) == 1:
                for idx, edge_row in rows_to_remove.iterrows():
                    if edge_row['to_node_id'] == node_id_old:
                        node_type_a = edge_row['from_node_type']
                        node_id_a = edge_row['from_node_id']
                        rows_to_remove = rows_to_remove.index
                        self.model.edge.df = self.model.edge.df.drop(rows_to_remove)
                        new_node = getattr(self.model, key, None)[node_id]
                        node_a = getattr(self.model, mapping[node_type_a], None)[int(node_id_a)]
                        self.model.edge.add(node_a, new_node)
                    if edge_row['from_node_id'] == node_id_old:
                        node_type_b = edge_row['to_node_type']
                        node_id_b = edge_row['to_node_id']
                        rows_to_remove = rows_to_remove.index
                        self.model.edge.df = self.model.edge.df.drop(rows_to_remove)
                        new_node = getattr(self.model, key, None)[node_id]
                        node_b = getattr(self.model, mapping[node_type_b], None)[int(node_id_b)]
                        self.model.edge.add(new_node, node_b)
            if len(rows_to_remove) > 1:
                for idx, edge_row in rows_to_remove.iterrows():
                    if edge_row['to_node_id'] == node_id_old:
                        node_type_a = edge_row['from_node_type']
                        node_id_a = edge_row['from_node_id']
                    if edge_row['from_node_id'] == node_id_old:
                        node_type_b = edge_row['to_node_type']
                        node_id_b = edge_row['to_node_id']
                rows_to_remove = rows_to_remove.index
                self.model.edge.df = self.model.edge.df.drop(rows_to_remove)
                new_node = getattr(self.model, key, None)[node_id]
                node_a = getattr(self.model, mapping[node_type_a], None)[int(node_id_a)]
                node_b = getattr(self.model, mapping[node_type_b], None)[int(node_id_b)]
                self.model.edge.add(node_a, new_node)
                self.model.edge.add(new_node, node_b)

            # Add discrete control if the new node is a pump
            if key == 'pump' and pump_geometry is not None:
                self.add_discrete_control_node_for_pump(node_id, pump_geometry)

            logging.info(f"Successfully adjusted node with old Node ID: {node_id_old}, new Node ID: {node_id}, Action: Aanpassen")
            return node_id

        except Exception as e:
            logging.error(f"Error adjusting node at row: {row}", exc_info=True)
            return None


    def adjust_edges(self, row, node_id_map):
        try:
            node_a = int(node_id_map.get(row['Node ID A.1'], row['Node ID A.1']))
            node_b = int(node_id_map.get(row['Node ID B.1'], row['Node ID B.1']))
            print(node_a, node_b)
            df_row_a_b = self.model.edge.df[(self.model.edge.df['from_node_id'] == node_a) & (self.model.edge.df['to_node_id'] == node_b)]
            df_row_b_a = self.model.edge.df[(self.model.edge.df['from_node_id'] == node_b) & (self.model.edge.df['to_node_id'] == node_a)]
            if df_row_a_b.empty and df_row_b_a.empty:
                logging.error(f"Edge not found between Node A: {node_a} and Node B: {node_b} at index {row}")
                return
            if not df_row_a_b.empty:
                df_row = df_row_a_b
            else:
                df_row = df_row_b_a
            self.model.edge.df.loc[df_row.index, ['from_node_id', 'to_node_id']] = self.model.edge.df.loc[df_row.index, ['to_node_id', 'from_node_id']].values
            if 'geometry' in df_row.columns:
                row_index = df_row.index[0]
                line = self.model.edge.df.loc[row_index].geometry
                reversed_coords = list(line.coords)[::-1]
                reversed_line = LineString(reversed_coords)
                self.model.edge.df.at[row_index, 'geometry'] = reversed_line
            print(f"Swapped edge direction between Node A: {node_a} and Node B: {node_b}")
            logging.info(f"Successfully swapped edge direction between Node A: {node_a} and Node B: {node_b}, Action: Aanpassen, Adjustment: Stroomrichting Omdraaien")
        except Exception as e:
            logging.error(f"Error adjusting edge at index {index+7}: {e}")

    def special_preprocessing_for_hollandse_delta(self):
        p1 = Proj('epsg:4326')  # WGS84
        p2 = Proj('epsg:28992')  # Rijksdriehoekstelsel
        transformer = Transformer.from_proj(p1, p2)

        def clean_coordinate(coord_str):
            if pd.isna(coord_str):
                return None
            coord_str = str(coord_str).replace('°E', '').replace('°N', '').replace(',', '.')
            return float(coord_str)

        for index, row in self.df.iterrows():
            if pd.notna(row['Coordinaat X']) and pd.notna(row['Coordinaat Y']):
                lon_str = row['Coordinaat X']
                lat_str = row['Coordinaat Y']
                lon = clean_coordinate(lon_str)
                lat = clean_coordinate(lat_str)
                x, y = transformer.transform(lon, lat)
                self.df.at[index, 'Coordinaat X'] = x
                self.df.at[index, 'Coordinaat Y'] = y

    def save_feedback(self):
        self.df['Naam.1'] = self.name
        self.df['Datum.1'] = datetime.now().strftime('%d-%m-%Y')
        self.df['Versie'] = self.versie
        self.df.to_excel(self.feedback_excel_processed, index=False)

    def run(self):
        if self.waterschap == "Hollandse Delta":
            self.special_preprocessing_for_hollandse_delta()
        self.process_model()
        self.save_feedback()
        self.write_ribasim_model()

# # Voorbeeld gebruik
# name = "Jerom Aerts (HKV)"
# waterschap = "HHSK"
# versie = "2024_6_1"

# feedback_excel = r"C:\Users\Aerts\Desktop\RIBASIM Project\Verwerken_Feedback\formulieren\feedback_formulier_HHSK_RB.xlsx"
# feedback_excel_processed = r"C:\Users\Aerts\Desktop\RIBASIM Project\Verwerken_Feedback\verwerkte_formulieren\feedback_formulier_RB_HHSK_processed.xlsx"

# ribasim_toml = r"C:\Users\Aerts\Desktop\RIBASIM Project\Verwerken_Feedback\modellen\SchielandendeKrimpenerwaard_boezemmodel_2024_6_1\ribasim.toml"
# output_folder = r"C:\Users\Aerts\Desktop\RIBASIM Project\Verwerken_Feedback\verwerkte_modellen"

# processor = RibasimFeedbackProcessor(name, waterschap, versie, feedback_excel, ribasim_toml, output_folder, feedback_excel_processed)
# processor.run()

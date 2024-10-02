from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel
from ribasim import Model, Node
from ribasim.geometry.edge import NodeData
from shapely.geometry import LineString, Point
from shapely.geometry.base import BaseGeometry

from ribasim_nl.case_conversions import pascal_to_snake_case


def read_arrow(filepath: Path) -> pd.DataFrame:
    df = pd.read_feather(filepath)
    if "time" in df.columns:
        df.set_index("time", inplace=True)
    return df


def node_properties_to_table(table, node_properties, node_id):
    # update DataFrame
    table_node_df = getattr(table, "node").df
    for column, value in node_properties.items():
        table_node_df.loc[node_id, [column]] = value


class BasinResults(BaseModel):
    filepath: Path
    _df = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._basin_df = read_arrow(self.filepath)
        return self._basin_df


class Model(Model):
    _basin_results: BasinResults | None = None

    @property
    def basin_results(self):
        if self._basin_results is None:
            filepath = self.filepath.parent.joinpath(self.results_dir, "basin.arrow").absolute().resolve()
            self._basin_results = BasinResults(filepath=filepath)
        return self._basin_results

    @property
    def next_node_id(self):
        return self.node_table().df.index.max() + 1

    def find_node_id(self, ds_node_id=None, us_node_id=None, **kwargs) -> int:
        """Find a node_id by it's properties"""
        # get node table
        df = self.node_table().df

        # filter node ids by properties
        for column, value in kwargs.items():
            df = df[df[column] == value]

        # filter node ids by us and ds node
        if (ds_node_id is not None) or (us_node_id is not None):
            edge_df = self.edge.df
            if ds_node_id is not None:
                df = df[df.node_id.isin(edge_df[edge_df.to_node_id == ds_node_id].from_node_id)]
            if us_node_id is not None:
                df = df[df.node_id.isin(edge_df[edge_df.from_node_id == us_node_id].to_node_id)]

        # check if we didn't find 0 or multiple node_ids
        node_ids = df.node_id.to_list()
        if len(node_ids) == 0:
            raise ValueError(
                f"no node_id found with properties {kwargs} us_node_id {us_node_id} and ds_node_id {ds_node_id}"
            )
        if len(node_ids) > 1:
            raise ValueError(
                f"multiple node_ids found ({node_ids}) with properties {kwargs} us_node_id {us_node_id} and ds_node_id {ds_node_id}"
            )

        # return if successfull
        else:
            return node_ids[0]

    @property
    def unassigned_basin_area(self):
        """Get unassigned basin area"""
        return self.basin.area.df[~self.basin.area.df.node_id.isin(self.basin.node.df.index)]

    @property
    def basin_node_without_area(self):
        """Get basin node without area"""
        return self.basin.node.df[~self.basin.node.df.index.isin(self.basin.area.df.node_id)]

    def upstream_node_id(self, node_id: int):
        """Get upstream node_id(s)"""
        return self.edge.df.set_index("to_node_id").loc[node_id].from_node_id

    def upstream_profile(self, node_id: int):
        """Get upstream basin-profile"""
        upstream_node_id = self.upstream_node_id(node_id)

        node_type = self.node_table().df.loc[upstream_node_id].node_type
        if node_type != "Basin":
            raise ValueError(f"Upstream node_type is not a Basin, but {node_type}")
        else:
            return self.basin.profile[upstream_node_id]

    def downstream_node_id(self, node_id: int):
        """Get downstream node_id(s)"""
        return self.edge.df.set_index("from_node_id").loc[node_id].to_node_id

    def downstream_profile(self, node_id: int):
        """Get upstream basin-profile"""
        downstream_node_id = self.downstream_node_id(node_id)

        node_type = self.node_table().df.loc[downstream_node_id].node_type
        if node_type != "Basin":
            raise ValueError(f"Upstream node_type is not a Basin, but {node_type}")
        else:
            return self.basin.profile[downstream_node_id]

    def get_node_type(self, node_id: int):
        return self.node_table().df.at[node_id, "node_type"]

    def get_node(self, node_id: int):
        """Return model-node by node_id"""
        node_type = self.get_node_type(node_id)
        return getattr(self, pascal_to_snake_case(node_type))[node_id]

    def remove_node(self, node_id: int, remove_edges: bool = False):
        """Remove node from model"""
        node_type = self.get_node_type(node_id)

        # read existing table
        table = getattr(self, pascal_to_snake_case(node_type))

        # remove node from all tables
        for attr in table.model_fields.keys():
            df = getattr(table, attr).df
            if df is not None:
                if node_id in df.columns:
                    getattr(table, attr).df = df[df.node_id != node_id]
                else:
                    getattr(table, attr).df = df[df.index != node_id]

        if remove_edges and (self.edge.df is not None):
            for row in self.edge.df[self.edge.df.from_node_id == node_id].itertuples():
                self.remove_edge(
                    from_node_id=row.from_node_id, to_node_id=row.to_node_id, remove_disconnected_nodes=False
                )
            for row in self.edge.df[self.edge.df.to_node_id == node_id].itertuples():
                self.remove_edge(
                    from_node_id=row.from_node_id, to_node_id=row.to_node_id, remove_disconnected_nodes=False
                )

        # remove from used node-ids so we can add it again in the same table
        if node_id in table._parent._used_node_ids:
            table._parent._used_node_ids.node_ids.remove(node_id)

    def update_node(self, node_id, node_type, data, node_properties: dict = {}):
        existing_node_type = self.node_table().df.at[node_id, "node_type"]

        # read existing table
        table = getattr(self, pascal_to_snake_case(existing_node_type))

        # save node, so we can add it later
        node_dict = table.node.df.loc[node_id].to_dict()
        node_dict.pop("node_type")
        node_dict["node_id"] = node_id

        # remove node from all tables
        for attr in table.model_fields.keys():
            df = getattr(table, attr).df
            if df is not None:
                if "node_id" in df.columns:
                    getattr(table, attr).df = df[df.node_id != node_id]
                else:
                    getattr(table, attr).df = df[df.index != node_id]

        # remove from used node-ids so we can add it again in the same table
        if node_id in table._parent._used_node_ids:
            table._parent._used_node_ids.node_ids.remove(node_id)

        # add to table
        table = getattr(self, pascal_to_snake_case(node_type))
        table.add(Node(**node_dict), data)

        # sanitize node_dict
        drop_keys = ["node_id", "node_type"] + list(node_properties.keys())
        node_dict = {k: v for k, v in node_dict.items() if k not in drop_keys}

        # complete node_properties
        node_properties = {**node_properties, **node_dict}
        node_properties_to_table(table, node_properties, node_id)

    def add_control_node(
        self,
        to_node_id: int | list,
        data,
        ctrl_type: Literal["DiscreteControl", "PidControl"] = "DiscreteControl",
        node_geom: Point | None = None,
        node_offset: int = 100,
        node_properties: dict = {},
    ):
        """Add a control_node to the network

        Parameters
        ----------
        to_node_id : int | list
            node_id or list of node_ids to connect control node to
        data:
            data for the control node

        offset_node_id : int | None, optional
            User can explicitly specify a offset_node_id for a left-offset of the control-node. by default None
        node_geom : tuple | None, optional
            User can explicitly specify an Point for the control node. Has to be set if multiple to_node_ids are defined.
            If None, see node_offset. by default None
        node_offset : int, optional
            left-side offset of control node to to_node_id in case it's one node_id and node_geom is not defined, In other cases this value is ignored.
            By default 100
        node_properties: dict, optional
            extra properties to add to the control node id.

        """
        # define node
        if node_geom is None:
            if isinstance(to_node_id, list):
                raise TypeError(f"to_node_id is a list ({to_node_id}. node_geom should be defined (is None))")
            else:
                linestring = self.edge.df[self.edge.df["to_node_id"] == to_node_id].iloc[0].geometry
                node_geom = Point(linestring.parallel_offset(node_offset, "left").coords[-1])
                to_node_id = [to_node_id]

        node_id = self.next_node_id
        node = Node(node_id=node_id, geometry=node_geom)

        # add node
        table = getattr(self, pascal_to_snake_case(ctrl_type))
        table.add(node, data)

        # add node properties
        node_properties_to_table(table, node_properties, node_id)

        # add edges
        for _to_node_id in to_node_id:
            self.edge.add(table[node_id], self.get_node(_to_node_id))

    def reverse_edge(self, from_node_id: int | None = None, to_node_id: int | None = None, edge_id: int | None = None):
        """Reverse an edge"""
        if self.edge.df is not None:
            if edge_id is None:
                # get original edge-data
                df = self.edge.df.copy()
                df.loc[:, ["edge_id"]] = df.index
                df = df.set_index(["from_node_id", "to_node_id"], drop=False)
                edge_data = dict(df.loc[from_node_id, to_node_id])
                edge_id = edge_data["edge_id"]
            else:
                edge_data = dict(self.edge.df.loc[edge_id])

            # revert node ids
            self.edge.df.loc[edge_id, ["from_node_id"]] = edge_data["to_node_id"]
            self.edge.df.loc[edge_id, ["to_node_id"]] = edge_data["from_node_id"]

            # revert geometry
            self.edge.df.loc[edge_id, ["geometry"]] = edge_data["geometry"].reverse()

    def remove_edge(self, from_node_id: int, to_node_id: int, remove_disconnected_nodes=True):
        """Remove an edge and disconnected nodes"""
        if self.edge.df is not None:
            # get original edge-data
            indices = self.edge.df[
                (self.edge.df.from_node_id == from_node_id) & (self.edge.df.to_node_id == to_node_id)
            ].index

            # remove edge from edge-table
            self.edge.df = self.edge.df[~self.edge.df.index.isin(indices)]

            # remove disconnected nodes
            if remove_disconnected_nodes:
                for node_id in [from_node_id, to_node_id]:
                    if node_id not in self.edge.df[["from_node_id", "to_node_id"]].to_numpy().ravel():
                        self.remove_node(node_id)

    def move_node(self, node_id: int, geometry: Point):
        node_type = self.node_table().df.at[node_id, "node_type"]

        # read existing table
        table = getattr(self, pascal_to_snake_case(node_type))

        # update geometry
        table.node.df.loc[node_id, ["geometry"]] = geometry

        # reset all edges
        edge_ids = self.edge.df[
            (self.edge.df.from_node_id == node_id) | (self.edge.df.to_node_id == node_id)
        ].index.to_list()
        self.reset_edge_geometry(edge_ids=edge_ids)

    def find_closest_basin(self, geometry: BaseGeometry, max_distance: float | None) -> NodeData:
        """Find the closest basin_node."""
        # only works when basin area are defined
        if self.basin.area.df is None:
            raise ValueError("No basin.area table defined for model")

        # get distance of geometry to basin / area
        distance_to_model_df = self.basin.area.df.distance(geometry)

        # get basin node-id
        basin_node_id = self.basin.area.df.at[distance_to_model_df.idxmin(), "node_id"]

        # check if distance isn't too large
        if max_distance is not None:
            if distance_to_model_df.min() > max_distance:
                raise (
                    Exception(
                        f"Closest basin {basin_node_id} further than {max_distance} from geometry: {distance_to_model_df.min()}"
                    )
                )

        return self.basin[basin_node_id]

    def fix_unassigned_basin_area(self, method: str = "within", distance: float = 100):
        """Assign a Basin node_id to a Basin / Area if the Area doesn't contain a basin node_id.

        Args:
            method (str): method to find basin node_id; `within` or `closest`. First start with `within`. Default is `within`
            distance (float, optional): for method closest, the distance to find an unassigned basin node_id. Defaults to 100.
        """
        if self.basin.node.df is not None:
            if self.basin.area.df is not None:
                basin_area_df = self.basin.area.df[~self.basin.area.df.node_id.isin(self.basin.node.df.index)]

                for row in basin_area_df.itertuples():
                    if method == "within":
                        # check if area contains basin-nodes
                        basin_df = self.basin.node.df[self.basin.node.df.within(row.geometry)]

                    elif method == "closest":
                        basin_df = self.basin.node.df[self.basin.node.df.within(row.geometry)]
                        # if method is `distance` and basin_df is emtpy we create a new basin_df
                        if basin_df.empty:
                            basin_df = self.basin.node.df[self.basin.node.df.distance(row.geometry) < distance]

                    else:
                        ValueError(f"Supported methods are 'within' or 'closest', got '{method}'.")

                    # check if basin_nodes within area are not yet assigned an area
                    basin_df = basin_df[~basin_df.index.isin(self.basin.area.df.node_id)]

                    # if we have one node left we are done
                    if len(basin_df) == 1:
                        self.basin.area.df.loc[row.Index, ["node_id"]] = basin_df.index[0]
            else:
                raise ValueError("Assign Basin Area to your model first")
        else:
            raise ValueError("Assign a Basin Node to your model first")

    def reset_edge_geometry(self, edge_ids: list | None = None):
        node_df = self.node_table().df
        if edge_ids is not None:
            df = self.edge.df[self.edge.df.index.isin(edge_ids)]
        else:
            df = self.edge.df

        for row in df.itertuples():
            from_point = Point(node_df.at[row.from_node_id, "geometry"].x, node_df.at[row.from_node_id, "geometry"].y)
            to_point = Point(node_df.at[row.to_node_id, "geometry"].x, node_df.at[row.to_node_id, "geometry"].y)
            geometry = LineString([from_point, to_point])
            self.edge.df.loc[row.Index, ["geometry"]] = geometry

    @property
    def edge_from_node_type(self):
        node_df = self.node_table().df
        return self.edge.df.from_node_id.apply(lambda x: node_df.at[x, "node_type"] if x in node_df.index else None)

    @property
    def edge_to_node_type(self):
        node_df = self.node_table().df
        return self.edge.df.to_node_id.apply(lambda x: node_df.at[x, "node_type"] if x in node_df.index else None)

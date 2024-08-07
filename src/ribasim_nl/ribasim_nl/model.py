from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel
from ribasim import Model, Node
from ribasim.geometry.edge import NodeData
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

from ribasim_nl.case_conversions import pascal_to_snake_case


def read_arrow(filepath: Path) -> pd.DataFrame:
    df = pd.read_feather(filepath)
    if "time" in df.columns:
        df.set_index("time", inplace=True)
    return df


def node_properties_to_table(table, node_properties, node_id):
    if isinstance(node_id, int):
        node_id = [node_id]
    # update DataFrame
    table_node_df = getattr(table, "node").df
    for column, value in node_properties.items():
        table_node_df.loc[table_node_df.node_id.isin(node_id), [column]] = value


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
        return self.node_table().df.node_id.max() + 1

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

    def get_node_type(self, node_id: int):
        return self.node_table().df.set_index("node_id").at[node_id, "node_type"]

    def get_node(self, node_id: int):
        """Return model-node by node_id"""
        node_type = self.get_node_type(node_id)
        return getattr(self, pascal_to_snake_case(node_type))[node_id]

    def update_node(self, node_id, node_type, data, node_properties: dict = {}):
        """Update a node type and/or data"""
        # get existing network node_type
        existing_node_type = self.node_table().df.set_index("node_id").at[node_id, "node_type"]

        # read existing table
        table = getattr(self, pascal_to_snake_case(existing_node_type))

        # save node, so we can add it later
        node_dict = table.node.df[table.node.df["node_id"] == node_id].iloc[0].to_dict()

        # remove node from all tables
        for attr in table.model_fields.keys():
            df = getattr(table, attr).df
            if df is not None:
                getattr(table, attr).df = df[df.node_id != node_id]

        # add to table
        table = getattr(self, pascal_to_snake_case(node_type))
        table.add(Node(**node_dict), data)

        # sanitize node_dict
        drop_keys = ["node_id", "node_type"] + list(node_properties.keys())
        node_dict = {k: v for k, v in node_dict.items() if k not in drop_keys}

        # complete node_properties
        node_properties = {**node_properties, **node_dict}
        node_properties_to_table(table, node_properties, node_id)

        # change type in edge table
        self.edge.df.loc[self.edge.df["from_node_id"] == node_id, ["from_node_type"]] = node_type
        self.edge.df.loc[self.edge.df["to_node_id"] == node_id, ["to_node_type"]] = node_type

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

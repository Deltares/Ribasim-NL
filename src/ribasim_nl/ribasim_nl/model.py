# %%
import warnings
from pathlib import Path
from typing import Literal

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import shapely
from pydantic import BaseModel
from ribasim import Model, Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, pump, tabulated_rating_curve

try:
    from ribasim.validation import flow_edge_neighbor_amount as edge_amount
except ImportError:
    from ribasim.validation import flow_link_neighbor_amount as edge_amount

from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry

from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.downstream import downstream_nodes
from ribasim_nl.geometry import split_basin
from ribasim_nl.parametrization.parameterize import Parameterize
from ribasim_nl.run_model import run
from ribasim_nl.upstream import upstream_nodes

manning_data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])
level_data = level_boundary.Static(level=[0])


class default_tables:
    basin = [
        basin.Profile(level=[0.0, 1.0], area=[0.01, 1000.0]),
        basin.Static(
            drainage=[0.0],
            potential_evaporation=[0.001 / 86400],
            infiltration=[0.0],
            precipitation=[0.005 / 86400],
        ),
        basin.State(level=[0]),
    ]
    outlet = [outlet.Static(flow_rate=[100])]
    pump = [pump.Static(flow_rate=[1])]
    manning_resistance = [
        manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])
    ]
    level_boundary = [level_boundary.Static(level=[0])]
    tabulated_rating_curve = [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 10])]


DEFAULT_TABLES = default_tables()


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


class Results(BaseModel):
    filepath: Path
    _df = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._basin_df = read_arrow(self.filepath)
        return self._basin_df


class Model(Model):
    _basin_results: Results | None = None
    _basin_outstate: Results | None = None
    _flow_results: Results | None = None
    _graph: nx.Graph | None = None
    _parameterize: Parameterize | None = None

    def __init__(self, **data):
        super().__init__(**data)
        self._parameterize = Parameterize(model=self)

    def parameterize(self, **kwargs):
        self._parameterize.run(**kwargs)

    @property
    def basin_results(self):
        if self._basin_results is None:
            filepath = self.filepath.parent.joinpath(self.results_dir, "basin.arrow").absolute().resolve()
            self._basin_results = Results(filepath=filepath)
        return self._basin_results

    @property
    def flow_results(self):
        if self._flow_results is None:
            filepath = self.filepath.parent.joinpath(self.results_dir, "flow.arrow").absolute().resolve()
            self._flow_results = Results(filepath=filepath)
        return self._flow_results

    @property
    def link_results(self):
        if self._basin_results is None:
            filepath = self.filepath.parent.joinpath(self.results_dir, "basin.arrow").absolute().resolve()
            self._basin_results = Results(filepath=filepath)
        return self._basin_results

    @property
    def basin_outstate(self):
        if self._basin_outstate is None:
            filepath = self.filepath.parent.joinpath(self.results_dir, "basin_state.arrow").absolute().resolve()
            self._basin_outstate = Results(filepath=filepath)
        return self._basin_outstate

    @property
    def graph(self):
        # create a DiGraph from edge-table
        if self._graph is None:
            graph = nx.from_pandas_edgelist(
                df=self.edge.df[["from_node_id", "to_node_id"]],
                source="from_node_id",
                target="to_node_id",
                create_using=nx.DiGraph,
            )
            if "meta_function" not in self.node_table().df.columns:
                node_attributes = {node_id: {"function": ""} for node_id in self.node_table().df.index}
            else:
                node_attributes = (
                    self.node_table()
                    .df.rename(columns={"meta_function": "function"})[["function"]]
                    .to_dict(orient="index")
                )
            nx.set_node_attributes(graph, node_attributes)

            self._graph = graph

        return self._graph

    @property
    def reset_graph(self):
        self._graph = None
        return self.graph

    @property
    def next_node_id(self):
        return self.node_table().df.index.max() + 1

    def run(self, **kwargs):
        """Run your Ribasim model"""
        return run(self.filepath, **kwargs)

    def update_state(self, time_stamp: pd.Timestamp | None = None):
        """Update basin.state with results or final basin_state (outstate)

        Args:
            time_stamp (pd.Timestamp | None, optional): Timestamp in results to update basin.state with . Defaults to None.
        """
        if time_stamp is None:
            df = self.basin_outstate.df
        else:
            df = self.basin_results.df.loc[time_stamp][["node_id", "level"]]
            df.reset_index(inplace=True, drop=True)
        df.index += 1
        df.index.name = "fid"
        self.basin.state.df = df

    # methods relying on networkx. Discuss making this all in a subclass of Model
    def _upstream_nodes(self, node_id, **kwargs):
        # get upstream nodes
        #     return list(nx.traversal.bfs_tree(self.graph, node_id, reverse=True))
        return upstream_nodes(graph=self.graph, node_id=node_id, **kwargs)

    def _downstream_nodes(self, node_id, **kwargs):
        # get downstream nodes
        return downstream_nodes(graph=self.graph, node_id=node_id, **kwargs)
        # return list(nx.traversal.bfs_tree(self.graph, node_id))

    def get_upstream_basins(self, node_id, **kwargs):
        # get upstream basin area
        upstream_node_ids = self._upstream_nodes(node_id, **kwargs)
        return self.basin.area.df[self.basin.area.df.node_id.isin(upstream_node_ids)]

    def get_downstream_basins(self, node_id, **kwargs):
        # get upstream basin area
        downstream_node_ids = self._downstream_nodes(node_id, **kwargs)
        return self.basin.area.df[self.basin.area.df.node_id.isin(downstream_node_ids)]

    def get_upstream_edges(self, node_id, **kwargs):
        # get upstream edges
        upstream_node_ids = self._upstream_nodes(node_id, **kwargs)
        mask = self.edge.df.from_node_id.isin(upstream_node_ids[1:]) & self.edge.df.to_node_id.isin(upstream_node_ids)
        return self.edge.df[mask]

    def get_downstream_edges(self, node_id, **kwargs):
        # get upstream edges
        downstream_node_ids = self._downstream_nodes(node_id, **kwargs)
        mask = self.edge.df.from_node_id.isin(downstream_node_ids) & self.edge.df.to_node_id.isin(
            downstream_node_ids[1:]
        )
        return self.edge.df[mask]

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
        _df = self.edge.df.set_index("to_node_id")
        if node_id in _df.index:
            return _df.loc[node_id].from_node_id

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
                if "node_id" in df.columns:
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

    def update_node(self, node_id, node_type, data: list | None = None, node_properties: dict = {}):
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
        if data is None:
            data = getattr(DEFAULT_TABLES, pascal_to_snake_case(node_type))
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

    def remove_edges(self, edge_ids: list[int]):
        if self.edge.df is not None:
            self.edge.df = self.edge.df[~self.edge.df.index.isin(edge_ids)]

    def add_basin(self, node_id, geometry, tables=None, **kwargs):
        # define node properties
        if "name" in kwargs.keys():
            name = kwargs["name"]
            kwargs.pop("name")
        else:
            name = ""

        node_properties = {k if k.startswith("meta_") else f"meta_{k}": v for k, v in kwargs.items()}

        # define tables, defaults if None
        if tables is None:
            tables = DEFAULT_TABLES.basin

        self.basin.add(Node(node_id=node_id, geometry=geometry, name=name, **node_properties), tables=tables)

    def connect_basins(self, from_basin_id, to_basin_id, node_type, geometry, tables=None, name="", **kwargs):
        if name is None:
            name = ""
        self.add_and_connect_node(
            from_basin_id=from_basin_id,
            to_basin_id=to_basin_id,
            node_type=node_type,
            geometry=geometry,
            tables=tables,
            name=name,
            **kwargs,
        )

    def add_and_connect_node(self, from_basin_id, to_basin_id, geometry, node_type, name="", tables=None, **kwargs):
        if name is None:
            name = ""

        # define node properties
        node_properties = {k if k.startswith("meta_") else f"meta_{k}": v for k, v in kwargs.items()}

        # define tables, defaults if None
        if tables is None:
            tables = getattr(DEFAULT_TABLES, pascal_to_snake_case(node_type))

        # add node
        node = getattr(self, pascal_to_snake_case(node_type)).add(
            Node(geometry=geometry, name=name, **node_properties), tables=tables
        )

        # add edges from and to node
        self.edge.add(self.get_node(from_basin_id), node)
        self.edge.add(node, self.get_node(to_basin_id))

    def add_basin_outlet(self, basin_id, geometry, node_type="Outlet", tables=None, **kwargs):
        # define node properties
        if "name" in kwargs.keys():
            name = kwargs["name"]
            kwargs.pop("name")
        else:
            name = ""

        node_properties = {k if k.startswith("meta_") else f"meta_{k}": v for k, v in kwargs.items()}

        # define tables, defaults if None
        if tables is None:
            tables = getattr(DEFAULT_TABLES, pascal_to_snake_case(node_type))

        # add outlet
        node = getattr(self, pascal_to_snake_case(node_type)).add(
            Node(geometry=geometry, name=name, **node_properties), tables=tables
        )

        # add edges from and to node
        self.edge.add(self.basin[basin_id], node)
        edge_geometry = self.edge.df.set_index(["from_node_id", "to_node_id"]).at[(basin_id, node.node_id), "geometry"]

        # add boundary
        geometry = shapely.affinity.scale(edge_geometry, xfact=1.05, yfact=1.05, origin="center").boundary.geoms[1]
        # geometry = edge_geometry.interpolate(1.05, normalized=True)
        boundary_node = self.level_boundary.add(Node(geometry=geometry), tables=DEFAULT_TABLES.level_boundary)
        self.edge.add(node, boundary_node)

    def reverse_direction_at_node(self, node_id):
        for edge_id in self.edge.df[
            (self.edge.df.from_node_id == node_id) | (self.edge.df.to_node_id == node_id)
        ].index:
            self.reverse_edge(edge_id=edge_id)

    def select_basin_area(self, geometry):
        geometry = shapely.force_2d(geometry)
        if isinstance(geometry, MultiPolygon):
            polygons = list(geometry.geoms)
        elif isinstance(geometry, Polygon):
            polygons = [geometry]
        else:
            raise TypeError("geometry cannot be used for selection, is not a (Multi)Polygon")

        mask = self.basin.area.df.geometry.apply(lambda x: any(x.equals(i) for i in polygons))

        if not mask.any():
            raise ValueError("Not any basin area equals input geometry")
        return mask

    def update_basin_area(self, node_id: int, geometry: Polygon | MultiPolygon, basin_area_fid: int | None = None):
        if pd.isna(basin_area_fid):
            mask = self.select_basin_area(geometry)
        else:
            mask = self.basin.area.df.index == basin_area_fid

        self.basin.area.df.loc[mask, ["node_id"]] = node_id

    def add_basin_area(self, geometry: MultiPolygon, node_id: int | None = None, meta_streefpeil: float | None = None):
        # if node_id is None, get an available node_id
        if pd.isna(node_id):
            basin_df = self.basin.node.df[self.basin.node.df.within(geometry)]
            if basin_df.empty:
                raise ValueError("No basin-node within basin area, specify node_id explicitly")
            elif len(basin_df) > 1:
                raise ValueError(
                    f"Multiple basin-nodes within area: {basin_df.index.to_numpy()}. Specify node_id explicitly"
                )
            else:
                node_id = basin_df.index[0]
        elif node_id not in self.basin.node.df.index:
            raise ValueError(f"Node_id {node_id} is not a basin")

        # check geometry and promote to mulitpolygon
        if not geometry.geom_type == "MultiPolygon":
            if geometry.geom_type == "Polygon":
                geometry = MultiPolygon([geometry])
            else:
                raise ValueError(f"geometry-type {geometry.geom_type} is not valid. Provide (Multi)Polygon instead")

        # if all correct, assign
        data = {"node_id": [node_id], "geometry": [geometry]}
        if meta_streefpeil is not None:
            data = {**data, "meta_streefpeil": [meta_streefpeil]}
        area_df = gpd.GeoDataFrame(data, crs=self.crs)
        area_df.index.name = "fid"
        area_df.index += self.basin.area.df.index.max() + 1
        self.basin.area.df = pd.concat([self.basin.area.df, area_df])

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

    def report_basin_area(self):
        gpkg = self.filepath.with_name("basin_node_area_errors.gpkg")
        self.unassigned_basin_area.to_file(gpkg, layer="unassigned_basin_area")

        unassigned_basin_node = self.basin.node.df[~self.basin.node.df.index.isin(self.basin.area.df.node_id)]
        unassigned_basin_node.to_file(gpkg, layer="unassigned_basin_node")

    def report_internal_basins(self):
        gpkg = self.filepath.with_name("internal_basins.gpkg")
        df = self.basin.node.df[~self.basin.node.df.index.isin(self.edge.df.from_node_id)]
        df.to_file(gpkg)

    def find_closest_basin(self, geometry: BaseGeometry, max_distance: float | None):
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

    def split_basin(
        self,
        line: LineString | None = None,
        basin_id: int | None = None,
        geometry: LineString | None = None,
        assign_unique_node: bool = True,
    ):
        if geometry is None:
            if line is None:
                raise ValueError("geometry cannot be None")
            else:
                DeprecationWarning("value `line` in split_basin funtion is deprecated. Use `geometry` in stead.")
                geometry = line

        if self.basin.area.df is None:
            raise ValueError("provide basin / area table first")

        line_centre = geometry.interpolate(0.5, normalized=True)

        # if basin_id is supplied, we select by that first
        if basin_id is not None:
            basin_area_df = self.basin.area.df.loc[self.basin.area.df.node_id == basin_id]
            basin_area_df = basin_area_df[basin_area_df.intersects(geometry)]
            if len(basin_area_df) > 1:
                mask = ~(
                    basin_area_df.contains(geometry.boundary.geoms[0])
                    | basin_area_df.contains(geometry.boundary.geoms[1])
                )
                basin_area_df = basin_area_df[mask]

        else:
            basin_area_df = self.basin.area.df[self.basin.area.df.contains(geometry.interpolate(0.5, normalized=True))]

        if len(basin_area_df) == 0:
            raise ValueError("No basin-areas intersecting cut_line")
        elif len(basin_area_df) > 1:
            raise ValueError("Multiple Overlapping basin-areas intersecting cut_line")

        # get all we need and remove area from basin.area.df
        basin_fid = int(basin_area_df.iloc[0].name)
        basin_geometry = basin_area_df.iloc[0].geometry
        self.basin.area.df = self.basin.area.df[self.basin.area.df.index != basin_fid]

        # get the polygon to cut
        basin_geoms = list(basin_geometry.geoms)
        cut_idx = next(idx for idx, i in enumerate(basin_geoms) if i.contains(line_centre))

        # split it
        right_basin_poly, left_basin_poly = split_basin(basin_geoms[cut_idx], geometry).geoms

        # concat left-over polygons to the right-side
        right_basin_poly = [right_basin_poly]
        left_basin_poly = [left_basin_poly]

        for idx, geom in enumerate(basin_geoms):
            if idx != cut_idx:
                if geom.distance(right_basin_poly[0]) < geom.distance(left_basin_poly[0]):
                    right_basin_poly += [geom]
                else:
                    left_basin_poly += [geom]

        right_basin_poly = MultiPolygon(right_basin_poly)
        left_basin_poly = MultiPolygon(left_basin_poly)

        # add polygons to area
        for poly in [right_basin_poly, left_basin_poly]:
            # by default we assign provided basin_id
            kwargs = {
                "node_id": basin_id,
                "geometry": poly,
            }

            # we override node_id to a unique basin-node within area if that is specified
            if assign_unique_node:
                if self.basin.node.df.geometry.within(poly).any():
                    node_ids = self.basin.node.df[self.basin.node.df.geometry.within(poly)].index.to_list()
                    if node_ids[0] not in self.basin.area.df.node_id.to_numpy():
                        kwargs["node_id"] = node_ids[0]

            self.basin.area.df.loc[self.basin.area.df.index.max() + 1] = kwargs

        if self.basin.area.df.crs is None:
            self.basin.area.df.crs = self.crs

    def redirect_edge(self, edge_id: int, from_node_id: int | None = None, to_node_id: int | None = None):
        if self.edge.df is not None:
            if from_node_id is not None:
                self.edge.df.loc[edge_id, ["from_node_id"]] = from_node_id
            if to_node_id is not None:
                self.edge.df.loc[edge_id, ["to_node_id"]] = to_node_id

        self.reset_edge_geometry(edge_ids=[edge_id])

    def deactivate_node(self, node_id: int):
        node_type = self.get_node_type(node_id)
        df = getattr(self, pascal_to_snake_case(node_type)).static.df
        df.loc[df.node_id == node_id, ["active"]] = False

    def remove_unassigned_basin_area(self):
        df = self.basin.area.df[~self.basin.area.df.index.isin(self.unassigned_basin_area.index)]
        if self.basin.area.df.node_id.duplicated().any():
            df = df.dissolve(by="node_id").reset_index()
            df.index.name = "fid"
        self.basin.area.df = df

    def explode_basin_area(self, remove_z=True):
        df = self.basin.area.df.explode().reset_index(drop=True)
        df.index.name = "fid"
        self.basin.area.df = df

        if remove_z:
            self.basin.area.df.loc[:, "geometry"] = gpd.GeoSeries(
                shapely.force_2d(self.basin.area.df.geometry.array), crs=self.basin.area.df.crs
            )

    def remove_basin_area(self, geometry):
        mask = self.select_basin_area(geometry)
        self.basin.area.df = self.basin.area.df[~mask]

    def merge_basins(
        self,
        basin_id: int | None = None,
        node_id: int | None = None,
        to_node_id: int | None = None,
        to_basin_id: int | None = None,
        are_connected=True,
    ):
        if basin_id is not None:
            warnings.warn("basin_id is deprecated, use node_id instead", DeprecationWarning)
            node_id = basin_id

        if to_basin_id is not None:
            warnings.warn("to_basin_id is deprecated, use to_node_id instead", DeprecationWarning)
            to_node_id = to_basin_id

        if node_id not in self.basin.node.df.index:
            raise ValueError(f"{node_id} is not a basin")
        to_node_type = self.node_table().df.at[to_node_id, "node_type"]
        if to_node_type not in ["Basin", "FlowBoundary", "LevelBoundary"]:
            raise ValueError(
                f'{to_node_id} not of valid type: {to_node_type} not in ["Basin", "FlowBoundary", "LevelBoundary"]'
            )

        if are_connected and (to_node_type != "FlowBoundary"):
            self._graph = None  # set self._graph to None, so it will regenerate on currend edge-table
            paths = [i for i in nx.all_shortest_paths(nx.Graph(self.graph), node_id, to_node_id) if len(i) == 3]

            if len(paths) == 0:
                raise ValueError(f"basin {node_id} not a direct neighbor of basin {to_node_id}")

            # remove flow-node and connected edges
            for path in paths:
                self.remove_node(path[1], remove_edges=True)

        # get a complete edge-list to modify
        edge_ids = self.edge.df[self.edge.df.from_node_id == node_id].index.to_list()
        edge_ids += self.edge.df[self.edge.df.to_node_id == node_id].index.to_list()

        # correct edge from and to attributes
        self.edge.df.loc[self.edge.df.from_node_id == node_id, "from_node_id"] = to_node_id
        self.edge.df.loc[self.edge.df.to_node_id == node_id, "to_node_id"] = to_node_id

        # remove self-connecting edge in case we merge to flow-boundary
        if to_node_type == "FlowBoundary":
            mask = (self.edge.df.from_node_id == to_node_id) & (self.edge.df.to_node_id == to_node_id)
            self.edge.df = self.edge.df[~mask]

        # reset edge geometries
        self.reset_edge_geometry(edge_ids=edge_ids)

        # merge area if basin has any assigned to it
        if to_node_type == "Basin":
            if node_id in self.basin.area.df.node_id.to_numpy():
                poly = self.basin.area.df.set_index("node_id").at[node_id, "geometry"]

                # polygon could be a series op polygons
                if isinstance(poly, pd.Series):
                    poly = poly.union_all()

                # if it is a polygon we convert it to multipolygon
                if isinstance(poly, Polygon):
                    poly = MultiPolygon([poly])

                # if to_node_id has area we union both areas
                if len(self.basin.area.df.loc[self.basin.area.df.node_id == to_node_id]) == 1:
                    poly = poly.union(self.basin.area.df.set_index("node_id").at[to_node_id, "geometry"])

                    self.basin.area.df.loc[self.basin.area.df.node_id == to_node_id, ["geometry"]] = poly

                # else we add a record to basin
                else:
                    self.basin.area.df.loc[self.basin.area.df.index.max() + 1] = {
                        "node_id": to_node_id,
                        "geometry": poly,
                    }

            if self.basin.area.df.crs is None:
                self.basin.area.df.crs = self.crs

        # if node type is flow_boundary, we change type to LevelBoundary
        if to_node_type == "FlowBoundary":
            self.update_node(to_node_id, "LevelBoundary", data=[level_boundary.Static(level=[0.0])])

        # finally we remove the basin
        self.remove_node(node_id)

    def invalid_topology_at_node(self, edge_type: str = "flow") -> gpd.GeoDataFrame:
        df_graph = self.edge.df
        df_node = self.node_table().df
        # Join df_edge with df_node to get to_node_type
        df_graph = df_graph.join(df_node[["node_type"]], on="from_node_id", how="left", rsuffix="_from")
        df_graph = df_graph.rename(columns={"node_type": "from_node_type"})

        df_graph = df_graph.join(df_node[["node_type"]], on="to_node_id", how="left", rsuffix="_to")
        df_graph = df_graph.rename(columns={"node_type": "to_node_type"})
        df_node = self.node_table().df

        """Check if the neighbor amount of the two nodes connected by the given edge meet the minimum requirements."""
        errors = []

        # filter graph by edge type
        df_graph = df_graph.loc[df_graph["edge_type"] == edge_type]

        # count occurrence of "from_node" which reflects the number of outneighbors
        from_node_count = (
            df_graph.groupby("from_node_id").size().reset_index(name="from_node_count")  # type: ignore
        )

        # append from_node_count column to from_node_id and from_node_type
        from_node_info = (
            df_graph[["from_node_id", "from_node_type"]]
            .drop_duplicates()
            .merge(from_node_count, on="from_node_id", how="left")
        )
        from_node_info = from_node_info[["from_node_id", "from_node_count", "from_node_type"]]

        # add the node that is not the upstream of any other nodes
        from_node_info = self._add_source_sink_node(df_node["node_type"], from_node_info, "from")

        # loop over all the "from_node" and check if they have enough outneighbor
        for _, row in from_node_info.iterrows():
            # from node's outneighbor
            if row["from_node_count"] < edge_amount[row["from_node_type"]][2]:
                node_id = row["from_node_id"]
                errors += [
                    {
                        "geometry": df_node.at[node_id, "geometry"],
                        "node_id": node_id,
                        "node_type": df_node.at[node_id, "node_type"],
                        "exception": f"must have at least {edge_amount[row['from_node_type']][2]} outneighbor(s) (got {row['from_node_count']})",
                    }
                ]

        # count occurrence of "to_node" which reflects the number of inneighbors
        to_node_count = (
            df_graph.groupby("to_node_id").size().reset_index(name="to_node_count")  # type: ignore
        )

        # append to_node_count column to result
        to_node_info = (
            df_graph[["to_node_id", "to_node_type"]].drop_duplicates().merge(to_node_count, on="to_node_id", how="left")
        )
        to_node_info = to_node_info[["to_node_id", "to_node_count", "to_node_type"]]

        # add the node that is not the downstream of any other nodes
        to_node_info = self._add_source_sink_node(df_node["node_type"], to_node_info, "to")

        # loop over all the "to_node" and check if they have enough inneighbor
        for _, row in to_node_info.iterrows():
            if row["to_node_count"] < edge_amount[row["to_node_type"]][0]:
                node_id = row["to_node_id"]
                errors += [
                    {
                        "geometry": df_node.at[node_id, "geometry"],
                        "node_id": node_id,
                        "node_type": df_node.at[node_id, "node_type"],
                        "exception": f"must have at least {edge_amount[row['to_node_type']][0]} inneighbor(s) (got {row['to_node_count']})",
                    }
                ]

        if len(errors) > 0:
            return gpd.GeoDataFrame(errors, crs=self.crs).set_index("node_id")
        else:
            return gpd.GeoDataFrame(
                [], columns=["node_id", "node_type", "exception"], geometry=gpd.GeoSeries(crs=self.crs)
            ).set_index("node_id")

    def validate_link_source_destination(self):
        """Check if links exist with reversed source-destination"""
        # remove function when this is available: https://github.com/Deltares/Ribasim/issues/2140
        df = self.link.df

        # on tuples we can easily check duplicates irrespective of order
        duplicated_links = pd.Series(
            list(
                zip(np.minimum(df["from_node_id"], df["to_node_id"]), np.maximum(df["from_node_id"], df["to_node_id"]))
            ),
            index=df.index,
        ).duplicated(keep=False)

        # if links are duplicated in reversed source-destination we raise an Exception
        if duplicated_links.any():
            raise ValueError(
                f"Links found with reversed source-destination: {list(df[duplicated_links].reset_index()[['link_id', 'from_node_id', 'to_node_id']].to_dict(orient='index').values())}"
            )

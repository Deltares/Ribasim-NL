from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from networkx import DiGraph, simple_cycles
from ribasim import Model, Node
from ribasim.geometry.link import NodeData
from ribasim.nodes import flow_demand, level_demand
from ribasim_nl.case_conversions import pascal_to_snake_case
from shapely.geometry import MultiPolygon, Point, Polygon

from ribasim_nl import CloudStorage
from ribasim_nl import Model as ModelNL


class Flushing:
    def __init__(
        self,
        model: ModelNL | Model | Path | str,
        lhm_flushing_path: Path | str = "Basisgegevens/LHM/lsw_flushing.gpkg",
        flushing_layer: str = "lsw_flushing_lhm43",
        flushing_id: str = "LSWNR",
        flushing_col: str = "doorsp_mmj",
        flushing_geom_offset: float = 50.0,
        significant_overlap: float = 0.5,
        convert_to_m3s: float = 1 / (1000 * 365 * 24 * 3600),
        dissolve_by_val: bool = True,
        debug_output: bool = False,
    ):
        """Initialize the Flushing class for adding flushing information to a Ribasim model.

        Parameters
        ----------
        model : ModelNL | Model | Path | str
            The Ribasim model to add flushing to, or a path to the model
        lhm_flushing_path : Path | str, optional
            Path to the flushing geopackage, by default "Basisgegevens/LHM/lsw_flushing.gpkg"
        flushing_layer : str, optional
            Name of the layer in the geopackage, by default "lsw_flushing_lhm43"
        flushing_id : str, optional
            Column name with the flushing IDs, by default "LSWNR"
        flushing_col : str, optional
            Column name with the flushing values, by default "doorsp_mmj"
        flushing_geom_offset: float, optional
            Horizontal offset used for placing the FlowDemand node (relative to the basin)
        significant_overlap : float, optional
            Threshold for considering area overlap significant, by default 0.5
        convert_to_m3s : float, optional
            Conversion factor to convert flushing_col to m3/s, by default 1 / (1000 * 365 * 24 * 3600)
        dissolve_by_val: bool, optional
            Dissolve geospatially by the integer value of 'flushing_col', by default True
        debug_output: bool, optional
            Print debug node - basin choices, by default True
        """
        self.cloud = CloudStorage()
        self.model = model
        self.lhm_flushing_path = self.cloud.joinpath(lhm_flushing_path)
        self.flushing_layer = flushing_layer
        self.flushing_id = flushing_id
        self.flushing_col = flushing_col
        self.flushing_geom_offset = flushing_geom_offset
        self.significant_overlap = significant_overlap
        self.convert_to_m3s = convert_to_m3s
        self.dissolve_by_val = dissolve_by_val
        self.debug_output = debug_output

    def add_flushing(
        self,
    ) -> ModelNL | Model:
        """Add flushing information to the Ribasim model.

        Returns
        -------
        ModelNL | Model
            The updated Ribasim model with flushing information added
        """
        # Synchronize flushing data and model files
        model, df_flushing = self._sync_files()

        # Reduce flushing data to non-null and matching data
        df_flushing_subset = df_flushing[~pd.isna(df_flushing[self.flushing_col])].copy()
        df_flushing_subset = df_flushing_subset[[self.flushing_id, self.flushing_col, "geometry"]].copy()

        if self.dissolve_by_val:
            df_flushing_subset = self._dissolve_flushing_data(df_flushing_subset)

        # Get handles to relevant tables
        all_nodes = model.node_table().df[["node_type", "geometry"]].copy()
        df_outlet_static = model.outlet.static.df.set_index("node_id").copy()
        df_pump_static = model.pump.static.df.set_index("node_id").copy()

        # Get an extended basin table with no 'bergend' basins
        df_basin = model.basin.node.df[["meta_categorie"]].join(
            model.basin.area.df.set_index("node_id")[["meta_aanvoer", "geometry"]]
        )
        df_basin = df_basin[df_basin.meta_categorie != "bergend"]
        df_basin = gpd.GeoDataFrame(df_basin, crs=model.basin.area.df.crs)

        # Reset the internal graph to ensure we have the most up-to-date version
        model.reset_graph

        # Find matching basins for each flushing geometry
        for flushing_row in df_flushing_subset.itertuples():
            flush_id = getattr(flushing_row, self.flushing_id)
            flush_val = getattr(flushing_row, self.flushing_col)

            basin_matches = df_basin.iloc[df_basin.sindex.query(flushing_row.geometry, predicate="intersects")].copy()
            basin_matches["area_match"] = basin_matches.intersection(flushing_row.geometry).area
            basin_matches["rel_area_match"] = basin_matches.area_match / basin_matches.area
            basin_matches["rel_area_flush"] = basin_matches.area_match / flushing_row.geometry.area
            basin_matches = basin_matches[basin_matches.rel_area_match >= self.significant_overlap]
            basin_matches = basin_matches.reset_index(drop=False)

            if len(basin_matches) == 0:
                # No (sufficiently) matching basins, continue
                continue

            # Find all downstream paths from the matching basins, contained by
            # the contour of the basins geometry and flushing geometry
            downstream_paths = []
            geom = shapely.union_all([flushing_row.geometry.buffer(0.1), *basin_matches.geometry.buffer(0.1).tolist()])
            for match in basin_matches.itertuples():
                # Find the downstream path(s) for this basin
                downstream_paths += self._all_downstream_paths(model.graph, match.node_id, all_nodes, limit_geom=geom)

            # Make a DataFrame of the allowed nodes (node type) present in the
            # paths found. For downstream nodes, we want all pumps that are
            # designated as 'afvoer'.
            dfd = self._find_downstream_nodes(model, downstream_paths, all_nodes, df_outlet_static, df_pump_static)
            dfd = dfd[dfd.afvoer].copy()

            # No allowed downstream nodes found at all, log warning and continue
            if len(dfd) == 0:
                basin_nids = basin_matches.node_id.tolist()
                print(f"WARNING: Polygon {flush_id=} with basin node_id's={basin_nids} has no valid upstream nodes")
                continue

            # Determine the contribution of each matching basin
            basins_cov = dfd.basin.unique().tolist()
            df_flush = basin_matches[basin_matches.node_id.isin(basins_cov)].copy()
            df_flush["rel_contrib"] = df_flush.area_match / df_flush.area_match.sum()
            df_flush = df_flush.set_index("node_id")

            # Check if all basins are connected
            basins_mis = basin_matches.node_id[~basin_matches.node_id.isin(basins_cov)].tolist()
            if len(basins_mis) > 0:
                max_cover = basin_matches.rel_area_flush.sum() * 100
                current_cover = df_flush.rel_area_flush.sum() * 100
                print(
                    f"WARNING: Polygon {flush_id=} missing upstream nodes for basins: {basins_mis}. Covered basins: {basins_cov}, {current_cover=:.1f}%, {max_cover=:.1f}%"
                )

            if self.debug_output:
                debug_str = []
                for nid, group in dfd.groupby("node_id"):
                    debug_str.append(f"node {nid} connects basins {group.basin.tolist()}")
                print(f"Polygon {flush_id=}, {', '.join(debug_str)}")

            # Determine flow demand. Each basin can have one or more multiple
            # downstream nodes. In case of multiple downstream nodes, divide
            # the demand of the basin evenly over all connected nodes.
            dfd["flow_demand"] = 0.0
            for basin_nid, group in dfd.groupby("basin"):
                # Divide by the number of unique paths in the group, not the
                # unique nodes: nodes in series should get the same demand.
                demand = df_flush.at[basin_nid, "rel_contrib"] * flushing_row.geometry.area * flush_val
                demand = demand * self.convert_to_m3s
                dfd.loc[group.index, "flow_demand"] += demand / len(group.path_id.unique())

                # add level demands to each basin to indicate streefpeil
                demand = float(model.basin.area.df[model.basin.area.df.node_id == basin_nid].meta_streefpeil.iat[0])
                model = self.add_level_demand(model, model.basin[basin_nid], demand)

            # Add flow demand as ribasim nodes to the selected nodes. In case
            # multiple basins are connected to the same node, sum individual
            # demands.
            for (target_nid, target_type), group in dfd.groupby(["node_id", "node_type"]):
                # Determine the connected basins and the flushing demand
                group_basins = group.basin.unique().tolist()
                demand = group.flow_demand.sum()

                # Dont add a flow demand if the demand is zero
                if demand == 0:
                    continue

                # Select the target node
                subpart = getattr(model, pascal_to_snake_case(target_type))
                target_node = subpart[target_nid]

                # Create and link the flow_demand
                metadata = {
                    f"meta_{self.flushing_id}": flush_id,
                    "meta_basin_nid": ",".join(map(str, group_basins)),
                }
                model = self.add_flushing_demand(model, target_node, demand, metadata=metadata)

                # Release min_upstream_level
                if target_type == "Pump":
                    subpart.static.df.loc[subpart.static.df.node_id == target_nid, "min_upstream_level"] = pd.NA

        return model

    def add_flushing_demand(
        self,
        model: ModelNL | Model,
        target_node: NodeData,
        demand: float,
        metadata: dict[str, str] | None,
    ):
        """Add a flushing demand node to the model and connect it to a target node.

        Parameters
        ----------
        model : ModelNL | Model
            The model to add the flushing demand to
        target_node : NodeData
            The node to connect the flushing demand to
        demand : float
            The constant demand value to apply at all timesteps
        metadata : dict[str, str]
            meta data as column - value, defaults to None

        Returns
        -------
        None

        """
        df_static = getattr(getattr(model, pascal_to_snake_case(target_node.node_type)), "static").df
        max_flow_rate = df_static.set_index("node_id").loc[target_node.node_id, ["flow_rate", "max_flow_rate"]].max()
        if max_flow_rate < demand:
            print(f"WARNING: {target_node} has {max_flow_rate=:.2e}m3/s, setting a greater {demand=:.2e}m3/s")

        if metadata is None:
            metadata = {}

        uniq_times = model.basin.time.df.time.unique()
        new_flow_demand = model.flow_demand.add(
            Node(
                model.node_table().df.index.max() + 1,
                Point(
                    target_node.geometry.x + self.flushing_geom_offset,
                    target_node.geometry.y,
                ),
                **metadata,
            ),
            [
                # @TODO hardcoded demand_priority=1 for now
                flow_demand.Time(
                    time=uniq_times,
                    demand_priority=1,
                    demand=len(uniq_times) * [demand],
                ),
            ],
        )
        model.link.add(new_flow_demand, target_node)

        return model

    def add_level_demand(
        self,
        model: ModelNL | Model,
        target_node: NodeData,
        demand: float,
    ):
        """Add a level demand node to the model and connect it to a target node.

        Parameters
        ----------
        model : ModelNL | Model
            The model to add the level demand to
        target_node : NodeData
            The node to connect the level demand to
        demand : float
            The constant demand value to apply at all timesteps

        Returns
        -------
        None

        """
        uniq_times = model.basin.time.df.time.unique()
        new_level_demand = model.level_demand.add(
            Node(
                model.node_table().df.index.max() + 1,
                Point(
                    target_node.geometry.x + self.flushing_geom_offset,
                    target_node.geometry.y,
                ),
            ),
            [
                # @TODO hardcoded demand_priority=2 for now
                level_demand.Time(
                    time=uniq_times,
                    demand_priority=2,
                    min_level=len(uniq_times) * [demand],
                    max_level=len(uniq_times) * [demand],
                ),
            ],
        )
        model.link.add(new_level_demand, target_node)

        return model

    def _find_downstream_nodes(
        self,
        model: ModelNL | Model,
        paths: list[list[int]],
        all_nodes: gpd.GeoDataFrame,
        df_outlet_static: pd.DataFrame,
        df_pump_static: pd.DataFrame,
    ) -> pd.DataFrame:
        """Find upstream nodes that can be used for flushing.

        Parameters
        ----------
        paths : list[list[int]]
            List of paths containing node IDs
        all_nodes : gpd.GeoDataFrame
            GeoDataFrame containing all nodes and their types
        df_outlet_static : pd.DataFrame
            DataFrame with static outlet information
        df_pump_static : pd.DataFrame
            DataFrame with static pump information

        Returns
        -------
        pd.DataFrame
            DataFrame containing upstream nodes and their properties
        """
        # Find unique nodes over all paths
        uniq_nodes = np.unique(np.concatenate(paths))

        # Ignore nodes in cycles
        cycles = list(simple_cycles(model.graph.subgraph(uniq_nodes)))
        if len(cycles) == 0:
            ignore_nodes = np.array([])
        else:
            ignore_nodes = np.unique(np.concatenate(cycles))
        uniq_nodes = uniq_nodes[~np.isin(uniq_nodes, ignore_nodes)]

        # Some areas have a lot of paths with duplicate nodes. Improve
        # performance by caching the response for unique nodes
        df_control_links = model.link.df[model.link.df.link_type == "control"]
        df_control_links = df_control_links.set_index("to_node_id")
        node_lookup = {}
        for nid in uniq_nodes:
            nid_type = all_nodes.node_type.at[nid]
            if nid_type in ["Pump"]:  # ignore outlet for now
                if nid in df_control_links.index:
                    # This node has incoming control links, check if a
                    # FlowDemand node is present already
                    incoming_nids = df_control_links.loc[[nid], "from_node_id"].tolist()
                    incoming_types = all_nodes.loc[incoming_nids, "node_type"].tolist()
                    allowed = "FlowDemand" not in incoming_types
                else:
                    # No incoming control link, so this node is allowed
                    allowed = True

                # Skip nodes that are not allowed
                if not allowed:
                    continue

                # Only pumps for now
                if all_nodes.node_type.at[nid] == "Pump":
                    bool_afvoer = bool(df_pump_static.at[nid, "meta_func_afvoer"])
                    bool_afvoer = bool_afvoer or bool(df_pump_static.at[nid, "meta_func_circulair"])
                node_lookup[nid] = (nid_type, bool_afvoer)

        dfd = {
            "basin": [],
            "path_id": [],
            "downstream_index": [],
            "node_id": [],
            "node_type": [],
            "afvoer": [],
        }
        is_added = {}
        for pid, path in enumerate(paths):
            basin_nid = path[0]

            for i, nid in enumerate(path):
                # We don't need duplicate upstream node_id's originating from
                # the same basin or non-applicable nodes
                if nid not in node_lookup or (basin_nid, nid) in is_added:
                    continue

                nid_type, bool_afvoer = node_lookup[nid]
                dfd["basin"].append(basin_nid)
                dfd["path_id"].append(pid)
                dfd["downstream_index"].append(i)
                dfd["node_id"].append(nid)
                dfd["node_type"].append(nid_type)
                dfd["afvoer"].append(bool_afvoer)
                is_added[(basin_nid, nid)] = True
        dfd = pd.DataFrame(dfd)

        return dfd

    def _all_downstream_paths(
        self,
        graph: DiGraph,
        start_node: int,
        all_nodes: gpd.GeoDataFrame,
        limit_geom: MultiPolygon | Polygon | None = None,
    ) -> list[list[int]]:
        """Find all downstream paths from a starting node.

        Parameters
        ----------
        graph : DiGraph
            NetworkX directed graph representing the network
        start_node : int
            Node ID to start the search from
        all_nodes : gpd.GeoDataFrame
            GeoDataFrame containing all nodes and their geometries
        limit_geom : MultiPolygon | Polygon | None, optional
            Geometry to limit the search area, by default None

        Returns
        -------
        list[list[int]]
            List of paths, where each path is a list of node IDs
        """
        # Initialize the return variable
        end_paths = []

        # Precompute nodes that intersect with limit_geom
        if limit_geom is not None:
            valid_indices = all_nodes.sindex.query(limit_geom, predicate="intersects")
            valid_nodes = set(all_nodes.index[valid_indices])
        else:
            valid_nodes = set(all_nodes.index)

        # Pre-build adjacency sets for faster lookups during DFS
        # Performs a set intersection operation with valid_nodes
        successors = {node: set(graph.successors(node)) & valid_nodes for node in valid_nodes}

        # Recursively fill the paths with a depth-first search
        self._dfs(graph, [start_node], end_paths, valid_nodes, successors)

        return end_paths

    def _dfs(
        self,
        graph: DiGraph,
        path: list[int],
        end_paths: list[list[int]],
        valid_nodes: set[int],
        successors: dict[int, set[int]],
    ):
        """Perform depth-first search to find upstream paths.

        Parameters
        ----------
        graph : DiGraph
            NetworkX directed graph representing the network
        path : list[int]
            Current path being explored
        end_paths : list[list[int]]
            List to store complete paths
        valid_nodes : set[int]
            Set of node IDs that are valid for the search
        successors : dict[int, set[int]]
            Pre-computed adjacency sets for faster lookups
        """
        last_node = path[-1]

        # Get successors that are not already in the path and are in valid_nodes
        unvisited_predecessors = successors[last_node] - set(path)

        # Stop looking in case of a dead end or a cycle
        if not unvisited_predecessors:
            end_paths.append(path)
            return

        # Recurse in successors
        for successor in sorted(unvisited_predecessors):
            self._dfs(graph, [*path, successor], end_paths, valid_nodes, successors)

    def _dissolve_flushing_data(self, df_flushing: pd.DataFrame) -> pd.DataFrame:
        # Round flushing_col to nearest integer value
        df = df_flushing.copy()
        df[self.flushing_col] = df[self.flushing_col].round().astype(int)

        # First step: dissolve by flushing_col and id if these are equal
        df = df.dissolve(by=[self.flushing_id, self.flushing_col])
        df = df.reset_index()

        # Second step: iteratively merge overlapping geometries with the
        # same flushing_col value
        new_rows = []
        for _, group in df.groupby(self.flushing_col):
            group["geometry"] = group.buffer(0.1)
            visited = []
            for cur_idx, row in group.iterrows():
                if cur_idx in visited:
                    continue
                subgroup = group[~group.index.isin(visited)].copy()
                idxs, midxs = [], [cur_idx]
                while len(midxs) > len(idxs):
                    idxs = midxs
                    midxs = subgroup.sindex.query(subgroup.loc[idxs].union_all(), predicate="intersects")
                    midxs = subgroup.index[midxs].tolist()
                if len(idxs) > 0:
                    new_row = row.copy()
                    new_row[self.flushing_id] = ",".join(map(str, subgroup.loc[idxs, self.flushing_id].tolist()))
                    new_row["geometry"] = subgroup.loc[idxs, "geometry"].union_all()
                    new_rows.append(new_row)
                    visited += idxs
        df = pd.concat(new_rows, axis=1).T

        return df

    def _sync_files(
        self,
    ) -> tuple[ModelNL | Model, gpd.GeoDataFrame]:
        """Synchronize and load required files.

        Returns
        -------
        tuple[ModelNL | Model, gpd.GeoDataFrame]
            Tuple containing:
            - The loaded Ribasim model
            - GeoDataFrame with flushing data
        """
        is_model = isinstance(self.model, ModelNL) or isinstance(self.model, Model)

        # Synchronize flushing data and model files
        filepaths = [self.lhm_flushing_path]
        if not is_model:
            filepaths.append(Path(self.model))
        self.cloud.synchronize(filepaths=filepaths)

        # Read the ribasim model
        model = self.model
        if not is_model:
            model = ModelNL.read(self.model)

        # Open the flushing data
        df_flushing = gpd.read_file(self.lhm_flushing_path, layer=self.flushing_layer)

        return model, df_flushing

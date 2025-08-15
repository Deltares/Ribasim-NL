from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from networkx import DiGraph
from ribasim import Model, Node
from ribasim.geometry.link import NodeData
from ribasim.nodes import flow_demand
from shapely.geometry import MultiPolygon, Point, Polygon

from ribasim_nl import CloudStorage
from ribasim_nl import Model as ModelNL
from ribasim_nl.case_conversions import pascal_to_snake_case


class Flushing:
    def __init__(
        self,
        model: ModelNL | Model | Path | str,
        lhm_flushing_path: Path | str = "Basisgegevens/LHM/lsw_flushing.gpkg",
        flushing_layer: str = "lsw_flushing_lhm43",
        flushing_id: str = "LSWNR",
        flushing_col: str = "doorsp_mmj",
        significant_overlap: float = 0.5,
        convert_to_m3s: float = 1 / (1000 * 365 * 24 * 3600),
        dissolve_by_val: bool = True,
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
        significant_overlap : float, optional
            Threshold for considering area overlap significant, by default 0.5
        convert_to_m3s : float, optional
            Conversion factor to convert flushing_col to m3/s, by default 1 / (1000 * 365 * 24 * 3600)
        dissolve_by_val: bool, optional
            Dissolve geospatially by the integer value of 'flushing_col', by default True
        """
        self.cloud = CloudStorage()
        self.model = model
        self.lhm_flushing_path = self.cloud.joinpath(lhm_flushing_path)
        self.flushing_layer = flushing_layer
        self.flushing_id = flushing_id
        self.flushing_col = flushing_col
        self.significant_overlap = significant_overlap
        self.convert_to_m3s = convert_to_m3s
        self.dissolve_by_val = dissolve_by_val

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
            basin_matches = basin_matches[basin_matches.rel_area_match >= self.significant_overlap]
            basin_matches = basin_matches.reset_index(drop=False)

            if len(basin_matches) == 0:
                # No (sufficiently) matching basins, continue
                continue

            # Find all upstream paths from the matching basins, contained by
            # the contour of the basin geometry and flushing geometry
            upstream_paths = []
            for match in basin_matches.itertuples():
                # Find the upstream path(s) for this basin
                geom = shapely.union_all([flushing_row.geometry.buffer(10), match.geometry.buffer(10)])
                upstream_paths += self._all_upstream_paths(model.graph, match.node_id, all_nodes, limit_geom=geom)

            # Make a DataFrame of the allowed nodes (node type) present in the
            # paths found.
            dfu = self._find_upstream_nodes(model, upstream_paths, all_nodes, df_outlet_static, df_pump_static)

            # No allowed upstream nodes found at all, log warning and continue
            if len(dfu) == 0:
                basin_nids = basin_matches.node_id.tolist()
                print(f"WARNING: Polygon {flush_id=} with basin node_id's={basin_nids} has no valid upstream nodes")
                continue

            # Select the least amount of required flushing upstream nodes
            dfu = self._find_upstream_candidates(dfu)

            # The result should contain at least one node
            if not dfu.optimal_choice.any():
                print(f"WARNING: Polygon {flush_id=} has upstream nodes but no optimal choice")
                continue

            # Determine the contribution of each matching basin
            basin_flush = basin_matches[basin_matches.node_id.isin(dfu[dfu.optimal_choice].basin.tolist())].copy()
            basin_flush["rel_contrib"] = basin_flush.area_match / basin_flush.area_match.sum()

            for (target_nid, target_type), group in dfu[dfu.optimal_choice].groupby(["node_id", "node_type"]):
                # Determine the flushing value and convert to m3/s
                contrib = basin_flush[basin_flush.node_id.isin(group.basin.tolist())].rel_contrib.sum()
                demand = contrib * flushing_row.geometry.area * flush_val
                demand = demand * self.convert_to_m3s

                # Select the target node
                target_node = getattr(model, pascal_to_snake_case(target_type))[target_nid]

                # Create and link the flow_demand
                model = self.add_flushing_demand(model, target_node, demand)

        return model

    def add_flushing_demand(
        self,
        model: ModelNL | Model,
        target_node: NodeData,
        demand: float,
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

        Returns
        -------
        None

        """
        df_static = getattr(getattr(model, pascal_to_snake_case(target_node.node_type)), "static").df
        max_flow_rate = df_static.set_index("node_id").loc[target_node.node_id, ["flow_rate", "max_flow_rate"]].max()
        if max_flow_rate < demand:
            print(f"WARNING: {target_node} has {max_flow_rate=}m3/s, setting a greater {demand=:.2e}m3/s")

        uniq_times = model.basin.time.df.time.unique()
        new_flow_demand = model.flow_demand.add(
            Node(
                model.node_table().df.index.max() + 1,
                Point(
                    target_node.geometry.x + 5,
                    target_node.geometry.y,
                ),
            ),
            [
                # Do not implement a demand_priority yet
                flow_demand.Time(
                    time=uniq_times,
                    demand=len(uniq_times) * [demand],
                ),
            ],
        )
        model.link.add(new_flow_demand, target_node)

        return model

    def _find_upstream_nodes(
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
        df_control_links = model.link.df[model.link.df.link_type == "control"]
        df_control_links = df_control_links.set_index("to_node_id")

        dfu = {
            "basin": [],
            "path_id": [],
            "upstream_index": [],
            "node_id": [],
            "node_type": [],
            "aanvoer": [],
        }
        for pid, path in enumerate(paths):
            for i, nid in enumerate(path):
                nid_type = all_nodes.node_type.at[nid]
                if nid_type in ["Outlet", "Pump"] and nid not in df_control_links.index:
                    if all_nodes.node_type.at[nid] == "Outlet":
                        bool_aanvoer = bool(df_outlet_static.at[nid, "meta_aanvoer"])
                    elif all_nodes.node_type.at[nid] == "Pump":
                        bool_aanvoer = bool(df_pump_static.at[nid, "meta_func_aanvoer"])
                    else:
                        raise TypeError(nid_type)
                    dfu["basin"].append(path[0])
                    dfu["path_id"].append(pid)
                    dfu["upstream_index"].append(i)
                    dfu["node_id"].append(nid)
                    dfu["node_type"].append(nid_type)
                    dfu["aanvoer"].append(bool_aanvoer)

        # Drop duplicate upstream node_id's originating from the same basin
        dfu = pd.DataFrame(dfu).drop_duplicates(subset=["basin", "node_id"])

        return dfu

    def _find_upstream_candidates(self, dfu: pd.DataFrame) -> pd.DataFrame:
        """Find the best upstream candidates for flushing.

        Parameters
        ----------
        dfu : pd.DataFrame
            DataFrame with upstream nodes and their properties

        Returns
        -------
        pd.DataFrame
            DataFrame with upstream nodes and their properties
        """
        dfu["optimal_choice"] = False
        all_basins = set(dfu.basin.tolist())
        covered_basins = set()
        best_sources = []
        for df in [dfu[dfu.aanvoer], dfu]:
            # Find a minimal coverage set
            coversets = self._exact_cover_minimum(df)
            if len(coversets) == 1 and len(coversets[0]) == 0:
                # No optimal set
                continue
            elif len(coversets) == 1:
                sources = coversets[0]
            elif len(coversets) > 1:
                # Multiple similar choices, chose the most upstream one
                # based on the sum of upstream indices
                weights = []
                for coverset in coversets:
                    weights.append(df[df.node_id.isin(coverset)].upstream_index.sum())
                sources = coversets[np.argmax(weights)]

            # Update the covered basins if this iteration provides better coverage
            basins = set(df[df.node_id.isin(sources)].basin.tolist())
            if len(basins) > len(covered_basins):
                covered_basins = basins.copy()
                best_sources = sources.copy()

            # Stop in case the coverage is complete
            if covered_basins == all_basins:
                break

        # Update the optimal choice
        if len(best_sources) > 0:
            dfu.loc[dfu.node_id.isin(best_sources), "optimal_choice"] = True

        return dfu

    def _exact_cover_minimum(self, df: pd.DataFrame) -> list[list[int]]:
        start_to_ends = {}
        for from_nid, group in df.groupby("node_id"):
            start_to_ends[from_nid] = set(group.basin.tolist())

        ends = sorted({e for es in start_to_ends.values() for e in es})
        end_to_starts = {e: {s for s, es in start_to_ends.items() if e in es} for e in ends}
        best_solution = [None, []]  # [best_length, list_of_solutions]
        best_partial = [0, []]  # [max_exact_once_count, partial_solution]

        self._search_exact_cover(start_to_ends, end_to_starts, set(ends), [], best_solution, best_partial)

        if best_solution[1]:
            return sorted(best_solution[1], key=lambda x: tuple(x))
        else:
            # No perfect cover → return best partial
            return [best_partial[1]] if best_partial[1] else []

    def _search_exact_cover(
        self,
        start_to_ends,
        end_to_starts,
        remaining_ends,
        partial_solution,
        best_solution,
        best_partial,
    ):
        # Track partial coverage
        covered_now = set()
        for s in partial_solution:
            covered_now |= start_to_ends[s]
        covered_exactly_once = [e for e in covered_now if sum(e in start_to_ends[s] for s in partial_solution) == 1]
        if len(covered_exactly_once) > best_partial[0]:
            best_partial[0] = len(covered_exactly_once)
            best_partial[1] = sorted(partial_solution)

        # Prune if longer than the best length found so far (for perfect covers)
        if best_solution[0] is not None and len(partial_solution) > best_solution[0]:
            return

        # If no endpoints remain → perfect cover
        if not remaining_ends:
            if best_solution[0] is None or len(partial_solution) < best_solution[0]:
                best_solution[0] = len(partial_solution)
                best_solution[1].clear()
                best_solution[1].append(sorted(partial_solution))
            elif len(partial_solution) == best_solution[0]:
                best_solution[1].append(sorted(partial_solution))
            return

        # Choose endpoint with fewest available starts
        e = min(remaining_ends, key=lambda ep: len(end_to_starts[ep]))
        for s in sorted(end_to_starts[e]):  # sorted for deterministic order
            # Explicit overlap check
            overlap_found = False
            for sel in partial_solution:
                if start_to_ends[s] & start_to_ends[sel]:
                    overlap_found = True
                    break
            if overlap_found:
                continue

            new_remaining = remaining_ends - start_to_ends[s]
            self._search_exact_cover(
                start_to_ends, end_to_starts, new_remaining, partial_solution + [s], best_solution, best_partial
            )

    def _all_upstream_paths(
        self,
        graph: DiGraph,
        start_node: int,
        all_nodes: gpd.GeoDataFrame,
        limit_geom: MultiPolygon | Polygon | None = None,
    ) -> list[list[int]]:
        """Find all upstream paths from a starting node.

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

        # Recursively fill the paths with a depth-first search
        self._dfs(graph, [start_node], end_paths, all_nodes, limit_geom)

        return end_paths

    def _dfs(
        self,
        graph: DiGraph,
        path: list[int],
        end_paths: list[list[int]],
        all_nodes: gpd.GeoDataFrame,
        limit_geom: MultiPolygon | Polygon | None,
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
        all_nodes : gpd.GeoDataFrame
            GeoDataFrame containing all nodes and their geometries
        limit_geom : MultiPolygon | Polygon | None
            Geometry to limit the search area
        """
        last_node = path[-1]
        # Get predecessors that are not already in the path
        unvisited_predecessors = []
        for n in graph.predecessors(last_node):
            if n not in path and (limit_geom is None or all_nodes.geometry.at[n].intersects(limit_geom)):
                unvisited_predecessors.append(n)

        # Stop looking in case of a dead end or a cycle
        if not unvisited_predecessors:
            end_paths.append(path)
            return

        # Recurse in predecessors
        for predecessor in unvisited_predecessors:
            self._dfs(graph, path + [predecessor], end_paths, all_nodes, limit_geom)

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

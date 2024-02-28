import pathlib
from typing import Dict, List, Optional, Tuple, Union

import geopandas as gpd
import networkx
import numpy as np
import pandas as pd


class AggregateCrossings:
    def __init__(
        self,
        agg_pg_path: Union[pathlib.Path, str],
        crossings_path: Union[pathlib.Path, str],
        crossings_layer: str,
        agg_pg_layer: Optional[str] = None,
        agg_pg_col: str = "aggr_class",
        filter_bool_col: str = "agg1_used",
        joker_area: float = 6.25e4,
        debug: bool = False,
    ) -> None:
        self.agg_peilgebieden = gpd.read_file(agg_pg_path, layer=agg_pg_layer)
        self.agg_peilgebieden = self.agg_peilgebieden.set_index("globalid")

        self.peilgebieden = gpd.read_file(crossings_path, layer="peilgebied")
        self.peilgebieden = self.peilgebieden.set_index("globalid")

        self.dfc = gpd.read_file(crossings_path, layer=crossings_layer)
        self.dfc = self.dfc[self.dfc[filter_bool_col]].copy()

        self.agg_col = agg_pg_col
        self.joker_area = joker_area
        self.debug = debug

    def aggregate_crossings(
        self,
    ) -> Tuple[gpd.GeoDataFrame, List, networkx.DiGraph, Dict[str, Tuple[float, float]]]:
        G, pos = self.make_graph()
        nodegroups, cat_per_node = self.find_nodegroups(G)
        nodegroups_merged = self.merge_nodegroups(nodegroups, cat_per_node)

        # Geef de peilgebieden in een groep een uniek groepid.
        peilgebieden = self.agg_peilgebieden.copy()
        peilgebieden["agg2"] = pd.NA
        for i, nodegroup in enumerate(nodegroups_merged):
            for node in nodegroup:
                if node.startswith("None_"):
                    continue

                if not pd.isnull(peilgebieden.at[node, "agg2"]):
                    print(f"ERROR: duplicaat peilgebied {node}")
                    break

                peilgebieden.at[node, "agg2"] = i

        # Geef peilgebieden die niet in een groep zijn ook een (uniek) groepid.
        i = len(nodegroups_merged)
        for row in peilgebieden.itertuples():
            if pd.isnull(row.agg2):
                peilgebieden.at[row.Index, "agg2"] = i
                i += 1

        return peilgebieden, nodegroups_merged, G, pos

    def make_graph(self) -> Tuple[Dict[str, Tuple[float, float]], networkx.DiGraph]:
        G = networkx.DiGraph()
        nodes = np.unique(
            np.hstack(
                [
                    self.dfc.peilgebied_from.dropna().values,
                    self.dfc.peilgebied_to.dropna().values,
                ]
            )
        )
        df_edges = self.dfc[["peilgebied_from", "peilgebied_to", "geometry"]].copy()
        edges = []
        pos = {}
        for i, row in enumerate(df_edges.itertuples()):
            if pd.isnull(row.peilgebied_from):
                nodename = f"None_{i}"
                df_edges.at[row.Index, "peilgebied_from"] = nodename
                pos[nodename] = (row.geometry.x, row.geometry.y)
            else:
                geom = self.peilgebieden.geometry.at[row.peilgebied_from].centroid
                pos[row.peilgebied_from] = (geom.x, geom.y)

            if pd.isnull(row.peilgebied_to):
                nodename = f"None_{i}"
                df_edges.at[row.Index, "peilgebied_to"] = nodename
                pos[nodename] = (row.geometry.x, row.geometry.y)
            else:
                geom = self.peilgebieden.geometry.at[row.peilgebied_to].centroid
                pos[row.peilgebied_to] = (geom.x, geom.y)

            edges.append(
                [
                    df_edges.at[row.Index, "peilgebied_from"],
                    df_edges.at[row.Index, "peilgebied_to"],
                ]
            )

        G.add_nodes_from(nodes)
        G.add_edges_from(edges)
        if self.debug:
            print(f"Number of edges: {G.number_of_edges()}")
            print(f"Number of nodes: {len(G)}")

        return G, pos

    def find_nodegroups(self, G: networkx.DiGraph) -> Tuple[List, Dict]:
        if self.debug:
            agg_unique = self.agg_peilgebieden[self.agg_col].unique()
            print(f"Uniek aantal {self.agg_col}: {len(agg_unique)}")

        cat_nodes = {}
        cat_area = {}
        for idx, row in self.agg_peilgebieden.iterrows():
            cat_nodes[idx] = row[self.agg_col]
            cat_area[idx] = row.geometry.area

        # References to outgoing nodes (adjacency list) en incoming nodes.
        G_adj = {k: v for k, v in G.adjacency()}
        G_inc = {k: [] for k in G_adj}
        for k, v in G_adj.items():
            for vi in v.keys():
                G_inc[vi].append(k)

        # This wil hold all the nodegroups.
        nodegroups = []

        # This will hold an administration of the (final) category of each node
        visited_nodes = {}

        # Look for nodes that have at least 2 incoming nodes. These incoming nodes
        # cannot be None and need to have no incoming nodes of their own.
        for k, v in G_inc.items():
            v = [vi for vi in v if not vi.startswith("None_")]
            v = [vi for vi in v if len(G_inc[vi]) == 0]
            if len(v) >= 2:
                # Group per category
                adj_groups = {}
                for vi in v:
                    cat = cat_nodes[vi]
                    if cat not in adj_groups:
                        adj_groups[cat] = []
                    adj_groups[cat].append(vi)

                # Only actually add the group if it has more than 1 node
                for cat, nodes in adj_groups.items():
                    if len(nodes) > 1:
                        for n in nodes:
                            visited_nodes[n] = cat
                        nodegroups.append(nodes.copy())

        for startnode in G:
            # Set the category if the area is larger than 'joker_area' or if the
            # node was visited (grouped) in the parallel nodes part. In the latter
            # case the category was already used and needs to be set now as well.
            cat = None
            if startnode in visited_nodes:
                cat = visited_nodes[startnode]
            elif not startnode.startswith("None_") and cat_area[startnode] >= self.joker_area:
                cat = cat_nodes[startnode]
            visited_nodes[startnode] = cat

            nodelist = [startnode]
            new_group = [startnode]
            while len(nodelist) > 0:
                # Bekijk de eerste node en verwijder deze van nodelist.
                node = nodelist.pop(0)

                # Zoek de verbonden peilgebieden en filter verbonden peilgebieden eruit
                # die niet dezelfde categorie hebben.
                connected_nodes = list(G_adj[node].keys())
                add_nodes = []
                for n in connected_nodes:
                    if n in new_group:
                        continue

                    if n.startswith("None_") or cat_area[n] < self.joker_area:
                        # Behandel None gebeiden als een joker
                        if n in visited_nodes:
                            curcat = visited_nodes[n]
                        elif cat is None:
                            curcat = None
                        else:
                            curcat = cat
                    else:
                        curcat = cat_nodes[n]
                        if cat is None:
                            cat = curcat

                    if curcat == cat:
                        # Als het geen wildcard categorie is, controleer dat de
                        # andere nodes in de groep ook deze categorie hebben.
                        if curcat is not None:
                            for n in add_nodes:
                                if visited_nodes[n] is None:
                                    visited_nodes[n] = curcat
                                else:
                                    assert visited_nodes[n] == curcat
                        # Voeg de node toe en administreer de categorie
                        add_nodes.append(n)
                        visited_nodes[n] = curcat

                # Administreer de toe te voegen nodes.
                nodelist += add_nodes
                new_group += add_nodes

            # Maak categorie in groep consistent
            cats = list(set([visited_nodes[n] for n in new_group]))
            cats = [c for c in cats if c is not None]
            if len(cats) == 0:
                # Groep met alleen None, laat deze zo
                pass
            elif len(cats) == 1:
                # Forceer categorie voor alle nodes in deze groep
                final_cat = cats[0]
                for n in new_group:
                    if visited_nodes[n] is None:
                        visited_nodes[n] = final_cat
            else:
                # Meerdere categorieen in dezelfde groep, error
                raise ValueError(cats)

            # Voeg de groep toe aan de lijst van groepen
            nodegroups.append(new_group)

        if self.debug:
            print(f"Number of node groups: {len(nodegroups)}")

        return nodegroups, visited_nodes

    def merge_nodegroups(self, nodegroups: List, cat_per_node: Dict) -> List:
        # Bekijk of de groep van nodes enige overlap heeft met een bestaande groep.
        # Zo ja, voeg dan de groep van nodes bij deze bestaande groep.
        cross_group = pd.DataFrame(index=np.unique(np.hstack(nodegroups)), columns=range(len(nodegroups)))
        cross_group = cross_group.fillna(False)
        for i, group in enumerate(nodegroups):
            cross_group.loc[group, i] = True

        nodes_per_group = {}
        for group in cross_group.columns:
            cg_idx = cross_group[group].values
            nodes_per_group[group] = cross_group.index[cg_idx].tolist()

        groups_per_node = {}
        for node, row in cross_group.iterrows():
            groups_per_node[node] = cross_group.columns[row.values].tolist()

        visited_groups = []
        visited_nodes = []
        nodegroups_merged = []
        for group in nodes_per_group:
            if group not in visited_groups:
                merged_group = []
                new_nodes = nodes_per_group[group].copy()
                visited_groups.append(group)
                merged_cat = None
                while len(new_nodes) > 0:
                    add_node = new_nodes.pop(0)
                    if add_node not in visited_nodes:
                        if merged_cat is None:
                            merged_cat = cat_per_node[add_node]

                        # Check category of group with category of add_node
                        if cat_per_node[add_node] is not None and merged_cat != cat_per_node[add_node]:
                            raise ValueError("Category conflict")

                        # Expand search based on add_node
                        for add_group in groups_per_node[add_node]:
                            if add_group not in visited_groups:
                                new_nodes += nodes_per_group[add_group].copy()
                                visited_groups.append(add_group)

                        # Add node
                        visited_nodes.append(add_node)
                        merged_group.append(add_node)

                if len(merged_group) > 0:
                    nodegroups_merged.append(merged_group)

        # Valideer dat iedere node maximaal aan 1 groep is toegewezen
        cross_group_merged = pd.DataFrame(
            index=np.unique(np.hstack(nodegroups)),
            columns=range(len(nodegroups_merged)),
        )
        cross_group_merged = cross_group_merged.fillna(False)
        for i, group in enumerate(nodegroups_merged):
            cross_group_merged.loc[group, i] = True
        if (cross_group_merged.sum(axis=1) != 1).any():
            raise ValueError("Nodes toegewezen aan meer dan 1 groep")

        if self.debug:
            print(f"Number of merged node groups: {len(nodegroups_merged)}")

        return nodegroups_merged

"""Renumber the nodes of an input model after those of a reference model, so that references to node IDs still hold.

The feedback forms, the feedback scripts and the coupling refer to the nodes of the input models of 2024 by ID. An input
model made from other data, or with other algorithms, numbers its nodes anew. `match_nodes` matches its nodes to those of
the reference: Basins by the overlap of their areas, the other nodes by type and location, preferring nodes that connect
the same Basins. `renumber_nodes` gives matched nodes the ID of their match, and the other nodes IDs after the highest ID of
the reference, and links the ID of the link between the same nodes in the reference. See `docs/dev/reproducing-models.qmd`.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely

from ribasim_nl.crossings.model import SORT_KEYS, colors, sorted_table
from ribasim_nl.model import Model

logger = logging.getLogger(__name__)

# the columns with node IDs, in the tables of the input models
NODE_ID_COLUMNS = ["node_id", "meta_node_id", "from_node_id", "to_node_id"]


@dataclass
class Reference:
    """The nodes, links and Basin areas of a reference model, to number the nodes of another model after.

    :param nodes: the nodes, with the columns node_id, node_type and geometry
    :param links: the links, with the columns link_id, from_node_id and to_node_id
    :param areas: the Basin areas, with the columns node_id and geometry
    """

    nodes: gpd.GeoDataFrame
    links: pd.DataFrame
    areas: gpd.GeoDataFrame

    @classmethod
    def from_model(cls, model: Model) -> "Reference":
        """The reference of a model."""
        assert model.basin.area.df is not None, "the model has no Basin areas"
        nodes = model.node.df.reset_index()[["node_id", "node_type", "geometry"]]
        links = pd.DataFrame(model.link.df[["from_node_id", "to_node_id"]]).reset_index()
        areas = model.basin.area.df.reset_index(drop=True)[["node_id", "geometry"]]
        return cls(gpd.GeoDataFrame(nodes, crs=model.crs), links, gpd.GeoDataFrame(areas, crs=model.crs))

    @classmethod
    def read(cls, path: Path) -> "Reference":
        """Read a reference from a GeoPackage with the layers node, link and area."""
        links = gpd.read_file(path, layer="link", ignore_geometry=True)
        return cls(gpd.read_file(path, layer="node"), pd.DataFrame(links), gpd.read_file(path, layer="area"))

    def write(self, path: Path) -> None:
        """Write the reference to a GeoPackage."""
        path.unlink(missing_ok=True)
        self.nodes.to_file(path, layer="node")
        geometry = gpd.GeoSeries([None] * len(self.links), index=self.links.index)
        gpd.GeoDataFrame(self.links, geometry=geometry, crs=self.nodes.crs).to_file(path, layer="link")
        self.areas.to_file(path, layer="area")


def _greedy(pairs: pd.DataFrame) -> dict[int, int]:
    """Match the pairs one to one, in their order."""
    mapping: dict[int, int] = {}
    taken: set[int] = set()
    for new, old in zip(pairs["new"], pairs["old"], strict=True):
        if new not in mapping and old not in taken:
            mapping[int(new)] = int(old)
            taken.add(int(old))
    return mapping


def match_basins(areas: gpd.GeoDataFrame, reference: gpd.GeoDataFrame) -> dict[int, int]:
    """Match the Basins one to one to those of the reference whose areas overlap more than half of both, largest first.

    Basins with the same area, which some models have, match in the order of their IDs.
    """
    assert areas.crs is not None, "the Basin areas have no CRS"
    overlay = gpd.overlay(
        areas[["node_id", "geometry"]].rename(columns={"node_id": "new"}),
        reference[["node_id", "geometry"]].rename(columns={"node_id": "old"}).to_crs(areas.crs),
        how="intersection",
        keep_geom_type=True,
    )
    overlay["overlap"] = overlay.area
    area_new = areas.dissolve("node_id").area
    area_old = reference.dissolve("node_id").area
    large = (overlay["overlap"] > 0.5 * overlay["new"].map(area_new)) & (
        overlay["overlap"] > 0.5 * overlay["old"].map(area_old)
    )
    overlay = overlay.loc[large].sort_values(["overlap", "new", "old"], ascending=[False, True, True], kind="stable")
    return _greedy(overlay)


def _neighbours(links: pd.DataFrame) -> tuple[dict[int, frozenset[int]], dict[int, frozenset[int]]]:
    """The upstream and downstream neighbours of each node."""
    upstream: dict[int, set[int]] = {}
    downstream: dict[int, set[int]] = {}
    for a, b in zip(links["from_node_id"].to_numpy(dtype=int), links["to_node_id"].to_numpy(dtype=int), strict=True):
        downstream.setdefault(int(a), set()).add(int(b))
        upstream.setdefault(int(b), set()).add(int(a))
    return {k: frozenset(v) for k, v in upstream.items()}, {k: frozenset(v) for k, v in downstream.items()}


def match_nodes(
    model: Model, reference: Reference, tolerance: float = 1.0, loose_tolerance: float = 250.0
) -> dict[int, int]:
    """Match the nodes of a model to those of a reference, as a mapping from their ID to that of the reference.

    Basins match by `match_basins`. The other nodes match one to one with a node of the same type within `tolerance`,
    first those that connect the same matched Basins, then by distance. The nodes that are left match with a node of the
    same type that connects the same Basins within `loose_tolerance`, nearest first: a crossing that moved along the
    boundary of its peilgebied, for instance.
    """
    assert model.basin.area.df is not None, "the model has no Basin areas"
    nodes = model.node.df.reset_index()[["node_id", "node_type", "geometry"]]
    basins = match_basins(model.basin.area.df.reset_index(drop=True), reference.areas)
    up_new, down_new = _neighbours(pd.DataFrame(model.link.df[["from_node_id", "to_node_id"]]))
    up_old, down_old = _neighbours(reference.links)
    old_nodes = reference.nodes.to_crs(model.crs)
    type_new = nodes.set_index("node_id")["node_type"]
    type_old = old_nodes.set_index("node_id")["node_type"]
    empty: frozenset[int] = frozenset()

    def basins_of(ids: frozenset[int], node_type: pd.Series) -> frozenset[int]:
        return frozenset(i for i in ids if node_type.get(i) == "Basin")

    def same_basins(new: int, old: int) -> bool:
        up = frozenset(basins.get(i) for i in basins_of(up_new.get(new, empty), type_new))
        down = frozenset(basins.get(i) for i in basins_of(down_new.get(new, empty), type_new))
        return up == basins_of(up_old.get(old, empty), type_old) and down == basins_of(
            down_old.get(old, empty), type_old
        )

    def candidates(new: gpd.GeoDataFrame, old: gpd.GeoDataFrame, distance: float) -> pd.DataFrame:
        """The pairs of nodes of a type within `distance`, their distance, and whether they connect the same Basins."""
        i_new, i_old = old.sindex.query(new.geometry, predicate="dwithin", distance=distance)
        pairs = pd.DataFrame({"new": new["node_id"].to_numpy()[i_new], "old": old["node_id"].to_numpy()[i_old]})
        same_type = pairs["new"].map(type_new).to_numpy() == pairs["old"].map(type_old).to_numpy()
        pairs = pairs.loc[same_type].reset_index(drop=True)
        pairs["distance"] = shapely.distance(
            new.geometry.to_numpy()[i_new][same_type], old.geometry.to_numpy()[i_old][same_type]
        )
        pairs["same_basins"] = [same_basins(a, b) for a, b in zip(pairs["new"], pairs["old"], strict=True)]
        return pairs

    others = nodes.loc[nodes["node_type"] != "Basin"].reset_index(drop=True)
    old_others = old_nodes.loc[old_nodes["node_type"] != "Basin"].reset_index(drop=True)
    pairs = candidates(others, old_others, tolerance)
    pairs = pairs.sort_values(
        ["same_basins", "distance", "new", "old"], ascending=[False, True, True, True], kind="stable"
    )
    mapping = basins | _greedy(pairs)

    left = others.loc[~others["node_id"].isin(mapping)].reset_index(drop=True)
    old_left = old_others.loc[~old_others["node_id"].isin(mapping.values())].reset_index(drop=True)
    if len(left) > 0 and len(old_left) > 0:
        loose = candidates(left, old_left, loose_tolerance)
        loose = loose.loc[loose["same_basins"]].sort_values(["distance", "new", "old"], kind="stable")
        loose_matches = _greedy(loose)
        for new, old in loose_matches.items():
            logger.info(
                f"{type_new[new]} {new} matches {old} at {loose.set_index(['new', 'old']).loc[(new, old), 'distance']:.1f} m"
            )
        mapping |= loose_matches
    return mapping


def renumber_nodes(model: Model, reference: Reference) -> dict[int, int]:
    """Renumber the nodes and links of a model after a reference, in place, and return the mapping of the node IDs.

    Matched nodes, see `match_nodes`, get the ID of the node they match; the others IDs after the highest ID of the
    reference, in the order of their IDs. Links get the ID of the link between the same nodes in the reference, or else
    IDs after its highest link ID. The tables are sorted again, and the Basin areas colored again, as
    `ribasim_nl.crossings.model` does.
    """
    matched = match_nodes(model, reference)
    next_id = int(max(reference.nodes["node_id"].max(), 0)) + 1
    mapping = {}
    for node_id in sorted(model.node.df.index):
        if node_id in matched:
            mapping[node_id] = matched[node_id]
        else:
            mapping[node_id] = next_id
            next_id += 1
    assert len(set(mapping.values())) == len(mapping), "node IDs are not unique after renumbering"
    unmatched_new = len(mapping) - len(matched)
    unmatched_old = len(reference.nodes) - len(matched)
    logger.info(f"{len(matched)} nodes matched, {unmatched_new} new and {unmatched_old} of the reference not matched")

    node = model.node.df
    node.index = pd.Index(node.index.map(mapping), name="node_id")
    for column in NODE_ID_COLUMNS:
        if column in node.columns:
            node[column] = node[column].map(mapping)
    model.node.df = node.sort_index(kind="stable")
    link = model.link.df
    for column in NODE_ID_COLUMNS:
        if column in link.columns:
            link[column] = link[column].map(mapping)
    old_link = {
        (int(a), int(b)): int(i)
        for a, b, i in zip(
            reference.links["from_node_id"], reference.links["to_node_id"], reference.links["link_id"], strict=True
        )
    }
    next_link = int(max(reference.links["link_id"].max(), -1)) + 1
    link_ids = []
    for a, b in zip(link["from_node_id"], link["to_node_id"], strict=True):
        if (int(a), int(b)) in old_link:
            link_ids.append(old_link[(int(a), int(b))])
        else:
            link_ids.append(next_link)
            next_link += 1
    assert len(set(link_ids)) == len(link_ids), "link IDs are not unique after renumbering"
    link.index = pd.Index(link_ids, name=link.index.name)
    model.link.df = link.sort_index(kind="stable")
    for name, keys in SORT_KEYS.items():
        node_type, table = name.split(" / ")
        snake = "".join(f"_{c.lower()}" if c.isupper() else c for c in node_type).lstrip("_")
        table_model = getattr(getattr(model, snake), table)
        df = table_model.df
        if df is None or len(df) == 0:
            continue
        df = df.reset_index(drop=True)
        for column in NODE_ID_COLUMNS:
            if column in df.columns:
                df[column] = df[column].map(mapping)
        df = sorted_table(df, keys)
        if "meta_color" in df.columns:
            df["meta_color"] = colors(len(df))
        table_model.df = df
    return mapping


def renumber_checks(checks: dict[str, gpd.GeoDataFrame], mapping: dict[int, int]) -> None:
    """Renumber the node IDs in the layers of `database_checks.gpkg` after `renumber_nodes`, in place."""
    for name, layer in checks.items():
        for column in ("node_id", "meta_node_id"):
            if column in layer.columns:
                renumbered = layer[column].map(mapping)
                assert renumbered.notna().sum() == layer[column].notna().sum(), f"{name}: unknown node IDs in {column}"
                # keep the type, which is object in some layers, so that they are written as before
                renumbered = renumbered.astype(layer[column].dtype)
                layer[column] = renumbered

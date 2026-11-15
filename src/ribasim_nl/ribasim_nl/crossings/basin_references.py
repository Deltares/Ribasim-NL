"""Trial: keep the references to the Basins of the input model of 2024 when the aggregation areas are refined.

The feedback forms and scripts refer to the Basins of the input models of 2024, which are the delivered aggregation
areas. With refined aggregation areas, see `ribasim_nl.crossings.aggregation`, the connector nodes at the crossings
between the delivered aggregation areas keep their ID, see `ribasim_nl.crossings.renumber`, but a Basin of 2024 becomes
several Basins. `BasinLookup` finds the Basin that replaces a Basin of 2024 at a location, and
`translate_feedback_form` rewrites the Basins in a feedback form with it.
"""

import logging
from collections.abc import Collection, Hashable, Iterable, Mapping
from pathlib import Path
from typing import cast

import geopandas as gpd
import pandas as pd
import shapely
from ribasim.geometry.node import NodeData
from ribasim.nodes import tabulated_rating_curve
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

from ribasim_nl.aquo import NODE_ID_FACTOR, authority_by_code
from ribasim_nl.crossings.aggregation import FIRST_NEW_NODE_ID, REFINED
from ribasim_nl.model import Model
from ribasim_nl.ribasim_feedback_processor import RibasimFeedbackProcessor

logger = logging.getLogger(__name__)


class BasinLookup:
    """The Basins that replace each Basin of the input model of 2024.

    :param areas: the Basin areas of a model with refined aggregation areas, without those of the storage Basins, with
        the columns node_id and geometry
    :param reference_areas: the Basin areas of the input model of 2024, with the columns node_id and geometry
    """

    def __init__(self, areas: gpd.GeoDataFrame, reference_areas: gpd.GeoDataFrame) -> None:
        areas = areas[["node_id", "geometry"]].dissolve("node_id")
        assert areas.crs is not None, "the Basin areas have no CRS"
        reference = reference_areas[["node_id", "geometry"]].to_crs(areas.crs).dissolve("node_id")
        overlay = gpd.overlay(
            areas.reset_index().rename(columns={"node_id": "new"}),
            reference.reset_index().rename(columns={"node_id": "old"}),
            how="intersection",
            keep_geom_type=True,
        )
        overlay["overlap"] = overlay.area
        # a Basin belongs to the Basin of 2024 that it overlaps most, if that covers most of it
        overlay = overlay.loc[overlay["overlap"] > 0.5 * overlay["new"].map(areas.area)]
        overlay = overlay.sort_values(["new", "overlap", "old"], ascending=[True, False, True], kind="stable")
        best = overlay.drop_duplicates("new")
        self.old_of: dict[int, int] = dict(zip(best["new"].astype(int), best["old"].astype(int), strict=True))
        self.areas: dict[int, BaseGeometry] = dict(zip(areas.index.astype(int), areas.geometry.to_numpy(), strict=True))

    @classmethod
    def read(cls, model: Model, reference_path: Path, removed: Collection[int] = ()) -> "BasinLookup":
        """The lookup of a model, with the reference of `verwerkt/boezemmodel_ids.gpkg`.

        :param removed: the Basins of 2024 that the feedback form removes, see `removed_basins`. They often cover the
            same area as another Basin of 2024, which the Basins then replace.
        """
        assert model.basin.area.df is not None, "the model has no Basin areas"
        areas = model.basin.area.df
        if "meta_categorie" in model.node.df.columns:
            # a storage Basin has the area of its Basin
            bergend = model.node.df.index[model.node.df["meta_categorie"] == "bergend"]
            areas = areas.loc[~areas["node_id"].isin(bergend)]
        reference = gpd.read_file(reference_path, layer="area")
        return cls(areas, reference.loc[~reference["node_id"].isin(list(removed))])

    def basins(self, old: int) -> list[int]:
        """The Basins that replace a Basin of 2024."""
        return sorted(new for new, o in self.old_of.items() if o == old)

    def merge(self, basin: int, into: int) -> None:
        """Record that a Basin is merged into another node, which gets its area if it is a Basin."""
        self.old_of.pop(basin, None)
        area = self.areas.pop(basin, None)
        if into in self.areas and area is not None:
            self.areas[into] = shapely.union(self.areas[into], area)

    def is_single(self, old: int) -> bool:
        """Whether a Basin of 2024 is replaced by one Basin with its ID."""
        return self.basins(old) == [old]

    def at(self, old: int, point: Point, max_distance: float = 2000.0) -> int:
        """The Basin that replaces a Basin of 2024 at or nearest to a location.

        Beyond `max_distance` the location says nothing, as for an administrative connection. Then it is the Basin
        that kept the ID, the one that covers most of the Basin of 2024.
        """
        candidates = self.basins(old)
        assert candidates, f"no Basin replaces Basin {old} of 2024"
        distance = {basin: self.areas[basin].distance(point) for basin in candidates}
        nearest = min(candidates, key=lambda basin: (distance[basin], basin))
        if distance[nearest] > max_distance:
            assert old in candidates, (
                f"the Basin nearest to {point} that replaces Basin {old} of 2024 is {distance[nearest]:.0f} m away"
            )
            return old
        return nearest


def remove_dangling_connectors(model: Model, among: Collection[int] | None = None) -> list[int]:
    """Remove the connector nodes that lack a node upstream or downstream, with their links, and return their IDs.

    A feedback form that removes a Basin of 2024 also removes its connector nodes, but not those that the Basin with
    its ID has to its neighbours within the delivered aggregation area. The Terminals and FlowBoundaries that are
    left without a connector node are removed as well.

    :param among: the nodes to consider, by default all
    """
    connector_types = ["Pump", "Outlet", "TabulatedRatingCurve", "ManningResistance"]
    removed: list[int] = []
    while True:
        node_type = model.node.df["node_type"]
        upstream = set(model.link.df["to_node_id"])
        downstream = set(model.link.df["from_node_id"])
        dangling = sorted(
            int(i)
            for i, kind in zip(node_type.index.to_numpy(), node_type.to_numpy(), strict=True)
            if (among is None or i in among)
            and (
                (kind in connector_types and (i not in upstream or i not in downstream))
                or (kind == "Terminal" and i not in upstream)
                or (kind == "FlowBoundary" and i not in downstream)
            )
        )
        if not dangling:
            return removed
        for node_id in dangling:
            model.remove_node(node_id, remove_links=True)
        removed += dangling


def rating_curves_between_levels(model: Model, unknown_level: float, tolerance: float = 0.005) -> list[int]:
    """Turn the ManningResistances between Basins with different target levels into TabulatedRatingCurves.

    They get the direction from the higher to the lower target level, as feedback forms do by hand. A crossing is a
    ManningResistance if the peilgebieden at it have the same target level, which no longer holds once a peilgebied
    is merged with another, such as a part of the boezem with a polder.

    :param model: model of the feedback stage, before `ribasim_param.add_outlets`
    :param unknown_level: the target level of Basins without one, which are skipped
    :return: the node IDs that changed
    """
    assert model.basin.area.df is not None, "the model has no Basin areas"
    level = model.basin.area.df.drop_duplicates("node_id").set_index("node_id")["meta_streefpeil"]
    level = pd.to_numeric(level, errors="coerce")
    level = level[level != unknown_level]
    changed: list[int] = []
    for node_id in sorted(model.node.df.index[model.node.df["node_type"] == "ManningResistance"]):
        link = model.link.df
        upstream = link.index[link["to_node_id"] == node_id]
        downstream = link.index[link["from_node_id"] == node_id]
        if len(upstream) != 1 or len(downstream) != 1:
            continue
        level_up = level.get(link.at[upstream[0], "from_node_id"])
        level_down = level.get(link.at[downstream[0], "to_node_id"])
        if pd.isna(level_up) or pd.isna(level_down) or abs(level_up - level_down) <= tolerance:
            continue
        if level_up < level_down:
            links = [upstream[0], downstream[0]]
            link.loc[links, ["from_node_id", "to_node_id"]] = link.loc[links, ["to_node_id", "from_node_id"]].to_numpy()
            link.loc[links, "geometry"] = shapely.reverse(link.loc[links, "geometry"].to_numpy())
        model.update_node(
            node_id,
            "TabulatedRatingCurve",
            data=[tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
        )
        changed.append(int(node_id))
    return changed


def removed_basins(feedback_excel: Path) -> frozenset[int]:
    """The Basins of 2024 that a feedback form removes."""
    form = RibasimFeedbackProcessor.load_feedback(feedback_excel)
    rows = form.loc[(form["Actie"] == "Verwijderen") & (form["Node Type"] == "Basin") & form["Node ID"].notna()]
    return frozenset(rows["Node ID"].astype(int))


def translate_feedback_form(processor: RibasimFeedbackProcessor, reference_path: Path) -> dict[str, int]:
    """Rewrite the Basins of 2024 in the rows of a feedback form to the Basins that replace them, in place.

    - A node that is added between two Basins goes between the Basins at its location.
    - A link of which the direction is reversed runs between a connector node and a Basin of 2024. The Basin becomes
      the neighbour of the connector node that replaces it.
    - A Basin of 2024 is removed if it covers the area of another Basin as well, or is not part of the model. With
      refined aggregation areas, the Basin with its ID merges into another Basin that covers most of its area, a
      copy of the same peilgebieden. Without one it stays if a Basin of 2024 that the form keeps covers most of its
      area, since no other Basin represents that area now. Else it is removed.
    - A Basin of 2024 that becomes another node, such as a boundary, does so with all Basins that replace it.
    - Target levels are only set for the Basins of 2024 that one Basin replaces, since the refined areas have their own.

    :param processor: the processor of the form, of which the model has refined aggregation areas
    :param reference_path: the nodes and Basin areas of the input model of 2024, `verwerkt/boezemmodel_ids.gpkg`
    :return: the number of rewritten references, of those that could not be rewritten, and of Basins that merge or stay
    """
    model = processor.model
    types_2024 = processor.df_node_types["node_type"]
    removed = removed_basins(Path(processor.feedback_excel))
    lookup = BasinLookup.read(model, reference_path, removed)
    reference = gpd.read_file(reference_path, layer="area")
    kept_2024 = shapely.make_valid(reference.loc[~reference["node_id"].isin(list(removed))].geometry.to_numpy())
    counts = {"rewritten": 0, "unchanged": 0, "failed": 0, "merged": 0, "kept": 0}

    def is_basin(node_id: float) -> bool:
        return pd.notna(node_id) and types_2024.get(int(node_id)) == "Basin"

    def neighbours(node_id: int) -> set[int]:
        link = model.link.df
        ids = set(link.loc[link["from_node_id"] == node_id, "to_node_id"])
        return {int(i) for i in ids | set(link.loc[link["to_node_id"] == node_id, "from_node_id"])}

    def rewrite(index: Hashable, column: str, old: int, new: int) -> None:
        counts["rewritten" if new != old else "unchanged"] += 1
        processor.df.loc[index, column] = new

    def covered(geometry: BaseGeometry, by: BaseGeometry) -> bool:
        return shapely.intersection(geometry, by).area > 0.5 * geometry.area

    skip: list[Hashable] = []
    for index, row in processor.df.iterrows():
        if row["Actie"] == "Verwijderen" and row["Node Type"] == "Basin" and pd.notna(row["Node ID"]):
            basin = int(row["Node ID"])
            if basin not in lookup.areas:
                continue
            area = shapely.make_valid(lookup.areas[basin])
            copies = [b for b, other in lookup.areas.items() if b != basin and covered(area, shapely.make_valid(other))]
            if copies:
                # the copy that it connects to, or else the one with the lowest ID
                linked = neighbours(basin) | {n for connector in neighbours(basin) for n in neighbours(connector)}
                target = min(copies, key=lambda b: (b not in linked, b))
                connectors = neighbours(basin) & neighbours(target)
                model.merge_basins(node_id=basin, to_node_id=target, are_connected=bool(connectors))
                lookup.merge(basin, target)
                counts["merged"] += 1
                skip.append(index)
            elif any(covered(area, other) for other in kept_2024):
                counts["kept"] += 1
                skip.append(index)
            else:
                lookup.merge(basin, -1)
        elif row["Actie"] == "Aanpassen" and row["Verbinding"] == "Node" and is_basin(row["Node ID.2"]):
            # a Basin that becomes another node, such as a boundary: the Basins that replace it become that node
            basin = int(row["Node ID.2"])
            replacing = lookup.basins(basin)
            if basin not in replacing:
                continue
            for other in replacing:
                if other != basin:
                    connectors = neighbours(other) & neighbours(basin)
                    model.merge_basins(node_id=other, to_node_id=basin, are_connected=bool(connectors))
                    lookup.merge(other, basin)
                    counts["merged"] += 1
        elif row["Actie"] == "Toevoegen":
            point = Point(row["Coordinaat X"], row["Coordinaat Y"])
            for column in ["Node ID A", "Node ID B"]:
                if is_basin(row[column]):
                    try:
                        rewrite(index, column, int(row[column]), lookup.at(int(row[column]), point))
                    except AssertionError as e:
                        # without the Basin, the node would be added without its links
                        counts["failed"] += 1
                        skip.append(index)
                        print(f"The feedback form adds no node at row {index}: {e}")
        elif row["Actie"] == "Aanpassen" and row["Verbinding"] in ("Edge", "Link"):
            a, b = row["Node ID A.1"], row["Node ID B.1"]
            column, basin, connector = ("Node ID A.1", a, b) if is_basin(a) else ("Node ID B.1", b, a)
            if not is_basin(basin) or pd.isna(connector):
                continue
            node_type = model.node.df["node_type"]
            replacing = [n for n in neighbours(int(connector)) if lookup.old_of.get(n) == int(basin)]
            replacing = [n for n in replacing if node_type.get(n) == "Basin"]
            if len(replacing) == 1:
                rewrite(index, column, int(basin), replacing[0])
            else:
                counts["failed"] += 1
                logger.warning(
                    f"row {index}: {len(replacing)} Basins that replace Basin {int(basin)} at node {int(connector)}"
                )
    processor.df = processor.df.loc[~processor.df.index.isin(skip)]

    # the node types of the form are those of 2024; add those of the new nodes
    node_type = model.node.df["node_type"]
    new_types = pd.DataFrame({"node_type": node_type[~node_type.index.isin(types_2024.index)]})
    processor.df_node_types = pd.concat([processor.df_node_types, new_types])
    processor.target_level_basins = frozenset(
        int(b) for b in types_2024.index[types_2024 == "Basin"] if lookup.is_single(int(b))
    )
    return counts


def shifted_past_new_boundaries(node_id: int, nodes: pd.DataFrame) -> int:
    """The ID of a node that the feedback stage adds, past the LevelBoundaries of the new FlowBoundaries.

    `ribasim_param.FlowBoundaries_to_LevelBoundaries` numbers the LevelBoundaries of the FlowBoundaries that the input
    model of 2024 lacks after the others. The nodes that the stage adds later follow these.

    :param node_id: the ID of the node, shifted by `added_node_shift`
    :param nodes: the Node table of a model of the feedback stage or later, by node ID
    """
    if "meta_old_node_id" not in nodes.columns:
        return node_id
    new = nodes.index[(nodes["node_type"] == "LevelBoundary") & (nodes["meta_old_node_id"] >= FIRST_NEW_NODE_ID)]
    return node_id + len(new) if len(new) > 0 and node_id >= new.min() else node_id


def write_added_nodes(feedback_database: Path, reference_path: Path, path: Path) -> None:
    """Write the nodes that the feedback stage of 2024 added, `verwerkt/feedback_ids.gpkg`, from its model.

    These are the nodes of the feedback model with an ID after those of the input model of 2024, with their type
    and location. The forcing scripts and the coupling refer to them by ID, see `locate_node`.
    """
    nodes = gpd.read_file(feedback_database, layer="Node", fid_as_index=True)
    last_2024 = int(gpd.read_file(reference_path, layer="node", ignore_geometry=True)["node_id"].max())
    added = nodes.loc[nodes.index > last_2024, ["node_type", "geometry"]].rename_axis("node_id").reset_index()
    path.unlink(missing_ok=True)
    added.to_file(path, layer="node")


def locate_node(
    nodes: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame,
    node_type: str,
    point: Point,
    tolerance: float = 5.0,
    basin_tolerance: float = 500.0,
) -> int | None:
    """The node of a type at a location, or None if there is none.

    A Basin is the one whose area holds the location, or else the nearest within `basin_tolerance`, since a Basin
    that the feedback stage makes from part of another can lie elsewhere in that part. Other nodes are within
    `tolerance`.

    :param nodes: the Node table of a model of the feedback stage or the profiles stage, by node ID
    :param areas: its Basin areas, with the column node_id
    """
    candidates = nodes.loc[nodes["node_type"] == node_type]
    if node_type == "Basin":
        if "meta_categorie" in candidates.columns:
            candidates = candidates.loc[candidates["meta_categorie"] != "bergend"]
        own = areas.loc[areas["node_id"].isin(candidates.index)]
        distance = own.geometry.distance(point).groupby(own["node_id"].to_numpy()).min()
        tolerance = basin_tolerance
        # of the Basins whose area holds the location, the one with the nearest node
        distance = distance.loc[distance == distance.min()] if len(distance) > 0 else distance
        tie = candidates.loc[distance.index].geometry.distance(point)
        order = pd.DataFrame({"distance": distance, "tie": tie}).sort_values(["distance", "tie"], kind="stable")
        return int(order.index[0]) if len(order) > 0 and order["distance"].iloc[0] <= tolerance else None
    distance = candidates.geometry.distance(point).sort_values(kind="stable")
    return int(distance.index[0]) if len(distance) > 0 and distance.iloc[0] <= tolerance else None


def locate_added_node(
    added: gpd.GeoDataFrame, node_id: int, nodes: gpd.GeoDataFrame, areas: gpd.GeoDataFrame
) -> int | None:
    """The node of a model that the feedback stage of 2024 added with an ID, see `locate_node`.

    :param added: the nodes that the feedback stage of 2024 added, by node ID, see `write_added_nodes`
    """
    if node_id not in added.index:
        return None
    node_type = cast(str, added.at[node_id, "node_type"])
    return locate_node(nodes, areas, node_type, cast(Point, added.at[node_id, "geometry"]))


def added_node_shift(input_model_toml: Path, reference_path: Path) -> int:
    """The difference between the ID of a node that the feedback stage adds, and the ID it had with the input model of 2024.

    Added nodes are numbered after the highest ID of the input model, in the order in which they are added.
    """
    # the node ID is the feature ID of the Node table
    node = gpd.read_file(input_model_toml.parent / "input/database.gpkg", layer="Node", fid_as_index=True)
    reference = gpd.read_file(reference_path, layer="node", ignore_geometry=True)
    return int(node.index.max()) - int(reference["node_id"].max())


def reversed_to_supply(node_functions: pd.DataFrame, target_level: pd.Series) -> tuple[int, ...]:
    """The draining or flow control nodes that supply instead, since another one runs the other way between the same Basins.

    Between the peilgebieden of a delivered aggregation area, a gemaal often lifts the water of the lower peilgebied,
    next to a stuw or inlet from the higher one. Both are categorized as drain or flow control, which lets the
    drainage circulate between the two, and which `ribasim_nl.control` does not allow within a category. If only one
    of the two Basins has no other draining or flow control node out, the nodes that leave it keep their function: a
    peilgebied that is pumped out, or one that is pumped up and drains back over a stuw. Else the direction with a Pump
    keeps its function if only one has Pumps, else the direction from the higher to the lower target level, else that
    of the node with the lowest ID. The nodes the other way supply.

    :param node_functions: the table of `ribasim_nl.workflow.node_function_table`
    :param target_level: the target level of the Basins, by node ID
    """
    nodes = node_functions.loc[node_functions["function"].isin(["drain", "flow_control"])]
    direction = pd.Series(list(zip(nodes["from_node_id"], nodes["to_node_id"], strict=True)), index=nodes.index)
    directions = set(direction)
    to_supply: list[int] = []
    done: set[tuple[int, int]] = set()
    for a, b in sorted(directions):
        if (b, a) not in directions or (b, a) in done:
            continue
        done.add((a, b))
        forward = sorted(direction.index[direction == (a, b)])
        backward = sorted(direction.index[direction == (b, a)])
        pumps_forward = (nodes.loc[forward, "node_type"] == "Pump").any()
        pumps_backward = (nodes.loc[backward, "node_type"] == "Pump").any()
        level_a, level_b = target_level.get(a), target_level.get(b)
        other_exit_a = ((nodes["from_node_id"] == a) & (nodes["to_node_id"] != b)).any()
        other_exit_b = ((nodes["from_node_id"] == b) & (nodes["to_node_id"] != a)).any()
        if other_exit_a != other_exit_b:
            # the Basin without another way out depends on the other: the nodes that leave it keep their function
            keep_forward = other_exit_b
        elif pumps_forward != pumps_backward:
            keep_forward = pumps_forward
        elif pd.notna(level_a) and pd.notna(level_b) and level_a != level_b:
            keep_forward = level_a > level_b
        else:
            keep_forward = forward[0] < backward[0]
        to_supply += backward if keep_forward else forward
    return tuple(int(i) for i in sorted(to_supply))


class References2024:
    """The nodes of a model with refined aggregation areas, by the ID that the scripts of 2024 refer to them with.

    - A Basin of 2024 is replaced by one or more Basins, see `BasinLookup`: `basin` gives the one at a location,
      `basins` all of them, and `merge_basins` merges them into the Basin they connect to.
    - Another node of the input model of 2024 kept its ID, see `ribasim_nl.crossings.renumber`.
    - A node that the feedback stage adds is numbered after the nodes of the input model, of which there are more
      now. The feedback scripts refer to nodes that the form and they themselves just added: these are added in the
      same order, so their ID is shifted by a fixed number, see `node`. The LevelBoundaries of the FlowBoundaries
      that the input model of 2024 lacks, see `ribasim_param.FlowBoundaries_to_LevelBoundaries`, shift the nodes that
      are added after them once more.
    - Later stages find an added node by its type and location in the feedback model of 2024, `added_path`, since
      the number of nodes that a script adds can depend on the Basins, see `locate_node`.

    :param model: model of the feedback stage or a later one
    :param reference_path: the nodes and Basin areas of the input model of 2024, `verwerkt/boezemmodel_ids.gpkg`
    :param input_model_toml: the input model with refined aggregation areas
    :param feedback_excel: the feedback form, of which the removed Basins of 2024 are not replaced
    :param added_path: the nodes that the feedback stage of 2024 added, `verwerkt/feedback_ids.gpkg`, for a model of
        a later stage
    """

    def __init__(
        self,
        model: Model,
        reference_path: Path,
        input_model_toml: Path,
        feedback_excel: Path,
        added_path: Path | None = None,
    ) -> None:
        self.model = model
        self.added = None if added_path is None else gpd.read_file(added_path, layer="node").set_index("node_id")
        nodes = gpd.read_file(reference_path, layer="node", ignore_geometry=True)
        self.types_2024 = nodes.set_index("node_id")["node_type"]
        self.last_2024 = int(nodes["node_id"].max())
        self.shift = added_node_shift(input_model_toml, reference_path)
        self.lookup = BasinLookup.read(model, reference_path, removed_basins(feedback_excel))

    def is_basin(self, node_id: int) -> bool:
        """Whether the ID is that of a Basin of the input model of 2024."""
        return self.types_2024.get(node_id) == "Basin"

    def node(self, node_id: int) -> int:
        """The ID of a node other than a Basin of 2024: its own if it is of 2024, else that of the added node."""
        assert not self.is_basin(node_id), f"{node_id} is a Basin of 2024, see `basin` and `basins`"
        if node_id <= self.last_2024:
            return node_id
        if self.added is None:
            return shifted_past_new_boundaries(node_id + self.shift, self.model.node.df)
        assert self.model.basin.area.df is not None, "the model has no Basin areas"
        located = locate_added_node(self.added, node_id, self.model.node.df, self.model.basin.area.df)
        # an ID that no node has, so that what refers to it does nothing, as for a node that 2024 lacked
        return -node_id if located is None else located

    def nodes(self, node_ids: Iterable[int]) -> tuple[int, ...]:
        """The IDs of several nodes, see `node`. Reports those that are not in the model, as some never were."""
        node_ids = tuple(node_ids)
        translated = tuple(self.node(node_id) for node_id in node_ids)
        missing = [old for old, new in zip(node_ids, translated, strict=True) if new not in self.model.node.df.index]
        if missing:
            print(f"Nodes of 2024 that are not in the model: {missing}")
        return translated

    def keys[T](self, by_node: Mapping[int, T]) -> dict[int, T]:
        """A mapping by node ID, with the IDs of `nodes`."""
        return dict(zip(self.nodes(by_node), by_node.values(), strict=True))

    def basins(self, basin_2024: int) -> list[int]:
        """The Basins that replace a Basin of 2024."""
        assert self.is_basin(basin_2024), f"{basin_2024} is not a Basin of 2024"
        return self.lookup.basins(basin_2024)

    def basin(self, basin_2024: int, near: NodeData | Point) -> NodeData:
        """The Basin that replaces a Basin of 2024 at or nearest to a location, or to a node."""
        point = near if isinstance(near, Point) else near.geometry
        return self.model.basin[self.lookup.at(basin_2024, point)]

    def _neighbouring_basins(self, basin: int) -> set[int]:
        """The Basins that a Basin is linked to, directly or through one connector node."""
        node_type = self.model.node.df["node_type"]
        near = self._linked(basin)
        beyond = {n for connector in near if node_type.get(connector) != "Basin" for n in self._linked(connector)}
        return {n for n in (near | beyond) - {basin} if node_type.get(n) == "Basin"}

    def merge_basins(self, node_id: int, to_node_id: int, are_connected: bool = True) -> None:
        """Merge the Basins that replace a Basin of 2024 into the Basin that replaces another, as `Model.merge_basins`.

        Each of them merges into the Basin of the other that it connects to, or else into the nearest one. Basins and
        boundaries that the feedback stage added are merged as they are.
        """
        sources = self.basins(node_id) if self.is_basin(node_id) else [self.node(node_id)]
        if not sources:
            logger.warning(f"no Basin replaces Basin {node_id} of 2024, which was to merge into {to_node_id}")
        for source in sources:
            connected = are_connected
            if self.is_basin(to_node_id):
                targets = [t for t in self.basins(to_node_id) if t != source]
                if not targets:
                    logger.warning(f"no Basin replaces Basin {to_node_id} of 2024, to merge Basin {source} into")
                    continue
                neighbours = self._neighbouring_basins(source)
                linked = [t for t in targets if t in neighbours]
                connected = are_connected and bool(linked)
                if are_connected and not linked:
                    logger.warning(f"Basin {source} connects to no Basin that replaces Basin {to_node_id} of 2024")
                area = self.lookup.areas[source]
                target = min(linked if connected else targets, key=lambda t: (self.lookup.areas[t].distance(area), t))
            else:
                target = self.node(to_node_id)
            self.model.merge_basins(node_id=source, to_node_id=target, are_connected=connected)
            self.lookup.merge(source, target)

    def translate_supply_flags(self, processor: RibasimFeedbackProcessor) -> None:
        """Rewrite the Basins and Outlets of which the feedback form sets the supply, for `set_aanvoer_flags`.

        A flag of a Basin of 2024 holds for all Basins that replace it.
        """

        def basins(basin_ids: tuple[int, ...] | None) -> tuple[int, ...]:
            return tuple(basin for old in basin_ids or () if self.is_basin(old) for basin in self.basins(old))

        processor._basin_aanvoer_on = basins(processor.basin_aanvoer_on)
        processor._basin_aanvoer_off = basins(processor.basin_aanvoer_off)
        processor._outlet_aanvoer_on = self.nodes(processor.outlet_aanvoer_on or ())
        processor._outlet_aanvoer_off = self.nodes(processor.outlet_aanvoer_off or ())

    def remove_basins(self, basin_2024: int) -> list[int]:
        """Remove the Basins that replace a Basin of 2024, with their storage Basins and the connector nodes between them.

        :return: the IDs of the removed nodes
        """
        model = self.model
        node_type = model.node.df["node_type"]
        category = (
            model.node.df["meta_categorie"] if "meta_categorie" in model.node.df.columns else pd.Series(dtype=object)
        )
        removed: list[int] = []
        for basin in self.basins(basin_2024):
            # the storage Basin is linked through a ManningResistance
            for resistance in self._linked(basin):
                if node_type.get(resistance) != "ManningResistance":
                    continue
                storage = [n for n in self._linked(resistance) if n != basin and category.get(n) == "bergend"]
                for node_id in (*storage, resistance) if storage else ():
                    model.remove_node(node_id, remove_links=True)
                    removed.append(node_id)
            model.remove_node(basin, remove_links=True)
            self.lookup.merge(basin, -1)
            removed.append(basin)
        return removed + remove_dangling_connectors(model)

    def _linked(self, node_id: int) -> set[int]:
        """The nodes that a node is linked to, in either direction."""
        link = self.model.link.df
        ids = set(link.loc[link["from_node_id"] == node_id, "to_node_id"])
        return {int(i) for i in ids | set(link.loc[link["to_node_id"] == node_id, "from_node_id"])}


def supply_opposite_nodes(model: Model, node_functions: pd.DataFrame) -> tuple[int, ...]:
    """Let the nodes of `reversed_to_supply` supply, in the table of the node functions, and return their IDs."""
    assert model.basin.area.df is not None, "the model has no Basin areas"
    target_level = model.basin.area.df.drop_duplicates("node_id").set_index("node_id")["meta_streefpeil"]
    to_supply = reversed_to_supply(node_functions, pd.to_numeric(target_level, errors="coerce"))
    node_functions.loc[node_functions.index.isin(to_supply), "function"] = "supply"
    print(f"{len(to_supply)} nodes supply, opposite another draining or flow control node")
    return to_supply


class CoupledReferences2024:
    """The nodes of the coupled model, by the ID that `pipeline/09_koppelen_modellen.py` refers to them with.

    That is the `waterbeheercode` of their authority times `NODE_ID_FACTOR` plus their ID in the models of 2024 of that
    authority. For the authorities with refined aggregation areas, a Basin of 2024 is the Basin that replaces it near a
    given node, and the nodes that the feedback stage added have shifted, see `References2024`. The other nodes keep
    their ID.

    :param data_dir: the data directory, with the reference and the models of each authority
    """

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self._types: dict[str, pd.Series] = {}
        self._lookup: dict[str, BasinLookup] = {}
        self._nodes: dict[str, gpd.GeoDataFrame] = {}
        # the nodes that the feedback stage of 2024 added, and the nodes and Basin areas of the feedback model
        self._added: dict[str, tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]] = {}

    def _reference_path(self, authority: str) -> Path:
        return self.data_dir / authority / "verwerkt/boezemmodel_ids.gpkg"

    def _read(self, authority: str) -> None:
        nodes = gpd.read_file(self._reference_path(authority), layer="node", ignore_geometry=True)
        self._types[authority] = nodes.set_index("node_id")["node_type"]
        database = self.data_dir / authority / f"modellen/{authority}_forcing/input/database.gpkg"
        self._nodes[authority] = gpd.read_file(database, layer="Node", fid_as_index=True)
        added = gpd.read_file(self._reference_path(authority).with_name("feedback_ids.gpkg"), layer="node")
        feedback = self.data_dir / authority / f"modellen/{authority}_feedback/input/database.gpkg"
        self._added[authority] = (
            added.set_index("node_id"),
            gpd.read_file(feedback, layer="Node", fid_as_index=True),
            gpd.read_file(feedback, layer="Basin / area"),
        )

    def _basin(self, authority: str, basin_2024: int, near: int | None) -> int:
        """The Basin that replaces a Basin of 2024 nearest to a node of the forcing model, or the one with its ID."""
        nodes = self._nodes[authority]
        if authority not in self._lookup:
            database = self.data_dir / authority / f"modellen/{authority}_forcing/input/database.gpkg"
            areas = gpd.read_file(database, layer="Basin / area")
            bergend = nodes.index[nodes["meta_categorie"] == "bergend"]
            reference = gpd.read_file(self._reference_path(authority), layer="area")
            form = self.data_dir / authority / "verwerkt/Feedback Formulier" / f"feedback_formulier_{authority}.xlsx"
            reference = reference.loc[~reference["node_id"].isin(list(removed_basins(form)))]
            self._lookup[authority] = BasinLookup(areas.loc[~areas["node_id"].isin(bergend)], reference)
        lookup = self._lookup[authority]
        if near is None:
            assert basin_2024 in lookup.basins(basin_2024), f"Basin {basin_2024} of 2024 of {authority} is replaced"
            return basin_2024
        return lookup.at(basin_2024, nodes.geometry.loc[near])

    def node(self, node_id: int, near: int | None = None) -> int:
        """The ID in the coupled model of a node, given as in 2024.

        :param node_id: the ID in the coupled model with the models of 2024
        :param near: for a Basin of 2024, a node of the same authority that it connects to, also given as in 2024
        """
        code, local = divmod(node_id, NODE_ID_FACTOR)
        authority = authority_by_code.get(code)
        if authority not in REFINED:
            return node_id
        if authority not in self._types:
            self._read(authority)
        types = self._types[authority]
        if types.get(local) == "Basin":
            near_local = None
            if near is not None and near // NODE_ID_FACTOR == code:
                near_local = self.node(near) % NODE_ID_FACTOR
            return code * NODE_ID_FACTOR + self._basin(authority, local, near_local)
        if local <= int(types.index.max()):
            return node_id
        # a node that the feedback stage added, which has the same ID in the later stages
        added, nodes, areas = self._added[authority]
        located = locate_added_node(added, local, nodes, areas)
        return -node_id if located is None else code * NODE_ID_FACTOR + located

    def keys[T](self, by_node: Mapping[int, T]) -> dict[int, T]:
        """A mapping by node ID, with the IDs of `node`."""
        return {self.node(node_id): value for node_id, value in by_node.items()}

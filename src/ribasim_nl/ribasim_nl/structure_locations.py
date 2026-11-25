"""Place the Pumps and Outlets of a level controlled model at the structures they represent.

The input model of a level controlled water authority has a connector node where a hydro-object crosses the boundary
of a peilgebied, see `ribasim_nl.crossings`. The gemaal or stuw that controls the flow there often lies some distance
along the hydro-object, and one crossing stands for all crossings between two peilgebieden without a structure of their
own. A node on a crossing would then be drawn on another watercourse than its structure, and the links to it would
follow that watercourse, see `ribasim_nl.network_snapping`.

`move_to_structures` moves each Pump to its gemaal and each node that is or becomes an Outlet to its stuw, see
`structure_locations`, when the input model is made, see `ribasim_nl.crossings.model.boezem_model`, and records the
code of the structure in `CODE_COLUMN`. The feedback form and the script of the feedback stage remove nodes, add nodes
and change their type, so `update_structures` matches the nodes without a structure again before that stage writes its
model, see `ribasim_nl.workflow.write_feedback_model`. The forcing stage no longer moves nodes to structures.

A moved node keeps the location of its crossing in the `CROSSING_COLUMNS` up to the end of the forcing stage, see
`drop_crossing_locations`. The crossing lies on the boundary between the two areas that the node connects, which is
what the parametrization looks at, with `crossing_points`: the gemaal whose name and capacity a Pump gets is the one
nearest to its crossing, see `ribasim_nl.assign_parametrization.AssignMetaData.add_meta_to_pumps`, a boundary node that
is added beside a node is placed beside its crossing, which sets its key and its water authority, see
`ribasim_nl.ribasim_parametrization.FlowBoundaries_to_LevelBoundaries`, and a node of a Basin that is split goes to the
part nearest to its crossing, see `ribasim_nl.split_basins.SplitBasins`. Other data refers to nodes by their key, see
`ribasim_nl.crossings.model.crossing_keys`.
"""

import logging
import typing
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import shapely.affinity

from ribasim_nl.keys import nonempty
from ribasim_nl.layout import move_nodes
from ribasim_nl.model import Model

LOG = logging.getLogger(__name__)

# Maximum distance between a crossing and the structure of its node [m], as between a Pump and the gemaal it is named
# after, see `ribasim_nl.workflow.assign_pump_and_basin_names`
MAX_STRUCTURE_DISTANCE = 100.0
# Maximum distance between an Outlet and a stuw behind another crossing between the same areas [m]. Two areas can share
# a boundary of kilometres, with inlets or outlets that the model does not have.
MAX_OUTLET_MOVE = 250.0
# Maximum distance between a node and its crossing, and between lines that touch [m]
TOLERANCE = 1.0
# Maximum distance between a stuw and the hydro-object it lies on [m]
ON_LINE_TOLERANCE = 2.0
# The coordinate reference systems of the layers of the crossings: Amersfoort / RD New, also with the NAP height, of
# which the coordinates in the plane are the same
RD_NEW = "EPSG:28992"
RD_EPSG = (28992, 7415)
# The node types that are or become Outlets, see `ribasim_nl.ribasim_parametrization.add_outlets`, and represent a stuw
OUTLET_TYPES = ("Outlet", "TabulatedRatingCurve", "FlowBoundary")
# The node column with the code of the structure of a node
CODE_COLUMN = "meta_code_waterbeheerder"
# The node columns with the location of the crossing of a node that is moved to its structure [m]
CROSSING_COLUMNS = ["meta_crossing_x", "meta_crossing_y"]


@dataclass(frozen=True)
class Crossings:
    """The crossings of the hydro-objects of a water authority with its peilgebieden, and its structures.

    :param rows: the crossings, with their location and the columns `hydroobject`, `gemaal` and `stuw` with the
        globalids of the hydro-objects they lie on and of their structures, separated by commas, and `area_from` and
        `area_to` with the areas they connect
    :param gemaal: the location of each gemaal, by globalid
    :param stuw: the location of each stuw, by globalid
    :param lines: the hydro-objects and culverts, by globalid
    :param codes: the code of each gemaal and stuw, by globalid, see `identifiers`
    """

    rows: gpd.GeoDataFrame
    gemaal: gpd.GeoSeries
    stuw: gpd.GeoSeries
    lines: gpd.GeoSeries
    codes: pd.Series = field(default_factory=lambda: pd.Series(dtype=object))

    def __post_init__(self) -> None:
        missing = {"hydroobject", "gemaal", "stuw", "area_from", "area_to"} - set(self.rows.columns)
        assert not missing, f"The crossings miss the columns {sorted(missing)}"
        for name in ("gemaal", "stuw", "lines"):
            assert getattr(self, name).index.is_unique, f"The globalids of the {name} are not unique"

    @classmethod
    def read(cls, path: Path) -> "Crossings":
        """Read the crossings and structures of `verwerkt/crossings.gpkg`, see `pipeline/00c_parse_crossings.py`."""

        def read(name: str, columns: list[str] | None = None) -> gpd.GeoDataFrame:
            df = gpd.read_file(path, layer=name, columns=columns)
            assert df.crs is not None and df.crs.to_epsg() in RD_EPSG, f"Layer {name} of {path} is in {df.crs}"
            df = df[df.geometry.notna() & ~df.geometry.is_empty].reset_index(drop=True)
            return df.set_geometry(gpd.GeoSeries(shapely.force_2d(df.geometry.to_numpy()), crs=RD_NEW))

        def layer(name: str) -> gpd.GeoSeries:
            df = read(name, ["globalid"]).drop_duplicates("globalid")
            return gpd.GeoSeries(df.geometry.to_numpy(), index=df["globalid"].to_numpy(), crs=RD_NEW)

        rows = read("crossings_hydroobject_filtered")
        # the aggregation areas become the Basins, or the peilgebieden where there are none
        for side in ("from", "to"):
            area = rows[f"agg_area_{side}"] if f"agg_area_{side}" in rows.columns else rows[f"peilgebied_{side}"]
            rows[f"area_{side}"] = area
        lines = pd.concat([layer("hydroobject"), layer("duikersifonhevel")])
        lines = lines[~lines.index.duplicated()]
        codes = pd.concat([identifiers(read(name, ["globalid", "code"])) for name in ("gemaal", "stuw")])
        return cls(
            gpd.GeoDataFrame(rows[["hydroobject", "gemaal", "stuw", "area_from", "area_to", "geometry"]]),
            layer("gemaal"),
            layer("stuw"),
            gpd.GeoSeries(lines, crs=RD_NEW),
            codes[~codes.index.duplicated(keep="first")],
        )


def identifiers(layer: pd.DataFrame) -> pd.Series:
    """The identifier of the objects of a layer, by globalid: their code, or their globalid if the code is a dummy.

    The postprocessing gives the objects without a code or without a globalid a dummy one from their row number.
    """
    layer = layer.rename(columns={"CODE": "code"})
    code = layer["code"].astype(object)
    globalid = layer["globalid"].astype(object).astype(str)
    dummy = code.isna() | code.astype(str).str.startswith("dummy")
    identifier = code.astype(str).str.strip().where(~dummy, globalid.str.strip("{}"))
    identifiers = pd.Series(identifier.to_numpy(), index=globalid.to_numpy())
    return identifiers[~identifiers.index.duplicated(keep="first")]


def _ids(value: object) -> list[str]:
    """The globalids in a cell of the crossings, which are separated by commas."""
    return [i for i in value.split(",") if i] if isinstance(value, str) else []


def structure_locations(
    nodes: gpd.GeoDataFrame,
    crossings: Crossings,
    *,
    max_distance: float = MAX_STRUCTURE_DISTANCE,
    max_outlet_move: float = MAX_OUTLET_MOVE,
    tolerance: float = TOLERANCE,
) -> dict[int, str]:
    """The gemaal of each Pump and the stuw of each Outlet that lies on a crossing, by the globalid of the structure.

    The nodes of `OUTLET_TYPES` count as Outlets. A Pump gets a gemaal that its crossing is matched with. An Outlet gets
    a stuw that its crossing is matched with, or else a stuw on or just behind a hydro-object of another crossing
    between the same two areas: on one of the hydro-objects of that crossing, or on a hydro-object that touches them.
    Such an Outlet stands for all crossings between these areas without a structure of their own, of which the input
    model keeps one.

    A structure is at most `max_distance` from its crossing, and a stuw of another crossing at most `max_outlet_move`
    from the Outlet. A structure gets one node: the nearest go first, and stuwen that the crossing of an Outlet is
    matched with before those of other crossings. Ties go to the lowest node id and globalid.

    A node with the code of a structure in `CODE_COLUMN` is at its structure already, and gets no other one. The
    structure at the location of such a node, or of another node that is not on a crossing, is taken.

    :param nodes: the nodes, with their node_type, and possibly the `CODE_COLUMN`
    :param crossings: the crossings and structures of the water authority
    :param max_distance: maximum distance between a crossing and the structure of its node
    :param max_outlet_move: maximum distance between an Outlet and a stuw of another crossing
    :param tolerance: maximum distance between a node and its crossing, and between hydro-objects that touch
    :return: per node that gets a structure, its globalid
    """
    rows = crossings.rows
    row_points = rows.geometry.to_numpy()
    row_tree = shapely.STRtree(row_points)
    line_ids = crossings.lines.index.to_numpy()
    line_geometries = crossings.lines.to_numpy()
    line_number = {globalid: i for i, globalid in enumerate(line_ids)}
    line_tree = shapely.STRtree(line_geometries)
    stuw_ids = crossings.stuw.index.to_numpy()
    stuw_points = crossings.stuw.to_numpy()
    stuw_tree = shapely.STRtree(stuw_points)
    structure_ids = np.concatenate([crossings.gemaal.index.to_numpy(), stuw_ids])
    structure_tree = shapely.STRtree(np.concatenate([crossings.gemaal.to_numpy(), stuw_points]))
    rows_between: dict[tuple[str, str], list[int]] = defaultdict(list)
    for i, pair in enumerate(zip(rows["area_from"], rows["area_to"], strict=True)):
        if isinstance(pair[0], str) and isinstance(pair[1], str):
            rows_between[pair].append(i)

    behind: dict[int, list[int]] = {}

    def stuwen_behind(row: int) -> list[int]:
        """The stuwen on or just behind the hydro-objects of a crossing, as their numbers."""
        if row not in behind:
            near = stuw_tree.query(row_points[row], predicate="dwithin", distance=max_distance)
            own = [line_number[i] for i in _ids(rows["hydroobject"].iat[row]) if i in line_number]
            behind[row] = []
            if len(near) > 0 and own:
                touching = line_tree.query(line_geometries[own], predicate="dwithin", distance=tolerance)[1]
                reach = shapely.union_all(line_geometries[np.unique(np.concatenate([own, touching]))])
                behind[row] = near[shapely.dwithin(stuw_points[near], reach, ON_LINE_TOLERANCE)].tolist()
        return behind[row]

    # the structures each node may get: (0 for those its own crossing is matched with, distance, node id, globalid)
    candidates: list[tuple[int, float, int, str]] = []
    taken: set[str] = set()
    connectors = nodes[nodes["node_type"].isin(["Pump", *OUTLET_TYPES])]
    has_code = (
        nonempty(connectors[CODE_COLUMN]).to_numpy()
        if CODE_COLUMN in connectors.columns
        else np.zeros(len(connectors), dtype=bool)
    )
    for node_id, node_type, point, placed in zip(
        connectors.index, connectors["node_type"], connectors.geometry, has_code, strict=True
    ):
        at_node = np.sort(row_tree.query(point, predicate="dwithin", distance=tolerance))
        if placed or len(at_node) == 0:
            taken |= set(structure_ids[structure_tree.query(point, predicate="dwithin", distance=tolerance)].tolist())
            continue
        column = "gemaal" if node_type == "Pump" else "stuw"
        structures = getattr(crossings, column)
        matched = {i for row in at_node for i in _ids(rows[column].iat[row])}
        for globalid in sorted(matched & set(structures.index)):
            distance = float(point.distance(structures[globalid]))
            if distance <= max_distance:
                candidates.append((0, distance, int(node_id), globalid))
        if node_type != "Pump":
            pairs = {(rows["area_from"].iat[row], rows["area_to"].iat[row]) for row in at_node}
            near = row_tree.query(point, predicate="dwithin", distance=max_outlet_move + max_distance)
            others = sorted({row for pair in pairs for row in rows_between.get(pair, [])} & set(near.tolist()))
            for stuw in sorted({stuw for row in others for stuw in stuwen_behind(row)}):
                distance = float(point.distance(stuw_points[stuw]))
                if distance <= max_outlet_move:
                    candidates.append((1, distance, int(node_id), str(stuw_ids[stuw])))

    result: dict[int, str] = {}
    for _, _, node_id, globalid in sorted(candidates):
        if node_id not in result and globalid not in taken:
            result[node_id] = globalid
            taken.add(globalid)
    return result


def move_to_structures(
    model: Model, crossings: Crossings, *, max_distance: float = MAX_STRUCTURE_DISTANCE
) -> dict[int, str]:
    """Move the Pumps and Outlets on a crossing to the structure they represent, see `structure_locations`.

    The control nodes of a moved node move along with it, and the ends of its links, which are routed at the end of
    the forcing stage, see `ribasim_nl.network_snapping.snap_to_network`. A node with a structure gets its code in
    `CODE_COLUMN`, as the nodes of the gravity controlled models.

    :param model: Ribasim model of a level controlled water authority
    :param crossings: the crossings and structures of the water authority
    :param max_distance: maximum distance between a crossing and the structure of its node
    :return: per moved node, the globalid of its structure
    """
    nodes, links = model.node.df, model.link.df
    structures = structure_locations(nodes, crossings, max_distance=max_distance)
    points = pd.concat([crossings.gemaal, crossings.stuw])
    locations: dict[int, shapely.Point] = {}
    moved: dict[int, str] = {}
    crossing_of: dict[int, shapely.Point] = {}
    control = links[links["link_type"] == "control"]
    controlled = control.groupby("from_node_id")["to_node_id"].agg(list)
    for node_id, globalid in structures.items():
        old = typing.cast(shapely.Point, nodes.at[node_id, "geometry"])
        new = typing.cast(shapely.Point, points[globalid])
        if old.distance(new) <= TOLERANCE:
            continue
        locations[node_id] = new
        moved[node_id] = globalid
        crossing_of[node_id] = old
        # the nodes that only control this node keep their place next to it
        for control_id in control.loc[control["to_node_id"] == node_id, "from_node_id"]:
            if controlled[control_id] == [node_id]:
                point = typing.cast(shapely.Point, nodes.at[control_id, "geometry"])
                locations[int(control_id)] = shapely.affinity.translate(point, new.x - old.x, new.y - old.y)
    # a link without a geometry, which is a straight line once the model is written, gets that line now
    missing = links.index[
        links.geometry.isna() & (links["from_node_id"].isin(locations) | links["to_node_id"].isin(locations))
    ]
    if len(missing) > 0:
        starts = nodes.geometry[links.loc[missing, "from_node_id"]]
        ends = nodes.geometry[links.loc[missing, "to_node_id"]]
        lines = [shapely.LineString([start, end]) for start, end in zip(starts, ends, strict=True)]
        links.loc[missing, "geometry"] = gpd.GeoSeries(lines, index=missing, crs=links.crs)
    move_nodes(model, locations)
    if CODE_COLUMN not in nodes.columns:
        nodes[CODE_COLUMN] = pd.Series(None, index=nodes.index, dtype=object)
    nodes.loc[list(structures), CODE_COLUMN] = [
        crossings.codes.get(globalid, globalid.strip("{}")) for globalid in structures.values()
    ]
    for column in CROSSING_COLUMNS:
        if column not in nodes.columns:
            nodes[column] = np.nan
    # a node that is moved again keeps the location of its crossing
    has_crossing = set(nodes.index[nodes[CROSSING_COLUMNS[0]].notna()])
    first = [node_id for node_id in moved if node_id not in has_crossing]
    if first:
        nodes.loc[first, CROSSING_COLUMNS] = [[crossing_of[node_id].x, crossing_of[node_id].y] for node_id in first]
    pumps = int((nodes.loc[list(moved), "node_type"] == "Pump").sum())
    LOG.info(f"Moved {pumps} Pumps to their gemaal and {len(moved) - pumps} Outlets to their stuw")
    return moved


def crossing_points(nodes: gpd.GeoDataFrame) -> gpd.GeoSeries:
    """The location of each node, or of its crossing if the node is moved to its structure, see `CROSSING_COLUMNS`."""
    points = gpd.GeoSeries(nodes.geometry.copy())
    if set(CROSSING_COLUMNS) <= set(nodes.columns):
        x, y = (nodes[column].astype(float) for column in CROSSING_COLUMNS)
        moved = (x.notna() & y.notna()).to_numpy()
        if moved.any():
            points[moved] = gpd.points_from_xy(x[moved], y[moved])
    return points


def update_structures(model: Model, crossings: Crossings) -> dict[int, str]:
    """Match the nodes without a structure again, after nodes are removed or added, or changed to another type.

    A moved node without the code of a structure, such as a Pump that a feedback form made an Outlet, which clears its
    code, see `ribasim_nl.ribasim_feedback_processor.RibasimFeedbackProcessor.adjust_node`, goes back to its crossing
    first. Then the nodes on a crossing without a structure get one, see `move_to_structures`: a stuw for a node that
    became an Outlet, the structure of a node that was removed, or that of a node that was added on a crossing. Nodes
    that have a structure keep it.

    :return: per moved node, the globalid of its structure
    """
    nodes = model.node.df
    if set(CROSSING_COLUMNS) <= set(nodes.columns):
        has_code = nonempty(nodes[CODE_COLUMN]) if CODE_COLUMN in nodes.columns else pd.Series(False, index=nodes.index)
        back = nodes.index[nodes[CROSSING_COLUMNS[0]].notna() & ~has_code]
        points = crossing_points(nodes)
        move_nodes(model, {int(node_id): typing.cast(shapely.Point, points[node_id]) for node_id in back})
        nodes.loc[back, CROSSING_COLUMNS] = np.nan
    return move_to_structures(model, crossings)


def drop_crossing_locations(model: Model) -> None:
    """Remove the `CROSSING_COLUMNS`, once the parametrization that looks at the crossings is done."""
    model.node.df = model.node.df.drop(columns=[c for c in CROSSING_COLUMNS if c in model.node.df.columns])

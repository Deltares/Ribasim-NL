"""Add Observation nodes with measured discharges to a model.

The measurement series and the model links they are compared with are taken from the koppeltabel that is also used
by `ribasim_nl.analyse_results`. Each series gets an Observation node at the measurement location, with the measured
values. If the series is measured on a single link, the Observation node observes the `flow_rate` of the connector
node of that link. If it adds and subtracts the flows of multiple links, it observes a Junction with that flow, the
net flow of the nodes beyond its link towards its Basin, see `ribasim_nl.flow_paths.Routes.flow_rate_links`. These
nodes are found by their type and location, which do not change when links are renumbered. Other Observation nodes are
not linked, as a reminder; `meta_koppeling` says why.

Series are linked before the Basin trees of a model are merged, see `ribasim_nl.flow_paths.merge_basin_trees`, which
keeps the observed Junctions, and the others again after, see `link_observations`, as a merged tree can combine the
inflows and outflows of a Basin.
"""

import logging
import re
import typing

import geopandas as gpd
import pandas as pd
import shapely
from pandera.typing import DataFrame
from ribasim import Node
from ribasim.input_base import NodeData
from ribasim.schemas import ObservationTimeSchema

from ribasim_nl.coupling_level_network import first_non_junction
from ribasim_nl.flow_paths import Routes
from ribasim_nl.model import Model
from ribasim_nl.settings import settings

LOG = logging.getLogger(__name__)

RESULTAATVERGELIJKING = settings.ribasim_nl_data_dir / "Basisgegevens/resultaatvergelijking"
COUPLING_XLSX = (
    RESULTAATVERGELIJKING
    / "koppeltabel_2026/Transformed_koppeltabel_versie_rapportage_26062026_Feedback_Verwerkt_HydroLogic.xlsx"
)
SPECIFICS_XLSX = RESULTAATVERGELIJKING / "koppeltabel_2026/Specifiek_bewerking_versierapportage_26062026.xlsx"
MEASUREMENT_CSVS = {
    "Aanvoer": RESULTAATVERGELIJKING / "meetreeksen_2026/Metingen_aanvoer_dag_totaal.csv",
    "Afvoer": RESULTAATVERGELIJKING / "meetreeksen_2026/Metingen_afvoer_dag_totaal.csv",
}

# Node types of which the flow_rate can be observed, in order of preference
OBSERVED_NODE_TYPES = (
    "Pump",
    "Outlet",
    "ManningResistance",
    "TabulatedRatingCurve",
    "LinearResistance",
    "FlowBoundary",
    "Junction",
)
# A structure can be converted between an Outlet and a Pump, keeping its location, see
# `ribasim_nl.control.set_continuous_level_control`, so the koppeltabel may name the other type
INTERCHANGEABLE_TYPES = {"Outlet": "Pump", "Pump": "Outlet"}
# Operations of the koppeltabel that sum the flows of the links, see ApplySpecificOperation
SUM_OPERATIONS = ("optellen", "optellen_en_negatief_maken")
# Operations of the koppeltabel after which the measurement has the sign of the model flow
NEGATE_OPERATIONS = ("negatief_maken", "optellen_en_negatief_maken")


def _points(value: object) -> list[shapely.Point]:
    """Parse the points in a koppeltabel cell like "[<POINT (1 2)>, <POINT (3 4)>]"."""
    return [shapely.Point(float(x), float(y)) for x, y in re.findall(r"POINT \(([-\d.e+]+) ([-\d.e+]+)\)", str(value))]


def _strings(value: object) -> list[str]:
    """Parse the strings in a koppeltabel cell like "['Basin', 'Pump']"."""
    return re.findall(r"'([^']*)'", str(value))


class LinkEnd(typing.NamedTuple):
    """The node to observe of a koppeltabel link, and the type of the node on the other end."""

    node_type: str
    geometry: shapely.Point
    other_type: str
    upstream: bool  # whether the other node is upstream


def _link_end(from_type: str, from_point: shapely.Point, to_type: str, to_point: shapely.Point) -> LinkEnd | None:
    """The preferred node to observe of a koppeltabel link, if any."""
    ends = [LinkEnd(from_type, from_point, to_type, False), LinkEnd(to_type, to_point, from_type, True)]
    ends = [end for end in ends if end.node_type in OBSERVED_NODE_TYPES]
    return min(ends, key=lambda end: OBSERVED_NODE_TYPES.index(end.node_type)) if ends else None


def _signs(operation: str | float | None, count: int) -> list[int] | None:
    """The sign of the flow of each of `count` links in a koppeltabel operation, or None if it does not add them.

    Without an operation, or with "negatief_maken", the series is measured on one link. A formula like
    "link1+link2-link3" names each link once.
    """
    if operation is None or (isinstance(operation, float) and pd.isna(operation)) or operation == "negatief_maken":
        return [1] if count == 1 else None
    if operation in SUM_OPERATIONS:
        return [1] * count
    terms = re.findall(r"([+-]?)link(\d+)", str(operation))
    if "".join(sign + f"link{i}" for sign, i in terms).lstrip("+") != str(operation).lstrip("+"):
        return None
    signs = {int(i): -1 if sign == "-" else 1 for sign, i in terms}
    if len(signs) != len(terms) or sorted(signs) != list(range(1, count + 1)):
        return None
    return [signs[i] for i in range(1, count + 1)]


def read_coupling(authority: str) -> gpd.GeoDataFrame:
    """The series of an authority, with the nodes to observe of their links.

    :return: a table with columns MeetreeksC, Aan/Af, negate (whether the measurement has the opposite sign of the
        flow_rate), ends (the `LinkEnd` per link, or None if the series does not add the flows of the links or a link
        has no node to observe), signs (per link the sign of its flow in the series), koppeling (why ends is None) and
        geometry of the measurement location
    """
    coupling = pd.read_excel(COUPLING_XLSX)
    coupling = coupling[coupling["Waterschap"] == authority]
    specifics = pd.read_excel(SPECIFICS_XLSX)
    specifics = specifics[specifics["Waterschap"] == authority].set_index(["MeetreeksC", "Aan/Af"])["Specifiek"]
    assert specifics.index.is_unique, f"Duplicate specific operations for {authority}"

    rows = []
    for _, row in coupling.iterrows():
        from_types, to_types = _strings(row["new_from_node_types"]), _strings(row["new_to_node_types"])
        from_points, to_points = _points(row["new_from_node_geometry"]), _points(row["new_to_node_geometry"])
        assert len(from_types) == len(to_types) == len(from_points) == len(to_points), row["MeetreeksC"]
        operation = specifics.get((row["MeetreeksC"], row["Aan/Af"]))
        ends: list[LinkEnd | None] | None = [
            _link_end(*link) for link in zip(from_types, from_points, to_types, to_points, strict=True)
        ]
        signs = _signs(operation, len(from_types))
        koppeling = ""
        if not from_types:
            ends, koppeling = None, "no links in the koppeltabel"
        elif signs is None:
            ends, koppeling = None, f"combines {len(from_types)} links with {operation}"
        elif None in ends:
            link = ends.index(None)
            ends, koppeling = None, f"no node to observe on its {from_types[link]} -> {to_types[link]} link"
        location = shapely.from_wkt(row["geometry"]) if isinstance(row["geometry"], str) else None
        if location is None:
            assert ends and ends[0] is not None, f"No location for {row['MeetreeksC']}"
            location = ends[0].geometry
        rows.append(
            {
                "MeetreeksC": row["MeetreeksC"],
                "Aan/Af": row["Aan/Af"],
                "negate": operation in NEGATE_OPERATIONS,
                "ends": ends,
                "signs": signs,
                "koppeling": koppeling,
                "geometry": location,
            }
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:28992")


def read_measurements() -> dict[str, pd.DataFrame]:
    """Daily measured discharges [m3/s] per measurement file, with a time index and a column per series."""
    measurements = {}
    for key, path in MEASUREMENT_CSVS.items():
        df = pd.read_csv(path, index_col="Datum")
        df.index = pd.to_datetime(df.index).rename("time")
        measurements[key] = df
    return measurements


def add_observations(model: Model, authority: str) -> Model:
    """Add an Observation node with the measured discharge for each series of an authority.

    :param model: Ribasim model
    :param authority: water authority as named in the koppeltabel

    :return: updated Ribasim model
    """
    coupling = read_coupling(authority)
    model = add_observation_nodes(model, coupling, read_measurements())
    links = model.link.df[model.link.df["link_type"] == "observation"]
    LOG.info(f"{authority} has {len(coupling)} Observation nodes, of which {len(links)} are linked")
    return model


class _Graph:
    """Flow neighbors of the nodes of a model, to look past Junctions, see `ribasim_nl.flow_paths.Routes`."""

    def __init__(self, model: Model) -> None:
        self.routes = Routes(model)
        node_type = model.node.df["node_type"]
        self.node_types = dict(zip(node_type.index.astype(int), node_type.astype(str), strict=True))

    def neighbor(self, node_id: int, upstream: bool) -> int | None:
        """The neighbor through Junctions, if there is exactly one, see `first_non_junction`."""
        return first_non_junction(self.routes, node_id, upstream)[0]

    def combining_junction(self, signs: dict[int, int]) -> tuple[int, int] | None:
        """The Junction whose flow rate is the sum of the flow rates of the nodes in `signs`, times their sign.

        :return: the Junction, and the sign of its flow rate relative to that sum, or None if there is none
        """
        for upstream in (False, True):
            found = self._combining_junction(signs, upstream)
            if found is not None:
                return found
        return None

    def _combining_junction(self, signs: dict[int, int], upstream: bool) -> tuple[int, int] | None:
        """The Junction of `combining_junction` downstream of the first node, or upstream if `upstream`.

        In a Basin tree, the Junction is on the way to the Basin, with the nodes of `signs` beyond it, which then all
        flow into the Basin with the same sign, and out of it with the opposite sign. Its flow rate is that of its link
        towards the Basin, see `Routes.flow_rate_links`. Other Junctions connect the nodes upstream of them with those
        downstream, so these have the same sign.
        """
        routes = self.routes
        node_ids = set(signs)
        links = routes.links_at(min(node_ids), upstream)
        if len(links) != 1:
            return None
        node_id = routes.next_node(links[0], upstream)
        if node_id not in routes.plain:
            return None
        if routes.tree_basin(node_id) is None:
            sign = set(signs.values())
            for _ in range(50):
                if len(sign) != 1 or node_id not in routes.plain:
                    return None
                if routes.node_ends(node_id, not upstream) == node_ids:
                    return node_id, sign.pop()
                links = routes.links_at(node_id, upstream)
                if len(links) != 1:
                    return None
                node_id = routes.next_node(links[0], upstream)
            return None
        while (parent_link := routes.parent_link(node_id)) is not None:
            inflows, outflows = routes.subtree(parent_link)
            if not inflows | outflows <= node_ids:
                return None
            if inflows | outflows == node_ids:
                sign = {signs[i] for i in inflows} | {-signs[i] for i in outflows}
                if len(sign) != 1:
                    return None
                towards_basin = routes.next_node(parent_link, upstream=True) == node_id
                return node_id, sign.pop() if towards_basin else -sign.pop()
            node_id = routes.other_node(parent_link, node_id)
        return None


def _match(
    model: Model, end: LinkEnd, graph: _Graph, tolerance: float, moved_tolerance: float
) -> tuple[int | None, str]:
    """The model node of a `LinkEnd`, or None and why there is none.

    Nodes on top of each other are moved apart, see `ribasim_nl.layout.separate_nodes`, so if there is no node within
    `tolerance`, the nearest node within `moved_tolerance` is used. If there is no node of the type of the koppeltabel,
    an Outlet may be a Pump instead, or the other way around, see `INTERCHANGEABLE_TYPES`.
    """
    nodes = model.node.df

    def nearby(node_type: str) -> list:
        candidates = nodes[nodes["node_type"] == node_type]
        distance = candidates.distance(end.geometry)
        within = list(candidates.index[distance <= tolerance])
        if not within and moved_tolerance > tolerance:
            # the nearest node within moved_tolerance, as the node may have been moved
            within = sorted(candidates.index[distance <= moved_tolerance], key=lambda c: distance[c])[:1]
        return within

    within = nearby(end.node_type)
    if not within and end.node_type in INTERCHANGEABLE_TYPES:
        within = nearby(INTERCHANGEABLE_TYPES[end.node_type])
    if len(within) > 1:
        # stacked nodes, keep those with a neighbor of the right type on the right side, possibly behind Junctions
        within = [c for c in within if graph.node_types.get(graph.neighbor(c, end.upstream)) == end.other_type]
    if len(within) != 1:
        return None, f"{len(within)} {end.node_type} nodes at the location in the koppeltabel"
    return int(within[0]), ""


def _observed_node(
    model: Model, row: pd.Series, graph: _Graph, tolerance: float, moved_tolerance: float
) -> tuple[int | None, int, str]:
    """The node to observe for a series of `read_coupling`, or None and why there is none.

    :return: the node, the sign of its flow rate relative to the series, and why there is no node
    """
    ends, signs = row["ends"], row["signs"]
    if not isinstance(ends, list):
        return None, 1, row["koppeling"]
    matches = [_match(model, end, graph, tolerance, moved_tolerance) for end in ends]
    unmatched = [reason for node_id, reason in matches if node_id is None]
    if unmatched:
        return None, 1, unmatched[0]
    node_signs = {node_id: sign for (node_id, _), sign in zip(matches, signs, strict=True) if node_id is not None}
    if len(node_signs) != len(ends):
        return None, 1, "the same node for multiple links"
    if len(node_signs) == 1:
        ((node_id, sign),) = node_signs.items()
        return node_id, sign, ""
    # the flows of multiple nodes added and subtracted are the flow of a Junction that combines exactly them
    found = graph.combining_junction(node_signs)
    if found is None:
        return None, 1, f"no Junction combines exactly its {len(ends)} links"
    return *found, ""


def _link(model: Model, observation_id: int, node_id: int, graph: _Graph) -> None:
    """Link an Observation node to the node it observes."""
    geometry = typing.cast(shapely.Point, model.node.df.at[node_id, "geometry"])
    model.link.add(model.observation[observation_id], NodeData(node_id, graph.node_types[node_id], geometry))


def add_observation_nodes(
    model: Model, coupling: gpd.GeoDataFrame, measurements: dict[str, pd.DataFrame], tolerance: float = 1.0
) -> Model:
    """Add Observation nodes for the series in `coupling`, see `read_coupling` and `read_measurements`.

    :param model: Ribasim model
    :param coupling: series with the node to observe
    :param measurements: measured discharges
    :param tolerance: maximum distance between the node to observe in `coupling` and in the model

    :return: updated Ribasim model
    """
    period = slice(pd.Timestamp(model.starttime), pd.Timestamp(model.endtime))

    graph = _Graph(model)
    times: list[pd.DataFrame] = [] if model.observation.time.df is None else [model.observation.time.df]
    for _, row in coupling.iterrows():
        node_id, sign, koppeling = _observed_node(model, row, graph, tolerance, tolerance)
        observation = model.observation.add(
            Node(geometry=row["geometry"], name=row["MeetreeksC"], meta_aan_af=row["Aan/Af"], meta_koppeling=koppeling)
        )
        if node_id is None:
            LOG.warning(f"Not linking the Observation of {row['MeetreeksC']}: {koppeling}")
        else:
            _link(model, observation.node_id, node_id, graph)

        measured = measurements["Aanvoer" if row["Aan/Af"] == "Aanvoer" else "Afvoer"]
        if row["MeetreeksC"] not in measured.columns:
            LOG.warning(f"No measurements of {row['MeetreeksC']}")
            continue
        series = measured[row["MeetreeksC"]].loc[period].dropna()
        times.append(
            pd.DataFrame(
                {
                    "node_id": observation.node_id,
                    "variable": "flow_rate",
                    "time": series.index,
                    "value": (-sign if row["negate"] else sign) * series.to_numpy(),
                }
            )
        )

    if times:
        model.observation.time.df = DataFrame[ObservationTimeSchema](pd.concat(times, ignore_index=True))
    return model


def link_observations(
    model: Model, authority: str | None = None, tolerance: float = 1.0, moved_tolerance: float = 50.0
) -> Model:
    """Link the Observation nodes that are not linked yet, for instance in a model coupled from other models.

    Series can be measured on links between models, of which only the coupled model has all nodes, or combine inflows
    and outflows of a Basin, which only a merged Basin tree can, see `ribasim_nl.flow_paths.merge_basin_trees`. If the
    observed flow rate has the opposite sign, so do the measured values.

    :param model: Ribasim model
    :param authority: water authority of the Observation nodes as named in the koppeltabel, or else the
        meta_waterbeheerder of each node
    :param tolerance: maximum distance between the node to observe in the koppeltabel and in the model
    :param moved_tolerance: maximum distance to a node that was moved to separate it from other nodes, see `_match`

    :return: updated Ribasim model
    """
    nodes = model.node.df
    linked = model.link.df.loc[model.link.df["link_type"] == "observation", "from_node_id"]
    unlinked = nodes[(nodes["node_type"] == "Observation") & ~nodes.index.isin(linked)]
    groups = unlinked.groupby("meta_waterbeheerder") if authority is None else [(authority, unlinked)]
    graph = _Graph(model)
    time = model.observation.time.df
    count = 0
    for group_authority, group in groups:
        if group.empty:
            continue
        coupling = read_coupling(str(group_authority)).set_index(["MeetreeksC", "Aan/Af"])
        assert coupling.index.is_unique, f"Duplicate series in the koppeltabel of {group_authority}"
        for observation_id, observation in group.iterrows():
            observation_id = int(typing.cast(int, observation_id))
            row = coupling.loc[(observation["name"], observation["meta_aan_af"])]
            node_id, sign, koppeling = _observed_node(model, row, graph, tolerance, moved_tolerance)
            if node_id is not None:
                _link(model, observation_id, node_id, graph)
                if sign == -1 and time is not None:
                    time.loc[time["node_id"] == observation_id, "value"] *= -1
                count += 1
            nodes.loc[observation_id, "meta_koppeling"] = koppeling
    LOG.info(f"Linked {count} of {len(unlinked)} unlinked Observation nodes")
    return model

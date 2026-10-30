"""Add Observation nodes with measured discharges to a model.

The measurement series and the model links they are compared with are taken from the koppeltabel that is also used
by `ribasim_nl.analyse_results`. Each series gets an Observation node at the measurement location, with the measured
values. If the series is measured on a single link, the Observation node observes the `flow_rate` of the connector
node of that link, or else of its Junction. The `flow_rate` of a Junction is the flow through it, the sum of its
outflows. These nodes are found by their type and location, which do not change when links are renumbered. Other Observation nodes are not linked, as a reminder; `meta_koppeling` says why.
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

from ribasim_nl.coupling_level_network import first_non_junction, flow_link_graphs
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
# Operations of the koppeltabel after which the measurement has the sign of the model flow, see ApplySpecificOperation
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


def _is_sum(operation: str | float | None, count: int) -> bool:
    """Whether a koppeltabel operation sums the flows of `count` links, possibly negated."""
    if operation is None or (isinstance(operation, float) and pd.isna(operation)) or operation == "negatief_maken":
        return count == 1
    sum_formula = "+".join(f"link{i + 1}" for i in range(count))
    return operation in ("optellen", "optellen_en_negatief_maken", sum_formula)


def read_coupling(authority: str) -> gpd.GeoDataFrame:
    """The series of an authority, with the nodes to observe of their links.

    :return: a table with columns MeetreeksC, Aan/Af, negate (whether the measurement has the opposite sign of the
        flow_rate), ends (the `LinkEnd` per link, or None if the series is not the sum of the links or a link has no
        node to observe), koppeling (why ends is None) and geometry of the measurement location
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
        koppeling = ""
        if not from_types:
            ends, koppeling = None, "no links in the koppeltabel"
        elif not _is_sum(operation, len(from_types)):
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
    assert model.node.df is not None
    links = model.link.df[model.link.df["link_type"] == "observation"] if model.link.df is not None else []
    LOG.info(f"{authority} has {len(coupling)} Observation nodes, of which {len(links)} are linked")
    return model


class _Graph:
    """Flow neighbors of the nodes of a model, to look past Junctions."""

    def __init__(self, model: Model) -> None:
        assert model.node.df is not None
        assert model.link.df is not None
        node_type = model.node.df["node_type"]
        self.node_types = dict(zip(node_type.index.astype(int), node_type.astype(str), strict=True))
        self.outgoing, self.incoming = flow_link_graphs(model.link.df.reset_index())

    def neighbor(self, node_id: int, upstream: bool) -> int | None:
        """The first neighbor that is not a Junction, if there is one."""
        return first_non_junction(node_id, self.incoming if upstream else self.outgoing, self.node_types)[0]

    def behind_junctions(self, node_id: int, upstream: bool) -> set[int]:
        """The nodes reached through Junctions from a Junction."""
        found, stack = set(), [node_id]
        while stack:
            for _, next_id in (self.incoming if upstream else self.outgoing).get(stack.pop(), []):
                if self.node_types[next_id] == "Junction":
                    stack.append(next_id)
                else:
                    found.add(next_id)
        return found

    def collecting_junction(self, node_ids: set[int], upstream: bool) -> int | None:
        """The Junction that collects the flow of exactly `node_ids`: downstream of them, or upstream if `upstream`."""
        node_id = next(iter(node_ids))
        for _ in range(50):
            links = (self.incoming if upstream else self.outgoing).get(node_id, [])
            if len(links) != 1 or self.node_types[links[0][1]] != "Junction":
                return None
            node_id = links[0][1]
            if self.behind_junctions(node_id, not upstream) == node_ids:
                return node_id
        return None


def _match(
    model: Model, end: LinkEnd, graph: _Graph, tolerance: float, moved_tolerance: float
) -> tuple[int | None, str]:
    """The model node of a `LinkEnd`, or None and why there is none.

    Nodes on top of each other are moved apart, see `ribasim_nl.layout.separate_nodes`, so if there is no node within
    `tolerance`, the nearest node within `moved_tolerance` is used. If there is no node of the type of the koppeltabel,
    an Outlet may be a Pump instead, or the other way around, see `INTERCHANGEABLE_TYPES`.
    """
    assert model.node.df is not None
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
) -> tuple[int | None, str]:
    """The node to observe for a series of `read_coupling`, or None and why there is none."""
    ends = row["ends"]
    if not isinstance(ends, list):
        return None, row["koppeling"]
    matches = [_match(model, end, graph, tolerance, moved_tolerance) for end in ends]
    unmatched = [reason for node_id, reason in matches if node_id is None]
    if unmatched:
        return None, unmatched[0]
    node_ids = {node_id for node_id, _ in matches if node_id is not None}
    if len(node_ids) != len(ends):
        return None, "the same node for multiple links"
    if len(node_ids) == 1:
        return node_ids.pop(), ""
    # the sum of the flows of multiple nodes is the flow of a Junction that combines exactly them, on either side
    for upstream in (False, True):
        junction = graph.collecting_junction(node_ids, upstream=upstream)
        if junction is not None:
            return junction, ""
    return None, f"no Junction combines exactly its {len(ends)} links"


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
    assert model.node.df is not None
    period = slice(pd.Timestamp(model.starttime), pd.Timestamp(model.endtime))

    graph = _Graph(model)
    times: list[pd.DataFrame] = [] if model.observation.time.df is None else [model.observation.time.df]
    for _, row in coupling.iterrows():
        node_id, koppeling = _observed_node(model, row, graph, tolerance, tolerance)
        observation = model.observation.add(
            Node(geometry=row["geometry"], name=row["MeetreeksC"], meta_aan_af=row["Aan/Af"], meta_koppeling=koppeling)
        )
        if node_id is None:
            LOG.warning(f"Not linking the Observation of {row['MeetreeksC']}: {koppeling}")
        else:
            geometry = typing.cast(shapely.Point, model.node.df.at[node_id, "geometry"])
            model.link.add(observation, NodeData(node_id, graph.node_types[node_id], geometry))

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
                    "value": -series.to_numpy() if row["negate"] else series.to_numpy(),
                }
            )
        )

    if times:
        model.observation.time.df = DataFrame[ObservationTimeSchema](pd.concat(times, ignore_index=True))
    return model


def link_observations(model: Model, tolerance: float = 1.0, moved_tolerance: float = 50.0) -> Model:
    """Link the Observation nodes that are not linked yet, for instance in a model coupled from other models.

    Series can be measured on links between models, of which only the coupled model has all nodes.

    :param model: Ribasim model with a meta_waterbeheerder column with the authority of each node
    :param tolerance: maximum distance between the node to observe in the koppeltabel and in the model
    :param moved_tolerance: maximum distance to a node that was moved to separate it from other nodes, see `_match`

    :return: updated Ribasim model
    """
    assert model.node.df is not None
    assert model.link.df is not None
    nodes = model.node.df
    linked = model.link.df.loc[model.link.df["link_type"] == "observation", "from_node_id"]
    unlinked = nodes[(nodes["node_type"] == "Observation") & ~nodes.index.isin(linked)]
    graph = _Graph(model)
    count = 0
    for authority, group in unlinked.groupby("meta_waterbeheerder"):
        coupling = read_coupling(str(authority)).set_index(["MeetreeksC", "Aan/Af"])
        assert coupling.index.is_unique, f"Duplicate series in the koppeltabel of {authority}"
        for observation_id, observation in group.iterrows():
            observation_id = int(typing.cast(int, observation_id))
            row = coupling.loc[(observation["name"], observation["meta_aan_af"])]
            node_id, koppeling = _observed_node(model, row, graph, tolerance, moved_tolerance)
            if node_id is not None:
                geometry = typing.cast(shapely.Point, nodes.at[node_id, "geometry"])
                model.link.add(
                    model.observation[observation_id], NodeData(node_id, graph.node_types[node_id], geometry)
                )
                count += 1
            nodes.loc[observation_id, "meta_koppeling"] = koppeling
    LOG.info(f"Linked {count} of {len(unlinked)} unlinked Observation nodes")
    return model

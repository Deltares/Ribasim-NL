"""Set the unknown capacities of Pumps and Outlets from the area they drain or supply.

This uses the same design discharges and supply as the parametrization of the vrij afwaterend models, see
`ribasim_nl.parametrization.static_data_xlsx.defaults`: per day, a drainage Pump discharges 15 mm and a drainage
Outlet 50 mm over the area it drains, and a supplying Pump or Outlet supplies 4 mm over the area downstream. The
drainage of each water body is split over the drains out of it, and each node is limited by what can flow into and
out of the water bodies around it.
"""

import logging
import math

import networkx as nx
import pandas as pd

from ribasim_nl import Model
from ribasim_nl.control import is_off_state, off_states
from ribasim_nl.downstream import downstream_nodes
from ribasim_nl.parametrization.conversions import round_to_significant_digits
from ribasim_nl.parametrization.static_data_xlsx import defaults
from ribasim_nl.upstream import upstream_nodes

__all__ = ["design_capacities", "known_capacities", "set_design_capacities"]

LOG = logging.getLogger(__name__)


def _mm_per_day(category: str) -> float:
    """Get the design flow rate of a category of the vrij afwaterend parametrization defaults."""
    value = defaults[category]["flow_rate_mm_per_day"]
    assert isinstance(value, int | float), f"{category} should have a numeric flow_rate_mm_per_day"
    return float(value)


# design flow rates in mm/day by node type, see the module docstring
DRAIN_MM_PER_DAY = {"Pump": _mm_per_day("Afvoergemaal"), "Outlet": _mm_per_day("Uitlaat")}
SUPPLY_MM_PER_DAY = {"Pump": _mm_per_day("Aanvoergemaal"), "Outlet": _mm_per_day("Inlaat")}
# `upstream_nodes` stops at inlets and `downstream_nodes` at outlets, while flow_control nodes pass both
SEARCH_FUNCTION = {"drain": "outlet", "supply": "inlet", "flow_control": ""}
# connectors without a capacity, that join Basins into one water body
PASSING_NODE_TYPES = {"Junction", "ManningResistance", "LinearResistance"}


def _flow_graph(model: Model, functions: pd.Series) -> nx.DiGraph:
    """Build the graph of the flow links, with the node type and search function of each node."""
    flow_link_df = model.link.df[model.link.df["link_type"] == "flow"]
    graph = nx.from_pandas_edgelist(flow_link_df, "from_node_id", "to_node_id", create_using=nx.DiGraph)
    node_types = model.node.df["node_type"]
    attributes = {
        node_id: {"node_type": node_types.at[node_id], "function": SEARCH_FUNCTION.get(functions.get(node_id), "")}
        for node_id in graph.nodes
    }
    nx.set_node_attributes(graph, attributes)
    return graph


def design_capacities(model: Model, function_table: pd.DataFrame) -> pd.DataFrame:
    """Compute the design capacity of each Pump and Outlet in `function_table` from the area it drains or supplies.

    A drain node gets its share of the drainage of the water body it comes from, see `_split_drainage`, and a supply
    node the area of the Basins downstream, up to the drain nodes. A flow_control node passes water in both
    directions, and gets the larger of both. A supply node that supplies no Basin, such as one out of the model,
    supplies the area upstream instead. The area of bergend Basins is left out, as it is the same as that of their
    doorgaand Basin. Finally the capacities are limited by the nodes around them, see `_limit_by_neighbours`.

    Parameters
    ----------
    model : Model
        The model, with the Basin areas, and Pump and Outlet static tables with a `meta_known_flow_rate` column.
    function_table : pd.DataFrame
        The function of the Pumps and Outlets by node_id: drain, supply or flow_control, see
        `ribasim_nl.control.add_function_to_peilbeheerst_node_table`.

    Returns
    -------
    pd.DataFrame
        By node_id, the drained and supplied area in m2, the design capacity from that area, and the capacity limited
        by the nodes around it, in m3/s.
    """
    assert model.basin.area.df is not None
    functions = function_table["function"]
    unknown = set(functions) - set(SEARCH_FUNCTION)
    assert not unknown, f"Unknown functions {unknown}, expected {set(SEARCH_FUNCTION)}"
    assert functions.index.is_unique, "Each node should have one function"
    node_types = model.node.df["node_type"].reindex(functions.index)
    assert node_types.isin(DRAIN_MM_PER_DAY).all(), "Only Pumps and Outlets have a capacity"

    graph = _flow_graph(model, functions)
    area_df = model.basin.area.df
    if "meta_categorie" in model.node.df.columns:
        bergend = model.node.df.index[model.node.df["meta_categorie"] == "bergend"]
        area_df = area_df[~area_df["node_id"].isin(bergend)]
    basin_area = area_df.set_index("node_id").area.groupby(level=0).sum()

    def area(node_ids: set[int]) -> float:
        return float(basin_area.reindex(list(node_ids)).sum())

    known = known_capacities(model)
    unknown = functions.index.difference(known.index)
    # mm/day to m/s
    drain_rate = {int(n): DRAIN_MM_PER_DAY[t] / 1000 / 86400 for n, t in zip(node_types.index, node_types, strict=True)}
    supply_rate = {
        int(n): SUPPLY_MM_PER_DAY[t] / 1000 / 86400 for n, t in zip(node_types.index, node_types, strict=True)
    }
    body_of = _water_bodies(graph)
    body_area = basin_area.groupby(basin_area.index.map(body_of)).sum()
    drains = functions.index[functions != "supply"]
    drained_area, minimum_area = _split_drainage(graph, body_of, body_area, drains, known, drain_rate)

    rows = []
    for node_id, function in zip(functions.index.astype(int), functions, strict=True):
        drained = drained_area.get(node_id, 0.0)
        supplied = area(downstream_nodes(graph, node_id, stop_at_outlet=True)) if function != "drain" else 0.0
        if function == "supply" and supplied == 0:
            supplied = area(upstream_nodes(graph, node_id, stop_at_inlet=True))
        drainage = max(drained, minimum_area.get(node_id, 0.0)) * drain_rate[node_id]
        design = max(drainage, supplied * supply_rate[node_id])
        rows.append((node_id, drained, supplied, design))
    capacities = pd.DataFrame(rows, columns=["node_id", "drained_area", "supplied_area", "design"]).set_index("node_id")
    capacity = _limit_by_neighbours(
        graph, body_of, body_area, capacities["design"], known, unknown, drain_rate, supply_rate
    )
    capacities["capacity"] = capacity.reindex(capacities.index).map(round_to_significant_digits)
    return capacities


def known_capacities(model: Model) -> pd.Series:
    """Get the largest max_flow_rate by node_id of the Pumps and Outlets with `meta_known_flow_rate`."""
    known = []
    for static_df in (model.pump.static.df, model.outlet.static.df):
        assert static_df is not None and "meta_known_flow_rate" in static_df, "Static tables need meta_known_flow_rate"
        assert static_df["meta_known_flow_rate"].notna().all(), "meta_known_flow_rate should be set for all nodes"
        node_known = static_df.groupby("node_id")["meta_known_flow_rate"].transform("any")
        known.append(static_df[node_known].groupby("node_id")["max_flow_rate"].max())
    known_capacity = pd.concat(known)
    missing = known_capacity.index[known_capacity.isna()]
    assert missing.empty, f"Nodes {missing.tolist()} have a known capacity without max_flow_rate"
    return known_capacity


def _water_bodies(graph: nx.DiGraph) -> dict[int, int]:
    """Number the groups of Basins that are joined by connectors without a capacity, by node_id of their members."""
    passing = graph.subgraph(
        n for n, node_type in graph.nodes(data="node_type") if node_type == "Basin" or node_type in PASSING_NODE_TYPES
    )
    return {
        node_id: i
        for i, component in enumerate(nx.weakly_connected_components(passing))
        if any(graph.nodes[node_id]["node_type"] == "Basin" for node_id in component)
        for node_id in component
    }


def _split_drainage(
    graph: nx.DiGraph,
    body_of: dict[int, int],
    body_area: pd.Series,
    drains: pd.Index,
    known: pd.Series,
    drain_rate: dict[int, float],
) -> tuple[dict[int, float], dict[int, float]]:
    """Split the drainage of each water body over the drains out of it, in m2 of drained area.

    A water body drains its own area and what the drains into it carry. The drains with a known capacity carry up to
    that capacity, and the other drains share the rest equally. Drains that flow out of a water body into a boundary
    take the drainage out of the model. Unknown drains in a loop that no unknown drain leaves would pass the drainage
    around forever, so in the water bodies of such a loop with known drains, these carry all. This repeats until the
    drained areas no longer change.

    Returns
    -------
    tuple[dict[int, float], dict[int, float]]
        The drained area of each drain, and the minimum drained area of the unknown drains: their share of the own
        area of the water body they come from, so they keep a capacity when the known drains carry all.
    """
    from_body: dict[int, int] = {}
    to_body: dict[int, int] = {}
    for node_id in drains.astype(int):
        from_bodies = {body_of.get(n, -1) for n in graph.predecessors(node_id)}
        to_bodies = {body_of.get(n, -1) for n in graph.successors(node_id)}
        if len(from_bodies) != 1 or -1 in from_bodies:
            continue
        from_body[node_id] = from_bodies.pop()
        if len(to_bodies) == 1 and -1 not in to_bodies:
            to_body[node_id] = to_bodies.pop()
    outflows: dict[int, list[int]] = {}
    for node_id, body in from_body.items():
        outflows.setdefault(body, []).append(node_id)
    # the area that a known capacity drains at the design rate
    known_capacity = dict(zip(known.index.astype(int), known.astype(float), strict=True))
    known_area = {n: known_capacity[n] / drain_rate[n] for n in from_body if n in known_capacity}
    own_area = {body: float(body_area.get(body, 0.0)) for body in outflows}
    # unknown drains in a loop that no unknown drain leaves would pass the drainage around forever, so where such a
    # loop has known drains, these carry all
    unknown_links = nx.DiGraph((from_body[n], to_body[n]) for n in to_body if n not in known_area)
    trapped: set[int] = set()
    for loop in nx.strongly_connected_components(unknown_links):
        loop_drains = [n for n, body in from_body.items() if body in loop and n not in known_area]
        if all(to_body.get(n) in loop for n in loop_drains):
            trapped.update(loop_drains)

    drained = dict.fromkeys(from_body, 0.0)
    for _ in range(10_000):
        inflow = dict.fromkeys(outflows, 0.0)
        for node_id, body in to_body.items():
            if body in inflow:
                inflow[body] += drained[node_id]
        new = {}
        for body, nodes in outflows.items():
            total = own_area[body] + inflow[body]
            known_nodes = [n for n in nodes if n in known_area]
            unknown_nodes = [n for n in nodes if n not in known_area and not (known_nodes and n in trapped)]
            known_total = sum(known_area[n] for n in known_nodes)
            # the known drains carry up to their capacity, or all if there are no other drains
            scale = 0.0 if known_total == 0 else total / known_total
            if unknown_nodes:
                scale = min(scale, 1.0)
            for n in known_nodes:
                new[n] = known_area[n] * scale
            rest = max(total - known_total, 0.0)
            for n in unknown_nodes:
                new[n] = rest / len(unknown_nodes)
            for n in nodes:
                new.setdefault(n, 0.0)
        if all(math.isclose(new[n], drained[n], rel_tol=1e-9, abs_tol=1.0) for n in drained):
            minimum = {
                n: own_area[from_body[n]] / len(outflows[from_body[n]]) for n in from_body if n not in known_area
            }
            return new, minimum
        drained = new
    raise AssertionError("The split of the drainage did not converge, is there a loop without a drain out of it?")


def _limit_by_neighbours(
    graph: nx.DiGraph,
    body_of: dict[int, int],
    body_area: pd.Series,
    design: pd.Series,
    known: pd.Series,
    unknown: pd.Index,
    drain_rate: dict[int, float],
    supply_rate: dict[int, float],
) -> pd.Series:
    """Limit the unknown capacities to what can flow into and out of the water bodies around the nodes.

    Basins joined by resistances or Junctions count as one water body. A node carries no more than the supply of
    the water body it flows into, at `supply_rate` in m/s, plus the Pumps and Outlets out of it. It also carries no
    more than the drainage of the water body it comes from, at `drain_rate` in m/s, plus the Pumps and Outlets into
    it. Nodes straight between both water bodies are left out, so that nodes in opposite directions do not keep up
    each other's capacity. Starting from the design capacities, this repeats until no capacity changes, so known
    capacities also limit nodes further away. Flows from or to a boundary, or through another node without a
    capacity, do not limit.
    """
    designed = dict(zip(design.index.astype(int), design.astype(float), strict=True))
    capacity = designed | dict(zip(known.index.astype(int), known.astype(float), strict=True))

    # the Pumps and Outlets into and out of each water body, or None if water also flows in or out otherwise
    inflows: dict[int, list[int] | None] = {}
    outflows: dict[int, list[int] | None] = {}
    for member, body in body_of.items():
        for connections, neighbours in ((inflows, graph.predecessors), (outflows, graph.successors)):
            nodes = connections.setdefault(body, [])
            for node_id in neighbours(member):
                if body_of.get(node_id) == body or nodes is None:
                    continue
                if node_id in capacity:
                    nodes.append(node_id)
                else:
                    connections[body] = nodes = None

    def bodies(node_ids: list[int]) -> set[int] | None:
        """The water bodies of the nodes, or None if not all of them are in one."""
        if not node_ids or not all(node_id in body_of for node_id in node_ids):
            return None
        return {body_of[node_id] for node_id in node_ids}

    def area(node_bodies: set[int]) -> float:
        return sum(body_area.get(body, 0.0) for body in node_bodies)

    # the nodes that limit each node, with the supply or drainage of the water body
    limits: dict[int, list[tuple[list[int], float]]] = {}
    for node_id in unknown:
        from_bodies = bodies(list(graph.predecessors(node_id)))
        to_bodies = bodies(list(graph.successors(node_id)))
        limits[node_id] = []
        if to_bodies is not None and all(outflows[body] is not None for body in to_bodies):
            outs = [out for body in to_bodies for out in outflows[body] or ()]
            outs = [
                out
                for out in outs
                if not from_bodies or not (bodies(list(graph.successors(out))) or set()) & from_bodies
            ]
            limits[node_id].append((outs, supply_rate[node_id] * area(to_bodies)))
        if from_bodies is not None and all(inflows[body] is not None for body in from_bodies):
            ins = [inflow for body in from_bodies for inflow in inflows[body] or ()]
            ins = [i for i in ins if not to_bodies or not (bodies(list(graph.predecessors(i))) or set()) & to_bodies]
            limits[node_id].append((ins, drain_rate[node_id] * area(from_bodies)))

    for _ in range(len(unknown) + 1):
        changed = False
        for node_id, node_limits in limits.items():
            limit = min((base + sum(capacity[n] for n in nodes) for nodes, base in node_limits), default=math.inf)
            new = min(designed[node_id], limit) if limit > 0 else designed[node_id]
            if not math.isclose(new, capacity[node_id]):
                capacity[node_id] = new
                changed = True
        if not changed:
            return pd.Series(capacity)
    raise AssertionError("The limits of the capacities did not converge")


def _set_unknown_capacities(static_df: pd.DataFrame, capacities: pd.Series) -> None:
    """Set the capacity of the unknown nodes, keeping the ratios between their control states and closed states."""
    node_capacity = static_df.groupby("node_id")["max_flow_rate"].transform("max")
    known = static_df.groupby("node_id")["meta_known_flow_rate"].transform("any")
    updates = static_df["node_id"].map(capacities)
    mask = ~known & updates.notna()
    off = is_off_state(static_df, off_states(static_df))
    # a zero limit on a node with a positive capacity is an intentional state-specific closure
    fraction = (static_df["max_flow_rate"] / node_capacity.where(node_capacity > 0)).fillna(1.0)
    fraction.loc[off & static_df["max_flow_rate"].eq(0)] = 0.0
    static_df.loc[mask, "max_flow_rate"] = (updates * fraction).loc[mask]
    missing = static_df.loc[static_df["max_flow_rate"].isna(), "node_id"].unique()
    assert len(missing) == 0, f"Nodes {missing.tolist()} have neither a known nor a design max_flow_rate"
    static_df["flow_rate"] = static_df["max_flow_rate"].where(~off, 0.0)


def set_design_capacities(model: Model, function_table: pd.DataFrame) -> pd.DataFrame:
    """Set the unknown capacities of the Pumps and Outlets to their design capacity, see `design_capacities`.

    Capacities are known where `meta_known_flow_rate` is set in the static table; these are kept, as are those of
    nodes that drain or supply no Basin. The flow_rate of all nodes is set to their max_flow_rate, except in the
    control states that switch them off.

    Parameters
    ----------
    model : Model
        The model, with Pump and Outlet static tables with a `meta_known_flow_rate` column.
    function_table : pd.DataFrame
        The function of the Pumps and Outlets by node_id: drain, supply or flow_control, see
        `ribasim_nl.control.add_function_to_peilbeheerst_node_table`.

    Returns
    -------
    pd.DataFrame
        The design capacities, see `design_capacities`, and whether each capacity was known.
    """
    capacities = design_capacities(model, function_table)
    zero = capacities.index[capacities["capacity"] <= 0]
    capacities["known"] = capacities.index.isin(known_capacities(model).index)
    static_dfs = [df for df in (model.pump.static.df, model.outlet.static.df) if df is not None]
    # nodes between boundaries affect no Basin, and keep their capacity
    unknown_zero = zero.difference(capacities.index[capacities["known"]])
    if not unknown_zero.empty:
        LOG.warning(f"Nodes {unknown_zero.tolist()} drain or supply no Basin, keeping their capacity")
    for static_df in static_dfs:
        _set_unknown_capacities(static_df, capacities["capacity"].drop(zero))
    unknown = capacities[~capacities["known"]].drop(unknown_zero)
    LOG.info(
        f"Set {len(unknown)} design capacities, from {unknown['capacity'].min():.3g} to "
        f"{unknown['capacity'].max():.3g} m3/s, total {unknown['capacity'].sum():.3g} m3/s"
    )
    return capacities

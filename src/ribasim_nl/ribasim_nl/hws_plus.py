"""The hws_plus model: the main water system (HWS) of a run of the coupled model, with its exchanges as boundaries.

The exchanges are those with the regional water systems in the run of the coupled LHM model. The model runs much faster than the coupled model, and is more accurate than the HWS model of `hws_transient`, which lacks
the regional water systems. In return, the HWS no longer affects the exchanges: an inlet takes the water it took in
the coupled run, also when the HWS is lower.

The model is cut through the structures between a Basin of the HWS and a regional Basin, along the links that Ribasim
makes of the links through Junctions, see `ribasim_nl.flow_paths.Routes`. Each of these structures keeps its node ID
and location, and becomes:

- a FlowBoundary with the daily flow of the run, if water flows into the HWS;
- a Pump with the daily flow of the run into a Terminal, in place of the regional Basin, if water flows out of it.

The other nodes of the HWS keep their IDs too, so the model can be compared with the coupled one node by node.

The FlowBoundaries carry the daily concentrations of all substances of the regional Basin they get their water from,
so the water of the HWS has the same sources as in the coupled run. A tracer per authority, named after it in
`ribasim_nl.aquo.waterbeheercode`, tells where the water comes from: the water of the regional water systems by the
authority of the Basin it comes from, and all other water as Rijkswaterstaat.
"""

import shutil
from collections import defaultdict
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from pandera.typing import DataFrame
from pandera.typing.geopandas import GeoDataFrame
from ribasim.geometry.node import NodeSchema
from ribasim.input_base import TableModel
from ribasim.schemas import (
    BasinConcentrationSchema,
    BasinConcentrationStateSchema,
    BasinTimeSchema,
    FlowBoundaryConcentrationSchema,
    FlowBoundaryTimeSchema,
    LevelBoundaryConcentrationSchema,
    PumpTimeSchema,
)

from ribasim_nl.aquo import authority_by_code, authority_code, waterbeheercode
from ribasim_nl.flow_paths import Routes
from ribasim_nl.model import Model, set_df_unvalidated

HWS = waterbeheercode["Rijkswaterstaat"]
CONNECTORS = frozenset(
    {"Outlet", "Pump", "ManningResistance", "LinearResistance", "TabulatedRatingCurve", "UserDemand"}
)
BOUNDARIES = frozenset({"FlowBoundary", "LevelBoundary", "Terminal"})
# A flow against the direction of an exchange of at most this is numerical noise
FLOW_TOLERANCE = 1e-6  # m3/s


@dataclass(frozen=True)
class Exchange:
    """A structure between a node of the HWS and a regional Basin."""

    node_id: int
    basin_id: int
    # the link of the structure on the side of the HWS, and on the side of the regional Basin
    hws_link_id: int
    regional_link_id: int


def cut(model: Model, routes: Routes) -> tuple[set[int], list[Exchange]]:
    """The nodes of the HWS, and the structures between it and the regional Basins.

    The HWS consists of the Basins and boundaries of Rijkswaterstaat, the boundaries that flow directly into its
    Basins, the structures between these and with other boundaries, the Junctions of the trees of its Basins, and the
    control and observation nodes that only have links with these.
    """
    node_type = model.node.df["node_type"]
    junctions = set(node_type.index[node_type == "Junction"])
    assert junctions == routes.plain, (
        f"Junctions with a Junction / static are not supported: {junctions - routes.plain}"
    )
    ends: defaultdict[int, tuple[set[int], set[int]]] = defaultdict(lambda: (set(), set()))
    for from_node_id, to_node_id in routes.simplified_links():
        ends[from_node_id][1].add(to_node_id)
        ends[to_node_id][0].add(from_node_id)

    stores = node_type.index[node_type.isin(BOUNDARIES | {"Basin"})]
    hws = set(stores[authority_code(stores) == HWS])
    hws |= {
        node_id
        for node_id in stores[node_type[stores].isin(BOUNDARIES).to_numpy()]
        if any(node_type[n] == "Basin" for n in (ends[node_id][0] | ends[node_id][1]) & hws)
    }
    kept = set(hws)
    exchanges = []
    for node_id in sorted(node_type.index[node_type.isin(CONNECTORS)]):
        upstream, downstream = ends[node_id]
        assert len(upstream) == len(downstream) == 1, f"{node_type[node_id]} #{node_id} does not connect two nodes"
        (up,), (down,) = upstream, downstream
        if up not in hws and down not in hws:
            continue
        other = down if up in hws else up
        if other in hws or node_type[other] in BOUNDARIES:
            kept |= {node_id, other}
            continue
        assert node_type[other] == "Basin", f"{node_type[node_id]} #{node_id} connects the HWS with {other}"
        hws_links = routes.links_at(node_id, upstream=up in hws)
        regional_links = routes.links_at(node_id, upstream=up not in hws)
        assert len(hws_links) == len(regional_links) == 1, f"{node_type[node_id]} #{node_id} has parallel links"
        exchanges.append(Exchange(node_id, other, hws_links[0], regional_links[0]))

    for junction_id in junctions:
        basin_id = routes.tree_basin(junction_id)
        assert basin_id is not None, f"Junction #{junction_id} is not in the tree of a single Basin"
        if basin_id in kept:
            kept.add(junction_id)

    # control and observation nodes, by the nodes at the other end of their links
    links = model.link.df
    links = links[links["link_type"] != "flow"]
    pairs = pd.concat(
        [
            pd.DataFrame({"node_id": links["from_node_id"], "other": links["to_node_id"]}),
            pd.DataFrame({"node_id": links["to_node_id"], "other": links["from_node_id"]}),
        ]
    )
    others = node_type.index[~node_type.isin(CONNECTORS | BOUNDARIES | {"Basin", "Junction"})]
    pairs = pairs[pairs["node_id"].isin(others)]
    all_kept = pairs["other"].isin(kept).groupby(pairs["node_id"]).all()
    kept |= set(all_kept.index[all_kept])
    return kept, exchanges


def into_hws(model: Model, exchanges: list[Exchange], flow: xr.DataArray) -> pd.DataFrame:
    """The flow into the HWS through each exchange, by time, from the flow rates of the run by time and link_id."""
    flow = flow.assign_coords(link_id=flow["link_id"].astype(np.int64))
    link_ids = [e.hws_link_id for e in exchanges]
    q = flow.sel(link_id=link_ids).to_pandas()
    # the flow of a link from the structure to the HWS flows into it
    sign = np.where(model.link.df.loc[link_ids, "from_node_id"].to_numpy() == [e.node_id for e in exchanges], 1, -1)
    return (q * sign).set_axis([e.node_id for e in exchanges], axis=1)


def flows_in(model: Model, exchange: Exchange, flow: pd.Series) -> bool:
    """Whether an exchange flows into the HWS, by its flow into the HWS, or by its links if there is no flow."""
    if (flow < -FLOW_TOLERANCE).any() and (flow > FLOW_TOLERANCE).any():
        raise ValueError(f"Node #{exchange.node_id} exchanges water with the HWS in both directions, from {flow.min()}")
    if (flow > FLOW_TOLERANCE).any():
        return True
    if (flow < -FLOW_TOLERANCE).any():
        return False
    return bool(model.link.df.at[exchange.hws_link_id, "from_node_id"] == exchange.node_id)


def changes(values: np.ndarray) -> np.ndarray:
    """Whether each value along the first axis differs from the previous one, or is the first or last.

    Boundaries keep a flow rate or concentration until the next row, so rows that repeat the previous value of a node
    can be left out without changing the model. The last row is kept, since the Ribasim core fails on a FlowBoundary
    with a single row.
    """
    changed = np.ones(values.shape, dtype=bool)
    changed[1:-1] = values[1:-1] != values[:-2]
    return changed


def boundary_flows(flow: pd.DataFrame) -> pd.DataFrame:
    """The table of the flow rates of boundaries by time and node_id, without repeated values, see `changes`."""
    values = flow.to_numpy()
    assert not np.isnan(values).any(), "Missing flow rates"
    i, j = np.nonzero(changes(values))
    return pd.DataFrame({"node_id": flow.columns[j], "time": flow.index[i], "flow_rate": values[i, j]})


def boundary_concentrations(concentration: xr.DataArray, sources: pd.Series) -> pd.DataFrame:
    """The concentrations of boundaries, the index of `sources`, from those of the Basins they take water from.

    `concentration.nc` gives the concentrations at the end of each day; these hold for that day. Repeated values are
    left out, see `changes`.
    """
    basin_ids = np.unique(sources.to_numpy())
    positions = pd.Index(concentration["node_id"].to_numpy()).get_indexer(pd.Index(basin_ids))
    assert (positions >= 0).all(), f"No concentrations of Basins {basin_ids[positions < 0].tolist()}"
    substances = concentration["substance"].to_numpy().astype(str)
    # by substance, since selecting Basins from the compressed NetCDF file is slow
    values = np.stack(
        [concentration.sel(substance=s).transpose("time", "node_id").to_numpy()[:, positions] for s in substances],
        axis=1,
    )
    assert not np.isnan(values).any(), "Missing concentrations"
    i, j, k = np.nonzero(changes(values))
    by_basin = pd.DataFrame(
        {
            "basin_id": basin_ids[k],
            "time": concentration["time"].to_numpy()[i],
            "substance": substances[j],
            "concentration": values[i, j, k],
        }
    )
    targets = pd.DataFrame({"node_id": sources.index, "basin_id": sources.to_numpy()})
    return targets.merge(by_basin, on="basin_id").drop(columns="basin_id")


def tracer_rows(node_ids: pd.Index, codes: np.ndarray, time: pd.Timestamp) -> pd.DataFrame:
    """Concentration 1 of the tracer of the authority with each waterbeheercode in `codes`, for each node."""
    names = [authority_by_code[int(code)] for code in codes]
    return pd.DataFrame({"node_id": node_ids, "time": time, "substance": names, "concentration": 1.0})


def concat(table: TableModel, *dfs: pd.DataFrame, lhm_fractions: Collection[str] = ()) -> pd.DataFrame:
    """The rows of a table with those of `dfs`, with the column `meta_lhm_fraction` of the table if it has one."""
    new = pd.concat(dfs, ignore_index=True)
    if table.df is None:
        return new
    if "meta_lhm_fraction" in table.df.columns:
        new["meta_lhm_fraction"] = new["substance"].isin(list(lhm_fractions))
    return pd.concat([table.df, new], ignore_index=True)


def lhm_fractions(model: Model) -> set[str]:
    """The substances that are fractions of the LHM, see `ribasim_nl.assign_lhm_fractions`."""
    tables = [model.basin.concentration.df, model.flow_boundary.concentration.df, model.level_boundary.concentration.df]
    return {
        str(substance)
        for df in tables
        if df is not None and "meta_lhm_fraction" in df.columns
        for substance in df.loc[df["meta_lhm_fraction"].astype(bool), "substance"]
    }


def terminal_routes(routes: Routes, outflows: list[Exchange]) -> dict[int, int]:
    """The links from each outflow to its regional Basin through the tree of that Basin, with the node they point to.

    The links of a Basin tree may point either way, see `Routes`. Once the Basin is a Terminal, the Junctions only
    pass on flow along the links, so these then point to the Basin.
    """
    by_basin: defaultdict[int, list[Exchange]] = defaultdict(list)
    for e in outflows:
        by_basin[e.basin_id].append(e)
    towards: dict[int, int] = {}
    for basin_id, exchanges in sorted(by_basin.items()):
        paths = routes.paths(basin_id)
        for e in exchanges:
            node_id = basin_id
            for link_id in paths[e.regional_link_id]:
                towards[link_id] = node_id
                node_id = routes.other_node(link_id, node_id)
            assert node_id == e.node_id, f"The route of Basin #{basin_id} to #{e.node_id} ends at #{node_id}"
    return towards


def hws_plus(model: Model, flow: xr.DataArray, concentration: xr.DataArray) -> Model:
    """Cut the HWS out of a run of the coupled model, with the exchanges in the run as boundaries, see the module.

    :param model: the coupled model, which is changed
    :param flow: the flow rates of the run by time and link_id, as in `flow.nc`
    :param concentration: the concentrations of the run by time, substance and node_id, as in `concentration.nc`
    """
    routes = Routes(model)
    kept, exchanges = cut(model, routes)
    flow_in = into_hws(model, exchanges, flow)
    is_in = {e.node_id: flows_in(model, e, flow_in[e.node_id]) for e in exchanges}
    inflows = [e for e in exchanges if is_in[e.node_id]]
    outflows = [e for e in exchanges if not is_in[e.node_id]]
    terminals = sorted({e.basin_id for e in outflows})
    towards = terminal_routes(routes, outflows)
    fractions = lhm_fractions(model)
    start = pd.Timestamp(model.starttime)

    links = model.link.df
    route_junctions = set(links.loc[list(towards), ["from_node_id", "to_node_id"]].to_numpy().ravel()) - {
        *terminals,
        *(e.node_id for e in outflows),
    }
    nodes = kept | {e.node_id for e in exchanges} | set(terminals) | route_junctions
    in_route = links.index.isin(list(towards))
    dropped = (
        ~(links["from_node_id"].isin(nodes) & links["to_node_id"].isin(nodes))
        | links.index.isin([e.regional_link_id for e in inflows])
        | ((links["from_node_id"].isin(route_junctions) | links["to_node_id"].isin(route_junctions)) & ~in_route)
    )
    model.remove_links(links.index[dropped].tolist())
    for node_model in model._nodes():
        for table in node_model._tables():
            df = table.df
            assert df is not None
            if "node_id" in df.columns:
                df = df[df["node_id"].isin(kept)]
                set_df_unvalidated(table, None if df.empty else df)
    for table in (model.discrete_control.variable, model.continuous_control.variable):
        if table.df is not None:
            assert table.df["listen_node_id"].isin(kept).all(), f"{table.tablename()} listens to removed nodes"
    node_df = model.node.df.loc[sorted(nodes)].copy()
    node_df.loc[[e.node_id for e in inflows], "node_type"] = "FlowBoundary"
    node_df.loc[[e.node_id for e in outflows], "node_type"] = "Pump"
    node_df.loc[terminals, "node_type"] = "Terminal"
    authority = pd.Series(authority_code(node_df.index), index=node_df.index)
    for e in exchanges:
        authority[e.node_id] = int(authority_code(e.basin_id))
    converted = [e.node_id for e in exchanges] + terminals
    node_df.loc[converted, "meta_couple_authority"] = authority[converted].map(authority_by_code)
    model.node.df = GeoDataFrame[NodeSchema](node_df)

    # the links with the HWS point into a FlowBoundary, and out of a Pump, and the routes to the Terminals into them
    for e in exchanges:
        if (model.link.df.at[e.hws_link_id, "from_node_id"] == e.node_id) != is_in[e.node_id]:
            model.reverse_link(link_id=e.hws_link_id)
    for link_id, node_id in towards.items():
        if model.link.df.at[link_id, "to_node_id"] != node_id:
            model.reverse_link(link_id=link_id)

    inflow = flow_in[[e.node_id for e in inflows]].clip(lower=0.0)
    outflow = (-flow_in[[e.node_id for e in outflows]]).clip(lower=0.0)
    model.flow_boundary.time.df = DataFrame[FlowBoundaryTimeSchema](
        concat(model.flow_boundary.time, boundary_flows(inflow))
    )
    model.pump.time.df = DataFrame[PumpTimeSchema](concat(model.pump.time, boundary_flows(outflow)))

    sources = pd.Series({e.node_id: e.basin_id for e in inflows})
    flow_boundaries = model.flow_boundary.node.df.index
    model.flow_boundary.concentration.df = DataFrame[FlowBoundaryConcentrationSchema](
        concat(
            model.flow_boundary.concentration,
            tracer_rows(flow_boundaries, authority[flow_boundaries].to_numpy(), start),
            boundary_concentrations(concentration, sources),
            lhm_fractions=fractions,
        )
    )
    level_boundaries = model.level_boundary.node.df.index
    if len(level_boundaries) > 0:
        model.level_boundary.concentration.df = DataFrame[LevelBoundaryConcentrationSchema](
            concat(
                model.level_boundary.concentration,
                tracer_rows(level_boundaries, authority[level_boundaries].to_numpy(), start),
                lhm_fractions=fractions,
            )
        )

    # the water of the HWS itself: its rain, drainage and initial water
    basins = model.basin.node.df.index
    assert (authority[basins] == HWS).all(), "The HWS has regional Basins"
    hws_rows = tracer_rows(basins, authority[basins].to_numpy(), start)
    model.basin.concentration.df = DataFrame[BasinConcentrationSchema](
        concat(
            model.basin.concentration,
            hws_rows.drop(columns="concentration").assign(drainage=1.0, precipitation=1.0, surface_runoff=1.0),
        )
    )
    model.basin.concentration_state.df = DataFrame[BasinConcentrationStateSchema](
        concat(model.basin.concentration_state, hws_rows.drop(columns="time"))
    )
    return model


def build_hws_plus(run_toml: Path, output_toml: Path) -> Model:
    """Write the hws_plus model of the run of the coupled model at `run_toml`, see `hws_plus`."""
    model = Model.read(run_toml, external=False)
    basin_time = model.basin.time.filepath
    assert basin_time is not None and basin_time.suffix == ".nc", f"Expected a NetCDF Basin / time, not {basin_time}"
    results_dir = run_toml.parent / model.results_dir
    with (
        xr.open_dataset(results_dir / "flow.nc") as flow,
        xr.open_dataset(results_dir / "concentration.nc") as concentration,
    ):
        model = hws_plus(model, flow["flow_rate"], concentration["concentration"])
    # the Basin / time of the HWS only, of the Basins that have one, from the NetCDF file with that of all Basins
    with xr.open_dataset(run_toml.parent / model.input_dir / basin_time) as ds:
        hws = ds["node_id"].isin(model.basin.node.df.index)
        time = ds.sel(node_id=hws, time=slice(model.starttime, model.endtime))
        model.basin.time.df = DataFrame[BasinTimeSchema](time.to_dataframe().reset_index())
    model.basin.time.filepath = basin_time
    shutil.rmtree(output_toml.parent, ignore_errors=True)
    model.write(output_toml)
    model.validate_ribasim_nl()
    return model

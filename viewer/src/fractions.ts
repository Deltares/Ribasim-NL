/**
 * The water of a Basin and of the flow over a link by its source, as fractions of a tracer set, for the stacked charts
 * of the panel. This module has only type imports and erasable syntax, so Node runs its tests on it as it is, see
 * fractions.test.ts.
 */

/**
 * A set of tracers that labels all water by its source, see `ribasim_nl.webmap.tracer_sets`. All water has one label
 * at most, so the tracers add up to Continuity, except for the water without a label, see `OTHER`.
 */
export interface TracerSet {
  name: string;
  /** In the order of the stacked charts, from the bottom */
  tracers: string[];
  /** The tracer that labels the water of each FlowBoundary and LevelBoundary node, by node id */
  boundaries: Record<string, string>;
  /** The tracers that are a tracer of the results minus others, which label part of its water */
  derived?: Record<string, { tracer: string; minus: string[] }>;
}

/** The layer of the water without a label in a set, which takes up the rest of Continuity */
export const OTHER = "Other";
/** The tracer of the water that the Basins have at the start */
export const INITIAL = "Initial";
/** The tracer of all water, which the layers of a set add up to */
export const CONTINUITY = "Continuity";
/** Nodes with one flow in and out, see https://ribasim.org/reference/ */
export const CONNECTOR_TYPES = new Set(["TabulatedRatingCurve", "Outlet", "Pump", "LinearResistance", "ManningResistance"]);
/** Nodes whose outflow is the water of their inflow: a UserDemand returns water it takes */
const PASSING_TYPES = new Set([...CONNECTOR_TYPES, "UserDemand"]);
const BOUNDARY_TYPES = new Set(["FlowBoundary", "LevelBoundary"]);

/** Where the water comes from that flows out of a node over a link. */
export type Origin =
  | { kind: "basin"; id: number }
  | { kind: "boundary"; id: number }
  /** A node that mixes the water that flows into it, see `mixInputs`, which flows out of it over `link` */
  | { kind: "mix"; id: number; link: number }
  /** A node whose water cannot be traced, like a Terminal, which only takes water */
  | { kind: "untraced"; id: number };

/** A flow link of a node, with the node at its other end. */
export interface FlowNeighbor {
  node: number;
  link: number;
  /** 1 if a positive flow over the link flows into the node whose neighbor this is, -1 if out of it */
  inflow: 1 | -1;
}

/** The flow links of a model. */
export interface FlowGraph {
  /** The node type, or undefined if the node is not in the model */
  nodeType(node: number): string | undefined;
  /** Whether a node is a Junction in the Junction / static table, which divides the flow it receives */
  inStatic(node: number): boolean;
  neighbors(node: number): readonly FlowNeighbor[];
  /** The nodes a flow link connects, or undefined if it is no flow link of the model */
  ends(link: number): { from: number; to: number } | undefined;
}

/**
 * Whether a node is a Junction that is not in the Junction / static table. Ribasim removes these, and connects their
 * neighbors directly, see https://ribasim.org/reference/node/junction.html#junctions-not-in-the-static-table.
 */
const isPlain = (graph: FlowGraph, node: number) => graph.nodeType(node) === "Junction" && !graph.inStatic(node);

/** The Basins and other nodes next to the connected plain Junctions of `start`, walking around `skipLink`. */
function walkJunctions(graph: FlowGraph, start: number, skipLink?: number): { basins: Set<number>; others: FlowNeighbor[] } {
  const basins = new Set<number>();
  const others: FlowNeighbor[] = [];
  const seen = new Set([start]);
  const stack = [start];
  while (stack.length) {
    for (const neighbor of graph.neighbors(stack.pop()!)) {
      if (neighbor.link === skipLink) continue;
      if (graph.nodeType(neighbor.node) === "Basin") basins.add(neighbor.node);
      else if (!isPlain(graph, neighbor.node)) others.push(neighbor);
      else if (!seen.has(neighbor.node)) {
        seen.add(neighbor.node);
        stack.push(neighbor.node);
      }
    }
  }
  return { basins, others };
}

/**
 * The Basin of the group of connected plain Junctions of a plain Junction, or null if the group does not have exactly
 * one. A group with one Basin connects each of its other neighbors to that Basin, see
 * https://ribasim.org/reference/node/junction.html#a-junction-tree-with-one-basin, so the water that flows out of
 * the group is that of its Basin. Other groups pass on the water where the links take it, see `mixInputs`.
 */
function groupBasin(graph: FlowGraph, junction: number): number | null {
  const { basins } = walkJunctions(graph, junction);
  return basins.size === 1 ? [...basins][0] : null;
}

/**
 * Where the water comes from that flows out of a node over a link: a Basin, a boundary, or a node that mixes water,
 * looking past connector nodes and UserDemands, which pass on the water they take, and past the plain Junctions of a
 * group with one Basin.
 */
export function origin(graph: FlowGraph, node: number, link: number, passed = new Set<number>()): Origin {
  const type = graph.nodeType(node);
  if (type === "Basin") return { kind: "basin", id: node };
  if (type !== undefined && BOUNDARY_TYPES.has(type)) return { kind: "boundary", id: node };
  if (type === "Junction") {
    const basin = graph.inStatic(node) ? null : groupBasin(graph, node);
    return basin === null ? { kind: "mix", id: node, link } : { kind: "basin", id: basin };
  }
  if (type !== undefined && PASSING_TYPES.has(type) && !passed.has(node)) {
    passed.add(node);
    const others = graph.neighbors(node).filter((neighbor) => neighbor.link !== link);
    if (others.length === 1) return origin(graph, others[0].node, others[0].link, passed);
    // A UserDemand can take water from several nodes, and returns the mix
    if (type === "UserDemand") return { kind: "mix", id: node, link };
  }
  return { kind: "untraced", id: node };
}

/**
 * The links of a node that mixes water, over which water can flow into it, for the water that flows out of it over
 * `link`. A Junction in the Junction / static table receives flow over the links that point into it, and mixes it like
 * the core, see https://ribasim.org/reference/node/junction.html#tracers, and so does a UserDemand. A plain Junction
 * in a group without a single Basin connects its neighbors upstream with those downstream, so its other links can
 * bring water.
 */
export function mixInputs(graph: FlowGraph, node: number, link: number): FlowNeighbor[] {
  const neighbors = graph.neighbors(node);
  if (graph.inStatic(node) || graph.nodeType(node) === "UserDemand") return neighbors.filter(({ inflow }) => inflow === 1);
  return neighbors.filter((neighbor) => neighbor.link !== link);
}

/**
 * A part of the flow over a link: the flow over `link` times `sign`, which is positive in the direction of the
 * charted link, with the water of `forward` where it is positive and of `backward` where it is negative. Without
 * `backward`, only the positive part counts, like the inflow of a Basin, see `basinFlowParts`.
 */
export interface FlowTerm {
  link: number;
  sign: 1 | -1;
  forward: Origin;
  backward: Origin | null;
}

export interface FlowParts {
  terms: FlowTerm[];
  /** The Basin of the Junction group that the link is part of, or null if it is no link of a group with one Basin */
  basin: number | null;
}

/**
 * The parts of the flow over a link, with where their water comes from.
 *
 * The flow over most links is a single part, with the water of the node it comes from. A link between the plain
 * Junctions of a group with one Basin, or between such a Junction and the Basin, has the net flow of the neighbors of
 * the group behind it, away from the Basin, which each exchange water with the Basin directly. So the flow of each of
 * these neighbors is a part: towards the Basin it has the water of the neighbor, away from it that of the Basin.
 */
export function flowParts(graph: FlowGraph, link: number): FlowParts {
  const ends = graph.ends(link);
  if (!ends) throw new Error(`Link #${link} is no flow link of the model.`);
  const nodes = [ends.from, ends.to];
  const junction = nodes.find((node) => isPlain(graph, node));
  const inGroup = nodes.every((node) => isPlain(graph, node) || graph.nodeType(node) === "Basin");
  const basin = junction !== undefined && inGroup ? groupBasin(graph, junction) : null;
  if (basin === null) {
    const forward = origin(graph, ends.from, link);
    return { terms: [{ link, sign: 1, forward, backward: origin(graph, ends.to, link) }], basin: null };
  }
  // The end of the link on its side away from the Basin
  let behind: number;
  if (ends.from === basin || ends.to === basin) behind = ends.from === basin ? ends.to : ends.from;
  else behind = walkJunctions(graph, ends.from, link).basins.has(basin) ? ends.to : ends.from;
  const side = walkJunctions(graph, behind, link);
  if (side.basins.size > 0) throw new Error(`The Junctions of Junction #${junction} do not form a tree.`);
  const towardsBasin = ends.from === behind ? 1 : -1;
  const basinWater: Origin = { kind: "basin", id: basin };
  const terms = side.others.map(({ node, link: other, inflow }): FlowTerm => {
    const water = origin(graph, node, other);
    return {
      link: other,
      sign: towardsBasin === inflow ? 1 : -1,
      forward: towardsBasin === 1 ? water : basinWater,
      backward: towardsBasin === 1 ? basinWater : water,
    };
  });
  return { terms, basin };
}

/** The node types with a flow that `nodeFlow` gives. */
export const FLOW_NODE_TYPES = new Set([...CONNECTOR_TYPES, "Junction", "FlowBoundary", "UserDemand", "Terminal"]);

/** The flow of a node: the links whose flows add up to it, and whether it is the flow rate or the inflow rate. */
export interface NodeFlow {
  variable: "flow_rate" | "inflow_rate";
  links: number[];
}

/** The link of a plain Junction towards the Basin of its group, or null if its group does not have a single Basin. */
function linkTowardsBasin(graph: FlowGraph, junction: number): number | null {
  const basin = isPlain(graph, junction) ? groupBasin(graph, junction) : null;
  if (basin === null) return null;
  const towards = graph.neighbors(junction).find(
    ({ node, link }) => node === basin || (isPlain(graph, node) && walkJunctions(graph, node, link).basins.has(basin)),
  );
  return towards?.link ?? null;
}

/**
 * The flow of a node, as the core and QGIS give it, see https://ribasim.org/reference/node/junction.html#flow-rate:
 * the flow rate of a connector node or FlowBoundary is its outflow, and that of a Junction the flow on its link
 * towards the Basin of its group, or else its inflow. A UserDemand has the inflow rate that it abstracts, and a
 * Terminal the inflow rate that leaves the model. Other nodes have none, see `FLOW_NODE_TYPES`.
 */
export function nodeFlow(graph: FlowGraph, node: number): NodeFlow | null {
  const type = graph.nodeType(node);
  if (type === undefined || !FLOW_NODE_TYPES.has(type)) return null;
  const neighbors = graph.neighbors(node);
  const inflows = neighbors.filter(({ inflow }) => inflow === 1).map(({ link }) => link);
  if (type === "UserDemand" || type === "Terminal") return { variable: "inflow_rate", links: inflows };
  if (type === "Junction") {
    const towards = linkTowardsBasin(graph, node);
    return { variable: "flow_rate", links: towards === null ? inflows : [towards] };
  }
  const outflow = neighbors.find(({ inflow }) => inflow === -1);
  return { variable: "flow_rate", links: outflow ? [outflow.link] : [] };
}

/**
 * The parts of the inflow or outflow of a Basin, like the `inflow_rate` and `outflow_rate` of the Basin results: the
 * water that it exchanges with each neighbor, also with those of a Junction tree of which it is the Basin, where that
 * flows into the Basin, or out of it.
 */
export function basinFlowParts(graph: FlowGraph, basin: number, inflow: boolean): FlowTerm[] {
  const basinWater: Origin = { kind: "basin", id: basin };
  return graph.neighbors(basin).flatMap((neighbor) => {
    let exchanges = [neighbor];
    if (isPlain(graph, neighbor.node) && groupBasin(graph, neighbor.node) === basin) {
      const side = walkJunctions(graph, neighbor.node, neighbor.link);
      if (side.basins.size > 0) throw new Error(`The Junctions of Junction #${neighbor.node} do not form a tree.`);
      // The other neighbors of the tree, whose positive inflow flows towards the Basin
      exchanges = side.others;
    }
    return exchanges.map(({ node, link, inflow: towardsBasin }): FlowTerm => {
      if (!inflow) return { link, sign: towardsBasin === 1 ? -1 : 1, forward: basinWater, backward: null };
      return { link, sign: towardsBasin, forward: origin(graph, node, link), backward: null };
    });
  });
}

/** The flow links and the Basins whose water the parts of a flow can need, see `waterFractions`. */
export interface TracedWaters {
  /** The links of the parts, and those over which water can flow into the nodes that mix it */
  links: Set<number>;
  basins: Set<number>;
}

/** The flow links and the Basins whose water the parts of a flow can need, see `flowParts` and `basinFlowParts`. */
export function tracedWaters(graph: FlowGraph, terms: FlowTerm[]): TracedWaters {
  const traced: TracedWaters = { links: new Set(), basins: new Set() };
  const seen = new Set<string>();
  const visit = (water: Origin | null) => {
    if (water?.kind === "basin") traced.basins.add(water.id);
    else if (water?.kind === "mix" && !seen.has(`${water.id} ${water.link}`)) {
      seen.add(`${water.id} ${water.link}`);
      for (const input of mixInputs(graph, water.id, water.link)) {
        traced.links.add(input.link);
        visit(origin(graph, input.node, input.link));
      }
    }
  };
  for (const term of terms) {
    traced.links.add(term.link);
    visit(term.forward);
    visit(term.backward);
  }
  return traced;
}

/** The share of each layer of a set in some water per time step, see `layerNames`. */
export type Fractions = Map<string, Float64Array>;

/** The layers of the stacked charts of a set: its tracers, then the water without a label in the set. */
export const layerNames = (set: TracerSet): string[] => [...set.tracers, OTHER];

/** The fractions of the water of a Basin, from its concentration of Continuity and of the tracers of a set. */
export function basinFractions(set: TracerSet, concentrations: Record<string, Float64Array>): Fractions {
  const continuity = concentrations[CONTINUITY];
  if (!continuity) throw new Error(`The concentrations have no ${CONTINUITY}.`);
  const other = Float64Array.from(continuity);
  const fractions: Fractions = new Map();
  for (const tracer of set.tracers) {
    const values = concentrations[tracer];
    if (values?.length !== other.length) throw new Error(`The concentrations have no ${tracer} for each time step.`);
    fractions.set(tracer, values);
    values.forEach((value, i) => (other[i] -= value));
  }
  return fractions.set(OTHER, other);
}

/** The fractions of water that has a single label: that of a boundary, or none. */
export function labelFractions(set: TracerSet, water: Origin, steps: number): Fractions {
  const boundaryLabel = water.kind === "boundary" ? set.boundaries[String(water.id)] : undefined;
  const label = boundaryLabel !== undefined && set.tracers.includes(boundaryLabel) ? boundaryLabel : OTHER;
  return new Map(layerNames(set).map((name) => [name, new Float64Array(steps).fill(name === label ? 1 : 0)]));
}

/** The flow rate of a link per time step, which must be known for each of them. */
function flowOf(flows: Map<number, Float64Array>, link: number, steps: number): Float64Array {
  const flow = flows.get(link);
  if (flow?.length !== steps) throw new Error(`No flow rate of link #${link} for each time step.`);
  return flow;
}

/** The nodes whose water `waterFractions` gave the fractions of: those that mix water, and those it cannot trace. */
export interface UsedWaters {
  mixed: Set<number>;
  untraced: Set<number>;
}

/**
 * The fractions of each water of a set per time step: of a Basin from `basins`, of a boundary its label, and of a node
 * that mixes water those of the water that flows into it, weighted by the flows of each time step, as the core mixes
 * the mass that flows into a Junction, see https://ribasim.org/reference/node/junction.html#tracers. The nodes whose
 * water it gives are added to `used`.
 */
export function waterFractions(
  set: TracerSet,
  graph: FlowGraph,
  flows: Map<number, Float64Array>,
  basins: Map<number, Fractions>,
  steps: number,
  used: UsedWaters = { mixed: new Set(), untraced: new Set() },
): (water: Origin) => Fractions {
  const known = new Map<string, Fractions>();
  const mixing = new Set<string>();
  const mix = (node: number, link: number): Fractions => {
    const result: Fractions = new Map(layerNames(set).map((name) => [name, new Float64Array(steps)]));
    const total = new Float64Array(steps);
    for (const input of mixInputs(graph, node, link)) {
      const flow = flowOf(flows, input.link, steps);
      let water: Fractions | undefined;
      for (let i = 0; i < steps; i++) {
        const q = input.inflow * flow[i];
        if (!(q > 0)) continue;
        water ??= fractionsOf(origin(graph, input.node, input.link));
        total[i] += q;
        for (const [name, values] of result) values[i] += q * water.get(name)![i];
      }
    }
    // Without inflow the water is unknown, which only matters for the rounding errors of the outflow
    for (const [name, values] of result) {
      values.forEach((value, i) => (values[i] = total[i] > 0 ? value / total[i] : name === OTHER ? 1 : 0));
    }
    return result;
  };
  const fractionsOf = (water: Origin): Fractions => {
    if (water.kind === "mix") used.mixed.add(water.id);
    if (water.kind === "untraced") used.untraced.add(water.id);
    if (water.kind === "basin") {
      const fractions = basins.get(water.id);
      if (!fractions) throw new Error(`No water of Basin #${water.id}.`);
      return fractions;
    }
    const key = water.kind === "mix" ? `${water.kind} ${water.id} ${water.link}` : `${water.kind} ${water.id}`;
    let result = known.get(key);
    if (result) return result;
    if (water.kind === "mix") {
      if (mixing.has(key)) throw new Error(`The water of node #${water.id} flows back into it, so it cannot be traced.`);
      mixing.add(key);
      result = mix(water.id, water.link);
      mixing.delete(key);
    } else {
      result = labelFractions(set, water, steps);
    }
    known.set(key, result);
    return result;
  };
  return fractionsOf;
}

/**
 * The flow of each layer of a set over a link per time step, positive in its direction, from the parts of the flow
 * and the fractions of their water, see `flowParts` and `waterFractions`.
 */
export function layerFlows(
  set: TracerSet,
  terms: FlowTerm[],
  flows: Map<number, Float64Array>,
  fractionsOf: (water: Origin) => Fractions,
  steps: number,
): Map<string, Float64Array> {
  const result = new Map(layerNames(set).map((name) => [name, new Float64Array(steps)]));
  for (const { link, sign, forward, backward } of terms) {
    const flow = flowOf(flows, link, steps);
    // The water that never flows does not matter, which may be unknown, like that of an empty Basin
    let ahead: Fractions | undefined;
    let back: Fractions | undefined;
    for (let i = 0; i < steps; i++) {
      const q = sign * flow[i];
      let water: Fractions | undefined;
      if (q > 0) water = ahead ??= fractionsOf(forward);
      else if (q < 0 && backward) water = back ??= fractionsOf(backward);
      if (water) for (const [name, values] of result) values[i] += q * water.get(name)![i];
    }
  }
  return result;
}

/** The total of the parts of a flow per time step, which is the flow over the link, see `flowParts`. */
export function partsTotal(terms: FlowTerm[], flows: Map<number, Float64Array>, steps: number): Float64Array {
  const total = new Float64Array(steps);
  for (const { link, sign, backward } of terms) {
    flows.get(link)?.forEach((flow, i) => (total[i] += backward ? sign * flow : Math.max(sign * flow, 0)));
  }
  return total;
}

/** The layers whose magnitude reaches `tolerance` times `scale` at some time step, and is not zero, in their order. */
export function visibleLayers(layers: Map<string, Float64Array>, scale: number, tolerance = 1e-3): string[] {
  const visible = (value: number) => value !== 0 && Math.abs(value) >= tolerance * scale;
  return [...layers].filter(([, values]) => values.some(visible)).map(([name]) => name);
}

/**
 * The edges of the stacked layers per time step: the positive parts stacked up from zero, and the negative parts down
 * from zero, both in the order of the layers. A layer without a value at a time step has no edge there, nor the layers
 * above it.
 */
export function stackLayers(layers: Float64Array[]): { up: Float64Array[]; down: Float64Array[] } {
  const steps = layers[0]?.length ?? 0;
  const up: Float64Array[] = [];
  const down: Float64Array[] = [];
  let [below, above] = [new Float64Array(steps), new Float64Array(steps)];
  for (const values of layers) {
    above = above.map((edge, i) => edge + Math.max(values[i], 0));
    below = below.map((edge, i) => edge + Math.min(values[i], 0));
    up.push(above);
    down.push(below);
  }
  return { up, down };
}

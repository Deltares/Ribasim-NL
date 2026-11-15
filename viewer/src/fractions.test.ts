import assert from "node:assert/strict";
import { test } from "node:test";
import {
  basinFlowParts,
  basinFractions,
  flowParts,
  labelFractions,
  layerFlows,
  nodeFlow,
  OTHER,
  origin,
  partsTotal,
  stackLayers,
  tracedWaters,
  visibleLayers,
  waterFractions,
  type FlowGraph,
  type FlowNeighbor,
  type Fractions,
  type Origin,
  type TracerSet,
} from "./fractions.ts";

/** A flow graph of nodes by id, and links as [id, from, to], with the Junctions in the Junction / static table. */
function graph(types: Record<number, string>, links: [number, number, number][], inStatic: number[] = []): FlowGraph {
  const neighbors = new Map<number, FlowNeighbor[]>();
  const add = (node: number, neighbor: FlowNeighbor) => neighbors.set(node, [...(neighbors.get(node) ?? []), neighbor]);
  for (const [link, from, to] of links) {
    add(from, { node: to, link, inflow: -1 });
    add(to, { node: from, link, inflow: 1 });
  }
  const ends = new Map(links.map(([link, from, to]) => [link, { from, to }]));
  return {
    nodeType: (node) => types[node],
    inStatic: (node) => inStatic.includes(node),
    neighbors: (node) => neighbors.get(node) ?? [],
    ends: (link) => ends.get(link),
  };
}

const basin = (id: number): Origin => ({ kind: "basin", id });

/**
 * The lake of https://github.com/Deltares/Ribasim/issues/3356: Basin 1 with a tree of Junctions 2 and 3, which
 * Outlets 4 and 6 flow into from Basins 8 and 10. Pump 5 flows out of it to Basin 9, and ManningResistance 7 connects
 * it to Basin 11. Link 2 between the Junctions points towards the Basin, unless `reversed`.
 */
function lake(reversed = false): FlowGraph {
  return graph(
    { 1: "Basin", 2: "Junction", 3: "Junction", 4: "Outlet", 5: "Pump", 6: "Outlet", 7: "ManningResistance", 8: "Basin", 9: "Basin", 10: "Basin", 11: "Basin" },
    [
      [1, 2, 1],
      reversed ? [2, 2, 3] : [2, 3, 2],
      [3, 4, 2],
      [4, 2, 5],
      [5, 6, 3],
      [6, 3, 7],
      [7, 8, 4],
      [8, 5, 9],
      [9, 10, 6],
      [10, 7, 11],
    ],
  );
}

test("the water over a link comes from the node it flows out of, past connector nodes and Junction groups", () => {
  const model = lake();
  // An Outlet into the tree takes the water of the Basin behind it, and gets that of the lake when it flows back
  assert.deepEqual(flowParts(model, 3), { terms: [{ link: 3, sign: 1, forward: basin(8), backward: basin(1) }], basin: null });
  // The link out of a Pump, which gets its water from the lake through the tree
  assert.deepEqual(flowParts(model, 8), { terms: [{ link: 8, sign: 1, forward: basin(1), backward: basin(9) }], basin: null });
});

test("a link between Junctions has the net flow of the neighbors behind it, each with its own water", () => {
  const { terms, basin: lakeBasin } = flowParts(lake(), 2);
  assert.equal(lakeBasin, 1);
  const byLink = new Map(terms.map((term) => [term.link, term]));
  assert.deepEqual([...byLink.keys()].sort(), [5, 6]);
  // Towards the lake an inflow has the water of its source, away from it that of the lake
  assert.deepEqual(byLink.get(5), { link: 5, sign: 1, forward: basin(10), backward: basin(1) });
  // A positive flow over link 6 flows away from the lake, against link 2
  assert.deepEqual(byLink.get(6), { link: 6, sign: -1, forward: basin(11), backward: basin(1) });

  // Link 2 drawn the other way around, away from the lake
  const reversed = new Map(flowParts(lake(true), 2).terms.map((term) => [term.link, term]));
  assert.deepEqual(reversed.get(5), { link: 5, sign: -1, forward: basin(1), backward: basin(10) });
  assert.deepEqual(reversed.get(6), { link: 6, sign: 1, forward: basin(1), backward: basin(11) });
});

test("the link between the tree and its Basin has the flow of all neighbors of the tree", () => {
  const { terms } = flowParts(lake(), 1);
  assert.deepEqual(
    terms.map(({ link, sign }) => [link, sign]).sort((a, b) => a[0] - b[0]),
    [
      [3, 1],
      [4, -1],
      [5, 1],
      [6, -1],
    ],
  );
});

const set: TracerSet = { name: "test", tracers: ["Rijn", "Neerslag"], boundaries: { "12": "Rijn" } };

test("the flow of each layer adds up to the flow of the link", () => {
  const model = lake();
  const steps = 2;
  // Outlet 6 lets in the Rhine water of Basin 10, and ManningResistance 7 lets out the rain of the lake, until it
  // turns in the second step, with the water of Basin 11
  const fractions: Record<number, Fractions> = {
    1: new Map([["Rijn", new Float64Array(steps)], ["Neerslag", new Float64Array(steps).fill(1)], [OTHER, new Float64Array(steps)]]),
    10: labelFractions(set, { kind: "boundary", id: 12 }, steps),
    11: new Map([["Rijn", new Float64Array([0, 0.5])], ["Neerslag", new Float64Array(steps)], [OTHER, new Float64Array([1, 0.5])]]),
  };
  const fractionsOf = (water: Origin) => fractions[water.id];
  const flows = new Map([
    [5, new Float64Array([3, 3])],
    [6, new Float64Array([1, -2])],
  ]);
  const { terms } = flowParts(model, 2);
  const layers = layerFlows(set, terms, flows, fractionsOf, steps);
  assert.deepEqual(layers.get("Rijn"), new Float64Array([3, 4]));
  assert.deepEqual(layers.get("Neerslag"), new Float64Array([-1, 0]));
  assert.deepEqual(layers.get(OTHER), new Float64Array([0, 1]));
  assert.deepEqual(partsTotal(terms, flows, steps), new Float64Array([2, 5]));
});

test("the water of a Basin without a label takes up the rest of Continuity", () => {
  const fractions = basinFractions(set, {
    Continuity: new Float64Array([1, 1]),
    Rijn: new Float64Array([0.25, 0]),
    Neerslag: new Float64Array([0.5, 1]),
  });
  assert.deepEqual([...fractions.keys()], ["Rijn", "Neerslag", OTHER]);
  assert.deepEqual(fractions.get(OTHER), new Float64Array([0.25, 0]));
  assert.throws(() => basinFractions(set, { Continuity: new Float64Array(2), Rijn: new Float64Array(2) }), /Neerslag/);
});

test("boundaries have their label, other water none", () => {
  assert.deepEqual(labelFractions(set, { kind: "boundary", id: 12 }, 1).get("Rijn"), new Float64Array([1]));
  for (const water of [{ kind: "boundary", id: 13 }, { kind: "untraced", id: 12 }] as Origin[]) {
    const fractions = labelFractions(set, water, 1);
    assert.deepEqual([fractions.get("Rijn")![0], fractions.get(OTHER)![0]], [0, 1]);
  }
});

test("water that cannot be traced, and Junction trees that reach their Basin twice", () => {
  const model = graph({ 1: "Basin", 2: "Terminal" }, [[1, 1, 2]]);
  assert.deepEqual(origin(model, 2, 1), { kind: "untraced", id: 2 });
  // A tree that reaches its Basin twice is no tree
  const loop = graph({ 1: "Basin", 2: "Junction", 3: "Junction", 4: "Outlet" }, [
    [1, 2, 1],
    [2, 3, 2],
    [3, 3, 1],
    [4, 4, 3],
  ]);
  assert.throws(() => flowParts(loop, 2), /do not form a tree/);
});

test("stacked layers and the layers worth showing", () => {
  const { up, down } = stackLayers([new Float64Array([1, -1]), new Float64Array([2, 3]), new Float64Array([-1, Number.NaN])]);
  // Plain arrays, since typed arrays compare their bytes, and those of NaN can differ
  const plain = (edges: Float64Array[]) => edges.map((edge) => [...edge]);
  assert.deepEqual(plain(up), [[1, 0], [3, 3], [3, Number.NaN]]);
  assert.deepEqual(plain(down), [[0, -1], [0, -1], [-1, Number.NaN]]);
  const layers = new Map([
    ["a", new Float64Array([0.5, 0])],
    ["b", new Float64Array([1e-4, -1e-4])],
    ["c", new Float64Array([0, -0.01])],
  ]);
  assert.deepEqual(visibleLayers(layers, 1), ["a", "c"]);
  // Without any flow, no layer has water
  assert.deepEqual(visibleLayers(new Map([["a", new Float64Array(2)]]), 0), []);
});

/** Fractions that are the same at each time step, by layer. */
const constant = (steps: number, shares: Record<string, number>): Fractions =>
  new Map([...set.tracers, OTHER].map((name) => [name, new Float64Array(steps).fill(shares[name] ?? 0)]));

/** The values of a layer, rounded to compare them. */
const rounded = (values: Float64Array | undefined) => [...values!].map((value) => Math.round(value * 1e9) / 1e9);

/**
 * The example of https://ribasim.org/reference/node/junction.html#junctions-in-the-static-table, with a FlowBoundary
 * that also flows into J1: Pump 2 takes the water of Basin 1, J1 (#3) divides it over Basin 4 and J2 (#5), which
 * supplies UserDemand 6 and passes on the rest to Basin 7, which the UserDemand returns its water to.
 */
function divided(): FlowGraph {
  return graph(
    { 1: "Basin", 2: "Pump", 3: "Junction", 4: "Basin", 5: "Junction", 6: "UserDemand", 7: "Basin", 12: "FlowBoundary" },
    [
      [1, 1, 2],
      [2, 2, 3],
      [3, 3, 4],
      [4, 3, 5],
      [5, 5, 6],
      [6, 6, 7],
      [7, 5, 7],
      [8, 12, 3],
    ],
    [3, 5],
  );
}

test("Junctions in the Junction / static table mix the water that flows into them", () => {
  const model = divided();
  const steps = 2;
  // The UserDemand returns the water it takes from J2, which is that of J1
  assert.deepEqual(origin(model, 6, 6), { kind: "mix", id: 5, link: 5 });
  const { terms } = flowParts(model, 6);
  const traced = tracedWaters(model, terms);
  assert.deepEqual([...traced.links].sort(), [2, 4, 6, 8]);
  // Basin 7 for a flow against the link
  assert.deepEqual([...traced.basins].sort(), [1, 7]);
  // The Pump lets in rain, the Rhine flows in first, and no water at all in the second time step
  const flows = new Map([
    [2, new Float64Array([3, 0])],
    [8, new Float64Array([1, 0])],
    [4, new Float64Array([2.8, 0])],
    [6, new Float64Array([0.5, 0])],
  ]);
  const basins = new Map([1, 7].map((id) => [id, constant(steps, { Neerslag: 1 })]));
  const used = { mixed: new Set<number>(), untraced: new Set<number>() };
  const fractionsOf = waterFractions(set, model, flows, basins, steps, used);
  const layers = layerFlows(set, terms, flows, fractionsOf, steps);
  assert.deepEqual(rounded(layers.get("Rijn")), [0.125, 0]);
  assert.deepEqual(rounded(layers.get("Neerslag")), [0.375, 0]);
  // J2, and J1 that flows into it
  assert.deepEqual([...used.mixed].sort(), [3, 5]);
  // Without inflow the water of J1 is unknown
  assert.deepEqual(rounded(fractionsOf(origin(model, 3, 3)).get(OTHER)), [0, 1]);
});

test("plain Junctions of a group without a single Basin connect each node upstream with each node downstream", () => {
  // Outlets 2 and 3 let the water of Basins 1 and 4 into Junction 5, which flows over Junction 6 into LevelBoundary 7
  const model = graph({ 1: "Basin", 2: "Outlet", 3: "Outlet", 4: "Basin", 5: "Junction", 6: "Junction", 7: "LevelBoundary" }, [
    [1, 1, 2],
    [2, 2, 5],
    [3, 4, 3],
    [4, 3, 5],
    [5, 5, 6],
    [6, 6, 7],
  ]);
  const { terms, basin } = flowParts(model, 6);
  assert.equal(basin, null);
  // Junction 6 passes on the water that flows into it, which Junction 5 passes on in turn
  const forward = { kind: "mix", id: 6, link: 6 };
  assert.deepEqual(terms, [{ link: 6, sign: 1, forward, backward: { kind: "boundary", id: 7 } }]);
  const steps = 1;
  const flows = new Map([
    [2, new Float64Array([1])],
    [4, new Float64Array([3])],
    [5, new Float64Array([4])],
    [6, new Float64Array([4])],
  ]);
  const basins = new Map([
    [1, constant(steps, { Rijn: 1 })],
    [4, constant(steps, { Neerslag: 0.5, [OTHER]: 0.5 })],
  ]);
  const layers = layerFlows(set, terms, flows, waterFractions(set, model, flows, basins, steps), steps);
  assert.deepEqual([...layers].map(([name, values]) => [name, rounded(values)]), [
    ["Rijn", [1]],
    ["Neerslag", [1.5]],
    [OTHER, [1.5]],
  ]);
});

test("a Junction tree passes on the water of a Junction in the Junction / static table next to it", () => {
  // Junction 3 divides the flow of Pump 2 over Basin 4 and the tree of Junction 5 with Basin 6
  const model = graph({ 1: "Basin", 2: "Pump", 3: "Junction", 4: "Basin", 5: "Junction", 6: "Basin" }, [
    [1, 1, 2],
    [2, 2, 3],
    [3, 3, 4],
    [4, 3, 5],
    [5, 5, 6],
  ], [3]);
  assert.deepEqual(flowParts(model, 5), {
    terms: [{ link: 4, sign: 1, forward: { kind: "mix", id: 3, link: 4 }, backward: { kind: "basin", id: 6 } }],
    basin: 6,
  });
  // Water that flows back into a Junction cannot be traced
  const loop = graph({ 1: "FlowBoundary", 2: "Junction", 3: "Junction", 4: "Basin" }, [
    [1, 1, 2],
    [2, 2, 3],
    [3, 3, 2],
    [4, 3, 4],
  ], [2, 3]);
  const flows = new Map([1, 2, 3, 4].map((link) => [link, new Float64Array([1])]));
  const fractionsOf = waterFractions(set, loop, flows, new Map(), 1);
  assert.throws(() => fractionsOf(origin(loop, 3, 4)), /flows back into it/);
});

test("the flow of a node is that of its links, as in the core", () => {
  const model = lake();
  // A Junction of a tree has the flow on its link towards the Basin, a connector node its outflow
  assert.deepEqual(nodeFlow(model, 2), { variable: "flow_rate", links: [1] });
  assert.deepEqual(nodeFlow(model, 3), { variable: "flow_rate", links: [2] });
  assert.deepEqual(nodeFlow(model, 5), { variable: "flow_rate", links: [8] });
  assert.equal(nodeFlow(model, 1), null);
  // Other Junctions have their inflow, a UserDemand the inflow it abstracts, and a FlowBoundary its outflow
  const mixing = divided();
  assert.deepEqual(nodeFlow(mixing, 3), { variable: "flow_rate", links: [2, 8] });
  assert.deepEqual(nodeFlow(mixing, 6), { variable: "inflow_rate", links: [5] });
  assert.deepEqual(nodeFlow(mixing, 12), { variable: "flow_rate", links: [8] });
});

test("the inflow and outflow of a Basin are what it exchanges with each neighbor, also through its Junction tree", () => {
  const model = lake();
  const steps = 1;
  // Outlets 4 and 6 let in the water of Basins 8 and 10, Pump 5 takes 1.5 from the lake, and ManningResistance 7
  // lets in the water of Basin 11
  const flows = new Map([
    [3, new Float64Array([2])],
    [4, new Float64Array([1.5])],
    [5, new Float64Array([1])],
    [6, new Float64Array([-0.5])],
  ]);
  const basins = new Map([
    [1, constant(steps, { [OTHER]: 1 })],
    [8, constant(steps, { Rijn: 1 })],
    [10, constant(steps, { Rijn: 1 })],
    [11, constant(steps, { Neerslag: 1 })],
  ]);
  const fractionsOf = waterFractions(set, model, flows, basins, steps);
  const split = (inflow: boolean) => {
    const terms = basinFlowParts(model, 1, inflow);
    const layers = layerFlows(set, terms, flows, fractionsOf, steps);
    return [rounded(partsTotal(terms, flows, steps)), ...[...layers.values()].map(rounded)];
  };
  // The total, then Rijn, Neerslag and Other
  assert.deepEqual(split(true), [[3.5], [3], [0.5], [0]]);
  assert.deepEqual(split(false), [[1.5], [0], [0], [1.5]]);
});

test("a UserDemand that takes water from several nodes returns the mix", () => {
  // UserDemand 3 takes water from Basins 1 and 2, and returns it to Basin 4
  const model = graph({ 1: "Basin", 2: "Basin", 3: "UserDemand", 4: "Basin" }, [
    [1, 1, 3],
    [2, 2, 3],
    [3, 3, 4],
  ]);
  assert.deepEqual(origin(model, 3, 3), { kind: "mix", id: 3, link: 3 });
  const flows = new Map([
    [1, new Float64Array([1])],
    [2, new Float64Array([3])],
    [3, new Float64Array([2])],
  ]);
  const basins = new Map([
    [1, constant(1, { Rijn: 1 })],
    [2, constant(1, { Neerslag: 1 })],
    [4, constant(1, { [OTHER]: 1 })],
  ]);
  const layers = layerFlows(set, flowParts(model, 3).terms, flows, waterFractions(set, model, flows, basins, 1), 1);
  assert.deepEqual([...layers.values()].map(rounded), [[0.5], [1.5], [0]]);
});

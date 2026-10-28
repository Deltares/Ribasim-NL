import { readColumns, type Comparison, type FeatureDiff, type Manifest } from "./data";

/**
 * deck.gl binary layer data. Layers must get the same object on every render,
 * since deck.gl compares data by reference and otherwise rebuilds all attributes.
 */
export interface LayerData {
  length: number;
  /** Start vertex of each path */
  startIndices?: Uint32Array;
  attributes: Record<string, { value: Float32Array | Uint8Array; size: number; normalized?: boolean }>;
}

/** Rows of one node or link type, with their deck.gl layer data. */
export interface Group {
  type: string;
  rows: Int32Array;
  /** Node positions, or link paths */
  data: LayerData;
  /** Position halfway along each link and its direction in degrees counterclockwise from east, only for links */
  arrows?: LayerData;
}

/**
 * Node and link types that keep their own layer when comparing models, also when unchanged, since their charts
 * compare the results of both models.
 */
export const ALWAYS_SHOWN_TYPES = new Set(["Observation", "observation"]);

/** Features that are the same in both models of a comparison, drawn faintly for context. */
export interface Context {
  nodes: Group;
  links: Group;
  /** Index within the context group, by node or link row */
  nodeLocal: Map<number, number>;
  linkLocal: Map<number, number>;
}

export interface Network {
  nodeId: Int32Array;
  nodeType: string[];
  nodeX: Float32Array;
  nodeY: Float32Array;
  nodeRow: Map<number, number>;
  nodeGroups: Group[];
  linkId: Int32Array;
  linkType: string[];
  fromNodeId: Int32Array;
  toNodeId: Int32Array;
  linkRow: Map<number, number>;
  linkGroups: Group[];
  /** Index within its group, for each node or link row, or -1 for the context */
  nodeLocal: Int32Array;
  linkLocal: Int32Array;
  /** Row in the Parquet file of the model the feature is from: the base model for removed features */
  nodeFileRow: Int32Array;
  linkFileRow: Int32Array;
  /** Only when comparing models */
  context: Context | null;
  /** Rows of the base model by id, only when comparing models */
  baseNodeRow: Map<number, number>;
  baseLinkRow: Map<number, number>;
  /** The base id of each link in both models, matched on its nodes since links get renumbered */
  baseLinkId: Map<number, number>;
  /** The ids of the flow links out of each node in the base model, by node id */
  baseOutflows: Map<number, number[]>;
}

/** The water authorities by their `waterbeheercode`, see `ribasim_nl.aquo`. */
export const AUTHORITIES: Record<number, string> = {
  2: "Wetterskip Fryslân",
  7: "Rijn en IJssel",
  9: "Rivierenland",
  11: "Amstel, Gooi en Vecht",
  12: "Hollands Noorderkwartier",
  13: "Rijnland",
  14: "De Stichtse Rijnlanden",
  15: "Delfland",
  25: "Brabantse Delta",
  27: "De Dommel",
  33: "Hunze en Aa's",
  34: "Noorderzijlvest",
  37: "Zuiderzeeland",
  38: "Aa en Maas",
  39: "Schieland en de Krimpenerwaard",
  40: "Hollandse Delta",
  42: "Scheldestromen",
  43: "Vallei en Veluwe",
  44: "Vechtstromen",
  59: "Drents Overijsselse Delta",
  60: "Limburg",
  80: "Rijkswaterstaat",
};
export const RIJKSWATERSTAAT = 80;

/** The `waterbeheercode` of the authority of a node: node ids are that code times 100 000 plus its id there. */
export function authorityOf(nodeId: number): number {
  return Math.floor(Math.abs(nodeId) / 100_000);
}

/** The name of an authority, or its code if it is unknown. */
export function authorityName(code: number): string {
  return AUTHORITIES[code] ?? `Waterbeheercode ${code}`;
}

/** The authorities of the nodes of a network: Rijkswaterstaat first, then the others by name. */
export function networkAuthorities(network: Network): number[] {
  const codes = [...new Set(Array.from(network.nodeId, authorityOf))];
  return codes.sort(
    (a, b) => Number(b === RIJKSWATERSTAAT) - Number(a === RIJKSWATERSTAAT) || authorityName(a).localeCompare(authorityName(b), "nl"),
  );
}

/**
 * Per node and link row, 1 if it belongs to an authority or connects to it: the nodes of the authority, the links
 * with a node of the authority, and the nodes of other authorities at their other end.
 */
export function authorityRows(network: Network, code: number): { nodes: Uint8Array; links: Uint8Array } {
  const nodes = Uint8Array.from(network.nodeId, (id) => (authorityOf(id) === code ? 1 : 0));
  const links = new Uint8Array(network.linkId.length);
  for (let row = 0; row < links.length; row++) {
    const from = network.fromNodeId[row];
    const to = network.toNodeId[row];
    if (authorityOf(from) !== code && authorityOf(to) !== code) continue;
    links[row] = 1;
    for (const end of [from, to]) {
      const endRow = network.nodeRow.get(end);
      if (endRow !== undefined) nodes[endRow] = 1;
    }
  }
  return { nodes, links };
}

function rowsByType(types: string[]): Map<string, number[]> {
  const groups = new Map<string, number[]>();
  types.forEach((type, row) => {
    let rows = groups.get(type);
    if (!rows) groups.set(type, (rows = []));
    rows.push(row);
  });
  return new Map([...groups].sort(([a], [b]) => a.localeCompare(b)));
}

function indexById(ids: Int32Array): Map<number, number> {
  const index = new Map<number, number>();
  ids.forEach((id, row) => index.set(id, row));
  if (index.size !== ids.length) throw new Error("Duplicate ids");
  return index;
}

const toStrings = (array: unknown[]) => array.map(String);

/** The point halfway along a lon/lat path, with the local direction, using an equirectangular approximation. */
function halfway(coords: ArrayLike<number>): [number, number, number] {
  const scale = Math.cos((coords[1] * Math.PI) / 180);
  const segments: number[] = [];
  let total = 0;
  for (let i = 2; i < coords.length; i += 2) {
    const length = Math.hypot((coords[i] - coords[i - 2]) * scale, coords[i + 1] - coords[i - 1]);
    segments.push(length);
    total += length;
  }
  let remaining = total / 2;
  for (let s = 0; s < segments.length; s++) {
    const i = 2 * s;
    const dx = coords[i + 2] - coords[i];
    const dy = coords[i + 3] - coords[i + 1];
    if (remaining <= segments[s] || s === segments.length - 1) {
      const f = segments[s] > 0 ? Math.min(remaining / segments[s], 1) : 0;
      const angle = (Math.atan2(dy, dx * scale) * 180) / Math.PI;
      return [coords[i] + f * dx, coords[i + 1] + f * dy, angle];
    }
    remaining -= segments[s];
  }
  return [coords[0], coords[1], 0];
}

interface Columns {
  nodes: { node_id: unknown[]; node_type: unknown[]; x: unknown[]; y: unknown[] };
  links: { link_id: unknown[]; link_type: unknown[]; from_node_id: unknown[]; to_node_id: unknown[]; coords: unknown[] };
}

async function readNetwork(manifest: Manifest): Promise<Columns> {
  const [nodes, links] = await Promise.all([
    readColumns(manifest.files.nodes, ["node_id", "node_type", "x", "y"]),
    readColumns(manifest.files.links, ["link_id", "link_type", "from_node_id", "to_node_id", "coords"]),
  ]);
  return { nodes, links };
}

/** Rows of the head model, followed by the rows of the base model that were removed. */
function mergedRows(head: unknown[], base: unknown[] | undefined, removed: (id: number) => boolean) {
  const baseRows = base ? base.flatMap((id, row) => (removed(Number(id)) ? [row] : [])) : [];
  const fileRow = Int32Array.from([...head.keys(), ...baseRows]);
  const pick = (headValues: unknown[], baseValues: unknown[] | undefined): unknown[] => [
    ...headValues,
    ...baseRows.map((row) => baseValues![row]),
  ];
  return { fileRow, pick };
}

type SetLocal = (row: number, i: number) => void;

function nodeGroup(type: string, rows: number[], nodeX: Float32Array, nodeY: Float32Array, local: SetLocal): Group {
  const positions = new Float32Array(rows.length * 2);
  rows.forEach((row, i) => {
    positions[2 * i] = nodeX[row];
    positions[2 * i + 1] = nodeY[row];
    local(row, i);
  });
  const data = { length: rows.length, attributes: { getPosition: { value: positions, size: 2 } } };
  return { type, rows: Int32Array.from(rows), data };
}

function linkGroup(type: string, rows: number[], coords: ArrayLike<number>[], local: SetLocal): Group {
  const startIndices = new Uint32Array(rows.length);
  let vertices = 0;
  rows.forEach((row, i) => {
    startIndices[i] = vertices;
    vertices += coords[row].length / 2;
    local(row, i);
  });
  const positions = new Float32Array(vertices * 2);
  const arrowPositions = new Float32Array(rows.length * 2);
  const arrowAngles = new Float32Array(rows.length);
  rows.forEach((row, i) => {
    positions.set(coords[row], startIndices[i] * 2);
    const [x, y, angle] = halfway(coords[row]);
    arrowPositions[2 * i] = x;
    arrowPositions[2 * i + 1] = y;
    arrowAngles[i] = angle;
  });
  return {
    type,
    rows: Int32Array.from(rows),
    data: { length: rows.length, startIndices, attributes: { getPath: { value: positions, size: 2 } } },
    arrows: {
      length: rows.length,
      attributes: { getPosition: { value: arrowPositions, size: 2 }, getAngle: { value: arrowAngles, size: 1 } },
    },
  };
}

/** Whether a feature of the base model was removed; removed links are keyed by their negated id. */
const isRemoved = (diffs: Map<number, FeatureDiff> | undefined, sign: 1 | -1) => (id: number) =>
  diffs?.get(sign * id)?.status === "removed";

/** The label of a link id; removed links have their negated id in the base model. */
export const linkLabel = (id: number) => (id < 0 ? `#${-id} (base)` : `#${id}`);

/** Flow neighbors of each Junction, upstream and downstream, built once per network. */
const junctionNeighbors = new WeakMap<Network, { upstream: Map<number, number[]>; downstream: Map<number, number[]> }>();

function neighborsOfJunctions(network: Network) {
  let neighbors = junctionNeighbors.get(network);
  if (neighbors) return neighbors;
  neighbors = { upstream: new Map(), downstream: new Map() };
  const isJunction = (id: number) => network.nodeType[network.nodeRow.get(id) ?? -1] === "Junction";
  const add = (map: Map<number, number[]>, key: number, value: number) => {
    const list = map.get(key);
    if (list) list.push(value);
    else map.set(key, [value]);
  };
  network.linkId.forEach((_, row) => {
    if (network.linkType[row] !== "flow") return;
    const from = network.fromNodeId[row];
    const to = network.toNodeId[row];
    if (isJunction(to)) add(neighbors.upstream, to, from);
    if (isJunction(from)) add(neighbors.downstream, from, to);
  });
  junctionNeighbors.set(network, neighbors);
  return neighbors;
}

/**
 * The node beyond any Junctions, following flow links in the given direction. Junctions only join links, so the
 * Basin behind them holds the water level. Prefers a Basin over other nodes, and returns the node itself when it is
 * no Junction or no node is found beyond it.
 */
export function pastJunctions(network: Network, nodeId: number, direction: "upstream" | "downstream"): number {
  const typeOf = (id: number) => network.nodeType[network.nodeRow.get(id) ?? -1];
  if (typeOf(nodeId) !== "Junction") return nodeId;
  const next = neighborsOfJunctions(network)[direction];
  const seen = new Set([nodeId]);
  const queue = [nodeId];
  let found: number | undefined;
  while (queue.length) {
    for (const neighbor of next.get(queue.shift()!) ?? []) {
      if (seen.has(neighbor)) continue;
      seen.add(neighbor);
      const type = typeOf(neighbor);
      if (type === "Basin") return neighbor;
      if (type === "Junction") queue.push(neighbor);
      else found ??= neighbor;
    }
  }
  return found ?? nodeId;
}

/** The ids of the flow links out of each node, by node id. */
function outflows(links: Columns["links"]): Map<number, number[]> {
  const result = new Map<number, number[]>();
  links.link_id.forEach((id, row) => {
    if (links.link_type[row] !== "flow") return;
    const from = Number(links.from_node_id[row]);
    let ids = result.get(from);
    if (!ids) result.set(from, (ids = []));
    ids.push(Number(id));
  });
  return result;
}

/** The base id of each link in both models by head id, matched on the from and to node like `webmap_diff`. */
function matchLinks(head: Columns["links"], base: Columns["links"]): Map<number, number> {
  const key = (links: Columns["links"], row: number) => `${links.from_node_id[row]}-${links.to_node_id[row]}`;
  const baseIds = new Map(base.link_id.map((id, row) => [key(base, row), Number(id)]));
  const matched = new Map<number, number>();
  head.link_id.forEach((id, row) => {
    const baseId = baseIds.get(key(head, row));
    if (baseId !== undefined) matched.set(Number(id), baseId);
  });
  return matched;
}

/**
 * The nodes and links of a model, grouped by type. When comparing models, removed features are added from the
 * base model, and only the features that differ, also in their results, and those of `ALWAYS_SHOWN_TYPES` are
 * grouped by type; the other unchanged features form the context.
 */
export async function loadNetwork(manifest: Manifest, comparison: Comparison | null = null): Promise<Network> {
  const [head, base] = await Promise.all([readNetwork(manifest), comparison ? readNetwork(comparison.base) : null]);
  const diff = comparison?.diff;

  const nodes = mergedRows(head.nodes.node_id, base?.nodes.node_id, isRemoved(diff?.nodes, 1));
  const nodeId = Int32Array.from(nodes.pick(head.nodes.node_id, base?.nodes.node_id) as number[]);
  const nodeType = toStrings(nodes.pick(head.nodes.node_type, base?.nodes.node_type));
  const nodeX = Float32Array.from(nodes.pick(head.nodes.x, base?.nodes.x) as number[]);
  const nodeY = Float32Array.from(nodes.pick(head.nodes.y, base?.nodes.y) as number[]);
  const nodeLocal = new Int32Array(nodeId.length).fill(-1);
  const nodeGrouped = (row: number) =>
    !diff ||
    ALWAYS_SHOWN_TYPES.has(nodeType[row]) ||
    diff.nodes.has(nodeId[row]) ||
    (diff.results?.basin.differences.has(nodeId[row]) ?? false);
  const nodeGroups = [...rowsByType(nodeType)]
    .map(([type, rows]) => nodeGroup(type, rows.filter(nodeGrouped), nodeX, nodeY, (row, i) => (nodeLocal[row] = i)))
    .filter((group) => group.rows.length > 0);

  const links = mergedRows(head.links.link_id, base?.links.link_id, isRemoved(diff?.links, -1));
  const headLinks = head.links.link_id.length;
  const linkId = Int32Array.from(links.pick(head.links.link_id, base?.links.link_id) as number[], (id, row) =>
    row < headLinks ? id : -id,
  );
  const linkType = toStrings(links.pick(head.links.link_type, base?.links.link_type));
  const coords = links.pick(head.links.coords, base?.links.coords) as ArrayLike<number>[];
  const linkLocal = new Int32Array(linkId.length).fill(-1);
  const linkGrouped = (row: number) =>
    !diff ||
    ALWAYS_SHOWN_TYPES.has(linkType[row]) ||
    diff.links.has(linkId[row]) ||
    (diff.results?.flow.differences.has(linkId[row]) ?? false);
  const linkGroups = [...rowsByType(linkType)]
    .map(([type, rows]) => linkGroup(type, rows.filter(linkGrouped), coords, (row, i) => (linkLocal[row] = i)))
    .filter((group) => group.rows.length > 0);

  let context: Context | null = null;
  if (diff) {
    const contextNodeLocal = new Map<number, number>();
    const contextLinkLocal = new Map<number, number>();
    const unchangedNodes = [...nodeId.keys()].filter((row) => !nodeGrouped(row));
    const unchangedLinks = [...linkId.keys()].filter((row) => !linkGrouped(row));
    context = {
      nodes: nodeGroup("unchanged", unchangedNodes, nodeX, nodeY, (row, i) => contextNodeLocal.set(row, i)),
      links: linkGroup("unchanged", unchangedLinks, coords, (row, i) => contextLinkLocal.set(row, i)),
      nodeLocal: contextNodeLocal,
      linkLocal: contextLinkLocal,
    };
  }

  return {
    nodeId,
    nodeType,
    nodeX,
    nodeY,
    nodeRow: indexById(nodeId),
    nodeGroups,
    nodeLocal,
    linkId,
    linkType,
    fromNodeId: Int32Array.from(links.pick(head.links.from_node_id, base?.links.from_node_id) as number[]),
    toNodeId: Int32Array.from(links.pick(head.links.to_node_id, base?.links.to_node_id) as number[]),
    linkRow: indexById(linkId),
    linkGroups,
    linkLocal,
    nodeFileRow: nodes.fileRow,
    linkFileRow: links.fileRow,
    context,
    baseNodeRow: base ? indexById(Int32Array.from(base.nodes.node_id as number[])) : new Map(),
    baseLinkRow: base ? indexById(Int32Array.from(base.links.link_id as number[])) : new Map(),
    baseLinkId: base ? matchLinks(head.links, base.links) : new Map(),
    baseOutflows: base ? outflows(base.links) : new Map(),
  };
}

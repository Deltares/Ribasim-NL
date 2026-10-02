import { readColumns, type Manifest } from "./data";

/** Rows of one node or link type, with positions laid out for deck.gl binary attributes. */
export interface Group {
  type: string;
  rows: Int32Array;
  positions: Float32Array;
  /** Start vertex of each path, only for links */
  startIndices?: Uint32Array;
  /** Position halfway along each path and its direction in degrees counterclockwise from east, only for links */
  arrowPositions?: Float32Array;
  arrowAngles?: Float32Array;
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
  /** Index within its group, for each node or link row */
  nodeLocal: Int32Array;
  linkLocal: Int32Array;
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

export async function loadNetwork(manifest: Manifest): Promise<Network> {
  const [nodes, links] = await Promise.all([
    readColumns(manifest.files.nodes, ["node_id", "node_type", "x", "y"]),
    readColumns(manifest.files.links, ["link_id", "link_type", "from_node_id", "to_node_id", "coords"]),
  ]);

  const nodeId = Int32Array.from(nodes.node_id as ArrayLike<number>);
  const nodeType = toStrings(nodes.node_type);
  const nodeX = Float32Array.from(nodes.x as ArrayLike<number>);
  const nodeY = Float32Array.from(nodes.y as ArrayLike<number>);
  const nodeLocal = new Int32Array(nodeId.length);
  const nodeGroups = [...rowsByType(nodeType)].map(([type, rows]) => {
    const positions = new Float32Array(rows.length * 2);
    rows.forEach((row, i) => {
      positions[2 * i] = nodeX[row];
      positions[2 * i + 1] = nodeY[row];
      nodeLocal[row] = i;
    });
    return { type, rows: Int32Array.from(rows), positions };
  });

  const linkId = Int32Array.from(links.link_id as ArrayLike<number>);
  const linkType = toStrings(links.link_type);
  const coords = links.coords as ArrayLike<ArrayLike<number>>;
  const linkLocal = new Int32Array(linkId.length);
  const linkGroups = [...rowsByType(linkType)].map(([type, rows]) => {
    const startIndices = new Uint32Array(rows.length);
    let vertices = 0;
    rows.forEach((row, i) => {
      startIndices[i] = vertices;
      vertices += coords[row].length / 2;
      linkLocal[row] = i;
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
    return { type, rows: Int32Array.from(rows), positions, startIndices, arrowPositions, arrowAngles };
  });

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
    fromNodeId: Int32Array.from(links.from_node_id as ArrayLike<number>),
    toNodeId: Int32Array.from(links.to_node_id as ArrayLike<number>),
    linkRow: indexById(linkId),
    linkGroups,
    linkLocal,
  };
}

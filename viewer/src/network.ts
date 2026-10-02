import { readColumns, type Manifest } from "./data";

/** Rows of one node or link type, with positions laid out for deck.gl binary attributes. */
export interface Group {
  type: string;
  rows: Int32Array;
  positions: Float32Array;
  /** Start vertex of each path, only for links */
  startIndices?: Uint32Array;
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
    rows.forEach((row, i) => positions.set(coords[row], startIndices[i] * 2));
    return { type, rows: Int32Array.from(rows), positions, startIndices };
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

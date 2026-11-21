/**
 * Match the nodes and links of the base model of a comparison with those of the head model, like
 * `ribasim_nl.webmap_diff`. No imports, so Node runs its tests as they are, see `matching.test.ts`.
 */

/** The ids of links and of their nodes, by row. */
export interface LinkEnds {
  link_id: unknown[];
  from_node_id: unknown[];
  to_node_id: unknown[];
}

/**
 * The id in a comparison of each node of the base model: its id in the head model, which is another one if it was
 * renumbered, or its negated id if it was removed, since another node of the head model can have that id.
 * `baseIds` has the id in the base model of the renumbered nodes, by their id in the head model.
 */
export function nodesFromBase(
  nodeIds: unknown[],
  baseIds: Map<number, number>,
  removed: (id: number) => boolean,
): Map<number, number> {
  const renumbered = new Map([...baseIds].map(([id, baseId]) => [baseId, id]));
  const nodes = new Map<number, number>();
  for (const value of nodeIds) {
    const id = Number(value);
    nodes.set(id, removed(id) ? -id : (renumbered.get(id) ?? id));
  }
  return nodes;
}

/**
 * The base id of each link in both models by head id, matched on the from and to node, since links get renumbered;
 * `headNode` gives the id in the head model of a node of the base model, see `nodesFromBase`.
 */
export function matchLinks(head: LinkEnds, base: LinkEnds, headNode: (baseId: number) => number): Map<number, number> {
  const key = (from: unknown, to: unknown) => `${from},${to}`;
  const baseIds = new Map(
    base.link_id.map((id, row) => [
      key(headNode(Number(base.from_node_id[row])), headNode(Number(base.to_node_id[row]))),
      Number(id),
    ]),
  );
  const matched = new Map<number, number>();
  head.link_id.forEach((id, row) => {
    const baseId = baseIds.get(key(head.from_node_id[row], head.to_node_id[row]));
    if (baseId !== undefined) matched.set(Number(id), baseId);
  });
  return matched;
}

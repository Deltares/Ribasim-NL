import assert from "node:assert/strict";
import { test } from "node:test";
import { isId, matchLinks, nodeRef, nodesByRef, nodesFromBase } from "./matching.ts";

// Base nodes 1, 2 and 3 are nodes 2, 3 and 1 of the head model, base node 4 is removed and base node 5 keeps its id;
// node 4 of the head model is added
const baseIds = new Map([
  [2, 1],
  [3, 2],
  [1, 3],
]);
const removed = (id: number) => id === 4;

test("nodesFromBase gives base nodes their head id, and removed nodes their negated id", () => {
  const nodes = nodesFromBase([1, 2, 3, 4, 5], baseIds, removed);
  assert.deepEqual([...nodes], [[1, 2], [2, 3], [3, 1], [4, -4], [5, 5]]);
  // BigInt ids, as they are read from Parquet
  assert.deepEqual([...nodesFromBase([1n, 4n], baseIds, removed)], [[1, 2], [4, -4]]);
});

test("nodeRef names a node by its water authority and key", () => {
  assert.equal(nodeRef("Rijkswaterstaat", "Markermeer"), "Rijkswaterstaat:Markermeer");
  assert.equal(nodeRef(null, "up:GA1-st5"), ":up:GA1-st5");
  assert.equal(nodeRef("Rijkswaterstaat", null), null);
  assert.equal(nodeRef("Rijkswaterstaat", ""), null);
  // A reference is never read as an id, also if the key is a number
  assert.equal(isId(nodeRef(null, "1278")!), false);
  assert.equal(isId("203892"), true);
  assert.equal(isId("-12"), true);
});

test("nodesByRef leaves out the references of several nodes", () => {
  const nodes = nodesByRef([1, 2, 3, -4], ["a:x", "a:y", "a:x", null]);
  assert.deepEqual([...nodes], [["a:y", 2]]);
});

test("matchLinks matches links between renumbered nodes", () => {
  const nodes = nodesFromBase([1, 2, 3, 4, 5], baseIds, removed);
  const headNode = (id: number) => nodes.get(id) ?? id;
  const base = { link_id: [10, 11, 12, 13], from_node_id: [1, 2, 3, 4], to_node_id: [2, 3, 4, 5] };
  // Link 20 is base link 10 and link 21 is base link 11; link 22 goes to the added node 4, not to the removed one
  const head = { link_id: [20, 21, 22], from_node_id: [2, 3, 1], to_node_id: [3, 1, 4] };
  assert.deepEqual([...matchLinks(head, base, headNode)], [[20, 10], [21, 11]]);
  // Without renumbering, links are matched on the same node ids
  assert.deepEqual([...matchLinks(head, base, (id) => id)], [[20, 11]]);
});

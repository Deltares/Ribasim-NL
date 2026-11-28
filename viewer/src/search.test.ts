import assert from "node:assert/strict";
import { test } from "node:test";
import {
  isPlaceQuery,
  matchRank,
  normalize,
  parseId,
  parsePlaces,
  searchedColumns,
  TextIndex,
  wktBounds,
} from "./search.ts";

test("normalize ignores case, accents and extra spaces", () => {
  assert.equal(normalize("  Wetterskip  Fryslân "), "wetterskip fryslan");
  assert.equal(normalize("ĲSSELMEER"), "ijsselmeer");
});

test("parseId reads ids, optionally only of nodes or links", () => {
  assert.deepEqual(parseId("203892"), { id: 203892, kind: null });
  assert.deepEqual(parseId(" #12 "), { id: 12, kind: null });
  assert.deepEqual(parseId("n12"), { id: 12, kind: "node" });
  assert.deepEqual(parseId("Node #12"), { id: 12, kind: "node" });
  assert.deepEqual(parseId("l 200001"), { id: 200001, kind: "link" });
  assert.equal(parseId("n12a"), null);
  assert.equal(parseId("Lelystad"), null);
});

test("matchRank prefers equal values, then prefixes, word starts and other occurrences", () => {
  assert.equal(matchRank("gemaal blauwe molen", "gemaal blauwe molen"), 0);
  assert.equal(matchRank("gemaal blauwe molen", "gemaal"), 1);
  assert.equal(matchRank("gemaal blauwe molen", "molen"), 2);
  assert.equal(matchRank("kgm-d-11420", "11420"), 2);
  // A later occurrence at the start of a word counts
  assert.equal(matchRank("kromolen molen", "molen"), 2);
  assert.equal(matchRank("kromolen", "molen"), 3);
  assert.equal(matchRank("gemaal de blocq van kuffeler", "kuffeler blocq"), 4);
  assert.equal(matchRank("gemaal de blocq van kuffeler", "blocq lely"), -1);
});

test("searchedColumns keeps names and codes, and leaves out categories", () => {
  // Names are searched also when many features share them, like those of the control nodes
  const table = {
    name: ["uitlaat", "uitlaat", "uitlaat", "uitlaat", "uitlaat", "uitlaat"],
    node_type: ["Pump", "Pump", "Pump", "Outlet", "Outlet", "Basin"],
    meta_code_waterbeheerder: ["KGM-1", "KGM-2", null, "", null, null],
    meta_rwzi_codeist: ["NL.1", "NL.1", "NL.2", "NL.2", null, null],
    meta_categorie: ["bergend", "bergend", "bergend", "bergend", "bergend", "doorgaand"],
    meta_streefpeil: [-0.4, -0.4, -0.4, -0.6, -0.6, -0.6],
  };
  assert.deepEqual(searchedColumns(table), ["name", "meta_code_waterbeheerder", "meta_rwzi_codeist"]);
});

test("TextIndex finds each feature once, by its best value", () => {
  const index = new TextIndex();
  index.add("node", 3, "name", "Gemaal Blauwe Molen");
  index.add("node", 3, "meta_code_waterbeheerder", "KGM-D-11420");
  index.add("node", 1, "name", "Molen");
  index.add("link", 2, "name", "Molen");
  index.add("node", 4, "meta_code_waterbeheerder", "MOLEN");
  index.add("node", 5, "name", "  ");
  index.add("node", 6, "name", null);
  const found = index.search("molen").map(({ kind, id, column }) => `${kind} ${id} ${column}`);
  assert.deepEqual(found, ["node 1 name", "link 2 name", "node 4 meta_code_waterbeheerder", "node 3 name"]);
  assert.deepEqual(
    index.search("kgm d").map(({ id, value, rank }) => [id, value, rank]),
    [[3, "KGM-D-11420", 4]],
  );
  assert.equal(index.name("node", 3), "Gemaal Blauwe Molen");
  assert.equal(index.name("link", 3), undefined);
  // A single character matches about everything
  assert.deepEqual(index.search("m"), []);
});

test("wktBounds spans all coordinates of a geometry", () => {
  assert.deepEqual(wktBounds("POINT(5.37476449 52.54272549)"), [5.37476449, 52.54272549, 5.37476449, 52.54272549]);
  assert.deepEqual(wktBounds("MULTIPOLYGON(((5 52,6 53.5,4.5 52)),((-1 51,5 52,5 52)))"), [-1, 51, 6, 53.5]);
  assert.equal(wktBounds("POLYGON EMPTY"), null);
});

test("places are searched for names, not for ids", () => {
  assert.ok(isPlaceQuery("Ee"));
  assert.ok(isPlaceQuery("'s-Hertogenbosch"));
  assert.ok(!isPlaceQuery("E"));
  assert.ok(!isPlaceQuery("203892"));
  assert.ok(!isPlaceQuery("n203892"));
});

test("parsePlaces reads towns, municipalities and provinces with a center", () => {
  const response = {
    response: {
      docs: [
        {
          id: "wpl-1",
          type: "woonplaats",
          weergavenaam: "Lelystad, Lelystad, Flevoland",
          centroide_ll: "POINT(5.37 52.54)",
        },
        { id: "weg-1", type: "weg", weergavenaam: "Markermeer, Oss", centroide_ll: "POINT(5.5 51.7)" },
        { id: "gem-1", type: "gemeente", weergavenaam: "Gemeente Lelystad" },
      ],
    },
  };
  assert.deepEqual(parsePlaces(response), [
    { id: "wpl-1", name: "Lelystad, Lelystad, Flevoland", type: "woonplaats", center: [5.37, 52.54] },
  ]);
  assert.throws(() => parsePlaces({ error: "Bad request" }));
});

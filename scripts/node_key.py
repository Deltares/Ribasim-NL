"""Look up the key of a node of the coupled model by the node ID it had in an earlier version of the model.

Issues, pull requests and feedback name nodes by their node ID, which is only right for the model of that moment,
see `docs/dev/node-keys.qmd`. This finds such a node in the model of a commit, or of `main` on a date, and prints
its water authority and key, its node ID in the latest model and a link to it in the model viewer, as a Markdown
table that can be pasted into an issue.

    pixi run node-key 1100540 1100695 --date 2026-11-19
    pixi run node-key 203892 --commit 21ce19f26c
    pixi run node-key --link 1100306 --date 2026-11-19

Without `--commit` or `--date` the IDs are those of the latest model, that of `--now`, by default `origin/main`.
`--link` takes link IDs: a link has no key, so it is found by the keys of its two nodes.

The nodes and links of a commit are read from its model in the model viewer: the snapshot that is named by the hash
of the `export_webmap` output in its `dvc.lock`, see `ribasim_nl.webmap_publish`. No credentials are needed. A commit
whose model is no longer published cannot be looked up; publish it again with `pixi run publish-webmap --commit`.

Models from before November 2026 have no keys. Their nodes are matched with the first model with keys, that of
`FIRST_KEYED_COMMIT`: the node of the same type at the same location, else the one of the same type and water
authority with the same name, else the node with the same ID and type, which then moved. The table says how each
node was matched, and how far a node that is matched by its ID moved: check those that moved far. A Junction is only
matched by its location, since the Junction trees are rebuilt in every run. `scripts/prune_remote.py` keeps the model
of `FIRST_KEYED_COMMIT` published.
"""

import argparse
import json
import re
import subprocess
import sys
import urllib.parse
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import requests
from ribasim_nl.keys import AUTHORITY_COLUMN, KEY_COLUMN
from ribasim_nl.webmap_local import resolve_commit
from ribasim_nl.webmap_publish import (
    BUCKET_NAME,
    MINIO_SERVER,
    MODEL,
    PREFIX,
    STORED_FILE,
    VIEWER_URL,
    export_hash,
    lock_at,
    snapshot_name,
)

REPO_DIR = Path(__file__).resolve().parents[1]
CACHE_DIR = REPO_DIR / ".cache" / "node_key"
STORE_URL = f"https://{MINIO_SERVER}/{BUCKET_NAME}/{PREFIX}"
MAIN = "origin/main"
# The manifest of a snapshot of the model viewer, see `ribasim_nl.webmap_publish.snapshot_name`
SNAPSHOT = re.compile(rf"{re.escape(MODEL)}/snapshots/[0-9a-f]{{32}}/manifest\.json")
# The first run of main in which the nodes have a key, of #1067
FIRST_KEYED_COMMIT = "cdecdaf91bc715377e2293c821c79ddf8381acba"
NODE_COLUMNS = ["node_id", "node_type", "name", "x", "y", KEY_COLUMN, AUTHORITY_COLUMN]
LINK_COLUMNS = ["link_id", "link_type", "from_node_id", "to_node_id"]
# Nodes at the same longitude and latitude within this number of decimals, about a centimetre, are at the same place
LOCATION_DECIMALS = 7
# The node IDs of the coupled model are the `waterbeheercode` of the authority times this, plus its ID in its own model
AUTHORITY_FACTOR = 100_000
EARTH_RADIUS = 6_371_000.0  # m
COMMIT_LENGTH = 10


@dataclass(eq=False)
class Version:
    """The nodes and links of the model of a commit, indexed by their ID."""

    commit: str
    nodes: pd.DataFrame
    links: pd.DataFrame

    @property
    def has_keys(self) -> bool:
        """Whether the nodes of this model have a key."""
        return bool(self.nodes[KEY_COLUMN].notna().any())

    @cached_property
    def references(self) -> pd.Series:
        """The reference of each node with a key, see `references`."""
        return references(self.nodes).dropna()

    @cached_property
    def node_ids(self) -> dict[str, int]:
        """The node ID by reference."""
        assert self.references.is_unique, f"Nodes of a water authority with the same key in {self.commit}"
        return dict(zip(self.references.to_numpy(), self.references.index.to_numpy().tolist(), strict=True))


def git(*args: str) -> str:
    """Run a git command in this repository and return its stripped output."""
    # The arguments are constants, the hash of a commit, see `resolve`, and a formatted date, see `main_on`
    result = subprocess.run(["git", *args], check=True, capture_output=True, text=True, cwd=REPO_DIR)  # NOSONAR
    return result.stdout.strip()


def resolve(revision: str) -> str:
    """The commit hash of a git revision."""
    commit = resolve_commit(revision, REPO_DIR)
    assert commit is not None, f"{revision} is not a git revision of this repository"
    return commit


def main_on(date: str) -> str:
    """The commit of `main` at the end of a date, like 2026-11-19."""
    day = pd.Timestamp(date)
    assert day == day.normalize(), f"Expected a date without a time, got {date}"
    commit = git("rev-list", "--max-count=1", f"--before={day:%Y-%m-%d}T23:59:59", MAIN)
    assert commit, f"{MAIN} has no commit on or before {date}"
    return commit


def download(name: str) -> Path:
    """Download a file of the model viewer, unless it is in the cache; return its path there.

    Raises a FileNotFoundError if the model viewer has no such file.
    """
    # The name comes from a manifest: only the manifest of a snapshot and the files it stores are read
    is_file = name.startswith("files/") and STORED_FILE.fullmatch(name.removeprefix("files/"))
    assert is_file or SNAPSHOT.fullmatch(name), f"Not a file of a model in the model viewer: {name}"
    path = CACHE_DIR / name
    if not path.is_file():
        response = requests.get(STORE_URL + name, timeout=300)  # NOSONAR: the name is checked above
        if response.status_code == 404:
            raise FileNotFoundError(STORE_URL + name)
        response.raise_for_status()
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(f"{path.name}.partial")
        partial.write_bytes(response.content)
        partial.replace(path)
    return path


def read_table(name: str, columns: list[str], index: str) -> pd.DataFrame:
    """Read columns of a Parquet file of the model viewer, indexed by an ID column; missing columns are empty."""
    path = download(f"files/{name}")
    available = set(pq.read_schema(path).names)
    assert index in available, f"{name} has no column {index}"
    table = pd.read_parquet(path, columns=[column for column in columns if column in available])
    table = table.reindex(columns=columns).set_index(index)
    assert table.index.is_unique, f"Duplicate {index} in {name}"
    return table


def read_version(commit: str) -> Version:
    """Read the nodes and links of the model of a commit from its snapshot in the model viewer."""
    md5 = export_hash(lock_at(commit))
    try:
        manifest = json.loads(download(snapshot_name(md5)).read_text())
    except FileNotFoundError:
        sys.exit(
            f"The model of {commit[:COMMIT_LENGTH]} is not published in the model viewer, snapshot {md5}. "
            f"Publish it with `pixi run publish-webmap --commit {commit[:COMMIT_LENGTH]}`."
        )
    files = manifest["files"]
    return Version(
        commit,
        read_table(files["nodes"]["path"], NODE_COLUMNS, "node_id"),
        read_table(files["links"]["path"], LINK_COLUMNS, "link_id"),
    )


def references(nodes: pd.DataFrame) -> pd.Series:
    """The reference of each node, `<authority>:<key>` as in a link to the model viewer, missing without a key."""
    authority = nodes[AUTHORITY_COLUMN].astype(object).where(nodes[AUTHORITY_COLUMN].notna(), "")
    reference = authority.astype(str) + ":" + nodes[KEY_COLUMN].astype(str)
    return reference.where(nodes[KEY_COLUMN].notna()).astype(object)


def distance(a: pd.Series, b: pd.Series) -> float:
    """The distance in m between two nodes, from their longitude `x` and latitude `y`."""
    latitude = np.radians((a["y"] + b["y"]) / 2)
    dx = np.radians(b["x"] - a["x"]) * np.cos(latitude)
    dy = np.radians(b["y"] - a["y"])
    return float(EARTH_RADIUS * np.hypot(dx, dy))


def distance_text(metres: float) -> str:
    """A distance for reading."""
    return f"{metres:.0f} m" if metres < 1000 else f"{metres / 1000:.1f} km"


def name_of(node: pd.Series) -> str:
    """The name of a node, empty if it has none."""
    return node["name"].strip() if isinstance(node["name"], str) else ""


def at_location(nodes: pd.DataFrame, node: pd.Series) -> pd.Index:
    """The IDs of the nodes of the type and at the location of a node."""
    same = (
        (nodes["node_type"] == node["node_type"])
        & (nodes["x"].round(LOCATION_DECIMALS) == round(node["x"], LOCATION_DECIMALS))
        & (nodes["y"].round(LOCATION_DECIMALS) == round(node["y"], LOCATION_DECIMALS))
    )
    return nodes.index[same.to_numpy()]


def named(nodes: pd.DataFrame, node_id: int, node: pd.Series) -> pd.Index:
    """The IDs of the nodes of the type and water authority of a node, with its name.

    The water authority follows from the node ID, see `AUTHORITY_FACTOR`.
    """
    same = (
        (nodes["node_type"] == node["node_type"])
        & (nodes.index // AUTHORITY_FACTOR == node_id // AUTHORITY_FACTOR)
        & (
            nodes["name"].astype(object).map(lambda name: name.strip() if isinstance(name, str) else "")
            == name_of(node)
        )
    )
    return nodes.index[same.to_numpy()]


def match_keyless(node_id: int, then: pd.DataFrame, keyed: pd.DataFrame) -> tuple[int | None, str]:
    """The node of a model with keys that is a node of a model without keys, and how it was matched.

    In this order: the one node of the same type at the same location, the one node of the same type and water
    authority with the same name, and the node with the same ID and type, with the distance it moved. A Junction is
    only matched by its location, and a node is not matched by its ID with a node of another name.
    """
    node = cast(pd.Series, then.loc[node_id])
    here = at_location(keyed, node)
    if len(here) == 1 and len(at_location(then, node)) == 1:
        return int(here[0]), "location"
    if name_of(node):
        same_name = named(keyed, node_id, node)
        if len(same_name) == 1 and len(named(then, node_id, node)) == 1:
            return int(same_name[0]), "name"
    if node["node_type"] == "Junction":
        return None, "a Junction that is not at this location anymore: the Junction trees are rebuilt in every run"
    if node_id not in keyed.index or keyed.at[node_id, "node_type"] != node["node_type"]:
        return (
            None,
            "no node of this type at this location, with this name or with this ID in the first model with keys",
        )
    other = cast(pd.Series, keyed.loc[node_id])
    if name_of(node) and name_of(other) and name_of(node) != name_of(other):
        return None, f"the node with this ID in the first model with keys has another name, {name_of(other)}"
    return node_id, f"node ID, {distance_text(distance(node, other))} away"


def lookup_node(node_id: int, then: Version, keyed: Version | None, now: Version) -> dict:
    """A node of the model `then`, with its key and its ID in the model `now`, and how it was matched.

    `keyed` is the first model with keys, to match a model `then` without keys with.
    """
    result: dict = {"node_id": node_id, "node_type": None, "name": None, "reference": None, "now_id": None}
    if node_id not in then.nodes.index:
        return result | {"match": "not a node of this model"}
    node = cast(pd.Series, then.nodes.loc[node_id])
    result |= {"node_type": node["node_type"], "name": name_of(node)}
    if then.has_keys:
        reference, match = then.references.get(node_id), "key"
        if reference is None:
            return result | {"match": "has no key"}
    else:
        assert keyed is not None, "A model without keys needs the first model with keys"
        keyed_id, match = match_keyless(node_id, then.nodes, keyed.nodes)
        if keyed_id is None:
            return result | {"match": match}
        reference = keyed.references.get(keyed_id)
        assert reference is not None, f"Node {keyed_id} of {keyed.commit} has no key"
    now_id = now.node_ids.get(reference)
    if now_id is None:
        return result | {"reference": reference, "match": f"{match}; not in the latest model"}
    return result | {"reference": reference, "now_id": now_id, "match": match}


def lookup_link(link_id: int, then: Version, keyed: Version | None, now: Version) -> dict:
    """A link of the model `then`, with the keys of its nodes and its ID in the model `now`.

    A link has no key: it is the link between the same two nodes. The Junction trees are rebuilt in every run, so
    a link between a Junction and another node is the one link on that side of the other node, if it has one.
    """
    result: dict = {"link_id": link_id, "link_type": None, "from": None, "to": None, "now_id": None}
    if link_id not in then.links.index:
        return result | {"match": "not a link of this model"}
    link = cast(pd.Series, then.links.loc[link_id])
    ends = {column: lookup_node(int(link[column]), then, keyed, now) for column in ("from_node_id", "to_node_id")}
    result |= {"link_type": link["link_type"], "from": ends["from_node_id"], "to": ends["to_node_id"]}
    links = now.links[now.links["link_type"] == link["link_type"]]
    found = {column: end["now_id"] for column, end in ends.items() if end["now_id"] is not None}
    if len(found) == 2:
        both = links.index[(links[list(found)] == pd.Series(found)).all(axis=1).to_numpy()]
        if len(both) == 1:
            return result | {"now_id": int(both[0]), "match": "its nodes"}
    # the node at the other side of a Junction, which has one link on that side if it is a connector node
    for column, other in (("from_node_id", "to_node_id"), ("to_node_id", "from_node_id")):
        if ends[other]["node_type"] == "Junction" and column in found and ends[column]["node_type"] != "Junction":
            side = links.index[(links[column] == found[column]).to_numpy()]
            if len(side) == 1:
                which = "from" if column == "from_node_id" else "to"
                return result | {"now_id": int(side[0]), "match": f"its {which} node"}
    return result | {"match": "no single link between or at these nodes in the latest model"}


def viewer_link(reference: str) -> str:
    """A Markdown link to a node in the model viewer by its reference, which shows it in every version with keys."""
    url = f"{VIEWER_URL}#node={urllib.parse.quote(reference, safe=':@')}"
    return f"[{cell(reference)}]({url})"


def cell(value: object) -> str:
    """A value as the text of a cell of a Markdown table."""
    return "" if value is None else str(value).replace("|", "\\|").replace("\n", " ")


def node_cell(node: dict) -> str:
    """The type, name and key of a looked up node."""
    if node["node_type"] is None:
        return f"#{node['node_id']}: {node['match']}"
    label = cell(f"{node['node_type']} {node['name']}".strip())
    return f"{label}, {viewer_link(node['reference'])}" if node["reference"] else f"{label}: {node['match']}"


def markdown_table(header: list[str], rows: list[list[object]]) -> str:
    """A Markdown table."""
    lines = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
    lines += ["| " + " | ".join("" if value is None else str(value) for value in row) + " |" for row in rows]
    return "\n".join(lines)


def report(node_ids: list[int], link_ids: list[int], then: Version, keyed: Version | None, now: Version) -> str:
    """The looked up nodes and links as Markdown tables."""
    tables = []
    if node_ids:
        nodes = [lookup_node(node_id, then, keyed, now) for node_id in node_ids]
        rows = [
            [
                node["node_id"],
                cell(node["node_type"]),
                cell(node["name"]),
                viewer_link(node["reference"]) if node["reference"] else "",
                node["now_id"],
                node["match"],
            ]
            for node in nodes
        ]
        tables.append(markdown_table(["node ID", "type", "name", "key", "node ID now", "matched by"], rows))
    if link_ids:
        links = [lookup_link(link_id, then, keyed, now) for link_id in link_ids]
        rows = [
            [
                link["link_id"],
                cell(link["link_type"]),
                node_cell(link["from"]) if link["from"] else "",
                node_cell(link["to"]) if link["to"] else "",
                link["now_id"],
                link["match"],
            ]
            for link in links
        ]
        tables.append(markdown_table(["link ID", "type", "from", "to", "link ID now", "matched by"], rows))
    return "\n\n".join(tables)


def main(argv: list[str] | None = None) -> None:
    """Look up the nodes and links given on the command line."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("node_ids", nargs="*", type=int, help="node IDs to look up")
    parser.add_argument("--link", nargs="+", type=int, default=[], metavar="LINK_ID", help="link IDs to look up")
    when = parser.add_mutually_exclusive_group()
    when.add_argument("--commit", help="git revision whose model has these IDs")
    when.add_argument("--date", help="date on which the model of main had these IDs, like 2026-11-19")
    parser.add_argument("--now", default=MAIN, help=f"git revision of the latest model, by default {MAIN}")
    args = parser.parse_args(argv)
    assert args.node_ids or args.link, "Give at least one node ID or link ID"

    git("fetch", "--quiet", "origin", "main")
    now_commit = resolve(args.now)
    then_commit = resolve(args.commit) if args.commit else main_on(args.date) if args.date else now_commit
    now = read_version(now_commit)
    assert now.has_keys, f"The model of {args.now} has no keys"
    then = now if then_commit == now_commit else read_version(then_commit)
    keyed = None if then.has_keys else read_version(resolve(FIRST_KEYED_COMMIT))
    date = git("show", "--no-patch", "--format=%cs", then.commit)
    print(
        f"IDs in the model of {then.commit[:COMMIT_LENGTH]} of {date}, "
        f"and in the model of {now.commit[:COMMIT_LENGTH]} now.\n"
    )
    print(report(args.node_ids, args.link, then, keyed, now))


if __name__ == "__main__":
    main()

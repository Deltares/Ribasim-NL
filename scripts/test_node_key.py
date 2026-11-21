"""Tests for looking up nodes and links by an earlier ID in scripts/node_key.py"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from node_key import (
    Version,
    lookup_link,
    lookup_node,
    match_keyless,
    references,
    report,
)

# Node IDs of two water authorities, see `AUTHORITY_FACTOR`
A, B = 1_100_000, 1_200_000


def nodes(rows: dict[int, tuple]) -> pd.DataFrame:
    """Nodes by ID, from (node_type, name, x, y, key, authority)."""
    columns = ["node_type", "name", "x", "y", "meta_key", "meta_waterbeheerder"]
    return pd.DataFrame.from_dict(rows, orient="index", columns=columns).rename_axis("node_id")


def links(rows: dict[int, tuple]) -> pd.DataFrame:
    """Links by ID, from (link_type, from_node_id, to_node_id)."""
    columns = ["link_type", "from_node_id", "to_node_id"]
    return pd.DataFrame.from_dict(rows, orient="index", columns=columns).rename_axis("link_id")


def keyless() -> Version:
    """A model without keys: two Basins of authority A, with a pump, an outlet and a Junction between them."""
    return Version(
        "then",
        nodes(
            {
                A + 1: ("Basin", "Polder", 5.0, 52.0, None, None),
                A + 2: ("Basin", "", 5.1, 52.0, None, None),
                A + 3: ("Pump", "Gemaal A", 5.04, 52.0, None, None),
                A + 4: ("Outlet", "", 5.06, 52.0, None, None),
                A + 5: ("Junction", "", 5.05, 52.0, None, None),
                A + 6: ("Outlet", "Stuw", 5.07, 52.0, None, None),
                # of another authority, with the name of the pump
                B + 3: ("Pump", "Gemaal A", 6.0, 53.0, None, None),
            }
        ),
        links(
            {
                10: ("flow", A + 1, A + 5),
                11: ("flow", A + 5, A + 3),
                12: ("flow", A + 3, A + 2),
                13: ("flow", A + 1, A + 4),
                14: ("flow", A + 4, A + 2),
            }
        ),
    )


def keyed() -> Version:
    """The same model with keys: the pump moved and has another ID, the outlet moved, the Junction is another one."""
    return Version(
        "keyed",
        nodes(
            {
                A + 1: ("Basin", "Polder", 5.0, 52.0, "polder", "AGV"),
                A + 2: ("Basin", "", 5.1, 52.0, "boezem", "AGV"),
                A + 9: ("Pump", "Gemaal A", 5.041, 52.0, "KGM1", "AGV"),
                A + 4: ("Outlet", "", 5.061, 52.0, "KST1", "AGV"),
                A + 5: ("Junction", "", 5.03, 52.0, "junction:polder>KGM1#1x1", "AGV"),
                A + 6: ("Outlet", "Andere stuw", 5.07, 52.1, "KST2", "AGV"),
                A + 3: ("Pump", "", 7.0, 52.0, "KGM9", "AGV"),
                B + 3: ("Pump", "Gemaal A", 6.0, 53.0, "KGM1", "HHNK"),
            }
        ),
        links(
            {
                20: ("flow", A + 1, A + 5),
                21: ("flow", A + 5, A + 9),
                22: ("flow", A + 9, A + 2),
                23: ("flow", A + 1, A + 4),
                24: ("flow", A + 4, A + 2),
            }
        ),
    )


def renumbered() -> Version:
    """The model with keys after its nodes and links got other IDs; the outlet KST2 is gone."""
    model = keyed()
    node_ids = {node_id: node_id + 50 for node_id in model.nodes.index}
    model_nodes = model.nodes.rename(index=node_ids).drop(index=A + 56)
    model_links = model.links.rename(index=lambda link_id: link_id + 100).replace(
        {"from_node_id": node_ids, "to_node_id": node_ids}
    )
    return Version("now", model_nodes, model_links)


def test_references():
    table = nodes(
        {
            1: ("Basin", "", 5.0, 52.0, "Markermeer", "Rijkswaterstaat"),
            2: ("Basin", "", 5.0, 52.0, "up:st1", None),
            3: ("Basin", "", 5.0, 52.0, None, "Rijkswaterstaat"),
        }
    )
    assert references(table).to_list()[:2] == ["Rijkswaterstaat:Markermeer", ":up:st1"]
    assert pd.isna(references(table)[3])


def test_match_keyless():
    then, first = keyless().nodes, keyed().nodes
    # at the same location
    assert match_keyless(A + 1, then, first) == (A + 1, "location")
    # moved, and with another ID: by its name, within its water authority
    assert match_keyless(A + 3, then, first) == (A + 9, "name")
    assert match_keyless(B + 3, then, first) == (B + 3, "location")
    # moved, without a name: by its ID, with the distance
    node_id, match = match_keyless(A + 4, then, first)
    assert node_id == A + 4 and match == "node ID, 68 m away"
    # a Junction is only matched by its location
    node_id, match = match_keyless(A + 5, then, first)
    assert node_id is None and match.startswith("a Junction")
    # the node with this ID has another name
    node_id, match = match_keyless(A + 6, then, first)
    assert node_id is None and match.endswith("another name, Andere stuw")


def test_match_keyless_ambiguous_location():
    """Two nodes of a type at one location are not matched by their location."""
    then = nodes({1: ("Pump", "", 5.0, 52.0, None, None), 2: ("Pump", "", 5.0, 52.0, None, None)})
    first = nodes({1: ("Pump", "", 5.0, 52.0, "a", "AGV"), 2: ("Pump", "", 5.0, 52.0, "b", "AGV")})
    assert match_keyless(2, then, first) == (2, "node ID, 0 m away")


def test_lookup_node():
    then, first, now = keyless(), keyed(), renumbered()
    # a model without keys goes through the first model with keys
    pump = lookup_node(A + 3, then, first, now)
    assert pump == {
        "node_id": A + 3,
        "node_type": "Pump",
        "name": "Gemaal A",
        "reference": "AGV:KGM1",
        "now_id": A + 59,
        "match": "name",
    }
    # a model with keys is matched by key, also in another water authority with the same key
    assert lookup_node(B + 3, first, None, now) | {"node_id": 0} == pump | {"node_id": 0, "reference": "HHNK:KGM1"} | {
        "now_id": B + 53,
        "match": "key",
    }
    assert lookup_node(A + 6, first, None, now)["match"] == "key; not in the latest model"
    assert lookup_node(99, first, None, now)["match"] == "not a node of this model"


def test_lookup_link():
    then, first, now = keyless(), keyed(), renumbered()
    # between the same two nodes
    assert lookup_link(13, then, first, now)["now_id"] == 123
    assert lookup_link(13, then, first, now)["match"] == "its nodes"
    # from a Junction that is gone: the one link to the pump
    link = lookup_link(11, then, first, now)
    assert (link["now_id"], link["match"]) == (121, "its to node")
    # from a Basin to a Junction that is gone: the Basin has two links
    assert lookup_link(10, then, first, now)["now_id"] is None
    assert lookup_link(99, then, first, now)["match"] == "not a link of this model"


def test_report():
    then = Version("then", nodes({1: ("Outlet", "a|b", 5.0, 52.0, "KST 1|x>y", "AGV")}), links({}))
    text = report([1, 2], [], then, None, then)
    lines = text.splitlines()
    assert lines[0] == "| node ID | type | name | key | node ID now | matched by |"
    # the bars of the name and the key are escaped, and the link to the viewer is encoded
    assert lines[2] == (
        "| 1 | Outlet | a\\|b | [AGV:KST 1\\|x>y](https://ribasim.nl/viewer#node=AGV:KST%201%7Cx%3Ey) | 1 | key |"
    )
    assert lines[3] == "| 2 |  |  |  |  | not a node of this model |"

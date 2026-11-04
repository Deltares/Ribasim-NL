"""Tests for the comparison of the results of two runs in `ribasim_nl.run_comparison`."""

from dataclasses import replace

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from ribasim_nl.run_comparison import (
    Run,
    compare_areas,
    compare_connections,
    compare_exchange,
    compare_levels,
    compare_network,
    compare_observations,
    connections,
    input_changes,
    markdown_table,
    match_links,
    node_mapping,
    one_run_basins,
    profile_digests,
    renumber,
    report,
    top_flows,
    top_one_run_flows,
)
from shapely.geometry import Point

FRYSLAN = 200_000
RIJNLAND = 1_300_000
TIMES = pd.date_range("2018-01-01", periods=3)


def nodes(rows: dict[int, tuple[str, float]]) -> gpd.GeoDataFrame:
    """Nodes by ID, from their type and x coordinate."""
    return gpd.GeoDataFrame(
        {
            "node_type": [node_type for node_type, _ in rows.values()],
            "name": [f"n{id}" for id in rows],
            "meta_categorie": "doorgaand",
        },
        geometry=[Point(x, 0.0) for _, x in rows.values()],
        index=pd.Index(list(rows), name="node_id"),
    )


def make_run(
    rows: dict[int, tuple[str, float]],
    links: list[tuple[int, int, float]],
    levels: dict[int, list[float]],
    area: float = 1e6,
) -> Run:
    """A run with the given nodes, links with their flow, and Basin levels."""
    basins = pd.Index(list(levels))
    return Run(
        nodes=nodes(rows),
        links=pd.DataFrame(
            {
                "from_node_id": [a for a, _, _ in links],
                "to_node_id": [b for _, b, _ in links],
                "mean": [flow for _, _, flow in links],
                "dry": [flow for _, _, flow in links],
            }
        ),
        levels=pd.DataFrame(levels, index=TIMES),
        balance=pd.DataFrame(
            dict.fromkeys(("precipitation", "evaporation", "drainage", "infiltration"), 1.0), index=basins
        ),
        bottom=pd.Series(0.0, index=basins),
        streefpeil=pd.Series(1.0, index=basins),
        area=pd.Series(area, index=basins),
        profile=pd.Series(1, index=basins, dtype="uint64"),
        observations=pd.DataFrame(columns=["name", "kge", "nse", "bias"], index=pd.Index([], dtype="int64")),
        solver=pd.Series([1.0, 2.0, 3.0], index=TIMES),
    )


BASE_ROWS = {
    FRYSLAN + 1: ("Basin", 0.0),
    FRYSLAN + 2: ("Junction", 10.0),
    FRYSLAN + 3: ("Outlet", 20.0),
    RIJNLAND + 1: ("Basin", 30.0),
}


def test_node_mapping_matches_by_location_first():
    # The Junction got another ID at the same location, and the ID it had is taken by a node elsewhere
    base = nodes({1: ("Basin", 0.0), 2: ("Junction", 10.0), 3: ("Outlet", 20.0)})
    head = nodes({1: ("Basin", 0.0), 2: ("Outlet", 20.0), 3: ("Junction", 10.0)})
    assert node_mapping(base, head).to_dict() == {1: 1, 2: 3, 3: 2}


def test_node_mapping_moved_and_removed():
    base = nodes({1: ("Basin", 0.0), 2: ("Basin", 10.0), 3: ("Outlet", 20.0), 4: ("Pump", 50.0)})
    # 1 moved, the type of 3 changed so it is another node, and 4 is removed
    head = nodes({1: ("Basin", 5.0), 2: ("Basin", 10.0), 3: ("Pump", 30.0)})
    assert node_mapping(base, head).to_dict() == {1: 1, 2: 2, 3: -3, 4: -4}


def test_node_mapping_location_is_not_id():
    """A node that has moved does not take the ID of a node that is matched by location."""
    base = nodes({1: ("Basin", 0.0), 2: ("Basin", 10.0)})
    head = nodes({1: ("Basin", 10.0), 2: ("Basin", 99.0)})
    mapping = node_mapping(base, head)
    assert mapping[2] == 1
    assert mapping[1] == -1


def test_node_mapping_observations_by_name():
    """The layout moved Observations whose IDs shifted, so an ID now belongs to another series."""
    base = nodes({1: ("Observation", 0.0), 2: ("Observation", 10.0), 3: ("Observation", 20.0), 4: ("Basin", 30.0)})
    base["name"] = ["a", "b", "gone", ""]
    head = nodes({2: ("Observation", 1.0), 3: ("Observation", 11.0), 4: ("Basin", 30.0)})
    head["name"] = ["a", "b", ""]
    # Observation 3 of base is not matched with Observation 3 of head, which measures b
    assert node_mapping(base, head).to_dict() == {1: 2, 2: 3, 3: -3, 4: 4}


def test_renumber_hides_shifted_ids():
    base_rows = {1: ("Basin", 0.0), 2: ("Junction", 10.0), 3: ("Basin", 20.0)}
    head_rows = {1: ("Basin", 0.0), 5: ("Junction", 10.0), 3: ("Basin", 20.0)}
    levels = {1: [1.0, 1.0, 1.0], 3: [2.0, 2.0, 2.0]}
    base = make_run(base_rows, [(1, 2, 1.0), (2, 3, 1.0)], levels)
    head = make_run(head_rows, [(1, 5, 1.0), (5, 3, 1.0)], levels)
    renumbered = renumber(base, node_mapping(base.nodes, head.nodes))
    matched = match_links(renumbered, head)
    assert matched["mean_base"].notna().all()
    assert matched["mean_head"].notna().all()
    assert top_flows(matched).empty
    assert compare_network(renumbered, head).empty


def test_renumber_asserts_nodes_are_known():
    run = make_run({1: ("Basin", 0.0)}, [(1, 2, 1.0)], {1: [1.0, 1.0, 1.0]})
    mapping = pd.Series({1: 1})
    with pytest.raises(AssertionError, match="not in the Node table"):
        renumber(run, mapping)


def test_compare_levels():
    base = make_run(BASE_ROWS, [], {FRYSLAN + 1: [1.0, 1.0, 1.0], RIJNLAND + 1: [2.0, 2.0, 2.0]})
    head = make_run(BASE_ROWS, [], {FRYSLAN + 1: [1.0, 1.0, 1.5], RIJNLAND + 1: [2.0, 2.0, 2.0]})
    table = compare_levels(base, head)
    assert table.loc[FRYSLAN + 1, "authority"] == "WetterskipFryslan"
    assert table.loc[RIJNLAND + 1, "authority"] == "Rijnland"
    assert table.loc[RIJNLAND + 1, "change"] == 0.0
    # The largest of the changes of the mean and percentiles, with its sign
    assert table.loc[FRYSLAN + 1, "change"] == pytest.approx(0.49)
    assert table.loc[FRYSLAN + 1, "above_streefpeil"] == pytest.approx(3.5 / 3 - 1)


def test_input_changes():
    base = make_run(BASE_ROWS, [(FRYSLAN + 1, FRYSLAN + 3, 1.0)], {FRYSLAN + 1: [1.0] * 3, RIJNLAND + 1: [1.0] * 3})
    rows = BASE_ROWS | {RIJNLAND + 1: ("Basin", 130.0)}
    head = make_run(rows, [(FRYSLAN + 3, FRYSLAN + 1, 1.0)], {FRYSLAN + 1: [1.0] * 3, RIJNLAND + 1: [1.0] * 3})
    head.area[FRYSLAN + 1] = 2e6
    head.streefpeil[FRYSLAN + 1] = 1.5
    head.profile[FRYSLAN + 1] = 2
    changes = input_changes(base, head, pd.Index([FRYSLAN + 1, RIJNLAND + 1]))
    assert changes[FRYSLAN + 1] == "area +100%, streefpeil +0.50 m, profile, links"
    assert changes[RIJNLAND + 1] == "moved 100 m"


def test_connections_trace_through_junctions():
    links = [(FRYSLAN + 1, FRYSLAN + 2, 1.0), (FRYSLAN + 2, RIJNLAND + 1, 1.0), (FRYSLAN + 3, FRYSLAN + 1, 1.0)]
    run = make_run(BASE_ROWS, links, {FRYSLAN + 1: [1.0] * 3, RIJNLAND + 1: [1.0] * 3})
    assert connections(run) == {(FRYSLAN + 1, RIJNLAND + 1)}


def test_connections_of_removed_nodes_keep_their_authority():
    """The negated IDs of nodes only in the base run still belong to their authority."""
    rows = {-(FRYSLAN + 1): ("Basin", 0.0), FRYSLAN + 2: ("Basin", 1.0), RIJNLAND + 1: ("Basin", 2.0)}
    run = make_run(rows, [(-(FRYSLAN + 1), FRYSLAN + 2, 1.0), (-(FRYSLAN + 1), RIJNLAND + 1, 1.0)], {})
    assert connections(run) == {(-(FRYSLAN + 1), RIJNLAND + 1)}


def test_compare_connections():
    base = make_run(BASE_ROWS, [(FRYSLAN + 1, RIJNLAND + 1, 1.0)], {FRYSLAN + 1: [1.0] * 3, RIJNLAND + 1: [1.0] * 3})
    head = make_run(BASE_ROWS, [(RIJNLAND + 1, FRYSLAN + 3, 1.0)], {FRYSLAN + 1: [1.0] * 3, RIJNLAND + 1: [1.0] * 3})
    table = compare_connections(base, head)
    assert table.loc["base", "from_node_id"] == FRYSLAN + 1
    assert table.loc["head", "to_node_id"] == FRYSLAN + 3


def test_compare_exchange_nets_both_directions():
    levels = {FRYSLAN + 1: [1.0] * 3, RIJNLAND + 1: [1.0] * 3}
    base = make_run(BASE_ROWS, [(FRYSLAN + 1, RIJNLAND + 1, 5.0)], levels)
    head = make_run(BASE_ROWS, [(FRYSLAN + 1, RIJNLAND + 1, 5.0), (RIJNLAND + 1, FRYSLAN + 3, 2.0)], levels)
    table = compare_exchange(base, head)
    assert table.loc["WetterskipFryslan - Rijnland", "mean_base"] == 5.0
    assert table.loc["WetterskipFryslan - Rijnland", "mean_head"] == 3.0


def test_top_flows_ignores_small_changes_and_links_in_one_run():
    levels = {FRYSLAN + 1: [1.0] * 3, RIJNLAND + 1: [1.0] * 3}
    base = make_run(BASE_ROWS, [(1, 2, 1.0), (2, 3, 1.0), (3, 4, 1.0)], levels)
    head = make_run(BASE_ROWS, [(1, 2, 1.001), (2, 3, 3.0), (4, 5, 1.0)], levels)
    matched = match_links(base, head)
    assert top_flows(matched).index.to_list() == [(2, 3)]
    names = pd.Series({3: "three"})
    one_run = top_one_run_flows(matched, names)
    assert one_run.index.to_list() == ["base", "head"]
    assert one_run.loc["base", "from_name"] == "three"


def test_match_links_asserts_unique_links():
    levels = {FRYSLAN + 1: [1.0] * 3}
    run = make_run(BASE_ROWS, [(1, 2, 1.0), (1, 2, 2.0)], levels)
    with pytest.raises(AssertionError, match="Multiple flow links"):
        match_links(run, run)


def test_areas():
    levels = {FRYSLAN + 1: [1.0] * 3, RIJNLAND + 1: [1.0] * 3}
    base = make_run(BASE_ROWS, [], levels)
    head = make_run(BASE_ROWS, [], levels)
    head.area[FRYSLAN + 1] = 5e5
    table = compare_areas(base, head)
    assert table.index.to_list() == [FRYSLAN + 1]
    assert table.loc[FRYSLAN + 1, "relative"] == pytest.approx(-0.5)


def test_one_run_basins_reports_positive_ids():
    levels = {FRYSLAN + 1: [1.0] * 3}
    head = make_run(BASE_ROWS, [], levels)
    rows = BASE_ROWS | {-(RIJNLAND + 1): ("Basin", 30.0)}
    base = make_run(rows, [], levels | {-(RIJNLAND + 1): [1.0] * 3})
    table = one_run_basins(base, head)
    assert table.index.to_list() == [RIJNLAND + 1]
    assert table.loc[RIJNLAND + 1, "run"] == "base"
    assert table.loc[RIJNLAND + 1, "authority"] == "Rijnland"


def test_compare_observations():
    levels = {FRYSLAN + 1: [1.0] * 3}
    base = make_run(BASE_ROWS, [], levels)
    head = make_run(BASE_ROWS, [], levels)
    columns = {"name": ["a", "b"], "nse": [0.1, 0.2], "bias": [0.0, 0.0]}
    base = replace(base, observations=pd.DataFrame(columns | {"kge": [0.5, 0.5]}, index=[1, 2]))
    head = replace(head, observations=pd.DataFrame(columns | {"kge": [0.5, 0.9]}, index=[1, 2]))
    table = compare_observations(base, head)
    assert table.index[0] == 2
    assert table.loc[2, "kge_head"] == 0.9


def test_profile_digests():
    profile = pd.DataFrame({"node_id": [1, 1, 2, 2], "level": [0.0, 1.0, 0.0, 1.0], "area": [10.0, 20.0, 10.0, 20.0]})
    digests = profile_digests(profile)
    assert digests[1] == digests[2]
    assert digests.equals(profile_digests(profile.iloc[::-1]))
    # Below the rounding it is the same profile
    assert digests.equals(profile_digests(profile.assign(level=profile["level"] + 1e-5)))
    swapped = profile.assign(area=[20.0, 10.0, 10.0, 20.0])
    assert profile_digests(swapped)[1] != digests[1]


def test_markdown_table():
    table = pd.DataFrame({"name": ["a|b", None], "value": [1.2345, np.nan]}, index=pd.Index([1, 2], name="id"))
    assert (
        markdown_table(table, digits=2)
        == "| id | name | value |\n| --- | --- | --- |\n| 1 | a/b | 1.23 |\n| 2 |  |  |\n"
    )
    assert markdown_table(table.iloc[:0]) == "None.\n"


def test_report_with_shifted_ids():
    base_rows = {FRYSLAN + 1: ("Basin", 0.0), FRYSLAN + 2: ("Junction", 10.0), RIJNLAND + 1: ("Basin", 30.0)}
    head_rows = {FRYSLAN + 1: ("Basin", 0.0), FRYSLAN + 9: ("Junction", 10.0), RIJNLAND + 1: ("Basin", 30.0)}
    base = make_run(
        base_rows,
        [(FRYSLAN + 1, FRYSLAN + 2, 1.0), (FRYSLAN + 2, RIJNLAND + 1, 1.0)],
        {FRYSLAN + 1: [1.0] * 3, RIJNLAND + 1: [1.0] * 3},
    )
    head = make_run(
        head_rows,
        [(FRYSLAN + 1, FRYSLAN + 9, 1.0), (FRYSLAN + 9, RIJNLAND + 1, 4.0)],
        {FRYSLAN + 1: [1.0, 1.0, 2.0], RIJNLAND + 1: [1.0] * 3},
    )
    text = report(base, head, ("abc", "def"))
    assert "1 nodes have another ID" in text
    assert "0 of the 2 flow links are in only one run" in text
    assert f"| {FRYSLAN + 9} | {RIJNLAND + 1} | 1.000 | 4.000 |" in text
    assert "## Connections between authorities in only one run\n\nNone." in text

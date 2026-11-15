from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import basin, manning_resistance, pump
from ribasim_nl.crossings.basin_references import (
    BasinLookup,
    References2024,
    rating_curves_between_levels,
    remove_dangling_connectors,
    removed_basins,
    reversed_to_supply,
)
from shapely.geometry import Point, box

from ribasim_nl import Model

CRS = "EPSG:28992"


def _model(levels: dict[int, float]) -> Model:
    """A model with Basins of 100 m wide in a row, with the given target levels, joined by ManningResistances."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs=CRS)
    areas = []
    for i, (node_id, level) in enumerate(levels.items()):
        model.basin.add(
            Node(node_id, Point(100 * i + 50, 50)),
            [basin.Profile(level=[0.0, 1.0], area=[10.0, 10.0]), basin.State(level=[1.0])],
        )
        areas.append({"node_id": node_id, "meta_streefpeil": level, "geometry": box(100 * i, 0, 100 * i + 100, 100)})
    model.basin.area.df = gpd.GeoDataFrame(areas, crs=CRS)
    return model


def test_basin_lookup():
    model = _model({1: -1.0, 10: -2.0, 11: -3.0})
    # the Basin of 2024 covered Basins 1 and 10, and another one Basin 11
    reference = gpd.GeoDataFrame({"node_id": [1, 2]}, geometry=[box(0, 0, 200, 100), box(200, 0, 300, 100)], crs=CRS)
    lookup = BasinLookup(model.basin.area.df, reference)
    assert lookup.basins(1) == [1, 10]
    assert lookup.basins(2) == [11]
    assert not lookup.is_single(1)
    assert lookup.at(1, Point(20, 20)) == 1
    assert lookup.at(1, Point(190, 20)) == 10
    # the nearest Basin that replaces it, also from beyond it
    assert lookup.at(1, Point(250, 20)) == 10
    # too far away to say which: the Basin that kept the ID
    assert lookup.at(1, Point(5000, 20)) == 1
    with pytest.raises(AssertionError, match="no Basin replaces Basin 3"):
        lookup.at(3, Point(0, 0))


def test_reversed_to_supply():
    def table(rows: list[tuple[int, str, int, int, str]]) -> pd.DataFrame:
        columns = ["node_id", "node_type", "from_node_id", "to_node_id", "function"]
        return pd.DataFrame(rows, columns=columns).set_index("node_id")

    level = pd.Series({1: -3.0, 2: -2.0, 3: -1.0})
    # a gemaal that lifts the lower Basin 1, beside an inlet from Basin 2, which drains on to Basin 3: the inlet supplies
    nodes = table(
        [
            (10, "Pump", 1, 2, "drain"),
            (11, "Outlet", 2, 1, "flow_control"),
            (12, "Pump", 2, 3, "drain"),
            (13, "Outlet", 3, 2, "supply"),
        ]
    )
    assert reversed_to_supply(nodes, level) == (11,)
    # Basin 2 is pumped up from Basin 1 and only drains back over a stuw, while Basin 1 drains on: the gemaal supplies
    nodes = table([(10, "Pump", 1, 2, "drain"), (11, "Outlet", 2, 1, "drain"), (12, "Pump", 1, 3, "drain")])
    assert reversed_to_supply(nodes, level) == (10,)
    # two stuwen, without another way out: from the higher to the lower target level
    nodes = table([(10, "Outlet", 1, 2, "flow_control"), (11, "Outlet", 2, 1, "flow_control")])
    assert reversed_to_supply(nodes, level) == (10,)
    # the same target level: the node with the lowest ID keeps its function
    assert reversed_to_supply(nodes, pd.Series({1: -2.0, 2: -2.0})) == (11,)
    # nodes that supply already are left
    nodes = table([(10, "Pump", 1, 2, "drain"), (11, "Outlet", 2, 1, "supply")])
    assert reversed_to_supply(nodes, level) == ()


def test_rating_curves_between_levels():
    model = _model({1: -1.0, 2: -1.0, 3: -0.4, 4: 0.5})
    resistance = [
        manning_resistance.Static(length=[100.0], manning_n=[0.04], profile_width=[10.0], profile_slope=[1.0])
    ]
    for node_id, (upstream, downstream) in {11: (1, 2), 12: (2, 3), 13: (3, 4)}.items():
        point = Point(100 * (upstream), 50)
        model.manning_resistance.add(Node(node_id, point), resistance)
        model.link.add(model.basin[upstream], model.manning_resistance[node_id])
        model.link.add(model.manning_resistance[node_id], model.basin[downstream])

    # Basin 4 has no target level: only the node between Basins 2 and 3 changes, and runs from the higher Basin 3
    assert rating_curves_between_levels(model, unknown_level=0.5) == [12]
    assert model.node.df.loc[[11, 12, 13], "node_type"].tolist() == [
        "ManningResistance",
        "TabulatedRatingCurve",
        "ManningResistance",
    ]
    links = model.link.df.set_index(["from_node_id", "to_node_id"])
    assert (3, 12) in links.index and (12, 2) in links.index
    assert [tuple(c) for c in links.at[(3, 12), "geometry"].coords] == [(250.0, 50.0), (200.0, 50.0)]


def _refined(tmp_path: Path) -> tuple[Model, Path, Path, Path]:
    """A model in which Basin 1 of 2024 is replaced by Basins 1 and 10, with the files that refer to 2024.

    In 2024, Basin 1 of 200 m wide drained through Pump 5 into Basin 2. Now Basin 10 is its half next to Basin 2,
    with ManningResistance 11 between Basins 1 and 10.
    """
    model = _model({1: -1.0, 10: -2.0, 2: -3.0})
    resistance = [
        manning_resistance.Static(length=[100.0], manning_n=[0.04], profile_width=[10.0], profile_slope=[1.0])
    ]
    model.pump.add(Node(5, Point(200, 50)), [pump.Static(flow_rate=[1.0])])
    model.manning_resistance.add(Node(11, Point(100, 50)), resistance)
    for upstream, connector, downstream in ((10, model.pump[5], 2), (1, model.manning_resistance[11], 10)):
        model.link.add(model.basin[upstream], connector)
        model.link.add(connector, model.basin[downstream])
    input_toml = tmp_path / "input_model/ribasim.toml"
    model.write(input_toml)

    reference = tmp_path / "boezemmodel_ids.gpkg"
    nodes = gpd.GeoDataFrame(
        {"node_id": [1, 2, 5], "node_type": ["Basin", "Basin", "Pump"]},
        geometry=[Point(100, 50), Point(250, 50), Point(200, 50)],
        crs=CRS,
    )
    nodes.to_file(reference, layer="node")
    areas = gpd.GeoDataFrame({"node_id": [1, 2]}, geometry=[box(0, 0, 200, 100), box(200, 0, 300, 100)], crs=CRS)
    areas.to_file(reference, layer="area")
    form = tmp_path / "form.xlsx"
    pd.DataFrame({"Actie": ["Verwijderen"], "Node Type": ["Pump"], "Node ID": [5]}).to_excel(
        form, sheet_name="Feedback_Formulier", startrow=7, index=False
    )
    return model, reference, input_toml, form


def test_references_2024(tmp_path: Path):
    model, reference, input_toml, form = _refined(tmp_path)
    refs = References2024(model, reference, input_toml, form)
    assert removed_basins(form) == frozenset()
    assert refs.basins(1) == [1, 10]
    assert refs.basin(1, Point(150, 50)).node_id == 10
    assert refs.basin(1, model.manning_resistance[11]).node_id == 1
    # a node of 2024 keeps its ID; the feedback stage numbers the nodes it adds after those of the input model
    assert refs.node(5) == 5
    added = model.pump.add(Node(geometry=Point(0, 50)), [pump.Static(flow_rate=[1.0])])
    assert added.node_id == 12
    assert refs.node(6) == 12
    assert refs.nodes([5, 6]) == (5, 12)
    assert refs.keys({6: "name"}) == {12: "name"}
    with pytest.raises(AssertionError, match="is a Basin of 2024"):
        refs.node(1)

    # Basin 2 merges into Basin 10, the Basin of the two that it connects to, without Pump 5 between them
    refs.merge_basins(node_id=2, to_node_id=1)
    assert sorted(model.node.df.index[model.node.df["node_type"] == "Basin"]) == [1, 10]
    assert 5 not in model.node.df.index
    assert refs.basins(1) == [1, 10]
    assert refs.basins(2) == []
    assert refs.lookup.areas[10].area == 20000.0


def test_remove_dangling_connectors(tmp_path: Path):
    model, _, _, _ = _refined(tmp_path)
    model.remove_node(2, remove_links=True)
    # only among the given nodes
    assert remove_dangling_connectors(model, among={11}) == []
    assert remove_dangling_connectors(model) == [5]
    assert 5 not in model.node.df.index

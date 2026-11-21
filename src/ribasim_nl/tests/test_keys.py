import geopandas as gpd
import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import basin, discrete_control, level_boundary, outlet
from ribasim_nl.crossings.model import crossing_keys
from ribasim_nl.keys import (
    KEY_COLUMN,
    NodeKey,
    fill_keys,
    lumped_keys,
    named_keys,
    number_duplicates,
    resolve_keys,
    validate_keys,
)
from ribasim_nl.parametrization.static_data_xlsx import read_node_sheet
from shapely.geometry import LineString, MultiPolygon, Point, Polygon

from ribasim_nl import Model


def square(x: float) -> MultiPolygon:
    return MultiPolygon([Polygon([(x - 5, -5), (x + 5, -5), (x + 5, 5), (x - 5, 5)])])


@pytest.fixture
def model() -> Model:
    """Two Basins in a row, connected by Outlets, draining to a LevelBoundary, all with a key.

    Basin 1 (up:st1) -> Outlet 3 (st1) -> Basin 2 (up:st2) -> Outlet 4 (st2) -> LevelBoundary 5 (bnd:1)
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id, x, key in ((1, 0, "up:st1"), (2, 20, "up:st2")):
        model.basin.add(
            Node(node_id, Point(x, 0), meta_key=key, meta_categorie="doorgaand"),
            [
                basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
                basin.State(level=[0.5]),
                basin.Area(geometry=[square(x)]),
            ],
        )
    for node_id, x, key in ((3, 10, "st1"), (4, 30, "st2")):
        model.outlet.add(Node(node_id, Point(x, 0), meta_key=key), [outlet.Static(flow_rate=[1.0])])
    model.level_boundary.add(Node(5, Point(40, 0), meta_key="bnd:1"), [level_boundary.Static(level=[0.0])])
    for from_node, to_node in (
        (model.basin[1], model.outlet[3]),
        (model.outlet[3], model.basin[2]),
        (model.basin[2], model.outlet[4]),
        (model.outlet[4], model.level_boundary[5]),
    ):
        model.link.add(from_node, to_node)
    return model


def test_number_duplicates():
    keys = pd.Series(["a", "b", "a", None, "c"], index=[10, 11, 12, 13, 14], dtype=object)
    order = pd.Series([2, 0, 1, 3, 4], index=keys.index)
    numbered = number_duplicates(keys, order)
    # equal keys are numbered in the given order, the others stay as they are
    assert numbered.tolist()[:3] == ["a@2", "b", "a@1"]
    assert pd.isna(numbered[13])
    assert numbered[14] == "c"

    # a key that another node has is numbered after it
    taken = pd.Series(["a@1", "a@2", "c"], dtype=object)
    numbered = number_duplicates(keys, order, taken=taken)
    assert numbered.tolist()[:3] == ["a@4", "b", "a@3"]
    assert numbered[14] == "c@2"


def test_node_id(model):
    assert model.node_id("st1") == 3
    assert model.node_ids(["up:st2", "bnd:1"]) == [2, 5]
    with pytest.raises(KeyError, match="0 nodes with key 'st9'"):
        model.node_id("st9")

    # keys are unique per water authority
    model.node.df["meta_waterbeheerder"] = ["A", "A", "A", "B", "B"]
    model.node.df.loc[4, KEY_COLUMN] = "st1"
    with pytest.raises(KeyError, match="2 nodes with key 'st1'"):
        model.node_id("st1")
    assert model.node_id("st1", authority="A") == 3
    assert model.node_id("st1", authority="B") == 4
    model.validate_keys()


def test_resolve_keys(model):
    """A configuration refers to the nodes of several water authorities by authority and key."""
    model.node.df["meta_waterbeheerder"] = ["A", "A", "A", "B", "B"]
    config = {
        NodeKey("A", "st1"): [NodeKey("B", "st2"), NodeKey("C", "st1")],
        NodeKey("C", "st1"): 1.0,
        "level": 2.5,
        "nodes": {NodeKey("A", "up:st1"), NodeKey("C", "st1")},
        "pair": (NodeKey("B", "bnd:1"), 3),
    }
    # keys of an authority that is not in the model give None, and are left out of sets and of the keys of dicts
    assert resolve_keys(model, config) == {3: [4, None], "level": 2.5, "nodes": {1}, "pair": (5, 3)}
    with pytest.raises(KeyError, match="0 nodes of A with key 'st2'"):
        resolve_keys(model, [NodeKey("A", "st2")])


def test_link_id(model):
    link_df = model.link.df
    link_id = model.link_id("st1", "up:st2")
    assert (link_df.at[link_id, "from_node_id"], link_df.at[link_id, "to_node_id"]) == (3, 2)
    with pytest.raises(KeyError, match="0 links from 'up:st2' to 'st1'"):
        model.link_id("up:st2", "st1")


def test_keys_follow_the_node_not_its_id(model):
    """A node keeps its key when its type changes, and when nodes around it are removed."""
    model.update_node(model.node_id("st1"), "Pump")
    assert model.get_node_type(model.node_id("st1")) == "Pump"
    model.remove_node(model.node_id("st2"), remove_links=True)
    assert model.node_id("st1") == 3
    with pytest.raises(KeyError):
        model.node_id("st2")


def test_fill_keys(model):
    # nodes that a script adds without a key
    added = model.outlet.add(Node(geometry=Point(30.4, 10.6)), [outlet.Static(flow_rate=[1.0])])
    same_place = model.outlet.add(Node(geometry=Point(30.4, 10.6)), [outlet.Static(flow_rate=[1.0])])
    coded = model.outlet.add(
        Node(geometry=Point(50, 10), meta_code_waterbeheerder="KST1"), [outlet.Static(flow_rate=[1.0])]
    )
    controller = model.discrete_control.add(
        Node(geometry=Point(10, 10)),
        [
            discrete_control.Variable(
                compound_variable_id=[1], listen_node_id=[1], variable=["level"], weight=[1.0], look_ahead=[0.0]
            ),
            discrete_control.Condition(compound_variable_id=[1], condition_id=[1], threshold_high=[0.5]),
            discrete_control.Logic(truth_state=["T"], control_state=["open"]),
        ],
    )
    model.link.add(controller, model.outlet[3])

    # a key that is looked up fills the keys of the nodes that have none
    assert model.node_id("discrete_control:st1") == controller.node_id
    assert model.node_id("KST1") == coded.node_id
    # nodes at the same location are numbered
    assert {model.node_id("loc:30,11@1"), model.node_id("loc:30,11@2")} == {added.node_id, same_place.node_id}
    # the keys that the nodes had are kept
    assert model.node_id("st1") == 3
    model.validate_keys()


def test_fill_keys_per_authority(model):
    """Equal keys are numbered within the nodes of a water authority, since a key is unique per authority."""
    model.node.df["meta_waterbeheerder"] = ["A", "A", "A", "B", "B"]
    in_a = model.outlet.add(
        Node(geometry=Point(50, 10), meta_waterbeheerder="A", meta_code_waterbeheerder="st2"),
        [outlet.Static(flow_rate=[1.0])],
    )
    in_b = model.outlet.add(
        Node(geometry=Point(60, 10), meta_waterbeheerder="B", meta_code_waterbeheerder="st2"),
        [outlet.Static(flow_rate=[1.0])],
    )
    fill_keys(model)
    keys = model.node.df[KEY_COLUMN]
    # authority A has no node with key st2 yet, authority B has
    assert keys[in_a.node_id] == "st2"
    assert keys[in_b.node_id] == "st2@2"
    model.validate_keys()


def test_keys_in_model_of_one_authority(model):
    """In the model of one water authority only some nodes have their authority set: keys are unique in the model."""
    model.node.df["meta_waterbeheerder"] = [None, None, "A", None, "A"]
    added = model.outlet.add(
        Node(geometry=Point(50, 10), meta_code_waterbeheerder="st1"), [outlet.Static(flow_rate=[1.0])]
    )
    fill_keys(model)
    assert model.node.df.at[added.node_id, KEY_COLUMN] == "st1@2"
    model.node.df.loc[added.node_id, KEY_COLUMN] = "st1"
    with pytest.raises(ValueError, match="2 nodes share their key"):
        validate_keys(model)


def test_fill_keys_storing_basin_and_junction(model):
    storing = model.basin.add(
        Node(geometry=Point(0, 10), meta_categorie="bergend"),
        [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])],
    )
    connector = model.outlet.add(Node(geometry=Point(0, 5)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(storing, connector)
    model.link.add(connector, model.basin[1])
    # a Junction between Outlet st2 and the boundary, which a second Outlet flows to as well
    second = model.outlet.add(Node(geometry=Point(30, 5), meta_key="st3"), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[2], second)
    model.remove_link(4, 5, remove_disconnected_nodes=False)
    junction = model.junction.add(Node(geometry=Point(35, 0)))
    model.link.add(model.outlet[4], junction)
    model.link.add(second, junction)
    model.link.add(junction, model.level_boundary[5])

    fill_keys(model)
    keys = model.node.df[KEY_COLUMN]
    assert keys[storing.node_id] == "bergend:up:st1"
    assert keys[connector.node_id] == "bergend_link:up:st1"
    assert keys[junction.node_id] == "junction:st2>bnd:1#2x1"
    model.validate_keys()


def test_validate_keys(model):
    model.node.df.loc[4, KEY_COLUMN] = "st1"
    with pytest.raises(ValueError, match="2 nodes share their key"):
        validate_keys(model)
    model.node.df.loc[4, KEY_COLUMN] = None
    with pytest.raises(ValueError, match="1 nodes without a key"):
        validate_keys(model)


def test_write_fills_keys(model, tmp_path):
    added = model.outlet.add(Node(geometry=Point(50, 10)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[2], added)
    model.link.add(added, model.level_boundary[5])
    toml = tmp_path / "model" / "ribasim.toml"
    model.write(toml)
    assert Model.read(toml).node_id("loc:50,10") == added.node_id


def test_apply_edits_by_key(model, tmp_path):
    edits_gpkg = tmp_path / "model_edits.gpkg"
    gpd.GeoDataFrame({"node_key": ["st1"], "node_type": ["Pump"]}, geometry=[Point(10, 0)], crs=28992).to_file(
        edits_gpkg, layer="update_node"
    )
    gpd.GeoDataFrame({"link_from_key": ["up:st2"], "link_to_key": ["st2"]}, geometry=[Point(25, 0)], crs=28992).to_file(
        edits_gpkg, layer="reverse_link"
    )
    gpd.GeoDataFrame({"key": ["new basin"]}, geometry=[Point(60, 0)], crs=28992).to_file(edits_gpkg, layer="add_basin")

    model.apply_edits(edits_gpkg, ["update_node", "reverse_link", "add_basin"])
    assert model.get_node_type(3) == "Pump"
    assert model.link_id("st2", "up:st2") in model.link.df.index
    assert model.get_node_type(model.node_id("new basin")) == "Basin"

    # a key that no node has fails, and the error tells which edit
    gpd.GeoDataFrame({"node_key": ["st9"], "node_type": ["Pump"]}, geometry=[Point(0, 0)], crs=28992).to_file(
        tmp_path / "invalid_edits.gpkg", layer="update_node"
    )
    with pytest.raises(KeyError) as excinfo:
        model.apply_edits(tmp_path / "invalid_edits.gpkg", ["update_node"])
    assert "in model edit update_node" in str(excinfo.value.__notes__)


def test_lumped_keys():
    """Keys of an input model that is lumped from HyDAMO data.

    Basin 1 -> structure 3 -> Basin 2 -> open water 4 and 5 on the same hydroobject -> boundary 6
    """
    node = gpd.GeoDataFrame(
        {
            "node_type": ["Basin", "Basin", "Outlet", "ManningResistance", "ManningResistance", "LevelBoundary"],
            "name": ["", "", "KST1", "", None, ""],
            "meta_id": [None, None, None, None, None, 28],
        },
        index=pd.Index([1, 2, 3, 4, 5, 6], name="node_id"),
        geometry=[Point(0, 0), Point(20, 0), Point(10, 0), Point(40, 0), Point(30, 0), Point(50, 0)],
        crs=28992,
    )
    link = pd.DataFrame(
        {
            "from_node_id": [1, 3, 2, 2, 4, 5],
            "to_node_id": [3, 2, 4, 5, 6, 6],
            "link_type": "flow",
        }
    )
    hydroobject = gpd.GeoDataFrame({"code": ["OW1"]}, geometry=[LineString([(25, 0), (45, 0)])], crs=28992)

    key, kind = lumped_keys(node, link, hydroobject)
    assert key.to_dict() == {
        1: "up:KST1",
        # the smallest key of the connector nodes it drains through
        2: "up:ow:OW1@1",
        3: "KST1",
        # numbered along the hydroobject, not by node ID
        4: "ow:OW1@2",
        5: "ow:OW1@1",
        6: "bnd:28",
    }
    assert kind[3] == "structure"
    assert kind[4] == "hydroobject"

    # without hydroobjects the nodes on open water are named after their location
    key, _ = lumped_keys(node, link, None)
    assert key[4] == "loc:40,0"
    assert key[2] == "up:loc:30,0"


def test_named_keys():
    """Keys of a model that is built from named objects, such as that of Rijkswaterstaat."""
    node = gpd.GeoDataFrame(
        {
            "node_type": ["Basin", "Basin", "Outlet", "Outlet", "LevelBoundary", "LevelBoundary"],
            "name": ["Waal", "Rijn", "Stuw Driel", "", "Noordzee", "Lobith"],
            "meta_code_waterbeheerder": [None, None, "DRIEL", None, None, None],
            "meta_meetlocatie_code": [None, None, None, None, None, "LOBH"],
        },
        index=pd.Index([1, 2, 3, 4, 5, 6], name="node_id"),
        geometry=[Point(0, 0), Point(20, 0), Point(10, 0), Point(10, 5), Point(30, 0), Point(-10, 0)],
        crs=28992,
    )
    link = pd.DataFrame({"from_node_id": [1, 3, 1, 4, 6, 2], "to_node_id": [3, 2, 4, 2, 1, 5], "link_type": "flow"})
    assert named_keys(node, link).to_dict() == {
        1: "Waal",
        2: "Rijn",
        3: "DRIEL",
        # a connector node without code or name, after the nodes it connects
        4: "Waal>Rijn",
        5: "bnd:Noordzee",
        6: "bnd:LOBH",
    }


def test_crossing_keys():
    """Keys of an input model that is built from the crossings of a level controlled water authority.

    Basin 1 (area A) -> crossing 3 -> Basin 2 (area B) -> crossings 4 and 5 -> boundaries 6 and 7
    """

    def layer(codes: dict[str, str | None]) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(
            {"globalid": list(codes), "code": list(codes.values())}, geometry=[Point(0, 0)] * len(codes)
        )

    post_processed_data = {
        "peilgebied": layer({"{a}": "A", "{b}": "B"}),
        "gemaal": layer({"{g1}": "KGM2", "{g2}": "KGM1"}),
        "stuw": layer({"{s1}": "KST1"}),
        # an object without a code is identified by its globalid
        "hydroobject": layer({"{h1}": "OW1", "{h2}": "dummy_code_7"}),
    }
    crossings = gpd.GeoDataFrame(
        {
            "node_id": [3, 4, 5],
            "peilgebied_from": [1, 2, 2],
            "peilgebied_to": [2, -999, -999],
            "agg_area_from": ["{a}", "{b}", "{b}"],
            "agg_area_to": ["{b}", None, None],
            # the smallest code of the two gemalen, a stuw, and open water
            "gemaal": ["{g1},{g2}", None, None],
            "stuw": [None, "{s1}", None],
            "hydroobject": ["{h1}", "{h1}", "{h2}"],
        },
        geometry=[Point(10, 0), Point(30, 0), Point(30, 5)],
    )
    nodes = gpd.GeoDataFrame(
        {
            "node_id": [1, 2, 3, 4, 5, 6, 7],
            "type": ["Basin", "Basin", "Pump", "Outlet", "Outlet", "LevelBoundary", "Terminal"],
        },
        geometry=[Point(0, 0), Point(20, 0), Point(10, 0), Point(30, 0), Point(30, 5), Point(40, 0), Point(40, 5)],
    )
    links = pd.DataFrame({"from": [1, 3, 2, 2, 4, 5], "to": [3, 2, 4, 5, 6, 7]})

    assert crossing_keys(nodes, links, crossings, post_processed_data).to_dict() == {
        1: "A",
        2: "B",
        3: "KGM1",
        4: "KST1",
        5: "ow:h2",
        6: "bnd:KST1",
        7: "bnd:ow:h2",
    }

    # crossings with the same structure also get the areas they connect
    crossings["stuw"] = [None, "{s1}", "{s1}"]
    crossings["agg_area_to"] = ["{b}", None, "{a}"]
    keys = crossing_keys(nodes, links, crossings, post_processed_data)
    assert keys[4] == "KST1|B>"
    assert keys[5] == "KST1|B>A"
    assert keys[7] == "bnd:KST1|B>A"


def test_read_node_sheet(model, tmp_path):
    """A sheet of `static_data.xlsx` refers to nodes by their key, and can hold nodes that are not in the model."""
    xlsx = tmp_path / "static_data.xlsx"
    pd.DataFrame({"key": ["st2", "st9", "st1"], "flow_rate": [2.0, 9.0, 1.0]}).to_excel(
        xlsx, sheet_name="Outlet", index=False
    )
    outlets = read_node_sheet(model, xlsx, "Outlet")
    assert outlets.columns.tolist() == ["node_id", "flow_rate"]
    assert outlets.set_index("node_id")["flow_rate"].to_dict() == {4: 2.0, 3: 1.0}

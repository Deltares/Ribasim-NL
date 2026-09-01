import sqlite3
from types import SimpleNamespace

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
import xugrid as xu
from ribasim import Model
from ribasim_nl.zonebudget import NodeGroups, aggregate_results, aggregate_seasonal_results, aggregate_time_results
from shapely.geometry import LineString, Point, box


def _model_and_results(*, with_boundaries=False, with_boundary_junction=False, with_reverse_group_link=False):
    node_records = [
        (1, "Basin", Point(0, 0)),
        (2, "Pump", Point(1, 0)),
        (3, "Basin", Point(2, 0)),
        (4, "Pump", Point(0, -1)),
        (5, "LevelBoundary", Point(2, -1)),
        (6, "Basin", Point(4, 0)),
        (7, "Outlet", Point(3, 0)),
        (8, "Junction", Point(3.5, 0)),
    ]
    if with_boundaries:
        node_records.extend(
            [
                (9, "FlowBoundary", Point(-2, 1)),
                (10, "FlowBoundary", Point(-2, -1)),
                (11, "FlowBoundary", Point(-1, 2)),
                (12, "LevelBoundary", Point(-1, -2)),
            ]
        )
    if with_boundary_junction:
        node_records.extend([(13, "Junction", Point(-1, 0.5)), (14, "Junction", Point(-1, -0.5))])
    nodes = gpd.GeoDataFrame(
        node_records,
        columns=["node_id", "node_type", "geometry"],
        crs=28992,
    ).set_index("node_id")
    link_records = [
        (10, 1, 2),
        (11, 2, 3),
        (12, 1, 4),
        (13, 4, 5),
        (14, 3, 7),
        (15, 7, 8),
        (16, 8, 6),
    ]
    if with_boundaries:
        if with_boundary_junction:
            link_records.extend([(17, 9, 13), (22, 13, 1), (18, 10, 14), (23, 14, 1), (19, 11, 1), (20, 12, 1)])
        else:
            link_records.extend([(17, 9, 1), (18, 10, 1), (19, 11, 1), (20, 12, 1)])
    if with_reverse_group_link:
        link_records.append((21, 3, 1))
    links = gpd.GeoDataFrame(
        [
            {
                "link_id": link_id,
                "from_node_id": from_id,
                "to_node_id": to_id,
                "link_type": "flow",
                "geometry": LineString((nodes.at[from_id, "geometry"], nodes.at[to_id, "geometry"])),
            }
            for link_id, from_id, to_id in link_records
        ],
        crs=nodes.crs,
    ).set_index("link_id")

    node_positions = pd.Series(np.arange(len(nodes)), index=nodes.index)
    connectivity = np.array(
        [(node_positions[from_id], node_positions[to_id]) for _, from_id, to_id in link_records], dtype=int
    )
    grid = xu.Ugrid1d(
        nodes.geometry.x.to_numpy(),
        nodes.geometry.y.to_numpy(),
        -1,
        connectivity,
        name="ribasim",
        crs=nodes.crs,
    )
    time = pd.date_range("2020-01-01", periods=4, freq="h")
    flow = np.tile(np.arange(1.0, len(links) + 1.0), (len(time), 1))
    drainage = np.full((len(time), len(nodes)), np.nan)
    drainage[:, [0, 2, 5]] = [1.0, 2.0, 3.0]
    dataset = xr.Dataset(
        data_vars={
            "flow_rate": (("time", grid.edge_dimension), flow),
            "drainage": (("time", grid.node_dimension), drainage),
            "level": (("time", grid.node_dimension), drainage + 10.0),
            "storage": (("time", grid.node_dimension), drainage * 100.0),
            "storage_rate": (("time", grid.node_dimension), drainage - 1.5),
        },
        coords={
            "time": time,
            "link_id": (grid.edge_dimension, links.index.to_numpy()),
            "node_id": (grid.node_dimension, nodes.index.to_numpy()),
        },
    )
    results = xu.UgridDataset(dataset, grid)
    model = SimpleNamespace(
        node=SimpleNamespace(df=nodes),
        link=SimpleNamespace(df=links),
        ribasim_version="2026.1.2",
        starttime=pd.Timestamp("2020-01-01"),
        endtime=pd.Timestamp("2020-01-01 04:00"),
        to_xugrid=lambda add_flow: results,
    )
    _add_basin_tables(model)
    return model, results


def _add_basin_tables(model):
    area = gpd.GeoDataFrame(
        {
            "node_id": [1, 3, 6],
            "geometry": [box(-0.4, -0.4, 0.4, 0.4), box(1.6, -0.4, 2.4, 0.4), box(3.6, -0.4, 4.4, 0.4)],
        },
        crs=model.node.df.crs,
    )
    profile = pd.DataFrame(
        {
            "node_id": [1, 1, 3, 3, 6, 6],
            "level": [0.0, 1.0] * 3,
            "area": [500.0, 1_000.0, 1_000.0, 2_000.0, 2_000.0, 4_000.0],
        }
    )
    model.basin = SimpleNamespace(area=SimpleNamespace(df=area), profile=SimpleNamespace(df=profile))


def test_mixed_resolution_aggregation():
    model, _ = _model_and_results()
    groups = NodeGroups.from_ids(model, {"A": [1], "B": [3]})

    result = aggregate_results(model, groups)

    assert result.diagnostics.contracted_connector_ids == (2,)
    assert result.diagnostics.ungrouped_basin_ids == (6,)
    assert set(result.links.index[:-1]) == {12, 13, 14, 15, 16}
    assert result.links.index[-1] == 17
    assert result.dataset["flow_rate"].sel(aggregated_nEdges=5).to_numpy().tolist() == [2.0] * 4

    group_nodes = result.nodes[result.nodes["group_id"].notna()]
    group_a_id = group_nodes.index[group_nodes["group_id"] == "A"].item()
    assert result.links.at[12, "from_node_id"] == group_a_id
    np.testing.assert_allclose(result.dataset["drainage"].to_numpy()[:, -2:], [[1.0, 2.0]] * 4)
    node_position = result.nodes.index.get_loc(group_a_id)
    assert result.dataset["inflow_rate"][:, node_position].to_numpy().tolist() == [0.0] * 4
    assert result.dataset["outflow_rate"][:, node_position].to_numpy().tolist() == [5.0] * 4


def test_internal_connector_and_junction_chain_is_absorbed():
    model, _ = _model_and_results()

    result = aggregate_results(model, NodeGroups.from_ids(model, {"B": [3, 6]}))

    assert not {7, 8}.intersection(result.nodes.index)
    assert not {14, 15, 16}.intersection(result.links.index)
    assert not (result.links["from_node_id"] == result.links["to_node_id"]).any()


def test_grouped_controls_are_removed_but_ungrouped_controls_remain():
    model, _ = _model_and_results()
    model.node.df["meta_waterbeheerder"] = None
    model.node.df.loc[[1, 3], "meta_waterbeheerder"] = ["A", "B"]
    model.node.df.loc[9] = {"node_type": "DiscreteControl", "geometry": Point(0.5, 0), "meta_waterbeheerder": "A"}
    model.node.df.loc[10] = {
        "node_type": "ContinuousControl",
        "geometry": Point(10, 0),
        "meta_waterbeheerder": None,
    }
    model.node.df.loc[11] = {"node_type": "FlowDemand", "geometry": Point(0.25, 0), "meta_waterbeheerder": "A"}

    result = aggregate_results(model, NodeGroups.from_node_column(model, "meta_waterbeheerder"))

    assert 9 not in result.nodes.index
    assert 10 in result.nodes.index
    assert 11 not in result.nodes.index


def test_node_column_values_can_remain_ungrouped():
    model, _ = _model_and_results()
    model.node.df["meta_waterbeheerder"] = None
    model.node.df.loc[[1, 3], "meta_waterbeheerder"] = ["A", "Rijkswaterstaat"]

    groups = NodeGroups.from_node_column(
        model,
        "meta_waterbeheerder",
        exclude=["Rijkswaterstaat"],
    )
    result = aggregate_results(model, groups)

    assert groups.basin_to_group.to_dict() == {1: "A"}
    assert 3 in result.nodes.index
    assert result.nodes.at[3, "node_type"] == "Basin"


def test_basin_areas_are_dissolved_and_profile_areas_are_summed():
    model, _ = _model_and_results()
    _add_basin_tables(model)
    model.basin.profile.df.loc[
        (model.basin.profile.df["node_id"] == 3) & (model.basin.profile.df["level"] == 0.0), "area"
    ] = 10_000.0

    result = aggregate_results(model, NodeGroups.from_ids(model, {"A": [1, 3]}))

    assert result.basin_areas is not None
    group_id = result.nodes.index[result.nodes["group_id"] == "A"].item()
    assert set(result.basin_areas.index) == {6, group_id}
    assert result.basin_areas.at[group_id, "meta_profile_area_m2"] == 3_000.0
    assert result.basin_areas.at[group_id, "geometry"].area == pytest.approx(1.28)


def test_missing_top_profile_area_is_rejected():
    model, _ = _model_and_results()
    _add_basin_tables(model)
    model.basin.profile.df = model.basin.profile.df[model.basin.profile.df["node_id"] != 3]

    with pytest.raises(ValueError, match=r"top profile area.*3"):
        aggregate_results(model, NodeGroups.from_ids(model, {"A": [1, 3]}))


def test_budget_reverses_negative_flow_and_normalizes_each_basin(tmp_path):
    model, dataset = _model_and_results()
    _add_basin_tables(model)
    dataset["flow_rate"][:] = 0.0
    dataset["flow_rate"][:, 1] = -2.0

    result = aggregate_results(model, NodeGroups.from_ids(model, {"A": [1], "B": [3]}))

    assert result.budget is not None
    assert result.basin_areas is not None
    group_a_id = result.nodes.index[result.nodes["group_id"] == "A"].item()
    group_b_id = result.nodes.index[result.nodes["group_id"] == "B"].item()
    first_time = pd.Timestamp("2020-01-01")
    budget = result.budget.set_index(["time", "node_id", "term"])
    assert budget.at[(first_time, group_a_id, "inflow"), "value_mm_day"] == pytest.approx(172_800.0)
    assert budget.at[(first_time, group_b_id, "outflow"), "value_mm_day"] == pytest.approx(86_400.0)
    assert budget.at[(first_time, group_a_id, "drainage"), "value_mm_day"] == pytest.approx(86_400.0)
    assert result.basin_areas.at[group_a_id, "meta_2020_inflow_mm_day"] == pytest.approx(172_800.0)

    toml = result.write_visualization_model(tmp_path / "aggregated")
    database = toml.parent / "input" / "database.gpkg"
    assert "Basin / area" in set(gpd.list_layers(database)["name"])
    exported_area = gpd.read_file(database, layer="Basin / area")
    assert set(exported_area["node_id"]) == {group_a_id, group_b_id, 6}
    assert "meta_2020_inflow_mm_day" in exported_area


def test_parallel_group_links_have_distinct_interiors():
    model, _ = _model_and_results(with_reverse_group_link=True)

    result = aggregate_results(model, NodeGroups.from_ids(model, {"A": [1], "B": [3]}))

    group_ids = set(result.nodes.index[result.nodes["group_id"].notna()])
    group_links = result.links[
        result.links["from_node_id"].isin(group_ids) & result.links["to_node_id"].isin(group_ids)
    ]
    assert len(group_links) == 2
    assert all(len(geometry.coords) == 3 for geometry in group_links.geometry)
    assert group_links.geometry.iloc[0].coords[1] != group_links.geometry.iloc[1].coords[1]


def test_filtered_flow_boundaries_are_aggregated_per_group():
    model, _ = _model_and_results(with_boundaries=True)
    model.node.df["meta_waterbeheerder"] = None
    model.node.df.loc[[1, 9, 10, 11, 12], "meta_waterbeheerder"] = "A"
    model.node.df["meta_category"] = None
    model.node.df.loc[[9, 10], "meta_category"] = "RWZI"
    model.node.df.loc[11, "meta_category"] = "OTHER"

    result = aggregate_results(
        model,
        NodeGroups.from_node_column(model, "meta_waterbeheerder"),
        aggregate_flow_boundaries=True,
        flow_boundary_filter={"meta_category": "RWZI"},
    )

    assert not {9, 10}.intersection(result.nodes.index)
    assert {11, 12}.issubset(result.nodes.index)
    flow_boundaries = result.nodes[result.nodes["node_type"] == "FlowBoundary"]
    assert len(flow_boundaries) == 2
    aggregated_id = flow_boundaries.index.difference([11]).item()
    assert flow_boundaries.at[aggregated_id, "geometry"].equals(Point(-2, 0))
    aggregated_link_id = result.links.index[result.links["from_node_id"] == aggregated_id].item()
    values = result.dataset["flow_rate"].where(result.dataset["link_id"] == aggregated_link_id, drop=True)
    assert values.to_numpy().ravel().tolist() == [17.0] * 4


def test_junction_to_aggregated_boundary_is_contracted():
    model, _ = _model_and_results(with_boundaries=True, with_boundary_junction=True)
    model.node.df["meta_waterbeheerder"] = None
    model.node.df.loc[[1, 9, 10], "meta_waterbeheerder"] = "A"
    model.node.df["meta_category"] = None
    model.node.df.loc[[9, 10], "meta_category"] = "RWZI"

    result = aggregate_results(
        model,
        NodeGroups.from_node_column(model, "meta_waterbeheerder"),
        aggregate_flow_boundaries=True,
        flow_boundary_filter={"meta_category": "RWZI"},
    )

    assert not {13, 14}.intersection(result.nodes.index)
    group_id = result.nodes.index[result.nodes["group_id"] == "A"].item()
    boundary_id = result.nodes.index[
        (result.nodes["node_type"] == "FlowBoundary") & (result.nodes["name"] == "A FlowBoundary")
    ].item()
    link_id = result.links.index[
        (result.links["from_node_id"] == boundary_id) & (result.links["to_node_id"] == group_id)
    ].item()
    values = result.dataset["flow_rate"].where(result.dataset["link_id"] == link_id, drop=True)
    assert values.to_numpy().ravel().tolist() == [20.0] * 4


def test_level_boundary_aggregation_has_separate_flag():
    model, _ = _model_and_results(with_boundaries=True)
    model.node.df["meta_waterbeheerder"] = None
    model.node.df.loc[[1, 9, 10, 11, 12], "meta_waterbeheerder"] = "A"

    result = aggregate_results(
        model,
        NodeGroups.from_node_column(model, "meta_waterbeheerder"),
        aggregate_level_boundaries=True,
    )

    assert {9, 10, 11}.issubset(result.nodes.index)
    assert 12 not in result.nodes.index


def test_polygon_edge_is_ambiguous():
    model, _ = _model_and_results()
    polygons = gpd.GeoDataFrame(
        {"zone": ["left", "right"], "geometry": [box(-1, -1, 0, 1), box(0, -1, 1, 1)]},
        crs=model.node.df.crs,
    )

    with pytest.raises(ValueError, match="multiple groups"):
        NodeGroups.from_polygons(model, polygons, "zone")


def test_time_aggregation_rejects_irregular_steps():
    _, dataset = _model_and_results()
    dataset = dataset.assign_coords(
        time=pd.to_datetime(["2020-01-01", "2020-01-01 01:00", "2020-01-01 03:00", "2020-01-01 04:00"], format="mixed")
    )

    with pytest.raises(ValueError, match="regular timesteps"):
        aggregate_time_results(dataset, "D")


def test_time_aggregation_uses_mean_rates():
    _, dataset = _model_and_results()
    dataset["flow_rate"][:, 0] = [1.0, 3.0, 5.0, 7.0]

    aggregated = aggregate_time_results(dataset, "2h")

    assert isinstance(aggregated, xu.UgridDataset)
    assert aggregated["flow_rate"][:, 0].to_numpy().tolist() == [2.0, 6.0]


def test_time_aggregation_keeps_signed_storage_rate():
    _, dataset = _model_and_results()
    dataset["storage_rate"][:, 0] = [-2.0, 4.0, -6.0, 8.0]

    aggregated = aggregate_time_results(dataset, "2h")

    assert aggregated["storage_rate"][:, 0].to_numpy().tolist() == [1.0, 1.0]
    assert "storage_increase" not in aggregated
    assert "storage_decrease" not in aggregated


def test_storage_is_summed_and_level_is_area_weighted():
    model, _ = _model_and_results()

    result = aggregate_results(model, NodeGroups.from_ids(model, {"A": [1, 3]}))

    group_id = result.nodes.index[result.nodes["group_id"] == "A"].item()
    position = result.nodes.index.get_loc(group_id)
    assert result.dataset["storage"][:, position].to_numpy().tolist() == [300.0] * 4
    assert result.dataset["storage_rate"][:, position].to_numpy().tolist() == [0.0] * 4
    assert result.dataset["level"][:, position].to_numpy().tolist() == pytest.approx([35.0 / 3.0] * 4)


def test_seasonal_aggregation_uses_april_and_october_boundaries():
    _, dataset = _model_and_results()
    daily = dataset.isel(time=np.zeros(366, dtype=int)).assign_coords(
        time=pd.date_range("2019-10-01", periods=366, freq="D")
    )
    daily["flow_rate"][:, 0] = np.select(
        [(daily["time"].dt.month >= 4) & (daily["time"].dt.month <= 9)],
        [6.5],
        default=2.0,
    )

    aggregated = aggregate_seasonal_results(daily)

    assert aggregated["time"].to_numpy().tolist() == [
        pd.Timestamp("2019-10-01").to_datetime64(),
        pd.Timestamp("2020-04-01").to_datetime64(),
    ]
    assert aggregated["flow_rate"][:, 0].to_numpy().tolist() == [2.0, 6.5]


def test_frequency_and_seasonal_aggregation_are_mutually_exclusive():
    model, _ = _model_and_results()

    with pytest.raises(ValueError, match="either frequency or seasonal"):
        aggregate_results(model, NodeGroups.from_ids(model, {"A": [1]}), frequency="YS", seasonal=True)


def test_user_demand_between_groups_is_rejected():
    model, _ = _model_and_results()
    model.node.df.at[2, "node_type"] = "UserDemand"

    with pytest.raises(ValueError, match="non-conservative"):
        aggregate_results(model, NodeGroups.from_ids(model, {"A": [1], "B": [3]}))


def test_user_demand_within_group_is_internal():
    model, _ = _model_and_results()
    model.node.df.at[2, "node_type"] = "UserDemand"

    result = aggregate_results(model, NodeGroups.from_ids(model, {"A": [1, 3]}))

    assert 2 not in result.nodes.index
    assert not {10, 11}.intersection(result.links.index)


def test_write_visualization_model(tmp_path):
    model, _ = _model_and_results()
    result = aggregate_results(model, NodeGroups.from_ids(model, {"A": [1], "B": [3]}))

    toml = result.write_visualization_model(tmp_path / "aggregated")

    assert toml.read_text(encoding="utf-8").splitlines() == [
        "starttime = 2020-01-01 00:00:00",
        "endtime = 2020-01-01 04:00:00",
        'crs = "EPSG:28992"',
        'input_dir = "input"',
        'results_dir = "results"',
        'ribasim_version = "2026.1.2"',
    ]
    database = toml.parent / "input" / "database.gpkg"
    assert set(gpd.list_layers(database)["name"]) == {"Node", "Link", "Basin / area", "ribasim_metadata"}
    assert "meta_group_id" in gpd.read_file(database, layer="Node")
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA table_info('Node')").fetchone()[1] == "node_id"
        assert connection.execute("PRAGMA table_info('Link')").fetchone()[1] == "link_id"
        assert connection.execute("SELECT value FROM ribasim_metadata WHERE key = 'schema_version'").fetchone() == (
            "11",
        )
    with xr.open_dataset(toml.parent / "results" / "flow.nc") as flow:
        assert flow.sizes == {"time": 4, "link_id": 6}
    with xr.open_dataset(toml.parent / "results" / "basin.nc") as basin:
        assert basin.sizes == {"time": 4, "node_id": 3}
        assert {
            "level",
            "storage",
            "inflow_rate",
            "outflow_rate",
            "storage_rate",
            "drainage",
        }.issubset(basin.data_vars)
        assert "storage_increase" not in basin
        assert "storage_decrease" not in basin
        assert basin["inflow_rate"].attrs["units"] == "m3 s-1"
        assert basin["level"].attrs["standard_name"] == "water_surface_height_above_reference_datum"
        assert basin.attrs == {
            "Conventions": "CF-1.12",
            "references": "https://ribasim.org",
            "ribasim_version": "2026.1.2",
            "title": "Ribasim results: basin",
        }
    with xr.open_dataset(toml.parent / "results" / "basin_state.nc") as basin_state:
        assert set(basin_state.data_vars) == {"level"}
        assert basin_state.sizes == {"node_id": 3}
    read_model = Model.read(toml)
    assert len(read_model.node.df) == len(result.nodes)
    assert set(read_model.to_xugrid(add_flow=True).data_vars).issuperset(
        {"level", "storage", "inflow_rate", "outflow_rate", "storage_rate"}
    )

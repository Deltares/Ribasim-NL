import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, outlet
from ribasim_nl.observations import _points, _strings, add_observation_nodes
from shapely.geometry import Point

from ribasim_nl import Model


def test_parse_cells():
    assert _points("[<POINT (1.5 2)>, <POINT (-3 4e+05)>]") == [Point(1.5, 2), Point(-3, 4e5)]
    assert _strings("['Basin', 'Pump']") == ["Basin", "Pump"]


def test_add_observation_nodes():
    model = Model(starttime="2020-01-01", endtime="2020-01-03", crs="EPSG:28992")
    model.basin.add(
        Node(1, Point(0, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
    )
    model.outlet.add(Node(2, Point(10, 0)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.outlet[2])
    coupling = gpd.GeoDataFrame(
        {
            "MeetreeksC": ["gemaal", "elders"],
            "Aan/Af": ["Aanvoer", "Afvoer"],
            "negate": [True, False],
            "node_type": ["Outlet", "Outlet"],
            "node_geometry": [Point(10.5, 0), Point(100, 0)],
        },
        geometry=[Point(12, 3), Point(100, 0)],
        crs="EPSG:28992",
    )
    time = pd.date_range("2019-12-31", "2020-01-04", name="time")
    measurements = {
        "Aanvoer": pd.DataFrame({"gemaal": [1.0, 2.0, None, 4.0, 5.0]}, index=time),
        "Afvoer": pd.DataFrame({"elders": 1.0}, index=time),
    }

    model = add_observation_nodes(model, coupling, measurements)
    # only the series at a connector node in the model is added, at the measurement location
    observations = model.observation.node.df
    assert observations["name"].tolist() == ["gemaal"]
    observation_id = observations.index[0]
    assert observations.at[observation_id, "geometry"].equals(Point(12, 3))
    links = model.link.df[model.link.df["link_type"] == "observation"]
    assert links[["from_node_id", "to_node_id"]].values.tolist() == [[observation_id, 2]]
    # measurements in the model period, with the sign of the flow_rate
    time_df = model.observation.time.df
    assert time_df["variable"].unique().tolist() == ["flow_rate"]
    assert time_df["time"].tolist() == [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-03")]
    assert time_df["value"].tolist() == [-2.0, -4.0]

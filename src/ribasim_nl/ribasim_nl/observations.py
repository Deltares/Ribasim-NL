"""Add Observation nodes with measured discharges to a model.

The measurement series and the model links they are compared with are taken from the koppeltabel that is also used
by `ribasim_nl.analyse_results`. Each series measured at a single connector node gets an Observation node at the
measurement location, observing the `flow_rate` of that connector node. Connector nodes are found by their type and
location, which do not change when links are renumbered. Series that combine multiple links are not added.
"""

import logging
import re
import typing

import geopandas as gpd
import pandas as pd
import shapely
from pandera.typing import DataFrame
from ribasim import Node
from ribasim.input_base import NodeData
from ribasim.schemas import ObservationTimeSchema

from ribasim_nl.cloud import CloudStorage
from ribasim_nl.model import Model

LOG = logging.getLogger(__name__)

RESULTAATVERGELIJKING = "Basisgegevens/resultaatvergelijking"
COUPLING_XLSX = (
    f"{RESULTAATVERGELIJKING}/koppeltabel_2026/"
    "Transformed_koppeltabel_versie_rapportage_26062026_Feedback_Verwerkt_HydroLogic.xlsx"
)
SPECIFICS_XLSX = f"{RESULTAATVERGELIJKING}/koppeltabel_2026/Specifiek_bewerking_versierapportage_26062026.xlsx"
MEASUREMENT_CSVS = {
    "Aanvoer": f"{RESULTAATVERGELIJKING}/meetreeksen_2026/Metingen_aanvoer_dag_totaal.csv",
    "Afvoer": f"{RESULTAATVERGELIJKING}/meetreeksen_2026/Metingen_afvoer_dag_totaal.csv",
}

# Node types of which the flow_rate can be observed
OBSERVED_NODE_TYPES = {
    "Pump",
    "Outlet",
    "ManningResistance",
    "TabulatedRatingCurve",
    "LinearResistance",
    "FlowBoundary",
}


def _points(value: object) -> list[shapely.Point]:
    """Parse the points in a koppeltabel cell like "[<POINT (1 2)>, <POINT (3 4)>]"."""
    return [shapely.Point(float(x), float(y)) for x, y in re.findall(r"POINT \(([-\d.e+]+) ([-\d.e+]+)\)", str(value))]


def _strings(value: object) -> list[str]:
    """Parse the strings in a koppeltabel cell like "['Basin', 'Pump']"."""
    return re.findall(r"'([^']*)'", str(value))


def read_coupling(cloud: CloudStorage, authority: str) -> gpd.GeoDataFrame:
    """The single-connector series of an authority, with the type and location of the observed connector node.

    :return: a table with columns MeetreeksC, Aan/Af, negate (whether the measurement has the opposite sign of the
        flow_rate), node_type and node_geometry of the connector node, and geometry of the measurement location
    """
    coupling = pd.read_excel(cloud.joinpath(COUPLING_XLSX))
    coupling = coupling[coupling["Waterschap"] == authority]
    specifics = pd.read_excel(cloud.joinpath(SPECIFICS_XLSX))
    specifics = specifics[specifics["Waterschap"] == authority].set_index(["MeetreeksC", "Aan/Af"])["Specifiek"]
    assert specifics.index.is_unique, f"Duplicate specific operations for {authority}"

    rows = []
    for _, row in coupling.iterrows():
        from_types, to_types = _strings(row["new_from_node_types"]), _strings(row["new_to_node_types"])
        from_points, to_points = _points(row["new_from_node_geometry"]), _points(row["new_to_node_geometry"])
        assert len(from_types) == len(to_types) == len(from_points) == len(to_points), row["MeetreeksC"]
        if len(from_types) != 1:
            LOG.info(f"Skipping {row['MeetreeksC']}: it combines {len(from_types)} links")
            continue
        if from_types[0] in OBSERVED_NODE_TYPES:
            node_type, node_geometry = from_types[0], from_points[0]
        elif to_types[0] in OBSERVED_NODE_TYPES:
            node_type, node_geometry = to_types[0], to_points[0]
        else:
            LOG.info(f"Skipping {row['MeetreeksC']}: no connector on its {from_types[0]} -> {to_types[0]} link")
            continue
        operation = specifics.get((row["MeetreeksC"], row["Aan/Af"]))
        assert pd.isna(operation) or operation in ("optellen", "negatief_maken"), (row["MeetreeksC"], operation)
        location = shapely.from_wkt(row["geometry"]) if isinstance(row["geometry"], str) else None
        rows.append(
            {
                "MeetreeksC": row["MeetreeksC"],
                "Aan/Af": row["Aan/Af"],
                "negate": operation == "negatief_maken",
                "node_type": node_type,
                "node_geometry": node_geometry,
                "geometry": location if location is not None else node_geometry,
            }
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:28992")


def read_measurements(cloud: CloudStorage) -> dict[str, pd.DataFrame]:
    """Daily measured discharges [m3/s] per measurement file, with a time index and a column per series."""
    measurements = {}
    for key, path in MEASUREMENT_CSVS.items():
        df = pd.read_csv(cloud.joinpath(path), index_col="Datum")
        df.index = pd.to_datetime(df.index).rename("time")
        measurements[key] = df
    return measurements


def add_observations(model: Model, authority: str, cloud: CloudStorage | None = None) -> Model:
    """Add an Observation node with the measured discharge for each series measured at a single connector node.

    :param model: Ribasim model
    :param authority: water authority as named in the koppeltabel
    :param cloud: cloud storage with the koppeltabel and measurements, defaults to CloudStorage()

    :return: updated Ribasim model
    """
    cloud = CloudStorage() if cloud is None else cloud
    coupling = read_coupling(cloud, authority)
    model = add_observation_nodes(model, coupling, read_measurements(cloud))
    assert model.node.df is not None
    count = (model.node.df["node_type"] == "Observation").sum()
    LOG.info(f"{authority} has {count} Observation nodes for {len(coupling)} series")
    return model


def add_observation_nodes(
    model: Model, coupling: gpd.GeoDataFrame, measurements: dict[str, pd.DataFrame], tolerance: float = 1.0
) -> Model:
    """Add Observation nodes for the series in `coupling`, see `read_coupling` and `read_measurements`.

    :param model: Ribasim model
    :param coupling: series with their observed connector node
    :param measurements: measured discharges
    :param tolerance: maximum distance between the connector node in `coupling` and in the model

    :return: updated Ribasim model
    """
    assert model.node.df is not None
    nodes = model.node.df
    period = slice(pd.Timestamp(model.starttime), pd.Timestamp(model.endtime))

    times: list[pd.DataFrame] = [] if model.observation.time.df is None else [model.observation.time.df]
    for _, row in coupling.iterrows():
        candidates = nodes[(nodes["node_type"] == row["node_type"])]
        candidates = candidates[candidates.distance(row["node_geometry"]) <= tolerance]
        if len(candidates) != 1:
            LOG.warning(f"Skipping {row['MeetreeksC']}: {len(candidates)} {row['node_type']} nodes at its location")
            continue
        node_id = int(candidates.index[0])
        observed = NodeData(node_id, row["node_type"], typing.cast(shapely.Point, candidates.at[node_id, "geometry"]))
        observation = model.observation.add(
            Node(geometry=row["geometry"], name=row["MeetreeksC"], meta_aan_af=row["Aan/Af"])
        )
        model.link.add(observation, observed)

        series = measurements["Aanvoer" if row["Aan/Af"] == "Aanvoer" else "Afvoer"][row["MeetreeksC"]]
        series = series.loc[period].dropna()
        times.append(
            pd.DataFrame(
                {
                    "node_id": observation.node_id,
                    "variable": "flow_rate",
                    "time": series.index,
                    "value": -series.to_numpy() if row["negate"] else series.to_numpy(),
                }
            )
        )

    if times:
        model.observation.time.df = DataFrame[ObservationTimeSchema](pd.concat(times, ignore_index=True))
    return model

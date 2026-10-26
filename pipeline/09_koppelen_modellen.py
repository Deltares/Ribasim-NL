# %%

import logging
import math
from pathlib import Path
from typing import NotRequired, TypedDict, cast

import geopandas as gpd
import pandas as pd
import shapely
from networkx import NetworkXNoPath
from ribasim import Node
from ribasim.nodes import manning_resistance, user_demand
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.coupling_level_apply import (
    ensure_doorlaat_afvoer_max_downstream_level,
    sync_static_controller_thresholds,
)
from ribasim_nl.coupling_level_common import finite_level, finite_levels
from ribasim_nl.coupling_level_network import first_non_junction, flow_link_graphs
from ribasim_nl.coupling_levels import run_coupling_level_check
from ribasim_nl.flow_paths import branch_from_route, merge_inflows, merge_outflows, place_on_link
from ribasim_nl.junctions import junctionify
from ribasim_nl.layout import move_nodes, offset_links, resolve_overlaps, separate_nodes
from ribasim_nl.model import find_toml
from ribasim_nl.network_snapping import snap_to_waterways
from ribasim_nl.observations import link_observations
from ribasim_nl.rwzi import attach_rwzi_junctions
from ribasim_nl.settings import settings
from shapely.geometry import LineString, Point
from tqdm import tqdm

from ribasim_nl import Model, Network

logger = logging.getLogger(__name__)

# reverse lookup: prefix -> authority name
_prefix_to_authority = {v: k for k, v in waterbeheercode.items()}

# Constants
SNAP_DISTANCE = 20
MIN_LEVEL_DIFF = 0.04  # Minimum level difference for the control
MIN_BASIN_OUTLET_DIFF = 0.5
COUPLING_LEVEL_UPSTREAM_SUPPLY_OFFSET = -0.04
COUPLING_LEVEL_RWS_PROFILE_OFFSET = 0.1
COUPLING_LEVEL_APPLY_RWS_INLET_MIN_UPSTREAM = True
COUPLING_LEVEL_APPLY_MAX_DOWNSTREAM_LEVEL = True
COUPLING_LEVEL_APPLY_DIRECT_MIN_UPSTREAM_LEVEL = True
COUPLING_LEVEL_TOLERANCE = 1e-6
COUPLING_LEVEL_APPLY_AUTHORITIES = {
    "DrentsOverijsselseDelta",
    "Noorderzijlvest",
    "Vechtstromen",
    "HunzeenAas",
    "Limburg",
    "DeDommel",
    "AaenMaas",
    "BrabantseDelta",
    "ValleienVeluwe",
    "StichtseRijnlanden",
    "RijnenIJssel",
}
# Configuration
data_dir = settings.ribasim_nl_data_dir
couple_lhm: bool = True
sub_models: list[str] = []

remove_nodes = [
    3401752,  # Dokwerd NZV
    3400015,  # Dokwerd NZV
    3400016,  # Dokwerd NZV
    3401753,  # Dokwerd NZV
    3400017,  # Dokkumer Nieuwe Zijlen NZV
    3400018,  # Dokkumer Nieuwe Zijlen NZV
    3401754,  # Dokkumer Nieuwe Zijlen NZV
    3401755,  # Dokkumer Nieuwe Zijlen NZV
    203706,  # Inlaat hoort bij NZV
    203840,  # Inlaat hoort bij NZV
    202773,  # "Gaarkeuken" Fryslân
    203809,  # "Gaarkeuken" Fryslân
    6002493,  # reparatie Helenavaart Limburg
    6000004,  # losse Limburg-boundary zonder flow-link
    6000120,  # losse Limburg-boundary zonder flow-link
    6000109,  # parallel aan AaenMaas FlowDemand-inlaat 3800601
    6000800,  # parallel aan AaenMaas FlowDemand-inlaat 3800601
    6002497,  # parallel aan doorlaat 6002496 tussen hetzelfde Limburg/AaenMaas basin-paar
    3800052,  # forceren koppeling kanaal van Deurne
    3800447,  # forceren koppeling kanaal van Deurne
    3802017,  # verwijderen rand binnenstad
    3800049,  # verwijderen rand binnenstad
    3803092,  # verwijderen zijtak Helanvaart Limburg
    3800036,  # verwijderen zijtak Helanvaart Limburg
    3800037,  # losse AaenMaas-boundary zonder flow-link
    4402318,  # parallele node Purit Vschtromen
]

# Pumps with min_upstream_level below upstream basin bottom; reset to NA
reset_pump_min_upstream_level = [
    5900602,  # min_upstream_level (2.61) below upstream Basin #4402052 bottom (3.3)
]

# Outlets with min_upstream_level below upstream basin bottom; reset to NA
reset_outlet_min_upstream_level = [
    3300727,  # min_upstream_level (14.76) below upstream Basin #4401600 bottom (16.3)
]

# Outlets with min_upstream_level below the effective upstream Basin bottom; clamp to validation-safe level
minimum_outlet_min_upstream_level = {
    3800291: 15.1,  # aanvoer min_upstream_level (9.36) below upstream Basin #3801452 bottom (15.1)
}

# force LevelBoundary node_id to Basin node_id, overriding the automatic coupling
forced_coupling = {
    3400005: 5901608,
    203787: 200184,  # ivm ontbreken Grootegaster tocht bij NZV
    203804: 200184,  # ivm ontbreken Grootegaster tocht bij NZV
    5900028: 8009406,  # ivm ontbreken Vollehovenmeer en Kadoelenmeer, voorkom dat WDOD met ZZL gekoppeld wordt
    5900029: 8009406,  # ivm ontbreken Vollehovenmeer en Kadoelenmeer, voorkom dat WDOD met ZZL gekoppeld wordt
    5900030: 8009406,  # ivm ontbreken Vollehovenmeer en Kadoelenmeer, voorkom dat WDOD met ZZL gekoppeld wordt
    5900031: 8009406,  # ivm ontbreken Vollehovenmeer en Kadoelenmeer, voorkom dat WDOD met ZZL gekoppeld wordt
    5900032: 8009406,  # ivm ontbreken Vollehovenmeer en Kadoelenmeer, voorkom dat WDOD met ZZL gekoppeld wordt
    5900033: 8009406,  # ivm ontbreken Vollehovenmeer en Kadoelenmeer, voorkom dat WDOD met ZZL gekoppeld wordt
    5900034: 8009406,  # ivm ontbreken Vollehovenmeer en Kadoelenmeer, voorkom dat WDOD met ZZL gekoppeld wordt
    5900096: 8009406,  # ivm ontbreken Vollehovenmeer en Kadoelenmeer, voorkom dat WDOD met ZZL gekoppeld wordt
    5900097: 8009406,  # ivm ontbreken Vollehovenmeer en Kadoelenmeer, voorkom dat WDOD met ZZL gekoppeld wordt
    1500551: 4000298,  # verbind Winsemius met Brielse Meer ipv Hartelkanaal.
    1500543: 8001616,  # verbind vdBurg met open zee ipv Nieuwe Waterweg
    1301498: 1300169,  # LevelBoundary buiten de boezem van HDSR geplaatst
    1301499: 1402004,  # LevelBoundary buiten de boezem van HDSR geplaatst
    1301496: 1402004,  # LevelBoundary buiten de boezem van HDSR geplaatst
    1301495: 1404801,  # LevelBoundary buiten de boezem van HDSR geplaatst; inlaat vanaf bovenstroomse TRC-basin
    1301492: 1402100,  # LevelBoundary buiten de boezem van HDSR geplaatst; pomp naar benedenstroomse TRC-basin
    4400015: 5903236,  # LB ligt op exact dezelfde locatie
    4402348: 5900072,  # LB ligt op exact dezelfde locatie
    6000124: 6002492,  # Reparatie Helenavaart
    6000125: 3801280,  # Forceren Kanaal van Deurne
    3800053: 3801280,  # Forceren Kanaal van Deurne
    2700009: 3801394,  # Vuchterstuw naar Binnenstad
    3800048: 3801394,  # Aansluiten Drongelens kanaal op Binnenstad
    3300009: 4401377,  # Levelboundary takt niet aan op Verlengde Hoogeveense Vaart
    6000003: 3801961,  # Forceren zijtak Helanvaart Limburg
    3800029: 6002294,  # Defensiekanaal Limburg: voorkom koppeling via Junction 6003577
    3801958: 6002408,  # AaenMaas FlowDemand-inlaat 3800601 koppelen aan Limburg-basin
    # Dokkumer Nieuwe Zijlen, zie #824: Fryslân-uitlaten op NZV-afvoerjunction 3401931, inlaat 203888 vanaf
    # NZV-aanvoerjunction 3402060. Een inlaat kan niet vanaf 3401931, die alleen connectors bovenstrooms heeft.
    203885: 3401931,
    203887: 3402060,
    203898: 3401931,
    203900: 3401931,
}

# Connector nodes whose link with a Rijkswaterstaat Basin starts straight, to be routed over its water, see
# `route_links_between_models`, rather than from the Rijkswaterstaat waterway nearest to them, see `get_rws_link`
straight_rws_links = {
    1100514,  # Opvoergemaal Steenenbeer, uit het IJmeer en niet via de Muidertrekvaart en het ARK, zie #916
    1101053,  # Zeesluis Muiden, uit het IJmeer en niet via de Muidertrekvaart en het ARK, zie #916
}

# Nodes to move after coupling, by (dx, dy) in meters
node_offsets = {
    3401931: (10.0, 0.0),  # NZV-afvoerjunction visueel scheiden van aanvoerjunction 3402060, zie #824
}

# Outlets that are open water connections, converted to ManningResistance after coupling, removing their control
outlet_to_manning_resistance = {
    1100600: {"profile_width": 10.0, "manning_n": 0.02},  # open water Angstel - ARK, zie #820
    # open water van Stichtse Rijnlanden naar andere beheerders, met de breedte van het BGT-water, zie #912
    1400719: {"profile_width": 40.0, "manning_n": 0.03},  # OW669, naar AmstelGooienVecht
    1400731: {"profile_width": 10.0, "manning_n": 0.03},  # OW690, naar AmstelGooienVecht
    1400734: {"profile_width": 7.0, "manning_n": 0.04},  # OW694, vistrap naar het ARK
}


class UserDemandConfig(TypedDict):
    """A UserDemand between Basins of different models, see `add_user_demands`."""

    name: str
    geometry: Point
    from_basin_id: int
    to_basin_id: int
    demand: float
    branch_towards_id: NotRequired[int]


class RouteJunctionConfig(TypedDict):
    """Links from a Basin to nodes, merged into a Junction on a link of that Basin, see `change_routes`."""

    basin_id: int
    node_ids: list[int]
    link_id: int


# UserDemands across models, added after coupling with straight links (pipelines), with the location in RD
user_demands: list[UserDemandConfig] = [
    {
        "name": "Evides Industriewater DWP Maasvlakte",  # zie #857
        "geometry": Point(65911, 439586),
        "from_basin_id": 4000298,  # Brielse Meer
        "to_basin_id": 8001886,  # Beerkanaal, 2e Maasvlakte
        "demand": 0.222,  # 800 m3/uur netto capaciteit
    },
    {
        "name": "Evides Industriewater DWP Botlek",  # zie #857
        "geometry": Point(76710, 433932),
        "from_basin_id": 4000234,  # Scheepvaart- en Voedingskanaal, zie #871
        "branch_towards_id": 4002322,  # vanaf de route naar ManningResistance 4002322, zie #871
        "to_basin_id": 8002247,  # Calandkanaal
        "demand": 0.388,  # 1400 m3/uur netto capaciteit
    },
]
USER_DEMAND_PRIORITY = 3  # industrie, as in pipeline/rijkswaterstaat/7_model_onttrekkingen.py
USER_DEMAND_MIN_LEVEL_OFFSET = 0.1  # above the bottom of the source Basin

# Nodes whose link from a Basin branches off the route from that Basin towards another node, see `branch_from_route`
route_branches = {
    1200327: (1202747, 1200393),  # HHNK-uitlaat vanaf de route naar uitlaat 1200393, zie #871
    1500460: (4000298, 4001451),  # Gemaal Winsemius vanaf de route naar Stenen Baak, zie #871
}

# Links from a Basin to nodes, merged into a Junction on a link of that Basin, see `merge_inflows` and `place_on_link`
route_junctions_on_link: list[RouteJunctionConfig] = [
    # inlaten Boven Regge op de Schipbeek, zie #861
    {"basin_id": 700797, "node_ids": [4401049, 4401103, 4401104], "link_id": 700438},
]

# Nodes connected directly to the first Junction downstream that their routes share, see `merge_outflows`
merged_outflows = [
    [4401049, 4401103, 4401104],  # inlaten Boven Regge, zie #861
]

# Node names to set after coupling
node_names = {
    203899: "Dokkumer Nieuwe Zijlen schutsluis",  # zie #824
    203901: "Dokkumer Nieuwe Zijlen stuw",  # zie #824
}

# Static metadata of connector nodes to set after coupling, e.g. levels that were boundary placeholders
connector_static_meta = {
    203886: {"meta_to_level": -0.60},  # zie #824
    203888: {"meta_from_level": -2.35},  # zie #824
    203899: {"meta_to_level": -0.60},  # zie #824
    203901: {"meta_to_level": -0.60},  # zie #824
}


# %% Functions
def replace_listen_node_id(model: Model, old_node_id: int, new_node_id: int | None) -> bool:
    """Replace listen_node_id references in all control tables.

    If new_node_id is None, removes the entries instead of replacing.
    Returns True if any references were found.
    """
    has_control = False
    tables = [
        model.discrete_control.variable.df,
        model.continuous_control.variable.df,
        model.pid_control.static.df,
        model.pid_control.time.df,
    ]
    for df in tables:
        if df is None:
            continue
        mask = df.listen_node_id == old_node_id
        if mask.any():
            has_control = True
            if new_node_id is not None:
                df.loc[mask, "listen_node_id"] = new_node_id
            else:
                df.drop(df.index[mask], inplace=True)
    return has_control


def remove_own_control_nodes(model: Model, node_id: int) -> None:
    """Remove the control nodes of `node_id` that control no other node."""
    assert model.link.df is not None
    control_df = model.link.df[model.link.df["link_type"] == "control"]
    for control_node_id in control_df.loc[control_df["to_node_id"] == node_id, "from_node_id"].unique():
        if (control_df["from_node_id"] == control_node_id).sum() == 1:
            model.remove_node(int(control_node_id), remove_links=True)


def initialize_models(toml_file: Path) -> tuple[Model, Network, gpd.GeoDataFrame]:
    """Initialize and load the model and network data.

    Parameters
    ----------
    toml_file : Path
        Path to the TOML file

    Returns
    -------
    tuple[Model, Network, gpd.GeoDataFrame]
        Tuple of (model, network, basin_areas_df)
    """
    # Load the model
    model = Model.read(toml_file)

    # Remove nodes if exists, with the control nodes that only control them, since their ids are not stable
    assert model.node.df is not None and model.link.df is not None
    for i in remove_nodes:
        if i not in model.node.df.index:
            continue
        controlled = model.link.df.loc[model.link.df["from_node_id"] == i, "to_node_id"].tolist()
        if model.get_node_type(i) == "DiscreteControl" and controlled:
            raise ValueError(f"DiscreteControl {i} in remove_nodes still controls {controlled}, remove those first.")
        replace_listen_node_id(model, i, None)
        remove_own_control_nodes(model, i)
        model.remove_node(node_id=i, remove_links=True)

    # Load the network
    network_gpkg = data_dir / "Rijkswaterstaat/verwerkt/netwerk.gpkg"
    network = Network.from_network_gpkg(network_gpkg)

    # Prepare basin areas dataframe
    assert model.basin.area.df is not None
    basin_areas_df = model.basin.area.df.set_index("node_id")
    assert model.basin.node is not None and model.basin.node.df is not None
    waterbeheerder_df = model.basin.node.df["meta_waterbeheerder"]
    basin_areas_df.loc[waterbeheerder_df.index, "meta_waterbeheerder"] = waterbeheerder_df

    return model, network, basin_areas_df


def create_link_geometry(
    couple_authority: str, boundary_node_authority: str, kwargs: dict, network: Network, couple_with_basin_id: int
) -> LineString:
    """Create appropriate link geometry based on authority type.

    Parameters
    ----------
    couple_authority : str
        Authority to couple with
    boundary_node_authority : str
        Authority of the boundary node
    kwargs : dict
        Link parameters including from_node and to_node
    network : Network
        Network object for RWS links
    couple_with_basin_id : int
        Basin ID to couple with

    Returns
    -------
    LineString
        LineString geometry for the link
    """
    straight = {kwargs["from_node"].node_id, kwargs["to_node"].node_id} & straight_rws_links
    if couple_authority == "Rijkswaterstaat" and not straight:
        if kwargs["meta_to_authority"] != boundary_node_authority:  # uitlaat
            geometry = get_rws_link(
                network=network,
                on_network_point=kwargs["to_node"].geometry,
                to_be_projected_point=kwargs["from_node"].geometry,
                reversed=False,
            )
        else:  # inlaat
            geometry = get_rws_link(
                network=network,
                on_network_point=kwargs["from_node"].geometry,
                to_be_projected_point=kwargs["to_node"].geometry,
                reversed=True,
            )
    else:
        # For non-RWS authorities and `straight_rws_links`, use simple line geometry
        geometry = LineString([kwargs["from_node"].geometry, kwargs["to_node"].geometry])

    return geometry


def reverse_flow_link_ids(model: Model, from_node_id: int, to_node_id: int) -> list[int]:
    """The flow links of an existing path from `to_node_id` to `from_node_id`, directly or through Junctions."""
    assert model.link.df is not None
    flow = model.link.df[model.link.df["link_type"] == "flow"]
    assert model.node.df is not None
    node_types = model.node.df["node_type"]
    stack: list[tuple[int, list[int]]] = [(to_node_id, [])]
    seen = {to_node_id}
    while stack:
        node_id, path = stack.pop()
        for link_id, next_node_id in flow.loc[flow["from_node_id"] == node_id, "to_node_id"].items():
            link_id = cast(int, link_id)
            if next_node_id == from_node_id:
                return [*path, int(link_id)]
            if node_types[next_node_id] == "Junction" and next_node_id not in seen:
                seen.add(next_node_id)
                stack.append((int(next_node_id), [*path, int(link_id)]))
    return []


def add_control(
    model: Model,
    couple_authority: str,
    has_control: bool,
    connector_node_id: pd.Series,
    upstream_basin: int | None,
    downstream_basin: int | None,
) -> None:
    """Add control for non-RWS boundaries when needed.

    Args:
        model: Model to add control to
        couple_authority: Authority to couple with
        has_control: Whether control already exists
        connector_node_id: Connector node series
        upstream_basin: Upstream basin ID (can be None)
        downstream_basin: Downstream basin ID (can be None)
    """
    if (
        couple_authority != "Rijkswaterstaat"
        and not has_control
        and len(connector_node_id) == 1
        and upstream_basin is not None
        and downstream_basin is not None
    ):
        ctrl_type = "DiscreteControl"  # Changed from ContinuousControl
        data = []  # Simplified for now - needs proper control data structure
        print(
            f"Adding {ctrl_type} for {connector_node_id.iloc[0]}, "
            f"while having {len(connector_node_id)} connector nodes."
        )
        # Note: The continuous_control parameters may need adjustment based on actual API
        # This is a placeholder implementation
        try:
            if data != []:
                model.add_control_node(
                    to_node_id=connector_node_id.iloc[0],
                    data=data,  # Simplified for now - needs proper control data structure
                    ctrl_type=ctrl_type,
                    node_offset=20,
                )
        except Exception as e:
            print(f"Failed to add control node: {e}")


def process_boundary_nodes(model: Model, network: Network, basin_areas_df: gpd.GeoDataFrame) -> list[dict]:
    """Process all boundary nodes to create coupling links.

    Parameters
    ----------
    model : Model
        Model to process
    network : Network
        Network for geometry creation
    basin_areas_df : gpd.GeoDataFrame
        Basin areas dataframe

    Returns
    -------
    list[dict]
        List of link table entries
    """
    assert model.basin.node is not None and model.basin.node.df is not None
    unique_waterbeheerders = model.basin.node.df.meta_waterbeheerder.unique()
    assert model.level_boundary.node is not None and model.level_boundary.node.df is not None
    boundary_node_ids = model.level_boundary.node.df[
        model.level_boundary.node.df.meta_couple_authority.isin(unique_waterbeheerders)
    ].index.to_list()

    all_link_table = []

    assert model.link.df is not None
    assert model.node.df is not None
    for boundary_node_id in tqdm(boundary_node_ids, desc="Coupling boundary nodes"):
        # Check whether the boundary has been merged already
        if boundary_node_id not in model.level_boundary.node.df.index:
            continue

        boundary_node = model.level_boundary[boundary_node_id]
        boundary_node_authority = cast(str, model.level_boundary.node.df.at[boundary_node_id, "meta_waterbeheerder"])
        couple_authority = cast(str, model.level_boundary.node.df.at[boundary_node_id, "meta_couple_authority"])

        # Some levelboundaries don't need to be coupled
        if pd.isna(couple_authority) or couple_authority in ("Noordzee", "Buitenland"):
            continue

        if couple_authority == boundary_node_authority:
            print(f"Boundary node {boundary_node} is already coupled with {couple_authority}.")
            continue

        # we check if coupling is overruled
        if boundary_node_id in forced_coupling:
            couple_with_basin_id = forced_coupling[boundary_node_id]
            if couple_with_basin_id not in model.node.df.index:
                # determine the authority that owns the missing target node
                target_prefix = couple_with_basin_id // 10**5
                target_authority = _prefix_to_authority.get(target_prefix)
                included_authorities = set(model.node.df["meta_waterbeheerder"].dropna().unique())
                if target_authority and target_authority not in included_authorities:
                    logger.warning(
                        "Forced coupling target %d (%s) not in model (authority not included), skipping %s.",
                        couple_with_basin_id,
                        target_authority,
                        boundary_node,
                    )
                else:
                    raise KeyError(
                        f"Forced coupling target {couple_with_basin_id} not found in model, "
                        f"but its authority '{target_authority}' is included. "
                        f"Check forced_coupling for boundary node {boundary_node_id}."
                    )
                continue
        else:
            # Check whether there are very close LB from the other authority
            lb_neighbors = model.level_boundary.node.df[
                model.level_boundary.node.df.meta_waterbeheerder == couple_authority
            ]
            distances = lb_neighbors.distance(boundary_node.geometry)
            lb_neighbors = lb_neighbors[distances < SNAP_DISTANCE]

            if len(lb_neighbors) > 1:
                print("Multiple close LB found, please check manually.")
                continue

            if len(lb_neighbors) == 1:
                merged_outlet = merge_lb(model, lb_neighbors, boundary_node_id)
                if merged_outlet is not None:
                    # TODO: Add Continuous control for the merged outlet?
                    continue

            distances = basin_areas_df[basin_areas_df.meta_waterbeheerder == couple_authority].distance(
                boundary_node.geometry
            )

            # Can happen if we don't couple all models
            if len(distances) == 0:
                print(f"Cannot find {couple_authority} basin area for {boundary_node}.")
                continue

            # Couple with closest basin area
            couple_with_basin_id = cast(int, distances.idxmin())

        # Create link table
        link_table = []

        # Upstream nodes (uitlaat)
        from_node_ids = model.upstream_node_id(boundary_node_id)
        if from_node_ids is not None:
            if not isinstance(from_node_ids, pd.Series):
                from_node_ids = pd.Series([from_node_ids])
            connector_node_id = from_node_ids
            upstream_basin = model.upstream_node_id(connector_node_id.iloc[0])
            downstream_basin = couple_with_basin_id
            link_table += [
                {
                    "from_node": model.get_node(i),
                    "to_node": model.get_node(couple_with_basin_id),
                    "meta_from_authority": boundary_node_authority,
                    "meta_to_authority": couple_authority,
                }
                for i in from_node_ids
            ]

        # Downstream nodes (inlaat)
        to_node_ids = model.downstream_node_id(boundary_node_id)
        if to_node_ids is not None:
            if not isinstance(to_node_ids, pd.Series):
                to_node_ids = pd.Series([to_node_ids])
            connector_node_id = to_node_ids
            upstream_basin = couple_with_basin_id
            downstream_basin = model.downstream_node_id(connector_node_id.iloc[0])
            link_table += [
                {
                    "from_node": model.get_node(couple_with_basin_id),
                    "to_node": model.get_node(i),
                    "meta_from_authority": couple_authority,
                    "meta_to_authority": boundary_node_authority,
                }
                for i in to_node_ids
            ]

        # Replace boundary id in all control tables with basin id
        has_control = replace_listen_node_id(model, boundary_node_id, couple_with_basin_id)
        if has_control and model.get_node_type(couple_with_basin_id) == "Junction":
            raise ValueError(f"Control listens to {boundary_node}, which cannot be coupled to a Junction.")

        # Add control node for non RWS boundaries when no control node is yet present
        # Disabled since no data is supplied yet to the control node
        add_control(model, couple_authority, has_control, connector_node_id, upstream_basin, downstream_basin)

        # Remove boundary node from model
        model.remove_node(boundary_node_id, remove_links=True)

        # Add links with geometry
        for kwargs in link_table:
            geometry = create_link_geometry(
                couple_authority, boundary_node_authority, kwargs, network, couple_with_basin_id
            )

            # Check for cycles
            cycle = reverse_flow_link_ids(model, kwargs["from_node"].node_id, kwargs["to_node"].node_id)
            if cycle:
                print(f"Link {kwargs['to_node']} -> {kwargs['from_node']} already exists.")
                model.link.df.drop(cycle, inplace=True)
                if kwargs["to_node"].node_type != "Basin":
                    print("Removing node", kwargs["to_node"])
                    model.remove_node(kwargs["to_node"].node_id, remove_links=True)
                else:
                    print("Removing node", kwargs["from_node"])
                    model.remove_node(kwargs["from_node"].node_id, remove_links=True)
                continue

            model.link.add(**kwargs, geometry=geometry)
            kwargs["geometry"] = geometry
            all_link_table.append(kwargs)

    return all_link_table


def fix_basin_profiles(model: Model) -> None:
    """Fix minimum upstream level of outlets by adjusting basin profiles.

    Args:
        model: Model to fix profiles for
    """
    assert model.pump.static.df is not None
    for pump_id in reset_pump_min_upstream_level:
        mask = model.pump.static.df.node_id == pump_id
        if mask.any():
            model.pump.static.df.loc[mask, "min_upstream_level"] = -math.inf

    assert model.outlet.static.df is not None
    for outlet_id in reset_outlet_min_upstream_level:
        mask = model.outlet.static.df.node_id == outlet_id
        if mask.any():
            model.outlet.static.df.loc[mask, "min_upstream_level"] = -math.inf

    for outlet_id, min_upstream_level in minimum_outlet_min_upstream_level.items():
        mask = model.outlet.static.df.node_id == outlet_id
        if mask.any():
            current = finite_levels(model.outlet.static.df.loc[mask, "min_upstream_level"])
            lower_than_minimum = current.lt(min_upstream_level).fillna(False)
            model.outlet.static.df.loc[current.loc[lower_than_minimum].index, "min_upstream_level"] = min_upstream_level
            if lower_than_minimum.any():
                sync_static_controller_thresholds(
                    model=model,
                    target_node_ids={outlet_id},
                    tolerance=COUPLING_LEVEL_TOLERANCE,
                )

    assert model.outlet.node is not None and model.outlet.node.df is not None
    assert model.basin.profile.df is not None
    for outlet in model.outlet.node.df.index:
        upstream_basin = model.upstream_node_id(outlet)
        if upstream_basin is None:
            continue
        if not isinstance(upstream_basin, pd.Series):
            upstream_basin = pd.Series([upstream_basin])
        for upstream_basin_id in upstream_basin:
            mask = model.basin.profile.df.node_id == upstream_basin_id
            basin = model.basin.profile.df[mask]
            # Get the current minimum level of the outlet
            min_level = model.outlet.static.df.set_index("node_id").at[outlet, "min_upstream_level"]
            if isinstance(min_level, pd.Series):
                min_level = min_level.iloc[0]
            # a missing or infinite min_upstream_level means no limit
            min_level = finite_level(min_level)
            if len(basin.level) == 0 or pd.isna(min_level):
                continue
            if min_level < basin.level.iloc[0]:
                print(f"Lowering basin {upstream_basin_id} profile {min_level}.")
                model.basin.profile.df.loc[(mask[mask]).index[0], "level"] = min_level - MIN_BASIN_OUTLET_DIFF


def node_in_model(model: Model, node_id: int) -> bool:
    """Check if a configured node is in the model, raising if its authority is included but the node is not."""
    assert model.node.df is not None
    if node_id in model.node.df.index:
        return True
    authority = _prefix_to_authority.get(node_id // 10**5)
    if authority in set(model.node.df["meta_waterbeheerder"].dropna().unique()):
        raise KeyError(f"Node {node_id} not found in model, but its authority '{authority}' is included.")
    return False


def update_node_metadata(model: Model) -> None:
    """Set the configured node names and connector static metadata."""
    assert model.node.df is not None
    for node_id, name in node_names.items():
        if node_in_model(model, node_id):
            model.node.df.loc[node_id, "name"] = name

    for node_id, meta in connector_static_meta.items():
        if not node_in_model(model, node_id):
            continue
        static_df = model.get_component(model.get_node_type(node_id)).static.df
        mask = static_df.node_id == node_id
        if not mask.any():
            raise KeyError(f"Node {node_id} in connector_static_meta has no static rows.")
        for column, value in meta.items():
            static_df.loc[mask, column] = value


def move_configured_nodes(model: Model) -> None:
    """Move the configured nodes by their offsets, see `node_offsets`."""
    assert model.node.df is not None
    locations = {}
    for node_id, (dx, dy) in node_offsets.items():
        if node_in_model(model, node_id):
            point = cast(Point, model.node.df.at[node_id, "geometry"])
            locations[node_id] = Point(point.x + dx, point.y + dy)
    move_nodes(model, locations)


def convert_outlets_to_manning_resistance(model: Model) -> None:
    """Convert the configured Outlets between two Basins to ManningResistance, removing their controllers."""
    assert model.link.df is not None
    assert model.node.df is not None
    for node_id, parameters in outlet_to_manning_resistance.items():
        if not node_in_model(model, node_id):
            continue
        node_type = model.get_node_type(node_id)
        if node_type != "Outlet":
            raise ValueError(f"Expected Outlet for node {node_id}, got {node_type}.")
        # the Basins may be behind Junctions
        outgoing, incoming = flow_link_graphs(model.link.df.reset_index())
        node_types = model.node.df["node_type"].to_dict()
        for graph in (incoming, outgoing):
            neighbor_id, _, _ = first_non_junction(node_id, graph, node_types)
            if neighbor_id is None or node_types[neighbor_id] != "Basin":
                raise ValueError(f"Outlet {node_id} should connect two Basins to become a ManningResistance.")

        control_df = model.link.df[model.link.df["link_type"] == "control"]
        for control_node_id in control_df.loc[control_df["to_node_id"] == node_id, "from_node_id"]:
            if (control_df["from_node_id"] == control_node_id).sum() != 1:
                raise ValueError(f"Control node {control_node_id} controls more nodes than Outlet {node_id}.")
        remove_own_control_nodes(model, node_id)

        static = manning_resistance.Static(
            length=[100.0],
            manning_n=[parameters["manning_n"]],
            profile_width=[parameters["profile_width"]],
            profile_slope=[3.0],
        )
        model.update_node(node_id, "ManningResistance", [static])


def route_links_between_models(model: Model, network: Network) -> list[int]:
    """Route the straight links between models, and in the Rijkswaterstaat model, along waterways and water.

    The connector nodes are often outside the area of the receiving Basin, so waterways up to 250 m around it are
    used, see `snap_to_waterways`.
    """
    assert model.link.df is not None
    nodes, flow = model.node.df, model.link.df[model.link.df["link_type"] == "flow"]
    assert nodes is not None
    from_authority = flow["from_node_id"].map(nodes["meta_waterbeheerder"]).to_numpy()
    to_authority = flow["to_node_id"].map(nodes["meta_waterbeheerder"]).to_numpy()
    straight = shapely.get_num_points(flow.geometry.to_numpy()) == 2
    rws = (from_authority == "Rijkswaterstaat") & (to_authority == "Rijkswaterstaat")
    link_ids = flow.index[straight & ((from_authority != to_authority) | rws)].tolist()
    top10nl = data_dir / "Basisgegevens/Top10NL/top10nl_Compleet.gpkg"
    snap_to_waterways(model, top10nl, link_ids, main_route=network.links, tolerance=250.0)
    return link_ids


def add_user_demands(model: Model) -> None:
    """Add the configured UserDemands between Basins of different models, see `user_demands`."""
    assert model.basin.profile.df is not None
    for config in user_demands:
        from_basin_id, to_basin_id = config["from_basin_id"], config["to_basin_id"]
        if not (node_in_model(model, from_basin_id) and node_in_model(model, to_basin_id)):
            continue
        for basin_id in (from_basin_id, to_basin_id):
            node_type = model.get_node_type(basin_id)
            if node_type != "Basin":
                raise ValueError(f"Expected Basin for node {basin_id} of UserDemand {config['name']}, got {node_type}.")

        bottom = model.basin.profile.df.loc[model.basin.profile.df.node_id == from_basin_id, "level"].min()
        node = Node(
            model.next_node_id,
            config["geometry"],
            name=config["name"],
            meta_sector="industrie",
            meta_waterbeheerder="Rijkswaterstaat",
        )
        static = user_demand.Static(
            demand_priority=[USER_DEMAND_PRIORITY],
            demand=[config["demand"]],
            return_factor=[0.0],
            min_level=[bottom + USER_DEMAND_MIN_LEVEL_OFFSET],
        )
        demand_node = model.user_demand.add(node, [static])
        model.link.add(model.basin[from_basin_id], demand_node)
        model.link.add(demand_node, model.basin[to_basin_id])
        if "branch_towards_id" in config:
            branch_from_route(model, demand_node.node_id, from_basin_id, config["branch_towards_id"])


def change_routes(model: Model) -> None:
    """Change the configured routes, see `route_branches`, `route_junctions_on_link` and `merged_outflows`."""
    for node_id, (basin_id, towards_id) in route_branches.items():
        if node_in_model(model, node_id) and node_in_model(model, basin_id):
            branch_from_route(model, node_id, basin_id, towards_id)

    for config in route_junctions_on_link:
        basin_id, node_ids = config["basin_id"], config["node_ids"]
        if all(node_in_model(model, i) for i in [basin_id, *node_ids]):
            junction_id = merge_inflows(model, basin_id, node_ids)
            place_on_link(model, junction_id, basin_id, config["link_id"])

    for node_ids in merged_outflows:
        if all(node_in_model(model, i) for i in node_ids):
            merge_outflows(model, node_ids)


def tidy_coupling_layout(model: Model, link_ids: list[int]) -> None:
    """Tidy the layout of the coupling: the layout of the models is already tidy, see `tidy_layout`.

    The links between models share routes towards the same Basins, which are merged into Junctions, and these and
    the routed links `link_ids` are offset, such that no links overlap.
    """
    assert model.link.df is not None
    link_ids = [i for i in link_ids if i in model.link.df.index]  # some were replaced, see `change_routes`
    assert model.node.df is not None
    junctions = set(model.node.df.index[model.node.df["node_type"] == "Junction"])
    junctionify(model)
    nodes, flow = model.node.df, model.link.df[model.link.df["link_type"] == "flow"]
    assert nodes is not None
    new_junctions = set(nodes.index[nodes["node_type"] == "Junction"]) - junctions
    authority = nodes["meta_waterbeheerder"]
    between = flow["from_node_id"].map(authority).to_numpy() != flow["to_node_id"].map(authority).to_numpy()
    at_new_junction = (flow["from_node_id"].isin(new_junctions) | flow["to_node_id"].isin(new_junctions)).to_numpy()
    offset_links(model, link_ids={*flow.index[between | at_new_junction], *link_ids})
    separate_nodes(model)
    resolve_overlaps(model, max_iterations=10)


def remove_invalid_topology_nodes(model: Model) -> None:
    """Remove nodes with invalid flow/control topology and clean up their control references."""
    for link_type in ["flow", "control"]:
        invalid_topology = model.invalid_topology_at_node(link_type=link_type)
        while not invalid_topology.empty:
            for node_id, row in invalid_topology.iterrows():
                node_id = cast(int, node_id)
                logger.warning(
                    "Invalid %s topology at node %d (%s): %s — removing node.",
                    link_type,
                    node_id,
                    row["node_type"],
                    row["exception"],
                )
                replace_listen_node_id(model, node_id, None)
                model.remove_node(node_id, remove_links=True)
            invalid_topology = model.invalid_topology_at_node(link_type=link_type)


def save_model_and_outputs(model: Model, all_link_table: list[dict], toml_file: Path) -> Path:
    """Save the model and create output files.

    Args:
        model: Model to save
        all_link_table: Link table data
        toml_file: Path to the input/decoupled TOML file

    Returns
    -------
        Path to the written coupled TOML file.
    """
    # Derive model path from input toml_file, adding -coupled to folder and file name
    root = toml_file.parents[1]
    decoupled_model_name = toml_file.stem
    model_name = f"{decoupled_model_name}_coupled"
    model_path = root / model_name
    output_toml_file = model_path / f"{model_name}.toml"
    ensure_doorlaat_afvoer_max_downstream_level(
        model,
        apply_authorities=COUPLING_LEVEL_APPLY_AUTHORITIES,
    )
    model.write(output_toml_file)
    model.validate_ribasim_nl()

    # Save links
    links = gpd.GeoDataFrame(all_link_table)
    links["from_node_id"] = links.from_node.apply(lambda x: x.node_id)
    links["to_node_id"] = links.to_node.apply(lambda x: x.node_id)
    links = links.drop(columns=["from_node", "to_node"])
    links.to_file(model_path / "link.gpkg")
    return output_toml_file


def run_configured_coupling_level_check(toml_file: Path) -> None:
    run_coupling_level_check(
        toml_file=toml_file,
        upstream_supply_offset=COUPLING_LEVEL_UPSTREAM_SUPPLY_OFFSET,
        rws_profile_offset=COUPLING_LEVEL_RWS_PROFILE_OFFSET,
        apply_rws_inlet_min_upstream=COUPLING_LEVEL_APPLY_RWS_INLET_MIN_UPSTREAM,
        apply_max_downstream_level=COUPLING_LEVEL_APPLY_MAX_DOWNSTREAM_LEVEL,
        apply_direct_min_upstream_level=COUPLING_LEVEL_APPLY_DIRECT_MIN_UPSTREAM_LEVEL,
        tolerance=COUPLING_LEVEL_TOLERANCE,
        apply_authorities=COUPLING_LEVEL_APPLY_AUTHORITIES,
    )
    model = Model.read(toml_file)
    if ensure_doorlaat_afvoer_max_downstream_level(
        model,
        apply_authorities=COUPLING_LEVEL_APPLY_AUTHORITIES,
    ):
        model.write(toml_file)
        model.validate_ribasim_nl()


def get_rws_link(
    network: Network,
    on_network_point: Point,
    to_be_projected_point: Point,
    reversed: bool = True,
) -> LineString:
    """Get a link-geometry from a network node to another point, possibly reversed.

    Args:
        network (Network): network to get geometry from
        on_network_point (Point): point to take closest network-node from, typically a Basin
        to_be_projected_point (Point): point to project to closest network link, typically an Outlet
        reversed (bool, optional): If True LineString vertices are ordered from on_network_point to to_be_projected_point
        If false LineString will be reversed. Defaults to True.
    """
    # from_network_node is closest node in network
    # TODO: Ideally we add/snap the node to the network, now we see overlaps/reversals
    connection_nodes = network.nodes[network.nodes["type"] == "connection"]
    to_network_node = connection_nodes.distance(on_network_point).idxmin()

    # now get closest links
    link_idx = iter(network.links.distance(to_be_projected_point).sort_values().index)
    link_geometry = None
    while link_geometry is None or link_geometry.is_empty:  # links can now be empty?
        try:
            idx = next(link_idx)
            link_geom = cast(LineString, network.links.at[idx, "geometry"])
            projected_point = link_geom.interpolate(link_geom.project(to_be_projected_point))
            # If the projected point is not close to any network node, add a new node
            if network.nodes.distance(projected_point).min() > SNAP_DISTANCE:
                from_network_node = network.add_node(projected_point, max_distance=SNAP_DISTANCE - 1)
            else:
                from_network_node = network.nodes.distance(projected_point).idxmin()
            link_geometry = network.get_line(from_network_node, to_network_node)
        except NetworkXNoPath:
            continue
        except StopIteration:
            link_geometry = LineString(tuple(to_be_projected_point.coords) + tuple(on_network_point.coords))

    # If the generated link doesn't start or end with the point, add it.
    if not link_geometry.boundary.geoms[0].equals(to_be_projected_point):
        link_geometry = LineString(tuple(to_be_projected_point.coords) + tuple(link_geometry.coords))
    if not link_geometry.boundary.geoms[1].equals(on_network_point):
        link_geometry = LineString(tuple(link_geometry.coords) + tuple(on_network_point.coords))

    if reversed:
        return link_geometry.reverse()
    else:
        return link_geometry


def merge_lb(model: Model, lb_neighbors: pd.DataFrame, boundary_node_id: int):
    """Merge level boundary nodes when they are close to each other.

    Args:
        model: Model to modify
        lb_neighbors: DataFrame of neighboring level boundary nodes
        boundary_node_id: ID of the boundary node to merge
    """
    neighbor_id = lb_neighbors.index[0]
    neighbor_node = model.level_boundary[neighbor_id]
    boundary_node = model.level_boundary[boundary_node_id]
    print(f"Merging {boundary_node} => {neighbor_node}.")

    from_node_ids = model.upstream_node_id(boundary_node_id)
    to_node_ids = model.downstream_node_id(boundary_node_id)

    # Inlet
    if from_node_ids is None and to_node_ids is not None:
        from_node_ids = model.upstream_node_id(neighbor_id)
        if from_node_ids is None:
            print(f"Cannot merge {boundary_node} => {neighbor_node}: Wrong direction")
            return
        if model.downstream_node_id(neighbor_id) is not None:
            print(f"Cannot merge {neighbor_node} => {boundary_node}: {neighbor_id} has both inflows and outflows.")
            return

    # Outlet
    elif to_node_ids is None and from_node_ids is not None:
        to_node_ids = model.downstream_node_id(neighbor_id)
        if to_node_ids is None:
            print(f"Cannot merge {neighbor_node} => {boundary_node}: Wrong direction")
            return
        if model.upstream_node_id(neighbor_id) is not None:
            print(f"Cannot merge {neighbor_node} => {boundary_node}: {neighbor_id} has both inflows and outflows.")
            return

    if isinstance(from_node_ids, pd.Series) or isinstance(to_node_ids, pd.Series):
        print(f"Cannot merge {neighbor_node} => {boundary_node}: Multiple inlets/outlets, please check manually.")
        return

    # TODO Handle Pump/Pump and Outlet/Pump
    if model.get_node_type(from_node_ids) != "Outlet" or model.get_node_type(to_node_ids) != "Outlet":
        print(
            f"Cannot merge {boundary_node} => {neighbor_node}: Expected Outlet, got {model.get_node_type(from_node_ids)} and {model.get_node_type(to_node_ids)}"
        )
        return

    # only merge if both nodes are controlled
    assert model.link.df is not None
    controlled_nodes = model.link.df[model.link.df["link_type"] == "control"].to_node_id.values
    if (from_node_ids in controlled_nodes) != (to_node_ids in controlled_nodes):
        print(f"Cannot merge {boundary_node} => {neighbor_node}: One of the two is controlled, the other is not")
        return

    # Update listen references for removed boundary nodes, to the Basins behind any Junctions: the boundary
    # downstream of the upstream outlet stands for the downstream Basin, the other one for the upstream Basin
    outgoing, incoming = flow_link_graphs(model.link.df.reset_index())
    assert model.node.df is not None
    node_types = model.node.df["node_type"].to_dict()
    upstream_basin_id, _, _ = first_non_junction(from_node_ids, incoming, node_types)
    downstream_basin_id, _, _ = first_non_junction(to_node_ids, outgoing, node_types)
    for lb_id in (boundary_node_id, neighbor_id):
        upstream_id = model.upstream_node_id(lb_id)
        is_downstream = not isinstance(upstream_id, pd.Series) and upstream_id == from_node_ids
        replacement_basin = downstream_basin_id if is_downstream else upstream_basin_id
        if replacement_basin is None:
            replacement_basin = upstream_basin_id if is_downstream else downstream_basin_id
        replace_listen_node_id(model, lb_id, replacement_basin)

    # Remove boundary node from model
    model.remove_node(boundary_node_id, remove_links=True)
    model.remove_node(neighbor_id, remove_links=True)

    # And merge the outlets
    merged_outlet = model.merge_outlets(from_node_ids, to_node_ids)
    return merged_outlet


def couple_model(toml_file: Path) -> None:
    """Couple the models merged in `toml_file`, and write the coupled model next to it."""
    model, network, basin_areas_df = initialize_models(toml_file)
    all_link_table = process_boundary_nodes(model, network, basin_areas_df)
    update_node_metadata(model)
    move_configured_nodes(model)
    convert_outlets_to_manning_resistance(model)
    fix_basin_profiles(model)
    remove_invalid_topology_nodes(model)
    link_ids = route_links_between_models(model, network)
    add_user_demands(model)
    # the RWZIs of Rijkswaterstaat and those that discharge into another model, see #860 and #871
    attach_rwzi_junctions(model, authorities={"Rijkswaterstaat"})
    change_routes(model)
    # series measured on links between models can only be observed in the coupled model
    link_observations(model)
    tidy_coupling_layout(model, link_ids)
    coupled_toml_file = save_model_and_outputs(model, all_link_table, toml_file)
    run_configured_coupling_level_check(coupled_toml_file)


# %% Process lhm_parts model

# couple LHM
if couple_lhm:
    toml_file = data_dir / "Rijkswaterstaat/modellen/lhm_parts/lhm.toml"
    couple_model(toml_file)

if sub_models:
    # couple sub-models if any
    sub_models_dir = data_dir / "Rijkswaterstaat/modellen/lhm_sub_models"
    if isinstance(sub_models, bool):
        selected_model_dirs = [p for p in sub_models_dir.iterdir() if p.is_dir() and not p.name.endswith("coupled")]
    else:
        selected_model_dirs = [sub_models_dir / model_name for model_name in sub_models]

    for model_dir in selected_model_dirs:
        if not model_dir.exists() or not model_dir.is_dir():
            print(f"Submodel directory does not exist, skipping: {model_dir}")
            continue
        try:
            toml_file = find_toml(model_dir)
        except ValueError:
            print(f"Not exactly one TOML in {model_dir}, skipping.")
            continue
        couple_model(toml_file)

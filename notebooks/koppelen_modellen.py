# %%

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd
from networkx import NetworkXNoPath
from ribasim_nl.aquo import waterbeheercode
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
# Configuration
data_dir = settings.ribasim_nl_data_dir
couple_lhm: bool = False
sub_models: list[str] | bool = [
    # "AAM-Limburg-RWS",
    "RDO-Noord"
]

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
    6002788,  # reparatie Helenavaart Limburg
    6002788,  # reparatie Helenavaart Limburg
    3800052,  # forceren koppeling kanaal van Deurne
    3800447,  # forceren koppeling kanaal van Deurne
    3803725,  # forceren koppeling kanaal van Deurne
]

# Pumps with min_upstream_level below upstream basin bottom; reset to NA
reset_pump_min_upstream_level = [
    5900602,  # min_upstream_level (2.61) below upstream Basin #4402052 bottom (3.3)
]

# Outlets with min_upstream_level below upstream basin bottom; reset to NA
reset_outlet_min_upstream_level = [
    3300727,  # min_upstream_level (14.76) below upstream Basin #4401600 bottom (16.3)
]

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
    1301495: 1404817,  # LevelBoundary buiten de boezem van HDSR geplaatst
    1301492: 1404817,  # LevelBoundary buiten de boezem van HDSR geplaatst
    4400015: 5903236,  # LB ligt op exact dezelfde locatie
    4402348: 5900072,  # LB ligt op exact dezelfde locatie
    6000124: 6002492,  # Reparatie Helenavaart
    6000125: 3801280,  # Forceren Kanaal van Deurne
    3800053: 3801280,  # Forceren Kanaal van Deurne
    2700009: 3803096,  # Vuchterstuw naar Drongelens Kanaal
    3800048: 3801394,  # Aansluiten Drongelens kanaal op Binnenstad
    3300009: 4401377,  # Levelboundary takt niet aan op Verlengde Hoogeveense Vaart
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


def get_basin_link(
    model: Model,
    basin_id: int,
    boundary_geometry: Point,
    reversed: bool = True,
) -> LineString:
    """Get a link-geometry from a basin to another point, possibly reversed."""
    basin_polygon = model.basin.area[basin_id].geometry
    links = model.link.df[(model.link.df.from_node_id == basin_id) | (model.link.df.to_node_id == basin_id)]
    links = links[links.geometry.intersects(basin_polygon)]
    # TODO: Complete implementation
    return LineString()


def initialize_models(toml_file: Path) -> tuple[Model, Network, pd.DataFrame]:
    """Initialize and load the model and network data.

    Parameters
    ----------
    toml_file : Path
        Path to the TOML file

    Returns
    -------
    tuple[Model, Network, pd.DataFrame]
        Tuple of (model, network, basin_areas_df)
    """
    # Load the model
    model = Model.read(toml_file)

    # Remove nodes if exists
    node_ids = model.node.df.index.to_numpy()
    for i in remove_nodes:
        if i in node_ids:
            replace_listen_node_id(model, i, None)
            model.remove_node(node_id=i, remove_links=True)

    # Load the network
    network_gpkg = data_dir / "Rijkswaterstaat/verwerkt/netwerk.gpkg"
    network = Network.from_network_gpkg(network_gpkg)

    # Prepare basin areas dataframe
    basin_areas_df = model.basin.area.df.set_index("node_id")
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
    if couple_authority == "Rijkswaterstaat":
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
        # For non-RWS authorities, use simple line geometry
        geometry = LineString([kwargs["from_node"].geometry, kwargs["to_node"].geometry])

    return geometry


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


def process_boundary_nodes(model: Model, network: Network, basin_areas_df: pd.DataFrame) -> list[dict]:
    """Process all boundary nodes to create coupling links.

    Parameters
    ----------
    model : Model
        Model to process
    network : Network
        Network for geometry creation
    basin_areas_df : pd.DataFrame
        Basin areas dataframe

    Returns
    -------
    list[dict]
        List of link table entries
    """
    unique_waterbeheerders = model.basin.node.df.meta_waterbeheerder.unique()
    boundary_node_ids = model.level_boundary.node.df[
        model.level_boundary.node.df.meta_couple_authority.isin(unique_waterbeheerders)
    ].index.to_list()

    all_link_table = []

    for boundary_node_id in tqdm(boundary_node_ids, desc="Coupling boundary nodes"):
        # Check whether the boundary has been merged already
        if boundary_node_id not in model.level_boundary.node.df.index:
            continue

        boundary_node = model.level_boundary[boundary_node_id]
        boundary_node_authority = model.level_boundary.node.df.at[boundary_node_id, "meta_waterbeheerder"]
        couple_authority = model.level_boundary.node.df.at[boundary_node_id, "meta_couple_authority"]

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
            couple_with_basin_id = distances.idxmin()

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
            cycles = model.link.df[
                (model.link.df.from_node_id == kwargs["to_node"].node_id)
                & (model.link.df.to_node_id == kwargs["from_node"].node_id)
            ]
            if len(cycles) > 0:
                print(f"Link {kwargs['from_node']} -> {kwargs['to_node']} already exists.")
                model.link.df.drop(cycles.index, inplace=True)
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
    for pump_id in reset_pump_min_upstream_level:
        mask = model.pump.static.df.node_id == pump_id
        if mask.any():
            model.pump.static.df.loc[mask, "min_upstream_level"] = pd.NA

    for outlet_id in reset_outlet_min_upstream_level:
        mask = model.outlet.static.df.node_id == outlet_id
        if mask.any():
            model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA

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
            if len(basin.level) == 0 or pd.isna(min_level):
                continue
            if min_level < basin.level.iloc[0]:
                print(f"Lowering basin {upstream_basin_id} profile {min_level}.")
                model.basin.profile.df.loc[(mask[mask]).index[0], "level"] = min_level - MIN_BASIN_OUTLET_DIFF


def remove_invalid_topology_nodes(model: Model) -> None:
    """Remove nodes with invalid flow/control topology and clean up their control references."""
    for link_type in ["flow", "control"]:
        invalid_topology = model.invalid_topology_at_node(link_type=link_type)
        while not invalid_topology.empty:
            for node_id, row in invalid_topology.iterrows():
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


def save_model_and_outputs(model: Model, all_link_table: list[dict], toml_file: Path) -> None:
    """Save the model and create output files.

    Args:
        model: Model to save
        all_link_table: Link table data
        toml_file: Path to the input/decoupled TOML file
    """
    # Derive model path from input toml_file, adding -coupled to folder and file name
    root = toml_file.parents[1]
    decoupled_model_name = toml_file.stem
    model_name = f"{decoupled_model_name}_coupled"
    model_path = root / model_name
    output_toml_file = model_path / f"{model_name}.toml"
    model.write(output_toml_file)

    # Save links
    links = gpd.GeoDataFrame(all_link_table)
    links["from_node_id"] = links.from_node.apply(lambda x: x.node_id)
    links["to_node_id"] = links.to_node.apply(lambda x: x.node_id)
    links = links.drop(columns=["from_node", "to_node"])
    links.to_file(model_path / "link.gpkg")


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
            link_geom = network.links.at[idx, "geometry"]
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
    controlled_nodes = model.link.df[model.link.df["link_type"] == "control"].to_node_id.values
    if (from_node_ids in controlled_nodes) != (to_node_ids in controlled_nodes):
        print(f"Cannot merge {boundary_node} => {neighbor_node}: One of the two is controlled, the other is not")
        return

    # Update listen references for removed boundary nodes
    upstream_basin_id = model.upstream_node_id(from_node_ids)
    if isinstance(upstream_basin_id, pd.Series):
        upstream_basin_id = upstream_basin_id.iloc[0]
    downstream_basin_id = model.downstream_node_id(to_node_ids)
    if isinstance(downstream_basin_id, pd.Series):
        downstream_basin_id = downstream_basin_id.iloc[0]
    replacement_basin = upstream_basin_id if upstream_basin_id is not None else downstream_basin_id
    replace_listen_node_id(model, boundary_node_id, replacement_basin)
    replace_listen_node_id(model, neighbor_id, replacement_basin)

    # Remove boundary node from model
    model.remove_node(boundary_node_id, remove_links=True)
    model.remove_node(neighbor_id, remove_links=True)

    # And merge the outlets
    merged_outlet = model.merge_outlets(from_node_ids, to_node_ids)
    return merged_outlet


def find_toml_path(model_dir: Path) -> Path:
    tomls = list(model_dir.glob("*.toml"))
    if len(tomls) == 0:
        raise ValueError(f"No TOML file found at: {model_dir}")
    elif len(tomls) > 1:
        raise ValueError(f"User provided more than one toml-file: {len(tomls)}, remove one! {tomls}")
    return tomls[0]


# %% Process lhm_parts model

# couple LHM
if couple_lhm:
    toml_file = data_dir / "Rijkswaterstaat/modellen/lhm_parts/lhm.toml"
    model, network, basin_areas_df = initialize_models(toml_file)
    all_link_table = process_boundary_nodes(model, network, basin_areas_df)
    fix_basin_profiles(model)
    remove_invalid_topology_nodes(model)
    save_model_and_outputs(model, all_link_table, toml_file)

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
            toml_file = find_toml_path(model_dir)
        except ValueError:
            print(f"Not exactly one TOML in {model_dir}, skipping.")
            continue
        model, network, basin_areas_df = initialize_models(toml_file)
        all_link_table = process_boundary_nodes(model, network, basin_areas_df)
        fix_basin_profiles(model)
        remove_invalid_topology_nodes(model)
        save_model_and_outputs(model, all_link_table, toml_file)

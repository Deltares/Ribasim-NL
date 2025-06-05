# %%

import pandas as pd
from networkx import NetworkXNoPath
from shapely.geometry import LineString, Point

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model, Network

cloud = CloudStorage()


# %% update RWS-HWS

SNAP_DISTANCE = 20


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


def merge_lb(model, lb_neighbors, boundary_node_id):
    neighbor_id = lb_neighbors.index[0]
    neighbor_node = model.level_boundary[neighbor_id]
    print(f"Linking {boundary_node} => {neighbor_node}.")
    from_node_ids = model.upstream_node_id(boundary_node_id)
    to_node_ids = model.downstream_node_id(boundary_node_id)
    # Inlet
    if from_node_ids is None and to_node_ids is not None:
        from_node_ids = model.upstream_node_id(neighbor_id)
        if from_node_ids is None:
            print(f"Cannot link {boundary_node} => {neighbor_node}: Wrong direction")
            return
    # Outlet
    elif to_node_ids is None and from_node_ids is not None:
        to_node_ids = model.downstream_node_id(neighbor_id)
        if to_node_ids is None:
            print(f"Cannot link {neighbor_node} => {boundary_node}: Wrong direction")
            return

    if isinstance(from_node_ids, pd.Series) or isinstance(to_node_ids, pd.Series):
        print(f"Cannot link {neighbor_node} => {boundary_node}: Multiple inlets/outlets, please check manually.")
        return

    # TODO Handle Pump/Pump and Outlet/Pump
    if model.get_node_type(from_node_ids) != "Outlet" or model.get_node_type(to_node_ids) != "Outlet":
        print(
            f"Cannot link {boundary_node} => {neighbor_node}: Expected Outlet, got {model.get_node_type(from_node_ids)} and {model.get_node_type(to_node_ids)}"
        )
        return

    # Remove boundary node from model
    model.remove_node(boundary_node_id, remove_edges=True)
    model.remove_node(neighbor_id, remove_edges=True)

    # And merge the outlets
    merged_outlet = model.merge_outlets(from_node_ids, to_node_ids)
    return merged_outlet


toml_file = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "lhm.toml")
model = Model.read(toml_file)

network_gpkg = cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg")
cloud.synchronize([network_gpkg])
network = Network.from_network_gpkg(network_gpkg)

unique_waterbeheerders = model.basin.node.df.meta_waterbeheerder.unique()

boundary_node_ids = model.level_boundary.node.df[
    model.level_boundary.node.df.meta_couple_authority.isin(unique_waterbeheerders)
].index.to_list()

# basin areas indexed met de node_id
basin_areas_df = model.basin.area.df.set_index("node_id")

# mask per waterbeheerder
waterbeheerder_df = model.basin.node.df["meta_waterbeheerder"]
basin_areas_df.loc[waterbeheerder_df.index, "meta_waterbeheerder"] = waterbeheerder_df
# waterbeheerder_mask_df = basin_areas_df.dissolve("meta_waterbeheerder")["geometry"]

for boundary_node_id in boundary_node_ids:
    # Check whether the boundary has been merged already
    if boundary_node_id not in model.level_boundary.node.df.index:
        continue

    boundary_node = model.level_boundary[boundary_node_id]
    boundary_node_authority = model.level_boundary.node.df.at[boundary_node_id, "meta_waterbeheerder"]
    couple_authority = model.level_boundary.node.df.at[boundary_node_id, "meta_couple_authority"]

    # Some levelboundaries don't need to be coupled
    if pd.isna(couple_authority):
        continue

    if couple_authority == boundary_node_authority:
        print(f"Boundary node {boundary_node} is already coupled with {couple_authority}.")
        continue

    # Check whether there are very close LB from the other authority
    lb_neighbors = model.level_boundary.node.df[model.level_boundary.node.df.meta_waterbeheerder == couple_authority]
    distances = lb_neighbors.distance(boundary_node.geometry)
    lb_neighbors = lb_neighbors[distances < SNAP_DISTANCE]

    if len(lb_neighbors) > 1:
        print("Multiple close LB found, please check manually.")
        continue

    if len(lb_neighbors) == 1:
        merged_outlet = merge_lb(model, lb_neighbors, boundary_node_id)
        if merged_outlet is not None:
            continue

    distances = basin_areas_df[basin_areas_df.meta_waterbeheerder == couple_authority].distance(boundary_node.geometry)

    # Can happen if we don't couple all models
    if len(distances) == 0:
        print(f"Cannot find {couple_authority} basin area for {boundary_node}.")
        continue

    # Couple with closest basin area
    couple_with_basin_id = distances.idxmin()

    # het aanmaken van een model.link.add tabel, nog zonder geometry
    link_table = []

    # Upstream nodes (uitlaat)
    from_node_ids = model.upstream_node_id(boundary_node_id)
    if from_node_ids is not None:
        if not isinstance(from_node_ids, pd.Series):
            from_node_ids = [from_node_ids]

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
            to_node_ids = [to_node_ids]
        link_table += [
            {
                "from_node": model.get_node(couple_with_basin_id),
                "to_node": model.get_node(i),
                "meta_from_authority": couple_authority,
                "meta_to_authority": boundary_node_authority,
            }
            for i in to_node_ids
        ]

    # Replace boundary id in discrete and continuous control with basin id
    for df in [model.discrete_control.variable.df, model.continuous_control.variable.df]:
        if df is not None:
            df.loc[df.listen_node_id == boundary_node_id, "listen_node_id"] = couple_with_basin_id
            df.loc[df.listen_node_id == boundary_node_id, "listen_node_id"] = couple_with_basin_id

    # Remove boundary node from model
    model.remove_node(boundary_node_id, remove_edges=True)

    # toevoegen edges, als het kan met een mooie geometrie
    for kwargs in link_table:
        # Use the geometry of the network to create a link
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
        # Otherwise just use the geometry of the nodes
        else:
            # if kwargs["meta_to_authority"] != boundary_node_authority:  # uitlaat
            #     geometry = get_basin_link(
            #         model,
            #         couple_with_basin_id,
            #         boundary_node.geometry,
            #         reversed=False,
            #     )
            # else:  # inlaat
            #     geometry = get_basin_link(
            #         model,
            #         couple_with_basin_id,
            #         boundary_node.geometry,
            #         reversed=True,
            #     )
            geometry = LineString([kwargs["from_node"].geometry, kwargs["to_node"].geometry])

        model.link.add(**kwargs, geometry=geometry)

# %%

model_path = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm_coupled")
toml_file = model_path / "lhm.toml"
model.write(toml_file)

# %%
# Let's save the peil validation output
# so we can check they're similar to the individual models
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_qlr", "output_controle_202502.qlr")
# cloud.synchronize([qlr_path])

controle_output = Control(ribasim_toml=toml_file, qlr_path=qlr_path)
indicators = controle_output.run_all()
# %%

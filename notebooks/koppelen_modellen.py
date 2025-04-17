# %%

import pandas as pd
from networkx import NetworkXNoPath
from shapely.geometry import LineString, Point

from ribasim_nl import CloudStorage, Model, Network

cloud = CloudStorage()

# %% update RWS-HWS


def get_link(
    network: Network,
    on_network_point: Point,
    to_be_projected_point: Point,
    reversed: bool = True,
) -> LineString:
    """Get a link-geometry from a network node to another point, possibly reversed.

    Args:
        network (Network): network to get geometry from
        on_network_point (Point): point to take closest network-node from
        to_be_projected_point (Point): point to project to closest network link
        reversed (bool, optional): If True LineString vertices are ordered from on_network_point to to_be_projected_point
        If false LineString will be reversed. Defaults to True.
    """
    # from_network_node is closest node in network
    to_network_node = network.nodes.distance(on_network_point).idxmin()

    # now get closest link
    link_idx = iter(network.links.distance(to_be_projected_point).sort_values().index)
    link_geometry = None
    while link_geometry is None:
        idx = next(link_idx)
        try:
            link_geom = network.links.at[idx, "geometry"]
            projected_point = link_geom.interpolate(link_geom.project(to_be_projected_point))
            if network.nodes.distance(projected_point).min() > 10:
                from_network_node = network.add_node(projected_point, max_distance=9)
            else:
                from_network_node = network.nodes.distance(projected_point).idxmin()
            link_geometry = network.get_line(from_network_node, to_network_node)
        except NetworkXNoPath:
            continue

    # if link_geometry boundary do not match on_network_point or
    if not link_geometry.boundary.geoms[0].equals(to_be_projected_point):
        link_geometry = LineString(tuple(to_be_projected_point.coords) + tuple(link_geometry.coords))
    if not link_geometry.boundary.geoms[1].equals(on_network_point):
        link_geometry = LineString(tuple(link_geometry.coords) + tuple(on_network_point.coords))

    if reversed:
        return link_geometry.reverse()
    else:
        return link_geometry


toml_file = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "lhm.toml")
model = Model.read(toml_file)

network_gpkg = cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg")

cloud.synchronize([network_gpkg])

network = Network.from_network_gpkg(network_gpkg)


boundary_node_ids = model.level_boundary.static.df[
    model.level_boundary.static.df.meta_couple_authority.isin(model.basin.node.df.meta_waterbeheerder.unique())
].node_id.to_list()


# basin areas indexed met de node_id
basin_areas_df = model.basin.area.df.set_index("node_id")

# mask per waterbeheerder
waterbeheerder_df = model.basin.node.df["meta_waterbeheerder"]
basin_areas_df.loc[waterbeheerder_df.index, "meta_waterbeheerder"] = waterbeheerder_df
waterbeheerder_mask_df = basin_areas_df.dissolve("meta_waterbeheerder")["geometry"]


for boundary_node_id in boundary_node_ids:
    boundary_node = model.level_boundary[boundary_node_id]
    boundary_node_authority = model.level_boundary.node.df.at[boundary_node_id, "meta_waterbeheerder"]
    couple_authority = model.level_boundary.static.df.set_index("node_id").at[boundary_node_id, "meta_couple_authority"]
    couple_with_basin_id = (
        basin_areas_df[basin_areas_df.meta_waterbeheerder == couple_authority].distance(boundary_node.geometry).idxmin()
    )
    couple_with_basin = model.basin[couple_with_basin_id]

    # het aanmaken van een model.link.add tabel, nog zonder geometry
    link_table = []
    from_node_ids = model.upstream_node_id(boundary_node_id)
    if from_node_ids is not None:
        if not isinstance(from_node_ids, pd.Series):
            link_table += [
                {
                    "from_node": model.get_node(from_node_ids),
                    "to_node": couple_with_basin,
                    "meta_from_authority": boundary_node_authority,
                    "meta_to_authority": couple_authority,
                }
            ]
        else:
            link_table += [
                {
                    "from_node": model.get_node(i),
                    "to_node": couple_with_basin,
                    "meta_from_authority": boundary_node_authority,
                    "meta_to_authority": couple_authority,
                }
                for i in from_node_ids
            ]

    to_node_ids = model.downstream_node_id(boundary_node_id)
    if to_node_ids is not None:
        if not isinstance(to_node_ids, pd.Series):
            link_table += [
                {
                    "from_node": couple_with_basin,
                    "to_node": model.get_node(to_node_ids),
                    "meta_from_authority": couple_authority,
                    "meta_to_authority": boundary_node_authority,
                }
            ]
        else:
            link_table += [
                {
                    "from_node": couple_with_basin,
                    "to_node": model.get_node(i),
                    "meta_from_authority": couple_authority,
                    "meta_to_authority": boundary_node_authority,
                }
                for i in from_node_ids
            ]

    # fixen van listen node ids
    mask = model.discrete_control.variable.df.listen_node_id == boundary_node_id
    for df in [model.discrete_control.variable.df, model.continuous_control.variable.df]:
        if df is not None:
            df.loc[mask, ["listen_node_id"]] = couple_with_basin_id
            df.loc[mask, ["listen_node_id"]] = couple_with_basin_id

    # verwijderen boundary node met edges
    model.remove_node(boundary_node_id, remove_edges=True)

    # toevoegen edges, als het kan met een mooie geometrie
    for kwargs in link_table:
        if kwargs["meta_to_authority"] != boundary_node_authority:  # uitlaat
            geometry = get_link(
                network=network,
                on_network_point=kwargs["to_node"].geometry,
                to_be_projected_point=kwargs["from_node"].geometry,
                reversed=False,
            )
        else:  # inlaat
            geometry = get_link(
                network=network,
                on_network_point=kwargs["from_node"].geometry,
                to_be_projected_point=kwargs["to_node"].geometry,
                reversed=True,
            )

        model.link.add(**kwargs, geometry=geometry)

# %%

model_path = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm_coupled")
toml_file = model_path / "lhm.toml"
model.write(toml_file)

# %%

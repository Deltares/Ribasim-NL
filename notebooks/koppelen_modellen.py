# %%

import pandas as pd

from ribasim_nl import CloudStorage, Model, Network

cloud = CloudStorage()

# %% update RWS-HWS


toml_file = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "lhm.toml")
model = Model.read(toml_file)

network = Network.from_network_gpkg(cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg"))


boundary_node_ids = model.level_boundary.static.df[
    (model.level_boundary.static.df.meta_to_authority == "Rijkswaterstaat")
    | (model.level_boundary.static.df.meta_from_authority == "Rijkswaterstaat")
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

    # FIXME: from_authority en to_authority lijken niet handig, omdat een boundary zowel kan koppelen naar een inlaat als uitlaat.
    # Ik stel voor dat we couple_authority gaan gebruiken waarbij een LevelBoundary i.i.g. maar 1 waterlichaam (van 1 authority) mag representeren
    # Fix het nu even zo
    from_authority, to_authority = model.level_boundary.static.df.set_index("node_id").loc[
        boundary_node_id, ["meta_from_authority", "meta_to_authority"]
    ]
    couple_authority = next(i for i in (from_authority, to_authority) if i != boundary_node_authority)
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
                    "to_node": to_node_ids,
                    "meta_from_authority": couple_authority,
                    "meta_to_authority": boundary_node_authority,
                }
            ]
        else:
            link_table += [
                {
                    "from_node": couple_with_basin,
                    "to_node": i,
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
        model.link.add(**kwargs)
        # # get netwerk_mask_poly
        # netwerk_mask_poly = waterbeheerder_mask_df[to_authority]

        # # get basin-id to couple to
        # to_node_id = (
        #     basin_areas_df[basin_areas_df.meta_waterbeheerder == to_authority].distance(boundary_node.geometry).idxmin()
        # )
        # to_node = model.basin[to_node_id]
        # listen_node_id = to_node_id

        # # get to network node
        # to_network_node = network.nodes.distance(to_node.geometry).idxmin()

        # # get node to couple from
        # from_node_id = model.upstream_node_id(boundary_node_id)
        # from_node_type = model.node_table().df.at[from_node_id, "node_type"]
        # from_node = getattr(model, pascal_to_snake_case(from_node_type))[from_node_id]

        # # get from network node
        # link_idx = iter(network.links.distance(from_node.geometry).sort_values().index)
        # link_geometry = None
        # while link_geometry is None:
        #     idx = next(link_idx)
        #     try:
        #         link_geom = network.links.at[idx, "geometry"]
        #         if link_geom.intersects(netwerk_mask_poly):
        #             continue
        #         projected_point = link_geom.interpolate(link_geom.project(from_node.geometry))
        #         if network.nodes.distance(projected_point).min() > 10:
        #             from_network_node = network.add_node(projected_point, max_distance=9)
        #         else:
        #             from_network_node = network.nodes.distance(projected_point).idxmin()
        #         link_geometry = network.get_line(from_network_node, to_network_node)
        #     except NetworkXNoPath:
        #         continue

        # # finish link-geometry
        # if link_geometry.boundary.geoms[0].distance(from_node.geometry) > 0.001:
        #     link_geometry = LineString(tuple(from_node.geometry.coords) + tuple(link_geometry.coords))
        # if link_geometry.boundary.geoms[1].distance(to_node.geometry) > 0.001:
        #     link_geometry = LineString(tuple(link_geometry.coords) + tuple(to_node.geometry.coords))

        # model.link.add(geometry=link_geometry, **kwargs)

    # except KeyError:
    #     # get basin-id to couple from
    #     from_node_id = basin_areas_df.distance(boundary_node.geometry).idxmin()
    #     from_node = model.basin[from_node_id]
    #     listen_node_id = from_node_id

    #     # get from network node
    #     from_network_node = network.nodes.distance(from_node.geometry).idxmin()

    #     # get node to couple to
    #     to_node_id = model.downstream_node_id(boundary_node_id)
    #     to_node_type = model.node_table().df.at[to_node_id, "node_type"]
    #     to_node = getattr(model, pascal_to_snake_case(to_node_type))[to_node_id]

    #     # get edge geometry
    #     link_idx = iter(network.links.distance(to_node.geometry).sort_values().index)
    #     edge_geometry = None
    #     while edge_geometry is None:
    #         idx = next(link_idx)
    #         try:
    #             link_geom = network.links.at[idx, "geometry"]
    #             if link_geom.intersects(netwerk_mask_poly):
    #                 continue
    #             projected_point = link_geom.interpolate(link_geom.project(to_node.geometry))
    #             if network.nodes.distance(projected_point).min() > 10:
    #                 to_network_node = network.add_node(projected_point, max_distance=9)
    #             else:
    #                 to_network_node = network.nodes.distance(projected_point).idxmin()
    #             edge_geometry = network.get_line(from_network_node, to_network_node)
    #         except NetworkXNoPath:
    #             continue

    # remove boundary node

    # update discrete control
    # mask = model.discrete_control.variable.df.listen_node_id == boundary_node_id
    # model.discrete_control.variable.df.loc[mask, ["listen_node_id"]] = listen_node_id

    # # add edge
    # link_id = model.link.df.index.max() + 1


# %%

model_path = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm_coupled")
toml_file = model_path / "lhm.toml"

model.write(toml_file)

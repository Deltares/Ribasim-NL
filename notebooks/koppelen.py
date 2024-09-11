# %%

import sqlite3

import pandas as pd
from networkx import NetworkXNoPath
from ribasim_nl import CloudStorage, Model, Network, reset_index
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.concat import concat
from shapely.geometry import LineString

cloud = CloudStorage()


def update_database(toml_file):
    database_gpkg = toml_file.with_name("database.gpkg")
    conn = sqlite3.connect(database_gpkg)

    # get table into DataFrame
    table = "Outlet / static"
    df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)

    # drop urban runoff column if exists
    df.rename(columns={"min_crest_level": "min_upstream_level"}, inplace=True)

    #  Write the DataFrame back to the SQLite table
    df.to_sql(table, conn, if_exists="replace", index=False)

    # # Close the connection
    conn.close()


# %% update RWS-HWS
model_path = cloud.joinpath("Rijkswaterstaat", "modellen", "hws")
toml_file = model_path / "hws.toml"
update_database(toml_file)
rws_model = Model.read(toml_file)

# some fixes
node_id = 8413
level = rws_model.upstream_profile(node_id).level.min() + 0.1

mask = (rws_model.tabulated_rating_curve.static.df.node_id == node_id) & (
    rws_model.tabulated_rating_curve.static.df.level < level
)
rws_model.tabulated_rating_curve.static.df.loc[mask, ["level"]] = level

# reset index
rws_model = reset_index(rws_model)

# # write model
rws_model.update_meta_properties(node_properties={"authority": "Rijkswaterstaat"})
rws_model.write(model_path.with_name("hws_temp") / "hws.toml")


# %% update AGV
model_path = cloud.joinpath("AmstelGooienVecht", "modellen", "AmstelGooienVecht_parametrized_2024_8_47")
if not model_path.exists():
    model_url = cloud.joinurl("AmstelGooienVecht", "modellen", "AmstelGooienVecht_parametrized_2024_8_47")
    cloud.download_content(model_url)
toml_file = model_path / "ribasim.toml"
update_database(toml_file)
model_to_couple = Model.read(toml_file)

# fix manning issue
model_to_couple.manning_resistance.static.df = model_to_couple.manning_resistance.static.df[
    model_to_couple.manning_resistance.static.df.node_id.isin(model_to_couple.node_table().df.index)
]

# fix boundary-node issue
model_to_couple.remove_node(957, remove_edges=False)

# reset index
model_to_couple = reset_index(model_to_couple, node_start=rws_model.next_node_id)

# # write model
model_to_couple.update_meta_properties(node_properties={"authority": "AmstelGooienVecht"})
model_to_couple.write(model_path.with_name("AmstelGooienVecht_temp") / "agv.toml")

# %% load network
netwerk_mask_poly = model_to_couple.basin.area.df.union_all()

models = [rws_model, model_to_couple]
coupled_model = concat(models)

network = Network.from_network_gpkg(cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg"))

# %%
boundary_node_ids = coupled_model.level_boundary.static.df[
    (coupled_model.level_boundary.static.df.meta_to_authority == "Rijkswaterstaat")
    | (coupled_model.level_boundary.static.df.meta_from_authority == "Rijkswaterstaat")
].node_id.to_list()
basin_ids = (
    coupled_model.node_table().df[coupled_model.node_table().df.meta_authority == "Rijkswaterstaat"].index.to_list()
)
basin_areas_df = coupled_model.basin.area.df[coupled_model.basin.area.df.node_id.isin(basin_ids)].set_index("node_id")

for boundary_node_id in boundary_node_ids:
    # boundary_node_id = boundary_node_ids[0]
    boundary_node = coupled_model.level_boundary[boundary_node_id]
    # get upstream node to couple from
    try:
        # get basin-id to couple to
        to_node_id = basin_areas_df.distance(boundary_node.geometry).idxmin()
        to_node = coupled_model.basin[to_node_id]
        listen_node_id = to_node_id

        # get to network node
        to_network_node = network.nodes.distance(to_node.geometry).idxmin()

        # get node to couple from
        from_node_id = coupled_model.upstream_node_id(boundary_node_id)
        from_node_type = coupled_model.node_table().df.at[from_node_id, "node_type"]
        from_node = getattr(coupled_model, pascal_to_snake_case(from_node_type))[from_node_id]

        # get from network node
        link_idx = iter(network.links.distance(from_node.geometry).sort_values().index)
        edge_geometry = None
        while edge_geometry is None:
            idx = next(link_idx)
            try:
                link_geom = network.links.at[idx, "geometry"]
                if link_geom.intersects(netwerk_mask_poly):
                    continue
                projected_point = link_geom.interpolate(link_geom.project(from_node.geometry))
                if network.nodes.distance(projected_point).min() > 10:
                    from_network_node = network.add_node(projected_point, max_distance=9)
                else:
                    from_network_node = network.nodes.distance(projected_point).idxmin()
                edge_geometry = network.get_line(from_network_node, to_network_node)
            except NetworkXNoPath:
                continue

    except KeyError:
        # get basin-id to couple from
        from_node_id = basin_areas_df.distance(boundary_node.geometry).idxmin()
        from_node = coupled_model.basin[from_node_id]
        listen_node_id = from_node_id

        # get from network node
        from_network_node = network.nodes.distance(from_node.geometry).idxmin()

        # get node to couple to
        to_node_id = coupled_model.downstream_node_id(boundary_node_id)
        to_node_type = coupled_model.node_table().df.at[to_node_id, "node_type"]
        to_node = getattr(coupled_model, pascal_to_snake_case(to_node_type))[to_node_id]

        # get edge geometry
        link_idx = iter(network.links.distance(to_node.geometry).sort_values().index)
        edge_geometry = None
        while edge_geometry is None:
            idx = next(link_idx)
            try:
                link_geom = network.links.at[idx, "geometry"]
                if link_geom.intersects(netwerk_mask_poly):
                    continue
                projected_point = link_geom.interpolate(link_geom.project(to_node.geometry))
                if network.nodes.distance(projected_point).min() > 10:
                    to_network_node = network.add_node(projected_point, max_distance=9)
                else:
                    to_network_node = network.nodes.distance(projected_point).idxmin()
                edge_geometry = network.get_line(from_network_node, to_network_node)
            except NetworkXNoPath:
                continue

    # remove boundary node
    coupled_model.remove_node(boundary_node_id, remove_edges=True)

    # update discrete control
    mask = coupled_model.discrete_control.variable.df.listen_node_id == boundary_node_id
    coupled_model.discrete_control.variable.df.loc[mask, ["listen_node_id"]] = listen_node_id

    # construct edge-geometry
    if edge_geometry.boundary.geoms[0].distance(from_node.geometry) > 0.001:
        edge_geometry = LineString(tuple(from_node.geometry.coords) + tuple(edge_geometry.coords))
    if edge_geometry.boundary.geoms[1].distance(to_node.geometry) > 0.001:
        edge_geometry = LineString(tuple(edge_geometry.coords) + tuple(to_node.geometry.coords))

    # add edge
    edge_id = coupled_model.edge.df.index.max() + 1
    coupled_model.edge.add(
        edge_id=edge_id,
        from_node=from_node,
        to_node=to_node,
        geometry=edge_geometry,
        meta_from_authority="AmstelGooiEnVecht",
        meta_to_authority="Rijkswaterstaat",
    )

# # remove node and it's edge
# #


# %%

model_path = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm")
toml_file = model_path / "lhm.toml"

coupled_model.write(toml_file)

# %%

# %%
import geopandas as gpd

from ribasim_nl import CloudStorage, Model, Network

cloud = CloudStorage()


# %% load model
ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_fix_model_network", "model.toml")
model = Model.read(ribasim_toml)

# %% network from HydroObjects
network_gpkg = cloud.joinpath("DeDommel", "verwerkt", "network.gpkg")
if network_gpkg.exists():
    network = Network.from_network_gpkg(network_gpkg)
else:
    network = Network.from_lines_gpkg(
        cloud.joinpath("DeDommel", "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="hydroobject"
    )

    network.to_file(network_gpkg)
# %% edges follow HydroObjects
model.reset_edge_geometry()
node_df = model.node_table().df
data = []
for row in model.edge.df.itertuples():
    try:
        # get or add node_from
        from_point = node_df.at[row.from_node_id, "geometry"]
        distance = network.nodes.distance(from_point)
        if distance.min() < 0.1:
            node_from = distance.idxmin()
        else:
            node_from = network.add_node(from_point, max_distance=10, align_distance=1)

        # get or add node_to
        to_point = node_df.at[row.to_node_id, "geometry"]
        distance = network.nodes.distance(to_point)
        if distance.min() < 0.1:
            node_to = distance.idxmin()
        else:
            node_to = network.add_node(to_point, max_distance=10, align_distance=1)

        if (node_from is not None) and (node_to is not None):
            # get line geometry
            geometry = network.get_line(node_from, node_to)

            # replace edge geometry
            model.edge.df.loc[row.Index, ["geometry"]] = geometry
        else:
            print(f"edge not updated for {row.Index} as node_from and node_to cannot be found")
            data += [row]
    except:  # noqa: E722
        print("edge not updated due to Exception")
        data += [row]
        continue

gpd.GeoDataFrame(data, crs=28992).to_file("rare_edges.gpkg")

# %% write model
ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_fix_edges", "model.toml")
model.write(ribasim_toml)

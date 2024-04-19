# %%
import geopandas as gpd
import ribasim
from ribasim.nodes import user_demand
from ribasim_nl import CloudStorage, Network
from shapely.geometry import LineString

cloud = CloudStorage()

MAX_DISTANCE = 50
TOLERANCE = 0.1

ribasim_toml = cloud.joinpath(
    "Rijkswaterstaat", "modellen", "hws_sturing_upgraded", "hws.toml"
)
model = ribasim.Model.read(ribasim_toml)

hydamo = cloud.joinpath("Rijkswaterstaat", "verwerkt", "hydamo.gpkg")
kunstwerken_gdf = gpd.read_file(
    hydamo, layer="kunstwerken", driver="GPKG", engine="pyogrio"
)

network = Network.from_network_gpkg(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg")
)

# %% add basin_node_ids to network
basin_area_gdf = model.basin.area.df.copy()
basin_area_gdf.rename(columns={"node_id": "basin_id"}, inplace=True)
nodes_df = network.nodes
nodes_df = nodes_df.sjoin(basin_area_gdf, how="left")

# %%

nodes = []
lines = []
user_static = []
node_id = model.node_table().df.node_id.max() + 1

for row in kunstwerken_gdf[kunstwerken_gdf.soort == "inlaat"].itertuples():
    print(f"{row.naam}")
    # parameters setten
    if row.sector == "drinkwater":
        return_factor = 0
        demand = float(row.productie)
        priority = 1
    elif row.sector == "energie":
        return_factor = 0.9
        demand = float(row.capaciteit)
        priority = 2
    else:
        raise ValueError(f"specify sector for demand-node with index {row.Index}")

    # vinden basin
    distance_to_model_df = model.basin.area.df.distance(row.geometry).sort_values()
    if distance_to_model_df.min() > MAX_DISTANCE:
        print(
            f"drinkwaterinlaat {row.naam} te ver van model-basin {distance_to_model_df.min()}"
        )
    else:
        basin_id = model.basin.area.df.at[distance_to_model_df.index[0], "node_id"]
        basin_node = model.basin[basin_id]

        # ophalen van de line
        network_from_id = network.move_node(
            basin_node.geometry,
            max_distance=MAX_DISTANCE,
            align_distance=100,
            node_types=["connection", "upstream_boundary"],
        )

        network_to_id = (
            nodes_df[nodes_df.basin_id == basin_id]
            .distance(row.geometry)
            .sort_values()
            .index[0]
        )

        line = network.get_line(network_from_id, network_to_id)
        if not nodes_df.at[network_to_id, "geometry"].equals(row.geometry):
            line = LineString(list(line.coords) + list(row.geometry.coords))

        lines += [line]
        min_level = (
            model.basin.state.df.set_index("node_id").at[basin_id, "level"] + 0.1
        )
        nodes += [
            {
                "node_id": node_id,
                "name": row.naam,
                "node_type": "UserDemand",
                "meta_waterbeheerder": row.beheerder,
                "meta_sector": row.sector,
                "meta_soort": row.soort,
                "geometry": row.geometry,
            }
        ]

        user_static += [
            {
                "basin_id": basin_id,
                "demand": demand,
                "return_factor": return_factor,
                "min_level": min_level,
                "priority": priority,
                "meta_referentie": row.referentie,
            }
        ]

        node_id += 1

# %% toevoegen aan model
for line, node, static in zip(lines, nodes, user_static):
    kwargs = {k: [v] for k, v in static.items() if k != "basin_id"}
    model.user_demand.add(ribasim.Node(**node), [user_demand.Static(**kwargs)])

    model.edge.add(
        model.basin[static["basin_id"]],
        model.user_demand[node["node_id"]],
        geometry=line,
    )
    model.edge.add(
        model.user_demand[node["node_id"]],
        model.basin[static["basin_id"]],
        geometry=line.reverse(),
    )


# %% wegschrijven model
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_prefix", "hws.toml")
model.write(ribasim_toml)

# %%

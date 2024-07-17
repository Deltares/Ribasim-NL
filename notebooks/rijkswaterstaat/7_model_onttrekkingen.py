# %%
import geopandas as gpd
import ribasim
from ribasim import Node
from ribasim.nodes import user_demand
from ribasim_nl import CloudStorage, Model, Network
from shapely.geometry import LineString, Point


def add_demand(
    model: Model,
    network: Network,
    network_nodes_df: gpd.GeoDataFrame,
    demand: float,
    return_factor: float,
    priority: int,
    name: str,
    geometry: Point,
    min_level: float | None = None,
    inlet_geometry: Point | None = None,
    outlet_geometry: Point | None = None,
    max_distance: float = 50,
    **kwargs,
):
    if inlet_geometry is not None:  # demand from basin at inlet
        inlet_basin_node = model.find_closest_basin(
            geometry=inlet_geometry, max_distance=max_distance
        )
    else:  # demand from basin at demand_node
        inlet_basin_node = model.find_closest_basin(
            geometry=geometry, max_distance=max_distance
        )
        inlet_geometry = geometry

    if outlet_geometry is not None:  # return-flow to basin at outlet
        outlet_basin_node = model.find_closest_basin(
            geometry=outlet_geometry, max_distance=max_distance
        )
    else:
        outlet_basin_node = inlet_basin_node

    # define demand_node
    demand_node_id = model.node_table().df.node_id.max() + 1
    demand_node = Node(demand_node_id, geometry=geometry, name=name, **kwargs)
    if min_level is None:
        min_level = model.basin.profile[inlet_basin_node.node_id].level.min() + 0.1

    model.user_demand.add(
        demand_node,
        [
            user_demand.Static(
                priority=[priority],
                demand=[demand],
                return_factor=[return_factor],
                min_level=[min_level],
            )
        ],
    )

    # Find network_node_id at inlet_basin
    network_from_id = network.move_node(
        inlet_basin_node.geometry,
        max_distance=max_distance,
        align_distance=100,
        node_types=["connection", "upstream_boundary"],
    )

    # Find network_node at inlet
    network_to_id = (
        network_nodes_df[network_nodes_df.basin_id == inlet_basin_node.node_id]
        .distance(inlet_geometry)
        .idxmin()
    )

    # Edge_geometry from basin to inlet to demand_node
    line = network.get_line(network_from_id, network_to_id)

    # Extend to demand_node
    if not line.coords[-1] == inlet_geometry.coords:
        line = LineString(list(line.coords) + list(inlet_geometry.coords))
    if not line.coords[-1] == geometry.coords:
        line = LineString(list(line.coords) + list(geometry.coords))

    # Edge from inlet_basin to demand_node
    model.edge.add(
        inlet_basin_node,
        model.user_demand[demand_node_id],
        geometry=line,
    )

    if outlet_geometry is not None:
        # Find network_node at outlet
        # network_from_id = (
        #     network_nodes_df[network_nodes_df.basin_id == outlet_basin_node.node_id]
        #     .distance(outlet_geometry)
        #     .idxmin()
        # )
        network_from_id = network.nodes.distance(outlet_geometry).idxmin()

        # Find network_node_id at outlet_basin
        network_to_id = network.move_node(
            outlet_basin_node.geometry,
            max_distance=max_distance,
            align_distance=100,
            node_types=["connection", "upstream_boundary"],
        )

        # Edge_geometry from basin to inlet to demand_node
        line = network.get_line(network_from_id, network_to_id)

        # Extend to demand_node
        if not line.coords[0] == outlet_geometry.coords:
            line = LineString(list(inlet_geometry.coords) + list(line.coords))
        if not line.coords[0] == geometry.coords:
            line = LineString(list(geometry.coords) + list(line.coords))

    else:  # we just revert the line via inlet
        line = line.reverse()

    # Edge from demand_node to outlet_basin
    model.edge.add(
        model.user_demand[demand_node_id],
        outlet_basin_node,
        geometry=line,
    )


cloud = CloudStorage()

MAX_DISTANCE = 50
TOLERANCE = 0.1


ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_sturing", "hws.toml")
model = Model.read(ribasim_toml)

hydamo = cloud.joinpath("Rijkswaterstaat", "verwerkt", "hydamo.gpkg")
onttrekkingen_gpkg = cloud.joinpath("Onttrekkingen", "onttrekkingen.gpkg")

network = Network.from_network_gpkg(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg")
)

# %% add basin_node_ids to network
basin_area_gdf = model.basin.area.df.copy()
basin_area_gdf.rename(columns={"node_id": "basin_id"}, inplace=True)
network.overlay(basin_area_gdf)  # basin_id toekennen aan netwerk

# network_nodes_df = network.nodes
# network_nodes_df = network_nodes_df.sjoin(basin_area_gdf, how="left")


# %% Drinkwater
drinkwater_gdf = gpd.read_file(
    onttrekkingen_gpkg, layer_name="Drinkwater_oppervlaktewater", engine="pyogrio"
)
drinkwater_gdf.dropna(subset=["productie"], inplace=True)


priority = 1
return_factor = 0.0

for row in drinkwater_gdf.itertuples():
    add_demand(
        model=model,
        network=network,
        network_nodes_df=network_nodes_df,
        demand=float(row.productie),
        return_factor=return_factor,
        priority=priority,
        name=row.naam,
        geometry=row.geometry,
        meta_sector="drinkwater",
        meta_beheerder=row.beheerder,
    )

# %% Energie
energie_gdf = gpd.read_file(
    onttrekkingen_gpkg, layer="Energiecentrales", engine="pyogrio"
)

energie_inlet_gdf = gpd.read_file(
    onttrekkingen_gpkg, layer="Energiecentrales-inlaat", engine="pyogrio"
)

energie_outlet_gdf = gpd.read_file(
    onttrekkingen_gpkg, layer="Energiecentrales-uitlaat", engine="pyogrio"
)

priority = 2
return_factor = 0.95

row = next(energie_inlet_gdf.itertuples())

# inlet geometry and reference to osm_id, and demand
inlet_geometry = row.geometry
osm_id = row.osm_id_Energiecentrales
demand = float(row.capaciteit)

# from plant name and (demand-)geometry and
plant = energie_gdf.set_index("osm_id").loc[osm_id]
name = plant["name"]
beheerder = plant.operator
geometry = plant.geometry.centroid
outlets = energie_outlet_gdf[energie_outlet_gdf["osm_id_Energiecentrales"] == osm_id]
outlet_geometry = outlets.geometry.iloc[0]

add_demand(
    model=model,
    network=network,
    network_nodes_df=network_nodes_df,
    demand=demand,
    return_factor=return_factor,
    priority=priority,
    name=name,
    geometry=geometry,
    inlet_geometry=inlet_geometry,
    outlet_geometry=outlet_geometry,
    meta_sector="energie",
    meta_beheerder=beheerder,
    max_distance=600,
)

# %% wegschrijven model
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_demand", "hws.toml")
model.write(ribasim_toml)

# %%

# %%
from typing import cast

import geopandas as gpd
from ribasim import Node
from ribasim.nodes import user_demand
from ribasim_nl.junctions import junctionify
from ribasim_nl.split_basins import split_basin_along_route
from shapely.geometry import LineString, Point

from ribasim_nl import CloudStorage, Model, Network


def add_demand(
    model: Model,
    network: Network,
    demand: float,
    return_factor: float,
    priority: int,
    name: str,
    geometry: Point,
    min_level: float | None = None,
    inlet_geometry: Point | None = None,
    outlet_geometry: Point | None = None,
    outlet_as_terminal: bool = False,
    max_distance: float = 50,
    **kwargs,
):
    if inlet_geometry is not None:  # demand from basin at inlet
        inlet_basin_node = model.find_closest_basin(geometry=inlet_geometry, max_distance=max_distance)
    else:  # demand from basin at demand_node
        inlet_basin_node = model.find_closest_basin(geometry=geometry, max_distance=max_distance)
        inlet_geometry = geometry

    if (outlet_geometry is not None) and (not outlet_as_terminal):  # return-flow to basin at outlet
        outlet_basin_node = model.find_closest_basin(geometry=outlet_geometry, max_distance=max_distance)
    else:
        outlet_basin_node = inlet_basin_node

    # define demand_node
    demand_node_id = model.node.df.index.max() + 1
    demand_node = Node(demand_node_id, geometry=geometry, name=name, **kwargs)
    if min_level is None:
        min_level = model.basin.profile[inlet_basin_node.node_id].level.min() + 0.1

    model.user_demand.add(
        demand_node,
        [
            user_demand.Static(
                demand_priority=[priority],
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
    network_to_id = network.nodes[network.nodes.basin_id == inlet_basin_node.node_id].distance(inlet_geometry).idxmin()

    # Link_geometry from basin to inlet to demand_node
    line = network.get_line(network_from_id, network_to_id)

    # Extend to demand_node
    if line.coords[-1] != inlet_geometry.coords:
        line = LineString(list(line.coords) + list(inlet_geometry.coords))
    if line.coords[-1] != geometry.coords:
        line = LineString(list(line.coords) + list(geometry.coords))

    # Link from inlet_basin to demand_node
    model.link.add(inlet_basin_node, model.user_demand[demand_node_id], geometry=line, name=name)

    if outlet_as_terminal:
        terminal_node_id = model.node.df.index.max() + 1
        model.terminal.add(Node(terminal_node_id, outlet_geometry))
        terminal_node = model.terminal[terminal_node_id]

        # Link from demand_node to terminal
        model.link.add(model.user_demand[demand_node_id], terminal_node, name=name)

    elif outlet_geometry is not None:
        # Find network_node at outlet
        network_from_id = (
            network.nodes[network.nodes.basin_id == outlet_basin_node.node_id].distance(outlet_geometry).idxmin()
        )

        # Find network_node_id at outlet_basin
        network_to_id = network.move_node(
            outlet_basin_node.geometry,
            max_distance=max_distance,
            align_distance=100,
            node_types=["connection", "upstream_boundary"],
        )

        # Link_geometry from basin to inlet to demand_node
        line = network.get_line(network_from_id, network_to_id)

        # Extend to demand_node
        if line.coords[0] != outlet_geometry.coords:
            line = LineString(list(outlet_geometry.coords) + list(line.coords))
        if line.coords[0] != geometry.coords:
            line = LineString(list(geometry.coords) + list(line.coords))

        # Link from demand_node to outlet_basin
        model.link.add(
            model.user_demand[demand_node_id],
            outlet_basin_node,
            geometry=line,
            name=name,
        )

    else:  # we just revert the line via inlet
        line = line.reverse()

        # Link from demand_node to outlet_basin
        model.link.add(
            model.user_demand[demand_node_id],
            outlet_basin_node,
            geometry=line,
            name=name,
        )


cloud = CloudStorage()

MAX_DISTANCE = 50
TOLERANCE = 0.1


ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_sturing/hws.toml")
model = Model.read(ribasim_toml)

hydamo = cloud.joinpath("Rijkswaterstaat/verwerkt/hydamo.gpkg")
onttrekkingen_gpkg = cloud.joinpath("Basisgegevens/Onttrekkingen/onttrekkingen.gpkg")

network = Network.from_network_gpkg(cloud.joinpath("Rijkswaterstaat/verwerkt/netwerk.gpkg"))

# %% Split the Amsterdam-Rijnkanaal north of the Lek in three, see https://github.com/Deltares/Ribasim-NL/issues/916
# It runs about 59 km from the Prinses Irenesluis to ManningResistance 4695 at the Noordzeekanaal. It is split before
# the demands are added, so they and the couplings connect to the section they are at.
ARK, IRENESLUIS, ARK_END = 5018, 5930, 4695
flow_df = model.link.df[model.link.df.link_type == "flow"]
(upstream_link_id,) = flow_df.index[(flow_df.from_node_id == IRENESLUIS) & (flow_df.to_node_id == ARK)]
(downstream_link_id,) = flow_df.index[(flow_df.from_node_id == ARK) & (flow_df.to_node_id == ARK_END)]
upstream_line = cast(LineString, flow_df.at[upstream_link_id, "geometry"])
downstream_line = cast(LineString, flow_df.at[downstream_link_id, "geometry"])
ark_route = LineString([*upstream_line.coords, *downstream_line.coords[1:]])
ark_basin_ids, _ = split_basin_along_route(
    model,
    ARK,
    ark_route,
    # ARK-Zuilen after gemaal De Aanvoerder, and ARK-Nigtevecht before Nigtevecht
    split_points=[Point(133742, 457096), Point(129815, 475689)],
    basin_names=["Amsterdam-Rijnkanaal Utrecht", "Amsterdam-Rijnkanaal Breukelen", "Amsterdam-Rijnkanaal Weesp"],
    manning_names=["ARK-Zuilen", "ARK-Nigtevecht"],
    manning_n=0.04,  # like ManningResistance 4695
    profile_slope=1.0,
)
assert ark_basin_ids[1] == ARK, "The Basin node of the ARK lies in Breukelen"
# Like the other ManningResistances, see 5_model_netwerk.py, the one at the end has the length of its links
assert model.manning_resistance.static.df is not None
end_links = model.link.df[(model.link.df.from_node_id == ARK_END) | (model.link.df.to_node_id == ARK_END)]
assert len(end_links) == 2, f"Expected two links of ManningResistance {ARK_END}"
model.manning_resistance.static.df.loc[model.manning_resistance.static.df.node_id == ARK_END, "length"] = (
    end_links.length.sum()
)
# The links of demands start from a network node at their Basin node, see `add_demand`
for basin_id in ark_basin_ids:
    if network.nodes.distance(model.basin[basin_id].geometry).min() > TOLERANCE:
        network_node_id = network.add_node(model.basin[basin_id].geometry, max_distance=TOLERANCE)
        assert network_node_id is not None, f"Basin {basin_id} is not on the network"

# %% add basin_node_ids to network
assert model.basin.area.df is not None
basin_area_gdf = model.basin.area.df.copy()
basin_area_gdf.rename(columns={"node_id": "basin_id"}, inplace=True)
network.overlay(basin_area_gdf)  # basin_id toekennen aan netwerk

# network_nodes_df = network.nodes
# network_nodes_df = network_nodes_df.sjoin(basin_area_gdf, how="left")


# %% Drinkwater
drinkwater_gdf = gpd.read_file(onttrekkingen_gpkg, layer="Drinkwater_oppervlaktewater")
drinkwater_gdf.dropna(subset=["productie"], inplace=True)


priority = 1
return_factor = 0.0

for row in drinkwater_gdf.itertuples():
    add_demand(
        model=model,
        network=network,
        demand=float(cast(float, row.productie)),
        return_factor=return_factor,
        priority=priority,
        name=cast(str, row.naam),
        geometry=cast(Point, row.geometry),
        meta_sector="drinkwater",
        meta_beheerder=row.beheerder,
    )

# %% Energie
energie_gdf = gpd.read_file(onttrekkingen_gpkg, layer="Energiecentrales")

energie_inlet_gdf = gpd.read_file(onttrekkingen_gpkg, layer="Energiecentrales-inlaat")
energie_inlet_gdf.drop_duplicates("osm_id_Energiecentrales", inplace=True)

mask = ~energie_inlet_gdf.naam.isin(["Centrale Bergum", "Pallas Reactor"])

energie_outlet_gdf = gpd.read_file(onttrekkingen_gpkg, layer="Energiecentrales-uitlaat")

priority = 2
return_factor = 0.95

for row in energie_inlet_gdf[mask].itertuples():
    # inlet geometry and reference to osm_id, and demand
    inlet_geometry = row.geometry
    osm_id = row.osm_id_Energiecentrales
    demand = float(cast(float, row.capaciteit))

    # from plant name and (demand-)geometry and
    plant = energie_gdf.set_index("osm_id").loc[osm_id]
    name = cast(str, plant["name"])
    print(name)
    beheerder = plant.operator
    geometry = plant.geometry.centroid
    outlets = energie_outlet_gdf[energie_outlet_gdf["osm_id_Energiecentrales"] == osm_id]
    outlet_geometry = outlets.geometry.iloc[0]

    add_demand(
        model=model,
        network=network,
        demand=demand,
        return_factor=return_factor,
        priority=priority,
        name=name,
        geometry=cast(Point, geometry),
        inlet_geometry=cast(Point, inlet_geometry),
        outlet_geometry=outlet_geometry,
        meta_sector="energie",
        meta_beheerder=beheerder,
        max_distance=50,
    )

# %% Industrie
industrie_gdf = gpd.read_file(onttrekkingen_gpkg, layer="Industrieen")

industrie_inlet_gdf = gpd.read_file(onttrekkingen_gpkg, layer="Industrie-inlaat")
industrie_inlet_gdf.rename(columns={"debiet(m3/s)": "demand"}, inplace=True)
industrie_inlet_gdf.loc[:, "basin_id"] = industrie_inlet_gdf.geometry.apply(  # ty: ignore[no-matching-overload]
    lambda x: basin_area_gdf.set_index("basin_id").distance(x).idxmin()
)
industrie_inlet_gdf.drop_duplicates(["osm_id_Industrieen", "basin_id"], inplace=True)
industrie_inlet_gdf = industrie_inlet_gdf[industrie_inlet_gdf["demand"] != 0]

mask = ~industrie_inlet_gdf.naam.isin(["Avebe", "Evides Geervliet", "Evides Veerweg", "Evides KPE Zinker"])

industrie_outlet_gdf = gpd.read_file(onttrekkingen_gpkg, layer="Industrie-uitlaat")

#
maas_line = model.link.df[model.link.df.name == "Maas"].union_all()
index = industrie_outlet_gdf[industrie_outlet_gdf["naam"] == "Chemelot"].index[0]
point = cast(Point, industrie_outlet_gdf.at[index, "geometry"])
point = maas_line.interpolate(maas_line.project(point))
industrie_outlet_gdf.loc[index, "geometry"] = point  # ty: ignore[invalid-assignment]

industrie_outlet_gdf.rename(columns={"debiet(m3/s)": "return_flow"}, inplace=True)

noordzee_outlet = Point(99060, 500050)
noordzee_plants = ["Crown van Gelder", "Tata Steel"]

priority = 3

for row in industrie_inlet_gdf[mask].itertuples():
    # row = next(industrie_inlet_gdf[mask].itertuples())
    # inlet geometry and reference to osm_id, and demand
    inlet_geometry = row.geometry
    osm_id = row.osm_id_Industrieen
    demand = float(cast(float, row.demand))

    # from plant name and (demand-)geometry and
    plant = industrie_gdf.set_index("osm_id").loc[osm_id]
    name = cast(str, plant["name"])
    print(name)
    geometry = plant.geometry.centroid
    outlets = industrie_outlet_gdf[industrie_outlet_gdf["osm_id_Industrieen"] == osm_id]
    if outlets.empty:  # no outlet = no return-flow
        outlet_geometry = noordzee_outlet
        outlet_as_terminal = True
        return_factor = 0
    else:
        outlet_as_terminal = False
        outlet_geometry = outlets.geometry.iloc[0]
        if outlets["return_flow"].isna().all():  # no outlet capacity defined, we assume 90%
            return_factor = 0.9
        else:  # else we calculate
            return_flow = outlets["return_flow"].sum()
            return_factor = return_flow / demand
            # if return_flow > demand:
            #     raise ValueError(f"return_flow > demand: {return_flow} > {demand}")

    add_demand(
        model=model,
        network=network,
        demand=demand,
        return_factor=return_factor,
        priority=priority,
        name=name,
        geometry=cast(Point, geometry),
        inlet_geometry=cast(Point, inlet_geometry),
        outlet_geometry=outlet_geometry,
        outlet_as_terminal=outlet_as_terminal,
        meta_sector="industrie",
        max_distance=50,
    )


# %% add junction nodes
junctionify(model)

# %% wegschrijven model
ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_demand/hws.toml")
model.write(ribasim_toml)

# %%

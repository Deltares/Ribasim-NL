# %%
import geopandas as gpd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl import CloudStorage, Model
from ribasim_nl.geometry import edge
from shapely.geometry import LineString, Point

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Vechtstromen", "modellen", "Vechtstromen_2024_6_3", "vechtstromen.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")


# %% read model
model = Model.read(ribasim_toml)

# %% some stuff we'll need again
manning_data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])
level_data = level_boundary.Static(level=[0])

basin_data = [
    basin.Profile(level=[0.0, 1.0], area=[0.01, 1000.0]),
    basin.Static(
        drainage=[0.0],
        potential_evaporation=[0.001 / 86400],
        infiltration=[0.0],
        precipitation=[0.005 / 86400],
    ),
    basin.State(level=[0]),
]
outlet_data = outlet.Static(flow_rate=[100])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2352686763

# Toevoegen benedenstroomse randvoorwaarden Beneden Dinkel


# verplaats basin 1375 naar het hydroobject
node_id = 1375
gdf = gpd.read_file(
    cloud.joinpath("Vechtstromen", "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="hydroobject", fid_as_index=True
)
model.basin.node.df.loc[node_id, "geometry"] = gdf.at[3135, "geometry"].interpolate(0.5, normalized=True)
edge_ids = model.edge.df[
    (model.edge.df.from_node_id == node_id) | (model.edge.df.to_node_id == node_id)
].index.to_list()
model.reset_edge_geometry(edge_ids=edge_ids)

# %%
# verplaats basin 1375 naar het hydroobject
gdf = gpd.read_file(
    cloud.joinpath("Vechtstromen", "verwerkt", "fix_user_data.gpkg"), layer="level_boundary", fid_as_index=True
)

# verbind basins met level_boundaries
for fid, node_id in [(1, 1375), (2, 1624)]:
    boundary_node_geometry = gdf.at[1, "geometry"]

    # line for interpolation
    basin_node_geometry = Point(
        model.basin.node.df.at[node_id, "geometry"].x, model.basin.node.df.at[node_id, "geometry"].y
    )
    line_geometry = LineString((basin_node_geometry, boundary_node_geometry))

    # define level_boundary_node
    boundary_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])
    level_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])

    # define manning_resistance_node
    outlet_node_geometry = line_geometry.interpolate(line_geometry.length - 20)
    outlet_node = model.outlet.add(Node(geometry=outlet_node_geometry), tables=[outlet_data])

    # draw edges
    # FIXME: we force edges to be z-less untill this is solved: https://github.com/Deltares/Ribasim/issues/1854
    model.edge.add(
        model.basin[node_id], outlet_node, geometry=edge(model.basin[node_id].geometry, outlet_node.geometry)
    )
    model.edge.add(outlet_node, boundary_node, geometry=edge(outlet_node.geometry, boundary_node.geometry))


#  %% write model
ribasim_toml = cloud.joinpath("Vechtstromen", "modellen", "Vechtstromen_fix_model_network", "vechtstromen.toml")
model.write(ribasim_toml)
# %%

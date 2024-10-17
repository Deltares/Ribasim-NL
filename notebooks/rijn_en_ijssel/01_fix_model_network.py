# %%
import geopandas as gpd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "RijnenIJssel"
short_name = "wrij"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{short_name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")


hydroobject_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="hydroobject", fid_as_index=True
)

duiker_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="duikersifonhevel", fid_as_index=True
)

# %% read model
model = Model.read(ribasim_toml)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_network", f"{short_name}.toml")
network_validator = NetworkValidator(model)

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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419605149
# Verwijderen duplicate edges

model.edge.df.drop_duplicates(inplace=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419620184
# toevoegen ontbrekende basins

basin_edges_df = network_validator.edge_incorrect_connectivity()
basin_nodes_df = network_validator.node_invalid_connectivity()

for row in basin_nodes_df.itertuples():
    # maak basin-node
    basin_node = model.basin.add(Node(geometry=row.geometry), tables=basin_data)

    # update edge_table
    model.edge.df.loc[basin_edges_df[basin_edges_df.from_node_id == row.node_id].index, ["from_node_id"]] = (
        basin_node.node_id
    )
    model.edge.df.loc[basin_edges_df[basin_edges_df.to_node_id == row.node_id].index, ["to_node_id"]] = (
        basin_node.node_id
    )

# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419649171
# update edge administratie

model.edge.df.loc[516, "from_node_id"] = 666
model.edge.df.loc[520, "from_node_id"] = 667
model.edge.df.loc[954, "to_node_id"] = 652
model.edge.df.loc[1271, "to_node_id"] = 662
model.edge.df.loc[1281, "to_node_id"] = 667

# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419747636

# fix edge_richting

# verplaatsen van `LevelBoundary` 47 binnen de basin, updaten naar `Basin` en reversen van `Edge` 1370
model.move_node(47, hydroobject_gdf.at[8781, "geometry"].boundary.geoms[0])
model.update_node(47, "Basin", data=basin_data)
model.reverse_edge(edge_id=1370)

# omdraaien richting van `Edge` 196
model.reverse_edge(edge_id=196)
model.reverse_edge(edge_id=188)
model.reverse_edge(edge_id=472)
model.reverse_edge(edge_id=513)
model.reverse_edge(edge_id=560)
model.reverse_edge(edge_id=391)
model.reverse_edge(edge_id=566)

# opruimen basin Arnhem nabij Lauwersgracht
model.remove_node(514, remove_edges=True)
model.remove_node(1101, remove_edges=True)
model.remove_edges([1364, 1363])

kdu = duiker_gdf.loc[548]
outlet_node = model.outlet.add(
    Node(name=kdu.code, geometry=kdu.geometry.interpolate(0.5, normalized=True), meta_object_type="duikersifonhevel"),
    tables=[outlet_data],
)
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[9528, "geometry"].boundary.geoms[0]))
model.edge.add(model.tabulated_rating_curve[265], basin_node)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[43])
model.edge.add(basin_node, model.pump[264])
model.edge.add(model.pump[264], model.level_boundary[44])


#  %% write model
# model.use_validation = True
model.write(ribasim_toml)

# %%

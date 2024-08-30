# %%

import geopandas as gpd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl import CloudStorage, Model, NetworkValidator
from shapely.geometry import Point

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_update_to_main", "model.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")


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

# %% read model
model = Model.read(ribasim_toml)

network_validator = NetworkValidator(model)

# %% verwijder duplicated edges

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2288780504
# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291081244
model.edge.df = model.edge.df.drop_duplicates(subset=["from_node_id", "to_node_id"])

if not network_validator.edge_duplicated().empty:
    raise Exception("nog steeds duplicated edges")

# %% toevoegen bovenstroomse knopen

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291091067
node_id = model.next_node_id
edge_id = model.edge.df.loc[model.edge.df.to_node_id == 251].index[0]
model.edge.df.loc[edge_id, ["from_node_id"]] = node_id

node = Node(node_id, model.edge.df.at[edge_id, "geometry"].boundary.geoms[0])
model.basin.area.df.loc[model.basin.area.df.node_id == 1009, ["node_id"]] = node_id
area = basin.Area(geometry=model.basin.area[node_id].geometry.to_list())
model.basin.add(node, basin_data + [area])

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291111647
for row in network_validator.edge_incorrect_connectivity().itertuples():
    # drop edge from model
    model.remove_edge(row.from_node_id, row.to_node_id, remove_disconnected_nodes=False)

    # add basin_node
    area = basin.Area(geometry=model.basin.area[row.from_node_id].geometry.to_list())
    basin_node = Node(row.from_node_id, row.geometry.boundary.geoms[0])
    model.basin.add(basin_node, basin_data + [area])

    # eindhovensch kanaal we need to add manning a 99% of the length
    if row.to_node_id == 2:
        geometry = row.geometry.interpolate(0.99, normalized=True)
        name = ""
        meta_object_type = "openwater"
    if row.to_node_id == 14:
        gdf = gpd.read_file(
            cloud.joinpath("DeDommel", "verwerkt", "4_ribasim", "hydamo.gpkg"),
            layer="duikersifonhevel",
            engine="pyogrio",
            fid_as_index=True,
        )
        kdu = gdf.loc[250]
        geometry = kdu.geometry.interpolate(0.5, normalized=True)
        geometry = Point(geometry.x, geometry.y)
        name = kdu.CODE
        meta_object_type = "duikersifonhevel"

    # add manning-node
    manning_node_id = model.next_node_id
    manning_data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])
    model.manning_resistance.add(
        Node(node_id=manning_node_id, geometry=geometry, name=name, meta_object_type=meta_object_type),
        [manning_data],
    )

    # add edges
    model.edge.add(model.basin[row.from_node_id], model.manning_resistance[manning_node_id])
    model.edge.add(model.manning_resistance[manning_node_id], model.level_boundary[row.to_node_id])


if not network_validator.edge_incorrect_connectivity().empty:
    raise Exception("nog steeds edges zonder knopen")

# %% verwijderen internal basins

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291271525
for row in network_validator.node_internal_basin().itertuples():
    if row.Index not in model.basin.area.df.node_id.to_numpy():  # remove or change to level-boundary
        edge_select_df = model.edge.df[model.edge.df.to_node_id == row.Index]
        if len(edge_select_df) == 1:
            if model.node_table().df.at[edge_select_df.iloc[0]["from_node_id"], "node_type"] == "FlowBoundary":
                model.remove_node(row.Index)
                model.remove_node(edge_select_df.iloc[0]["from_node_id"])
                model.edge.df.drop(index=edge_select_df.index[0], inplace=True)

df = model.node_table().df[model.node_table().df.node_type == "Basin"]

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291876800
boundary_node = Node(node_id=28, geometry=model.flow_boundary[28].geometry)

for node_id in [29, 1828, 615, 28, 1329]:
    model.remove_node(node_id, remove_edges=True)

level_data = level_boundary.Static(level=[0])
model.level_boundary.add(boundary_node, [level_data])

model.edge.add(model.tabulated_rating_curve[614], model.level_boundary[28])

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2292014475
model.remove_node(node_id=1898, remove_edges=True)

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2292017813
for node_id in [1891, 989, 1058]:
    model.remove_node(node_id, remove_edges=True)

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291988317
# for from_node_id, to_node_id in [799, 1580, 625, 1123, 597, 978]:
for from_node_id, to_node_id in [[616, 1032], [1030, 616], [393, 1242], [1852, 393], [353, 1700], [1253, 353]]:
    model.reverse_edge(from_node_id, to_node_id)

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2292050862

gdf = gpd.read_file(
    cloud.joinpath("DeDommel", "verwerkt", "1_ontvangen_data", "Geodata", "data_Q42018.gpkg"),
    layer="HydroObject",
    engine="pyogrio",
    fid_as_index=True,
)

geometry = gdf.loc[2751].geometry.interpolate(0.5, normalized=True)
node_id = model.next_node_id
data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])

model.manning_resistance.add(Node(node_id=node_id, geometry=geometry), [manning_data])

model.edge.df = model.edge.df[~((model.edge.df.from_node_id == 611) & (model.edge.df.to_node_id == 1643))]
model.edge.add(model.basin[1643], model.manning_resistance[node_id])
model.edge.add(model.manning_resistance[node_id], model.basin[1182])
model.edge.add(model.tabulated_rating_curve[611], model.basin[1182])

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2293457160
model.update_node(417, "Outlet", [outlet.Static(flow_rate=[0])])

if not network_validator.node_internal_basin().empty:
    raise Exception("nog steeds interne basins")

df = network_validator.edge_incorrect_type_connectivity(
    from_node_type="ManningResistance", to_node_type="LevelBoundary"
)

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2293486609
for node_id in df.from_node_id:
    model.update_node(node_id, "Outlet", [outlet.Static(flow_rate=[100])])

# # see: https://github.com/Deltares/Ribasim-NL/issues/132
model.basin.area.df.loc[model.basin.area.df.duplicated("node_id"), ["node_id"]] = -1
model.basin.area.df.reset_index(drop=True, inplace=True)
model.basin.area.df.index.name = "fid"
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest", distance=100)
model.fix_unassigned_basin_area()

model.basin.area.df = model.basin.area.df[~model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]

# # %% write model
model.edge.df.reset_index(drop=True, inplace=True)
model.edge.df.index.name = "edge_id"
ribasim_toml = ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_fix_model_network", "model.toml")

model.write(ribasim_toml)

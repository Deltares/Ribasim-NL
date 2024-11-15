# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "Limburg"
short_name = "limburg"

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


# HIER KOMEN ISSUES

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426118811

# Verwijderen edge met 0m lengte
model.remove_node(2434, remove_edges=True)
model.remove_node(1308, remove_edges=True)
model.merge_basins(basin_id=2396, to_basin_id=1669, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426151899

# Corrigeren ontbrekende basins en outlets nabij modelrand
# geen kdu bij 2202
geometry = hydroobject_gdf.at[3099, "geometry"]
basin_node = model.basin.add(Node(geometry=geometry.boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(Node(geometry=geometry.interpolate(271)), tables=[outlet_data])
model.redirect_edge(edge_id=2202, from_node_id=outlet_node.node_id)
model.edge.add(basin_node, outlet_node)

for fid, edge_id, boundary_node_id in ((2054, 2244, 63), (9794, 2295, 103), (9260, 2297, 105), (3307, 2305, 113)):
    kdu = duiker_gdf.loc[fid]
    basin_node = model.basin.add(
        Node(geometry=model.edge.df.loc[edge_id, "geometry"].boundary.geoms[0]), tables=basin_data
    )
    outlet_node = model.outlet.add(
        Node(
            name=kdu.code, geometry=kdu.geometry.interpolate(0.5, normalized=True), meta_object_type="duikersifonhevel"
        ),
        tables=[outlet_data],
    )
    model.redirect_edge(edge_id=edge_id, from_node_id=outlet_node.node_id)
    model.edge.add(basin_node, outlet_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426258242

# Corrigeren netwerk bij Jeker
for node_id in [276, 2003, 990, 2395, 989]:
    model.remove_node(node_id, remove_edges=True)

basin_node = model.basin.add(Node(geometry=model.edge.df.at[2257, "geometry"].boundary.geoms[1]))
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[2099, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)
model.redirect_edge(edge_id=2257, to_node_id=basin_node.node_id)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[82])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426373368

# Corrigeren Snelle Loop bij Defensiekanaal
outlet_node = model.outlet.add(
    Node(geometry=model.edge.df.at[2357, "geometry"].boundary.geoms[1]), tables=[outlet_data]
)
model.redirect_edge(edge_id=2357, to_node_id=outlet_node.node_id)
model.edge.add(outlet_node, model.basin[1452])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426401489

# Corrigeren Panheelsebeek
model.remove_node(node_id=940, remove_edges=True)
model.reverse_edge(edge_id=211)
model.merge_basins(basin_id=2465, to_basin_id=1340, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426443778

# Toevoegen Wellse Molenbeek
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[3100, "geometry"].boundary.geoms[1]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[687, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)

model.redirect_edge(edge_id=2240, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2239, from_node_id=basin_node.node_id, to_node_id=outlet_node.node_id)
model.edge.add(basin_node, model.manning_resistance[425])
model.edge.add(outlet_node, model.level_boundary[59])


# %%
model.remove_node(node_id=1036, remove_edges=True)
kdu = duiker_gdf.loc[4664]
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[477, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(name=kdu.code, geometry=kdu.geometry.interpolate(0.5, normalized=True), meta_object_type="duikersifonhevel"),
    tables=[outlet_data],
)
model.edge.add(outlet_node, model.basin[1389])
model.edge.add(basin_node, outlet_node)

kdu = duiker_gdf.loc[3709]
outlet_node = model.outlet.add(
    Node(name=kdu.code, geometry=kdu.geometry.interpolate(0.5, normalized=True), meta_object_type="duikersifonhevel"),
    tables=[outlet_data],
)

model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[39])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426653554

# Correctie Panheelderbeek bij kanaal Wessem-Nederweert
model.remove_edges(edge_ids=[2316, 2309, 2307, 2308, 2310, 2312, 2315, 2317])
model.remove_node(114, remove_edges=True)
model.reverse_edge(edge_id=1999)
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[1649, "geometry"].boundary.geoms[1]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[4110, "geometry"].boundary.geoms[1]), tables=[outlet_data]
)

model.edge.add(model.tabulated_rating_curve[270], basin_node)
model.edge.add(basin_node, model.manning_resistance[1316])
model.edge.add(basin_node, model.manning_resistance[1315])
model.edge.add(basin_node, model.manning_resistance[1130])
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[115])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426706167

# Correctie edge-richting bij Ijsselsteinseweg

model.reverse_edge(edge_id=2332)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426763136

# Opname Helenavaart
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[112, "geometry"].boundary.geoms[0]), tables=basin_data)
model.redirect_edge(edge_id=2329, to_node_id=basin_node.node_id)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[112, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[123])

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[1702, "geometry"].interpolate(0.1, normalized=True)), tables=[outlet_data]
)
model.redirect_edge(edge_id=2328, to_node_id=outlet_node.node_id)
model.edge.add(outlet_node, basin_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426789675

# Opname Oude Helenavaart/kanaal van Deurne
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[1565, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[3277, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)

model.redirect_edge(edge_id=2327, from_node_id=outlet_node.node_id)
model.edge.add(basin_node, outlet_node)

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[1565, "geometry"].interpolate(0.98, normalized=True)), tables=[outlet_data]
)
model.redirect_edge(edge_id=2323, from_node_id=outlet_node.node_id)
model.edge.add(basin_node, outlet_node)

model.redirect_edge(edge_id=2326, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2325, to_node_id=basin_node.node_id)

model.edge.add(model.tabulated_rating_curve[238], basin_node)

# 2 edges die afwateren op Oude Helenavaart
model.remove_edges(edge_ids=[2322, 2324])


# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426816843

# Verwijderen afwaterende basisn Mooks kanaal
model.remove_node(34, remove_edges=True)

# EINDE ISSUES


# %%
# corrigeren knoop-topologie

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.edge_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.edge_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet", data=[outlet_data])


## UPDATEN STATIC TABLES

# %%
# basin-profielen/state updaten
df = pd.DataFrame(
    {
        "node_id": np.repeat(model.basin.node.df.index.to_numpy(), 2),
        "level": [0.0, 1.0] * len(model.basin.node.df),
        "area": [0.01, 1000.0] * len(model.basin.node.df),
    }
)
df.index.name = "fid"
model.basin.profile.df = df

df = model.basin.profile.df.groupby("node_id")[["level"]].max().reset_index()
df.index.name = "fid"
model.basin.state.df = df

# %%
# tabulated_rating_curves updaten
df = pd.DataFrame(
    {
        "node_id": np.repeat(model.tabulated_rating_curve.node.df.index.to_numpy(), 2),
        "level": [0.0, 5] * len(model.tabulated_rating_curve.node.df),
        "flow_rate": [0, 0.1] * len(model.tabulated_rating_curve.node.df),
    }
)
df.index.name = "fid"
model.tabulated_rating_curve.static.df = df


# %%

# level_boundaries updaten
df = pd.DataFrame(
    {
        "node_id": model.level_boundary.node.df.index.to_list(),
        "level": [0.0] * len(model.level_boundary.node.df),
    }
)
df.index.name = "fid"
model.level_boundary.static.df = df

# %%
# manning_resistance updaten
length = len(model.manning_resistance.node.df)
df = pd.DataFrame(
    {
        "node_id": model.manning_resistance.node.df.index.to_list(),
        "length": [100.0] * length,
        "manning_n": [100.0] * length,
        "profile_width": [100.0] * length,
        "profile_slope": [100.0] * length,
    }
)
df.index.name = "fid"
model.manning_resistance.static.df = df

# %%
# flow boundaries updaten
length = len(model.flow_boundary.node.df)
df = pd.DataFrame(
    {
        "node_id": model.flow_boundary.node.df.index.to_list(),
        "flow_rate": [0.0] * length,
    }
)
df.index.name = "fid"
model.flow_boundary.static.df = df

model.fix_unassigned_basin_area()

#  %% write model
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()
# %%

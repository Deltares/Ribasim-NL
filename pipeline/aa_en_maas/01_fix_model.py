# %%
from typing import cast

import geopandas as gpd
import numpy as np
import pandas as pd
from pandera.typing import DataFrame
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet
from ribasim.schemas import (
    BasinProfileSchema,
    BasinStateSchema,
    FlowBoundaryStaticSchema,
    LevelBoundaryStaticSchema,
    ManningResistanceStaticSchema,
    TabulatedRatingCurveStaticSchema,
)
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.model import default_tables
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, MultiPolygon, Point

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "AaenMaas"
name = "aam"


# %% Check if model exist, otherwise download
ribasim_dir = cloud.joinpath(authority, f"modellen/{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")

# %%
model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)

# %% some stuff we'll need again
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
outlet_data = outlet.Static(flow_rate=[5.0])


# HIER KOMEN ISSUES

# %% https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421959240

# Verwijderen link met 0m lengte
model.remove_node(34, remove_links=True)
model.update_node(1568, "LevelBoundary", data=[level_data], node_properties={"name": ""})


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421991252

# Corrigeren netwerk Den Bosch

# Binnenstad
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[2788, "geometry"]).boundary.geoms[0]), tables=basin_data
)

model.reverse_link(link_id=2077)
model.redirect_link(link_id=2077, to_node_id=basin_node.node_id)
model.redirect_link(link_id=2078, from_node_id=basin_node.node_id)
model.redirect_link(link_id=2079, from_node_id=basin_node.node_id)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[4999, "geometry"]).interpolate(0.5, normalized=True)),
    tables=[outlet_data],
)
model.link.add(model.level_boundary[46], outlet_node)
model.link.add(outlet_node, basin_node)

# Dommel
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[9055, "geometry"]).interpolate(0.5, normalized=True)),
    tables=basin_data,
)
model.redirect_link(link_id=2082, from_node_id=basin_node.node_id)

outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[9055, "geometry"]).interpolate(0.1, normalized=True)),
    tables=[outlet_data],
)

model.link.add(model.level_boundary[49], outlet_node)
model.link.add(outlet_node, basin_node)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422078500

# Corrigeren netwerk bij sluis Empel
for node_id in [729, 730, 1990, 1962]:
    model.remove_node(node_id, remove_links=True)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2431933060
# Omkeren linkrichting
for link_id in [131, 398, 407, 495, 513, 515, 894]:
    model.reverse_link(link_id=link_id)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422164355

# Corrigeren netwerk bij Spuisluis Crèvecoeur
model.remove_node(411, remove_links=True)
model.remove_node(4, remove_links=True)

model.redirect_link(link_id=2018, to_node_id=1950)

outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[4825, "geometry"]).boundary.geoms[0], name="AKW839"),
    tables=[outlet_data],
)

model.link.add(outlet_node, model.level_boundary[5])
model.link.add(model.basin[1627], outlet_node)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422373708

# Corrigeren Afleidingskanaal bij Holthees

# nabij grens
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[8868, "geometry"]).boundary.geoms[0]), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[4680, "geometry"]).interpolate(0.1, normalized=True)),
    tables=[outlet_data],
)

model.redirect_link(link_id=2091, from_node_id=56, to_node_id=outlet_node.node_id)
model.redirect_link(link_id=2092, from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)
model.redirect_link(link_id=2093, from_node_id=653, to_node_id=basin_node.node_id)
model.redirect_link(link_id=2094, from_node_id=basin_node.node_id, to_node_id=82)

# nabij afleidingskanaal
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[7466, "geometry"]).boundary.geoms[0]), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[8456, "geometry"]).interpolate(0.1, normalized=True)),
    tables=[outlet_data],
)
model.redirect_link(link_id=2089, to_node_id=outlet_node.node_id)
model.redirect_link(link_id=2088, from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)
model.link.add(model.tabulated_rating_curve[82], basin_node)
model.link.add(basin_node, model.tabulated_rating_curve[853])

# nabij Maas
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[2385, "geometry"]).boundary.geoms[0]), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[2539, "geometry"]).boundary.geoms[0]), tables=[outlet_data]
)

model.redirect_link(link_id=2054, to_node_id=basin_node.node_id)
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[26])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422452167

# Toevoegen basin bij Oude Zuid-Willemsvaart
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[3174, "geometry"]).boundary.geoms[0], name="20301"),
    tables=[outlet_data],
)
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[6499, "geometry"]).boundary.geoms[0]), tables=basin_data
)
model.redirect_link(link_id=2102, to_node_id=outlet_node.node_id)
model.link.add(outlet_node, basin_node)
model.redirect_link(link_id=2106, to_node_id=2026)

outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[646, "geometry"]).interpolate(0.9, normalized=True)),
    tables=[outlet_data],
)
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[66])

# EINDE ISSUES
##fixes:
for node_id in [213, 364, 415, 439, 467, 501, 587, 602, 634, 664, 682, 683, 684, 809, 811, 1053]:
    model.remove_node(node_id, remove_links=True)

# %%
# corrigeren knoop-topologie

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
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
model.basin.profile.df = DataFrame[BasinProfileSchema](df)

assert model.basin.profile.df is not None
df = model.basin.profile.df.groupby("node_id")[["level"]].max().reset_index()
df.index.name = "fid"
model.basin.state.df = DataFrame[BasinStateSchema](df)

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
model.tabulated_rating_curve.static.df = DataFrame[TabulatedRatingCurveStaticSchema](df)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2431933060
assert model.tabulated_rating_curve.static.df is not None
model.tabulated_rating_curve.static.df.loc[
    model.tabulated_rating_curve.static.df.node_id.isin([280, 335, 373, 879]), "flow_rate"
] = 0.0


# %%

# level_boundaries updaten
df = pd.DataFrame(
    {
        "node_id": model.level_boundary.node.df.index.to_list(),
        "level": [0.0] * len(model.level_boundary.node.df),
    }
)
df.index.name = "fid"
model.level_boundary.static.df = DataFrame[LevelBoundaryStaticSchema](df)

# %%
# manning_resistance updaten
length = len(model.manning_resistance.node.df)
df = pd.DataFrame(
    {
        "node_id": model.manning_resistance.node.df.index.to_list(),
        "length": [100.0] * length,
        "manning_n": [0.03] * length,
        "profile_width": [100.0] * length,
        "profile_slope": [100.0] * length,
    }
)
df.index.name = "fid"
model.manning_resistance.static.df = DataFrame[ManningResistanceStaticSchema](df)

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
model.flow_boundary.static.df = DataFrame[FlowBoundaryStaticSchema](df)

# %% read
basin_node_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="unassigned_basin_node")
basin_area_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="unassigned_basin_area")
internal_basin_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="internal_basins")
remove_nodes_df = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="remove_node")
areas_gdf = gpd.read_file(areas_gpkg, layer="areas")

# %% the areas assigned to a basin by hand, by geometry and by area code
for row in basin_area_edits_gdf[basin_area_edits_gdf["to_node_id"].notna()].itertuples():
    model.add_basin_area(cast(MultiPolygon, row.geometry), node_id=int(cast(str, row.to_node_id)))
for row in basin_node_edits_gdf[basin_node_edits_gdf["to_area_code"].notna()].itertuples():
    area = areas_gdf.loc[areas_gdf["code"] == row.to_area_code].union_all()
    model.add_basin_area(area, node_id=cast(int, row.node_id))

# %% merge_basins
for row in basin_node_edits_gdf[basin_node_edits_gdf["to_node_id"].notna()].itertuples():
    are_connected = True if pd.isna(row.connected) else row.connected
    model.merge_basins(
        node_id=cast(int, row.node_id), to_node_id=cast(int, row.to_node_id), are_connected=are_connected
    )

mask = internal_basin_edits_gdf["to_node_id"].notna() & internal_basin_edits_gdf["add_object"].isna()
for row in internal_basin_edits_gdf[mask].itertuples():
    are_connected = True if pd.isna(row.connected) else row.connected
    model.merge_basins(
        node_id=cast(int, row.node_id), to_node_id=cast(int, row.to_node_id), are_connected=are_connected
    )

# %% add and connect nodes
for row in internal_basin_edits_gdf[internal_basin_edits_gdf.add_object.notna()].itertuples():
    from_basin_id = cast(int, row.node_id)
    to_basin_id = cast(int, row.to_node_id)
    if row.add_object == "stuw":
        node_type = "TabulatedRatingCurve"
    model.add_and_connect_node(
        from_basin_id, int(to_basin_id), geometry=row.geometry, node_type=node_type, name=row.add_object_name
    )

# %% reverse direction at node
for row in internal_basin_edits_gdf[internal_basin_edits_gdf["reverse_direction"]].itertuples():
    model.reverse_direction_at_node(node_id=row.node_id)

# %% change node_type
for row in basin_node_edits_gdf[basin_node_edits_gdf["change_to_node_type"].notna()].itertuples():
    if row.change_to_node_type:
        model.update_node(row.node_id, row.change_to_node_type, data=[level_boundary.Static(level=[0])])


# %% remove_nodes
for row in remove_nodes_df.itertuples():
    model.remove_node(node_id=cast(int, row.node_id), remove_links=cast(bool, row.remove_links))


# %% merge nodes

# Manning worden outlets tpv stuw/duiker
outlet_ids = [
    586,
    479,
    531,
    510,
    475,
    681,
    1006,
    1005,
    551,
    488,
    1115,
    566,
    686,
    469,
    569,
    533,
    526,
    632,
    1018,
    723,
    722,
    570,
    697,
    708,
    640,
    521,
    1063,
    452,
    1054,
    625,
    1056,
    694,
    524,
    425,
    693,
    442,
    538,
    702,
    710,
    622,
    680,
    584,
    487,
    1062,
    506,
    541,
    527,
    577,
    1050,
    582,
    1051,
    599,
    597,
    597,
    597,
    597,
    807,
    628,
    721,
    573,
    1073,
    621,
    534,
    496,
    555,
    559,
    718,
    481,
    691,
    1046,
    438,
    430,
    717,
    1058,
    698,
    571,
    1001,
    701,
    494,
    509,
    594,
    1016,
    615,
    470,
    1014,
    490,
    649,
    476,
    1028,
    727,
]

for node_id in dict.fromkeys(outlet_ids):
    model.update_node(node_id=node_id, node_type="Outlet")

# ligt benedenstrooms een stuw
model.update_node(node_id=1077, node_type="Outlet")  # wordt outlet, was manning
assert model.outlet.static.df is not None
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1077, "min_upstream_level"] = 4.48499

model.redirect_link(link_id=1147, from_node_id=1219, to_node_id=172)
model.reverse_link(link_id=734)
model.reverse_link(link_id=1582)


merge_pairs = [
    (1321, 1411),
    (1562, 1411),
    (1155, 1514),
    (1565, 1618),
    (2006, 1618),
    (1366, 1249),
    (1437, 1861),
    (1303, 1360),
    (1707, 1346),
    (1162, 1457),
    (1468, 1277),
    (1355, 1517),
    (1344, 1626),
    (1428, 1589),
    (1668, 1710),
    (1292, 1484),
    (1966, 1580),
    (1281, 1642),
    (1642, 1510),
    (1571, 1332),
    (1332, 1582),
    (1208, 1279),
    (1279, 1582),
    (1663, 1722),
    (1794, 1836),
    (1455, 1276),
    (1968, 1494),
    (1615, 1331),
    (1911, 1331),
]

for basin_id, to_basin_id in merge_pairs:
    model.merge_basins(
        node_id=basin_id,
        to_node_id=to_basin_id,
        are_connected=True,
    )


# %% Connect basins:
actions = [
    "add_basin",
    "update_node",
    "connect_basins",
]

available_layers = gpd.list_layers(model_edits_gpkg).name.to_list()
actions = [a for a in actions if a in available_layers]

model.apply_edits(model_edits_gpkg, actions, sort_by_order=True, drop_na=True)


# %% corrigeren knoop-topologie
outlet_data = outlet.Static(flow_rate=[5.0])
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet", data=[outlet_data])

# %% sanitize node-table
# TabulatedRatingCurve to Outlet
for row in model.node.df[model.node.df.node_type == "TabulatedRatingCurve"].itertuples():
    node_id = row.Index
    model.update_node(node_id=node_id, node_type="Outlet")

# basins and outlets we've added do not have category, we fill with hoofdwater
model.node.df.loc[model.basin.node.df.index[model.basin.node.df["meta_categorie"].isna()], "meta_categorie"] = (
    "hoofdwater"
)
model.node.df.loc[model.outlet.node.df.index[model.outlet.node.df["meta_categorie"].isna()], "meta_categorie"] = (
    "hoofdwater"
)


# somehow Sluis Engelen (beheerregister AAM) has been named Henriettesluis
model.node.df.loc[
    (model.node.df["node_type"] == "Outlet") & (model.node.df.name == "Henriëttesluis"),
    "name",
] = "AKW855"

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(authority=authority, layers=["gemaal", "stuw", "sluis"])
df.set_index("code", inplace=True)
names = df["naam"]


# set meta_gestuwd in basins
model.node.df.loc[model.node.df["node_type"] == "Basin", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Outlet", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Pump", "meta_gestuwd"] = True

# set stuwen als gestuwd
model.node.df.loc[
    (model.node.df["node_type"] == "Outlet") & (model.node.df["meta_object_type"] == "stuw"),
    "meta_gestuwd",
] = True

# toevoegen 261HTE, zo goed mogelijk
df = gpd.read_file(hydamo_gpkg, layer="stuw")
geometry = df.set_index("code").at["261HTE", "geometry"]

model.connect_basins(from_basin_id=1280, to_basin_id=1126, node_type="Outlet", geometry=geometry, name="261HTE")
# Fixes kleine basins
# model traag door te kleine basins
model.merge_basins(node_id=1866, to_node_id=1118, are_connected=True)  # Klein oppervlakte basin
model.merge_basins(node_id=1687, to_node_id=1870, are_connected=True)
model.merge_basins(node_id=1320, to_node_id=1870, are_connected=True)
model.merge_basins(node_id=1901, to_node_id=1798, are_connected=True)
model.merge_basins(node_id=1733, to_node_id=2011, are_connected=True)
model.merge_basins(node_id=1550, to_node_id=2011, are_connected=True)
model.merge_basins(node_id=1160, to_node_id=1944, are_connected=True)
model.merge_basins(node_id=1405, to_node_id=1641, are_connected=True)
model.merge_basins(node_id=1641, to_node_id=1394, are_connected=True)
model.merge_basins(node_id=1453, to_node_id=1883, are_connected=True)
model.merge_basins(node_id=1896, to_node_id=1311, are_connected=True)
model.merge_basins(node_id=1126, to_node_id=1296, are_connected=True)
model.merge_basins(node_id=1588, to_node_id=1815, are_connected=True)
model.merge_basins(node_id=1950, to_node_id=1627, are_connected=True)
model.merge_basins(node_id=1627, to_node_id=1394, are_connected=True)
model.merge_basins(node_id=1914, to_node_id=1877, are_connected=True)
model.merge_basins(node_id=1200, to_node_id=1877, are_connected=True)
model.merge_basins(node_id=1887, to_node_id=1617, are_connected=True)
model.merge_basins(node_id=1519, to_node_id=1857, are_connected=True)
model.merge_basins(node_id=1949, to_node_id=1852, are_connected=True)

# Drongelens kanaal
model.merge_basins(node_id=1801, to_node_id=1879, are_connected=True)
model.merge_basins(node_id=1879, to_node_id=1743, are_connected=True)
model.merge_basins(node_id=1743, to_node_id=1275, are_connected=True)

# set bovenstroomse basins als gestuwd
node_df = model.node.df[model.node.df["meta_gestuwd"] & model.node.df["node_type"].isin(["Outlet", "Pump"])]

upstream_node_ids = [model.upstream_node_id(i) for i in node_df.index]
basin_node_ids = model.basin.node.df.index.intersection(upstream_node_ids)
model.node.df.loc[basin_node_ids, "meta_gestuwd"] = True

# set álle benedenstroomse outlets van gestuwde basins als gestuwd (dus ook duikers en andere objecten)
downstream_node_ids = pd.Index(pd.Series([model.downstream_node_id(i) for i in basin_node_ids]).explode())
model.node.df.loc[model.outlet.node.df.index.intersection(downstream_node_ids), "meta_gestuwd"] = True

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["LevelBoundary", "FlowBoundary"], "columns": {"meta_name": "name"}},
        {"node_types": ["Basin", "ManningResistance"], "columns": {"name": ""}},
    ],
    names=names,
)


# %% set flow-boundaries to level-boundaries (plus outlet)
model.replace_flow_boundaries_by_level_boundaries()

# basin Deurnsche Peel
model.move_node(node_id=1961, geometry=Point(192066.2, 377928.7))
model.move_node(node_id=552, geometry=Point(189926.3, 382222.4))
model.redirect_link(link_id=2065, from_node_id=255, to_node_id=1961)
model.redirect_link(link_id=2063, from_node_id=1093, to_node_id=1961)
model.reverse_direction_at_node(node_id=3092)


# Eindhovens kanaal
model.move_node(node_id=1089, geometry=Point(170038.16, 384348.73))
model.redirect_link(link_id=2073, from_node_id=1922, to_node_id=997)

# Fix Drongelens kanaal
df = gpd.read_file(model_edits_gpkg, layer="add_basin_area", fid_as_index=True)
assert model.basin.area.df is not None
model.basin.area.df.loc[model.basin.area.df.node_id == 1276, "geometry"] = df.at[154, "geometry"]
# add new basin
basin_node = model.basin.add(
    Node(geometry=cast(Point, model.outlet.node.df.loc[1067].geometry), meta_categorie="doorgaand"),
    tables=default_tables.basin,
)
model.add_basin_area(cast(MultiPolygon, df.at[155, "geometry"]), node_id=basin_node.node_id)

# fix topology
model.move_node(node_id=1067, geometry=Point(145057.6, 409049.8))
model.redirect_link(link_id=2081, from_node_id=basin_node.node_id, to_node_id=1067)
outlet_node = model.outlet.add(Node(geometry=Point(148449.6, 410398.1)), tables=default_tables.outlet)
model.link.add(outlet_node, basin_node)
model.link.add(model.level_boundary[48], outlet_node)
# nog een basin-merge i.v.m. manning
model.merge_basins(node_id=2016, to_node_id=1394)

# %% give the holes that the edits left in the basin areas to a basin
model.fill_basin_area_holes(areas_gpkg)

# %%
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model = reset_static_tables(model)
model.use_validation = True
model.validate_link_source_destination()
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %% Test run model
# result = model.run()

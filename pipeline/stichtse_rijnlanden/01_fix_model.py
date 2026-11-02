# %%
from typing import cast

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet, tabulated_rating_curve
from ribasim_nl.geometry import connected_parts, polygon_parts, split_basin_multi_polygon
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, MultiPolygon, Point
from shapely.geometry.base import BaseGeometry

from ribasim_nl import CloudStorage, Model, NetworkValidator

# Initialize cloud storage and set authority/model parameters
cloud = CloudStorage()
authority = "StichtseRijnlanden"
name = "hdsr"

# Define the path to the Ribasim model configuration file
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
verbeteringen_gpkg = cloud.joinpath("StichtseRijnlanden/verwerkt/modelfouten_met_verbeter_acties_BD_311024.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")

# %% read
model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)
split_lijnen_df = gpd.read_file(verbeteringen_gpkg, layer="area_split_lijnen", fid_as_index=True)
extra_area_df = gpd.read_file(verbeteringen_gpkg, layer="nieuwe_areas_voor_opvulling", fid_as_index=True)
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
afsluitmiddel_gdf = gpd.read_file(hydamo_gpkg, layer="afsluitmiddel", fid_as_index=True)
areas_gdf = gpd.read_file(areas_gpkg, layer="areas")

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

tabulated_rating_curve_data = tabulated_rating_curve.Static(level=[0.0, 5], flow_rate=[0, 0.1])

# HIER KOMEN ISSUES

# %%
model.remove_links(link_ids=[2677])
for node_id in [52, 53, 2056, 2053, 2042, 749]:
    model.remove_node(node_id, remove_links=True)
# the basins of LevelBoundaries 38, 52 and 53, which the lumping adds back, but the old fix script removed with their
# links first
for node_id in [2102, 2103, 2104]:
    model.remove_node(node_id, remove_links=True)

model.link.add(model.tabulated_rating_curve[924], model.level_boundary[51])
model.link.add(model.tabulated_rating_curve[937], model.level_boundary[51])
model.link.add(model.level_boundary[38], model.outlet[85])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2456843484

# basins verwijderen

# basins samenvoegen
for basin_id, to_basin_id in [
    [1990, 1445],
    [1395, 1394],
    [1459, 1394],
    [1759, 1575],
    [1798, 1836],
    [1827, 1819],
    [1936, 2028],
    [1937, 2028],
    [2029, 1383],
    [2040, 1640],
    [2043, 1812],
    [2048, 1605],
    [2076, 2088],
    [2077, 1461],
    [2080, 2089],
    [2081, 1863],
    [2082, 1423],
    [2083, 1675],
    [1678, 1671],
    [1934, 1563],
    [2074, 1469],
]:
    model.merge_basins(node_id=basin_id, to_node_id=to_basin_id)

model.merge_basins(node_id=1662, to_node_id=1745, are_connected=False)
model.update_node(
    node_id=1336,
    node_type="TabulatedRatingCurve",
    data=[tabulated_rating_curve_data],
    node_properties={"name": "ST1999", "meta_object_type": "stuw"},
)

# verwijderen knopen
for node_id in [2078, 733]:
    model.remove_node(node_id, remove_links=True)

# toevoegen link
model.reverse_link(link_id=876)
model.link.add(model.pump[565], model.level_boundary[56])

# updaten naar level_boundary
model.update_node(1965, "LevelBoundary", data=[level_data])


# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2456913415

# opheffen ST3122
geometry = Point(117995, 443062.5)
model.move_node(node_id=889, geometry=geometry)
model.node.df.loc[889, "name"] = "ST1985"

for link_id in [359, 360, 361]:
    model.redirect_link(link_id=link_id, to_node_id=1572)

assert model.basin.area.df is not None
result = split_basin_multi_polygon(
    MultiPolygon([model.basin.area[1572].geometry.union_all().buffer(0.1).buffer(-0.1)]),
    line=cast(LineString, split_lijnen_df.at[28, "geometry"]),
)
model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != 1572]
model.add_basin_area(result[1], node_id=1572)
model.add_basin_area(result[0], node_id=1914)

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457283397

# oplossen Loperiksingel/Nassaukade bij rand HHSK
# the western part of area AW0838, across the line, is of Schieland en de Krimpenerwaard
loperiksingel, schieland = split_basin_multi_polygon(
    MultiPolygon(polygon_parts(areas_gdf.loc[areas_gdf["code"] == "AW0838"].union_all())),
    line=cast(LineString, split_lijnen_df.at[1, "geometry"]),
)
boundary_node = model.level_boundary.add(Node(geometry=model.level_boundary[75].geometry), tables=[level_data])

outlet_geom, basin_geom = cast(LineString, hydroobject_gdf.at[10466, "geometry"]).boundary.geoms
model.move_node(75, geometry=basin_geom)
model.update_node(node_id=75, node_type="Basin", data=basin_data)

outlet_node = model.outlet.add(Node(geometry=outlet_geom), tables=[outlet_data])
model.link.add(model.basin[75], outlet_node)
model.link.add(outlet_node, boundary_node)
model.add_basin_area(loperiksingel, node_id=75)


# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457334403

# Toevoegen inlaat en basin bij Sportpark Het Hofland Schoonhoven
hydro_object = cast(LineString, hydroobject_gdf.at[8099, "geometry"])
afsluitmiddel = afsluitmiddel_gdf.loc[91]
outlet_node = model.outlet.add(
    Node(
        geometry=hydro_object.interpolate(hydro_object.project(cast(Point, afsluitmiddel.geometry))),
        name=afsluitmiddel.code,
        meta_object_type="afsluitmiddel",
    ),
    tables=[outlet_data],
)

basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[7950, "geometry"]).boundary.geoms[0]), tables=basin_data
)
model.redirect_link(link_id=2721, from_node_id=basin_node.node_id)
model.link.add(model.level_boundary[74], outlet_node)
model.link.add(outlet_node, basin_node)
# the area around it, which the lumping gives to a neighbouring basin
model.add_basin_area(areas_gdf.loc[areas_gdf["code"] == "AW0657"].union_all(), node_id=basin_node.node_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457447764

# Split Basin / Area met area_split_lijnen


# the pieces that split lines cut off a basin area, by basin and number, and the areas without basin yet
pieces: dict[tuple[int, int], BaseGeometry] = {}
for basin_node_id, split_lijnen_select_df in split_lijnen_df[split_lijnen_df.opmerkingen.isna()].groupby("node_id"):
    # iterate trough split_lines per basin creating a series with geometries
    basin_node_id = int(cast(float, basin_node_id))
    series = gpd.GeoSeries(
        [MultiPolygon(polygon_parts(model.basin.area[basin_node_id].geometry.union_all()))], crs=model.crs
    )

    for row in split_lijnen_select_df.itertuples():
        line = cast(LineString, row.geometry)
        if len(series[series.intersects(line)]) != 1:
            raise ValueError(f"line with fid {row.Index} intersects basin {basin_node_id} more than once")

        # select polygon
        fid = series[series.intersects(line)].index[0]
        try:
            result = split_basin_multi_polygon(series.loc[fid], line=line)
        except ValueError:
            result = split_basin_multi_polygon(series.loc[fid], line=LineString(line.boundary.geoms))

        series.loc[fid] = result[0]
        series = pd.concat([series, gpd.GeoSeries([result[1]], crs=series.crs)], ignore_index=True)

    # the piece with the basin node remains its basin area, the others have no basin yet
    model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != basin_node_id]
    with_node = series.contains(model.basin[basin_node_id].geometry)
    if with_node.any():
        model.add_basin_area(series[with_node].iloc[0], node_id=basin_node_id)
    pieces |= {(basin_node_id, k): geometry for k, geometry in enumerate(series[~with_node])}

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457597461

# Basins toevoegen met nieuwe_areas_voor_opvulling
extra_areas = extra_area_df[extra_area_df.samenvoegen_met_node_id.isna()].geometry

# Mergen van nieuwe basin / area's met de toegewezen basin
for basin_node_id, df in extra_area_df[extra_area_df.samenvoegen_met_node_id.notna()].groupby(
    "samenvoegen_met_node_id"
):
    model.add_basin_area(cast(gpd.GeoDataFrame, df).union_all(), node_id=int(cast(float, basin_node_id)))

# Vlakken zonder node mergen met de juiste vlakken mét node
pieces[(2019, 2)] = pieces[(2019, 2)].union(pieces.pop((2021, 1)))
model.add_basin_area(extra_areas.pop(1), node_id=1772)
model.add_basin_area(model.basin.area[1698].geometry.union_all(), node_id=1434)
extra_areas[17] = extra_areas[17].union(extra_areas.pop(50))
model.add_basin_area(model.basin.area[1382].geometry.union_all(), node_id=2090)

# 2 niet aansluitende vlakken toekennen aan de juiste nodes bij Nieuwegein geinoord
model.add_basin_area(areas_gdf.loc[areas_gdf["code"] == "AW1134"].union_all(), node_id=1944)
model.add_basin_area(extra_areas.pop(29), node_id=1524)

# Handmatig fixen van verkeerd toegewezen node_ids aan areas
model.add_basin_area(model.basin.area[1855].geometry.union_all(), node_id=1854)
model.add_basin_area(extra_areas.pop(14), node_id=1855)
model.add_basin_area(model.basin.area[1630].geometry.union_all(), node_id=2075)
model.add_basin_area(model.basin.area[1684].geometry.union_all(), node_id=1518)
model.remove_node(node_id=1031, remove_links=True)

# Een aantal basins mergen
model.merge_basins(node_id=2018, to_node_id=1922)
model.merge_basins(node_id=1630, to_node_id=1415, are_connected=False)
model.merge_basins(node_id=1633, to_node_id=2090, are_connected=False)
model.merge_basins(node_id=1382, to_node_id=2090, are_connected=False)


# 2x een basin area met niet aansluitende vlakken "exploden" en toekennen aan de juiste nodes (gelijk aan Nieuwegein Geinoord)
# de delen die niet aansluiten op het deel met de basin-knoop worden losse vlakken
missing_geoms = []
for basin_node_id in [1452, 1647]:
    geometry, detached = connected_parts(
        model.basin.area[basin_node_id].geometry.union_all(), model.basin[basin_node_id].geometry
    )
    model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != basin_node_id]
    model.add_basin_area(geometry, node_id=basin_node_id)
    missing_geoms += detached

# the pieces, extra areas and detached parts get the basin within or near them, as basin areas without basin first
geometries = [*pieces.values(), *extra_areas, *missing_geoms]
df = gpd.GeoDataFrame({"node_id": [None] * len(geometries)}, geometry=geometries, crs=model.crs)
df.index = pd.RangeIndex(model.basin.area.df.index.max() + 1, model.basin.area.df.index.max() + 1 + len(df), name="fid")
model.basin.area.df = pd.concat([model.basin.area.df, df])
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest")
model.fix_unassigned_basin_area()
# the ones that got a basin take their area from the other basin areas
assigned = model.basin.area.df[
    model.basin.area.df.index.isin(df.index) & model.basin.area.df.node_id.isin(model.basin.node.df.index)
]
model.basin.area.df = model.basin.area.df.drop(index=assigned.index)
for row in assigned.itertuples():
    model.add_basin_area(cast(MultiPolygon, row.geometry), node_id=int(cast(int, row.node_id)))

# bufferen met 0.1 en -0.1 om slivers op te lossen
model.basin.area.df.loc[:, "geometry"] = model.basin.area.df.buffer(0.1).buffer(-0.1)

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2459498845

# Link-richtingen omdraaien
for link_id in [
    30,
    31,
    32,
    40,
    65,
    147,
    173,
    176,
    215,
    261,
    262,
    288,
    289,
    551,
    576,
    582,
    724,
    770,
    792,
    798,
    853,
    893,
    1079,
    1085,
    1091,
    1098,
    1644,
    1708,
    2105,
    2643,
    2652,
    2659,
    2669,
    2678,
    2684,
    2563,
    1238,
    91,
    1436,
]:
    model.reverse_link(link_id=link_id)

# fix 2 incorrecte links
model.link.df.loc[916, "from_node_id"] = 1363
model.link.df.loc[1330, "from_node_id"] = 1365

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457447764

# Split Basin / Area met area_split_lijnen

# nog een aantal basins mergen
model.merge_basins(node_id=1882, to_node_id=1867)
model.merge_basins(node_id=1888, to_node_id=1867)
model.merge_basins(node_id=1676, to_node_id=1375)
model.merge_basins(node_id=1866, to_node_id=1883)
model.merge_basins(node_id=1870, to_node_id=1883)
model.merge_basins(node_id=1695, to_node_id=1800)
model.merge_basins(node_id=2011, to_node_id=2050)
model.merge_basins(node_id=2049, to_node_id=2050)
model.merge_basins(node_id=1892, to_node_id=1947)
model.merge_basins(node_id=1834, to_node_id=1947)
model.merge_basins(node_id=2010, to_node_id=1863)
model.merge_basins(node_id=1901, to_node_id=1897)
model.merge_basins(node_id=1896, to_node_id=1897)
model.merge_basins(node_id=2021, to_node_id=2051)
model.merge_basins(node_id=1721, to_node_id=2033)
model.merge_basins(node_id=2032, to_node_id=2033)
model.merge_basins(node_id=1797, to_node_id=2100)
model.merge_basins(node_id=1938, to_node_id=2047)
model.merge_basins(node_id=2033, to_node_id=1438)
model.merge_basins(node_id=1751, to_node_id=1673)
model.merge_basins(node_id=1945, to_node_id=1474)
model.merge_basins(node_id=2009, to_node_id=1960)
model.merge_basins(node_id=2041, to_node_id=1783)
model.merge_basins(node_id=2006, to_node_id=1890)
model.merge_basins(node_id=1432, to_node_id=1563)
model.merge_basins(node_id=1864, to_node_id=1386)
model.merge_basins(node_id=1928, to_node_id=1505)
model.merge_basins(node_id=1960, to_node_id=1855)
model.merge_basins(node_id=1963, to_node_id=2015)
model.merge_basins(node_id=2016, to_node_id=2015)
model.merge_basins(node_id=1915, to_node_id=1920)
model.merge_basins(node_id=1931, to_node_id=1906)
model.merge_basins(node_id=1826, to_node_id=1906)
model.merge_basins(node_id=1906, to_node_id=1388)
model.merge_basins(node_id=2046, to_node_id=1905)
model.merge_basins(node_id=1553, to_node_id=1905)
model.merge_basins(node_id=1918, to_node_id=1391)
model.merge_basins(node_id=1391, to_node_id=1532)
model.merge_basins(node_id=2045, to_node_id=1396)
model.merge_basins(node_id=1873, to_node_id=1396)
model.merge_basins(node_id=1506, to_node_id=1396)
model.merge_basins(node_id=1500, to_node_id=1396)
model.merge_basins(node_id=1911, to_node_id=1703)
model.merge_basins(node_id=1684, to_node_id=1703)
model.merge_basins(node_id=1563, to_node_id=1562)
model.merge_basins(node_id=2099, to_node_id=2012)
model.merge_basins(node_id=1828, to_node_id=2012)
model.merge_basins(node_id=1815, to_node_id=2012)
model.merge_basins(node_id=2096, to_node_id=2094)
model.merge_basins(node_id=1876, to_node_id=1812)
model.merge_basins(node_id=1664, to_node_id=2047)
model.merge_basins(node_id=1705, to_node_id=2047)
model.merge_basins(node_id=1559, to_node_id=2047)
model.merge_basins(node_id=1690, to_node_id=1698)
model.merge_basins(node_id=1780, to_node_id=1642, are_connected=False)
model.merge_basins(node_id=2023, to_node_id=1427, are_connected=False)
model.merge_basins(node_id=1400, to_node_id=1455)
model.merge_basins(node_id=1455, to_node_id=1435, are_connected=False)
model.merge_basins(node_id=1726, to_node_id=1744)
model.merge_basins(node_id=2005, to_node_id=1833, are_connected=False)
model.merge_basins(node_id=1884, to_node_id=1387, are_connected=False)
model.merge_basins(node_id=1880, to_node_id=1483, are_connected=False)
model.merge_basins(node_id=1807, to_node_id=1467)
model.merge_basins(node_id=1863, to_node_id=1860)
model.merge_basins(node_id=1860, to_node_id=2050)
model.merge_basins(node_id=1543, to_node_id=1524)
model.merge_basins(node_id=1902, to_node_id=1545)
model.merge_basins(node_id=1789, to_node_id=1792, are_connected=False)
model.merge_basins(node_id=1435, to_node_id=1436, are_connected=False)
model.merge_basins(node_id=1590, to_node_id=1786, are_connected=False)
model.merge_basins(node_id=1957, to_node_id=1591)
model.merge_basins(node_id=1529, to_node_id=1428, are_connected=False)
model.merge_basins(node_id=1587, to_node_id=1503)
model.merge_basins(node_id=1389, to_node_id=1390)
model.remove_node(node_id=687, remove_links=True)
model.remove_node(node_id=764, remove_links=True)
model.remove_node(node_id=312, remove_links=True)
model.remove_node(node_id=1051, remove_links=True)
model.remove_node(node_id=741, remove_links=True)
model.remove_node(node_id=1343, remove_links=True)
model.redirect_link(link_id=1547, from_node_id=1914)
model.redirect_link(link_id=838, to_node_id=1524)
model.merge_basins(node_id=1944, to_node_id=1523, are_connected=False)
model.remove_node(node_id=663, remove_links=True)
model.remove_node(node_id=86, remove_links=True)
model.remove_node(node_id=669, remove_links=True)
model.remove_node(node_id=737, remove_links=True)
model.merge_basins(node_id=1408, to_node_id=1672)
model.merge_basins(node_id=1425, to_node_id=1558)
model.merge_basins(node_id=1995, to_node_id=1646)
model.merge_basins(node_id=1692, to_node_id=1646)
model.merge_basins(node_id=1514, to_node_id=1577)
model.merge_basins(node_id=1522, to_node_id=1507)
model.merge_basins(node_id=1681, to_node_id=1763)
model.merge_basins(node_id=1638, to_node_id=1762)
model.merge_basins(node_id=1761, to_node_id=1724)
model.merge_basins(node_id=1724, to_node_id=1754)
model.merge_basins(node_id=1754, to_node_id=1765)
model.merge_basins(node_id=1765, to_node_id=1778)
model.merge_basins(node_id=1735, to_node_id=1778)
model.redirect_link(link_id=2272, from_node_id=1572)
model.update_node(node_id=1194, node_type="Outlet")
model.update_node(node_id=1197, node_type="Outlet")
model.update_node(node_id=678, node_type="Outlet")
model.update_node(node_id=730, node_type="Outlet")
# EINDE ISSUES


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


# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)

# %%
# buffer out small slivers

model.explode_basin_area()  # all multipolygons to singles
actions = [
    #  "remove_basin_area",
    "remove_node",
    "remove_link",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_link",
    "move_node",
    "update_node",
    "connect_basins",
    "redirect_link",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_gpkg).name.to_list()]
model.apply_edits(model_edits_gpkg, actions, sort_by_order=True)

# the pieces, extra areas and detached parts that got no basin
model.remove_unassigned_basin_area()

# %% corrigeren knoop-topologie
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")


# %%

# sanitize node-table
for node_id in model.tabulated_rating_curve.node.df.index:
    model.update_node(node_id=node_id, node_type="Outlet")

# ManningResistance that are duikersifonhevel to outlet
for node_id in model.manning_resistance.node.df[
    model.manning_resistance.node.df["meta_object_type"] == "duikersifonhevel"
].index:
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

# manning resistance must be outlet
model.update_node(node_id=1212, node_type="Outlet")  # duiker wordt outlet, was manning
model.update_node(node_id=717, node_type="Outlet")  # duiker wordt outlet, was manning

# name-column contains the code we want to keep, meta_name the name we want to have
df = pd.concat(
    [
        gpd.read_file(hydamo_gpkg, layer="gemaal"),
        gpd.read_file(hydamo_gpkg, layer="sluis"),
        gpd.read_file(hydamo_gpkg, layer="stuw"),
    ],
    ignore_index=True,
)
df.set_index("code", inplace=True)
names = df["naam"].str.title()

# set meta_gestuwd in basins
model.node.df.loc[model.node.df["node_type"] == "Basin", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Outlet", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Pump", "meta_gestuwd"] = True

model.basin.area.df[model.basin.area.df.node_id != 1423]
# set stuwen als gestuwd

# 1349 was Manning maar is duiker dus outlet
model.update_node(node_id=1349, node_type="Outlet")

model.node.df.loc[
    (model.node.df["node_type"] == "Outlet")
    & model.node.df["meta_object_type"].isin(["stuw", "afsluitmiddel", "sluis"]),
    "meta_gestuwd",
] = True

# remove_nodes
model.remove_node(node_id=482, remove_links=True)
model.remove_node(node_id=93, remove_links=True)
model.remove_node(node_id=1344, remove_links=True)
model.remove_node(node_id=469, remove_links=True)
model.remove_node(node_id=470, remove_links=True)
model.remove_node(node_id=198, remove_links=True)
model.remove_node(node_id=139, remove_links=True)
model.remove_node(node_id=173, remove_links=True)
model.remove_node(node_id=251, remove_links=True)
model.remove_node(node_id=884, remove_links=True)
model.remove_node(node_id=934, remove_links=True)
model.remove_node(node_id=748, remove_links=True)
model.remove_node(node_id=77, remove_links=True)
model.remove_node(node_id=27, remove_links=True)
model.remove_node(node_id=68, remove_links=True)

# Koekoek gaat (direct) naar Broeik (basin 2089)
model.redirect_link(link_id=68, from_node_id=100, to_node_id=2089)


# set bovenstroomse basins als gestuwd
node_df = model.node.df[model.node.df["meta_gestuwd"] & model.node.df["node_type"].isin(["Outlet", "Pump"])]

upstream_node_ids = [model.upstream_node_id(i) for i in node_df.index]
basin_node_ids = model.basin.node.df.index.intersection(upstream_node_ids)
model.node.df.loc[basin_node_ids, "meta_gestuwd"] = True

# set álle benedenstroomse outlets van gestuwde basins als gestuwd (dus ook duikers en andere objecten)
downstream_node_ids = pd.Series([model.downstream_node_id(i) for i in basin_node_ids]).explode().to_numpy()
model.node.df.loc[model.outlet.node.df.index.intersection(downstream_node_ids), "meta_gestuwd"] = True

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "LevelBoundary", "FlowBoundary", "ManningResistance"], "columns": {"name": ""}},
    ],
    names=names,
)

model.node.df.loc[186, "name"] = "Westriool"
model.node.df.loc[252, "name"] = "Inlaat Vreeswijk"


def node_id_by_code(code: str) -> int:
    """Return the ID of the only node with `meta_code_waterbeheerder` `code`."""
    node_ids = model.node.df.index[model.node.df["meta_code_waterbeheerder"] == code]
    assert len(node_ids) == 1, f"Expected one node with code {code}, got {node_ids.tolist()}"
    return int(node_ids[0])


# Open water connections between Basins of Stichtse Rijnlanden are ManningResistances, not Outlets that drain their
# upstream Basin, see https://github.com/Deltares/Ribasim-NL/issues/912. They keep their node and link IDs. Those to
# other authorities are converted after coupling, see `outlet_to_manning_resistance` in 09_koppelen_modellen.py.
OPEN_WATER_CODES = ["OW575", "OW663"]
for code in OPEN_WATER_CODES:
    model.update_node(node_id=node_id_by_code(code), node_type="ManningResistance")
# OW713 is not open water, but stuw ST1068
model.node.df.loc[node_id_by_code("OW713"), "meta_code_waterbeheerder"] = "ST1068"

# %% give the holes that the edits left in the basin areas to a basin
model.fill_basin_area_holes(areas_gpkg, excluded=schieland)

#  %% write model
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

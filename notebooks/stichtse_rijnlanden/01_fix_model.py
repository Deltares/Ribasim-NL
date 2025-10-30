# %%
import inspect

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, tabulated_rating_curve
from ribasim_nl.geometry import split_basin_multi_polygon
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, MultiPolygon, Point

from ribasim_nl import CloudStorage, Model, NetworkValidator

# Initialize cloud storage and set authority/model parameters
cloud = CloudStorage()
authority = "StichtseRijnlanden"
name = "hdsr"

# Define the path to the Ribasim model configuration file
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg")
verbeteringen_gpkg = cloud.joinpath("StichtseRijnlanden", "verwerkt", "modelfouten_met_verbeter_acties_BD_311024.gpkg")

cloud.synchronize(filepaths=[ribasim_dir, verbeteringen_gpkg, hydamo_gpkg, model_edits_gpkg])

# %% read
model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)
split_lijnen_df = gpd.read_file(verbeteringen_gpkg, layer="area_split_lijnen", fid_as_index=True)
extra_area_df = gpd.read_file(verbeteringen_gpkg, layer="nieuwe_areas_voor_opvulling", fid_as_index=True)
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
afsluitmiddel_gdf = gpd.read_file(hydamo_gpkg, layer="afsluitmiddel", fid_as_index=True)

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

tabulated_rating_curve_data = tabulated_rating_curve.Static(level=[0.0, 5], flow_rate=[0, 0.1])

# HIER KOMEN ISSUES

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2455143137

# verwijderen duplicated links
model.link.df.drop_duplicates(inplace=True)


# %%
model.remove_links(link_ids=[2677])
for node_id in [52, 53, 2056, 2053, 2042]:
    model.remove_node(node_id, remove_links=True)

model.link.add(model.tabulated_rating_curve[924], model.level_boundary[51])
model.link.add(model.tabulated_rating_curve[937], model.level_boundary[51])
model.link.add(model.level_boundary[38], model.outlet[85])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2444112017
# toevoegen ontbrekende basins

basin_links_df = network_validator.link_incorrect_connectivity()
basin_nodes_df = network_validator.node_invalid_connectivity()

for row in basin_nodes_df.itertuples():
    # maak basin-node
    basin_node = model.basin.add(Node(geometry=row.geometry), tables=basin_data)

    # update link_table
    model.link.df.loc[basin_links_df[basin_links_df.from_node_id == row.node_id].index, ["from_node_id"]] = (
        basin_node.node_id
    )
    model.link.df.loc[basin_links_df[basin_links_df.to_node_id == row.node_id].index, ["to_node_id"]] = (
        basin_node.node_id
    )

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
    model.merge_basins(basin_id=basin_id, to_basin_id=to_basin_id)

model.merge_basins(basin_id=1662, to_basin_id=1745, are_connected=False)
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
model.tabulated_rating_curve.node.df.loc[889, "name"] = "ST1985"

for link_id in [359, 360, 361]:
    model.redirect_link(link_id=link_id, to_node_id=1572)

model.basin.area.df.at[1, "geometry"].buffer(0.1).buffer(-0.1)

result = split_basin_multi_polygon(
    MultiPolygon([model.basin.area.df.at[1, "geometry"].buffer(0.1).buffer(-0.1)]),
    line=split_lijnen_df.at[28, "geometry"],
)
model.basin.area.df.loc[1, "geometry"] = result[1]
model.basin.area.df.loc[480, "geometry"] = model.basin.area.df.at[480, "geometry"].union(result[0])


# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457283397

# oplossen Loperiksingel/Nassaukade bij rand HHSK
geometry, _ = split_basin_multi_polygon(model.basin.area.df.at[135, "geometry"], line=split_lijnen_df.at[1, "geometry"])
model.basin.area.df.loc[135, "geometry"] = geometry
boundary_node = model.level_boundary.add(Node(geometry=model.level_boundary[75].geometry), tables=[level_data])

outlet_geom, basin_geom = hydroobject_gdf.at[10466, "geometry"].boundary.geoms
model.move_node(75, geometry=basin_geom)
model.update_node(node_id=75, node_type="Basin", data=basin_data)

outlet_node = model.outlet.add(Node(geometry=outlet_geom), tables=[outlet_data])
model.link.add(model.basin[75], outlet_node)
model.link.add(outlet_node, boundary_node)


# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457334403

# Toevoegen inlaat en basin bij Sportpark Het Hofland Schoonhoven
hydro_object = hydroobject_gdf.at[8099, "geometry"]
afsluitmiddel = afsluitmiddel_gdf.loc[91]
outlet_node = model.outlet.add(
    Node(
        geometry=hydro_object.interpolate(hydro_object.project(afsluitmiddel.geometry)),
        name=afsluitmiddel.code,
        meta_object_type="afsluitmiddel",
    ),
    tables=[outlet_data],
)

basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[7950, "geometry"].boundary.geoms[0]), tables=basin_data)
model.redirect_link(link_id=2721, from_node_id=basin_node.node_id)
model.link.add(model.level_boundary[74], outlet_node)
model.link.add(outlet_node, basin_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457385390

# Basin / Area Rijnsweerd mergen met Utrecht Centrum
geometry = model.basin.area.df.at[501, "geometry"]
model.basin.area.df.loc[194, "geometry"] = model.basin.area.df.at[194, "geometry"].union(geometry)
model.basin.area.df = model.basin.area.df[~model.basin.area.df.index.isin([501, 541])]

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457447764

# Split Basin / Area met area_split_lijnen


# group split_lines by basin_fid
# basin_fid, split_lijnen_select_df = list(split_lijnen_df[split_lijnen_df.opmerkingen.isna()].groupby("basin_area_fid"))[
#     0
# ]
new_basin_area_df = gpd.GeoSeries([], crs=model.crs)
for basin_fid, split_lijnen_select_df in split_lijnen_df[split_lijnen_df.opmerkingen.isna()].groupby("basin_area_fid"):
    # iterate trough split_lines per basin creating a series with geometries
    series = gpd.GeoSeries([model.basin.area.df.at[basin_fid, "geometry"]], crs=model.crs)

    for row in split_lijnen_select_df.itertuples():
        if not len(series[series.intersects(row.geometry)]) == 1:
            raise ValueError(f"line with fid {row.Index} intersects basin with fid {basin_fid} more than once")

        # select polygon
        fid = series[series.intersects(row.geometry)].index[0]
        try:
            result = split_basin_multi_polygon(series.loc[fid], line=row.geometry)
        except ValueError:
            result = split_basin_multi_polygon(series.loc[fid], line=LineString(row.geometry.boundary.geoms))

        series.loc[fid] = result[0]
        series = pd.concat([series, gpd.GeoSeries([result[1]], crs=series.crs)], ignore_index=True)

    # update existing basin / area if basin / node is within
    basin_row = model.basin.area.df.loc[basin_fid]
    drop_fid = True
    if basin_row.node_id in model.basin.node.df.index.to_numpy():  # check is valid node
        if series.contains(
            model.basin[basin_row.node_id].geometry
        ).any():  # check if any of series is contained by node-geometry
            model.basin.area.df.loc[basin_fid, "geometry"] = series[
                series.contains(model.basin[basin_row.node_id].geometry)
            ].iloc[0]
            series = series[~series.contains(model.basin[basin_row.node_id].geometry)]
            drop_fid = False

    if drop_fid:  # if we haven't updated existing record, we drop it
        model.basin.area.df = model.basin.area.df[model.basin.area.df.index != basin_fid]

    new_basin_area_df = pd.concat([new_basin_area_df, series], ignore_index=True)


# concat new_basin_area_df with existing model.basin.area.df
new_basin_area_df = gpd.GeoDataFrame(geometry=new_basin_area_df)
new_basin_area_df["node_id"] = None

new_basin_area_df.index += model.basin.area.df.index.max() + 1
new_basin_area_df.index.name = "fid"

model.basin.area.df = pd.concat([model.basin.area.df, new_basin_area_df])

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457597461

# Basins toevoegen met nieuwe_areas_voor_opvulling
new_basin_area_df = extra_area_df[extra_area_df.samenvoegen_met_basin_area_fid.isna()].reset_index()[["geometry"]]
new_basin_area_df["node_id"] = None
new_basin_area_df.index += model.basin.area.df.index.max() + 1
new_basin_area_df.index.name = "fid"
model.basin.area.df = pd.concat([model.basin.area.df, new_basin_area_df])

# Mergen van nieuwe basin / area's met de toegewezen basin / area fid
for basin_id, df in extra_area_df[extra_area_df.samenvoegen_met_basin_area_fid.notna()].groupby(
    "samenvoegen_met_basin_area_fid"
):
    model.basin.area.df.loc[int(basin_id), "geometry"] = model.basin.area.df.at[int(basin_id), "geometry"].union(
        df.union_all()
    )

# Vlakken zonder node mergen met de juiste vlakken mét node
for merge_fid, with_fid in [[591, 589], [599, 364], [300, 74], [611, 605], [28, 20]]:
    model.basin.area.df.loc[with_fid, "geometry"] = model.basin.area.df.at[with_fid, "geometry"].union(
        model.basin.area.df.at[merge_fid, "geometry"]
    )
    model.basin.area.df = model.basin.area.df[model.basin.area.df.index != merge_fid]

# 2 niet aansluitende vlakken "exploden" toekennen aan de juiste nodes bij Nieuwegein geinoord
geoms = [i for i in model.basin.area.df.at[154, "geometry"].geoms if i.area > 100]
model.basin.area.df.loc[606, "geometry"] = model.basin.area.df.at[606, "geometry"].union(geoms[0])
model.basin.area.df.at[154, "geometry"] = MultiPolygon([geoms[1]])

# Handmatig fixen van verkeerd toegewezen node_ids aan areas
model.basin.area.df.loc[9, "node_id"] = 2084
model.basin.area.df.loc[604, "node_id"] = 1855
model.basin.area.df.loc[436, "node_id"] = 1854
model.basin.area.df.loc[244, "node_id"] = 2075
model.basin.area.df.loc[289, "node_id"] = 1518
model.basin.area.df.loc[154, "node_id"] = 1944
model.remove_node(node_id=1031, remove_links=True)

# Een aantal basins mergen
model.merge_basins(basin_id=2018, to_basin_id=1922)
model.merge_basins(basin_id=1630, to_basin_id=1415, are_connected=False)
model.merge_basins(basin_id=1633, to_basin_id=2090, are_connected=False)
model.merge_basins(basin_id=1382, to_basin_id=2090, are_connected=False)


# 2x een basin area met niet aansluitende vlakken "exploden" en toekennen aan de juiste nodes (gelijk aan Nieuwegein Geinoord)
missing_geoms = []
geoms = list(model.basin.area.df.at[91, "geometry"].geoms)
model.basin.area.df.loc[91, "geometry"] = geoms[0]

missing_geoms += [geoms[1]]
geoms = list(model.basin.area.df.at[259, "geometry"].geoms)
model.basin.area.df.loc[259, "geometry"] = geoms[1]
missing_geoms += [geoms[0]]

df = gpd.GeoDataFrame(data=[], geometry=gpd.GeoSeries(missing_geoms, crs=model.crs))
df.index.name = "fid"
df.index += model.basin.area.df.index.max() + 1
model.basin.area.df = pd.concat([model.basin.area.df, df])

# automatisch geometetrisch toekennen van knoop-ids aan area op basis van geometrie (within, closest witin)
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest")
model.fix_unassigned_basin_area()

# bufferen met 0.1 en -0.1 om slivers op te lossen
model.basin.area.df.loc[:, "geometry"] = model.basin.area.df.buffer(0.1).buffer(-0.1)

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2459498845

# Edge-richtingen omdraaien
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
]:
    model.reverse_link(link_id=link_id)


# fix 2 incorrecte links
model.link.df.loc[916, "from_node_id"] = 1363
model.link.df.loc[1330, "from_node_id"] = 1365

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457447764

# Split Basin / Area met area_split_lijnen

# nog een aantal basins mergen
model.merge_basins(basin_id=1882, to_node_id=1867)
model.merge_basins(basin_id=1888, to_node_id=1867)
model.merge_basins(basin_id=1676, to_node_id=1375)
model.merge_basins(basin_id=1866, to_node_id=1883)
model.merge_basins(basin_id=1870, to_node_id=1883)
model.merge_basins(basin_id=1695, to_node_id=1800)
model.merge_basins(basin_id=2011, to_node_id=2050)
model.merge_basins(basin_id=2049, to_node_id=2050)
model.merge_basins(basin_id=1892, to_node_id=1947)
model.merge_basins(basin_id=1834, to_node_id=1947)
model.merge_basins(basin_id=2010, to_node_id=1863)
model.merge_basins(basin_id=1901, to_node_id=1897)
model.merge_basins(basin_id=1896, to_node_id=1897)
model.merge_basins(basin_id=2021, to_node_id=2051)
model.merge_basins(basin_id=1721, to_node_id=2033)
model.merge_basins(basin_id=2032, to_node_id=2033)
model.merge_basins(basin_id=1797, to_node_id=2100)
model.merge_basins(basin_id=1938, to_node_id=2047)
model.merge_basins(basin_id=2033, to_node_id=1438)
model.merge_basins(basin_id=1751, to_node_id=1673)
model.merge_basins(basin_id=1945, to_node_id=1474)
model.merge_basins(basin_id=2009, to_node_id=1960)
model.merge_basins(basin_id=2041, to_node_id=1783)
model.merge_basins(basin_id=2006, to_node_id=1890)
model.merge_basins(basin_id=1432, to_node_id=1563)
model.merge_basins(basin_id=1864, to_node_id=1386)
model.merge_basins(basin_id=1928, to_node_id=1505)
model.merge_basins(basin_id=1960, to_node_id=1855)
model.merge_basins(basin_id=1963, to_node_id=2015)
model.merge_basins(basin_id=2016, to_node_id=2015)
model.merge_basins(basin_id=1915, to_node_id=1920)
model.merge_basins(basin_id=1931, to_node_id=1906)
model.merge_basins(basin_id=1826, to_node_id=1906)
model.merge_basins(basin_id=1906, to_node_id=1388)
model.merge_basins(basin_id=2046, to_node_id=1905)
model.merge_basins(basin_id=1553, to_node_id=1905)
model.merge_basins(basin_id=1918, to_node_id=1391)
model.merge_basins(basin_id=1391, to_node_id=1532)
model.merge_basins(basin_id=2045, to_node_id=1396)
model.merge_basins(basin_id=1873, to_node_id=1396)
model.merge_basins(basin_id=1506, to_node_id=1396)
model.merge_basins(basin_id=1500, to_node_id=1396)
model.merge_basins(basin_id=1911, to_node_id=1703)
model.merge_basins(basin_id=1684, to_node_id=1703)
model.merge_basins(basin_id=1563, to_node_id=1562)
model.merge_basins(basin_id=2099, to_node_id=2012)
model.merge_basins(basin_id=1828, to_node_id=2012)
model.merge_basins(basin_id=1815, to_node_id=2012)
model.merge_basins(basin_id=2096, to_node_id=2094)
model.merge_basins(basin_id=1876, to_node_id=1812)
model.merge_basins(basin_id=1664, to_node_id=2047)
model.merge_basins(basin_id=1705, to_node_id=2047)
model.merge_basins(basin_id=1559, to_node_id=2047)
model.merge_basins(basin_id=1690, to_node_id=1698)

model.merge_basins(basin_id=1780, to_basin_id=1642, are_connected=False)
model.merge_basins(basin_id=2023, to_basin_id=1427, are_connected=False)
model.merge_basins(basin_id=1400, to_basin_id=1455)
model.merge_basins(basin_id=1455, to_basin_id=1435, are_connected=False)
model.merge_basins(basin_id=1726, to_basin_id=1744)
model.merge_basins(basin_id=2005, to_basin_id=1833, are_connected=False)
model.merge_basins(basin_id=1884, to_basin_id=1387, are_connected=False)
model.merge_basins(basin_id=1880, to_basin_id=1483, are_connected=False)
model.merge_basins(basin_id=1807, to_basin_id=1467)
model.merge_basins(basin_id=1863, to_basin_id=1860)
model.merge_basins(basin_id=1860, to_basin_id=2050)
model.merge_basins(basin_id=1543, to_basin_id=1524)
model.merge_basins(basin_id=1902, to_basin_id=1545)
model.merge_basins(basin_id=1789, to_basin_id=1792, are_connected=False)
model.merge_basins(basin_id=1435, to_basin_id=1436, are_connected=False)
model.merge_basins(basin_id=1590, to_basin_id=1786, are_connected=False)
model.merge_basins(basin_id=1957, to_basin_id=1591)
# model.merge_basins(basin_id=1528, to_basin_id=1428, are_connected=False)
model.merge_basins(basin_id=1529, to_basin_id=1428, are_connected=False)
model.merge_basins(basin_id=1587, to_basin_id=1503)
model.merge_basins(basin_id=1389, to_basin_id=1390)
model.remove_node(node_id=687, remove_links=True)
model.remove_node(node_id=764, remove_links=True)
model.remove_node(node_id=312, remove_links=True)
model.remove_node(node_id=1051, remove_links=True)
model.remove_node(node_id=741, remove_links=True)
model.remove_node(node_id=1343, remove_links=True)
model.redirect_link(link_id=1547, from_node_id=1914)
model.redirect_link(link_id=838, to_node_id=1524)
model.merge_basins(basin_id=1944, to_basin_id=1523, are_connected=False)
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
for action in gpd.list_layers(model_edits_gpkg).name:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_gpkg, layer=action, fid_as_index=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# remove unassigned basin area
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
model.basin.node.df.loc[model.basin.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"
model.outlet.node.df.loc[model.outlet.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# somehow Sluis Engelen (beheerregister AAM) has been named Henriettesluis
model.outlet.node.df.loc[model.outlet.node.df.name == "Henriëttesluis", "name"] = "AKW855"

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
model.basin.node.df["meta_gestuwd"] = False
model.outlet.node.df["meta_gestuwd"] = False
model.pump.node.df["meta_gestuwd"] = True

# set stuwen als gestuwd

model.outlet.node.df.loc[
    model.outlet.node.df["meta_object_type"].isin(["stuw", "afsluitmiddel", "sluis"]), "meta_gestuwd"
] = True

# set bovenstroomse basins als gestuwd
node_df = model.node_table().df
node_df = node_df[(node_df["meta_gestuwd"] == True) & node_df["node_type"].isin(["Outlet", "Pump"])]  # noqa: E712

upstream_node_ids = [model.upstream_node_id(i) for i in node_df.index]
basin_mask = model.basin.node.df.index.isin(upstream_node_ids)
model.basin.node.df.loc[basin_mask, "meta_gestuwd"] = True

# set álle benedenstroomse outlets van gestuwde basins als gestuwd (dus ook duikers en andere objecten)
downstream_node_ids = (
    pd.Series([model.downstream_node_id(i) for i in model.basin.node.df[basin_mask].index]).explode().to_numpy()
)
model.outlet.node.df.loc[model.outlet.node.df.index.isin(downstream_node_ids), "meta_gestuwd"] = True

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "LevelBoundary", "FlowBoundary", "ManningResistance"], "columns": {"name": ""}},
    ],
    names=names,
)


#  %% write model
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%
# result = model.run()
# assert result == 0

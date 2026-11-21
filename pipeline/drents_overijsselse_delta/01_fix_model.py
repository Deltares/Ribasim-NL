# %%
from typing import cast

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet
from ribasim_nl.geometry import polygon_parts, split_basin_multi_polygon
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, MultiPolygon, Point

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "DrentsOverijsselseDelta"
name = "dod"
run_model = False

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
hydroobject_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
duikersifonhevel_gpkg = cloud.joinpath(authority, "aangeleverd/Aanlevering_202311/HyDAMO_WM_20231117.gpkg")


model_edits_path = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
fix_user_data_path = cloud.joinpath(authority, "verwerkt/fix_user_data.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")

# the areas of the NW boezem, the basin of LevelBoundary 28, which the lumping gives to its neighbours
NW_BOEZEM = [f"peilgebiedpraktijk.{i}" for i in (252, 580, 1245, 1355, 1437, 1451)]
# the areas of the basins that this script adds, or that share their area with basin 2576, which it removes, by the
# key of the basin
BASIN_AREAS = {
    "bnd:83": [259, 1146, 1243],
    "up:ow:hydroobject.208565": [
        *(198, 199, 200, 202, 428, 794, 857, 870, 922, 1352, 1381, 1396, 1397, 1398, 1422, 1461, 1477, 1478, 1507),
        *(1552, 1553, 1575, 1661, 1662, 1934, 1936),
    ],
    "loc:184436,536338": [1134],
    "loc:204572,507737": [1602],
    "loc:192828,514468": [1196, 1464],
}

duikersifonhevel_gdf = gpd.read_file(
    duikersifonhevel_gpkg,
    fid_as_index=True,
    layer="duikersifonhevel",
)

split_line_gdf = gpd.read_file(fix_user_data_path, layer="split_basins", fid_as_index=True)
hydroobject_gdf = gpd.read_file(hydroobject_gpkg, layer="hydroobject", fid_as_index=True)


# %% read model
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

network_validator = NetworkValidator(model)

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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393458802
model.remove_node(n("bnd:7"), remove_links=True)
model.remove_node(n("bnd:84"), remove_links=True)
# the basin of LevelBoundary 7, which the lumping adds back, but the old fix script removed with its link first
model.remove_node(n("up:bnd:7"))

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393672367

# Omdraaien link-richting rondom outlets (inlaten/uitlaten)
# for link_id in [2282, ]

# https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393731749

# Opruimen Reeve

# basin 2484 wordt LevelBoundary (IJssel)
model.update_node(n("up:duikersifonhevel.280076"), "LevelBoundary", data=[level_data])

# nodes 1536, 762, 761, 1486 + aangesloten links gooien we weg
for node_id in [
    n("ow:hydroobject.208565"),
    n("duikersifonhevel.280196"),
    n("duikersifonhevel.280171"),
    n("ow:hydroobject.208556"),
]:
    model.remove_node(node_id, remove_links=True)

# links 2841, 2842, 2843, 2846 gooien we weg
model.remove_links(
    [
        lk("bnd:10", "duikersifonhevel.280149"),
        lk("bnd:10", "duikersifonhevel.280076"),
        lk("bnd:10", "stuw.43708"),
        lk("bnd:10", "gemaal.20228"),
    ]
)

# duiker 286309 voegen we toe
kdu = duikersifonhevel_gdf.loc[9063]
outlet_node = model.outlet.add(
    Node(
        geometry=cast(Point, kdu.geometry.interpolate(0.5, normalized=True)),
        name=f"duikersifonhevel.{kdu.objectid}",
        meta_key=f"duikersifonhevel.{kdu.objectid}",
    ),
    tables=[outlet_data],
)

model.link.add(model.level_boundary[n("bnd:10")], outlet_node)
model.link.add(outlet_node, model.basin[n("up:ow:hydroobject.208565")])
model.link.add(model.manning_resistance[n("duikersifonhevel.280149")], model.basin[n("up:ow:hydroobject.208565")])
model.link.add(model.manning_resistance[n("duikersifonhevel.280076")], model.basin[n("up:ow:hydroobject.208565")])
model.link.add(model.tabulated_rating_curve[n("stuw.43708")], model.basin[n("up:ow:hydroobject.208565")])
model.link.add(model.basin[n("up:ow:hydroobject.208565")], model.pump[n("gemaal.20228")])

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393871075

# Ramsgeul bij Ramspol
for node_id in [n("bnd:81"), n("duikersifonhevel.281075")]:
    model.remove_node(node_id, remove_links=True)

model.update_node(n("bnd:83"), "Basin", data=basin_data)
model.move_node(n("bnd:83"), cast(LineString, hydroobject_gdf.at[21045, "geometry"]).boundary.geoms[0])
model.reverse_link(link_id=lk("bnd:83", "duikersifonhevel.628820"))

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399104407

# Deelstroomgebied Frysland

# Nieuwe Kanaal / Tussen Linde voorzien van basin nabij gemaal.20 (node_id 701)
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[19671, "geometry"]).boundary.geoms[1]), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[19671, "geometry"]).interpolate(0.5, normalized=True)),
    tables=[outlet_data],
)

# basin 1623 verbinden met inlaatduikers (3x) en gemaal 1623; overige verbindingen verwijderen.
model.remove_links(
    [
        lk("bnd:94", "duikersifonhevel.750731"),
        lk("bnd:94", "gemaal.20"),
        lk("bnd:94", "duikersifonhevel.11344"),
        lk("bnd:94", "sluis.2284"),
        lk("bnd:94", "duikersifonhevel.11327"),
    ]
)

# nw basin verbinden met gemaal 20, level boundary 94 en alle inlaatduikers
model.reverse_link(link_id=lk("up:duikersifonhevel.11344", "duikersifonhevel.11344"))
model.link.add(outlet_node, model.level_boundary[n("bnd:94")])
model.link.add(basin_node, outlet_node)
model.link.add(basin_node, model.manning_resistance[n("duikersifonhevel.11344")])
model.link.add(basin_node, model.manning_resistance[n("duikersifonhevel.11327")])
model.link.add(basin_node, model.manning_resistance[n("duikersifonhevel.750731")])
model.link.add(basin_node, model.outlet[n("sluis.2284")])
model.link.add(model.pump[n("gemaal.20")], basin_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399209787

# Aansluiten NW boezem op Fryslan

assert model.basin.area.df is not None
model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != n("up:duikersifonhevel.11301")]

# basin 1682 te veranderen in een LevelBoundary
model.update_node(n("up:duikersifonhevel.12121"), "LevelBoundary", [level_data])

# Alle links die nu naar basin 1717 lopen naar LevelBoundary 1682 of opheffen
model.remove_node(n("bnd:27"), remove_links=True)
model.remove_node(n("ow:hydroobject.591335"), remove_links=True)
model.remove_links(
    [
        lk("gemaal.49", "up:duikersifonhevel.11301"),
        lk("up:duikersifonhevel.11301", "duikersifonhevel.777931"),
        lk("up:duikersifonhevel.11301", "duikersifonhevel.777932"),
    ]
)

boundary_node = model.level_boundary.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[7778, "geometry"]).boundary.geoms[0]), tables=[level_data]
)

model.link.add(model.pump[n("gemaal.49")], boundary_node)
model.update_node(n("duikersifonhevel.777932"), "Outlet", data=[outlet_data])
model.link.add(boundary_node, model.outlet[n("duikersifonhevel.777932")])
model.update_node(n("duikersifonhevel.777931"), "Outlet", data=[outlet_data])
model.link.add(boundary_node, model.outlet[n("duikersifonhevel.777931")])

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399328441

# Misc pump benedenstroomse links
for link_id in [lk("bnd:26", "gemaal.119"), lk("bnd:77", "gemaal.11421"), lk("bnd:100", "gemaal.11446")]:
    model.reverse_link(link_id=link_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399355028

# Misc tabulated_rating_curve (stuwen) stroomrichting
for link_id in [lk("up:duikersifonhevel.12017", "stuw.491"), lk("up:gemaal.43", "stuw.32341")]:
    model.reverse_link(link_id=link_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399382478

# Misc manning_resistance (duikers) stroomrichting
for link_id in [lk("duikersifonhevel.284491", "up:stuw.43152"), lk("duikersifonhevel.24595", "up:stuw.1837")]:
    model.reverse_link(link_id=link_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399425885
# Opknippen NW boezem

areas_gdf = gpd.read_file(areas_gpkg, layer="areas")
nw_boezem = MultiPolygon(polygon_parts(areas_gdf[areas_gdf["code"].isin(NW_BOEZEM)].union_all()))
poly1, poly2 = split_basin_multi_polygon(nw_boezem, cast(LineString, split_line_gdf.at[15, "geometry"]))
model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != n("up:duikersifonhevel.11454")]
model.add_basin_area(poly1, node_id=n("up:duikersifonhevel.11454"))

poly1, poly2 = split_basin_multi_polygon(poly2, cast(LineString, split_line_gdf.at[16, "geometry"]))
model.add_basin_area(poly1, node_id=n("up:duikersifonhevel.26145"))

poly1, poly2 = split_basin_multi_polygon(poly2, cast(LineString, split_line_gdf.at[17, "geometry"]))
model.add_basin_area(poly1, node_id=n("up:duikersifonhevel.12017"))

basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[19608, "geometry"]).boundary.geoms[1]), tables=basin_data
)
model.add_basin_area(poly2, node_id=basin_node.node_id)


model.move_node(
    n("up:duikersifonhevel.26145"), cast(LineString, hydroobject_gdf.at[19566, "geometry"]).boundary.geoms[1]
)
model.merge_basins(
    node_id=n("up:duikersifonhevel.138060"), to_node_id=n("up:ow:hydroobject.470044"), are_connected=True
)
model.merge_basins(node_id=n("up:duikersifonhevel.26602"), to_node_id=n("up:ow:hydroobject.470044"), are_connected=True)
model.merge_basins(node_id=n("up:ow:hydroobject.13028"), to_node_id=n("up:ow:hydroobject.470044"), are_connected=True)

model.merge_basins(node_id=n("up:ow:hydroobject.470044"), to_node_id=n("up:stuw.108316"), are_connected=True)

model.merge_basins(node_id=n("up:stuw.108316"), to_node_id=n("up:duikersifonhevel.26145"), are_connected=True)
model.merge_basins(node_id=n("up:ow:hydroobject.5808"), to_node_id=n("up:duikersifonhevel.26145"), are_connected=True)
model.merge_basins(node_id=n("up:ow:hydroobject.5824"), to_node_id=n("up:duikersifonhevel.26145"), are_connected=True)

model.remove_node(n("ow:hydroobject.470053"), remove_links=True)
model.remove_node(n("duikersifonhevel.25667"), remove_links=True)
model.remove_node(n("sluis.1974"), remove_links=True)
model.remove_node(n("up:sluis.1974"), remove_links=True)

link_ids = [
    lk("gemaal.39", "bnd:28"),
    lk("gemaal.24", "bnd:28"),
    lk("gemaal.40", "bnd:28"),
    lk("gemaal.33", "bnd:28"),
    lk("gemaal.34", "bnd:28"),
    lk("gemaal.50", "bnd:28"),
    lk("gemaal.32", "bnd:28"),
    lk("gemaal.23", "bnd:28"),
    lk("sluis.2285", "bnd:28"),
    lk("sluis.1644", "bnd:28"),
    lk("duikersifonhevel.11542", "bnd:28"),
    lk("duikersifonhevel.11798", "bnd:28"),
    lk("ow:hydroobject.497881", "bnd:28"),
    lk("ow:hydroobject.5957", "bnd:28"),
    lk("ow:hydroobject.369637", "bnd:28"),
    lk("hydroobject.556796___6", "bnd:28"),
    lk("gemaal.25", "bnd:28"),
    lk("gemaal.18", "bnd:28"),
    lk("duikersifonhevel.784030", "bnd:28"),
    lk("duikersifonhevel.78490", "bnd:28"),
    lk("duikersifonhevel.25453", "bnd:28"),
    lk("hydroobject.371175___4", "bnd:28"),
    lk("ow:hydroobject.553267", "bnd:28"),
    lk("hydroobject.550059", "bnd:28"),
    lk("duikersifonhevel.26796", "bnd:28"),
    lk("gemaal.44", "bnd:28"),
    lk("ow:hydroobject.369633", "bnd:28"),
    lk("ow:hydroobject.6023", "bnd:28"),
    lk("ow:hydroobject.6594", "bnd:28"),
    lk("ow:hydroobject.42170", "bnd:28"),
    lk("gemaal.21", "bnd:28"),
    lk("duikersifonhevel.758412", "bnd:28"),
    lk("duikersifonhevel.25251", "bnd:28"),
    lk("duikersifonhevel.757772", "bnd:28"),
    lk("duikersifonhevel.761611", "bnd:28"),
    lk("ow:hydroobject.427482", "bnd:28"),
    lk("hydroobject.370891___2", "bnd:28"),
    lk("ow:hydroobject.556791", "bnd:28"),
]

for link_id in link_ids:
    model.redirect_link(link_id, to_node_id=basin_node.node_id)

model.remove_links([lk("gemaal.19", "bnd:28")])
model.link.add(basin_node, model.pump[n("gemaal.19")])
model.link.add(basin_node, model.outlet[n("sluis.1973")])

for link_id in [lk("duikersifonhevel.27146", "bnd:28"), lk("duikersifonhevel.784347", "bnd:28")]:
    model.redirect_link(link_id, to_node_id=n("bnd:31"))

model.redirect_link(lk("sluis.3335", "up:duikersifonhevel.284886"), to_node_id=n("up:ow:hydroobject.204441"))

model.basin.area.df.loc[model.basin.area.df.node_id == n("hydroobject.10168___6"), ["node_id"]] = n(
    "up:ow:hydroobject.204441"
)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2400050483

# Ontbrekende basin beneden-Vecht
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[12057, "geometry"]).boundary.geoms[0]), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[12057, "geometry"]).interpolate(0.5, normalized=True)),
    tables=[outlet_data],
)

for link_id in [
    lk("duikersifonhevel.778260", "bnd:50"),
    lk("ow:hydroobject.215609", "bnd:50"),
    lk("gemaal.19590", "bnd:50"),
    lk("sluis.3328", "bnd:50"),
    lk("sluis.3326", "bnd:50"),
    lk("stuw.50984", "bnd:50"),
]:
    model.redirect_link(link_id, to_node_id=basin_node.node_id)

model.remove_node(n("bnd:76"), remove_links=True)
model.link.add(basin_node, model.pump[n("gemaal.11491")])
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[n("bnd:50")])


# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399931763

# Samenvoegen Westerveldse Aa
model.merge_basins(
    node_id=n("up:ow:hydroobject.295916"), to_node_id=n("up:duikersifonhevel.1016527"), are_connected=True
)
model.merge_basins(
    node_id=n("up:duikersifonhevel.483866"), to_node_id=n("up:duikersifonhevel.1016527"), are_connected=True
)
model.merge_basins(
    node_id=n("up:duikersifonhevel.1016527"), to_node_id=n("up:ow:hydroobject.204441"), are_connected=True
)
model.merge_basins(
    node_id=n("up:duikersifonhevel.286203"), to_node_id=n("up:ow:hydroobject.204441"), are_connected=True
)
model.merge_basins(
    node_id=n("up:duikersifonhevel.278830"), to_node_id=n("up:ow:hydroobject.204441"), are_connected=True
)
model.merge_basins(
    node_id=n("up:ow:hydroobject.291103@2"), to_node_id=n("up:ow:hydroobject.204441"), are_connected=True
)
model.merge_basins(node_id=n("up:stuw.92921"), to_node_id=n("up:ow:hydroobject.204441"), are_connected=True)
model.merge_basins(
    node_id=n("up:duikersifonhevel.278829"), to_node_id=n("up:ow:hydroobject.204441"), are_connected=True
)
model.merge_basins(node_id=n("up:gemaal.11450"), to_node_id=n("up:ow:hydroobject.204441"), are_connected=True)
model.merge_basins(node_id=n("up:duikersifonhevel.425643"), to_node_id=n("up:sluis.3335"), are_connected=False)


# %%
# corrigeren knoop-topologie

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")

# %%
model.explode_basin_area()  # all multipolygons to singles

# update from layers
actions = [
    "remove_basin_area",
    "split_basin",
    "merge_basins",
    "add_basin",
    "update_node",
    "add_basin_area",
    "update_basin_area",
    "deactivate_node",
    "move_node",
    "redirect_link",
    "reverse_link",
    "remove_node",
    "connect_basins",
]

actions = [i for i in actions if i in gpd.list_layers(model_edits_path).name.to_list()]
model.apply_edits(model_edits_path, actions, sort_by_order=True)

# fixes:
# Gemaal Westerveld is een inlaat gemaal en een uitlaat ernaast, dus richting omdraaien evt takken toevoegen
for link_id in [
    lk("duikersifonhevel.424568", "up:ow:hydroobject.204441"),
    lk("gemaal.11450", "bnd:51"),
    lk("bnd:51", "duikersifonhevel.424568"),
    lk("up:ow:hydroobject.204441", "gemaal.11450"),
]:
    model.reverse_link(link_id=link_id)

# De Heuvel # 617 verkeerde riching, dus omdraaien
for link_id in [lk("gemaal.11533", "up:duikersifonhevel.276958"), lk("up:gemaal.11533", "gemaal.11533")]:
    model.reverse_link(link_id=link_id)

# Outlet Zedemuden moet kunnen inlaten, dus richting omdraaien evt takken toevoegen
# the edits add this Pump: it gets its code as key here, to refer to it
model.node.df.loc[model.find_node_id(name="gemaal.27"), "meta_key"] = "gemaal.27"
for link_id in [
    lk("bnd:56", "gemaal.27"),
    lk("gemaal.27", "loc:197542,525242"),
]:
    model.reverse_link(link_id=link_id)

# node #968 aanvoer
for link_id in [lk("up:stuw.67288", "duikersifonhevel.624652"), lk("duikersifonhevel.624652", "bnd:4")]:
    model.reverse_link(link_id=link_id)

# node #832 aanvoer
for link_id in [lk("up:gemaal.11428", "duikersifonhevel.425596"), lk("duikersifonhevel.425596", "bnd:3")]:
    model.reverse_link(link_id=link_id)

# Reest gevoed door Ommerskanaal (Lutterhoofddiep 1m3/s)
for link_id in [lk("up:ow:hydroobject.13578", "stuw.2053"), lk("stuw.2053", "bnd:18")]:
    model.reverse_link(link_id=link_id)

# De Haar is aanvoer
for link_id in [lk("gemaal.11396", "up:duikersifonhevel.285046"), lk("up:gemaal.11396", "gemaal.11396")]:
    model.reverse_link(link_id=link_id)

# Outlets richting omdraaien, zijn inlaten!
for link_id in [
    lk("gemaal.96", "up:duikersifonhevel.25661"),
    lk("up:duikersifonhevel.26651", "gemaal.96"),
    lk("stuw.2095", "up:duikersifonhevel.23536"),
    lk("up:stuw.2095", "stuw.2095"),
    lk("duikersifonhevel.20543", "up:stuw.2095"),
    lk("up:duikersifonhevel.20543", "duikersifonhevel.20543"),
    lk("duikersifonhevel.18971", "up:duikersifonhevel.18968"),
    lk("up:duikersifonhevel.18971", "duikersifonhevel.18971"),
]:
    model.reverse_link(link_id=link_id)

# Blijdestein moet ook aanvoeren
# Outlets richting omdraaien, zijn inlaten!
for link_id in [lk("gemaal.101", "up:duikersifonhevel.134542"), lk("up:gemaal.100", "gemaal.101")]:
    model.reverse_link(link_id=link_id)

# Outlet 1011 is een inlaat
# Outlet richting omdraaien, zijn inlaten!
for link_id in [
    lk("duikersifonhevel.12004", "up:duikersifonhevel.12017"),
    lk("up:duikersifonhevel.11958", "duikersifonhevel.12004"),
]:
    model.reverse_link(link_id=link_id)

# Duinersteeg aantal Slagen
model.update_node(node_id=n("ow:hydroobject.5957"), node_type="Outlet")
# Outlets richting omdraaien, zijn inlaten!
for link_id in [
    lk("up:duikersifonhevel.27120", "ow:hydroobject.5957"),
    lk("ow:hydroobject.5957", "loc:197542,525242"),
]:
    model.reverse_link(link_id=link_id)

# Outlet Tussen de Diepen node #1035 richting omdraaien, is inlaat
for link_id in [
    lk("up:duikersifonhevel.758412", "duikersifonhevel.758412"),
    lk("duikersifonhevel.758412", "loc:197542,525242"),
]:
    model.reverse_link(link_id=link_id)

# Outlet Lokbrug node #1081 richting omdraaien, is inlaat
for link_id in [
    lk("duikersifonhevel.24655", "up:gemaal.21189"),
    lk("up:duikersifonhevel.24655", "duikersifonhevel.24655"),
]:
    model.reverse_link(link_id=link_id)

# Outlet Haarsluis node #1338 richting omdraaien, is inlaat
for link_id in [
    lk("duikersifonhevel.13492", "up:duikersifonhevel.13343"),
    lk("up:duikersifonhevel.13372", "duikersifonhevel.13492"),
]:
    model.reverse_link(link_id=link_id)

# Outlet O-36-16_A_2 node #1242 richting omdraaien, is inlaat
for link_id in [
    lk("up:duikersifonhevel.25225", "duikersifonhevel.21511"),
    lk("up:duikersifonhevel.21511", "duikersifonhevel.21511"),
]:
    model.reverse_link(link_id=link_id)

# Outlet node #952 richting omdraaien, is inlaat
for link_id in [
    lk("duikersifonhevel.24624", "up:stuw.1788"),
    lk("up:duikersifonhevel.24624", "duikersifonhevel.24624"),
]:
    model.reverse_link(link_id=link_id)

# Outlet node #1242 richting omdraaien, is inlaat
for link_id in [lk("duikersifonhevel.21511", "up:duikersifonhevel.21511")]:
    model.reverse_link(link_id=link_id)

# Outlet node #1227 richting omdraaien, is inlaat
for link_id in [
    lk("duikersifonhevel.24648", "up:duikersifonhevel.13280"),
    lk("up:duikersifonhevel.22221", "duikersifonhevel.24648"),
]:
    model.reverse_link(link_id=link_id)

# Outlet node #984 richting omdraaien, is inlaat
for link_id in [
    lk("duikersifonhevel.24646", "up:duikersifonhevel.754571"),
    lk("up:duikersifonhevel.24646", "duikersifonhevel.24646"),
]:
    model.reverse_link(link_id=link_id)

# 't Vosje #571 is een inlaat
for link_id in [lk("gemaal.11438", "bnd:2"), lk("up:gemaal.11438", "gemaal.11438")]:
    model.reverse_link(link_id=link_id)

# 't Katje #562 is een inlaat
for link_id in [lk("up:gemaal.11421", "gemaal.11421"), lk("gemaal.11421", "bnd:77")]:
    model.reverse_link(link_id=link_id)

# 't Raasje #563 is een inlaat
for link_id in [lk("gemaal.11424", "down:gemaal.11424"), lk("up:gemaal.11421", "gemaal.11424")]:
    model.reverse_link(link_id=link_id)

# make outlets from manning
outlet_ids = [n("duikersifonhevel.285706")]

for node_id in dict.fromkeys(outlet_ids):
    model.update_node(node_id=node_id, node_type="ManningResistance")

# fixes vistrap eruit
model.remove_node(n("ow:hydroobject.576937"), remove_links=True)
model.remove_node(n("ow:hydroobject.215609"), remove_links=True)

model.update_node(node_id=n("ow:hydroobject.497881"), node_type="Outlet")
model.reverse_link(link_id=lk("duikersifonhevel.20901", "up:gemaal.21190"))
model.reverse_link(link_id=lk("up:duikersifonhevel.20901", "duikersifonhevel.20901"))

model.merge_basins(node_id=n("up:duikersifonhevel.24652"), to_node_id=n("up:stuw.1754"), are_connected=False)
model.merge_basins(node_id=n("up:duikersifonhevel.78810"), to_node_id=n("up:gemaal.8491"), are_connected=False)
model.merge_basins(node_id=n("up:duikersifonhevel.482746"), to_node_id=n("up:gemaal.11472"), are_connected=True)
model.merge_basins(node_id=n("bnd:56"), to_node_id=n("bnd:59"), are_connected=True)
model.merge_basins(
    node_id=n("up:duikersifonhevel.11454"), to_node_id=n("up:duikersifonhevel.11301"), are_connected=True
)
model.merge_basins(node_id=n("up:duikersifonhevel.25600"), to_node_id=n("up:KOR-03481"), are_connected=False)
model.merge_basins(
    node_id=n("down:duikersifonhevel.582382"), to_node_id=n("up:duikersifonhevel.276709"), are_connected=False
)
model.merge_basins(node_id=n("up:duikersifonhevel.285705"), to_node_id=n("up:stuw.43107"), are_connected=True)
model.merge_basins(node_id=n("up:duikersifonhevel.1000048"), to_node_id=n("up:stuw.24023"), are_connected=True)

model.remove_link(from_node_id=n("stuw.43135"), to_node_id=n("up:duikersifonhevel.276575"))

for key, numbers in BASIN_AREAS.items():
    codes = [f"peilgebiedpraktijk.{i}" for i in numbers]
    model.add_basin_area(areas_gdf[areas_gdf["code"].isin(codes)].union_all(), node_id=n(key))


model = reset_static_tables(model)


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

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(authority=authority, layers=["gemaal", "stuw", "sluis"])
df.loc[:, "code"] = df.nen3610id.str.removeprefix("NL.WBHCODE.59.").str.lower()
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


# %% set flow-boundaries to level-boundaries (plus outlet)
model.replace_flow_boundaries_by_level_boundaries()


# %% give the holes that the edits left in the basin areas to a basin
model.fill_basin_area_holes(areas_gpkg)

#  %% write model
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.validate_link_source_destination()
model.report_basin_area()
model.report_internal_basins()
# %% Test run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

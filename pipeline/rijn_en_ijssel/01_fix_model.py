# %%
from typing import cast

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet
from ribasim_nl.geometry import line_between
from ribasim_nl.model import DEFAULT_TABLES
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, Point

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "RijnenIJssel"
name = "wrij"
run_model = False
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
gsk3e_gpkg = cloud.joinpath("Basisgegevens/GSK3E/gsk3e.gpkg")

DIE_WILD_BASIN_AREA_BUFFER = 25.0  # half the width of the strip along Die Wild [m]

# %% read model
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

network_validator = NetworkValidator(model)

hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
duiker_gdf = gpd.read_file(hydamo_gpkg, layer="duikersifonhevel", fid_as_index=True)

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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419649171
# update link administratie

model.link.df.loc[lk("ow:LV74120074", "up:ST74140001"), "from_node_id"] = n("ow:LV74120074")
model.link.df.loc[lk("ow:LV74120067", "up:ST74140001"), "from_node_id"] = n("ow:LV74120067")
model.link.df.loc[lk("up:DR24140008", "DR24140008"), "to_node_id"] = n("DR24140008")
model.link.df.loc[lk("up:ow:LV54520078", "ow:LV54520078"), "to_node_id"] = n("ow:LV54520078")
model.link.df.loc[lk("up:ow:LV74120067", "ow:LV74120067"), "to_node_id"] = n("ow:LV74120067")

# %% see: https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2419747636

# fix link_richting

# verplaatsen van `LevelBoundary` 47 binnen de basin, updaten naar `Basin` en reversen van `Link` 1370
model.move_node(n("bnd:47"), cast(LineString, hydroobject_gdf.at[8781, "geometry"]).boundary.geoms[0])
model.update_node(n("bnd:47"), "Basin", data=basin_data)
model.reverse_link(link_id=lk("ST48230014", "bnd:47"))

# omdraaien richting van `Link` 196
for link_id in [
    lk("ST74350252", "up:ST74320107"),
    lk("ST74330026", "up:ST74450065"),
    lk("AF4180001", "up:DR74160023"),
    lk("DR32110100", "up:DR32110077"),
    lk("DR24230047", "up:ST24230096"),
    lk("GM96370013", "up:ST96410001"),
    lk("ST24310001", "up:ST24230096"),
]:
    model.reverse_link(link_id=link_id)

# opruimen basin Arnhem nabij Lauwersgracht
model.remove_node(n("ST16120018"), remove_links=True)
model.remove_node(n("up:ST16120018"), remove_links=True)
model.remove_links([lk("GM16120052", "bnd:43"), lk("ST16120011", "bnd:43")])

kdu = duiker_gdf.loc[548]
outlet_node = model.outlet.add(
    Node(
        name=kdu.code,
        geometry=cast(Point, kdu.geometry.interpolate(0.5, normalized=True)),
        meta_object_type="duikersifonhevel",
    ),
    tables=[outlet_data],
)
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[9528, "geometry"]).boundary.geoms[0]), tables=basin_data
)
model.link.add(model.tabulated_rating_curve[n("ST16120011")], basin_node)
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[n("bnd:43")])
model.link.add(basin_node, model.pump[n("GM16120052")])
model.link.add(model.pump[n("GM16120052")], model.level_boundary[n("bnd:44")])

# %% see https://github.com/Deltares/Ribasim-NL/issues/151#issuecomment-2422536079

# corrigeren ontbrekende outlets nabij Rijkswateren
for fid, link_id, boundary_node_id in (
    (14276, lk("up:bnd:19", "bnd:19"), n("bnd:19")),
    (14259, lk("up:bnd:25", "bnd:25"), n("bnd:25")),
    (14683, lk("up:bnd:27", "bnd:27"), n("bnd:27")),
    (3294, lk("up:bnd:38", "bnd:38"), n("bnd:38")),
):
    kdu = duiker_gdf.loc[fid]
    outlet_node = model.outlet.add(
        Node(
            name=kdu.code,
            geometry=cast(Point, kdu.geometry.interpolate(0.5, normalized=True)),
            meta_object_type="duikersifonhevel",
        ),
        tables=[outlet_data],
    )
    model.redirect_link(link_id=link_id, to_node_id=outlet_node.node_id)
    model.link.add(outlet_node, model.level_boundary[boundary_node_id])

# 1349 heeft geen duiker
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[10080, "geometry"]).interpolate(0.5, normalized=True)),
    tables=[outlet_data],
)
model.redirect_link(link_id=lk("up:bnd:33", "bnd:33"), to_node_id=outlet_node.node_id)
model.link.add(outlet_node, model.level_boundary[n("bnd:33")])

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
actions = [
    "remove_node",
    "remove_link",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_link",
    "update_node",
    "move_node",
    "connect_basins",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_gpkg).name.to_list()]
model.apply_edits(model_edits_gpkg, actions)


# %% merge basins
model.merge_basins(node_id=n("down:DR84650048"), to_node_id=n("up:ST84650033"))
model.merge_basins(node_id=n("up:DR96430054"), to_node_id=n("up:DR96540001"))

# %% connect 's Heerenberg (286) with the Oude Rijn (1036) along Die Wild in Germany, instead of over two boundaries
# see https://github.com/Deltares/Ribasim-NL/issues/992
# LevelBoundary 837 and FlowBoundary 49 are this waterway, so they become Basins. Between them is the Hoher Absturz, and
# downstream of them a ManningResistance.
die_wild_gdf = gpd.read_file(gsk3e_gpkg, layer="waterways", where="GEWKZ3E = '279982' AND ABSNAME = 'Die Wild'")
assert len(die_wild_gdf) == 1
die_wild = cast(LineString, die_wild_gdf.geometry.iloc[0].reverse())  # GSK3E draws waterways in upstream direction
hoher_absturz = cast(Point, gpd.GeoSeries([Point(307305.41, 5750171.41)], crs="EPSG:25832").to_crs(model.crs).iloc[0])
model.update_node(node_id=n("down:ST08410055"), node_type="Basin", data=basin_data)
model.update_node(node_id=n("bnd:49"), node_type="Basin", data=basin_data)
model.node.df.loc[[n("down:ST08410055"), n("bnd:49")], "meta_categorie"] = pd.NA
outlet_node = model.outlet.add(
    Node(
        name="qbw_9638",
        geometry=hoher_absturz,
        meta_object_type="stuw",
    ),
    tables=[outlet_data],
)
model.remove_links([lk("bnd:49", "up:ST09210004")])
manning_node = model.manning_resistance.add(
    Node(
        geometry=die_wild.interpolate(
            (
                die_wild.project(model.basin[n("bnd:49")].geometry)
                + die_wild.project(model.basin[n("up:ST09210004")].geometry)
            )
            / 2
        ),
        meta_categorie="hoofdwater",
    ),
    tables=DEFAULT_TABLES.manning_resistance,
)
for from_node, to_node in (
    (model.basin[n("down:ST08410055")], outlet_node),
    (outlet_node, model.basin[n("bnd:49")]),
    (model.basin[n("bnd:49")], manning_node),
    (manning_node, model.basin[n("up:ST09210004")]),
):
    model.link.add(from_node, to_node, geometry=line_between(die_wild, from_node.geometry, to_node.geometry))

# Germany has no peilgebieden: their basin areas are a strip along Die Wild, outside of the existing basin areas
assert model.basin.area.df is not None
existing_areas = model.basin.area.df.union_all()
for node_id, reach in (
    (n("down:ST08410055"), line_between(die_wild, model.outlet[n("ST08410055")].geometry, outlet_node.geometry)),
    (n("bnd:49"), line_between(die_wild, outlet_node.geometry, manning_node.geometry)),
):
    model.add_basin_area(reach.buffer(DIE_WILD_BASIN_AREA_BUFFER).difference(existing_areas), node_id=node_id)

# %%
# Sanitize node_table
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
model.node.df.loc[model.pump.node.df.index[model.pump.node.df["meta_categorie"].isna()], "meta_categorie"] = (
    "hoofdwater"
)

# opruimen rondom inlaat schipbeek
for node_id in [
    n("ST96410005"),
    n("ST96410007"),
    n("ST96410008"),
    n("ST96410009"),
]:  # we hebben echt maar 1 sifon nodig
    model.remove_node(node_id, remove_links=True)

model.reverse_direction_at_node(node_id=n("GM96370013"))  # inlaat schipbeek is verkeerd-om getekend
model.merge_basins(
    node_id=n("up:ST96410001"), to_node_id=n("up:AF9637001")
)  # zandvang weg, zodat we gemakkelijker kunnen inlaten bij Boven-Regge

# name-column contains the code we want to keep, meta_name the name we want to have
df = pd.concat(
    [
        gpd.read_file(hydamo_gpkg, layer="sluis"),
        gpd.read_file(hydamo_gpkg, layer="gemaal"),
        gpd.read_file(hydamo_gpkg, layer="stuw"),
    ],
    ignore_index=True,
)
df = df.set_index("code")
names = df["naam"]
names.loc["qbw_9638"] = "Hoher Absturz"

# set meta_gestuwd in basins
model.node.df.loc[model.node.df["node_type"] == "Basin", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Outlet", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Pump", "meta_gestuwd"] = True

# set stuwen als gestuwd

model.node.df.loc[
    (model.node.df["node_type"] == "Outlet") & model.node.df["meta_object_type"].isin(["stuw"]),
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

# edits n.a.v. review control (script 4)
model.reverse_direction_at_node(node_id=n("DR96820021"))  # gebied Schipbeek: dit moet een uitlaat zijn
model.remove_node(node_id=n("ST80740037"), remove_links=True)  # gebied Lochem: vistrap naast verdeelwerk
model.remove_node(node_id=n("ST80360094"), remove_links=True)  # rest: Afvoer deelgebied (zonnepanelen?)
model.remove_node(
    node_id=n("DR74160023"), remove_links=True
)  # rest: Extra afvoer (verkeerd-om getekend), maar hoofdafvoer gaat via Kievekampsbrug (388)
model.remove_node(
    node_id=n("ST74220175"), remove_links=True
)  # rest: Direct aan secundair gebied, hoofdafvoer loopt via 423
model.remove_node(
    node_id=n("GM09220001"), remove_links=True
)  # rest: Secundair gebied, hoofdtak loopt over 564 (Erfkamerlingschap)
model.remove_node(
    node_id=n("ST84820026"), remove_links=True
)  # rest: Secundair gebied, hoofdtak loopt over 121 (eefde aflaatwerk)
model.remove_node(node_id=n("GM24310001"), remove_links=True)  # rest: Gemaal Dunoweg voor nu eruit
model.remove_node(
    node_id=n("LV74620106"), remove_links=True
)  # rode bollen: zorgt voor lek, secundaire manning waterloop in gestuwd gebied
model.merge_basins(
    node_id=n("up:ST80770033"), to_node_id=n("up:LV80770026")
)  # rode bollen: juist streefpeil bij aflaatwerk nabij inlaat
model.remove_node(
    node_id=n("ST80190047"), remove_links=True
)  # rode bollen: 323 kan worden opgeheven, zodat afwatering via 1679 loopt
model.remove_node(
    node_id=n("ST54970005"), remove_links=True
)  # rode bollen: 387 kan worden opgeheven, is een stuw in een kwelsloot met een hoger peil

# Doesburg is overdadig geschematiseerd, inclusief vispassage
for node_id in [n("Af54960001"), n("AF54960002"), n("ST54960004")]:
    model.remove_node(node_id=node_id, remove_links=True)

# Verdeelwerk Koppelleiding:
model.move_node(node_id=n("LV80320076___1"), geometry=Point(248797.688, 445732.748))
model.update_node(node_id=n("LV80320076___1"), node_type="Outlet")
sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "ManningResistance", "LevelBoundary"], "columns": {"name": ""}},
        {"node_types": ["FlowBoundary"], "columns": {"meta_name": "name"}},
    ],
    names=names,
)

# %%
model.node.df.loc[model.flow_boundary.node.df.index, "meta_categorie"] = "buitenlandse aanvoer"

# %% give the holes that the edits left in the basin areas to a basin
model.fill_basin_area_holes(areas_gpkg)

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%
if run_model:
    result = model.run()
    assert result.exit_code == 0

# %%
from typing import cast

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, MultiPolygon, Point

from ribasim_nl import CloudStorage, Model, NetworkValidator

# Initialize cloud storage and set authority/model parameters

cloud = CloudStorage()

authority = "Limburg"
name = "limburg"
run_model = False

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")

# the areas of the basins that this script adds, by node ID, which the lumping gives to their neighbours
basin_areas: dict[int, list[str]] = {}

# %% read model

model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id
network_validator = NetworkValidator(model)

hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
duiker_gdf = gpd.read_file(hydamo_gpkg, layer="duikersifonhevel", fid_as_index=True)
basin_node_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="unassigned_basin_node")
rename_basin_area_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="rename_basin_area")
add_basin_area_gdf = gpd.read_file(model_edits_gpkg, layer="add_basin_area")
connect_basins_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="connect_basins")
reverse_link_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="reverse_link")
add_basin_outlet_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="add_basin_outlet")
remove_node_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="remove_node")
# update_node_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="update_node")
# merge_basins_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="merge_basins")


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

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426118811

# Verwijderen link met 0m lengte
model.remove_node(n("down:loc:189487,338737"), remove_links=True)
model.remove_node(n("loc:189487,338737"), remove_links=True)
model.merge_basins(node_id=n("up:loc:189487,338737"), to_node_id=n("up:S_226716"), are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426151899

# Corrigeren ontbrekende basins en outlets nabij modelrand
# an outlet between the basins at the model boundary, which the lumping adds back, and their LevelBoundaries, with the
# node IDs they got when this script added these basins too
# geen kdu bij 2202
geometry = cast(LineString, hydroobject_gdf.at[3099, "geometry"])
basin_id = n("up:bnd:37")
outlet_node = model.outlet.add(Node(node_id=basin_id + 1, geometry=geometry.interpolate(271)), tables=[outlet_data])
model.redirect_link(link_id=lk("up:bnd:37", "bnd:37"), from_node_id=outlet_node.node_id)
model.link.add(model.basin[basin_id], outlet_node)
model.move_node(basin_id, geometry.boundary.geoms[0])

for basin_id, fid, link_id in (
    (n("up:bnd:63"), 2054, lk("up:bnd:63", "bnd:63")),
    (n("up:bnd:103"), 9794, lk("up:bnd:103", "bnd:103")),
    (n("up:bnd:105"), 9260, lk("up:bnd:105", "bnd:105")),
    (n("up:bnd:113"), 3307, lk("up:bnd:113", "bnd:113")),
):
    kdu = duiker_gdf.loc[fid]
    outlet_node = model.outlet.add(
        Node(
            node_id=basin_id + 1,
            name=kdu.code,
            geometry=cast(Point, kdu.geometry.interpolate(0.5, normalized=True)),
            meta_object_type="duikersifonhevel",
            meta_key=kdu.code,
        ),
        tables=[outlet_data],
    )
    model.redirect_link(link_id=link_id, from_node_id=outlet_node.node_id)
    model.link.add(model.basin[basin_id], outlet_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426258242

# Corrigeren netwerk bij Jeker
for node_id in [n("S_96056"), n("up:W_261303_0___2"), n("W_261303_0___2"), n("up:W_261299_0"), n("W_261299_0")]:
    model.remove_node(node_id, remove_links=True)

# the link from this LevelBoundary ends at a node that is not in the model, so it has no pair of keys
(link_id,) = model.link.df.index[model.link.df.from_node_id == n("bnd:74")]
basin_node = model.basin.add(
    Node(geometry=cast(LineString, model.link.df.at[link_id, "geometry"]).boundary.geoms[1]), tables=basin_data
)
basin_areas[basin_node.node_id] = [f"AG_JEK_JEKjeker_BN_{i}_LAT" for i in (20, 22, 34, 35)]
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[2099, "geometry"]).interpolate(0.9, normalized=True)),
    tables=[outlet_data],
)
model.redirect_link(link_id=link_id, to_node_id=basin_node.node_id)
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[n("bnd:82")])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426373368

# Corrigeren Snelle Loop bij Defensiekanaal
# the link from this LevelBoundary ends at a node that is not in the model, so it has no pair of keys
(link_id,) = model.link.df.index[model.link.df.from_node_id == n("bnd:144")]
snelle_loop_outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, model.link.df.at[link_id, "geometry"]).boundary.geoms[1]), tables=[outlet_data]
)
model.redirect_link(link_id=link_id, to_node_id=snelle_loop_outlet_node.node_id)
model.link.add(snelle_loop_outlet_node, model.basin[n("up:S_181151")])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426401489

# Corrigeren Panheelsebeek
model.remove_node(node_id=n("W_242792_0"), remove_links=True)
model.reverse_link(link_id=lk("S_98126", "up:W_242792_0"))
model.merge_basins(node_id=n("up:W_242792_0"), to_node_id=n("up:W_2627_0___0"), are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426443778

# Toevoegen Wellse Molenbeek
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[3100, "geometry"]).boundary.geoms[1]), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[687, "geometry"]).interpolate(0.9, normalized=True)),
    tables=[outlet_data],
)

model.redirect_link(link_id=lk("D_25036", "bnd:59"), to_node_id=basin_node.node_id)
model.redirect_link(link_id=lk("D_25040", "bnd:59"), from_node_id=basin_node.node_id, to_node_id=outlet_node.node_id)
model.link.add(basin_node, model.manning_resistance[n("D_25040")])
model.link.add(outlet_node, model.level_boundary[n("bnd:59")])


# %%
model.remove_node(node_id=n("W_2248_2"), remove_links=True)
kdu = duiker_gdf.loc[4664]
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[477, "geometry"]).boundary.geoms[0]), tables=basin_data
)
basin_areas[basin_node.node_id] = [
    f"AG_WGN_{i}" for i in (305, 306, 307, 308, 309, 466, 467, 468, 469, 571, 572, 579, 704, 705, 706)
] + [f"AG_WGN_{i}" for i in range(807, 814)]
outlet_node = model.outlet.add(
    Node(
        name=kdu.code,
        geometry=cast(Point, kdu.geometry.interpolate(0.5, normalized=True)),
        meta_object_type="duikersifonhevel",
        meta_key=kdu.code,
    ),
    tables=[outlet_data],
)
model.link.add(outlet_node, model.basin[n("up:D_25263")])
model.link.add(basin_node, outlet_node)

kdu = duiker_gdf.loc[3709]
outlet_node = model.outlet.add(
    Node(
        name=kdu.code,
        geometry=cast(Point, kdu.geometry.interpolate(0.5, normalized=True)),
        meta_object_type="duikersifonhevel",
        meta_key=kdu.code,
    ),
    tables=[outlet_data],
)

model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[n("bnd:39")])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426653554

# Correctie Panheelderbeek bij kanaal Wessem-Nederweert
model.remove_links(
    link_ids=[
        lk("S_98126", "bnd:115"),
        lk("S_98126", "bnd:114"),
        lk("ow:W_224499_0", "bnd:114"),
        lk("ow:W_245631_1", "bnd:114"),
        lk("ow:W_309566_0", "bnd:114"),
        lk("ow:W_309566_0", "bnd:115"),
        lk("ow:W_224499_0", "bnd:115"),
        lk("ow:W_245631_1", "bnd:115"),
    ]
)
model.remove_node(n("bnd:114"), remove_links=True)
model.reverse_link(link_id=lk("up:W_2086_0___3", "ow:W_309566_0"))
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[1649, "geometry"]).boundary.geoms[1]), tables=basin_data
)
basin_areas[basin_node.node_id] = [f"AG_HUT_{i}" for i in (68, 89, 106, 107, 108, 109, 110, 240)]
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[4110, "geometry"]).boundary.geoms[1]), tables=[outlet_data]
)

model.link.add(model.tabulated_rating_curve[n("S_98126")], basin_node)
model.link.add(basin_node, model.manning_resistance[n("ow:W_309566_0")])
model.link.add(basin_node, model.manning_resistance[n("ow:W_224499_0")])
model.link.add(basin_node, model.manning_resistance[n("ow:W_245631_1")])
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[n("bnd:115")])

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426706167

# Correctie link-richting bij Ijsselsteinseweg

model.reverse_link(link_id=lk("bnd:125", "S_98039"))

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426763136

# Opname Helenavaart
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[112, "geometry"]).boundary.geoms[0]), tables=basin_data
)
model.redirect_link(link_id=lk("G_273038", "bnd:123"), to_node_id=basin_node.node_id)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[112, "geometry"]).interpolate(0.9, normalized=True)),
    tables=[outlet_data],
)
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[n("bnd:123")])

outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[1702, "geometry"]).interpolate(0.1, normalized=True)),
    tables=[outlet_data],
)
model.redirect_link(link_id=lk("bnd:122", "G_273038"), to_node_id=outlet_node.node_id)
model.link.add(outlet_node, basin_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426789675

# Opname Oude Helenavaart/kanaal van Deurne
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[1565, "geometry"]).boundary.geoms[0]), tables=basin_data
)
basin_areas[basin_node.node_id] = [
    f"AG_EKBAWK_{i}" for i in (512, 1861, 4539, 6428, 6638, 6639, 6640, 6641, 6643, 6644)
]
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[3277, "geometry"]).interpolate(0.9, normalized=True)),
    tables=[outlet_data],
)

model.redirect_link(link_id=lk("S_282001", "bnd:121"), from_node_id=outlet_node.node_id)
model.link.add(basin_node, outlet_node)

outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[1565, "geometry"]).interpolate(0.98, normalized=True)),
    tables=[outlet_data],
)
model.redirect_link(link_id=lk("S_282001", "bnd:120"), from_node_id=outlet_node.node_id)
model.link.add(basin_node, outlet_node)

model.redirect_link(link_id=lk("W_2306_0", "bnd:121"), to_node_id=basin_node.node_id)
model.redirect_link(link_id=lk("D_18761", "bnd:121"), to_node_id=basin_node.node_id)

model.link.add(model.tabulated_rating_curve[n("S_282001")], basin_node)

# 2 links die afwateren op Oude Helenavaart
model.remove_links(link_ids=[lk("D_18761", "bnd:120"), lk("W_2306_0", "bnd:120")])


# %% https://github.com/Deltares/Ribasim-NL/issues/154#issuecomment-2426816843

# Verwijderen afwaterende basisn Mooks kanaal
model.remove_node(n("bnd:34"), remove_links=True)

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
areas_gdf = gpd.read_file(areas_gpkg, layer="areas")
for node_id, codes in basin_areas.items():
    model.add_basin_area(areas_gdf[areas_gdf["code"].isin(codes)].union_all(), node_id=node_id)
# %%
# rename node-ids, by geometry, since the fids of the basin areas in the input model can change
for row in rename_basin_area_gdf.itertuples():
    model.update_basin_area(node_id=n(cast(str, row.to_node_key)), geometry=cast(MultiPolygon, row.geometry))

# %%add basin area
for row in add_basin_area_gdf.itertuples():
    model.add_basin_area(node_id=n(cast(str, row.node_key)), geometry=row.geometry)

# %% merge_basins

# merge basins
selection_df = basin_node_edits_gdf[basin_node_edits_gdf["to_node_key"].notna()]
for row in selection_df.fillna({"connected": True}).itertuples():
    model.merge_basins(
        node_id=n(cast(str, row.node_key)), to_node_id=n(cast(str, row.to_node_key)), are_connected=row.connected
    )

# %% reverse links

# reverse links
for row in reverse_link_gdf.itertuples():
    model.reverse_link(link_id=lk(cast(str, row.link_from_key), cast(str, row.link_to_key)))


# %% remove nodes
for row in remove_node_gdf.itertuples():
    model.remove_node(node_id=n(cast(str, row.node_key)), remove_links=row.remove_links)


# %% update nodes
# for row in update_node_gdf.itertuples():
#     model.update_node(
#         node_id=row.node_id,
#         node_type=row.node_type,
#         node_properties={"meta_code_waterbeheerder": row.meta_code_waterbeheerder},
#     )

# %% merge_basins
# for row in merge_basins_gdf.itertuples():
#     model.merge_basins(node_id=row.node_id, to_node_id=row.to_node_id, are_connected=row.are_connected)

# %% change node_type

# change node type
selection_df = basin_node_edits_gdf[basin_node_edits_gdf["change_node_type"].notna()]
for row in basin_node_edits_gdf[basin_node_edits_gdf["change_node_type"].notna()].itertuples():
    if row.change_node_type:
        model.update_node(node_id=n(cast(str, row.node_key)), node_type=row.change_node_type)
# %% extrad fixes
outlet_ids = [
    n("D_19576"),
    n("D_21900"),
    n("W_153150_0___5"),
    n("W_3108_0"),
    n("W_2886_0___2"),
    n("W_153162_0___4"),
    n("W_242507_0___4"),
    n("W_1399_0___0"),
    n("ow:W_2397_0"),
    n("W_2032_0___10"),
    n("ow:W_293737_0"),
    n("ow:W_184016_0"),
    n("W_161636_0___0"),
    n("ow:W_3135_0"),
    n("W_1818_0___2"),
    n("W_2133_0___1"),
    n("W_242596_0___6"),
    n("W_242613_0___3"),
    n("W_242594_0___0"),
    n("ow:W_1791_0"),
    n("W_2425_1___1"),
    n("ow:W_2266_1"),
    n("ow:W_243068_0"),
    n("ow:W_310345_0"),
    n("W_3233_0"),
    n("ow:W_153283_0"),
    n("ow:W_2312_0"),
    n("ow:W_309377_0"),
    n("W_2543_0___6"),
    n("ow:W_308185_0"),
    n("ow:W_243078_0"),
    n("W_2296_2___12"),
    n("W_243029_0___8"),
    n("W_243034_0"),
    n("W_2098_0"),
    n("W_153227_0___18"),
    n("ow:W_243020_0"),
    n("ow:W_243459_0"),
    n("ow:W_243763_0"),
    n("W_243723_0"),
    n("W_2618_0"),
    n("W_2371_0___2"),
    n("W_2370_0___1"),
    n("ow:W_3220_0"),
    n("ow:W_3003_0"),
    n("W_3003_1___0"),
    n("W_3003_2___0"),
    n("W_2867_0___4"),
    n("W_2682_0___1"),
    n("W_2987_0___6"),
    n("W_243697_0___3"),
    n("W_2680_0___0"),
    n("ow:W_303911_0"),
    n("W_153245_0___3"),
    n("W_3056_0"),
    n("W_242728_0___2"),
    n("D_13674"),
    n("W_242731_0___7"),
    n("W_1486_1"),
    n("ow:W_281079_0"),
    n("W_2548_0___3"),
    n("ow:W_242194_0"),
    n("W_2952_0___0"),
    n("W_243656_0___5"),
    n("W_243490_0___5"),
    n("ow:W_242202_0"),
    n("W_2016_5___0"),
    n("W_2198_0___11"),
    n("W_243048_0___1"),
    n("ow:W_2187_2"),
    n("ow:W_243044_2@1"),
    n("ow:W_243044_2@2"),
    n("ow:W_243047_2"),
    n("W_2636_0___11"),
    n("W_242351_0___1"),
    n("W_292386_0___4"),
    n("W_243653_0___0"),
    n("W_2952_0___9"),
    n("ow:W_3216_0"),
    n("W_3092_1___0"),
    n("W_2568_0___0"),
    n("W_2296_6"),
    n("W_242318_0___2"),
    n("W_308819_0___4"),
    n("W_168768_0"),
    n("W_242300_0___2"),
    n("ow:W_242120_0"),
]

for node_id in dict.fromkeys(outlet_ids):
    model.update_node(node_id=node_id, node_type="Outlet")

merge_pairs = [
    (n("up:D_18598"), n("up:W_242003_2___0")),
    (n("up:ow:W_1993_0"), n("up:ow:W_242726_0")),
    (n("up:ow:W_242726_0"), n("up:W_242727_0")),
    (n("up:W_242728_0___2"), n("up:W_242727_0")),
    (n("up:ow:W_242730_0"), n("up:W_242731_0___7")),
    (n("up:W_243318_0___1"), n("up:S_98903")),
    (n("up:ow:W_1456_0"), n("up:D_20541")),
    (n("up:W_242603_0___2"), n("up:S_97517")),
    (n("up:W_241973_0___1"), n("up:G_270436")),
    (n("up:S_98254"), n("up:S_98908")),
    (n("up:W_161636_0___0"), n("up:S_206856")),
    (n("up:W_243432_0___7"), n("up:D_23026")),
]

for basin_id, to_basin_id in merge_pairs:
    model.merge_basins(
        node_id=basin_id,
        to_node_id=to_basin_id,
        are_connected=True,
    )

model.remove_node(n("W_311233_0"), remove_links=True)
model.remove_node(n("S_167551"), remove_links=True)
model.remove_node(n("W_216235_0"), remove_links=True)
model.remove_node(n("W_160977_0"), remove_links=True)
model.remove_node(n("bnd:127"), remove_links=True)  # remove FlowBoundary connected to LevelBoundary at Tungelroyse beek

# %% remove nodes

# remove nodes
for node_key in basin_node_edits_gdf[basin_node_edits_gdf["remove_node_key"].notna()].node_key:
    model.remove_node(node_id=n(node_key), remove_links=True)

# %% add and connect basins

# add and connect basins
for row in connect_basins_gdf.itertuples():
    from_basin_id = n(cast(str, row.node_key))
    to_basin_id = n(cast(str, row.to_node_key))
    if row.add_object == "duikersifonhevel":
        node_type = "TabulatedRatingCurve"
    model.add_and_connect_node(
        from_basin_id, to_basin_id, geometry=row.geometry, node_type=node_type, name=row.add_object_name
    )

# %% add basin outlet

# add basin outlet
for row in add_basin_outlet_gdf.itertuples():
    basin_id = n(cast(str, row.node_key))
    if row.add_object == "duikersifonhevel":
        node_type = "TabulatedRatingCurve"
    model.add_basin_outlet(basin_id, geometry=row.geometry, node_type=node_type, name=row.add_object_name)


# %% corrigeren knoop-topologie
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")

# %% verwijderen i.v.m koppelen met Aa en Maas
model.remove_node(node_id=n("bnd:144"), remove_links=True)
model.remove_node(node_id=snelle_loop_outlet_node.node_id, remove_links=True)

# %%
# Sanitize node_table
for node_id in model.tabulated_rating_curve.node.df.index:
    model.update_node(node_id=node_id, node_type="Outlet")

# ManningResistance that are duikersifonhevel to outlet
for node_id in model.manning_resistance.node.df[
    model.manning_resistance.node.df["meta_object_type"] == "duikersifonhevel"
].index:
    model.update_node(node_id=node_id, node_type="Outlet")

# nodes we've added do not have category, we fill with hoofdwater
model.node.df.loc[model.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# name-column contains the code we want to keep, meta_name the name we want to have
df = pd.concat(
    [
        gpd.read_file(hydamo_gpkg, layer="duikersifonhevel"),
        gpd.read_file(hydamo_gpkg, layer="gemaal"),
        gpd.read_file(hydamo_gpkg, layer="stuw"),
    ],
    ignore_index=True,
)
df.set_index("code", inplace=True)
names = df["naam"]

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "ManningResistance", "LevelBoundary"], "columns": {"name": ""}},
        {"node_types": ["FlowBoundary"], "columns": {"meta_name": "name"}},
    ],
    names=names,
)

# %%

# set buitenlandse aanvoer
model.node.df.loc[model.flow_boundary.node.df.index, "meta_categorie"] = "buitenlandse aanvoer"

# %%

# set meta_gestuwd in basins
model.node.df.loc[model.node.df["node_type"] == "Basin", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Outlet", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Pump", "meta_gestuwd"] = True

node_ids = model.node.df[
    model.node.df["meta_code_waterbeheerder"].str.startswith("S_")
    | model.node.df["meta_code_waterbeheerder"].str.startswith("P_")
].index

upstream_node_ids = [model.upstream_node_id(i) for i in node_ids]

basin_node_ids = model.basin.node.df.index[model.basin.node.df.index.isin(upstream_node_ids)]
model.node.df.loc[model.node.df.index.isin(basin_node_ids), "meta_gestuwd"] = True

downstream_node_ids = pd.Series([model.downstream_node_id(i) for i in basin_node_ids]).explode().to_numpy()
model.node.df.loc[
    (model.node.df["node_type"] == "Outlet") & model.node.df.index.isin(downstream_node_ids),
    "meta_gestuwd",
] = True


# %% merge issues
model.move_node(node_id=n("up:W_2982_0___3"), geometry=Point(196405.5, 396944.8))
model.move_node(node_id=n("W_2982_0___3"), geometry=Point(196895.8, 397237.0))
model.redirect_link(
    link_id=lk("ow:W_243102_0", "bnd:6"), from_node_id=n("ow:W_243102_0"), to_node_id=n("up:W_2982_0___3")
)
model.redirect_link(
    link_id=lk("W_242731_0___25", "bnd:6"), from_node_id=n("W_242731_0___25"), to_node_id=n("up:W_2982_0___3")
)

# %% performance bottlenecks
model.merge_basins(node_id=n("up:S_98662"), to_node_id=n("up:ow:W_242141_0"))

# Zijtak sluit niet aan op Vlootbeek, verlegd en redirect
model.move_node(node_id=n("W_244621_0___16"), geometry=Point(193403.3, 352653.3))
model.redirect_link(link_id=lk("W_261466_0___3", "bnd:42"), to_node_id=n("up:S_96174"))

# Merge bij Niers
model.merge_basins(node_id=n("up:ow:W_1477_0"), to_node_id=n("up:ow:W_242161_0"))


# %% give the holes that the edits left in the basin areas to a basin
model.fill_basin_area_holes(areas_gpkg)

#  %% write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.validate_link_source_destination()
model.report_basin_area()
model.report_internal_basins()

# %% Test run model
if run_model:
    result = model.run()
    assert result.exit_code == 0
# %%

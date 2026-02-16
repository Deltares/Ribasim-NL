# %%
import inspect

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl.geometry import split_basin_multi_polygon, split_line
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.model import default_tables
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "DrentsOverijsselseDelta"
name = "dod"

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
hydroobject_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
duikersifonhevel_gpkg = cloud.joinpath(authority, "aangeleverd/Aanlevering_202311/HyDAMO_WM_20231117.gpkg")


model_edits_path = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
fix_user_data_path = cloud.joinpath(authority, "verwerkt/fix_user_data.gpkg")

cloud.synchronize(
    filepaths=[ribasim_dir, hydroobject_gpkg, duikersifonhevel_gpkg, model_edits_path, fix_user_data_path]
)

duikersifonhevel_gdf = gpd.read_file(
    duikersifonhevel_gpkg,
    fid_as_index=True,
    layer="duikersifonhevel",
)

split_line_gdf = gpd.read_file(fix_user_data_path, layer="split_basins", fid_as_index=True)
hydroobject_gdf = gpd.read_file(hydroobject_gpkg, layer="hydroobject", fid_as_index=True)


# %% read model
model = Model.read(ribasim_toml)

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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393424844
# Verwijderen duplicate links

model.link.df.drop_duplicates(inplace=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393458802

# Toevoegen ontbrekende basins (oplossen topologie)
model.remove_node(7, remove_links=True)
model.remove_node(84, remove_links=True)
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

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393672367

# Omdraaien link-richting rondom outlets (inlaten/uitlaten)
# for link_id in [2282, ]

# https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393731749

# Opruimen Reeve

# basin 2484 wordt LevelBoundary (IJssel)
model.update_node(2484, "LevelBoundary", data=[level_data])

# nodes 1536, 762, 761, 1486 + aangesloten links gooien we weg
for node_id in [1536, 762, 761, 1486]:
    model.remove_node(node_id, remove_links=True)

# links 2841, 2842, 2843, 2846 gooien we weg
model.remove_links([2841, 2842, 2843, 2846])

# duiker 286309 voegen we toe
kdu = duikersifonhevel_gdf.loc[9063]
outlet_node = model.outlet.add(
    Node(geometry=kdu.geometry.interpolate(0.5, normalized=True), name=f"duikersifonhevel.{kdu.objectid}"),
    tables=[outlet_data],
)

model.link.add(model.level_boundary[10], outlet_node)
model.link.add(outlet_node, model.basin[2240])
model.link.add(model.manning_resistance[849], model.basin[2240])
model.link.add(model.manning_resistance[760], model.basin[2240])
model.link.add(model.tabulated_rating_curve[187], model.basin[2240])
model.link.add(model.basin[2240], model.pump[1100])

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393871075

# Ramsgeul bij Ramspol
for node_id in [81, 839]:
    model.remove_node(node_id, remove_links=True)

model.update_node(83, "Basin", data=basin_data)
model.move_node(83, hydroobject_gdf.at[21045, "geometry"].boundary.geoms[0])
model.reverse_link(link_id=3013)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399104407

# Deelstroomgebied Frysland

# Nieuwe Kanaal / Tussen Linde voorzien van basin nabij gemaal.20 (node_id 701)
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[19671, "geometry"].boundary.geoms[1]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[19671, "geometry"].interpolate(0.5, normalized=True)),
    tables=[outlet_data],
)

# basin 1623 verbinden met inlaatduikers (3x) en gemaal 1623; overige verbindingen verwijderen.
model.remove_links([3038, 3040, 3037, 3041, 3039])

# nw basin verbinden met gemaal 20, level boundary 94 en alle inlaatduikers
model.reverse_link(link_id=2282)
model.link.add(outlet_node, model.level_boundary[94])
model.link.add(basin_node, outlet_node)
model.link.add(basin_node, model.manning_resistance[1182])
model.link.add(basin_node, model.manning_resistance[969])
model.link.add(basin_node, model.manning_resistance[1050])
model.link.add(basin_node, model.outlet[539])
model.link.add(model.pump[701], basin_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399209787

# Aansluiten NW boezem op Fryslan

# basin /area 1681 op te knippen nabij basin 1717 (rode lijn)
model.split_basin(geometry=split_line_gdf.at[14, "geometry"])
model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != 1717]

# basin 1682 te veranderen in een LevelBoundary
model.update_node(1682, "LevelBoundary", [level_data])

# Alle links die nu naar basin 1717 lopen naar LevelBoundary 1682 of opheffen
model.remove_node(27, remove_links=True)
model.remove_node(1556, remove_links=True)
model.remove_links([945, 2537, 2536])

boundary_node = model.level_boundary.add(Node(geometry=hydroobject_gdf.at[7778, "geometry"].boundary.geoms[0]))

model.link.add(model.pump[642], boundary_node)
model.update_node(1202, "Outlet", data=[outlet_data])
model.link.add(boundary_node, model.outlet[1202])
model.update_node(1203, "Outlet", data=[outlet_data])
model.link.add(boundary_node, model.outlet[1203])

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399328441

# Misc pump benedenstroomse links
for link_id in [2862, 3006, 3049]:
    model.reverse_link(link_id=link_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399355028

# Misc tabulated_rating_curve (stuwen) stroomrichting
for link_id in [1884, 2197]:
    model.reverse_link(link_id=link_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399382478

# Misc manning_resistance (duikers) stroomrichting
for link_id in [1081, 518]:
    model.reverse_link(link_id=link_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399425885
# Opknippen NW boezem

poly1, poly2 = split_basin_multi_polygon(model.basin.area.df.at[598, "geometry"], split_line_gdf.at[15, "geometry"])
model.basin.area.df.loc[model.basin.area.df.node_id == 1681, ["geometry"]] = poly1

poly1, poly2 = split_basin_multi_polygon(poly2, split_line_gdf.at[16, "geometry"])
model.basin.area.df.loc[598] = {"node_id": 1686, "geometry": poly1}

poly1, poly2 = split_basin_multi_polygon(poly2, split_line_gdf.at[17, "geometry"])
model.basin.area.df.loc[model.basin.area.df.index.max() + 1] = {"geometry": poly1, "node_id": 1695}
model.basin.area.df.crs = model.crs

tables = [*basin_data, basin.Area(geometry=[poly2])]
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[19608, "geometry"].boundary.geoms[1]), tables=tables)


model.move_node(1686, hydroobject_gdf.at[19566, "geometry"].boundary.geoms[1])
model.merge_basins(basin_id=2426, to_basin_id=1696, are_connected=True)
model.merge_basins(basin_id=2460, to_basin_id=1696, are_connected=True)
model.merge_basins(basin_id=1648, to_basin_id=1696, are_connected=True)

model.merge_basins(basin_id=1696, to_basin_id=2453, are_connected=True)

model.merge_basins(basin_id=2453, to_basin_id=1686, are_connected=True)
model.merge_basins(basin_id=1719, to_basin_id=1686, are_connected=True)
model.merge_basins(basin_id=1858, to_basin_id=1686, are_connected=True)

model.remove_node(1532, remove_links=True)
model.remove_node(722, remove_links=True)
model.remove_node(536, remove_links=True)
model.remove_node(2506, remove_links=True)

link_ids = [
    2866,
    2867,
    2868,
    2869,
    2870,
    2871,
    2872,
    2873,
    2875,
    2876,
    2877,
    2878,
    2879,
    2880,
    2881,
    2883,
    2885,
    2886,
    2889,
    2890,
    2891,
    2895,
    2897,
    2899,
    2901,
    2902,
    2903,
    2905,
    2906,
    2907,
    2908,
    2910,
    2911,
    2912,
    2913,
    2915,
    2916,
    2918,
]

for link_id in link_ids:
    model.redirect_link(link_id, to_node_id=basin_node.node_id)

model.remove_links([2887, 2892])
model.link.add(basin_node, model.pump[547])
model.link.add(basin_node, model.outlet[540])

for link_id in [2914, 2894]:
    model.redirect_link(link_id, to_node_id=31)

model.redirect_link(461, to_node_id=1585)

model.basin.area.df.loc[model.basin.area.df.node_id == 1545, ["node_id"]] = 1585

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2400050483

# Ontbrekende basin beneden-Vecht
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[12057, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[12057, "geometry"].interpolate(0.5, normalized=True)), tables=[outlet_data]
)

for link_id in [2956, 2957, 2958, 2959, 2960, 2961]:
    model.redirect_link(link_id, to_node_id=basin_node.node_id)

model.remove_node(76, remove_links=True)
model.link.add(basin_node, model.pump[598])
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[50])


# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2399931763

# Samenvoegen Westerveldse Aa
model.merge_basins(basin_id=1592, to_basin_id=1645, are_connected=True)
model.merge_basins(basin_id=1593, to_basin_id=1645, are_connected=True)

model.merge_basins(basin_id=1645, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2567, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2303, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2549, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2568, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2572, to_basin_id=1585, are_connected=True)
model.merge_basins(basin_id=2374, to_basin_id=1585, are_connected=True)

model.merge_basins(basin_id=2559, to_basin_id=2337, are_connected=False)


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
    "redirect_link",
    "reverse_link",
    "deactivate_node",
    "move_node",
    "remove_node",
    "connect_basins",
]

actions = [i for i in actions if i in gpd.list_layers(model_edits_path).name.to_list()]
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_path, layer=action, fid_as_index=True)
    if "order" in df.columns:
        df.sort_values("order", inplace=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# remove unassigned basin area
model.fix_unassigned_basin_area()
model.remove_unassigned_basin_area()

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
model.basin.node.df.loc[model.basin.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"
model.outlet.node.df.loc[model.outlet.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# somehow Sluis Engelen (beheerregister AAM) has been named Henriettesluis
model.outlet.node.df.loc[model.outlet.node.df.name == "Henriëttesluis", "name"] = "AKW855"

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(authority=authority, layers=["gemaal", "stuw", "sluis"])
df.loc[:, "code"] = df.nen3610id.str.removeprefix("NL.WBHCODE.59.").str.lower()
df.set_index("code", inplace=True)
names = df["naam"]


# set meta_gestuwd in basins
model.basin.node.df["meta_gestuwd"] = False
model.outlet.node.df["meta_gestuwd"] = False
model.pump.node.df["meta_gestuwd"] = True

# set stuwen als gestuwd

model.outlet.node.df.loc[model.outlet.node.df["meta_object_type"] == "stuw", "meta_gestuwd"] = True

# set bovenstroomse basins als gestuwd
node_df = model.node.df
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


# %% set flow-boundaries to level-boundaries (plus outlet)
for row in model.flow_boundary.node.df.itertuples():
    node_id = row.Index
    basin_node_id = model.downstream_node_id(node_id)

    # get link geometry and remove link
    link_id = model.link.df[model.link.df["from_node_id"] == node_id].index[0]
    link_geometry = model.link.df.at[link_id, "geometry"]
    model.link.df = model.link.df[model.link.df.index != link_id]

    # outlet node.geometry 10m from upstream or at 10% of link.geometry
    if link_geometry.length > 20:
        outlet_node_geometry = link_geometry.interpolate(10)
    else:
        outlet_node_geometry = link_geometry.interpolate(0.1, normalized=True)

    # change flow_boundary to level_boundary and add outlet_node
    model.update_node(node_id, node_type="LevelBoundary")
    outlet_node = model.outlet.add(
        node=Node(geometry=outlet_node_geometry, name=row.name), tables=default_tables.outlet
    )

    # remove old links and add 2 new
    left_link_geometry, right_link_geometry = list(split_line(link_geometry, outlet_node_geometry).geoms)
    model.link.add(model.level_boundary[node_id], outlet_node, geometry=left_link_geometry)
    model.link.add(outlet_node, model.basin[basin_node_id], geometry=right_link_geometry)


#  %% write model
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.validate_link_source_destination()
model.report_basin_area()
model.report_internal_basins()
# %%
# %% Test run model

# model = Model.read(ribasim_toml)
# status_code = model.run()

# assert status_code == 0

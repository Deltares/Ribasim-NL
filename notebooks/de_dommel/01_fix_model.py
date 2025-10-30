# %%

import inspect

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import Point, Polygon

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()
authority = "DeDommel"
short_name = "dommel"
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"

areas_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "areas.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg")
q_data_gpkg = cloud.joinpath(authority, "verwerkt", "1_ontvangen_data", "Geodata", "data_Q42018.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")

cloud.synchronize(filepaths=[ribasim_dir, areas_gpkg, hydamo_gpkg, q_data_gpkg])

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

# %% read model and watervlakken
model = Model.read(ribasim_toml)

network_validator = NetworkValidator(model)

# TODO file not in the cloud


# %% verwijder duplicated links

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2288780504
# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291081244
model.link.df = model.link.df.drop_duplicates(subset=["from_node_id", "to_node_id"])

if not network_validator.link_duplicated().empty:
    raise Exception("nog steeds duplicated links")

# %% toevoegen bovenstroomse knopen

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291091067
node_id = model.next_node_id
link_id = model.link.df.loc[model.link.df.to_node_id == 251].index[0]
model.link.df.loc[link_id, ["from_node_id"]] = node_id

node = Node(node_id, model.link.df.at[link_id, "geometry"].boundary.geoms[0])
model.basin.area.df.loc[model.basin.area.df.node_id == 1009, ["node_id"]] = node_id
area = basin.Area(geometry=model.basin.area[node_id].geometry.to_list())
model.basin.add(node, basin_data + [area])

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291111647
for row in network_validator.link_incorrect_connectivity().itertuples():
    # drop link from model
    model.remove_link(row.from_node_id, row.to_node_id, remove_disconnected_nodes=False)

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
            hydamo_gpkg,
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
    outlet_node_id = model.next_node_id
    outlet_data = outlet.Static(flow_rate=[100])
    model.outlet.add(
        Node(node_id=outlet_node_id, geometry=geometry, name=name, meta_object_type=meta_object_type),
        [outlet_data],
    )

    # add links
    model.link.add(model.basin[row.from_node_id], model.outlet[outlet_node_id])
    model.link.add(model.outlet[outlet_node_id], model.level_boundary[row.to_node_id])


if not network_validator.link_incorrect_connectivity().empty:
    raise Exception("nog steeds links zonder knopen")

# %% verwijderen internal basins

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291271525
for row in network_validator.node_internal_basin().itertuples():
    if row.Index not in model.basin.area.df.node_id.to_numpy():  # remove or change to level-boundary
        link_select_df = model.link.df[model.link.df.to_node_id == row.Index]
        if len(link_select_df) == 1:
            if model.node_table().df.at[link_select_df.iloc[0]["from_node_id"], "node_type"] == "FlowBoundary":
                model.remove_node(row.Index)
                model.remove_node(link_select_df.iloc[0]["from_node_id"])
                model.link.df.drop(index=link_select_df.index[0], inplace=True)

df = model.node_table().df[model.node_table().df.node_type == "Basin"]

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291876800
boundary_node = Node(node_id=28, geometry=model.flow_boundary[28].geometry)

for node_id in [29, 1828, 615, 28, 1329]:
    model.remove_node(node_id, remove_links=True)

level_data = level_boundary.Static(level=[0])
model.level_boundary.add(boundary_node, [level_data])

model.link.add(model.tabulated_rating_curve[614], model.level_boundary[28])
# %%
# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2292014475
model.remove_node(node_id=1898, remove_links=True)

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2292017813
model.update_node(989, "Outlet", [outlet.Static(flow_rate=[0])])
model.update_node(1891, "LevelBoundary", [level_data])

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291988317
# for from_node_id, to_node_id in [799, 1580, 625, 1123, 597, 978]:
for from_node_id, to_node_id in [[616, 1032], [1030, 616], [393, 1242], [1852, 393], [353, 1700], [1253, 353]]:
    model.reverse_link(from_node_id, to_node_id)

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2292050862

gdf = gpd.read_file(
    q_data_gpkg,
    layer="HydroObject",
    engine="pyogrio",
    fid_as_index=True,
)

geometry = gdf.loc[2751].geometry.interpolate(0.5, normalized=True)
node_id = model.next_node_id
manning_data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])

model.manning_resistance.add(Node(node_id=node_id, geometry=geometry), [manning_data])

model.link.df = model.link.df[~((model.link.df.from_node_id == 611) & (model.link.df.to_node_id == 1643))]
model.link.add(model.basin[1643], model.manning_resistance[node_id])
model.link.add(model.manning_resistance[node_id], model.basin[1182])
model.link.add(model.tabulated_rating_curve[611], model.basin[1182])

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2293457160
model.update_node(417, "Outlet", [outlet.Static(flow_rate=[0])])

if not network_validator.node_internal_basin().empty:
    raise Exception("nog steeds interne basins")


# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2367724440
gdf = gpd.read_file(
    hydamo_gpkg,
    layer="stuw",
    fid_as_index=True,
)
kst = gdf.loc[35]
geometry = Point(kst.geometry.x, kst.geometry.y)
name = kst.CODE
meta_object_type = "stuw"

outlet_node_id = model.next_node_id

kst_node = model.outlet.add(
    Node(node_id=outlet_node_id, geometry=geometry, name=name, meta_object_type=meta_object_type),
    [outlet_data],
)


gdf = gpd.read_file(
    hydamo_gpkg,
    layer="hydroobject",
    engine="pyogrio",
    fid_as_index=True,
)
geometry = gdf.at[2822, "geometry"].interpolate(0.5, normalized=True)
basin_node_id = model.next_node_id
basin_node = model.basin.add(
    Node(node_id=basin_node_id, geometry=geometry, meta_krw_name="Witte Loop/Peelrijt", meta_krw_id="NL27_KD_3_2"),
    basin_data,
)

model.remove_link(from_node_id=664, to_node_id=8, remove_disconnected_nodes=False)
model.link.add(model.manning_resistance[664], basin_node)
model.link.add(basin_node, kst_node)
model.link.add(kst_node, model.level_boundary[8])

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2293486609
df = network_validator.link_incorrect_type_connectivity(
    from_node_type="ManningResistance", to_node_type="LevelBoundary"
)

for node_id in df.from_node_id:
    model.update_node(node_id, "Outlet", [outlet.Static(flow_rate=[100])])

# see: https://github.com/Deltares/Ribasim-NL/issues/132
model.basin.area.df.loc[model.basin.area.df.duplicated("node_id"), ["node_id"]] = -1
model.basin.area.df.reset_index(drop=True, inplace=True)
model.basin.area.df.index.name = "fid"
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest", distance=100)
model.fix_unassigned_basin_area()

model.basin.area.df = model.basin.area.df[~model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]


# fix basin area

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2370880128
basin_polygon = model.basin.area.df.union_all()
holes = [Polygon(interior) for polygon in basin_polygon.buffer(10).buffer(-10).geoms for interior in polygon.interiors]
geoseries = gpd.GeoSeries(holes, crs=28992)

drainage_areas_df = gpd.read_file(areas_gpkg, layer="drainage_areas")

drainage_areas_df = drainage_areas_df[drainage_areas_df.buffer(-10).intersects(basin_polygon)]

for idx, geometry in enumerate(geoseries):
    # select drainage-area
    drainage_area_select = drainage_areas_df[drainage_areas_df.contains(geometry.buffer(-10))]
    if not drainage_area_select.empty:
        if not len(drainage_area_select) == 1:
            raise ValueError("hole contained by multiple drainage areas, can't fix that yet")

        drainage_area = drainage_area_select.iloc[0].geometry

        # find basin_id to merge to
        selected_basins_df = model.basin.area.df[model.basin.area.df.buffer(-10).within(drainage_area)].set_index(
            "node_id"
        )
        intersecting_basins_df = selected_basins_df.intersection(geometry.buffer(10))
        assigned_basin_id = selected_basins_df.intersection(geometry.buffer(10)).area.idxmax()

        # clip and merge geometry
        geometry = geometry.buffer(10).difference(basin_polygon)
        geometry = (
            model.basin.area.df.set_index("node_id")
            .at[assigned_basin_id, "geometry"]
            .union(geometry)
            .buffer(0.1)
            .buffer(-0.1)
        )
        model.basin.area.df.loc[model.basin.area.df.node_id == assigned_basin_id, "geometry"] = geometry
# %% fix_basin_area


for action in ["merge_basins", "remove_node", "update_node"]:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_gpkg, layer=action, fid_as_index=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])


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
for node_type in model.node_table().df.node_type.unique():
    table = getattr(model, pascal_to_snake_case(node_type)).node
    table.df.loc[table.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(layers=["sluis", "gemaal", "stuw", "duiker"], authority=authority)
df = df[df.code.notna()]
df.set_index("code", inplace=True)
names = df["naam"]


# set meta_gestuwd in basins
model.basin.node.df["meta_gestuwd"] = False
model.outlet.node.df["meta_gestuwd"] = False
model.pump.node.df["meta_gestuwd"] = True

# set stuwen als gestuwd

model.outlet.node.df.loc[model.outlet.node.df["meta_object_type"] == "stuw", "meta_gestuwd"] = True

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
        {"node_types": ["Basin", "ManningResistance"], "columns": {"name": ""}},
        {"node_types": ["FlowBoundary", "LevelBoundary"], "columns": {"meta_name": "name"}},
    ],
    names=names,
)

# add level_boundary at keersop (intake from Kannaal van Bocholt naar Heerentals)
level_boundary_node = model.level_boundary.add(
    Node(geometry=Point(152152, 365151)), tables=[level_boundary.Static(level=[0])]
)
outlet_node = model.outlet.add(
    Node(geometry=Point(152150, 365169), name="Inlaat kanaal Bocholt naar Heerentals"),
    tables=[outlet.Static(flow_rate=[0])],
)
model.link.add(level_boundary_node, outlet_node)
model.link.add(outlet_node, model.basin[1609])

# label flow-boundaries to buitenlandse-aanvoer
model.flow_boundary.node.df["meta_categorie"] = "buitenlandse aanvoer"

# %%
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{short_name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %% Test run model

result = model.run()
assert result.simulation_time is not None
# %%

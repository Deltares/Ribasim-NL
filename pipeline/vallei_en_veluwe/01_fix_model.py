# %%
from typing import cast

import geopandas as gpd
import pandas as pd
from ribasim.nodes import basin, level_boundary, outlet
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, MultiPolygon

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "ValleienVeluwe"
name = "venv"

# %% Check if model exist, otherwise download
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
fix_user_data_gpkg = cloud.joinpath(authority, "verwerkt/fix_user_data.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
model_edits_aanvoer_gpkg = cloud.joinpath(authority, "verwerkt/model_edits_aanvoer.gpkg")

# %% read model
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id
network_validator = NetworkValidator(model)
split_line_gdf = gpd.read_file(fix_user_data_gpkg, layer="split_basins", fid_as_index=True)

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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2401959032

# Oplossen verkeerde takrichting
for link_id in [
    lk("KDU-21040", "bnd:38"),
    lk("up:KSL-22", "KST-2202"),
    lk("KGM-10", "down:KGM-10"),
    lk("KST-671", "up:KDU-1009"),
    lk("KST-3300", "up:KGM-91"),
    lk("WL_1860", "bnd:24"),
]:
    model.reverse_link(link_id=link_id)

# model.invalid_topology_at_node().to_file("topo_errors.gpkg")


# %% see: https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2402031275

# Veluwemeer at Harderwijk verwijderen
for node_id in [n("bnd:24"), n("WL_1860")]:
    model.remove_node(node_id, remove_links=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2402229646

# Veluwemeer at Elburg verwijderen
for node_id in [n("bnd:3"), n("up:bnd:3")]:
    model.remove_node(node_id, remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2402281396

# Verwijderen basins zonder area of toevoegen/opknippen basin /area
model.split_basin(line=cast(LineString, split_line_gdf.at[1, "geometry"]))
model.split_basin(line=cast(LineString, split_line_gdf.at[2, "geometry"]))
model.split_basin(line=cast(LineString, split_line_gdf.at[3, "geometry"]))
model.merge_basins(node_id=n("up:KST-3008"), to_node_id=n("up:WL_105699___3"))
model.merge_basins(node_id=n("up:ow:WL_2717"), to_node_id=n("up:KDU-1500"))
model.merge_basins(node_id=n("up:KDU-536"), to_node_id=n("up:KDU-2347"))
model.merge_basins(node_id=n("up:KGM-182"), to_node_id=n("up:KDU-719"))
model.merge_basins(node_id=n("up:KGM-44"), to_node_id=n("up:KGM-98"), are_connected=False)
model.merge_basins(node_id=n("up:KST-320"), to_node_id=n("up:ow:WL_519"))
model.merge_basins(node_id=n("up:KGM-41"), to_node_id=n("up:KST-56"))
model.merge_basins(node_id=n("up:KST-2311"), to_node_id=n("up:KGM-73"), are_connected=False)
model.merge_basins(node_id=n("up:KSL-27"), to_node_id=n("up:KGM-164"))
model.merge_basins(node_id=n("up:KDU-76790"), to_node_id=n("up:KDU-18587"))
model.merge_basins(node_id=n("up:KDU-3114"), to_node_id=n("up:KST-2416"))
model.merge_basins(node_id=n("up:KDU-385"), to_node_id=n("up:ow:WL_2453"))
model.update_node(n("down:KGM-98"), "LevelBoundary", data=[level_data])
model.merge_basins(node_id=n("down:KST-2315"), to_node_id=n("up:WL_17858___3"), are_connected=False)
model.merge_basins(node_id=n("down:KDU-60071"), to_node_id=n("up:KST-3052"), are_connected=False)
model.merge_basins(node_id=n("up:ow:WL_1679"), to_node_id=n("up:KST-3976"))
model.merge_basins(node_id=n("up:WL_112933"), to_node_id=n("up:KST-3043"))
model.remove_node(node_id=n("KST-2420"), remove_links=True)
model.merge_basins(node_id=n("up:KST-2419"), to_node_id=n("up:ow:WL_3293"), are_connected=False)
model.remove_node(n("up:KST-4763"), remove_links=True)
model.remove_node(n("KST-4763"), remove_links=True)
model.remove_node(n("KST-4762"), remove_links=True)
model.merge_basins(node_id=n("up:KDU-2845"), to_node_id=n("up:WL_2121___0"))
model.merge_basins(node_id=n("up:KDU-60071"), to_node_id=n("up:KST-3051"), are_connected=False)
model.merge_basins(node_id=n("up:WL_2223___4"), to_node_id=n("up:KGM-135"))
model.merge_basins(node_id=n("up:KST-4217"), to_node_id=n("up:KST-2323"))
model.merge_basins(node_id=n("up:KST-4114"), to_node_id=n("up:KST-2323"))
model.merge_basins(node_id=n("up:KST-4115"), to_node_id=n("up:KST-2323"))
model.merge_basins(node_id=n("up:KST-4116"), to_node_id=n("up:KST-2323"))
model.merge_basins(node_id=n("up:KDU-17759"), to_node_id=n("up:KST-1488"))
model.merge_basins(node_id=n("up:KGM-91"), to_node_id=n("up:KDU-19140"))
model.merge_basins(node_id=n("up:ow:WL_31669"), to_node_id=n("up:KDU-19140"))
model.merge_basins(node_id=n("up:WL_31710___2"), to_node_id=n("up:KDU-19140"))
model.merge_basins(node_id=n("up:KST-1606"), to_node_id=n("up:KDU-18867"))
model.merge_basins(node_id=n("up:KDU-18525"), to_node_id=n("up:KDU-18867"))
model.merge_basins(node_id=n("up:KSL-23"), to_node_id=n("up:KSL-22"))
model.merge_basins(node_id=n("up:KGM-81"), to_node_id=n("up:KST-3862"))
model.merge_basins(node_id=n("up:KDU-5947"), to_node_id=n("up:ow:WL_19738"))
model.merge_basins(node_id=n("up:KST-809"), to_node_id=n("up:ow:WL_19738"))
model.remove_node(n("WL_13431___2"), remove_links=True)
model.remove_node(n("ow:WL_35310"), remove_links=True)
model.remove_node(n("WL_280___1"), remove_links=True)
model.merge_basins(node_id=n("down:KGM-180"), to_node_id=n("up:KST-3862"), are_connected=False)
model.merge_basins(node_id=n("up:KDU-18711"), to_node_id=n("up:KDU-19140"), are_connected=True)
assert model.basin.area.df is not None

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


# buffer out small slivers
model.basin.area.df.loc[:, ["geometry"]] = (
    model.basin.area.df.buffer(0.1)
    .buffer(-0.1)
    .apply(lambda x: x if x.geom_type == "MultiPolygon" else MultiPolygon([x]))  # ty: ignore[no-matching-overload]
)

# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)

# %%
model.explode_basin_area()  # all multipolygons to singles
# model.basin.area.df.to_file("basin_area.gpkg", layer="model_basin_area")
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)

actions = [
    "remove_basin_area",
    "remove_node",
    "remove_link",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_link",
    "move_node",
    "connect_basins",
    "update_node",
    "redirect_link",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_gpkg).name.to_list()]
model.apply_edits(model_edits_gpkg, actions, sort_by_order=True)

# Valleikanaal verkeerd geschematiseerd
model.redirect_link(link_id=lk("KST-2311", "up:KST-1842"), to_node_id=n("up:KST-2312"))
model.redirect_link(link_id=lk("KST-2310", "up:KST-1842"), to_node_id=n("up:KST-2312"))
model.redirect_link(link_id=lk("KDU-17313", "up:KST-1842"), to_node_id=n("up:KST-2312"))
model.redirect_link(link_id=lk("ow:WL_2453", "up:KST-67"), to_node_id=n("up:KGM-19"))
model.redirect_link(link_id=lk("KGM-3", "up:KSL-6"), to_node_id=n("bnd:1"))
model.redirect_link(link_id=lk("up:KDU-3797", "WL_1702"), from_node_id=n("up:KST-3227"))

# %% Aanvoer edits

# fix boundary levels so we can get inflow
model.reverse_direction_at_node(n("KSL-20"))  # sluis Dieren is not an inlet
model.reverse_direction_at_node(n("KST-1777"))  #
model.reverse_direction_at_node(n("KSL-23"))  # Laakse Duiker


layers = gpd.list_layers(model_edits_aanvoer_gpkg).name.to_list()
# the layers with the old names of the link edits, such as `reverse_edge`, are no edit methods, and have no rows
old_layers = [i for i in layers if i.endswith("_edge")]
assert all(gpd.read_file(model_edits_aanvoer_gpkg, layer=i).empty for i in old_layers), old_layers
model.apply_edits(model_edits_aanvoer_gpkg, [i for i in layers if i not in old_layers], sort_by_order=True)

# remove node (wrong connection)
model.remove_node(n("KSL-6"), remove_links=True)
model.remove_node(n("ow:WL_105070"), remove_links=True)
model.remove_node(n("ow:WL_2452"), remove_links=True)
model.remove_node(n("bnd:16"), remove_links=True)
model.remove_node(n("bnd:17"), remove_links=True)
model.remove_node(n("bnd:18"), remove_links=True)
model.remove_node(n("WL_5608___3"), remove_links=True)
model.remove_node(n("KDU-5352"), remove_links=True)
model.remove_node(n("KDU-18525"), remove_links=True)
model.remove_node(n("KDU-43969"), remove_links=True)
model.remove_node(n("KST-3227"), remove_links=True)
model.remove_node(n("KST-710"), remove_links=True)
model.remove_node(n("KGM3-2"), remove_links=True)
model.remove_node(n("ow:WL_23405"), remove_links=True)
model.remove_node(n("KST-225"), remove_links=True)
model.remove_node(n("KDU-3797"), remove_links=True)
model.remove_node(n("WL_114851"), remove_links=True)
model.remove_node(n("WL_114868"), remove_links=True)
model.remove_node(n("ow:WL_329"), remove_links=True)
model.remove_node(n("KDU-870"), remove_links=True)
model.remove_node(n("KDU-59769"), remove_links=True)  # Duiker vervangen door pomp Oostsingel
model.remove_node(n("KST-2317"), remove_links=True)  # Stuw Asschat, zit er 2 keer in
model.remove_node(n("KST-2310"), remove_links=True)  # stuw de Groep, zit er 2 keer in
model.remove_node(n("ow:WL_112582"), remove_links=True)
model.remove_node(n("ow:WL_112474"), remove_links=True)
model.remove_node(n("ow:WL_19593"), remove_links=True)
model.remove_node(n("ow:WL_19449"), remove_links=True)
# merge basins
# model.merge_basins(node_id=1044, to_node_id=1103)

model.merge_basins(node_id=n("up:KST-1604"), to_node_id=n("up:KGM-113"))
model.merge_basins(node_id=n("up:WL_2317___3"), to_node_id=n("up:ow:WL_779"))
model.merge_basins(node_id=n("up:ow:WL_779"), to_node_id=n("up:ow:WL_2453"))
model.merge_basins(node_id=n("up:ow:WL_2453"), to_node_id=n("up:KST-74"))
model.merge_basins(node_id=n("up:KGM-19"), to_node_id=n("up:KST-74"))
model.merge_basins(node_id=n("up:KDU-867"), to_node_id=n("up:KST-673"))
model.merge_basins(node_id=n("up:KDU-2711"), to_node_id=n("up:KST-59"))
model.merge_basins(node_id=n("up:KDU-1952"), to_node_id=n("up:ow:WL_2478"))
model.merge_basins(node_id=n("up:KDU-371"), to_node_id=n("up:KDU-2325"))
model.merge_basins(node_id=n("up:KSL-6"), to_node_id=n("up:KDU-5365"))
model.merge_basins(node_id=n("up:KDU-5365"), to_node_id=n("up:KDU-5352"))
model.merge_basins(node_id=n("up:KST-3280"), to_node_id=n("up:KDU-76056"))
model.merge_basins(node_id=n("up:ow:WL_3293"), to_node_id=n("up:KST-2420"))
model.merge_basins(node_id=n("up:KDU-3797"), to_node_id=n("up:KDU-3737"))
model.merge_basins(node_id=n("up:KDU-3737"), to_node_id=n("up:KST-347"))
model.merge_basins(node_id=n("up:KDU-366"), to_node_id=n("up:KST-4169"))
model.merge_basins(node_id=n("up:KDU-18473"), to_node_id=n("up:KDU-18867"))
model.merge_basins(node_id=n("up:KST-3227"), to_node_id=n("up:KST-347"))
model.merge_basins(node_id=n("up:KST-4251"), to_node_id=n("up:KST-3524"))
model.merge_basins(node_id=n("up:KDU-870"), to_node_id=n("up:KDU-1009"), are_connected=False)

model.remove_node(n("KGM-180"), remove_links=True)
# %% corrigeren knoop-topologie
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")


# %% sanitize node-table
# TabulatedRatingCurve to Outlet
for row in model.node.df[model.node.df.node_type == "TabulatedRatingCurve"].itertuples():
    node_id = row.Index
    model.update_node(node_id=node_id, node_type="Outlet")

# basins and outlets we've added do not have category, we fill with hoofdwater
# model.node.df.loc[model.basin.node.df.index[model.basin.node.df["meta_categorie"].isna()], "meta_categorie"] = "hoofdwater"
# model.node.df.loc[model.outlet.node.df.index[model.outlet.node.df["meta_categorie"].isna()], "meta_categorie"] = "hoofdwater"

# somehow Sluis Engelen (beheerregister AAM) has been named Henriettesluis
# model.node.df.loc[
#     (model.node.df["node_type"] == "Outlet") & (model.node.df.name == "Henriëttesluis"),
#     "name",
# ] = "AKW855"

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


# %% give the holes that the edits left in the basin areas to a basin
model.fill_basin_area_holes(areas_gpkg)

#  %% write model
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
df = model.report_internal_basins()
if not df.empty:
    print(f"internal basins!: {df['node_id'].to_list()}")

# %%

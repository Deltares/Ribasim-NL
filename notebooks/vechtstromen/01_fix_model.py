# %%
import inspect
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, pump
from ribasim.validation import can_connect
from ribasim_nl.cloud import ModelVersion
from ribasim_nl.geometry import drop_z, link, split_basin, split_basin_multi_polygon
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely import wkt
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.ops import nearest_points

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()
authority = "Vechtstromen"
name = "vechtstromen"
run_model = True


def get_latest_hws_model_version() -> ModelVersion:
    model_versions = [
        i for i in cloud.uploaded_models("Rijkswaterstaat") if i is not None and getattr(i, "model", None) == "hws"
    ]
    if model_versions:
        return sorted(model_versions, key=lambda x: getattr(x, "sorter", ""))[-1]
    raise ValueError("No Rijkswatersdtaat/modellen/hws models found")


# paths that should be synced
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
fix_user_data_gpkg = cloud.joinpath(authority, "verwerkt/fix_user_data.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
ribasim_areas_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/areas.gpkg")
lhm_gemaal_gdb = cloud.joinpath(authority, "verwerkt", "1_ontvangen_data", "LHM20230418.gdb")
wateraanvoer_shp = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/wateraanvoergebieden_20250416/Wateraanvoer.shp")
hws_model = get_latest_hws_model_version().path_string
hws_model_dir = cloud.joinpath(f"Rijkswaterstaat/modellen/{hws_model}")
hws_model_toml = hws_model_dir / "hws.toml"

cloud.synchronize(
    filepaths=[
        ribasim_dir,
        fix_user_data_gpkg,
        model_edits_gpkg,
        hydamo_gpkg,
        ribasim_areas_gpkg,
        hws_model_dir,
        wateraanvoer_shp,
    ]
)

# %%
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
split_line_gdf = gpd.read_file(fix_user_data_gpkg, layer="split_basins", fid_as_index=True)
level_boundary_gdf = gpd.read_file(fix_user_data_gpkg, layer="level_boundary", fid_as_index=True)
ribasim_areas_gdf = gpd.read_file(ribasim_areas_gpkg, fid_as_index=True, layer="areas")
drainage_areas_df = gpd.read_file(cloud.joinpath("Vechtstromen/verwerkt/4_ribasim/areas.gpkg"), layer="drainage_areas")

model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)

# %% some stuff we'll need again
manning_data = manning_resistance.Static(length=[100], manning_n=[0.03], profile_width=[10], profile_slope=[1])
level_data = level_boundary.Static(level=[0])
pump_data = pump.Static(flow_rate=[1])

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


def update_nodes(model: Model, node_ids: list[int], node_type: str) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.update_node(node_id=node_id, node_type=node_type)


def remove_nodes(model: Model, node_ids: list[int]) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.remove_node(node_id, remove_links=True)


def redirect_links(model: Model, redirects: list[dict]) -> None:
    for kwargs in redirects:
        model.redirect_link(**kwargs)


def merge_basin_pairs(model: Model, basin_pairs: list[tuple[int, int]], are_connected: bool = True) -> None:
    for node_id, to_node_id in basin_pairs:
        model.merge_basins(node_id=node_id, to_node_id=to_node_id, are_connected=are_connected)


def reverse_links(model: Model, link_ids: list[int]) -> None:
    for link_id in link_ids:
        model.reverse_link(link_id=link_id)


def read_lhm_structure_points(
    gdb_path: Path,
    crs,
    layer_names: tuple[str, ...] = ("STUW", "SLUIS", "GEMAAL", "DUIKERSIFONHEVEL"),
) -> gpd.GeoDataFrame:
    structure_gdfs = []

    for layer_name in layer_names:
        df = gpd.read_file(gdb_path, layer=layer_name)
        if "SHAPE" not in df.columns:
            continue

        shape_series = df["SHAPE"].astype("string")
        mask = shape_series.notna()
        if not mask.any():
            continue

        geometry = gpd.GeoSeries(shape_series.loc[mask].map(wkt.loads), crs=crs)
        geometry = geometry.map(
            lambda geom: (
                geom
                if geom.geom_type == "Point"
                else geom.interpolate(0.5, normalized=True)
                if geom.geom_type == "LineString"
                else geom.centroid
            )
        )

        structure_gdf = gpd.GeoDataFrame(
            df.loc[mask, ["CODE", "NAAM"]].copy(),
            geometry=geometry,
            crs=crs,
        )
        structure_gdf["source_layer"] = layer_name
        structure_gdfs.append(structure_gdf)

    if not structure_gdfs:
        return gpd.GeoDataFrame(columns=["CODE", "NAAM", "source_layer", "geometry"], geometry="geometry", crs=crs)

    return gpd.GeoDataFrame(
        pd.concat(structure_gdfs, ignore_index=True),
        geometry="geometry",
        crs=crs,
    )


def get_manning_nodes_near_structures(
    model: Model,
    structure_gdf: gpd.GeoDataFrame,
    max_distance: float = 5,
) -> tuple[list[int], list[int], pd.DataFrame]:
    manning_gdf = model.node.df.loc[model.node.df["node_type"] == "ManningResistance"].copy()
    manning_gdf.index.name = "node_id"
    manning_gdf = manning_gdf.reset_index()
    manning_gdf = gpd.GeoDataFrame(manning_gdf, geometry="geometry", crs=model.crs)

    if manning_gdf.empty or structure_gdf.empty:
        return [], [], pd.DataFrame(columns=["source_layer", "distance", "CODE", "NAAM", "target_node_type"])

    nearest_gdf = gpd.sjoin_nearest(
        manning_gdf[["node_id", "geometry"]],
        structure_gdf[["source_layer", "CODE", "NAAM", "geometry"]],
        how="left",
        max_distance=max_distance,
        distance_col="distance",
    )
    nearest_gdf = nearest_gdf.dropna(subset=["source_layer"])

    if nearest_gdf.empty:
        return [], [], pd.DataFrame(columns=["source_layer", "distance", "CODE", "NAAM", "target_node_type"])

    nearest_gdf = nearest_gdf.sort_values(["distance"]).drop_duplicates(subset=["node_id"]).set_index("node_id")
    nearest_gdf["target_node_type"] = nearest_gdf["source_layer"].map(
        lambda layer: "Pump" if layer == "GEMAAL" else "Outlet"
    )

    pump_node_ids = nearest_gdf[nearest_gdf["source_layer"] == "GEMAAL"].index.to_list()
    outlet_node_ids = nearest_gdf[nearest_gdf["source_layer"] != "GEMAAL"].index.to_list()

    print(
        "ManningResistance -> type fixes binnen "
        f"{max_distance} m: Pump={len(pump_node_ids)}, Outlet={len(outlet_node_ids)}"
    )
    for node_id, row in nearest_gdf.sort_index().iterrows():
        print(
            f"node_id={node_id} -> {row['target_node_type']} "
            f"(layer={row['source_layer']}, code={row['CODE']}, distance={row['distance']:.3f} m)"
        )

    return outlet_node_ids, pump_node_ids, nearest_gdf[["source_layer", "distance", "CODE", "NAAM", "target_node_type"]]


def clean_basin_area_geometry(geometry, label: str, min_polygon_area: float = 1000.0):
    geometry = shapely.force_2d(geometry).buffer(0)
    if geometry.geom_type == "Polygon":
        polygons = [geometry]
    elif geometry.geom_type == "MultiPolygon":
        polygons = list(geometry.geoms)
    else:
        raise ValueError(f"Geometry voor {label} heeft ongeldig type: {geometry.geom_type}")

    polygons = [polygon for polygon in polygons if polygon.area >= min_polygon_area]
    if not polygons:
        raise ValueError(f"Geometry voor {label} is leeg na verwijderen van slivers")

    return MultiPolygon(polygons)


def first_meta_streefpeil(area_df: gpd.GeoDataFrame) -> float | None:
    if "meta_streefpeil" not in area_df.columns:
        return None

    streefpeil_series = area_df["meta_streefpeil"].dropna()
    return float(streefpeil_series.iloc[0]) if not streefpeil_series.empty else None


# # drop z in basin.nodes, zodat we hieronder geen crashes meer krijgen.
basin_mask = model.node.df["node_type"] == "Basin"
model.node.df.loc[basin_mask, "geometry"] = model.node.df.loc[basin_mask, "geometry"].apply(drop_z)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385111465

# Verwijderen duplicate links

model.link.df.drop_duplicates(inplace=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2352686763

# Toevoegen benedenstroomse randvoorwaarden Beneden Dinkel

# verander basin met node_id 2250 naar type level_boundary
model.update_node(2250, "LevelBoundary", data=[level_data])


# verplaats basin 1375 naar het hydroobject
node_id = 1375

model.node.df.loc[node_id, "geometry"] = hydroobject_gdf.at[3135, "geometry"].interpolate(0.5, normalized=True)
link_ids = model.link.df[
    (model.link.df.from_node_id == node_id) | (model.link.df.to_node_id == node_id)
].index.to_list()
model.reset_link_geometry(link_ids=link_ids)

# verplaats basin 1375 naar het hydroobject


# verbind basins met level_boundaries
for fid, node_id in [(1, 1375), (2, 1624)]:
    boundary_node_geometry = level_boundary_gdf.at[fid, "geometry"]

    # line for interpolation
    basin_node_geometry = Point(
        model.basin.node.df.at[node_id, "geometry"].x, model.basin.node.df.at[node_id, "geometry"].y
    )
    line_geometry = LineString((basin_node_geometry, boundary_node_geometry))

    # define level_boundary_node
    boundary_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])
    level_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])

    # define manning_resistance_node
    outlet_node_geometry = line_geometry.interpolate(line_geometry.length - 20)
    outlet_node = model.outlet.add(Node(geometry=outlet_node_geometry), tables=[outlet_data])

    from_node_id = model.basin[node_id].node_id
    to_node_id = outlet_node.node_id

    # draw links
    # FIXME: we force links to be z-less untill this is solved: https://github.com/Deltares/Ribasim/issues/1854
    model.link.add(
        model.basin[node_id], outlet_node, geometry=link(model.basin[node_id].geometry, outlet_node.geometry)
    )
    model.link.add(outlet_node, boundary_node, geometry=link(outlet_node.geometry, boundary_node.geometry))

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382565944

# Verwijderen Twentekanaal (zit al bij RWS-HWS)
remove_node_ids = [1562, 1568, 1801, 1804, 1810, 1900, 2114, 2118, 2119, 32]

# remove by link so we also remove all resistance nodes in between
link_df = model.link.df[
    model.link.df.from_node_id.isin(remove_node_ids) | model.link.df.to_node_id.isin(remove_node_ids)
][["from_node_id", "to_node_id"]]

for row in link_df.itertuples():
    model.remove_link(from_node_id=row.from_node_id, to_node_id=row.to_node_id, remove_disconnected_nodes=True)

# add level_boundaries at twentekanaal for later coupling
hws_model = Model.read(hws_model_toml)
basin_ids = hws_model.node.df[hws_model.node.df.name.str.contains("Twentekanaal")].index.to_list()
twentekanaal_poly = hws_model.basin.area.df[hws_model.basin.area.df.node_id.isin(basin_ids)].union_all()

connect_node_ids = [
    i for i in set(link_df[["from_node_id", "to_node_id"]].to_numpy().flatten()) if i in model.node.df.index
]

for node_id in connect_node_ids:
    node = model.get_node(node_id=node_id)

    # update node to Outlet if it's a manning resistance
    if node.node_type == "ManningResistance":
        model.update_node(node.node_id, "Outlet", data=[outlet_data])
        node = model.get_node(node_id=node_id)

    _, boundary_node_geometry = nearest_points(node.geometry, twentekanaal_poly.boundary)

    boundary_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])

    # draw link in the correct direction
    if model.link.df.from_node_id.isin([node_id]).any():  # supply
        model.link.add(boundary_node, node, geometry=link(boundary_node.geometry, node.geometry))
    else:
        model.link.add(node, boundary_node, geometry=link(node.geometry, boundary_node.geometry))

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385525533

# Opruimen situatie rondom gemaal Oude Drostendiep

# pumps met node_id 639, 608 en 603 op te heffen (1 gemaal ipv 3)
remove_node_ids = [639, 608, 603]

for node_id in remove_node_ids:
    model.remove_node(node_id, remove_links=True)

# remove by link so we also remove all resistance nodes in between
link_df = model.link.df[
    model.link.df.from_node_id.isin(remove_node_ids) | model.link.df.to_node_id.isin(remove_node_ids)
][["from_node_id", "to_node_id"]]

for row in link_df.itertuples():
    model.remove_link(from_node_id=row.from_node_id, to_node_id=row.to_node_id, remove_disconnected_nodes=True)

# basin met node_id 1436 te verplaatsen naar locatie basin node_id 2259
basin_id = 1436
model.node.df.loc[basin_id, "geometry"] = model.basin[2259].geometry
link_ids = model.link.df[
    (model.link.df.from_node_id == basin_id) | (model.link.df.to_node_id == basin_id)
].index.to_list()

model.reset_link_geometry(link_ids=link_ids)

# basin met node_id 2259 opheffen (klein niets-zeggend bakje)
model.remove_node(2259, remove_links=True)

# stuw ST05005 (node_id 361) verbinden met basin met node_id 1436
model.link.add(model.tabulated_rating_curve[361], model.basin[basin_id])
model.link.add(model.basin[basin_id], model.pump[635])

# basin met node_id 2250 verplaatsen naar logische plek bovenstrooms ST05005 en bendenstrooms ST02886 op hydroobjec
basin_id = 2255
model.node.df.loc[basin_id, ["geometry"]] = hydroobject_gdf.at[6444, "geometry"].interpolate(0.5, normalized=True)

link_ids = model.link.df[
    (model.link.df.from_node_id == basin_id) | (model.link.df.to_node_id == basin_id)
].index.to_list()

model.reset_link_geometry(link_ids=link_ids)

model.split_basin(split_line_gdf.at[9, "geometry"])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385409772

incorrect_links_df = network_validator.link_incorrect_connectivity()
false_basin_ids = [1356, 1357, 1358, 1359, 1360, 1361, 1362, 1363, 1364, 1365, 1366, 1367, 1368, 1369, 1370]

for false_basin_id in false_basin_ids:
    basin_geom = (
        incorrect_links_df[incorrect_links_df.from_node_id == false_basin_id].iloc[0].geometry.boundary.geoms[0]
    )
    basin_node = model.basin.add(Node(geometry=basin_geom), tables=basin_data)

    # fix link topology
    model.link.df.loc[
        incorrect_links_df[incorrect_links_df.from_node_id == false_basin_id].index.to_list(), ["from_node_id"]
    ] = basin_node.node_id

    model.link.df.loc[
        incorrect_links_df[incorrect_links_df.to_node_id == false_basin_id].index.to_list(), ["to_node_id"]
    ] = basin_node.node_id

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2386671759


# basin 2224 en manning_resistance 898 (DK28491) opheffen
# tabulated_rating_cuves 336 (ST03745) en 238 (ST03744) opheffen
remove_node_ids = [2224, 898, 336, 238]

for node_id in remove_node_ids:
    model.remove_node(node_id, remove_links=True)

# pump 667 (GM00088) verbinden met basin 1495
model.link.add(model.pump[667], model.basin[1495])

# model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387026622

# opruimen basin at Amsterdamscheveld
model.remove_node(1683, remove_links=True)

# verbinden basin node_id 1680 met tabulated_rating_curve node_id 101 en 125
model.link.add(model.basin[1680], model.tabulated_rating_curve[101])
model.link.add(model.basin[1680], model.tabulated_rating_curve[125])

# verbinden pump node_id 622 met basin node_id 1680
model.link.add(model.pump[622], model.basin[1680])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387056481

# Fix stieltjeskanaal

# split basin_area bij manning_resistance node_id 1365

line = split_line_gdf.at[1, "geometry"]

basin_polygon = model.basin.area.df.at[8, "geometry"].geoms[0]
basin_polygons = split_basin(basin_polygon, line)

model.basin.area.df.loc[8, ["geometry"]] = MultiPolygon([basin_polygons.geoms[0]])
model.basin.area.df.loc[model.basin.area.df.index.max() + 1, ["geometry"]] = MultiPolygon([basin_polygons.geoms[1]])

# hef basin node_id 1901 op
model.remove_node(1901, remove_links=True)

# hef pump node_id 574 (GM00246) en node_id 638 (GM00249) op
model.remove_node(574, remove_links=True)
model.remove_node(638, remove_links=True)

# verbind basin node_id 1876 met pump node_ids 626 (GM00248) en 654 (GM00247)
model.link.add(model.basin[1876], model.pump[626])
model.link.add(model.basin[1876], model.pump[654])
model.link.add(model.tabulated_rating_curve[113], model.basin[1876])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387815013

# opruimen Zwinderskanaal


# split basin_area bij rode lijn
line = split_line_gdf.at[2, "geometry"]
basin_polygon = model.basin.area.df.at[65, "geometry"].geoms[0]
basin_polygons = split_basin(basin_polygon, line)

model.basin.area.df.loc[65, ["geometry"]] = MultiPolygon([basin_polygons.geoms[0]])
model.basin.area.df.loc[model.basin.area.df.index.max() + 1, ["geometry"]] = MultiPolygon([basin_polygons.geoms[1]])

# verwijderen basin 2226, 2258, 2242 en manning_resistance 1366
for node_id in [2226, 2258, 2242, 1366, 1350]:
    model.remove_node(node_id, remove_links=True)

# verbinden tabulated_rating_curves 327 (ST03499) en 510 (ST03198) met basin 1897
model.link.add(model.basin[1897], model.tabulated_rating_curve[327])
model.link.add(model.tabulated_rating_curve[510], model.basin[1897])

# verbinden basin 1897 met tabulated_rating_curve 279 (ST03138)
model.link.add(model.basin[1897], model.tabulated_rating_curve[279])

# verbinden basin 1897 met manning_resistance 1351 en 1352
model.link.add(model.basin[1897], model.manning_resistance[1351])
model.link.add(model.basin[1897], model.manning_resistance[1352])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387888742

# Oplossen situatie Van Echtenskanaal/Scholtenskanaal Klazinaveen

# verplaatsen level_boundary 33 naar splitsing scholtenskanaal/echtenskanaal en omzetten naar basin
outlet_node_geometry = model.level_boundary[33].geometry
model.update_node(33, "Basin", data=basin_data)
model.move_node(node_id=33, geometry=hydroobject_gdf.loc[2679].geometry.boundary.geoms[1])

# plaatsen outlet bij oorspronkelijke plaats level_boundary 33
outlet_node = model.outlet.add(Node(geometry=outlet_node_geometry), tables=[outlet_data])

# plaatsen nieuwe level_boundary op scholtenskanaal aan H&A zijde scholtenskanaal van outlet
boundary_node = model.level_boundary.add(Node(geometry=level_boundary_gdf.at[3, "geometry"]), tables=[level_data])

# toevoegen links vanaf nieuwe basin 33 naar nieuwe outlet naar nieuwe boundary
model.link.add(model.basin[33], outlet_node, geometry=link(model.basin[33].geometry, outlet_node.geometry))
model.link.add(outlet_node, boundary_node)

# opheffen manning_resistance 1330 bij GM00213
model.remove_node(1330, remove_links=True)

# verbinden nieuwe basin met outlet en oorspronkijke manning_knopen en pompen in oorspronkelijke richting
for link_id in [2711, 2712, 2713, 2714, 2708]:
    model.reverse_link(link_id=link_id)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2388009499

# basin met node_id 1873 gaat richting geesburg
model.move_node(node_id=1873, geometry=hydroobject_gdf.loc[6616].geometry.boundary.geoms[1])
model.basin.area.df.loc[model.basin.area.df.node_id == 1873, ["node_id"]] = pd.NA
# ege 2700, 2701, 2702 worden opgeheven
model.link.df = model.link.df[~model.link.df.index.isin([2700, 2701, 2702])]

# basin 1873 wordt verbonden met manning_resistance 1054
model.link.add(
    model.basin[1873],
    model.manning_resistance[1054],
    geometry=link(model.basin[1873].geometry, model.manning_resistance[1054].geometry),
)

# manning_resistance 1308 en 1331 worden verbonden met basin 1873
model.link.add(
    model.manning_resistance[1308],
    model.basin[1873],
    geometry=link(model.manning_resistance[1308].geometry, model.basin[1873].geometry),
)
model.link.add(
    model.basin[1873],
    model.manning_resistance[1331],
    geometry=link(model.basin[1873].geometry, model.manning_resistance[1331].geometry),
)

# level_boundary 26 wordt een outlet
model.update_node(26, "Outlet", data=[outlet_data])

# nieuwe level_boundary benedenstrooms nieuwe outlet 26
boundary_node = model.level_boundary.add(Node(geometry=level_boundary_gdf.at[4, "geometry"]), tables=[level_data])


# basin 1873 wordt verbonden met outlet en outlet met level_boundary
model.link.add(
    model.outlet[26],
    model.basin[1873],
    geometry=link(model.outlet[26].geometry, model.basin[1873].geometry),
)

model.link.add(boundary_node, model.outlet[26])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2388334544

# Kruising Dinkel/Kanaal Almelo Nordhorn corrigeren

# ege 2700, 2701, 2702 worden opgeheven
model.link.df = model.link.df[~model.link.df.index.isin([2690, 2691, 2692, 2693, 2694, 2695, 2696])]

# basin / area splitten bij rode lijn in twee vlakken
line = split_line_gdf.at[3, "geometry"]

total_basin_polygon = model.basin.area.df.at[544, "geometry"]
basin_polygon = next(i for i in model.basin.area.df.at[544, "geometry"].geoms if i.intersects(line))
basin_polygons = split_basin(basin_polygon, line)
model.basin.area.df.loc[544, ["geometry"]] = MultiPolygon(
    [i for i in model.basin.area.df.at[544, "geometry"].geoms if not i.intersects(line)] + [basin_polygons.geoms[0]]
)
model.basin.area.df.loc[model.basin.area.df.index.max() + 1, ["geometry"]] = MultiPolygon([basin_polygons.geoms[1]])

# basin op dinkel bovenstrooms kanaal
dinkel_basin_node = model.basin.add(
    Node(geometry=hydroobject_gdf.loc[2966].geometry.boundary.geoms[1]), tables=basin_data
)

# basin in kanaal
kanaal_basin_node = model.basin.add(
    Node(geometry=hydroobject_gdf.loc[7720].geometry.boundary.geoms[1]), tables=basin_data
)

# links v.a. tabulated_rating_curve 298 (ST01865) en 448 (ST01666) naar dinkel-basin
model.link.add(
    model.tabulated_rating_curve[298],
    dinkel_basin_node,
    geometry=link(model.tabulated_rating_curve[298].geometry, dinkel_basin_node.geometry),
)

model.link.add(
    model.tabulated_rating_curve[448],
    dinkel_basin_node,
    geometry=link(model.tabulated_rating_curve[448].geometry, dinkel_basin_node.geometry),
)

# link v.a. manning_resistance 915 naar dinkel basin
model.link.add(
    model.manning_resistance[915],
    dinkel_basin_node,
    geometry=link(model.manning_resistance[915].geometry, dinkel_basin_node.geometry),
)

# links v.a. dinkel basin naar tabulate_rating_curves 132 (ST02129) en 474 (ST02130)
model.link.add(
    dinkel_basin_node,
    model.tabulated_rating_curve[132],
    geometry=link(dinkel_basin_node.geometry, model.tabulated_rating_curve[132].geometry),
)

model.link.add(
    dinkel_basin_node,
    model.tabulated_rating_curve[474],
    geometry=link(dinkel_basin_node.geometry, model.tabulated_rating_curve[474].geometry),
)

# nieuwe manning_resistance in nieuwe dinkel-basin bovenstrooms kanaal
manning_node = model.manning_resistance.add(
    Node(geometry=hydroobject_gdf.at[7721, "geometry"].interpolate(0.5, normalized=True)), tables=[manning_data]
)

# nieuwe basin verbinden met nieuwe manning_resistance en nieuw kanaal basin
model.link.add(
    dinkel_basin_node,
    manning_node,
    geometry=link(dinkel_basin_node.geometry, manning_node.geometry),
)

model.link.add(
    manning_node,
    kanaal_basin_node,
    geometry=link(manning_node.geometry, kanaal_basin_node.geometry),
)

# nieuw kanaal-basin vervinden met tabulated_rating_curve 471 (ST01051)
model.link.add(
    kanaal_basin_node,
    model.tabulated_rating_curve[471],
    geometry=link(kanaal_basin_node.geometry, model.tabulated_rating_curve[471].geometry),
)

# nieuw kanaal-basin vervinden met manning_resistance 1346
model.link.add(
    kanaal_basin_node,
    model.manning_resistance[1346],
    geometry=link(kanaal_basin_node.geometry, model.manning_resistance[1346].geometry),
)

# nieuwe outletlet bij grensduiker kanaal
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[7746, "geometry"].boundary.geoms[0]), tables=[outlet_data]
)

# nieuwe basin verbinden met outlet verbinden met level_boundary 21
model.link.add(
    outlet_node,
    kanaal_basin_node,
    geometry=link(outlet_node.geometry, kanaal_basin_node.geometry),
)

model.link.add(
    model.level_boundary[21],
    outlet_node,
    geometry=link(model.level_boundary[21].geometry, outlet_node.geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2389192454
model.reverse_link(link_id=2685)
model.remove_node(node_id=2229, remove_links=True)
model.link.add(
    model.basin[1778],
    model.outlet[1080],
    geometry=link(model.basin[1778].geometry, model.outlet[1080].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2389198178
model.reverse_link(link_id=2715)
model.reverse_link(link_id=2720)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390712613

# Oplossen toplogische situatie kanaal Coevorden

# opheffen basin 2243 en basin 2182
model.remove_node(2243, remove_links=True)
model.remove_node(2182, remove_links=True)
model.remove_node(1351, remove_links=True)
model.remove_node(1268, remove_links=True)
model.remove_node(1265, remove_links=True)

# onknippen basin bij rode lijn
line = split_line_gdf.at[4, "geometry"]
basin_area_row = model.basin.area.df[model.basin.area.df.contains(line.centroid)].iloc[0]
basin_area_index = basin_area_row.name
basin_polygon = basin_area_row.geometry.geoms[0]
basin_polygons = split_basin(basin_polygon, line)

model.basin.area.df.loc[basin_area_index, ["geometry"]] = MultiPolygon([basin_polygons.geoms[1]])
model.basin.area.df.loc[model.basin.area.df.index.max() + 1, ["geometry"]] = MultiPolygon([basin_polygons.geoms[0]])

# # verplaatsen basin 1678 naar kruising waterlopen
model.move_node(node_id=1678, geometry=hydroobject_gdf.loc[6594].geometry.boundary.geoms[1])

# verwijderen links 809, 814, 807, 810, 1293, 2772
model.link.df = model.link.df[~model.link.df.index.isin([809, 814, 807, 810, 887])]


# verbinden manning 1270, 1127 en pumps 644, 579 en 649 met basin 1678
for node_id in [1270, 1127]:
    model.link.add(
        model.manning_resistance[node_id],
        model.basin[1678],
        geometry=link(model.manning_resistance[node_id].geometry, model.basin[1678].geometry),
    )

for node_id in [644, 579, 649]:
    model.link.add(
        model.pump[node_id],
        model.basin[1678],
        geometry=link(model.pump[node_id].geometry, model.basin[1678].geometry),
    )

# verplaatsen manning 1267 naar basin-link tussen 1678 en 1678
model.move_node(node_id=1267, geometry=hydroobject_gdf.loc[6609].geometry.boundary.geoms[1])

# maak nieuwe manning-node tussen 1678 en 1897
manning_node = model.manning_resistance.add(
    Node(geometry=hydroobject_gdf.loc[6596].geometry.interpolate(0.5, normalized=True)), tables=[manning_data]
)

# verbinden basin 1897 met manning-node
model.link.add(
    model.basin[1897],
    manning_node,
    geometry=link(model.basin[1897].geometry, manning_node.geometry),
)

model.link.add(
    manning_node,
    model.basin[1678],
    geometry=link(manning_node.geometry, model.basin[1678].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390952469

# Schoonebekerdiep v.a. Twist Bült

# verplaatsen basin 1909 nabij tabulated_rating_curve 383 (ST03607)
model.move_node(1909, geometry=hydroobject_gdf.loc[6865].geometry.boundary.geoms[1])

# verwijderen links 780 en 778
model.link.df = model.link.df[~model.link.df.index.isin([780, 778])]

# toevoegen link tussen tabulated_rating_curve 383 en basin 1909
model.link.add(
    model.tabulated_rating_curve[383],
    model.basin[1909],
    geometry=link(model.tabulated_rating_curve[383].geometry, model.basin[1909].geometry),
)

# toevoegen link tussen manning_resistance 851 en basin 1909
model.link.add(
    model.manning_resistance[851],
    model.basin[1909],
    geometry=link(model.manning_resistance[851].geometry, model.basin[1909].geometry),
)

# opknippen basin 1538 nabij 1909 en verbinden basin 1909 met 1539 via nieuwe manning_knoop
line = split_line_gdf.at[5, "geometry"]
model.split_basin(line=line)
manning_node = model.manning_resistance.add(
    Node(geometry=drop_z(line.intersection(hydroobject_gdf.at[6866, "geometry"]))), tables=[manning_data]
)

model.node.df.loc[1909, "geometry"] = drop_z(model.basin[1909].geometry)
model.link.add(model.basin[1909], manning_node)
model.link.add(manning_node, model.basin[1539], geometry=link(manning_node.geometry, model.basin[1539].geometry))

# verwijderen link 2716,2718,2718,2719
model.link.df = model.link.df[~model.link.df.index.isin([2716, 2717, 2718, 2719])]

# opknippen basin 2181 nabij 1881 en verbinden basin 1881 met 2181 via nieuwe manning_knoop
model.move_node(1881, geometry=hydroobject_gdf.loc[6919].geometry.boundary.geoms[1])
line = split_line_gdf.at[6, "geometry"]
model.split_basin(line=line)
manning_node = model.manning_resistance.add(
    Node(geometry=line.intersection(hydroobject_gdf.at[6879, "geometry"])), tables=[manning_data]
)
model.node.df.loc[1881, "geometry"] = drop_z(model.basin[1881].geometry)
model.link.add(model.basin[1881], manning_node)
model.link.add(manning_node, model.basin[2181], geometry=link(manning_node.geometry, model.basin[2181].geometry))

for node_id in [139, 251, 267, 205]:
    model.link.add(
        model.tabulated_rating_curve[node_id],
        model.basin[1881],
        geometry=link(model.tabulated_rating_curve[node_id].geometry, model.basin[1881].geometry),
    )

model.move_node(1269, hydroobject_gdf.at[7749, "geometry"].interpolate(0.5, normalized=True))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391168839

# Molengoot Hardenberg

# opheffen basin 1903
model.remove_node(1903, remove_links=True)

# verbinden basin 1433 met pump 621
model.link.add(
    model.basin[1433],
    model.pump[621],
    geometry=link(model.basin[1433].geometry, model.pump[621].geometry),
)

# verbinden tabulated_rating_curves 99 en 283 met basin 1433
for node_id in [99, 283]:
    model.link.add(
        model.tabulated_rating_curve[node_id],
        model.basin[1433],
        geometry=link(model.tabulated_rating_curve[node_id].geometry, model.basin[1433].geometry),
    )

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390898004

model.remove_node(1131, remove_links=True)
model.remove_node(1757, remove_links=True)
model.link.add(
    model.basin[1588],
    model.tabulated_rating_curve[112],
    geometry=link(model.basin[1588].geometry, model.tabulated_rating_curve[112].geometry),
)
model.link.add(
    model.basin[1588],
    model.manning_resistance[57],
    geometry=link(model.basin[1588].geometry, model.manning_resistance[57].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391191673

# verwijderen basin 1905
model.remove_node(1905, remove_links=True)

# verbinden manning_resistance 995 met basin 2148
model.link.add(
    model.basin[2148],
    model.manning_resistance[995],
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391460899

# Samenvoegen basin-knopen Overijsselse Vecht & Coevorden Vechtkanaal
for basin_id in [1845, 2244, 2006, 1846]:
    model.merge_basins(node_id=basin_id, to_node_id=2222)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391666745

# Opruimen basins Nieuw-Amsterdam

# opknippen basin 1611 bij rode lijn, area mergen met basin 1879
basin_polygon = model.basin.area.df.set_index("node_id").at[1611, "geometry"]
left_poly, right_poly = split_basin_multi_polygon(basin_polygon, split_line_gdf.at[8, "geometry"])
model.basin.area.df.loc[model.basin.area.df.node_id == 1611, ["geometry"]] = right_poly

left_poly = model.basin.area.df.set_index("node_id").at[1879, "geometry"].union(left_poly)
model.basin.area.df.loc[model.basin.area.df.node_id == 1879, ["geometry"]] = MultiPolygon([left_poly])

# merge basins 2186, 2173, 2022, 1611, 2185 in basin 1902
for basin_id in [2186, 2173, 2022, 1611, 2185]:
    model.merge_basins(node_id=basin_id, to_node_id=1902)

# verplaats 1902 iets bovenstrooms
model.move_node(1902, hydroobject_gdf.at[6615, "geometry"].boundary.geoms[1])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391686269
model.remove_node(2198, remove_links=True)
model.remove_node(2200, remove_links=True)
model.link.add(model.basin[2111], model.pump[671])
model.link.add(model.tabulated_rating_curve[542], model.basin[2111])
model.link.add(model.pump[671], model.basin[2316])
model.link.add(model.basin[2316], model.tabulated_rating_curve[542])

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391710413

model.remove_node(2202, remove_links=True)
model.link.add(model.basin[1590], model.pump[657])
model.link.add(model.manning_resistance[1058], model.basin[1590])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391672700

# Merge basin 2176 in 1605
model.merge_basins(node_id=2176, to_node_id=1605)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391726774
# Merge basins 2206 in 1518
model.merge_basins(node_id=2206, to_node_id=1518, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391734144
# dood takje uit Overijsselse Vecht
model.remove_node(2210, remove_links=True)
model.remove_node(1294, remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391740603

# Merge basin 2225 met 2304
model.merge_basins(node_id=2225, to_node_id=2304)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391815016

# Wetteringe als laterale inflow
model.merge_basins(node_id=2231, to_node_id=1853)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391750536

# Rondom SL00010 opruimen
model.remove_node(2230, remove_links=True)
model.remove_node(2251, remove_links=True)
model.link.add(model.outlet[41], model.level_boundary[15])
model.link.add(model.basin[1442], model.pump[664])
model.link.add(model.basin[1442], model.pump[665])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391820198

# Merge basin 2232 in 1591
model.merge_basins(node_id=2232, to_node_id=1591)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391825301

# Basin 2236 naar LevelBoundary
model.update_node(2236, "LevelBoundary", data=[level_data])

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391829471

# Merge basin 2246 en 1419
model.merge_basins(node_id=2246, to_node_id=1419)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391946915

# Opruimen Elsbeek

# Basin 2256 verplaatsen naar punt
model.move_node(node_id=2256, geometry=hydroobject_gdf.loc[2896].geometry.boundary.geoms[1])

# Basin knippen over lijn
model.basin.area.df.loc[model.basin.area.df.node_id == 2256, ["node_id"]] = pd.NA
model.split_basin(split_line_gdf.at[10, "geometry"])

# Links 446, 1516, 443 en 444 verwijderen
model.link.df = model.link.df[~model.link.df.index.isin([446, 1516, 443, 444])]

# tabulated_rating_curves 202 en 230 verbinden met basin 2256
model.link.add(
    model.tabulated_rating_curve[202],
    model.basin[2256],
    geometry=link(model.tabulated_rating_curve[202].geometry, model.basin[2256].geometry),
)
model.link.add(
    model.tabulated_rating_curve[230],
    model.basin[2256],
    geometry=link(model.tabulated_rating_curve[230].geometry, model.basin[2256].geometry),
)

# resistance 954 verbinden met basin 2256
model.link.add(
    model.manning_resistance[954],
    model.basin[2256],
    geometry=link(model.manning_resistance[954].geometry, model.basin[2256].geometry),
)

# basin 2256 verbinden met resistance 1106
model.link.add(
    model.basin[2256],
    model.manning_resistance[1106],
    geometry=link(model.basin[2256].geometry, model.manning_resistance[1106].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391984234

# Merge basin 2261 in basin 1698
model.merge_basins(node_id=2261, to_node_id=1698)
# model.remove_node(390, remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391995841

# Merge basin 2260 met basin 1645
model.merge_basins(node_id=2260, to_node_id=1645)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392010526
# Merge basin 2220 met basin 1371
model.merge_basins(node_id=2220, to_node_id=1371, are_connected=False)


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392017041

# Kanaal Almelo Nordhorn bij Almelo
model.merge_basins(node_id=2219, to_node_id=1583, are_connected=False)
model.merge_basins(node_id=2209, to_node_id=1583, are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392022887

# Merge basin 2203 met 2227
model.merge_basins(node_id=2203, to_node_id=2227, are_connected=False)
model.remove_node(1219, remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392026739

# Merge basin 2014 met 2144
model.merge_basins(node_id=2014, to_node_id=2144)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392030268

# Merge basin 1696 met 1411
model.merge_basins(node_id=1696, to_node_id=1411)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392037263

# Merge basin 2264 met 1459
model.merge_basins(node_id=2264, to_node_id=1459)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392043973

# Merge basin 2212 en 2310
model.merge_basins(node_id=2212, to_node_id=2310)
poly = model.basin.area.df.at[59, "geometry"].union(model.basin.area.df.set_index("node_id").at[2310, "geometry"])
model.basin.area.df.loc[model.basin.area.df.node_id == 2310, ["geometry"]] = MultiPolygon([poly])

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392048684

# Merge basin 2253 in basin 2228
model.merge_basins(node_id=2253, to_node_id=2228)


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392052379

# Merge basin 2221 in basin 1634
model.merge_basins(node_id=2221, to_node_id=1634)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392076634

# Verbinding rondwaterleiding / Lennelwaterleiding herstellen
model.merge_basins(node_id=1859, to_node_id=2235, are_connected=False)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382572457

# Administratie basin node_id in node_table en Basin / Area correct maken
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest", distance=100)
model.fix_unassigned_basin_area()
model.basin.area.df = model.basin.area.df[~model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]

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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382578661

# opvullen gaten
basin_polygon = model.basin.area.df.union_all()
# holes = [Polygon(interior) for polygon in basin_polygon.buffer(10).buffer(-10).geoms for interior in polygon.interiors]
holes = [Polygon(interior) for interior in basin_polygon.buffer(10).buffer(-10).interiors]
holes_df = gpd.GeoSeries(holes, crs=28992)
holes_df.index = holes_df.index + 1

# splitsen Alemelo-Nordhorn / Overijsselskanaal. Overijsselskanaal zit in HWS
line = split_line_gdf.at[12, "geometry"]
idx = holes_df[holes_df.intersects(line)].index[0]
poly = split_basin(holes_df[holes_df.intersects(line)].iloc[0], line).geoms[0]
poly = model.basin.area.df.set_index("node_id").at[1583, "geometry"].union(poly)
model.basin.area.df.loc[model.basin.area.df.node_id == 1583, ["geometry"]] = MultiPolygon([poly])

# Split Overijsselskanaal bij Zwolsekanaal
line = split_line_gdf.at[13, "geometry"]
poly1, poly2 = split_basin(holes_df[holes_df.intersects(line)].iloc[0], line).geoms

poly1 = model.basin.area.df.set_index("node_id").at[2116, "geometry"].union(poly1)
poly1 = MultiPolygon([i for i in poly1.geoms if i.geom_type == "Polygon"])
model.basin.area.df.loc[model.basin.area.df.node_id == 2116, ["geometry"]] = poly1

poly2 = model.basin.area.df.set_index("node_id").at[2115, "geometry"].union(poly2)
poly2 = MultiPolygon([i for i in poly2.geoms if i.geom_type == "Polygon"] + [holes_df.loc[38], holes_df.loc[29]])
model.basin.area.df.loc[model.basin.area.df.node_id == 2115, ["geometry"]] = poly2

# de rest gaan we automatisch vullen
holes_df = holes_df[~holes_df.index.isin([10, 22, 29, 32, 38, 39, 41])]

drainage_areas_df = drainage_areas_df[drainage_areas_df.buffer(-10).intersects(basin_polygon)]

for _idx, geometry in enumerate(holes_df):
    # select drainage-area
    drainage_area_select = drainage_areas_df[drainage_areas_df.contains(geometry.buffer(-10))]
    if not drainage_area_select.empty:
        if not len(drainage_area_select) == 1:
            raise ValueError("hole contained by multiple drainage areas, can't fix that yet")

        drainage_area = drainage_area_select.iloc[0].geometry

        # find basin_id to merge to
        selected_basins_df = model.basin.area.df[
            model.basin.area.df.node_id.isin(model.basin.node.df[model.basin.node.df.within(drainage_area)].index)
        ].set_index("node_id")
        if selected_basins_df.empty:
            selected_basins_df = model.basin.area.df[
                model.basin.area.df.buffer(-10).intersects(drainage_area)
            ].set_index("node_id")

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

        if isinstance(geometry, Polygon):
            geometry = MultiPolygon([geometry])
        model.basin.area.df.loc[model.basin.area.df.node_id == assigned_basin_id, "geometry"] = geometry
# buffer out small slivers
model.basin.area.df.loc[:, ["geometry"]] = (
    model.basin.area.df.buffer(0.1)
    .buffer(-0.1)
    .apply(lambda x: x if x.geom_type == "MultiPolygon" else MultiPolygon([x]))
)
# %%
# Fix small nodata holes (slivers in the basins: https://github.com/Deltares/Ribasim-NL/issues/316 )
ribasim_areas_gdf = ribasim_areas_gdf.to_crs(model.basin.area.df.crs)
ribasim_areas_gdf.loc[:, "geometry"] = ribasim_areas_gdf.buffer(-0.01).buffer(0.01)

# Exclude Twentekanaal by setting their geometry to NaN
codes_to_exclude = [
    "AFW_E/20",
    "AFW_E/TWK1600/30",
    "AFW_E/20/005",
    "AFW_E/TWK1600/20",
    "AFW_E/20/10",
    "AFW_E/TWK2500/10",
    "AFW_E/ENSCHEDE 3",
    "AFW_E/20/010",
    "AFW_E/20/030",
    "AFW_E/24/010",
]
ribasim_areas_gdf.loc[ribasim_areas_gdf["code"].isin(codes_to_exclude), "geometry"] = pd.NA

# Process basin area
processed_basin_area_df = model.basin.area.df.copy()
processed_basin_area_df = processed_basin_area_df.dissolve(by="node_id").reset_index()
processed_basin_area_df = processed_basin_area_df[processed_basin_area_df["geometry"].notna()]
processed_basin_area_df = processed_basin_area_df[processed_basin_area_df.geometry.area > 0]
processed_basin_area_df = processed_basin_area_df[~processed_basin_area_df.geometry.is_empty]

# Combine all geometries into a single polygon
combined_geometry = processed_basin_area_df.geometry.union_all()
combined_basin_area_gdf = gpd.GeoDataFrame(geometry=[combined_geometry], crs=processed_basin_area_df.crs)

# Get the bounding box and calculate internal NoData areas
bounding_box = combined_basin_area_gdf.geometry.union_all().envelope
internal_no_data_areas = bounding_box.difference(combined_geometry)
internal_no_data_gdf = gpd.GeoDataFrame(geometry=[internal_no_data_areas], crs=combined_basin_area_gdf.crs)
exploded_internal_no_data_gdf = internal_no_data_gdf.explode(index_parts=True).reset_index(drop=True)

# Apply area threshold for sliver removal
threshold_area = 10
exploded_internal_no_data_gdf = exploded_internal_no_data_gdf[
    exploded_internal_no_data_gdf.geometry.area > threshold_area
]

# Clip to remove areas where ribasim_areas_gdf is NoData (NaN geometries)
ribasim_not_na_gdf = ribasim_areas_gdf[~ribasim_areas_gdf.geometry.isna()]
exploded_internal_no_data_gdf = gpd.overlay(
    exploded_internal_no_data_gdf, ribasim_not_na_gdf, how="intersection", keep_geom_type=True
)
unique_codes = exploded_internal_no_data_gdf["code"].unique()
filtered_ribasim_areas_gdf = ribasim_areas_gdf[ribasim_areas_gdf["code"].isin(unique_codes)]
combined_basin_areas_gdf = gpd.overlay(
    filtered_ribasim_areas_gdf, model.basin.area.df, how="union", keep_geom_type=True
).explode()
combined_basin_areas_gdf["area"] = combined_basin_areas_gdf.geometry.area
non_null_basin_areas_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()]
largest_area_node_ids = non_null_basin_areas_gdf.loc[
    non_null_basin_areas_gdf.groupby("code")["area"].idxmax(), ["code", "node_id"]
]
combined_basin_areas_gdf = combined_basin_areas_gdf.merge(
    largest_area_node_ids, on="code", how="left", suffixes=("", "_largest")
)

# Fill missing node_id with the largest_area node_id
combined_basin_areas_gdf["node_id"] = combined_basin_areas_gdf["node_id"].fillna(
    combined_basin_areas_gdf["node_id_largest"]
)

combined_basin_areas_gdf.drop(columns=["node_id_largest"], inplace=True)
combined_basin_areas_gdf = combined_basin_areas_gdf.drop_duplicates()
combined_basin_areas_gdf = combined_basin_areas_gdf.dissolve(by="node_id").reset_index()
combined_basin_areas_gdf = combined_basin_areas_gdf[["node_id", "geometry"]]
combined_basin_areas_gdf.index.name = "fid"
model.basin.area.df = combined_basin_areas_gdf

# buffer out small slivers
model.basin.area.df.loc[:, ["geometry"]] = (
    model.basin.area.df.buffer(0.1)
    .buffer(-0.1)
    .apply(lambda x: x if x.geom_type == "MultiPolygon" else MultiPolygon([x]))
)
# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)


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
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_object_type", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump", "TabulatedRatingCurve"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "ManningResistance"], "columns": {"name": ""}},
        {"node_types": ["LevelBoundary", "FlowBoundary"], "columns": {"meta_name": "name"}},
    ],
    names=names,
)

# %%
actions = [
    "remove_basin_area",
    "remove_node",
    "remove_link",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_link",
    "connect_basins",
    "move_node",
    "update_node",
    "redirect_link",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_gpkg).name.to_list()]
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    df = gpd.read_file(model_edits_gpkg, layer=action, fid_as_index=True)
    if "order" in df.columns:
        df.sort_values("order", inplace=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = row._asdict()
        if inspect.getfullargspec(method).varkw != "kwargs":
            keywords = inspect.getfullargspec(method).args
            kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# remove unassigned basin area
model.remove_unassigned_basin_area()
# %% Custom model adjustments

# fmt: off
CUSTOM_REDIRECT_LINKS = [
    {"link_id": 89, "to_node_id": 1561},
    {"link_id": 1989, "from_node_id": 2333},
    {"link_id": 1990, "from_node_id": 2333},
]

MERGE_TO_NODE = [
    (2115, 1405), (1378, 1431), (2211, 1727), (1538, 33), (1963, 1518),
    (2245, 1818), (2026, 1818), (1412, 2107), (1592, 1765), (1765, 1817),
    (2159, 1890), (1654, 2163), (1628, 2143), (1821, 2143), (2144, 2143),
    (2116, 1730), (2177, 1730),
]

NODES_TO_REMOVE = [
    619, 660, 698, 1243, 1242, 1252, 836, 80, 265, 126,
    166, 313, 393, 146, 835, 1370, 358, 188, 219, 345,
    654, 1045, 125, 601, 121, 590, 624, 573, 570, 657,
    652, 633, 178, 319, 395, 114, 442, 562, 438, 167,
    561, 456, 673, 1128, 413, 842, 463, 341, 235,
]

MERGE_TO_BASIN = [
    (1488, 1834), (1848, 2158), (1891, 2158), (1994, 2158), (1648, 1826),
    (1646, 1513), (1897, 1678), (2181, 1678), (1678, 1700), (1876, 1633),
    (2187, 1873), (2174, 1873), (1875, 1879), (1908, 1879), (1902, 1879),
    (1441, 1879), (1957, 1879), (1717, 1528), (1995, 1442), (2262, 1431),
    (1853, 1852), (2137, 2138), (1819, 2138), (33, 1377), (1560, 1478),
    (2228, 2121), (2050, 1812), (1811, 1812), (1386, 2157), (2307, 2157),
    (1610, 1454), (1664, 1588), (2234, 1418), (2317, 1700), (1982, 1431),
    (1589, 1431), (1584, 1817), (1817, 2135), (1588, 1561), (1826, 1843),
]

FULL_CONTROL_REDIRECT_LINKS = [
    {"link_id": 1400, "from_node_id": 2300},
    {"link_id": 1183, "to_node_id": 1575},
]

FULL_CONTROL_REVERSE_LINK_IDS = [
    39, 2105, 1328, 104, 1684, 547, 1001, 1732, 1423, 1013,
    255, 2407, 725, 2201, 249, 2113, 1782, 664, 1086, 2261,
    2079, 2713, 2681, 2824, 1062, 2180, 1173, 1343, 427, 1712,
    36, 1777, 1674, 549, 2822, 1337, 27, 1891, 796, 2383,
    131, 1953, 1183, 1993,
]
# fmt: on

# --- Connectivity and topology edits ---
redirect_links(model, CUSTOM_REDIRECT_LINKS)
merge_basin_pairs(model, MERGE_TO_NODE)
model.merge_basins(node_id=2254, to_node_id=1493, are_connected=False)
remove_nodes(model, NODES_TO_REMOVE)
merge_basin_pairs(model, MERGE_TO_BASIN)
reverse_links(model, [1171, 1475])

# --- Full-control topology fixes moved here to keep node_ids stable for steering ---
redirect_links(model, FULL_CONTROL_REDIRECT_LINKS)
reverse_links(model, FULL_CONTROL_REVERSE_LINK_IDS)

# Basin 1843 splitsen met Wateraanvoer OBJECTID 377, plus extra basin + kunstwerken.
assert model.basin.area.df is not None
assert model.basin.profile.df is not None
assert model.basin.static.df is not None
assert model.basin.state.df is not None

basin_1843_area_df = model.basin.area.df.loc[model.basin.area.df.node_id == 1843].copy()
if basin_1843_area_df.empty:
    raise ValueError("Geen bestaande basin_area gevonden voor node_id 1843")

old_basin_1843_geometry = basin_1843_area_df.geometry.union_all()
if old_basin_1843_geometry.geom_type == "Polygon":
    old_basin_1843_geometry = MultiPolygon([old_basin_1843_geometry])
elif old_basin_1843_geometry.geom_type != "MultiPolygon":
    raise ValueError(f"Oude geometry voor node_id 1843 heeft ongeldig type: {old_basin_1843_geometry.geom_type}")

old_basin_1843_node_geometry = model.basin[1843].geometry

streefpeil_1843 = first_meta_streefpeil(basin_1843_area_df)

wateraanvoer_df = gpd.read_file(wateraanvoer_shp)
objectid_column = next((column for column in wateraanvoer_df.columns if column.upper() == "OBJECTID"), None)
if objectid_column is None:
    raise ValueError("Kolom OBJECTID niet gevonden in Wateraanvoer.shp")

new_basin_area_df = wateraanvoer_df.loc[wateraanvoer_df[objectid_column] == 377].copy()
if new_basin_area_df.empty:
    raise ValueError(f"Geen feature gevonden met OBJECTID=377 in {wateraanvoer_shp}")

basin_2148_area_df = wateraanvoer_df.loc[wateraanvoer_df[objectid_column] == 1753].copy()
if basin_2148_area_df.empty:
    raise ValueError(f"Geen feature gevonden met OBJECTID=1753 in {wateraanvoer_shp}")

if new_basin_area_df.crs is not None and new_basin_area_df.crs != model.crs:
    new_basin_area_df = new_basin_area_df.to_crs(model.crs)
    basin_2148_area_df = basin_2148_area_df.to_crs(model.crs)

new_basin_geometry = clean_basin_area_geometry(
    new_basin_area_df.geometry.union_all(),
    label="OBJECTID 377",
)
new_basin_1843_geometry = clean_basin_area_geometry(
    old_basin_1843_geometry.difference(new_basin_geometry),
    label="node_id 1843",
)
basin_2148_geometry = clean_basin_area_geometry(
    basin_2148_area_df.geometry.union_all(),
    label="OBJECTID 1753",
)

model.basin.area.df = model.basin.area.df.loc[model.basin.area.df.node_id != 1843].copy()
model.add_basin_area(geometry=new_basin_1843_geometry, node_id=1843, meta_streefpeil=streefpeil_1843)
model.move_node(node_id=1843, geometry=Point(237696.58, 496023.31))

basin_2148_area_existing_df = model.basin.area.df.loc[model.basin.area.df.node_id == 2148].copy()
if basin_2148_area_existing_df.empty:
    raise ValueError("Geen bestaande basin_area gevonden voor node_id 2148")

streefpeil_2148 = first_meta_streefpeil(basin_2148_area_existing_df)
model.basin.area.df = model.basin.area.df.loc[model.basin.area.df.node_id != 2148].copy()
model.add_basin_area(geometry=basin_2148_geometry, node_id=2148, meta_streefpeil=streefpeil_2148)

new_basin_node_id = 2340
model.basin.add(Node(node_id=new_basin_node_id, geometry=old_basin_1843_node_geometry, name="Basin Westerhaar aanvoer"))

for basin_table in [model.basin.profile, model.basin.static, model.basin.state]:
    table_df = basin_table.df
    source_rows = table_df.loc[table_df["node_id"] == 1843].copy()
    if source_rows.empty:
        raise ValueError(f"Geen brondata gevonden voor basin-tabel {type(basin_table).__name__} en node_id 1843")
    source_rows.loc[:, "node_id"] = new_basin_node_id
    basin_table.df = pd.concat([table_df, source_rows], ignore_index=True)

model.add_basin_area(
    geometry=new_basin_geometry,
    node_id=new_basin_node_id,
    meta_streefpeil=streefpeil_1843,
)

aanvoer_pump = model.pump.add(
    Node(node_id=2341, geometry=Point(238834.91, 496511.08), name="Aanvoergemaal Westerhaar"),
    tables=[pump_data],
)
model.link.add(model.basin[1843], aanvoer_pump)
model.link.add(aanvoer_pump, model.basin[1455])

westerhaar_uitlaat_outlet = model.outlet.add(
    Node(node_id=2342, geometry=Point(238833.885, 496508.498), name="Uitlaat Westerhaar"),
    tables=[outlet_data],
)
if not (
    can_connect(model.basin[new_basin_node_id].node_type, "Outlet")
    and can_connect("Outlet", model.basin[1843].node_type)
):
    raise ValueError(
        "Gevraagde route met Outlet is ongeldig: "
        f"{model.basin[new_basin_node_id].node_type} -> Outlet -> {model.basin[1843].node_type} "
        f"(nodes: {new_basin_node_id} -> {westerhaar_uitlaat_outlet.node_id} -> 1843)."
    )
model.link.add(model.basin[new_basin_node_id], westerhaar_uitlaat_outlet)
model.link.add(westerhaar_uitlaat_outlet, model.basin[1843])

inlaatstuw_outlet = model.outlet.add(
    Node(node_id=2343, geometry=Point(243225.458, 495478.36), name="Inlaatstuw"),
    tables=[outlet_data],
)
model.link.add(model.basin[1844], inlaatstuw_outlet)
model.link.add(inlaatstuw_outlet, model.basin[1450])

inlaat_outlet_1708 = model.outlet.add(
    Node(node_id=2344, geometry=Point(238366.036, 496286.96), name="Inlaat"),
    tables=[outlet_data],
)
model.link.add(model.basin[1843], inlaat_outlet_1708)
model.link.add(inlaat_outlet_1708, model.basin[1708])

uitlaat_outlet_1569 = model.outlet.add(
    Node(node_id=2345, geometry=Point(241738.30642, 513291.13972), name="Uitlaat"),
    tables=[outlet_data],
)
model.link.add(model.basin[2068], uitlaat_outlet_1569)
model.link.add(uitlaat_outlet_1569, model.basin[1569])

redirect_links(
    model,
    [
        {"link_id": 525, "to_node_id": new_basin_node_id},
        {"link_id": 1737, "from_node_id": new_basin_node_id},
        {"link_id": 1890, "from_node_id": 1843},
        {"link_id": 102, "to_node_id": 1843},
        {"link_id": 1891, "to_node_id": 1843},
        {"link_id": 1889, "from_node_id": 1843},
        {"link_id": 1403, "from_node_id": new_basin_node_id},
        {"link_id": 523, "to_node_id": new_basin_node_id},
        {"link_id": 2840, "from_node_id": 2341, "to_node_id": new_basin_node_id},
    ],
)

from_node = model.get_node(2298)
to_node = model.get_node(1583)
if not (can_connect(from_node.node_type, "Outlet") and can_connect("Outlet", to_node.node_type)):
    raise ValueError(
        f"Gevraagde route met Outlet is ongeldig: {from_node.node_type} -> Outlet -> {to_node.node_type} "
        f"(nodes: {from_node.node_id} -> {to_node.node_id})."
    )
extra_connection_node = model.outlet.add(
    Node(node_id=2346, geometry=Point(241321.928, 486550.60)),
    tables=[outlet_data],
)
model.link.add(from_node, extra_connection_node)
model.link.add(extra_connection_node, to_node)

from_node = model.get_node(1575)
to_node = model.get_node(2304)
if not (can_connect(from_node.node_type, "Outlet") and can_connect("Outlet", to_node.node_type)):
    raise ValueError(
        f"Gevraagde route met Outlet is ongeldig: {from_node.node_type} -> Outlet -> {to_node.node_type} "
        f"(nodes: {from_node.node_id} -> {to_node.node_id})."
    )
nieuwe_drostendiep_outlet = model.outlet.add(
    Node(node_id=2347, geometry=Point(250205.53, 524562.22), name="Uitlaat Nieuwe Drostendiep"),
    tables=[outlet_data],
)
model.link.add(from_node, nieuwe_drostendiep_outlet)
model.link.add(nieuwe_drostendiep_outlet, to_node)

# Deze GDB-typefix stond pre-refactor in 04_add_full_control.py, dus na 02_prepare_model.
# Dynamisch opnieuw zoeken in 01 geeft andere matches omdat link-/nodegeometrie dan nog niet geprepareerd is.
# Daarom de pre-refactor uitkomst vastzetten, zodat de refactor geen node-types en sturing verandert.
gdb_outlet_node_ids = [1184, 1336, 1337, 1368, 1369]
gdb_pump_node_ids = []

# fmt: off
OUTLET_TYPE_FIX_NODE_IDS = [
    1147, 1307, 1032, 1304, 1340, 1314, 1301, 1329, 1359, 1146,
    1251, 1140, 1335, 1237, 1238, 1177, 1153, 1156, 1359, 1136,
    1137, 1168, 1169, 1167, 1353, 1142, *gdb_outlet_node_ids,
]

MANNING_TYPE_FIX_NODE_IDS = [
    778, 882, 824, 689, 692, 908, 868, 724, 893, 863,
    293, 937, 894, 749, 936, 878, 713, 200, 869, 958,
    952, 822, 768, 49, 771, 801, 806, 859,
]

OBSOLETE_NODE_IDS = [
    1141, 614, 88, 129, 48, 147, 1151, 497, 462, 417,
    1142, 3008, 431, 288, 631, 637, 328, 699, 505, 206,
    321, 392, 488, 1343, 305, 330, 51, 719, 380, 711,
]

CONNECTED_BASIN_MERGES = [
    (1904, 1675), (1389, 2138), (2194, 2138), (1443, 1371), (1571, 1371),
    (2241, 1864), (1869, 1621), (2215, 1978), (1987, 1433), (2263, 1721),
    (1973, 1551), (1988, 1775), (2101, 1775), (1496, 1838), (2196, 1838),
    (2072, 1540), (1976, 1540), (1770, 1540), (2314, 1627), (2058, 2138),
    (1738, 2138), (2233, 1529), (1529, 1444), (1630, 2123), (1966, 1467),
]

UNCONNECTED_BASIN_MERGES = [
    (2094, 1681), (1863, 1779), (1787, 2333), (1642, 1510),
]

FINAL_BASIN_MERGES = [
    (1405, 1730), (2178, 1730),  # Kanaal Almelo Haandrik
    (1873, 1878), (1879, 1878),  # Verlengde Hoogeveensche Vaart
    (1633, 1700),  # Stieltjeskanaalsluis
]
# fmt: on

update_nodes(model, OUTLET_TYPE_FIX_NODE_IDS, "Outlet")
update_nodes(model, MANNING_TYPE_FIX_NODE_IDS, "ManningResistance")
update_nodes(model, gdb_pump_node_ids, "Pump")
remove_nodes(model, OBSOLETE_NODE_IDS)

model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 2300, "level"] = 15.33

merge_basin_pairs(model, CONNECTED_BASIN_MERGES)
merge_basin_pairs(model, UNCONNECTED_BASIN_MERGES, are_connected=False)
merge_basin_pairs(model, FINAL_BASIN_MERGES)
# %%

# sanitize node-table
for node_id in model.tabulated_rating_curve.node.df.index:
    model.update_node(node_id=node_id, node_type="Outlet")

# ManningResistance that are duikersifonhevel to outlet
for node_id in model.manning_resistance.node.df[
    model.manning_resistance.node.df["meta_object_type"] == "duikersifonhevel"
].index:
    model.update_node(node_id=node_id, node_type="Outlet")

# nodes we've added do not have category, we fill with hoofdwater
model.node.df.loc[model.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# %%
model.node.df.loc[model.flow_boundary.node.df.index, "meta_categorie"] = "buitenlandse aanvoer"

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

#  %% write model
model.basin.area.df.loc[:, ["meta_area"]] = model.basin.area.df.area
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%
if run_model:
    result = model.run()
    assert result.exit_code == 0

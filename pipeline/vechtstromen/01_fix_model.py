# %%
import inspect
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pandas as pd
import shapely
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, pump
from ribasim.validation import can_connect
from ribasim_nl.geometry import drop_z, link, polygon_parts, split_basin, split_basin_multi_polygon
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.keys import KEY_COLUMN, NODE_ARGUMENTS
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely import wkt
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import nearest_points

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()
authority = "Vechtstromen"
name = "vechtstromen"
run_model = False

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
fix_user_data_gpkg = cloud.joinpath(authority, "verwerkt/fix_user_data.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")
lhm_gemaal_gdb = cloud.joinpath(authority, "verwerkt", "1_ontvangen_data", "LHM20230418.gdb")
wateraanvoer_shp = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/wateraanvoergebieden_20250416/Wateraanvoer.shp")
hws_model_dir = cloud.joinpath("Rijkswaterstaat/modellen/hws_2025_10_1")
hws_model_toml = hws_model_dir / "hws.toml"

# %%
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
split_line_gdf = gpd.read_file(fix_user_data_gpkg, layer="split_basins", fid_as_index=True)
level_boundary_gdf = gpd.read_file(fix_user_data_gpkg, layer="level_boundary", fid_as_index=True)
areas_gdf = gpd.read_file(areas_gpkg, layer="areas")

# the areas at the crossing of the Dinkel and the Kanaal Almelo-Nordhorn, of the basin of a LevelBoundary
DINKEL_KANAAL = [
    *(f"AFW_E/35/0{i}0" for i in range(1, 9)),
    "AFW_E/35000005/010",
    "AFW_E/37/010",
    "AFW_E/37BYA/020",
    "AFW_E/38/020",
    "AFW_E/38/030",
    "AFW_E/38/040",
    "AFW_E/38BYA/010",
    "AFW_E/39/120",
    "AFW_E/39/130",
    "AFW_E/39PO/030",
]

model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id
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
outlet_data = outlet.Static(flow_rate=[5.0])


def basin_area_of(model: Model, node_id: int) -> MultiPolygon:
    """The basin area of `node_id`."""
    assert model.basin.area.df is not None
    return MultiPolygon(polygon_parts(model.basin.area.df.loc[model.basin.area.df.node_id == node_id].union_all()))


def split_basin_area_to(model: Model, area: BaseGeometry, line: LineString, to_node_id: int) -> None:
    """Split the polygon of `area` that `line` crosses, and give the part nearest to basin `to_node_id` to it."""
    polygon = next(p for p in polygon_parts(area) if p.intersects(line))
    part = min(split_basin(polygon, line).geoms, key=lambda p: p.distance(model.basin[to_node_id].geometry))
    model.add_basin_area(part, node_id=to_node_id)


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

        def to_point(geom: Any) -> Point:
            return (
                geom
                if geom.geom_type == "Point"
                else geom.interpolate(0.5, normalized=True)
                if geom.geom_type == "LineString"
                else geom.centroid
            )

        geometry = geometry.map(to_point)

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
model.node.df.loc[basin_mask, "geometry"] = model.node.df.loc[basin_mask, "geometry"].apply(drop_z)  # ty: ignore[no-matching-overload]

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2352686763

# Toevoegen benedenstroomse randvoorwaarden Beneden Dinkel

# verander basin met node_id 2250 naar type level_boundary
model.update_node(n("down:DK12209"), "LevelBoundary", data=[level_data])


# verplaats basin 1375 naar het hydroobject
node_id = n("down:ST01112")

model.node.df.loc[node_id, "geometry"] = cast(LineString, hydroobject_gdf.at[3135, "geometry"]).interpolate(  # ty: ignore[invalid-assignment]
    0.5, normalized=True
)
link_ids = model.link.df[
    (model.link.df.from_node_id == node_id) | (model.link.df.to_node_id == node_id)
].index.to_list()
model.reset_link_geometry(link_ids=link_ids)

# verplaats basin 1375 naar het hydroobject


# verbind basins met level_boundaries
for fid, node_id in [(1, n("down:ST01112")), (2, n("down:ow:WL00364-2"))]:
    boundary_node_geometry = cast(Point, level_boundary_gdf.at[fid, "geometry"])

    # line for interpolation
    basin_node_point = cast(Point, model.basin.node.df.at[node_id, "geometry"])
    basin_node_geometry = Point(basin_node_point.x, basin_node_point.y)
    line_geometry = LineString((basin_node_geometry, boundary_node_geometry))

    # define level_boundary_node
    boundary_node = model.level_boundary.add(Node(geometry=boundary_node_geometry), tables=[level_data])
    # the model edits remove this second LevelBoundary at the same place, by its key
    level_node = model.level_boundary.add(
        Node(geometry=boundary_node_geometry, meta_key=f"duplicate:level_boundary:{fid}"),
        tables=[level_data],
    )

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
remove_node_ids = [
    n("up:ST02055"),
    n("up:ow:WL00588-3"),
    n("up:DK29483"),
    n("up:ow:WL01009-6"),
    n("up:GM00020"),
    n("up:GM00181"),
    n("up:SL00015"),
    n("up:DK31294"),
    n("up:ow:WL00625-4@2"),
    n("bnd:32"),
]

# remove by link so we also remove all resistance nodes in between
link_df = model.link.df[
    model.link.df.from_node_id.isin(remove_node_ids) | model.link.df.to_node_id.isin(remove_node_ids)
][["from_node_id", "to_node_id"]]

for row in link_df.itertuples():
    model.remove_link(
        from_node_id=cast(int, row.from_node_id), to_node_id=cast(int, row.to_node_id), remove_disconnected_nodes=True
    )

# add level_boundaries at twentekanaal for later coupling
hws_model = Model.read(hws_model_toml)
basin_ids = hws_model.node.df[hws_model.node.df.name.str.contains("Twentekanaal")].index.to_list()
assert hws_model.basin.area.df is not None
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

    # several of these boundaries are at the same place: their key follows from the node they connect to
    boundary_node = model.level_boundary.add(
        Node(
            geometry=boundary_node_geometry,
            meta_key=f"twentekanaal:{model.node.df.at[node_id, KEY_COLUMN]}",
        ),
        tables=[level_data],
    )

    # draw link in the correct direction
    if model.link.df.from_node_id.isin([node_id]).any():  # supply
        model.link.add(boundary_node, node, geometry=link(boundary_node.geometry, node.geometry))
    else:
        model.link.add(node, boundary_node, geometry=link(node.geometry, boundary_node.geometry))

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385525533

# Opruimen situatie rondom gemaal Oude Drostendiep

# pumps met node_id 639, 608 en 603 op te heffen (1 gemaal ipv 3)
remove_node_ids = [n("GM00259"), n("GM00257"), n("GM00256")]

for node_id in remove_node_ids:
    model.remove_node(node_id, remove_links=True)

# remove by link so we also remove all resistance nodes in between
link_df = model.link.df[
    model.link.df.from_node_id.isin(remove_node_ids) | model.link.df.to_node_id.isin(remove_node_ids)
][["from_node_id", "to_node_id"]]

for row in link_df.itertuples():
    model.remove_link(
        from_node_id=cast(int, row.from_node_id), to_node_id=cast(int, row.to_node_id), remove_disconnected_nodes=True
    )

# basin met node_id 1436 te verplaatsen naar locatie basin node_id 2259
basin_id = n("up:GM00256")
model.node.df.loc[basin_id, "geometry"] = model.basin[n("up:GM00258")].geometry  # ty: ignore[invalid-assignment]
link_ids = model.link.df[
    (model.link.df.from_node_id == basin_id) | (model.link.df.to_node_id == basin_id)
].index.to_list()

model.reset_link_geometry(link_ids=link_ids)

# basin met node_id 2259 opheffen (klein niets-zeggend bakje)
model.remove_node(n("up:GM00258"), remove_links=True)

# stuw ST05005 (node_id 361) verbinden met basin met node_id 1436
model.link.add(model.tabulated_rating_curve[n("ST05005")], model.basin[basin_id])
model.link.add(model.basin[basin_id], model.pump[n("GM00258")])

# basin met node_id 2250 verplaatsen naar logische plek bovenstrooms ST05005 en bendenstrooms ST02886 op hydroobjec
basin_id = n("up:ST05005")
model.node.df.loc[basin_id, ["geometry"]] = cast(LineString, hydroobject_gdf.at[6444, "geometry"]).interpolate(  # ty: ignore[invalid-assignment]
    0.5, normalized=True
)

link_ids = model.link.df[
    (model.link.df.from_node_id == basin_id) | (model.link.df.to_node_id == basin_id)
].index.to_list()

model.reset_link_geometry(link_ids=link_ids)

# the part of the area across the line goes to basin 2255
area = areas_gdf.loc[areas_gdf["code"] == "BC4070775"].union_all()
split_basin_area_to(model, area, cast(LineString, split_line_gdf.at[9, "geometry"]), n("up:ST05005"))

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2385409772

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2386671759


# basin 2224 en manning_resistance 898 (DK28491) opheffen
# tabulated_rating_cuves 336 (ST03745) en 238 (ST03744) opheffen
remove_node_ids = [n("up:DK28491"), n("DK28491"), n("ST03745"), n("ST03744")]

for node_id in remove_node_ids:
    model.remove_node(node_id, remove_links=True)

# pump 667 (GM00088) verbinden met basin 1495
model.link.add(model.pump[n("GM00088")], model.basin[n("up:SL00018")])

# model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387026622

# opruimen basin at Amsterdamscheveld
model.remove_node(n("up:ST03258"), remove_links=True)

# verbinden basin node_id 1680 met tabulated_rating_curve node_id 101 en 125
model.link.add(model.basin[n("up:DK18674")], model.tabulated_rating_curve[n("ST03258")])
model.link.add(model.basin[n("up:DK18674")], model.tabulated_rating_curve[n("ST04330")])

# verbinden pump node_id 622 met basin node_id 1680
model.link.add(model.pump[n("GM00166")], model.basin[n("up:DK18674")])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387056481

# Fix stieltjeskanaal

# split basin_area bij manning_resistance node_id 1365

line = cast(LineString, split_line_gdf.at[1, "geometry"])
split_basin_area_to(model, basin_area_of(model, n("up:DK22251")), line, n("up:ow:WL03771-5"))

# hef basin node_id 1901 op
model.remove_node(n("up:GM00246"), remove_links=True)

# hef pump node_id 574 (GM00246) en node_id 638 (GM00249) op
model.remove_node(n("GM00246"), remove_links=True)
model.remove_node(n("GM00249"), remove_links=True)

# verbind basin node_id 1876 met pump node_ids 626 (GM00248) en 654 (GM00247)
model.link.add(model.basin[n("up:ow:WL03771-5")], model.pump[n("GM00248")])
model.link.add(model.basin[n("up:ow:WL03771-5")], model.pump[n("GM00247")])
model.link.add(model.tabulated_rating_curve[n("ST03276")], model.basin[n("up:ow:WL03771-5")])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387815013

# opruimen Zwinderskanaal


# split basin_area bij rode lijn
line = cast(LineString, split_line_gdf.at[2, "geometry"])
split_basin_area_to(model, basin_area_of(model, n("up:DK22039")), line, n("up:DK21370"))

# verwijderen basin 2226, 2258, 2242 en manning_resistance 1366
for node_id in [n("up:ST03499"), n("up:ST03138"), n("up:WL03769-1___0"), n("WL02749-0___0"), n("ow:WL03769-0")]:
    model.remove_node(node_id, remove_links=True)

# verbinden tabulated_rating_curves 327 (ST03499) en 510 (ST03198) met basin 1897
model.link.add(model.basin[n("up:DK21370")], model.tabulated_rating_curve[n("ST03499")])
model.link.add(model.tabulated_rating_curve[n("ST03198")], model.basin[n("up:DK21370")])

# verbinden basin 1897 met tabulated_rating_curve 279 (ST03138)
model.link.add(model.basin[n("up:DK21370")], model.tabulated_rating_curve[n("ST03138")])

# verbinden basin 1897 met manning_resistance 1351 en 1352
model.link.add(model.basin[n("up:DK21370")], model.manning_resistance[n("WL03769-1___0")])
model.link.add(model.basin[n("up:DK21370")], model.manning_resistance[n("ow:WL03696-0")])

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2387888742

# Oplossen situatie Van Echtenskanaal/Scholtenskanaal Klazinaveen

# verplaatsen level_boundary 33 naar splitsing scholtenskanaal/echtenskanaal en omzetten naar basin
outlet_node_geometry = model.level_boundary[n("bnd:33")].geometry
model.update_node(n("bnd:33"), "Basin", data=basin_data)
model.move_node(node_id=n("bnd:33"), geometry=hydroobject_gdf.loc[2679].geometry.boundary.geoms[1])

# plaatsen outlet bij oorspronkelijke plaats level_boundary 33
outlet_node = model.outlet.add(Node(geometry=outlet_node_geometry), tables=[outlet_data])

# plaatsen nieuwe level_boundary op scholtenskanaal aan H&A zijde scholtenskanaal van outlet
boundary_node = model.level_boundary.add(
    Node(geometry=cast(Point, level_boundary_gdf.at[3, "geometry"])), tables=[level_data]
)

# toevoegen links vanaf nieuwe basin 33 naar nieuwe outlet naar nieuwe boundary
model.link.add(
    model.basin[n("bnd:33")], outlet_node, geometry=link(model.basin[n("bnd:33")].geometry, outlet_node.geometry)
)
model.link.add(outlet_node, boundary_node)

# opheffen manning_resistance 1330 bij GM00213
model.remove_node(n("WL08126"), remove_links=True)

# verbinden nieuwe basin met outlet en oorspronkijke manning_knopen en pompen in oorspronkelijke richting
for link_id in [
    lk("bnd:33", "DK20536"),
    lk("bnd:33", "DK25528"),
    lk("bnd:33", "GM00137"),
    lk("bnd:33", "GM00213"),
    lk("bnd:33", "DK17914"),
]:
    model.reverse_link(link_id=link_id)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2388009499

# basin met node_id 1873 gaat richting geesburg
model.move_node(node_id=n("up:ow:WL07329-0"), geometry=hydroobject_gdf.loc[6616].geometry.boundary.geoms[1])
assert model.basin.area.df is not None
model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != n("up:ow:WL07329-0")]
# ege 2700, 2701, 2702 worden opgeheven
model.link.df = model.link.df[
    ~model.link.df.index.isin([lk("bnd:26", "ow:WL03773-2"), lk("bnd:26", "ow:WL03158-2"), lk("bnd:26", "DK28693")])
]

# basin 1873 wordt verbonden met manning_resistance 1054
model.link.add(
    model.basin[n("up:ow:WL07329-0")],
    model.manning_resistance[n("DK28693")],
    geometry=link(model.basin[n("up:ow:WL07329-0")].geometry, model.manning_resistance[n("DK28693")].geometry),
)

# manning_resistance 1308 en 1331 worden verbonden met basin 1873
model.link.add(
    model.manning_resistance[n("ow:WL03158-2")],
    model.basin[n("up:ow:WL07329-0")],
    geometry=link(model.manning_resistance[n("ow:WL03158-2")].geometry, model.basin[n("up:ow:WL07329-0")].geometry),
)
model.link.add(
    model.basin[n("up:ow:WL07329-0")],
    model.manning_resistance[n("ow:WL03773-2")],
    geometry=link(model.basin[n("up:ow:WL07329-0")].geometry, model.manning_resistance[n("ow:WL03773-2")].geometry),
)

# level_boundary 26 wordt een outlet
model.update_node(n("bnd:26"), "Outlet", data=[outlet_data])

# nieuwe level_boundary benedenstrooms nieuwe outlet 26
boundary_node = model.level_boundary.add(
    Node(geometry=cast(Point, level_boundary_gdf.at[4, "geometry"])), tables=[level_data]
)


# basin 1873 wordt verbonden met outlet en outlet met level_boundary
model.link.add(
    model.outlet[n("bnd:26")],
    model.basin[n("up:ow:WL07329-0")],
    geometry=link(model.outlet[n("bnd:26")].geometry, model.basin[n("up:ow:WL07329-0")].geometry),
)

model.link.add(boundary_node, model.outlet[n("bnd:26")])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2388334544

# Kruising Dinkel/Kanaal Almelo Nordhorn corrigeren

# ege 2700, 2701, 2702 worden opgeheven
model.link.df = model.link.df[
    ~model.link.df.index.isin(
        [
            lk("ST02130", "bnd:21"),
            lk("ST01054", "bnd:21"),
            lk("ST02129", "bnd:21"),
            lk("DK04325", "bnd:21"),
            lk("ST01666", "bnd:21"),
            lk("ST01865", "bnd:21"),
            lk("ow:WL00317-1", "bnd:21"),
        ]
    )
]

# basin / area splitten bij rode lijn in twee vlakken
line = cast(LineString, split_line_gdf.at[3, "geometry"])
dinkel_polygons = polygon_parts(areas_gdf[areas_gdf["code"].isin(DINKEL_KANAAL)].union_all())
basin_polygon = next(i for i in dinkel_polygons if i.intersects(line))
basin_polygons = split_basin(basin_polygon, line)

# basin op dinkel bovenstrooms kanaal
dinkel_basin_node = model.basin.add(
    Node(geometry=hydroobject_gdf.loc[2966].geometry.boundary.geoms[1]), tables=basin_data
)

# basin in kanaal
kanaal_basin_node = model.basin.add(
    Node(geometry=hydroobject_gdf.loc[7720].geometry.boundary.geoms[1]), tables=basin_data
)

# the part of the areas across the line goes to the dinkel basin, the rest to the kanaal basin
model.add_basin_area(basin_polygons.geoms[1], node_id=dinkel_basin_node.node_id)
rest = [i for i in dinkel_polygons if not i.intersects(line)] + [basin_polygons.geoms[0]]
model.add_basin_area(MultiPolygon(rest), node_id=kanaal_basin_node.node_id)

# links v.a. tabulated_rating_curve 298 (ST01865) en 448 (ST01666) naar dinkel-basin
model.link.add(
    model.tabulated_rating_curve[n("ST01865")],
    dinkel_basin_node,
    geometry=link(model.tabulated_rating_curve[n("ST01865")].geometry, dinkel_basin_node.geometry),
)

model.link.add(
    model.tabulated_rating_curve[n("ST01666")],
    dinkel_basin_node,
    geometry=link(model.tabulated_rating_curve[n("ST01666")].geometry, dinkel_basin_node.geometry),
)

# link v.a. manning_resistance 915 naar dinkel basin
model.link.add(
    model.manning_resistance[n("DK04325")],
    dinkel_basin_node,
    geometry=link(model.manning_resistance[n("DK04325")].geometry, dinkel_basin_node.geometry),
)

# links v.a. dinkel basin naar tabulate_rating_curves 132 (ST02129) en 474 (ST02130)
model.link.add(
    dinkel_basin_node,
    model.tabulated_rating_curve[n("ST02129")],
    geometry=link(dinkel_basin_node.geometry, model.tabulated_rating_curve[n("ST02129")].geometry),
)

model.link.add(
    dinkel_basin_node,
    model.tabulated_rating_curve[n("ST02130")],
    geometry=link(dinkel_basin_node.geometry, model.tabulated_rating_curve[n("ST02130")].geometry),
)

# nieuwe manning_resistance in nieuwe dinkel-basin bovenstrooms kanaal
manning_node = model.manning_resistance.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[7721, "geometry"]).interpolate(0.5, normalized=True)),
    tables=[manning_data],
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
    model.tabulated_rating_curve[n("ST01054")],
    geometry=link(kanaal_basin_node.geometry, model.tabulated_rating_curve[n("ST01054")].geometry),
)

# nieuw kanaal-basin vervinden met manning_resistance 1346
model.link.add(
    kanaal_basin_node,
    model.manning_resistance[n("ow:WL00317-1")],
    geometry=link(kanaal_basin_node.geometry, model.manning_resistance[n("ow:WL00317-1")].geometry),
)

# nieuwe outletlet bij grensduiker kanaal
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[7746, "geometry"]).boundary.geoms[0]), tables=[outlet_data]
)

# nieuwe basin verbinden met outlet verbinden met level_boundary 21
model.link.add(
    outlet_node,
    kanaal_basin_node,
    geometry=link(outlet_node.geometry, kanaal_basin_node.geometry),
)

model.link.add(
    model.level_boundary[n("bnd:21")],
    outlet_node,
    geometry=link(model.level_boundary[n("bnd:21")].geometry, outlet_node.geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2389192454
model.reverse_link(link_id=lk("ST04518", "bnd:16"))
model.remove_node(node_id=n("up:DK20196_BO"), remove_links=True)
model.link.add(
    model.basin[n("up:ST03404")],
    model.outlet[n("DK20196_BO")],
    geometry=link(model.basin[n("up:ST03404")].geometry, model.outlet[n("DK20196_BO")].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2389198178
model.reverse_link(link_id=lk("DK25408", "bnd:34"))
model.reverse_link(link_id=lk("ow:WL05791-16@1", "bnd:34"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390712613

# Oplossen toplogische situatie kanaal Coevorden

# opheffen basin 2243 en basin 2182
model.remove_node(n("up:ow:WL03769-1"), remove_links=True)
model.remove_node(n("up:ow:WL05791-16@3"), remove_links=True)
model.remove_node(n("WL03769-1___0"), remove_links=True)
model.remove_node(n("ow:WL05791-16@3"), remove_links=True)
model.remove_node(n("ow:WL03769-1"), remove_links=True)

# # verplaatsen basin 1678 naar kruising waterlopen
model.move_node(node_id=n("up:ow:WL03768-2"), geometry=hydroobject_gdf.loc[6594].geometry.boundary.geoms[1])

# onknippen basin bij rode lijn
line = cast(LineString, split_line_gdf.at[4, "geometry"])
basin_area_row = model.basin.area.df[model.basin.area.df.contains(line.centroid)].iloc[0]
split_basin_area_to(model, basin_area_row.geometry, line, n("up:ow:WL03768-2"))

# verwijderen links 809, 814, 807, 810, 1293, 2772
model.link.df = model.link.df[
    ~model.link.df.index.isin(
        [
            lk("GM00197", "up:DK22251"),
            lk("ow:WL03300", "up:DK22251"),
            lk("GM00251", "up:DK22251"),
            lk("GM00093", "up:DK22251"),
        ]
    )
]


# verbinden manning 1270, 1127 en pumps 644, 579 en 649 met basin 1678
for node_id in [n("ow:WL03300"), n("ow:WL05791-16@2")]:
    model.link.add(
        model.manning_resistance[node_id],
        model.basin[n("up:ow:WL03768-2")],
        geometry=link(model.manning_resistance[node_id].geometry, model.basin[n("up:ow:WL03768-2")].geometry),
    )

for node_id in [n("GM00197"), n("GM00251"), n("GM00093")]:
    model.link.add(
        model.pump[node_id],
        model.basin[n("up:ow:WL03768-2")],
        geometry=link(model.pump[node_id].geometry, model.basin[n("up:ow:WL03768-2")].geometry),
    )

# verplaatsen manning 1267 naar basin-link tussen 1678 en 1678
model.move_node(node_id=n("ow:WL03767"), geometry=hydroobject_gdf.loc[6609].geometry.boundary.geoms[1])

# maak nieuwe manning-node tussen 1678 en 1897
manning_node = model.manning_resistance.add(
    Node(geometry=cast(Point, hydroobject_gdf.loc[6596].geometry.interpolate(0.5, normalized=True))),
    tables=[manning_data],
)

# verbinden basin 1897 met manning-node
model.link.add(
    model.basin[n("up:DK21370")],
    manning_node,
    geometry=link(model.basin[n("up:DK21370")].geometry, manning_node.geometry),
)

model.link.add(
    manning_node,
    model.basin[n("up:ow:WL03768-2")],
    geometry=link(manning_node.geometry, model.basin[n("up:ow:WL03768-2")].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390952469

# Schoonebekerdiep v.a. Twist Bült

# verplaatsen basin 1909 nabij tabulated_rating_curve 383 (ST03607)
model.move_node(n("down:bnd:23"), geometry=hydroobject_gdf.loc[6865].geometry.boundary.geoms[1])

# verwijderen links 780 en 778
model.link.df = model.link.df[~model.link.df.index.isin([lk("DK21938", "up:ST03340"), lk("ST03607", "up:ST03340")])]

# toevoegen link tussen tabulated_rating_curve 383 en basin 1909
model.link.add(
    model.tabulated_rating_curve[n("ST03607")],
    model.basin[n("down:bnd:23")],
    geometry=link(model.tabulated_rating_curve[n("ST03607")].geometry, model.basin[n("down:bnd:23")].geometry),
)

# toevoegen link tussen manning_resistance 851 en basin 1909
model.link.add(
    model.manning_resistance[n("DK21938")],
    model.basin[n("down:bnd:23")],
    geometry=link(model.manning_resistance[n("DK21938")].geometry, model.basin[n("down:bnd:23")].geometry),
)

# opknippen basin 1538 nabij 1909 en verbinden basin 1909 met 1539 via nieuwe manning_knoop
line = cast(LineString, split_line_gdf.at[5, "geometry"])
model.split_basin(line=line)
manning_node = model.manning_resistance.add(
    Node(geometry=drop_z(cast(Point, line.intersection(cast(LineString, hydroobject_gdf.at[6866, "geometry"]))))),
    tables=[manning_data],
)

model.node.df.loc[n("down:bnd:23"), "geometry"] = drop_z(model.basin[n("down:bnd:23")].geometry)  # ty: ignore[invalid-assignment]
model.link.add(model.basin[n("down:bnd:23")], manning_node)
model.link.add(
    manning_node,
    model.basin[n("up:ST03340")],
    geometry=link(manning_node.geometry, model.basin[n("up:ST03340")].geometry),
)

# verwijderen link 2716,2718,2718,2719
model.link.df = model.link.df[
    ~model.link.df.index.isin(
        [lk("ST03247", "bnd:34"), lk("ST03340", "bnd:34"), lk("ST03296", "bnd:34"), lk("ST03580", "bnd:34")]
    )
]

# opknippen basin 2181 nabij 1881 en verbinden basin 1881 met 2181 via nieuwe manning_knoop
model.move_node(n("up:GM00191"), geometry=hydroobject_gdf.loc[6919].geometry.boundary.geoms[1])
line = cast(LineString, split_line_gdf.at[6, "geometry"])
split_basin_area_to(model, basin_area_of(model, n("up:ow:WL05791-16@2")), line, n("up:GM00191"))
manning_node = model.manning_resistance.add(
    Node(geometry=cast(Point, line.intersection(cast(LineString, hydroobject_gdf.at[6879, "geometry"])))),
    tables=[manning_data],
)
model.node.df.loc[n("up:GM00191"), "geometry"] = drop_z(model.basin[n("up:GM00191")].geometry)  # ty: ignore[invalid-assignment]
model.link.add(model.basin[n("up:GM00191")], manning_node)
model.link.add(
    manning_node,
    model.basin[n("up:ow:WL05791-16@2")],
    geometry=link(manning_node.geometry, model.basin[n("up:ow:WL05791-16@2")].geometry),
)

for node_id in [n("ST03247"), n("ST03296"), n("ST03580"), n("ST03340")]:
    model.link.add(
        model.tabulated_rating_curve[node_id],
        model.basin[n("up:GM00191")],
        geometry=link(model.tabulated_rating_curve[node_id].geometry, model.basin[n("up:GM00191")].geometry),
    )

model.move_node(
    n("ow:WL05791-16@1"), cast(LineString, hydroobject_gdf.at[7749, "geometry"]).interpolate(0.5, normalized=True)
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391168839

# Molengoot Hardenberg

# opheffen basin 1903
model.remove_node(n("up:GM00101"), remove_links=True)

# verbinden basin 1433 met pump 621
model.link.add(
    model.basin[n("up:GM00159")],
    model.pump[n("GM00101")],
    geometry=link(model.basin[n("up:GM00159")].geometry, model.pump[n("GM00101")].geometry),
)

# verbinden tabulated_rating_curves 99 en 283 met basin 1433
for node_id in [n("ST03347"), n("ST02896")]:
    model.link.add(
        model.tabulated_rating_curve[node_id],
        model.basin[n("up:GM00159")],
        geometry=link(model.tabulated_rating_curve[node_id].geometry, model.basin[n("up:GM00159")].geometry),
    )

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2390898004

model.remove_node(n("ow:WL01165-19"), remove_links=True)
model.remove_node(n("up:ST01353"), remove_links=True)
model.link.add(
    model.basin[n("up:ow:WL01165-19")],
    model.tabulated_rating_curve[n("ST01353")],
    geometry=link(model.basin[n("up:ow:WL01165-19")].geometry, model.tabulated_rating_curve[n("ST01353")].geometry),
)
model.link.add(
    model.basin[n("up:ow:WL01165-19")],
    model.manning_resistance[n("vw034_89")],
    geometry=link(model.basin[n("up:ow:WL01165-19")].geometry, model.manning_resistance[n("vw034_89")].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391191673

# verwijderen basin 1905
model.remove_node(n("up:DK17739"), remove_links=True)

# verbinden manning_resistance 995 met basin 2148
model.link.add(
    model.basin[n("up:DK02511")],
    model.manning_resistance[n("DK17739")],
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391460899

# Samenvoegen basin-knopen Overijsselse Vecht & Coevorden Vechtkanaal
for basin_id in [n("up:DK26272"), n("up:ow:WL03781-13"), n("up:ow:WL06799"), n("up:DK21481")]:
    model.merge_basins(node_id=basin_id, to_node_id=n("up:ow:WL02747-4"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391666745

# Opruimen basins Nieuw-Amsterdam

# opknippen basin 1611 bij rode lijn, area mergen met basin 1879
basin_polygon = model.basin.area.df.set_index("node_id").at[n("up:ow:WL03772-1"), "geometry"]
left_poly, right_poly = split_basin_multi_polygon(
    cast(MultiPolygon, basin_polygon), cast(LineString, split_line_gdf.at[8, "geometry"])
)
model.basin.area.df.loc[model.basin.area.df.node_id == n("up:ow:WL03772-1"), ["geometry"]] = right_poly  # ty: ignore[invalid-assignment]

left_poly = cast(
    Polygon,
    cast(MultiPolygon, model.basin.area.df.set_index("node_id").at[n("up:ow:WL03161"), "geometry"]).union(left_poly),
)
model.basin.area.df.loc[model.basin.area.df.node_id == n("up:ow:WL03161"), ["geometry"]] = MultiPolygon([left_poly])  # ty: ignore[invalid-assignment]

# merge basins 2186, 2173, 2022, 1611, 2185 in basin 1902
for basin_id in [
    n("up:ow:WL06797-1@2"),
    n("up:ow:WL03772-2"),
    n("up:ow:WL03162@2"),
    n("up:ow:WL03772-1"),
    n("up:DK26372"),
]:
    model.merge_basins(node_id=basin_id, to_node_id=n("up:SL00008"))

# verplaats 1902 iets bovenstrooms
model.move_node(n("up:SL00008"), cast(LineString, hydroobject_gdf.at[6615, "geometry"]).boundary.geoms[1])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391686269
model.remove_node(n("up:ST02804"), remove_links=True)
model.remove_node(n("up:GM00182"), remove_links=True)
model.link.add(model.basin[n("up:ow:WL00451-4")], model.pump[n("GM00182")])
model.link.add(model.tabulated_rating_curve[n("ST02804")], model.basin[n("up:ow:WL00451-4")])
model.link.add(model.pump[n("GM00182")], model.basin[n("up:ST01676")])
model.link.add(model.basin[n("up:ST01676")], model.tabulated_rating_curve[n("ST02804")])

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391710413

model.remove_node(n("up:GM00194"), remove_links=True)
model.link.add(model.basin[n("up:GM00092")], model.pump[n("GM00194")])
model.link.add(model.manning_resistance[n("DK26416")], model.basin[n("up:GM00092")])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391672700

# Merge basin 2176 in 1605
model.merge_basins(node_id=n("up:ow:WL03775-1"), to_node_id=n("up:ST03211"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391726774
# Merge basins 2206 in 1518
model.merge_basins(node_id=n("up:ST04067"), to_node_id=n("up:ow:WL03440-7"), are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391734144
# dood takje uit Overijsselse Vecht
model.remove_node(n("up:ow:WL07806"), remove_links=True)
model.remove_node(n("ow:WL07806"), remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391740603

# Merge basin 2225 met 2304
model.merge_basins(node_id=n("up:DK24425"), to_node_id=n("up:DK18155"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391815016

# Wetteringe als laterale inflow
model.merge_basins(node_id=n("up:du"), to_node_id=n("up:ST04838"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391750536

# Rondom SL00010 opruimen
model.remove_node(n("up:GM00231"), remove_links=True)
model.remove_node(n("down:SL00010"), remove_links=True)
model.link.add(model.outlet[n("SL00010")], model.level_boundary[n("bnd:15")])
model.link.add(model.basin[n("up:DK25518")], model.pump[n("GM00232")])
model.link.add(model.basin[n("up:DK25518")], model.pump[n("GM00231")])


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391820198

# Merge basin 2232 in 1591
model.merge_basins(node_id=n("up:DK04860"), to_node_id=n("up:ST01683"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391825301

# Basin 2236 naar LevelBoundary
model.update_node(n("up:ST00558"), "LevelBoundary", data=[level_data])

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391829471

# Merge basin 2246 en 1419
model.merge_basins(node_id=n("up:ow:WL02245-2-0"), to_node_id=n("up:ST00101"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391946915

# Opruimen Elsbeek

# Basin 2256 verplaatsen naar punt
model.move_node(node_id=n("up:DK29373"), geometry=hydroobject_gdf.loc[2896].geometry.boundary.geoms[1])

# Basin knippen over lijn
model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != n("up:DK29373")]
split_basin_area_to(
    model, basin_area_of(model, n("up:ST04431")), cast(LineString, split_line_gdf.at[10, "geometry"]), n("up:DK29373")
)

# Links 446, 1516, 443 en 444 verwijderen
model.link.df = model.link.df[
    ~model.link.df.index.isin(
        [
            lk("DK11088", "up:ST04431"),
            lk("up:ST04431", "ow:WL00282-2"),
            lk("ST04683", "up:ST04431"),
            lk("ST04448", "up:ST04431"),
        ]
    )
]

# tabulated_rating_curves 202 en 230 verbinden met basin 2256
model.link.add(
    model.tabulated_rating_curve[n("ST04683")],
    model.basin[n("up:DK29373")],
    geometry=link(model.tabulated_rating_curve[n("ST04683")].geometry, model.basin[n("up:DK29373")].geometry),
)
model.link.add(
    model.tabulated_rating_curve[n("ST04448")],
    model.basin[n("up:DK29373")],
    geometry=link(model.tabulated_rating_curve[n("ST04448")].geometry, model.basin[n("up:DK29373")].geometry),
)

# resistance 954 verbinden met basin 2256
model.link.add(
    model.manning_resistance[n("DK11088")],
    model.basin[n("up:DK29373")],
    geometry=link(model.manning_resistance[n("DK11088")].geometry, model.basin[n("up:DK29373")].geometry),
)

# basin 2256 verbinden met resistance 1106
model.link.add(
    model.basin[n("up:DK29373")],
    model.manning_resistance[n("ow:WL00282-2")],
    geometry=link(model.basin[n("up:DK29373")].geometry, model.manning_resistance[n("ow:WL00282-2")].geometry),
)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391984234

# Merge basin 2261 in basin 1698
model.merge_basins(node_id=n("up:ST02504"), to_node_id=n("up:DK15163"))
# model.remove_node(390, remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2391995841

# Merge basin 2260 met basin 1645
model.merge_basins(node_id=n("up:ST00875"), to_node_id=n("up:WL00714-11"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392010526
# Merge basin 2220 met basin 1371
model.merge_basins(node_id=n("up:ST00373"), to_node_id=n("up:DK05134"), are_connected=False)


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392017041

# Kanaal Almelo Nordhorn bij Almelo
model.merge_basins(node_id=n("up:ST01682"), to_node_id=n("up:ow:WL00448-2"), are_connected=False)
model.merge_basins(node_id=n("up:GM00207"), to_node_id=n("up:ow:WL00448-2"), are_connected=False)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392022887

# Merge basin 2203 met 2227
model.merge_basins(node_id=n("up:ST03006"), to_node_id=n("up:ST03168"), are_connected=False)
model.remove_node(n("ow:WL03777@2"), remove_links=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392026739

# Merge basin 2014 met 2144
model.merge_basins(node_id=n("up:ST00916"), to_node_id=n("up:DK12629"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392030268

# Merge basin 1696 met 1411
model.merge_basins(node_id=n("up:ST00957"), to_node_id=n("up:ow:WL00712-18"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392037263

# Merge basin 2264 met 1459
model.merge_basins(node_id=n("up:ow:WL03777@2"), to_node_id=n("up:ST01276"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392043973

# Merge basin 2212 en 2310
model.merge_basins(node_id=n("up:ST00239"), to_node_id=n("up:ow:WL06796-14"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392048684

# Merge basin 2253 in basin 2228
model.merge_basins(node_id=n("up:ST02015"), to_node_id=n("up:ST02809"))


# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392052379

# Merge basin 2221 in basin 1634
model.merge_basins(node_id=n("up:WL01863-3"), to_node_id=n("up:ow:WL01863-9"))

# %% https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2392076634

# Verbinding rondwaterleiding / Lennelwaterleiding herstellen
model.merge_basins(node_id=n("up:GM00112"), to_node_id=n("up:ST04476"), are_connected=False)

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

# %% https://github.com/Deltares/Ribasim-NL/issues/890
# FlowBoundary 25 is the inflow of the Wetteringe, not of the CPK, which is LevelBoundary 34
assert model.node.df.at[n("bnd:25"), "node_type"] == "FlowBoundary", model.node.df.at[n("bnd:25"), "node_type"]
assert model.node.df.at[n("bnd:25"), "name"] == "CPK", model.node.df.at[n("bnd:25"), "name"]
model.node.df.loc[n("bnd:25"), "name"] = "Wetteringe"

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
        # itertuples rows are namedtuples, whose `_asdict` ty sees as a column
        kwargs = cast(Any, row)._asdict()
        # a layer refers to a node by its key, and to a link by the keys of its two nodes, see `Model.apply_edits`
        for argument in sorted(NODE_ARGUMENTS):
            key = kwargs.pop(argument.removesuffix("_id") + "_key", None)
            if pd.notna(key):
                kwargs[argument] = n(key)
        if "link_from_key" in kwargs:
            kwargs["link_id"] = lk(kwargs.pop("link_from_key"), kwargs.pop("link_to_key"))
        if inspect.getfullargspec(method).varkw != "kwargs":
            keywords = inspect.getfullargspec(method).args
            kwargs = {k: v for k, v in kwargs.items() if k in keywords}
        method(**kwargs)

# %% Custom model adjustments

# fmt: off
CUSTOM_REDIRECT_LINKS = [
    {"link_id": lk("vw034_89", "up:ST02237"), "to_node_id": n("up:ow:WL01181-5")},
    {"link_id": lk("up:GM00234", "ST04777"), "from_node_id": n("loc:235324,512655")},
    {"link_id": lk("up:GM00234", "GM00234"), "from_node_id": n("loc:235324,512655")},
]

MERGE_TO_NODE = [
    (n("up:ow:WL00588-6"), n("up:ST00245")), (n("up:DK12481"), n("up:ow:WL01265-14")), (n("up:DK28022"), n("up:ST02860")), (n("up:DK20536"), n("bnd:33")), (n("up:DK18457"), n("up:ow:WL03440-7")),
    (n("up:ST02645"), n("up:DK15988")), (n("up:DK13613"), n("up:DK15988")), (n("up:ow:WL00328"), n("up:ST00126")), (n("up:DK15528"), n("up:DK00277")), (n("up:DK00277"), n("up:ow:WL01220-5")),
    (n("up:ow:WL02781-1"), n("up:DK20616")), (n("up:ow:WL03049-1"), n("up:DK17873")), (n("up:DK14459"), n("up:ow:WL01604-23")), (n("up:ow:WL01604-22"), n("up:ow:WL01604-23")), (n("up:DK12629"), n("up:ow:WL01604-23")),
    (n("up:ST01777"), n("up:ST03541")), (n("up:DK23361"), n("up:ST03541")),
]

NODES_TO_REMOVE = [
    n("GM00229"), n("GM00230"), n("DK15887"), n("ow:WL03883"), n("ow:WL01973-6"), n("ow:WL01181-9"), n("DK24276"), n("ST02906"), n("ST02907"), n("ST01741"),
    n("ST04037"), n("ST02656"), n("ST04039"), n("ST00296"), n("DK08962"), n("ow:WL07333"), n("ST03743"), n("ST03742"), n("ST03741"), n("ST03505"),
    n("GM00247"), n("DK27313"), n("ST04330"), n("GM00213"), n("ST04019"), n("GM00215"), n("GM00238"), n("GM00243"), n("GM00237"), n("GM00194"),
    n("GM00209"), n("GM00236"), n("ST02861"), n("ST02274"), n("ST01755"), n("ST00453"), n("ST04679"), n("GM00190"), n("ST03791"), n("ST03792"),
    n("GM00217"), n("ST03807"), n("GM00202"), n("ow:WL01181-5"), n("ST03940"), n("DK28613"), n("ST02360"), n("ST03436"), n("ST03140"),
]

MERGE_TO_BASIN = [
    (n("up:DK08962"), n("up:ST01592")), (n("up:ST03588"), n("up:ow:WL02747-62")), (n("up:ow:WL06800-16"), n("up:ow:WL02747-62")), (n("up:ow:WL03418"), n("up:ow:WL02747-62")), (n("up:DK01810"), n("up:GM00178")),
    (n("up:ST04605"), n("up:ow:WL01909-9")), (n("up:DK21370"), n("up:ow:WL03768-2")), (n("up:ow:WL05791-16@2"), n("up:ow:WL03768-2")), (n("up:ow:WL03768-2"), n("up:DK22015")), (n("up:ow:WL03771-5"), n("up:DK22251")),
    (n("up:SL00005"), n("up:ow:WL07329-0")), (n("up:DK20930"), n("up:ow:WL07329-0")), (n("up:GM00243"), n("up:ow:WL03161")), (n("up:DK22716"), n("up:ow:WL03161")), (n("up:SL00008"), n("up:ow:WL03161")),
    (n("up:DK24394"), n("up:ow:WL03161")), (n("up:ow:WL05221"), n("up:ow:WL03161")), (n("up:DK18395"), n("up:SL00004")), (n("up:ow:WL03736"), n("up:DK25518")), (n("up:DK16025"), n("up:ow:WL01265-14")),
    (n("up:ST04838"), n("up:ow:WL02926-4")), (n("up:ow:WL01442-9"), n("up:ST00220")), (n("up:ow:WL01455-8"), n("up:ST00220")), (n("bnd:33"), n("up:GM00245")), (n("up:ST04024"), n("up:DK15887")),
    (n("up:ST02809"), n("up:ST00266")), (n("up:ow:WL01069"), n("up:ST01855")), (n("up:ow:WL01068-4-0"), n("up:ST01855")), (n("up:ow:WL02214-7"), n("up:ST01973")), (n("up:ow:WL02261-5"), n("up:ST01973")),
    (n("up:ow:WL02102-0"), n("up:ST02615")), (n("up:ow:WL01165-16"), n("up:ow:WL01165-19")), (n("up:ST03536"), n("up:DK21454")), (n("up:DK22039"), n("up:DK22015")), (n("up:DK15943"), n("up:ow:WL01265-14")),
    (n("up:ST02640"), n("up:ow:WL01265-14")), (n("up:ow:WL01220-1"), n("up:ow:WL01220-5")), (n("up:ow:WL01220-5"), n("up:ow:WL01181-0")), (n("up:ow:WL01165-19"), n("up:ow:WL01181-5")), (n("up:GM00178"), n("up:DK04366")),
]

FULL_CONTROL_REDIRECT_LINKS = [
    {"link_id": lk("up:DK14315", "DK14315"), "from_node_id": n("twentekanaal:ST02055")},
    {"link_id": lk("DK17636", "up:DK18155"), "to_node_id": n("up:ST03459")},
]

FULL_CONTROL_REVERSE_LINK_IDS = [
    lk("GM00146", "up:SL00017"), lk("up:GM00146", "GM00146"), lk("ST04531", "up:SL00004"), lk("down:ST04531", "ST04531"), lk("up:DK25843", "ST02973"), lk("ST02973", "up:DK27007"), lk("DK28737", "up:ST03347"), lk("up:DK28737", "DK28737"), lk("up:DK20369", "DK29136"), lk("DK29136", "up:DK21846"),
    lk("ow:WL03296-2", "up:ST03198"), lk("up:DK24977", "ow:WL03296-2"), lk("DK22336", "up:ow:WL02926-4"), lk("up:DK17294", "DK26410"), lk("DK26410", "up:ST03198"), lk("up:DK22336", "DK22336"), lk("up:ST02994", "ST02994"), lk("ST02994", "up:ow:WL03046-5"), lk("DK21875", "up:ST03086"), lk("up:DK21875", "DK21875"),
    lk("up:DK19428", "GM00137"), lk("GM00137", "bnd:33"), lk("GM00231", "bnd:15"), lk("up:DK25518", "GM00231"), lk("DK21040", "up:ow:WL03325-10"), lk("up:DK17966", "DK21040"), lk("down:DK20932", "DK20932"), lk("DK20932", "up:DK17947"), lk("DK09836", "up:ST00610"), lk("up:DK09836", "DK09836"),
    lk("ST03588", "up:SL00017"), lk("up:ST03588", "ST03588"), lk("up:ST03326", "ST04266"), lk("ST04266", "up:ST03026"), lk("SL00010", "bnd:15"), lk("up:DK25518", "SL00010"), lk("DK01810", "up:ow:WL00588-6"), lk("up:DK01810", "DK01810"), lk("DK25430", "up:DK18674"), lk("up:DK25430", "DK25430"),
    lk("GM00106", "up:ST03194"), lk("up:GM00106", "GM00106"), lk("DK17636", "up:DK18155"), lk("up:DK17636", "DK17636"),
]
# fmt: on

# --- Connectivity and topology edits ---
redirect_links(model, CUSTOM_REDIRECT_LINKS)
merge_basin_pairs(model, MERGE_TO_NODE)
model.merge_basins(node_id=n("up:GM00048"), to_node_id=n("up:ST04058"), are_connected=False)
model.remove_nodes(NODES_TO_REMOVE, remove_links=True)
merge_basin_pairs(model, MERGE_TO_BASIN)
reverse_links(model, [lk("DK23050", "up:DK17947"), lk("up:DK23050", "DK23050")])

# --- Full-control topology fixes moved here to keep node_ids stable for steering ---
redirect_links(model, FULL_CONTROL_REDIRECT_LINKS)
reverse_links(model, FULL_CONTROL_REVERSE_LINK_IDS)

# Basin 1843 splitsen met Wateraanvoer OBJECTID 377, plus extra basin + kunstwerken.
assert model.basin.area.df is not None
assert model.basin.profile.df is not None
assert model.basin.static.df is not None
assert model.basin.state.df is not None

basin_1843_area_df = model.basin.area.df.loc[model.basin.area.df.node_id == n("up:DK04366")].copy()
if basin_1843_area_df.empty:
    raise ValueError("Geen bestaande basin_area gevonden voor Basin up:DK04366")

old_basin_1843_geometry = basin_1843_area_df.geometry.union_all()
if old_basin_1843_geometry.geom_type == "Polygon":
    old_basin_1843_geometry = MultiPolygon([old_basin_1843_geometry])
elif old_basin_1843_geometry.geom_type != "MultiPolygon":
    raise ValueError(f"Oude geometry voor Basin up:DK04366 heeft ongeldig type: {old_basin_1843_geometry.geom_type}")

old_basin_1843_node_geometry = model.basin[n("up:DK04366")].geometry

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
    label="Basin up:DK04366",
)
basin_2148_geometry = clean_basin_area_geometry(
    basin_2148_area_df.geometry.union_all(),
    label="OBJECTID 1753",
)

model.basin.area.df = model.basin.area.df.loc[model.basin.area.df.node_id != n("up:DK04366")].copy()
model.add_basin_area(geometry=new_basin_1843_geometry, node_id=n("up:DK04366"), meta_streefpeil=streefpeil_1843)
model.move_node(node_id=n("up:DK04366"), geometry=Point(237696.58, 496023.31))

basin_2148_area_existing_df = model.basin.area.df.loc[model.basin.area.df.node_id == n("up:DK02511")].copy()
if basin_2148_area_existing_df.empty:
    raise ValueError("Geen bestaande basin_area gevonden voor Basin up:DK02511")

streefpeil_2148 = first_meta_streefpeil(basin_2148_area_existing_df)
model.basin.area.df = model.basin.area.df.loc[model.basin.area.df.node_id != n("up:DK02511")].copy()
model.add_basin_area(geometry=basin_2148_geometry, node_id=n("up:DK02511"), meta_streefpeil=streefpeil_2148)

new_basin_node_id = model.basin.add(
    Node(geometry=old_basin_1843_node_geometry, name="Basin Westerhaar aanvoer")
).node_id

for basin_table in [model.basin.profile, model.basin.static, model.basin.state]:
    table_df = basin_table.df
    source_rows = table_df.loc[table_df["node_id"] == n("up:DK04366")].copy()
    if source_rows.empty:
        raise ValueError(f"Geen brondata gevonden voor basin-tabel {type(basin_table).__name__} en Basin up:DK04366")
    source_rows.loc[:, "node_id"] = new_basin_node_id
    basin_table.df = pd.concat([table_df, source_rows], ignore_index=True)

model.add_basin_area(
    geometry=new_basin_geometry,
    node_id=new_basin_node_id,
    meta_streefpeil=streefpeil_1843,
)

aanvoer_pump = model.pump.add(
    Node(
        geometry=Point(238834.91, 496511.08),
        name="Aanvoergemaal Westerhaar",
        meta_key="aanvoergemaal-westerhaar",
    ),
    tables=[pump_data],
)
model.link.add(model.basin[n("up:DK04366")], aanvoer_pump)
model.link.add(aanvoer_pump, model.basin[n("up:ST01677")])

westerhaar_uitlaat_outlet = model.outlet.add(
    Node(geometry=Point(238833.885, 496508.498), name="Uitlaat Westerhaar"),
    tables=[outlet_data],
)
if not (
    can_connect(model.basin[new_basin_node_id].node_type, "Outlet")
    and can_connect("Outlet", model.basin[n("up:DK04366")].node_type)
):
    raise ValueError(
        "Gevraagde route met Outlet is ongeldig: "
        f"{model.basin[new_basin_node_id].node_type} -> Outlet -> {model.basin[n('up:DK04366')].node_type} "
        f"(nodes: {new_basin_node_id} -> {westerhaar_uitlaat_outlet.node_id} -> up:DK04366)."
    )
model.link.add(model.basin[new_basin_node_id], westerhaar_uitlaat_outlet)
model.link.add(westerhaar_uitlaat_outlet, model.basin[n("up:DK04366")])

inlaatstuw_outlet = model.outlet.add(
    Node(geometry=Point(243225.458, 495478.36), name="Inlaatstuw"),
    tables=[outlet_data],
)
model.link.add(model.basin[n("up:ow:WL01711-4")], inlaatstuw_outlet)
model.link.add(inlaatstuw_outlet, model.basin[n("up:DK30436_BO")])

inlaat_outlet_1708 = model.outlet.add(
    Node(geometry=Point(238366.036, 496286.96), name="Inlaat"),
    tables=[outlet_data],
)
model.link.add(model.basin[n("up:DK04366")], inlaat_outlet_1708)
model.link.add(inlaat_outlet_1708, model.basin[n("up:DK02627")])

uitlaat_outlet_1569 = model.outlet.add(
    Node(geometry=Point(241738.30642, 513291.13972), name="Uitlaat"),
    tables=[outlet_data],
)
model.link.add(model.basin[n("down:DK20932")], uitlaat_outlet_1569)
model.link.add(uitlaat_outlet_1569, model.basin[n("up:GM00115")])

redirect_links(
    model,
    [
        {"link_id": lk("GM00009", "up:DK04366"), "to_node_id": new_basin_node_id},
        {"link_id": lk("up:DK04366", "ST01745"), "from_node_id": new_basin_node_id},
        {"link_id": lk("up:DK04366", "DK31244"), "from_node_id": n("up:DK04366")},
        {"link_id": lk("GM00007", "up:DK04366"), "to_node_id": n("up:DK04366")},
        {"link_id": lk("DK01810", "up:DK04366"), "to_node_id": n("up:DK04366")},
        {"link_id": lk("up:DK04366", "ST00724"), "from_node_id": n("up:DK04366")},
        {"link_id": lk("up:DK04366", "GM00178"), "from_node_id": new_basin_node_id},
        {"link_id": lk("ST01763", "up:DK04366"), "to_node_id": new_basin_node_id},
        {
            "link_id": lk("aanvoergemaal-westerhaar", "up:ST01677"),
            "from_node_id": aanvoer_pump.node_id,
            "to_node_id": new_basin_node_id,
        },
    ],
)

from_node = model.get_node(n("twentekanaal:ow:WL00448-2"))
to_node = model.get_node(n("up:ow:WL00448-2"))
if not (can_connect(from_node.node_type, "Outlet") and can_connect("Outlet", to_node.node_type)):
    raise ValueError(
        f"Gevraagde route met Outlet is ongeldig: {from_node.node_type} -> Outlet -> {to_node.node_type} "
        f"(nodes: {from_node.node_id} -> {to_node.node_id})."
    )
extra_connection_node = model.outlet.add(
    Node(geometry=Point(241321.928, 486550.60)),
    tables=[outlet_data],
)
model.link.add(from_node, extra_connection_node)
model.link.add(extra_connection_node, to_node)

from_node = model.get_node(n("up:ST03459"))
to_node = model.get_node(n("up:DK18155"))
if not (can_connect(from_node.node_type, "Outlet") and can_connect("Outlet", to_node.node_type)):
    raise ValueError(
        f"Gevraagde route met Outlet is ongeldig: {from_node.node_type} -> Outlet -> {to_node.node_type} "
        f"(nodes: {from_node.node_id} -> {to_node.node_id})."
    )
nieuwe_drostendiep_outlet = model.outlet.add(
    Node(geometry=Point(250205.53, 524562.22), name="Uitlaat Nieuwe Drostendiep"),
    tables=[outlet_data],
)
model.link.add(from_node, nieuwe_drostendiep_outlet)
model.link.add(nieuwe_drostendiep_outlet, to_node)

# Deze GDB-typefix stond pre-refactor in 04_add_full_control.py, dus na 02_prepare_model.
# Dynamisch opnieuw zoeken in 01 geeft andere matches omdat link-/nodegeometrie dan nog niet geprepareerd is.
# Daarom de pre-refactor uitkomst vastzetten, zodat de refactor geen node-types en sturing verandert.
gdb_outlet_node_ids = [n("ow:WL00262"), n("WL05569___3"), n("WL02126-1___4"), n("WL00510-1___16"), n("WL00510-1___6")]
gdb_pump_node_ids = []

# fmt: off
OUTLET_TYPE_FIX_NODE_IDS = [
    n("ow:WL00714-0-0"), n("ow:WL03182-7"), n("DK17636"), n("ow:WL06694-3"), n("ow:WL01052-0"), n("ow:WL03325-2"), n("ow:WL03296-2"), n("WL02311"), n("WL01863-2___4"), n("ow:WL01391-35"),
    n("ow:WL01374-2"), n("ow:WL01052-8"), n("WL00964-3___3"), n("ow:WL00609-4-0-0"), n("ow:WL00607-1-0-0"), n("ow:WL00840-0"), n("ow:WL00859-0"), n("ow:WL05820-5"), n("WL01863-2___4"), n("ow:WL02102-15"),
    n("ow:WL01959-32"), n("ow:WL01803-3"), n("ow:WL01758-3"), n("ow:WL01863-9"), n("WL02926-0___1"), n("ow:WL08175"), *gdb_outlet_node_ids,
]

MANNING_TYPE_FIX_NODE_IDS = [
    n("DK24519"), n("DK24281"), n("DK11840"), n("DK04546"), n("DK01390"), n("DK25487"), n("DK13262"), n("DK25521"), n("DK19287"), n("DK21305"),
    n("ST02645"), n("DK28416"), n("DK01495"), n("DK29791"), n("DK06236"), n("DK31895"), n("DK23689"), n("ST03127"), n("DK00105"), n("DK29066"),
    n("DK03265"), n("DK06275"), n("DK01810"), n("SL00002"), n("DK22561"), n("DK25247"), n("DK23608"), n("DK20623"),
]

OBSOLETE_NODE_IDS = [
    n("ow:WL07883"), n("GM00130"), n("ST03877"), n("ST03798"), n("SL00013"), n("ST03845"), n("ow:WL05469"), n("ST00298"), n("ST00508"), n("ST00724"),
    n("ow:WL08175"), n("ST02866"), n("ST00803"), n("GM00047"), n("GM00221"), n("ST01973"), n("DK21646"), n("ST03711"), n("ST01269"),
    n("ST03905"), n("ST00856"), n("ST00872"), n("WL01698-3_4_inlaat"), n("ST01592"), n("ST03819"), n("SL00012"), n("DK28532"), n("ST00837"), n("DK04366"),
]

CONNECTED_BASIN_MERGES = [
    (n("up:ST02862"), n("up:ST00803")), (n("up:DK03265"), n("up:ST00220")), (n("up:DK06275"), n("up:ST00220")), (n("up:DK10091"), n("up:DK05134")), (n("up:DK13817"), n("up:DK05134")),
    (n("up:ST03266"), n("up:ow:WL03325-10")), (n("up:ST03620"), n("up:DK25702")), (n("up:DK19061"), n("up:DK18861")), (n("up:DK26826"), n("up:GM00159")), (n("up:DK11462"), n("up:DK00769")),
    (n("up:DK22326"), n("up:DK25843")), (n("up:DK18408"), n("up:ST03261")), (n("up:DK23188"), n("up:ST03261")), (n("up:DK29530"), n("up:ST04517")), (n("up:ow:WL07085"), n("up:ST04517")),
    (n("up:ow:WL01769"), n("up:ow:WL01764-19")), (n("up:DK06993"), n("up:ow:WL01764-19")), (n("up:DK01920"), n("up:ow:WL01764-19")), (n("up:DK32117"), n("up:ow:WL01823-5")), (n("up:DK00102"), n("up:ST00220")),
    (n("up:DK05222"), n("up:ST00220")), (n("up:GM00146"), n("up:ST04475")), (n("up:ST04475"), n("up:ow:WL07638-3")), (n("up:ow:WL00712-6"), n("up:ST00579")), (n("up:ow:WL00733"), n("up:ST02542")),
]

UNCONNECTED_BASIN_MERGES = [
    (n("up:DK23461"), n("up:DK17294")), (n("up:DK22829"), n("up:DK25792")), (n("up:ST03347"), n("loc:235324,512655")), (n("up:ST04383"), n("up:DK27338")),
]

FINAL_BASIN_MERGES = [
    (n("up:ST00245"), n("up:ST03541")), (n("up:DK17947"), n("up:ST03541")),  # Kanaal Almelo Haandrik
    (n("up:ow:WL07329-0"), n("up:DK20656")), (n("up:ow:WL03161"), n("up:DK20656")),  # Verlengde Hoogeveensche Vaart
    (n("up:DK22251"), n("up:DK22015")),  # Stieltjeskanaalsluis
]
# fmt: on

model.update_nodes(OUTLET_TYPE_FIX_NODE_IDS, "Outlet")
model.update_nodes(MANNING_TYPE_FIX_NODE_IDS, "ManningResistance")
model.update_nodes(gdb_pump_node_ids, "Pump")
model.remove_nodes(OBSOLETE_NODE_IDS, remove_links=True)

model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == n("twentekanaal:ST02055"), "level"] = 15.33

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

# %% give the holes that the edits left in the basin areas to a basin
# the Twentekanaal is part of the model of Rijkswaterstaat
model.fill_basin_area_holes(areas_gpkg, excluded=twentekanaal_poly)

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

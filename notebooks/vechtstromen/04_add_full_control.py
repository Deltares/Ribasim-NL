# %%
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely
from peilbeheerst_model.controle_output import Control
from ribasim import Node
from ribasim.validation import can_connect
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.model import DEFAULT_TABLES
from ribasim_nl.parametrization.basin_tables import update_basin_static
from shapely import wkt
from shapely.geometry import MultiPolygon, Point

from ribasim_nl import CloudStorage, Model

# execute model run
MODEL_EXEC: bool = True

# model settings
AUTHORITY: str = "Vechtstromen"
SHORT_NAME: str = "vechtstromen"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"


# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {38, 40, 996}


# %%
# Helpers


def update_nodes(model: Model, node_ids: list[int], node_type: str) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.update_node(node_id=node_id, node_type=node_type)


def remove_nodes(model: Model, node_ids: list[int]) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.remove_node(node_id, remove_links=True)


def set_static_values(static_df, node_values: dict[int, float], column: str) -> None:
    for node_id, value in node_values.items():
        static_df.loc[static_df.node_id == node_id, column] = value


def set_max_flow_rate(static_df, max_flow_rate_by_node_id: dict[int, float]) -> None:
    max_flow_rate = static_df["node_id"].map(max_flow_rate_by_node_id)
    mask = max_flow_rate.notna()

    static_df.loc[mask, "max_flow_rate"] = max_flow_rate[mask]
    static_df.loc[mask, "flow_rate"] = max_flow_rate[mask]


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


def _as_node_id_list(node_ids) -> list[int]:
    if node_ids is None:
        return []
    if isinstance(node_ids, pd.Series):
        return [int(node_id) for node_id in node_ids.to_list()]
    if isinstance(node_ids, (list, tuple, set)):
        return [int(node_id) for node_id in node_ids]
    return [int(node_ids)]


def _connected_node_ids(model: Model, node_id: int, direction: str) -> list[int]:
    return _as_node_id_list(getattr(model, f"{direction}_node_id")(node_id))


def _first_node_of_type_along_network(
    model: Model,
    start_node_id: int,
    direction: str,
    target_node_types: tuple[str, ...],
    pass_through_node_types: tuple[str, ...],
    max_iter: int = 500,
) -> int | None:
    frontier = [start_node_id]
    visited = {start_node_id}

    for _ in range(max_iter):
        if not frontier:
            return None

        next_frontier = []
        for current_node_id in frontier:
            for next_node_id in _connected_node_ids(model, current_node_id, direction):
                if next_node_id in visited:
                    continue

                visited.add(next_node_id)
                next_node_type = model.get_node_type(next_node_id)

                if next_node_type in target_node_types:
                    return next_node_id
                if next_node_type in pass_through_node_types:
                    next_frontier.append(next_node_id)

        frontier = next_frontier

    return None


def _first_basin_along_network(model: Model, start_node_id: int, direction: str, max_iter: int = 500) -> int | None:
    return _first_node_of_type_along_network(
        model=model,
        start_node_id=start_node_id,
        direction=direction,
        target_node_types=("Basin",),
        pass_through_node_types=("Junction", "ManningResistance", "Outlet", "Pump", "TabulatedRatingCurve"),
        max_iter=max_iter,
    )


def _find_first_control_node_along_network(
    model: Model, start_node_id: int, direction: str, max_iter: int = 500
) -> int | None:
    return _first_node_of_type_along_network(
        model=model,
        start_node_id=start_node_id,
        direction=direction,
        target_node_types=("Outlet", "Pump"),
        pass_through_node_types=("Basin", "Junction", "ManningResistance", "TabulatedRatingCurve"),
        max_iter=max_iter,
    )


def sync_basin_profiles_with_target_levels(model: Model, basin_level_change_df: pd.DataFrame) -> pd.DataFrame:
    assert model.basin.profile.df is not None

    if basin_level_change_df.empty:
        return basin_level_change_df

    basin_profile_df = model.basin.profile.df
    profile_change_records = []

    for row in basin_level_change_df.itertuples():
        if pd.isna(row.old_level) or pd.isna(row.new_level) or row.old_level == row.new_level:
            continue

        profile_mask = basin_profile_df["node_id"] == row.target_basin_id
        if not profile_mask.any():
            continue

        level_shift = row.new_level - row.old_level
        basin_profile_df.loc[profile_mask, "level"] += level_shift
        profile_change_records.append(
            {
                "target_basin_id": row.target_basin_id,
                "old_level": row.old_level,
                "new_level": row.new_level,
                "level_shift": level_shift,
            }
        )

    profile_change_df = pd.DataFrame(profile_change_records)
    if not profile_change_df.empty:
        print(f"ManningResistance-netwerk: {len(profile_change_df)} basinprofielen aangepast")
        for row in profile_change_df.itertuples():
            print(
                f"target_basin_id={row.target_basin_id}: profielniveau verschoven met {row.level_shift} m "
                f"({row.old_level} -> {row.new_level})"
            )

    return profile_change_df


def validate_basin_levels_against_profiles(model: Model, label: str) -> pd.DataFrame:
    assert model.basin.state.df is not None
    assert model.basin.profile.df is not None

    min_profile_level = model.basin.profile.df.groupby("node_id")["level"].min().rename("min_profile_level")
    basin_level_df = model.basin.state.df.merge(min_profile_level, on="node_id", how="left")
    basin_level_df["level_margin"] = basin_level_df["level"] - basin_level_df["min_profile_level"]

    invalid_df = basin_level_df[
        basin_level_df["min_profile_level"].notna() & basin_level_df["level"].lt(basin_level_df["min_profile_level"])
    ].sort_values("level_margin")

    if invalid_df.empty:
        print(f"{label}: geen basins met level onder profielbodem")
    else:
        print(f"{label}: {len(invalid_df)} basins met level onder profielbodem")
        print(invalid_df[["node_id", "level", "min_profile_level", "level_margin"]].head(20).to_string(index=False))

    return invalid_df


def propagate_target_levels_to_manning_upstream_basins(
    model: Model, aanvoergebieden_df: gpd.GeoDataFrame
) -> pd.DataFrame:
    assert model.node.df is not None
    assert model.basin.area.df is not None
    assert model.basin.state.df is not None

    basin_area_df = model.basin.area.df
    supply_area_polygon = aanvoergebieden_df.geometry.union_all()
    manning_gdf = model.node.df.loc[model.node.df["node_type"] == "ManningResistance"].copy()
    manning_gdf = gpd.GeoDataFrame(manning_gdf, geometry="geometry", crs=model.crs)
    manning_gdf = manning_gdf[manning_gdf.within(supply_area_polygon)]
    manning_node_ids = manning_gdf.index.to_list()
    manning_node_ids = [node_id for node_id in manning_node_ids if node_id != 1151]
    basin_level_change_records = []

    for manning_node_id in manning_node_ids:
        upstream_control_node_id = _find_first_control_node_along_network(model, manning_node_id, "upstream")
        if upstream_control_node_id is None:
            continue

        upstream_basin_id = _first_basin_along_network(model, manning_node_id, "upstream")
        if upstream_basin_id is None:
            continue

        control_node_id = _find_first_control_node_along_network(model, manning_node_id, "downstream")
        if control_node_id is None:
            continue

        control_upstream_basin_id = _first_basin_along_network(model, control_node_id, "upstream")
        if control_upstream_basin_id is None:
            continue

        source_mask = basin_area_df["node_id"] == control_upstream_basin_id
        target_mask = basin_area_df["node_id"] == upstream_basin_id
        if not source_mask.any() or not target_mask.any():
            continue

        source_level = basin_area_df.loc[source_mask, "meta_streefpeil"].iloc[0]
        old_level = basin_area_df.loc[target_mask, "meta_streefpeil"].iloc[0]

        if pd.isna(source_level) or old_level == source_level:
            continue

        basin_area_df.loc[target_mask, "meta_streefpeil"] = source_level
        basin_level_change_records.append(
            {
                "manning_node_id": manning_node_id,
                "target_basin_id": upstream_basin_id,
                "old_level": old_level,
                "new_level": source_level,
                "control_node_id": control_node_id,
                "control_node_type": model.get_node_type(control_node_id),
                "source_basin_id": control_upstream_basin_id,
                "upstream_control_node_id": upstream_control_node_id,
            }
        )

    model.basin.state.df = basin_area_df[["node_id", "meta_streefpeil"]].rename(columns={"meta_streefpeil": "level"})

    basin_level_change_df = pd.DataFrame(basin_level_change_records)
    if basin_level_change_df.empty:
        print("Geen basinpeilen aangepast via ManningResistance-netwerk.")
        return basin_level_change_df

    basin_level_change_df = basin_level_change_df.sort_values(["target_basin_id", "manning_node_id"]).drop_duplicates(
        subset=["target_basin_id"], keep="last"
    )

    print(f"ManningResistance-netwerk: {len(basin_level_change_df)} basinpeilen aangepast")
    for row in basin_level_change_df.itertuples():
        print(
            f"target_basin_id={row.target_basin_id}: {row.old_level} -> {row.new_level} "
            f"(manning_node_id={row.manning_node_id}, "
            f"source_basin_id={row.source_basin_id}, "
            f"control_node_id={row.control_node_id}, control_node_type={row.control_node_type}, "
            f"upstream_control_node_id={row.upstream_control_node_id})"
        )

    sync_basin_profiles_with_target_levels(model=model, basin_level_change_df=basin_level_change_df)

    return basin_level_change_df


def add_supply_area_control(
    model: Model,
    aanvoergebieden_df: gpd.GeoDataFrame,
    area_name: str,
    ignore_intersecting_links: list[int],
    supply_nodes: list[int],
    drain_nodes: list[int],
    flow_control_nodes: list[int],
) -> None:
    polygon = aanvoergebieden_df.loc[[area_name], "geometry"].union_all()

    # toevoegen sturing
    add_controllers_to_supply_area(
        model=model,
        polygon=polygon,
        exclude_nodes=EXCLUDE_NODES,
        ignore_intersecting_links=ignore_intersecting_links,
        drain_nodes=drain_nodes,
        flushing_nodes={},  # doorspoeling op uitlaten
        supply_nodes=supply_nodes,
        flow_control_nodes=flow_control_nodes,
        control_node_types=CONTROL_NODE_TYPES,
        add_supply_nodes=True,
    )


def run_model_and_control(model: Model, ribasim_toml, qlr_path):
    model.write(ribasim_toml)

    if MODEL_EXEC:
        model.run()
        Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path).run_all()
        return Model.read(ribasim_toml)

    return model


# %%
# Definieren paden en syncen met cloud

cloud = CloudStorage()

ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "aanvoergebieden.gpkg")
lhm_gemaal_gdb = cloud.joinpath(AUTHORITY, "Eerste_levering", "LHM20230418.gdb")
wateraanvoer_shp = Path(
    r"D:\Projecten\D2306.LHM_RIBASIM\02.brongegevens\Vechtstromen\verwerkt\1_ontvangen_data\wateraanvoergebieden_20250416\Wateraanvoer.shp"
)
new_basin_wateraanvoer_objectid = 377
basin_2148_wateraanvoer_objectid = 1753
min_basin_area_polygon_area = 1000.0

cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


# %%
# Read data

model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# Alle uitlaten en inlaten op 100 m3/s, geen cap verdeling. Dit wordt de max flow in model.
for static_df in [model.outlet.static.df, model.pump.static.df]:
    static_df["max_flow_rate"] = 100.0
    static_df["flow_rate"] = 100.0

# %%Duikers voor nu op 1m3/s
node_ids = model.outlet.node.df[model.outlet.node.df["meta_object_type"] == "duikersifonhevel"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)

# %%


model.redirect_link(1400, from_node_id=2300)
model.redirect_link(1183, to_node_id=1575)

# Linkrichting fixes
reverse_link_ids = [
    39,
    2105,
    1328,
    104,
    1684,
    547,
    1001,
    1732,
    1423,
    1013,
    255,
    2407,
    725,
    2201,
    249,
    2113,
    1782,
    664,
    1086,
    2261,
    2079,
    2713,
    2681,
    2824,
    1062,
    2180,
    1173,
    1343,
    427,
    1712,
    36,
    1777,
    1674,
    549,
    2822,
    1337,
    27,
    1891,
    796,
    2383,
    131,
    1953,
    1183,
    1993,
]

for link_id in reverse_link_ids:
    model.reverse_link(link_id=link_id)

# Basin 1843 splitsen met Wateraanvoer OBJECTID 377, plus extra basin + outlet.
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

streefpeil_1843_series = basin_1843_area_df["meta_streefpeil"].dropna()
streefpeil_1843 = float(streefpeil_1843_series.iloc[0]) if not streefpeil_1843_series.empty else None

wateraanvoer_df = gpd.read_file(wateraanvoer_shp)
objectid_column = next((column for column in wateraanvoer_df.columns if column.upper() == "OBJECTID"), None)
if objectid_column is None:
    raise ValueError("Kolom OBJECTID niet gevonden in Wateraanvoer.shp")


def clean_basin_area_geometry(geometry, label: str):
    geometry = shapely.force_2d(geometry).buffer(0)
    if geometry.geom_type == "Polygon":
        polygons = [geometry]
    elif geometry.geom_type == "MultiPolygon":
        polygons = list(geometry.geoms)
    else:
        raise ValueError(f"Geometry voor {label} heeft ongeldig type: {geometry.geom_type}")

    polygons = [polygon for polygon in polygons if polygon.area >= min_basin_area_polygon_area]
    if not polygons:
        raise ValueError(f"Geometry voor {label} is leeg na verwijderen van slivers")

    return MultiPolygon(polygons)


new_basin_area_df = wateraanvoer_df.loc[wateraanvoer_df[objectid_column] == new_basin_wateraanvoer_objectid].copy()
if new_basin_area_df.empty:
    raise ValueError(f"Geen feature gevonden met OBJECTID={new_basin_wateraanvoer_objectid} in {wateraanvoer_shp}")

basin_2148_area_df = wateraanvoer_df.loc[wateraanvoer_df[objectid_column] == basin_2148_wateraanvoer_objectid].copy()
if basin_2148_area_df.empty:
    raise ValueError(f"Geen feature gevonden met OBJECTID={basin_2148_wateraanvoer_objectid} in {wateraanvoer_shp}")

if new_basin_area_df.crs is not None and new_basin_area_df.crs != model.crs:
    new_basin_area_df = new_basin_area_df.to_crs(model.crs)
    basin_2148_area_df = basin_2148_area_df.to_crs(model.crs)

new_basin_geometry = clean_basin_area_geometry(
    new_basin_area_df.geometry.union_all(),
    label=f"OBJECTID {new_basin_wateraanvoer_objectid}",
)
new_basin_1843_geometry = clean_basin_area_geometry(
    old_basin_1843_geometry.difference(new_basin_geometry),
    label="node_id 1843",
)
basin_2148_geometry = clean_basin_area_geometry(
    basin_2148_area_df.geometry.union_all(),
    label=f"OBJECTID {basin_2148_wateraanvoer_objectid}",
)

model.basin.area.df = model.basin.area.df.loc[model.basin.area.df.node_id != 1843].copy()
model.add_basin_area(geometry=new_basin_1843_geometry, node_id=1843, meta_streefpeil=streefpeil_1843)
model.move_node(node_id=1843, geometry=Point(237696.58, 496023.31))

basin_2148_area_existing_df = model.basin.area.df.loc[model.basin.area.df.node_id == 2148].copy()
if basin_2148_area_existing_df.empty:
    raise ValueError("Geen bestaande basin_area gevonden voor node_id 2148")

streefpeil_2148_series = basin_2148_area_existing_df["meta_streefpeil"].dropna()
streefpeil_2148 = float(streefpeil_2148_series.iloc[0]) if not streefpeil_2148_series.empty else None
model.basin.area.df = model.basin.area.df.loc[model.basin.area.df.node_id != 2148].copy()
model.add_basin_area(geometry=basin_2148_geometry, node_id=2148, meta_streefpeil=streefpeil_2148)

new_basin_node_id = 2340
new_basin_node_geometry = old_basin_1843_node_geometry
model.basin.add(Node(node_id=new_basin_node_id, geometry=new_basin_node_geometry, name="Basin Westerhaar aanvoer"))

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


model.node._update_used_ids()
model.link._update_used_ids()
aanvoer_pump = model.pump.add(
    Node(geometry=Point(238834.91, 496511.08), name="Aanvoergemaal Westerhaar"),
    tables=DEFAULT_TABLES.pump,
)
model.link.add(model.basin[1843], aanvoer_pump)
model.link.add(aanvoer_pump, model.basin[1455])

westerhaar_uitlaat_outlet = model.outlet.add(
    Node(geometry=Point(238833.885, 496508.498), name="Uitlaat Westerhaar"),
    tables=DEFAULT_TABLES.outlet,
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
    Node(geometry=Point(243225.458, 495478.36), name="Inlaatstuw"),
    tables=DEFAULT_TABLES.outlet,
)
model.link.add(model.basin[1844], inlaatstuw_outlet)
model.link.add(inlaatstuw_outlet, model.basin[1450])

inlaat_outlet_1708 = model.outlet.add(
    Node(geometry=Point(238366.036, 496286.96), name="Inlaat"),
    tables=DEFAULT_TABLES.outlet,
)
model.link.add(model.basin[1843], inlaat_outlet_1708)
model.link.add(inlaat_outlet_1708, model.basin[1708])

uitlaat_outlet_1569 = model.outlet.add(
    Node(geometry=Point(241738.30642, 513291.13972), name="Uitlaat"),
    tables=DEFAULT_TABLES.outlet,
)
model.link.add(model.basin[2068], uitlaat_outlet_1569)
model.link.add(uitlaat_outlet_1569, model.basin[1569])


# Gerichte link-aanpassingen rond Westerhaar.
model.redirect_link(link_id=525, to_node_id=new_basin_node_id)
model.redirect_link(link_id=1737, from_node_id=new_basin_node_id)
model.redirect_link(link_id=1890, from_node_id=1843)
model.redirect_link(link_id=102, to_node_id=1843)
model.redirect_link(link_id=1891, to_node_id=1843)
model.redirect_link(link_id=1889, from_node_id=1843)
model.redirect_link(link_id=1403, from_node_id=new_basin_node_id)
model.redirect_link(link_id=523, to_node_id=new_basin_node_id)
model.redirect_link(link_id=2840, from_node_id=2341, to_node_id=new_basin_node_id)


# Extra outlet-node tussen 2298 en 1583.
from_node = model.get_node(2298)
to_node = model.get_node(1583)

if not (can_connect(from_node.node_type, "Outlet") and can_connect("Outlet", to_node.node_type)):
    raise ValueError(
        f"Gevraagde route met Outlet is ongeldig: {from_node.node_type} -> Outlet -> {to_node.node_type} "
        f"(nodes: {from_node.node_id} -> {to_node.node_id})."
    )

extra_connection_node = model.outlet.add(
    Node(geometry=Point(241321.928, 486550.60)),
    tables=DEFAULT_TABLES.outlet,
)
model.link.add(from_node, extra_connection_node)
model.link.add(extra_connection_node, to_node)

# Nieuwe outlet-node bij Nieuwe Drostendiep.
from_node = model.get_node(1575)
to_node = model.get_node(2304)

if not (can_connect(from_node.node_type, "Outlet") and can_connect("Outlet", to_node.node_type)):
    raise ValueError(
        f"Gevraagde route met Outlet is ongeldig: {from_node.node_type} -> Outlet -> {to_node.node_type} "
        f"(nodes: {from_node.node_id} -> {to_node.node_id})."
    )

nieuwe_drostendiep_outlet = model.outlet.add(
    Node(geometry=Point(250205.53, 524562.22), name="Uitlaat Nieuwe Drostendiep"),
    tables=DEFAULT_TABLES.outlet,
)
model.link.add(from_node, nieuwe_drostendiep_outlet)
model.link.add(nieuwe_drostendiep_outlet, to_node)

# %%
# Node type fixes

lhm_structure_gdf = read_lhm_structure_points(lhm_gemaal_gdb, crs=model.crs)
gdb_outlet_node_ids, gdb_pump_node_ids, gdb_manning_type_change_df = get_manning_nodes_near_structures(
    model=model,
    structure_gdf=lhm_structure_gdf,
    max_distance=2,
)

# make outlet nodes
update_nodes(
    model,
    [
        1147,
        1307,
        1032,
        1304,
        1340,
        1314,
        1301,
        1329,
        1359,
        1146,
        1251,
        1140,
        1335,
        1237,
        1238,
        1177,
        1153,
        1156,
        1359,
        1136,
        1137,
        1168,
        1169,
        1167,
        1353,
        1142,
        *gdb_outlet_node_ids,
    ],
    "Outlet",
)

# make manning nodes
update_nodes(
    model,
    [
        778,
        882,
        824,
        689,
        692,
        908,
        868,
        724,
        893,
        863,
        293,
        937,
        894,
        749,
        936,
        878,
        713,
        200,
        869,
        958,
        952,
        822,
        768,
        49,
        771,
        801,
        806,
        859,
    ],
    "ManningResistance",
)

# make pump nodes
update_nodes(model, gdb_pump_node_ids, "Pump")

gdb_basin_level_change_df = propagate_target_levels_to_manning_upstream_basins(
    model=model,
    aanvoergebieden_df=aanvoergebieden_df,
)

# Verwijderen nutteloze kunstwerken voor LHM
remove_nodes(
    model,
    [
        1141,
        614,
        88,
        129,
        48,
        147,
        1151,
        497,
        462,
        417,
        1142,
        3008,
        431,
        288,
        631,
        637,
        328,
        699,
        505,
        206,
        321,
        392,
        488,
        1343,
        305,
        330,
        51,
        719,
        380,
        711,
    ],
)


# Streefpeil te laag
set_static_values(
    model.outlet.static.df,
    {},
    "min_upstream_level",
)
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 2300, "level"] = 15.33

for node_id, to_node_id in [
    (1904, 1675),
    (1389, 2138),
    (2194, 2138),
    (1443, 1371),
    (1571, 1371),
    (2241, 1864),
    (1869, 1621),
    (2215, 1978),
    (1987, 1433),
    (2263, 1721),
    (1973, 1551),
    (1988, 1775),
    (2101, 1775),
    (1496, 1838),
    (2196, 1838),
    (2072, 1540),
    (1976, 1540),
    (1770, 1540),
    (2314, 1627),
    (2058, 2138),
    (1738, 2138),
    (2233, 1529),
    (1529, 1444),
    (1630, 2123),
    (1966, 1467),
]:
    model.merge_basins(node_id=node_id, to_node_id=to_node_id, are_connected=True)
for node_id, to_node_id in [
    (2094, 1681),
    (1863, 1779),
    (1787, 2333),
    (1642, 1510),
]:
    model.merge_basins(node_id=node_id, to_node_id=to_node_id, are_connected=False)

assert model.basin.area.df is not None
basin_level_overrides_after_fixes = {
    1700: 9.1,
    new_basin_node_id: 9.7,
}
for basin_node_id, basin_level in basin_level_overrides_after_fixes.items():
    basin_level_mask = model.basin.area.df["node_id"] == basin_node_id
    if not basin_level_mask.any():
        continue

    old_basin_level = model.basin.area.df.loc[basin_level_mask, "meta_streefpeil"].iloc[0]
    model.basin.area.df.loc[basin_level_mask, "meta_streefpeil"] = basin_level
    if pd.notna(old_basin_level) and old_basin_level != basin_level:
        sync_basin_profiles_with_target_levels(
            model=model,
            basin_level_change_df=pd.DataFrame(
                [
                    {
                        "target_basin_id": basin_node_id,
                        "old_level": old_basin_level,
                        "new_level": basin_level,
                    }
                ]
            ),
        )

model.basin.state.df = model.basin.area.df[["node_id", "meta_streefpeil"]].rename(columns={"meta_streefpeil": "level"})
validate_basin_levels_against_profiles(model=model, label="Na handmatige basinpeil fixes")

# %%
# Max-capaciteiten pompen en sluizen

outlet_max_flow_rate_by_node_id = {
    #   454: 16.2,  # Paradijssluis
}

pump_max_flow_rate_by_node_id = {
    #   704: 6.7,  # Paradijssluis
}

set_max_flow_rate(model.outlet.static.df, outlet_max_flow_rate_by_node_id)
set_max_flow_rate(model.pump.static.df, pump_max_flow_rate_by_node_id)


def print_node_list_diff(label: str, before_nodes: list[int], after_nodes: list[int]) -> None:
    before_nodes = set(before_nodes)
    after_nodes = set(after_nodes)

    added_nodes = sorted(after_nodes - before_nodes)
    removed_nodes = sorted(before_nodes - after_nodes)

    print(f"{label}: {len(added_nodes)} toegevoegd, {len(removed_nodes)} verwijderd t.o.v. handmatig")
    print(f"{label} toegevoegd: {added_nodes}")
    print(f"{label} verwijderd: {removed_nodes}")


def print_supply_role_conflicts(
    label: str,
    source_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> None:
    source_supply_nodes = set(source_supply_nodes)
    manual_drain_nodes = set(manual_drain_nodes)
    manual_flow_control_nodes = set(manual_flow_control_nodes)

    print(
        f"{label} overruled door handmatig: "
        f"flow_control={sorted(source_supply_nodes & manual_flow_control_nodes)}, "
        f"drain={sorted(source_supply_nodes & manual_drain_nodes)}"
    )


def combine_control_node_roles(
    kunstwerk_supply_nodes: list[int],
    kunstwerk_drain_nodes: list[int],
    manual_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> tuple[list[int], list[int], list[int]]:
    flow_control_nodes = list(dict.fromkeys(manual_flow_control_nodes))
    supply_nodes = list(dict.fromkeys(node_id for node_id in manual_supply_nodes if node_id not in flow_control_nodes))
    drain_nodes = list(
        dict.fromkeys(
            node_id
            for node_id in manual_drain_nodes
            if node_id not in flow_control_nodes and node_id not in supply_nodes
        )
    )

    manual_nodes = set(flow_control_nodes + supply_nodes + drain_nodes)
    kunstwerk_supply_nodes = list(
        dict.fromkeys(node_id for node_id in kunstwerk_supply_nodes if node_id not in manual_nodes)
    )
    kunstwerk_drain_nodes = list(
        dict.fromkeys(
            node_id
            for node_id in kunstwerk_drain_nodes
            if node_id not in manual_nodes and node_id not in kunstwerk_supply_nodes
        )
    )

    supply_nodes += kunstwerk_supply_nodes
    drain_nodes += kunstwerk_drain_nodes

    return supply_nodes, drain_nodes, flow_control_nodes


def normalize_name_series(series):
    return series.astype("string").str.strip().str.replace(r"\s+", " ", regex=True).str.lower()


def get_lhm_gemaal_supply_nodes(
    model: Model,
    lhm_gemaal_gdb: Path,
    layer: str = "GEMAAL",
    name_column: str = "NAAM",
    name_prefix: str = "inlaat",
    node_types: list[str] = CONTROL_NODE_TYPES,
) -> list[int]:
    gemaal_df = gpd.read_file(lhm_gemaal_gdb, layer=layer)

    inlaat_mask = normalize_name_series(gemaal_df[name_column]).str.startswith(name_prefix.lower(), na=False)
    inlaat_names = set(normalize_name_series(gemaal_df.loc[inlaat_mask, name_column]).dropna())

    control_node_df = model.node.df[model.node.df["node_type"].isin(node_types)].copy()
    control_node_df["_match_name"] = normalize_name_series(control_node_df["name"])
    matched_node_df = control_node_df[control_node_df["_match_name"].isin(inlaat_names)]

    matched_node_ids = matched_node_df.index.astype(int).drop_duplicates().to_list()
    matched_names = set(matched_node_df["_match_name"].dropna())
    unmatched_names = sorted(inlaat_names - matched_names)

    print(f"LHM GEMAAL inlaten: {len(inlaat_names)} namen, {len(matched_node_ids)} Ribasim-nodes gevonden")
    if unmatched_names:
        print(f"LHM GEMAAL inlaten zonder match: {unmatched_names}")

    return matched_node_ids


# %%
#
# #Toevoegen aanvoergebieden
# Handmatige indeling control-, supply- en drain-nodes

flow_control_nodes = [
    500,
    700,
    135,
    428,
    359,
    478,
    509,
    42,
    1004,
    971,
    1314,
    765,
    1074,
    1088,
    522,
    365,
    156,
    337,
    518,
    208,
    455,
    672,
    331,
    317,
    672,
    198,
    282,
    498,
    38,
    89,
    349,
    271,
    1083,
    820,
    998,
    908,
    845,
    840,
    1041,
    81,
    361,
    635,
    231,
    632,
    312,
    364,
    666,
    464,
    1269,
    445,
    513,
    159,
    405,
    853,
    201,
    191,
    539,
    2334,
    446,
    963,
    373,
]

supply_nodes = [
    75,
    625,
    983,
    1307,
    253,
    2344,
    481,
    1301,
    709,
    629,
    1017,
    477,
    459,
    1013,
    1065,
    623,
    1000,
    553,
    736,
    250,
    762,
    1057,
    670,
    554,
    914,
    986,
    199,
    2340,
    2339,
    1006,
    680,
    768,
    694,
    1025,
    1018,
    102,
    1003,
    1007,
    1025,
    403,
    978,
    991,
    492,
    399,
    469,
    734,
    1080,
    1012,
    1013,
    200,
    325,
    155,
    677,
    973,
    286,
    676,
    626,
    584,
    1087,
    709,
    1020,
    2318,
    606,
    26,
    501,
    611,
    726,
    979,
    1069,
    720,
    846,
    621,
    658,
    432,
    564,
    547,
    684,
    648,
    961,
    571,
    44,
    734,
    589,
    988,
    447,
    1005,
    2341,
    53,
    2343,
    665,
    595,
    467,
    690,
    964,
    990,
    465,
    1037,
    993,
    985,
    760,
    1032,
    768,
    468,
    2346,
    976,
    297,
]

drain_nodes = [
    2342,
    171,
    112,
    124,
    729,
    452,
    1008,
    966,
    445,
    598,
    354,
    895,
    840,
    92,
    592,
    2345,
    880,
    772,
    805,
    873,
    845,
    928,
    909,
    733,
    94,
    398,
    416,
    781,
    820,
    792,
    424,
    792,
    780,
    921,
    243,
    2324,
    510,
    790,
    850,
    440,
    714,
    662,
    913,
    241,
    401,
    617,
    52,
    2338,
    233,
    281,
    565,
    224,
    475,
    1066,
    531,
    1008,
    664,
    161,
    577,
    500,
    600,
    807,
    764,
    736,
    412,
    540,
    763,
    226,
    971,
    556,
    557,
    444,
    552,
    339,
    869,
    2918,
    960,
    222,
    1132,
    455,
    856,
    249,
    761,
    174,
    350,
    212,
    1102,
    1089,
    1071,
    568,
    724,
    667,
    730,
    674,
    604,
    586,
    776,
    221,
    334,
    450,
    514,
    740,
    334,
    704,
    87,
    783,
    1353,
    494,
    46,
    38,
    490,
    240,
    134,
    1050,
    650,
    605,
    181,
    614,
    583,
    1051,
    429,
    637,
    36,
    829,
    99,
    283,
    303,
    106,
    734,
    882,
    303,
    432,
    106,
    79,
    154,
    162,
    379,
    864,
    703,
    215,
    273,
    741,
    39,
    40,
    45,
    88,
    90,
    91,
    98,
    118,
    136,
    142,
    144,
    150,
    157,
    160,
    163,
    169,
    185,
    195,
    203,
    211,
    225,
    228,
    257,
    266,
    276,
    284,
    296,
    302,
    304,
    306,
    309,
    675,
    333,
    348,
    353,
    376,
    384,
    388,
    400,
    402,
    408,
    423,
    433,
    443,
    449,
    451,
    453,
    467,
    473,
    487,
    491,
    501,
    504,
    505,
    511,
    519,
    529,
    530,
    533,
    548,
    549,
    550,
    558,
    578,
    582,
    595,
    610,
    621,
    628,
    645,
    666,
    738,
    686,
    707,
    712,
    713,
    715,
    722,
    723,
    743,
    773,
    791,
    795,
    801,
    806,
    817,
    822,
    831,
    852,
    855,
    860,
    881,
    890,
    891,
    906,
    907,
    919,
    930,
    932,
    933,
    935,
    949,
    952,
    958,
    965,
    975,
    981,
    1064,
    1095,
    1290,
    2335,
    2336,
    342,
    247,
    597,
    512,
]


manual_flow_control_nodes = list(flow_control_nodes)
manual_supply_nodes = list(supply_nodes)
manual_drain_nodes = list(drain_nodes)

lhm_gemaal_supply_nodes = get_lhm_gemaal_supply_nodes(model=model, lhm_gemaal_gdb=lhm_gemaal_gdb)

if IS_SUPPLY_NODE_COLUMN not in model.node.df.columns:
    model.node.df[IS_SUPPLY_NODE_COLUMN] = False
model.node.df.loc[lhm_gemaal_supply_nodes, IS_SUPPLY_NODE_COLUMN] = True

print_supply_role_conflicts(
    label="LHM GEMAAL inlaten",
    source_supply_nodes=lhm_gemaal_supply_nodes,
    manual_drain_nodes=manual_drain_nodes,
    manual_flow_control_nodes=manual_flow_control_nodes,
)

supply_nodes, drain_nodes, flow_control_nodes = combine_control_node_roles(
    kunstwerk_supply_nodes=lhm_gemaal_supply_nodes,
    kunstwerk_drain_nodes=[],
    manual_supply_nodes=manual_supply_nodes,
    manual_drain_nodes=manual_drain_nodes,
    manual_flow_control_nodes=manual_flow_control_nodes,
)

print_node_list_diff("supply_nodes", manual_supply_nodes, supply_nodes)


# Per gebied: links die intersecten die we kunnen negeren.

supply_area_ignore_links = {
    "Boven Regge": [868, 1072, 1257, 1258, 1259, 1260, 2508, 2640],
    "Regge": [
        27,
        403,
        404,
        405,
        429,
        473,
        474,
        512,
        612,
        622,
        623,
        624,
        800,
        868,
        1072,
        1156,
        1243,
        1249,
        1250,
        1938,
        2499,
        2508,
        2580,
        2582,
        2594,
        2641,
        91,
        92,
        93,
        392,
        472,
        2193,
    ],
    "Lateraalkanaal": [403, 92, 428, 269, 270, 404, 405, 1243, 436, 992, 2479, 1058, 1113, 1156, 1252, 1394, 1396],
    "Bolscherbeek": [],
    "Vriezenveen": [1891, 1291, 436, 992, 2479, 487, 489, 491, 492],
    "Dooze": [918, 2629, 133, 1236, 1783, 2411],
    "Schipbeek": [541],
    "Dinkel": [],
    "Overijsselsch Kanaal noord": [133, 1172, 2671],
    "Vecht": [1044, 258, 17, 41, 42, 1046, 1047],
    "Coevorden-Zwinderen": [1136, 2109, 2398, 2425, 2436, 2234],
    "Geesbrug": [],
    "Braambergersloot": [71, 2109, 2777, 659],
    "Nieuwe Drostendiep": [1993, 1183, 2445, 71, 255, 2362, 2404, 2623],
    "Schoonebeek": [],
    "Oranjekanaal": [598, 2414, 2604],
    "Oosterwijk": [2633],
    "Oosterhesselen": [],
}

for area_name, ignore_intersecting_links in supply_area_ignore_links.items():
    print(f"Toevoegen {area_name}")
    add_supply_area_control(
        model=model,
        aanvoergebieden_df=aanvoergebieden_df,
        area_name=area_name,
        ignore_intersecting_links=ignore_intersecting_links,
        supply_nodes=supply_nodes,
        drain_nodes=drain_nodes,
        flow_control_nodes=flow_control_nodes,
    )


# %%
# Add all remaining inlets/outlets

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes={},
    exclude_nodes=list(EXCLUDE_NODES),
)


# %%
# EXCLUDE_NODES op 0 m3/s zetten

mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, ["flow_rate", "min_flow_rate", "max_flow_rate"]] = 0.0


# %%
# Junctionify(!)

model = junctionify(model)


# %%
# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=1)
model = run_model_and_control(model, ribasim_toml_dry, qlr_path)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model = run_model_and_control(model, ribasim_toml_wet, qlr_path)

# hoofd run
update_basin_static(model=model, precipitation_mm_per_day=1.5)
model = run_model_and_control(model, ribasim_toml, qlr_path)

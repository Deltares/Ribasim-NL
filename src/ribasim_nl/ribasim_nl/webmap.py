"""Export a Ribasim model to web-friendly files for the documentation model viewer."""

import hashlib
import json
import shutil
import sqlite3
import tomllib
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import shapely
import xarray as xr

from ribasim_nl.case_conversions import pascal_to_snake_case, snake_to_pascal_case

WEB_CRS = "EPSG:4326"
# Simplification tolerances in model CRS units (m)
LINK_TOLERANCE = 2.0
WATERBOARD_TOLERANCE = 25.0
ROW_GROUP_SIZE = 4096
NETCDF_NODES_PER_ROW_GROUP = 16
SKIP_TABLES = {"layer_styles", "ribasim_metadata"}


def file_hash(path: Path) -> str:
    """Return a short content hash of a file, used for cache busting."""
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()[:16]


def table_slug(name: str) -> str:
    """Convert a Ribasim table name like "Basin / profile" to "basin_profile"."""
    node_type, _, table = name.partition(" / ")
    assert table, f"Expected '<NodeType> / <table>', got {name!r}"
    return f"{pascal_to_snake_case(node_type)}_{table}"


def downcast_int64(table: pa.Table) -> pa.Table:
    """Cast int64 columns that fit to int32, since JavaScript readers return int64 as BigInt."""
    info = np.iinfo(np.int32)
    for i, field in enumerate(table.schema):
        if field.type == pa.int64():
            column = table.column(i)
            values = column.drop_null().to_numpy()
            if len(values) == 0 or (info.min <= values.min() and values.max() <= info.max):
                table = table.set_column(i, field.name, column.cast(pa.int32()))
    return table


def write_parquet(df: pd.DataFrame, path: Path, row_group_size: int = ROW_GROUP_SIZE) -> None:
    """Write a DataFrame to zstd compressed Parquet with column statistics for row group lookup."""
    table = downcast_int64(pa.Table.from_pandas(df, preserve_index=False))
    pq.write_table(table, path, compression="zstd", row_group_size=row_group_size, write_statistics=True)


def lonlat_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add float32 `x` (longitude) and `y` (latitude) columns from point geometries."""
    assert (gdf.geom_type == "Point").all(), "Expected only Point geometries"
    web = gdf.to_crs(WEB_CRS)
    gdf = gdf.copy()
    gdf["x"] = web.geometry.x.astype("float32")
    gdf["y"] = web.geometry.y.astype("float32")
    return gdf


def flat_coords(geometry: gpd.GeoSeries) -> list[np.ndarray]:
    """Return per-LineString interleaved float32 lon/lat coordinate arrays."""
    assert (geometry.geom_type == "LineString").all(), "Expected only LineString geometries"
    return [np.asarray(shapely.get_coordinates(g), dtype="float32").ravel() for g in geometry.to_crs(WEB_CRS).values]


def read_features(database: Path, layer: str, id_column: str) -> gpd.GeoDataFrame:
    """Read a layer whose id column is the GeoPackage FID, so GDAL does not return it as a column."""
    gdf = gpd.read_file(database, layer=layer, fid_as_index=True, use_arrow=True)
    return gdf.rename_axis(id_column).reset_index()


def export_nodes(database: Path, path: Path) -> gpd.GeoDataFrame:
    """Export the Node table with all attributes and lon/lat columns, sorted by node_id."""
    nodes = read_features(database, "Node", "node_id")
    assert nodes.node_id.is_unique, "Node table has duplicate node_id"
    nodes = lonlat_columns(nodes).sort_values("node_id")
    write_parquet(pd.DataFrame(nodes.drop(columns="geometry")), path)
    return nodes


def export_links(database: Path, path: Path) -> None:
    """Export the Link table with simplified geometry as interleaved lon/lat `coords`, sorted by link_id."""
    links = read_features(database, "Link", "link_id")
    assert links.link_id.is_unique, "Link table has duplicate link_id"
    links = links.sort_values("link_id")
    links["geometry"] = links.geometry.simplify(LINK_TOLERANCE)
    df = pd.DataFrame(links.drop(columns="geometry"))
    table = downcast_int64(pa.Table.from_pandas(df, preserve_index=False)).append_column(
        "coords", pa.array(flat_coords(links.geometry), type=pa.list_(pa.float32()))
    )
    pq.write_table(table, path, compression="zstd", row_group_size=ROW_GROUP_SIZE, write_statistics=True)


def export_basin_area(database: Path, path: Path) -> None:
    """Export Basin / area polygons as vector tiles; attributes are looked up by node_id."""
    area = gpd.read_file(database, layer="Basin / area", columns=["node_id"], use_arrow=True)
    assert area.node_id.notna().all(), "Basin / area has missing node_id"
    pyogrio.write_dataframe(
        area[["node_id", "geometry"]],
        path,
        driver="PMTiles",
        layer="basin_area",
        dataset_options={"MINZOOM": "6", "MAXZOOM": "13", "MAX_SIZE": "2000000", "MAX_FEATURES": "500000"},
    )


def export_waterboards(source: Path, path: Path) -> None:
    """Export simplified water board boundaries as GeoJSON."""
    boards = gpd.read_file(source, columns=["naam", "code"])
    assert len(boards) > 0, f"No water boards in {source}"
    boards["naam"] = boards["naam"].str.strip()
    boards["geometry"] = boards.geometry.simplify(WATERBOARD_TOLERANCE)
    boards.to_crs(WEB_CRS).to_file(path, driver="GeoJSON", layer_options={"COORDINATE_PRECISION": "5"})


def attribute_tables(database: Path) -> list[str]:
    """List the non-spatial Ribasim tables in a GeoPackage."""
    with sqlite3.connect(database) as con:
        rows = con.execute("SELECT table_name FROM gpkg_contents WHERE data_type = 'attributes'").fetchall()
    return sorted(name for (name,) in rows if name not in SKIP_TABLES)


def netcdf_tables(config: dict, input_dir: Path) -> dict[str, Path]:
    """Map table names like "Basin / time" to the NetCDF files referenced in the TOML."""
    tables = {}
    for section, values in config.items():
        if not isinstance(values, dict):
            continue
        for key, value in values.items():
            if isinstance(value, str) and value.endswith(".nc"):
                tables[f"{snake_to_pascal_case(section)} / {key}"] = input_dir / value
    return tables


def export_gpkg_table(database: Path, name: str, path: Path) -> int:
    """Export a GeoPackage attribute table sorted by node_id, return the number of rows."""
    with warnings.catch_warnings():
        # GDAL reads Ribasim's TIMESTAMP columns as strings, parsed below
        warnings.filterwarnings("ignore", message="Field format 'TIMESTAMP' not supported")
        df = pyogrio.read_dataframe(database, layer=name, read_geometry=False, use_arrow=True)
    assert "node_id" in df.columns, f"Table {name!r} has no node_id column"
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"]).astype("datetime64[ms]")
    df = df.sort_values("node_id", kind="stable")
    write_parquet(df, path)
    return len(df)


def export_netcdf_table(source: Path, path: Path) -> int:
    """Export a (time, node_id) NetCDF table to long-format float32 Parquet, one row group per node block."""
    with xr.open_dataset(source) as ds:
        assert set(ds.dims) == {"time", "node_id"}, f"Unsupported dimensions in {source}: {dict(ds.sizes)}"
        node_ids = ds["node_id"].to_numpy().astype("int32")
        times = ds["time"].to_numpy().astype("datetime64[ms]")
        variables = list(ds.data_vars)
        data = {v: ds[v].transpose("node_id", "time").to_numpy().astype("float32") for v in variables}

    schema = pa.schema([("node_id", pa.int32()), ("time", pa.timestamp("ms"))] + [(v, pa.float32()) for v in variables])
    order = np.argsort(node_ids, kind="stable")
    ntime = len(times)
    with pq.ParquetWriter(path, schema, compression="zstd", write_statistics=True) as writer:
        for start in range(0, len(order), NETCDF_NODES_PER_ROW_GROUP):
            block = order[start : start + NETCDF_NODES_PER_ROW_GROUP]
            columns = {
                "node_id": np.repeat(node_ids[block], ntime),
                "time": np.tile(times, len(block)),
            } | {v: data[v][block].ravel() for v in variables}
            writer.write_table(pa.table(columns, schema=schema), row_group_size=len(block) * ntime)
    return len(node_ids) * ntime


def export_webmap(toml_path: Path, waterboards_path: Path, output_dir: Path) -> dict:
    """Export a Ribasim model to a directory of web-friendly files plus a `manifest.json`.

    The viewer fetches files as `<path>?v=<hash>` so they can be cached indefinitely.
    """
    assert toml_path.is_file(), f"TOML not found: {toml_path}"
    assert waterboards_path.is_file(), f"Water boards not found: {waterboards_path}"
    with toml_path.open("rb") as f:
        config = tomllib.load(f)
    input_dir = toml_path.parent / config.get("input_dir", ".")
    database = input_dir / "database.gpkg"
    assert database.is_file(), f"Database not found: {database}"

    if output_dir.exists():
        shutil.rmtree(output_dir)
    (output_dir / "tables").mkdir(parents=True)

    def entry(path: Path) -> dict:
        return {
            "path": path.relative_to(output_dir).as_posix(),
            "hash": file_hash(path),
            "bytes": path.stat().st_size,
        }

    nodes = export_nodes(database, output_dir / "nodes.parquet")
    export_links(database, output_dir / "links.parquet")
    export_basin_area(database, output_dir / "basin_area.pmtiles")
    export_waterboards(waterboards_path, output_dir / "waterboards.geojson")
    files = {
        name: entry(output_dir / filename)
        for name, filename in [
            ("nodes", "nodes.parquet"),
            ("links", "links.parquet"),
            ("basin_area", "basin_area.pmtiles"),
            ("waterboards", "waterboards.geojson"),
        ]
    }

    tables = []
    sources: list[tuple[str, Path | None]] = [(name, None) for name in attribute_tables(database)]
    sources += list(netcdf_tables(config, input_dir).items())
    for name, netcdf in sorted(sources):
        path = output_dir / "tables" / f"{table_slug(name)}.parquet"
        rows = export_gpkg_table(database, name, path) if netcdf is None else export_netcdf_table(netcdf, path)
        tables.append({"name": name, "node_type": name.partition(" / ")[0], "rows": rows} | entry(path))

    x, y = nodes["x"], nodes["y"]
    manifest = {
        "model": toml_path.stem,
        "ribasim_version": config.get("ribasim_version"),
        "starttime": str(config["starttime"]),
        "endtime": str(config["endtime"]),
        "bounds": [float(x.min()), float(y.min()), float(x.max()), float(y.max())],
        "node_types": nodes["node_type"].value_counts().sort_index().to_dict(),
        "files": files,
        "tables": tables,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest

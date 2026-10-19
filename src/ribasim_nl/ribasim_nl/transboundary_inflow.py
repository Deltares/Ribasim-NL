"""Apply foreign inflows without changing model nodes or their metadata."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import geopandas as gpd
import numpy as np
import pandas as pd

from ribasim_nl.aquo import get_id, get_name

if TYPE_CHECKING:
    from ribasim_nl.model import Model

logger = logging.getLogger(__name__)

Input = Literal["timeseries", "constant_global", "constant_summer", "constant_winter"]
Mapping = Literal["name", "coordinate", "hydroobject"]


def _map_by_name(table: pd.DataFrame, nodes: "gpd.GeoDataFrame") -> pd.Series:
    """Match overview names to existing node names."""
    names = nodes["name"].astype("string").str.strip()
    if names.isna().any() or names.duplicated().any():
        raise ValueError("Foreign FlowBoundary names must be present and unique for name mapping")
    node_ids = table["name"].str.strip().map(pd.Series(nodes.index, index=names))
    if node_ids.isna().any():
        missing = table.loc[node_ids.isna(), "name"].tolist()
        raise ValueError(f"Mapping names not found among foreign FlowBoundaries: {missing}")
    return node_ids.astype("int64")


def _map_by_coordinate(table: pd.DataFrame, nodes: "gpd.GeoDataFrame") -> pd.Series:
    """Match overview coordinates to a unique node within one metre."""
    if nodes.crs is None or nodes.crs.to_epsg() != 28992:
        raise ValueError("Coordinate mapping requires model nodes in EPSG:28992")
    coordinates = table[["coordinate_x", "coordinate_y"]].apply(pd.to_numeric, errors="raise")
    if not np.isfinite(coordinates.to_numpy(dtype=float)).all():
        raise ValueError("Boundary coordinates must be finite")
    rows, positions = nodes.sindex.query(
        gpd.points_from_xy(coordinates["coordinate_x"], coordinates["coordinate_y"]), predicate="dwithin", distance=1.0
    )
    counts = np.bincount(rows, minlength=len(table))
    if (counts != 1).any():
        row = np.flatnonzero(counts != 1)[0]
        x, y = coordinates.iloc[row]
        raise ValueError(f"Expected one foreign FlowBoundary within 1 m of ({x}, {y}); found {counts[row]}")
    return pd.Series(nodes.index.take(positions), index=table.index.take(rows), dtype="int64").reindex(table.index)


def _couple_nodes(table: pd.DataFrame, nodes: "gpd.GeoDataFrame", mapping: Mapping) -> pd.Series:
    """Dispatch node mapping and validate one-to-one coverage."""
    node_ids = _map_by_name(table, nodes) if mapping == "name" else _map_by_coordinate(table, nodes)
    if node_ids.duplicated().any() or set(node_ids) != set(nodes.index):
        raise ValueError("The mapping must cover each foreign FlowBoundary exactly once")
    return node_ids


def _read_mapping(mapping_table: Path, authority: str | int, input: Input, mapping: Mapping) -> pd.DataFrame:
    """Filter the overview by authority and validate the requested constant values."""
    code = get_id(name=authority) if isinstance(authority, str) else authority
    authority_name = get_name(id=code)
    table = pd.read_csv(mapping_table, dtype={"id": "string", "name": "string", "timeseries": "string"})
    required = {"id", "name", "authority_aquo", "timeseries", "constant_global", "constant_summer", "constant_winter"}
    if mapping == "coordinate":
        required.update(("coordinate_x", "coordinate_y"))
    if missing := required.difference(table.columns):
        raise ValueError(f"Missing mapping columns: {sorted(missing)}")
    if table["id"].isna().any() or table["id"].duplicated().any():
        raise ValueError("Boundary mapping IDs must be present and unique")
    codes = pd.to_numeric(table["authority_aquo"], errors="raise")
    table = table.loc[codes.eq(code)].copy()
    if "authority_name" in table and not table["authority_name"].eq(authority_name).all():
        raise ValueError(f"Mapping authority name and Aquo code disagree for {authority_name}")

    constants = pd.to_numeric(table["constant_global"], errors="raise")
    if not np.isfinite(constants.to_numpy(dtype=float)).all() or constants.lt(0).any():
        raise ValueError("constant_global must be finite and non-negative for every selected boundary")
    table["constant_global"] = constants
    if input in ("constant_summer", "constant_winter"):
        selected = pd.to_numeric(table[input], errors="raise")
        available = selected.dropna()
        if not np.isfinite(available.to_numpy(dtype=float)).all() or available.lt(0).any():
            raise ValueError(f"{input} must be finite and non-negative")
        table[input] = selected
    return table


def _timeseries_path(reference: str, directory: Path) -> Path:
    """Resolve an extensionless CSV reference within the overview directory."""
    relative = Path(reference.replace("\\", "/"))
    path = (directory / f"{relative}.csv").resolve()
    if relative.is_absolute() or not path.is_relative_to(directory):
        raise ValueError(f"Timeseries reference must stay within the overview directory: {reference}")
    return path


def _read_timeseries(path: Path) -> pd.DataFrame:
    """Read an exported measurement sheet, preserving local wall-clock timestamps."""
    frame = pd.read_csv(path, dtype={"Datum": "string"}, low_memory=False)
    if "Datum" not in frame:
        raise ValueError(f"Missing Datum column in {path}")
    frame["Datum"] = pd.to_datetime(frame["Datum"], format="ISO8601", errors="raise")
    missing = frame["Datum"].isna()
    if missing.any():
        logger.warning("%s: dropping %s rows without a timestamp", path, int(missing.sum()))
    frame = frame.loc[~missing].copy()
    if frame["Datum"].dt.tz is not None:
        raise ValueError(f"Expected timezone-naive local timestamps in {path}")
    return frame.set_index("Datum").sort_index()


def _resample(
    values: pd.DataFrame, start: pd.Timestamp, stop: pd.Timestamp, frequency: Literal["h", "D"]
) -> pd.DataFrame:
    """Average finer measurements or interpolate coarser ones without extrapolation."""
    timestep = pd.Timedelta(hours=24 if frequency == "D" else 1)
    grid = pd.date_range(start, stop, freq=timestep)
    source_timestep = values.index.unique().to_series().diff().median()
    if source_timestep > timestep:
        values = values.groupby(level=0).mean()
        return values.reindex(values.index.union(grid)).interpolate(method="time", limit_area="inside").reindex(grid)
    return values.resample(timestep, origin=start).mean().reindex(grid)


def _prepare_measurements(
    values: pd.DataFrame,
    start: pd.Timestamp,
    stop: pd.Timestamp,
    frequency: Literal["h", "D"],
    fallback: pd.Series,
    path: Path,
) -> pd.DataFrame:
    """Resample a file's selected columns together, filling gaps with per-node constants."""
    numeric = values.apply(pd.to_numeric, errors="coerce")
    invalid = int((values.notna() & numeric.isna()).sum().sum())
    if invalid:
        logger.warning("%s: treating %s non-numeric measurements as missing", path, invalid)
    if np.isinf(numeric.to_numpy(dtype=float, na_value=np.nan)).any():
        raise ValueError(f"Infinite measurements in {path}")
    result = _resample(numeric, start, stop, frequency)
    if np.isinf(result.to_numpy(dtype=float, na_value=np.nan)).any():
        raise ValueError(f"Infinite resampled measurements in {path}")
    negative = int(result.lt(0).sum().sum())
    if negative:
        logger.warning("%s: clipping %s negative flow rates to zero", path, negative)
    missing = result.isna().sum()
    if missing.gt(0).any():
        logger.warning(
            "%s: using constant_global for missing intervals per node: %s", path, missing.loc[missing.gt(0)].to_dict()
        )
    return result.loc[:, result.notna().any()].clip(lower=0).fillna(fallback)


def _prepare_timeseries(
    table: pd.DataFrame,
    mapping_table: Path,
    frequency: Literal["h", "D"],
    start: pd.Timestamp,
    stop: pd.Timestamp,
) -> pd.DataFrame:
    """Read and resample each source file once, returning only boundaries with measurements."""
    references = table["timeseries"].str.strip().replace("", None)
    directory = mapping_table.parent.resolve()
    paths = {reference: _timeseries_path(reference, directory) for reference in references.dropna().unique()}
    columns = (
        table.get("timeseries_column", table["name"])
        .astype("string")
        .str.strip()
        .replace("", None)
        .fillna(table["name"])
    )
    time_frames = []
    for path, group in table.groupby(references.map(paths), sort=False):
        assert isinstance(path, Path)
        if not path.is_file():
            logger.warning("%s: missing timeseries file %s; using constant_global", group["name"].tolist(), path)
            continue
        frame = _read_timeseries(path)
        selected = columns.loc[group.index].str.strip()
        missing = selected.loc[~selected.isin(frame.columns)]
        if not missing.empty:
            logger.warning("%s: missing measurement columns %s; using constant_global", path, missing.tolist())
        values = frame.reindex(columns=selected)
        values.columns = pd.Index(group["node_id"], name="node_id")
        rates = _prepare_measurements(
            values, start, stop, frequency, group.set_index("node_id")["constant_global"], path
        )
        time_frames.append(rates.rename_axis("time").reset_index().melt(id_vars="time", value_name="flow_rate"))
    return (
        pd.concat(time_frames, ignore_index=True)
        if time_frames
        else pd.DataFrame(columns=["node_id", "time", "flow_rate"])
    )


def _replace_forcing(model: "Model", node_ids: pd.Series, static: pd.DataFrame, time: pd.DataFrame) -> None:
    """Replace forcing for selected nodes, leaving all other boundaries untouched."""
    affected = set(node_ids)
    old_static, old_time = model.flow_boundary.static.df, model.flow_boundary.time.df
    if old_static is not None:
        static = pd.concat([old_static.loc[~old_static["node_id"].isin(affected)], static], ignore_index=True)
    if old_time is not None:
        time = pd.concat([old_time.loc[~old_time["node_id"].isin(affected)], time], ignore_index=True)
    model.flow_boundary.static.df = static if not static.empty else None
    model.flow_boundary.time.df = time if not time.empty else None


def add_transboundary_inflow(
    model: "Model",
    mapping_table: Path,
    *,
    authority: str | int,
    input: Input = "timeseries",
    mapping: Mapping = "name",
    frequency: Literal["h", "D"] = "D",
) -> None:
    """Apply mapped constant or hourly/daily foreign inflows to existing FlowBoundaries.

    The overview CSV supplies authority_aquo, name, timeseries and constant columns.
    Timeseries paths are relative to its directory, omit .csv, and select the
    measurement column matching name (or an optional timeseries_column).
    Missing measurements or seasonal values always fall back to constant_global.
    Node names, coordinates, metadata and links are never modified.
    """
    if mapping == "hydroobject":
        raise NotImplementedError("Mapping by hydroobject_global_id is not implemented")
    if mapping not in ("name", "coordinate"):
        raise ValueError(f"Unsupported mapping method: {mapping}")
    if input not in ("timeseries", "constant_global", "constant_summer", "constant_winter"):
        raise ValueError(f"Unsupported inflow input: {input}")
    if frequency not in ("h", "D"):
        raise ValueError(f"Unsupported resampling frequency: {frequency}")
    table = _read_mapping(mapping_table, authority, input, mapping)
    node_table = model.flow_boundary.node
    nodes = None if node_table is None else node_table.df
    if nodes is None or nodes.empty:
        if not table.empty:
            raise ValueError(f"Mapping contains boundaries for authority {authority}, but the model has none")
        logger.info("No foreign inflows for authority %s", authority)
        return
    if "meta_categorie" not in nodes:
        raise ValueError("FlowBoundary nodes must have the existing meta_categorie classification")
    nodes = nodes.loc[nodes["meta_categorie"].eq("buitenlandse aanvoer")]
    table["node_id"] = _couple_nodes(table, nodes, mapping)
    if table.empty:
        logger.info("No foreign inflows for authority %s", authority)
        return
    start, stop = pd.Timestamp(model.starttime), pd.Timestamp(model.endtime)
    if start >= stop:
        raise ValueError("Model starttime must precede endtime")
    rates = table["constant_global"] if input == "timeseries" else table[input]
    if rates.isna().any():
        logger.warning("Missing %s values for %s boundaries; using constant_global", input, int(rates.isna().sum()))
    static = table[["node_id"]].assign(flow_rate=rates.fillna(table["constant_global"])).reset_index(drop=True)
    time = pd.DataFrame(columns=["node_id", "time", "flow_rate"])
    if input == "timeseries":
        time = _prepare_timeseries(table, mapping_table, frequency, start, stop)
        static = static.loc[~static["node_id"].isin(time["node_id"])]
    _replace_forcing(model, table["node_id"], static, time)
    logger.info("Applied %s foreign inflows for %s using %s", len(table), authority, input)

"""Compare two Ribasim models for the model viewer, to review the changes of a pull request.

Nodes are matched by their key and water authority, see `ribasim_nl.keys`, or else by node_id, and links by
from_node_id and to_node_id, since nodes and links get renumbered.
A feature in both models is changed if its geometry, an attribute, its rows in an input table or its
Basin / area differ. Initial Basin levels within 1 cm are equal, see `TABLE_TOLERANCES`. If both models have results, the Basin levels and flow rates that differ are listed too.
"""

import json
from collections.abc import Collection
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
import xarray as xr

from ribasim_nl.keys import AUTHORITY_COLUMN, KEY_COLUMN
from ribasim_nl.webmap import model_results_dir, read_config, read_features

# Geometries that differ less than this, in model CRS units (m), are equal
GEOMETRY_TOLERANCE = 0.01
TABLE_BATCH_ROWS = 1 << 20
# Copies of node and link IDs that the models had until November 2026, so that a model without them does not change in
# every node. The key of a node identifies it, see `ribasim_nl.keys`: a model that gets keys does not change either.
IGNORED_COLUMNS = {
    "meta_link_id_waterbeheerder",
    "meta_node_id_waterbeheerder",
    "meta_node_id",
    "meta_old_node_id",
    KEY_COLUMN,
}
# Columns of input tables with the node_id of another node than that of the row
NODE_ID_COLUMNS = ["listen_node_id"]
# The result variable compared per result set, with the absolute and relative tolerance below which results are
# equal. The relative tolerance applies to the largest magnitude of a feature, and is chosen above solver noise.
RESULT_VARIABLES = {"basin": ("level", 1e-3, 0.0), "flow": ("flow_rate", 1e-3, 1e-2)}
# Columns of input tables that are equal within an absolute tolerance, per table. The initial Basin levels are the
# final levels of a run, see `ribasim_nl.regional_model`, so they change slightly with every change of the model.
TABLE_TOLERANCES = {"Basin / state": {"level": 0.01}}


def values_equal(base: pd.Series, head: pd.Series) -> np.ndarray:
    """Element-wise equality where missing values are equal to each other."""
    both_missing = base.isna().to_numpy() & head.isna().to_numpy()
    equal = base.eq(head).fillna(False).to_numpy(dtype=bool)
    return equal | both_missing


def geometries_equal(base: np.ndarray, head: np.ndarray) -> np.ndarray:
    """Element-wise geometry equality within `GEOMETRY_TOLERANCE`, regardless of vertex order."""
    return np.asarray(
        shapely.equals_exact(shapely.normalize(base), shapely.normalize(head), tolerance=GEOMETRY_TOLERANCE),
        dtype=bool,
    )


def node_translation(base: pd.DataFrame, head: pd.DataFrame) -> pd.Series:
    """The node_id in the head model of each node of the base model, or its negated node_id if it is only in the base.

    Nodes with the same key and water authority are the same node, whatever their node_id, see `ribasim_nl.keys`.
    A node without a key in one of the models is the same node as the one with its node_id in the other model, like
    in models without keys.
    """

    def keys(nodes: pd.DataFrame) -> pd.DataFrame:
        if KEY_COLUMN not in nodes.columns:
            return pd.DataFrame({"node_id": pd.Series(dtype="int64"), "authority": [], "key": []})
        authority = nodes[AUTHORITY_COLUMN] if AUTHORITY_COLUMN in nodes.columns else pd.Series("", index=nodes.index)
        table = pd.DataFrame(
            {
                "node_id": nodes["node_id"].to_numpy(),
                "authority": authority.astype(object).where(authority.notna(), "").to_numpy(),
                "key": nodes[KEY_COLUMN].to_numpy(),
            }
        )
        table = table[table["key"].notna()]
        assert not table.duplicated(["authority", "key"]).any(), "Nodes of a water authority with the same key"
        return table

    base_ids, head_ids = base["node_id"].to_numpy(), head["node_id"].to_numpy()
    translation = pd.Series(-base_ids, index=pd.Index(base_ids, name="node_id"), name="head_node_id")
    base_keys, head_keys = keys(base), keys(head)
    pairs = base_keys.merge(head_keys, on=["authority", "key"], suffixes=("_base", "_head"))
    translation[pairs["node_id_base"].to_numpy()] = pairs["node_id_head"].to_numpy()
    # a node with a key in both models that is not matched is another node, also if its node_id is the same
    same_id = np.setdiff1d(
        np.intersect1d(base_ids, head_ids),
        np.intersect1d(base_keys["node_id"].to_numpy(), head_keys["node_id"].to_numpy()),
    )
    same_id = np.setdiff1d(np.setdiff1d(same_id, pairs["node_id_base"].to_numpy()), pairs["node_id_head"].to_numpy())
    translation[same_id] = same_id
    return translation


def translate(node_ids: pd.Series, translation: pd.Series) -> np.ndarray:
    """The node_ids of the base model in the head model, see `node_translation`: an unknown node keeps its node_id."""
    translated = node_ids.map(translation)
    return translated.where(translated.notna(), node_ids).to_numpy()


def matched_ids(base: pd.DataFrame, head: pd.DataFrame, id_column: str, key: list[str]) -> pd.Series:
    """The base id of each feature in both models, indexed by head id, matched on the key columns."""
    columns = list(dict.fromkeys([*key, id_column]))
    merged = head[columns].merge(base[columns], on=key, suffixes=("", "_base"), validate="one_to_one")
    base_ids = merged[id_column] if id_column in key else merged[f"{id_column}_base"]
    return pd.Series(base_ids.to_numpy(), index=pd.Index(merged[id_column].to_numpy(), name=id_column))


def compare_features(
    base: gpd.GeoDataFrame, head: gpd.GeoDataFrame, id_column: str, key: list[str] | None = None
) -> tuple[dict[int, dict], dict[int, dict]]:
    """Compare features matched on the key columns, by default the id column.

    Return the added and changed features by head id, and the removed features by base id. Changed features
    list the columns that differ, and have their `base_id` if it differs from their head id.
    """
    key = key or [id_column]
    for df in (base, head):
        assert df[id_column].is_unique, f"Duplicate {id_column}"
        assert not df.duplicated(key).any(), f"Duplicate {key}"
    base = base.set_index(key, drop=False)
    head = head.set_index(key, drop=False)
    diff = {
        int(id): {"status": "added", "changes": []} for id in head.loc[head.index.difference(base.index), id_column]
    }
    removed = {
        int(id): {"status": "removed", "changes": []} for id in base.loc[base.index.difference(head.index), id_column]
    }

    common = head.index.intersection(base.index)
    base, head = base.loc[common], head.loc[common]
    changed: dict[str, np.ndarray] = {"geometry": ~geometries_equal(base.geometry.to_numpy(), head.geometry.to_numpy())}
    missing = pd.Series(None, index=common, dtype=object)
    # Renumbering features that are matched on other columns is no change
    for column in sorted((set(base.columns) | set(head.columns)) - {"geometry", id_column} - IGNORED_COLUMNS):
        changed[column] = ~values_equal(base.get(column, missing), head.get(column, missing))
    head_ids = head[id_column].to_numpy()
    base_ids = base[id_column].to_numpy()
    for column, mask in changed.items():
        for head_id, base_id in zip(head_ids[mask], base_ids[mask], strict=True):
            entry: dict = diff.setdefault(int(head_id), {"status": "changed", "changes": []})
            entry["changes"].append(column)
            if base_id != head_id:
                entry["base_id"] = int(base_id)
    return diff, removed


def normalize_columns(df: pd.DataFrame, columns: list[str], string_columns: Collection[str] = ()) -> pd.DataFrame:
    """Columns in a fixed order with dtypes that hash equally for equal values.

    Missing columns are null, like an empty column: missing values of `string_columns`, NaN otherwise.
    """
    result = {}
    for column in columns:
        if column not in df:
            result[column] = (
                pd.Series(pd.NA, index=df.index, dtype="string")
                if column in string_columns
                else np.full(len(df), np.nan)
            )
        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            result[column] = df[column].astype("datetime64[ms]").astype("int64").where(df[column].notna(), np.nan)
        elif pd.api.types.is_numeric_dtype(df[column]):
            result[column] = df[column].astype("float64")
        else:
            result[column] = df[column].astype("string")
    return pd.DataFrame(result, index=df.index)


def node_digests(
    path: Path, columns: list[str], string_columns: Collection[str] = (), translation: pd.Series | None = None
) -> pd.DataFrame:
    """The row count and an order-sensitive digest of the rows of each node in a table sorted by node_id.

    With a `translation`, see `node_translation`, the node_ids are those of the head model.
    """
    file = pq.ParquetFile(path)
    present = [column for column in columns if column in file.schema_arrow.names]
    ids_out: list[np.ndarray] = []
    counts_out: list[np.ndarray] = []
    digests_out: list[np.ndarray] = []
    # The last node of a batch may continue in the next one
    pending_id, pending_count, pending_digest = None, 0, np.zeros(1, dtype="uint64")
    for batch in file.iter_batches(batch_size=TABLE_BATCH_ROWS, columns=["node_id", *present]):
        df = batch.to_pandas()
        ids = df["node_id"].to_numpy()
        if translation is not None:
            for column in NODE_ID_COLUMNS:
                if column in df.columns:
                    df[column] = translate(df[column], translation)
        assert np.all(ids[1:] >= ids[:-1]), f"{path} is not sorted by node_id"
        assert pending_id is None or ids[0] >= pending_id, f"{path} is not sorted by node_id"
        hashes = pd.util.hash_pandas_object(normalize_columns(df, columns, string_columns), index=False).to_numpy()
        starts = np.flatnonzero(np.r_[True, ids[1:] != ids[:-1]])
        counts = np.diff(np.r_[starts, len(ids)])
        position = (np.arange(len(ids)) - np.repeat(starts, counts)).astype("uint64")
        continues = ids[0] == pending_id
        if continues:
            position[: counts[0]] += np.uint64(pending_count)
        # Unsigned integer overflow wraps around, which is fine for a digest
        digests = np.add.reduceat(hashes * (2 * position + 1), starts)
        group_ids = ids[starts]
        if continues:
            counts[0] += pending_count
            np.add(digests[:1], pending_digest, out=digests[:1])
        elif pending_id is not None:
            ids_out.append(np.array([pending_id]))
            counts_out.append(np.array([pending_count]))
            digests_out.append(pending_digest)
        ids_out.append(group_ids[:-1])
        counts_out.append(counts[:-1])
        digests_out.append(digests[:-1])
        pending_id, pending_count, pending_digest = int(group_ids[-1]), int(counts[-1]), digests[-1:]
    if pending_id is not None:
        ids_out.append(np.array([pending_id]))
        counts_out.append(np.array([pending_count]))
        digests_out.append(pending_digest)
    node_ids = np.concatenate([np.zeros(0, dtype="int64"), *ids_out])
    if translation is not None:
        node_ids = translate(pd.Series(node_ids), translation)
    return pd.DataFrame(
        {
            "rows": np.concatenate([np.zeros(0, dtype="int64"), *counts_out]),
            "digest": np.concatenate([np.zeros(0, dtype="uint64"), *digests_out]),
        },
        index=pd.Index(node_ids, name="node_id"),
    )


def nodes_differ_within(
    base_path: Path | None, head_path: Path | None, tolerances: dict[str, float], translation: pd.Series | None = None
) -> pd.Index:
    """The node_ids whose rows in a table differ, with the columns of `tolerances` equal within their tolerance.

    Rows are matched by node_id and their order per node. The whole table is read, so this is only for small tables.
    With a `translation`, see `node_translation`, the node_ids of the base table are those of the head model.
    """
    tables = [
        pd.read_parquet(path) if path else pd.DataFrame({"node_id": pd.Series(dtype="int64")})
        for path in (base_path, head_path)
    ]
    if translation is not None:
        for column in ["node_id", *NODE_ID_COLUMNS]:
            if column in tables[0].columns:
                tables[0][column] = translate(tables[0][column], translation)
    columns = sorted({c for df in tables for c in df.columns} - {"node_id"})
    assert set(tolerances) <= set(columns), f"Columns {set(tolerances) - set(columns)} are not in the tables"
    base, head = (
        df.reindex(columns=["node_id", *columns]).assign(row=df.groupby("node_id").cumcount()) for df in tables
    )
    joined = base.merge(head, on=["node_id", "row"], how="outer", suffixes=("_base", "_head"), indicator=True)
    equal = (joined["_merge"] == "both").to_numpy(copy=True)
    for column in columns:
        base_values, head_values = joined[f"{column}_base"], joined[f"{column}_head"]
        if column in tolerances:
            both_missing = (base_values.isna() & head_values.isna()).to_numpy()
            difference = (base_values.astype("float64") - head_values.astype("float64")).abs()
            close = difference.lt(tolerances[column]).to_numpy()
            equal &= close | both_missing
        else:
            equal &= values_equal(base_values, head_values)
    return pd.Index(joined.loc[~equal, "node_id"].unique())


def compare_tables(
    base_dir: Path, base_manifest: dict, head_dir: Path, head_manifest: dict, translation: pd.Series | None = None
) -> dict[int, list[str]]:
    """Return the names of the exported input tables whose rows differ, per node_id of the head model.

    With a `translation`, see `node_translation`, the rows of a node are compared with those of the same node in the
    base model, whatever its node_id there. Nodes that are only in the base model have their negated node_id.
    """
    base_tables = {table["name"]: table for table in base_manifest["tables"]}
    head_tables = {table["name"]: table for table in head_manifest["tables"]}
    # equal files have the rows of other nodes if the nodes are renumbered
    renumbered = translation is not None and bool(((translation > 0) & (translation != translation.index)).any())
    changes: dict[int, list[str]] = {}
    for name in sorted(base_tables.keys() | head_tables.keys()):
        base_table, head_table = base_tables.get(name), head_tables.get(name)
        if base_table and head_table and base_table["hash"] == head_table["hash"] and not renumbered:
            continue
        paths = [
            base_dir / base_table["path"] if base_table else None,
            head_dir / head_table["path"] if head_table else None,
        ]
        if name in TABLE_TOLERANCES:
            for node_id in nodes_differ_within(*paths, TABLE_TOLERANCES[name], translation):
                changes.setdefault(int(node_id), []).append(name)
            continue
        schemas = [pq.read_schema(path) for path in paths if path]
        columns = sorted({c for schema in schemas for c in schema.names} - {"node_id"} - IGNORED_COLUMNS)
        # So that a column that is missing in one table hashes like the same column without values in the other
        string_columns = {
            field.name
            for schema in schemas
            for field in schema
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
        }
        empty = pd.DataFrame({"rows": [], "digest": []}, index=pd.Index([], name="node_id"))
        base_digests, head_digests = (
            node_digests(path, columns, string_columns, translation if path is paths[0] else None) if path else empty
            for path in paths
        )
        joined = base_digests.merge(
            head_digests, how="outer", left_index=True, right_index=True, suffixes=("_base", "_head")
        )
        differs = ~(
            (joined["rows_base"] == joined["rows_head"]) & (joined["digest_base"] == joined["digest_head"])
        ).to_numpy()
        for node_id in joined.index[differs]:
            changes.setdefault(int(node_id), []).append(name)
    return changes


def read_basin_areas(database: Path) -> gpd.GeoSeries:
    """Basin / area geometries by node_id, with the areas of a node merged."""
    area = gpd.read_file(database, layer="Basin / area", columns=["node_id"], use_arrow=True)
    assert area.node_id.notna().all(), f"Basin / area has missing node_id in {database}"
    duplicated = area.node_id.duplicated(keep=False)
    merged = area[duplicated].dissolve("node_id").geometry
    return gpd.GeoSeries(pd.concat([area[~duplicated].set_index("node_id").geometry, merged]).sort_index())


def compare_basin_areas(base: gpd.GeoSeries, head: gpd.GeoSeries) -> list[int]:
    """The node_ids whose Basin / area is added, removed or differs."""
    common = head.index.intersection(base.index)
    differs = ~geometries_equal(base.loc[common].to_numpy(), head.loc[common].to_numpy())
    return sorted(int(id) for id in common[differs].union(head.index.symmetric_difference(base.index)))


def results_match_links(toml_path: Path, config: dict, links: pd.DataFrame) -> bool:
    """Whether every link in the flow results connects the same nodes in the model, so the results are not outdated."""
    with xr.open_dataset(model_results_dir(toml_path, config) / "flow.nc") as ds:
        results = pd.DataFrame(
            {
                "link_id": ds["link_id"].to_numpy().astype("int64"),
                "from_node_id": ds["from_node_id"].to_numpy(),
                "to_node_id": ds["to_node_id"].to_numpy(),
            }
        )
    columns = ["link_id", "from_node_id", "to_node_id"]
    merged = results.merge(links[columns], on="link_id", how="left", suffixes=("", "_model"), validate="one_to_one")
    return bool(
        (
            (merged["from_node_id"] == merged["from_node_id_model"])
            & (merged["to_node_id"] == merged["to_node_id_model"])
        ).all()
    )


def read_result(export_dir: Path, manifest: dict, kind: str, variable: str) -> pd.DataFrame:
    """One result variable of an export, with a row per time step and a column per feature id."""
    entry = manifest["results"][kind]
    df = pd.read_parquet(export_dir / entry["by_id"]["path"], columns=[entry["id"], "time", variable])
    return df.pivot(index="time", columns=entry["id"], values=variable)


def compare_results(base: pd.DataFrame, head: pd.DataFrame, base_ids: pd.Series, atol: float, rtol: float) -> dict:
    """Compare a result variable at the time steps of both models, for the features matched by `base_ids`.

    Returns the number of compared features, and the difference head - base with the largest magnitude per
    head id, for features where it exceeds `atol` plus `rtol` times the largest magnitude of the feature.
    A value missing in one model only is a difference of unknown size, reported as None.
    """
    times = base.index.intersection(head.index)
    assert len(times) > 0, "The results have no time steps in common"
    base_ids = base_ids[base_ids.index.isin(head.columns) & base_ids.isin(base.columns)]
    head_values = head.loc[times, base_ids.index].to_numpy(dtype="float64")
    base_values = base.loc[times, base_ids.to_numpy()].to_numpy(dtype="float64")
    delta = head_values - base_values
    delta[np.isnan(head_values) & np.isnan(base_values)] = 0.0
    delta[np.isnan(delta)] = np.inf
    scale = np.fmax(
        np.nanmax(np.abs(head_values), axis=0, initial=0.0), np.nanmax(np.abs(base_values), axis=0, initial=0.0)
    )
    largest = delta[np.abs(delta).argmax(axis=0), np.arange(delta.shape[1])]
    differs = np.abs(largest) > atol + rtol * scale
    return {
        "compared": len(base_ids),
        "differences": {
            int(id): float(value) if np.isfinite(value) else None
            for id, value in zip(base_ids.index[differs], largest[differs], strict=True)
        },
    }


def flatten(config: dict, prefix: str = "") -> dict[str, str]:
    """Flatten nested TOML tables to dotted keys with string values."""
    flat = {}
    for key, value in config.items():
        if isinstance(value, dict):
            flat |= flatten(value, f"{prefix}{key}.")
        else:
            flat[f"{prefix}{key}"] = str(value)
    return flat


def compare_config(base: dict, head: dict) -> list[dict]:
    """The TOML settings that differ, as dicts with key, base and head value; None if not set."""
    base_flat, head_flat = flatten(base), flatten(head)
    return [
        {"key": key, "base": base_flat.get(key), "head": head_flat.get(key)}
        for key in sorted(base_flat.keys() | head_flat.keys())
        if base_flat.get(key) != head_flat.get(key)
    ]


def diff_models(
    base_toml: Path,
    head_toml: Path,
    base_dir: Path,
    head_dir: Path,
    output_path: Path,
    labels: tuple[str, str] | None = None,
) -> dict:
    """Compare two models and their `export_webmap` output, and write the differences as JSON.

    Nodes and links map ids to `{"status": "added" | "removed" | "changed", "changes": [...]}`, where the
    changes name the attribute columns, the tables, "geometry" or "Basin / area" that differ.
    Changed nodes and links have a `base_id` if they were renumbered. Removed nodes and links are keyed by their
    negated id in the base model, since that id may be used by another node or link in the head model.
    `base_node_ids` has the id in the base model of every renumbered node in both models, changed or not, by its id
    in the head model, to find its results and tables in the base model.
    If both models have results, `results` has the Basin levels and flow rates that differ per head id,
    see `compare_results`, otherwise it is None. Results whose links are not those of their model are outdated,
    and are not compared; `results_note` then says why. The labels name the models, by default their model names.
    """
    base_config, _, base_database = read_config(base_toml)
    head_config, _, head_database = read_config(head_toml)
    base_manifest = json.loads((base_dir / "manifest.json").read_text())
    head_manifest = json.loads((head_dir / "manifest.json").read_text())

    base_nodes = read_features(base_database, "Node", "node_id")
    head_nodes = read_features(head_database, "Node", "node_id")
    # Node ids are not stable either: the same node can have another id in the base model
    translation = node_translation(base_nodes, head_nodes)
    node_key = ["head_node_id"]
    nodes, removed_nodes = compare_features(
        base_nodes.assign(head_node_id=translate(base_nodes["node_id"], translation)),
        head_nodes.assign(head_node_id=head_nodes["node_id"]),
        "node_id",
        key=node_key,
    )
    # The ids of removed nodes may be in use in the head model
    nodes |= {-id: diff for id, diff in removed_nodes.items()}
    # Link ids are not stable, links are renumbered when the network is rebuilt
    base_links = read_features(base_database, "Link", "link_id")
    head_links = read_features(head_database, "Link", "link_id")
    link_key = ["from_node_id", "to_node_id"]
    # the links of the base model between the nodes of the head model
    translated_links = base_links.assign(**{c: translate(base_links[c], translation) for c in link_key})
    links, removed_links = compare_features(translated_links, head_links, "link_id", key=link_key)
    # The ids of removed links may be in use in the head model
    links |= {-id: diff for id, diff in removed_links.items()}
    node_changes = compare_tables(base_dir, base_manifest, head_dir, head_manifest, translation)
    base_areas = read_basin_areas(base_database)
    base_areas.index = pd.Index(translate(base_areas.index.to_series(), translation), name=base_areas.index.name)
    for node_id in compare_basin_areas(base_areas, read_basin_areas(head_database)):
        node_changes.setdefault(node_id, []).append("Basin / area")
    # Tables of added and removed nodes, or of nodes in neither Node table, add no information
    matched = translation[translation > 0]
    # The node_id in the base model of the nodes in both models, by their node_id in the head model
    base_node_ids = pd.Series(matched.index.to_numpy(), index=pd.Index(matched.to_numpy(), name="node_id"))
    renumbered = base_node_ids[base_node_ids.to_numpy() != base_node_ids.index.to_numpy()]
    for node_id, changes in node_changes.items():
        if node_id in base_node_ids.index:
            entry = nodes.setdefault(node_id, {"status": "changed", "changes": []})
            entry["changes"].extend(changes)
            if base_node_ids[node_id] != node_id:
                entry["base_id"] = int(base_node_ids[node_id])

    results, results_note = None, None
    outdated = [
        name
        for name, toml, config, manifest, model_links in [
            ("base", base_toml, base_config, base_manifest, base_links),
            ("head", head_toml, head_config, head_manifest, head_links),
        ]
        if "results" in manifest and not results_match_links(toml, config, model_links)
    ]
    if outdated:
        models = " and ".join(outdated) + (" models" if len(outdated) > 1 else " model")
        results_note = f"The results of the {models} are outdated: their links differ from the model"
        print(results_note)
    elif "results" in base_manifest and "results" in head_manifest:
        base_ids = {
            "basin": base_node_ids,
            "flow": matched_ids(translated_links, head_links, "link_id", link_key),
        }
        results = {}
        for kind, (variable, atol, rtol) in RESULT_VARIABLES.items():
            base_result = read_result(base_dir, base_manifest, kind, variable)
            head_result = read_result(head_dir, head_manifest, kind, variable)
            units = head_manifest["results"][kind]["variables"][variable]["units"]
            results[kind] = {"variable": variable, "units": units, "atol": atol, "rtol": rtol} | compare_results(
                base_result, head_result, base_ids[kind], atol, rtol
            )

    base_label, head_label = labels or (base_manifest["model"], head_manifest["model"])
    diff = {
        "base": {"model": base_manifest["model"], "toml": base_toml.as_posix(), "label": base_label},
        "head": {"model": head_manifest["model"], "toml": head_toml.as_posix(), "label": head_label},
        "config": compare_config(base_config, head_config),
        "nodes": {str(id): nodes[id] for id in sorted(nodes)},
        "links": {str(id): links[id] for id in sorted(links)},
        "base_node_ids": {str(id): int(renumbered[id]) for id in sorted(renumbered.index)},
        "results": results,
        "results_note": results_note,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(diff, indent=1))
    return diff

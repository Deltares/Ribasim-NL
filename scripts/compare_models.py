"""Compare two model directories (or other DVC outputs) table by table.

Used to verify that code changes do not change pipeline outputs.

Usage:
    pixi run python scripts/compare_models.py <reference> <candidate> [--include-results] [--rtol 0]

Both arguments may be directories or single files. GeoPackages are compared per table
through SQLite (geometry blobs byte for byte), Arrow and NetCDF files per column or variable,
TOML files after parsing, and other files by content hash. Directories named `results` are
skipped unless `--include-results` is given, since they hold simulation output.
"""

import argparse
import hashlib
import sqlite3
import sys
import tomllib
from pathlib import Path

import numpy as np
import pandas as pd
import shapely

SKIP_TABLE_PREFIXES = ("gpkg_", "rtree_", "sqlite_")
SKIP_SUFFIXES = {".log", ".png", ".html"}


def _frames_differ(name: str, ref: pd.DataFrame, cand: pd.DataFrame, rtol: float) -> list[str]:
    """Return human readable differences between two DataFrames, empty if equal."""
    if list(ref.columns) != list(cand.columns):
        missing = [c for c in ref.columns if c not in cand.columns]
        added = [c for c in cand.columns if c not in ref.columns]
        if missing or added:
            return [f"{name}: columns differ, missing={missing}, added={added}"]
        return [f"{name}: column order differs"]
    if len(ref) != len(cand):
        return [f"{name}: row count {len(ref)} != {len(cand)}"]

    diffs = []
    for col in ref.columns:
        a = ref[col].reset_index(drop=True)
        b = cand[col].reset_index(drop=True)
        if a.dtype != b.dtype:
            diffs.append(f"{name}.{col}: dtype {a.dtype} != {b.dtype}")
        a_na, b_na = a.isna().to_numpy(), b.isna().to_numpy()
        if not np.array_equal(a_na, b_na):
            diffs.append(f"{name}.{col}: {(a_na != b_na).sum()} rows differ in missingness")
            continue
        a, b = a[~a_na], b[~b_na]
        if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b) and not pd.api.types.is_bool_dtype(a):
            av, bv = a.to_numpy(dtype=float), b.to_numpy(dtype=float)
            ne = ~np.isclose(av, bv, rtol=rtol, atol=0.0) if rtol > 0 else av != bv
            if ne.any():
                maxdiff = np.abs(av - bv)[ne].max()
                diffs.append(f"{name}.{col}: {ne.sum()} values differ (max abs diff {maxdiff:.6g})")
        else:
            ne = a.to_numpy() != b.to_numpy()
            if np.asarray(ne).any():
                idx = int(np.flatnonzero(ne)[0])
                if isinstance(a.iloc[idx], bytes) and a.iloc[idx].startswith(b"GP"):
                    distance = shapely.hausdorff_distance(
                        _gpkg_geometries(a[ne].to_numpy()), _gpkg_geometries(b[ne].to_numpy())
                    )
                    diffs.append(
                        f"{name}.{col}: {int(np.sum(ne))} geometries differ "
                        f"(max Hausdorff distance {np.nanmax(distance):.3g})"
                    )
                else:
                    diffs.append(
                        f"{name}.{col}: {int(np.sum(ne))} values differ (first: {a.iloc[idx]!r} != {b.iloc[idx]!r})"
                    )
    return diffs


def _gpkg_geometries(blobs: np.ndarray) -> np.ndarray:
    """Convert GeoPackage geometry blobs to shapely geometries."""
    # header: magic (2), version (1), flags (1), srs_id (4), envelope (0, 32, 48 or 64 bytes)
    envelope_sizes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}
    wkbs = [blob[8 + envelope_sizes[(blob[3] >> 1) & 0b111] :] for blob in blobs]
    return shapely.from_wkb(wkbs)


def compare_gpkg(ref: Path, cand: Path, rtol: float) -> list[str]:
    """Compare all user tables of two GeoPackages."""

    def tables(path: Path) -> dict[str, pd.DataFrame]:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as con:
            names = [
                r[0]
                for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
                if not r[0].startswith(SKIP_TABLE_PREFIXES)
            ]
            dfs = {}
            # table and column names come from the database itself
            for n in sorted(names):
                df = pd.read_sql(f'SELECT * FROM "{n}"', con)  # noqa: S608
                # also compare the SQLite storage class of each value (INTEGER, REAL, TEXT, NULL)
                if len(df.columns) > 0:
                    typeofs = ", ".join(f'typeof("{c}") AS "typeof:{c}"' for c in df.columns)
                    df = df.join(pd.read_sql(f'SELECT {typeofs} FROM "{n}"', con))  # noqa: S608
                dfs[n] = df
            return dfs

    a, b = tables(ref), tables(cand)
    diffs = [f"{ref.name}: table {n} missing in candidate" for n in a.keys() - b.keys()]
    diffs += [f"{ref.name}: table {n} added in candidate" for n in b.keys() - a.keys()]
    for n in sorted(a.keys() & b.keys()):
        diffs += _frames_differ(f"{ref.name}:{n}", a[n], b[n], rtol)
    return diffs


def compare_arrow(ref: Path, cand: Path, rtol: float) -> list[str]:
    """Compare two Arrow IPC files."""
    return _frames_differ(ref.name, pd.read_feather(ref), pd.read_feather(cand), rtol)


def compare_netcdf(ref: Path, cand: Path, rtol: float) -> list[str]:
    """Compare two NetCDF files variable by variable."""
    import xarray as xr

    with xr.open_dataset(ref) as a, xr.open_dataset(cand) as b:
        if set(a.variables) != set(b.variables):
            return [f"{ref.name}: variables differ {set(a.variables) ^ set(b.variables)}"]
        diffs = []
        for var in a.variables:
            av, bv = a[var].values, b[var].values
            if av.shape != bv.shape:
                diffs.append(f"{ref.name}:{var}: shape {av.shape} != {bv.shape}")
            elif np.issubdtype(av.dtype, np.floating):
                if not np.allclose(av, bv, rtol=rtol, atol=0.0, equal_nan=True):
                    diffs.append(f"{ref.name}:{var}: values differ")
            elif not np.array_equal(av, bv):
                diffs.append(f"{ref.name}:{var}: values differ")
        return diffs


def compare_toml(ref: Path, cand: Path) -> list[str]:
    """Compare two TOML files after parsing."""
    a = tomllib.loads(ref.read_text())
    b = tomllib.loads(cand.read_text())
    return [] if a == b else [f"{ref.name}: TOML content differs"]


def compare_file(ref: Path, cand: Path, rtol: float) -> list[str]:
    """Compare two files based on their type."""
    match ref.suffix.lower():
        case ".gpkg":
            return compare_gpkg(ref, cand, rtol)
        case ".arrow" | ".feather":
            return compare_arrow(ref, cand, rtol)
        case ".nc":
            return compare_netcdf(ref, cand, rtol)
        case ".toml":
            return compare_toml(ref, cand)
        case _:
            same = hashlib.sha256(ref.read_bytes()).digest() == hashlib.sha256(cand.read_bytes()).digest()
            return [] if same else [f"{ref.name}: content differs"]


def compare(ref: Path, cand: Path, include_results: bool = False, rtol: float = 0.0) -> list[str]:
    """Compare a reference and candidate path, returning a list of differences."""
    if ref.is_file():
        return compare_file(ref, cand, rtol)

    def files(root: Path) -> set[Path]:
        return {
            p.relative_to(root)
            for p in root.rglob("*")
            if p.is_file()
            and p.suffix.lower() not in SKIP_SUFFIXES
            and (include_results or "results" not in p.relative_to(root).parts)
        }

    a, b = files(ref), files(cand)
    diffs = [f"{p}: missing in candidate" for p in sorted(a - b)]
    diffs += [f"{p}: added in candidate" for p in sorted(b - a)]
    for rel in sorted(a & b):
        diffs += [f"{rel.parent / d}" if rel.parent != Path() else d for d in compare_file(ref / rel, cand / rel, rtol)]
    return diffs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("reference", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--include-results", action="store_true", help="also compare `results` directories")
    parser.add_argument("--rtol", type=float, default=0.0, help="relative tolerance for floats, default exact")
    args = parser.parse_args()

    for path in (args.reference, args.candidate):
        if not path.exists():
            sys.exit(f"{path} does not exist")

    diffs = compare(args.reference, args.candidate, args.include_results, args.rtol)
    for diff in diffs:
        print(diff)
    print(f"{len(diffs)} differences between {args.reference} and {args.candidate}")
    sys.exit(1 if diffs else 0)


if __name__ == "__main__":
    main()

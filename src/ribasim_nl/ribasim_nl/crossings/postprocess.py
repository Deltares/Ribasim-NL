"""Shared steps of the postprocessing of the delivered data of the level controlled water authorities.

The scripts `pipeline/<authority>/00_postprocess.py` port the preprocessing and postprocessing of `peilbeheerst_model`
by HKV, which turned the data delivered by each water authority into its postprocessed data, the input of
`pipeline/00c_parse_crossings.py`. HKV corrected some of the delivered data by hand before that; `apply_edits` applies
these corrections, which `derive_edits` derived from the postprocessed data of 2024. See `docs/dev/reproducing-models.qmd`.
"""

import difflib
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

EDIT_OF = "edit_of"
OCCURRENCE = "occurrence"


def occurrences(keys: pd.Series) -> np.ndarray:
    """The number of earlier rows with the same key, for each row."""
    return keys.groupby(keys, sort=False, dropna=False).cumcount().to_numpy()


def apply_edits(df: gpd.GeoDataFrame, edits: gpd.GeoDataFrame, key: str) -> gpd.GeoDataFrame:
    """Replace rows of `df` by the rows of `edits` that edit them, keeping the order of the rows.

    An edit names the row it replaces by its key, in the column `edit_of`, and its occurrence, the number of earlier rows
    of `df` with that key. A row can be replaced by several rows, which take its place in their order, or removed by an
    edit without geometry. Columns that the edits lack are taken from the row they replace.

    :param df: the delivered data
    :param edits: the corrections, with the columns `edit_of` and `occurrence`, and the columns to replace
    :param key: the column that identifies the rows, such as their code
    :return: the corrected data, with a new index
    """
    assert key in df.columns, f"no column {key}"
    assert {EDIT_OF, OCCURRENCE} <= set(edits.columns), f"the edits need the columns {EDIT_OF} and {OCCURRENCE}"
    df = df.reset_index(drop=True)
    occurrence = occurrences(df[key])
    rows_of = dict(list(edits.groupby([EDIT_OF, OCCURRENCE], sort=False)))
    found = set(zip(df[key], occurrence, strict=True))
    missing = [k for k in rows_of if k not in found]
    assert not missing, f"edits of rows that are not in the data: {missing}"

    parts = []
    for i, k in enumerate(zip(df[key], occurrence, strict=True)):
        if k not in rows_of:
            parts.append(df.iloc[[i]])
            continue
        replacement = rows_of[k].drop(columns=[EDIT_OF, OCCURRENCE])
        replacement = replacement[replacement.geometry.notna()]
        if len(replacement) == 0:
            continue
        rows = df.iloc[[i] * len(replacement)].reset_index(drop=True)
        for column in replacement.columns:
            rows[column] = replacement[column].to_numpy()
        parts.append(rows)
    out = pd.concat(parts, ignore_index=True)
    return gpd.GeoDataFrame(out, geometry=df.geometry.name, crs=df.crs)


def derive_edits(
    original: gpd.GeoDataFrame, target: gpd.GeoDataFrame, key: str, columns: list[str]
) -> gpd.GeoDataFrame:
    """The edits that turn `original` into `target`, for `apply_edits`.

    The rows are aligned by their key. A row of `original` is edited if it differs from its row in `target` in the key,
    the given columns or the geometry, if `target` lacks it, or if rows of `target` that `original` lacks follow or
    precede it. Such rows go with a neighbouring row with the same key, if there is one.

    :param original: the delivered data
    :param target: the corrected data, with the columns `key` and `columns`
    :param key: the column that identifies the rows
    :param columns: the columns to compare and keep, besides the key and geometry
    :return: the edits
    """
    original = original.reset_index(drop=True)
    target = target.reset_index(drop=True)
    keys_a, keys_b = original[key].tolist(), target[key].tolist()
    opcodes = difflib.SequenceMatcher(a=keys_a, b=keys_b, autojunk=False).get_opcodes()
    match: dict[int, int] = {}
    inserted: list[tuple[int, list[int]]] = []  # (the original row before which they come, the target rows)
    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "equal":
            match.update(zip(range(i1, i2), range(j1, j2), strict=True))
        elif j2 > j1:
            inserted.append((i2, list(range(j1, j2))))
    before: dict[int, list[int]] = {}
    after: dict[int, list[int]] = {}
    for position, js in inserted:
        following = position if position < len(original) else None
        preceding = position - 1 if position > 0 else None
        # the first rows with the key of the preceding row go with it, the others with the following row if they all
        # have its key, else with the preceding row, which keeps their order
        n = 0
        if preceding is not None:
            while n < len(js) and keys_b[js[n]] == keys_a[preceding]:
                n += 1
            if n > 0:
                after[preceding] = after.get(preceding, []) + js[:n]
        rest = js[n:]
        if len(rest) == 0:
            continue
        if following is not None and all(keys_b[j] == keys_a[following] for j in rest):
            before[following] = before.get(following, []) + rest
        elif preceding is not None:
            after[preceding] = after.get(preceding, []) + rest
        else:
            assert following is not None, "the target has rows, but the original none"
            before[following] = before.get(following, []) + rest

    def same(i: int, j: int) -> bool:
        a, b = original.iloc[i], target.iloc[j]
        if not a.geometry.equals_exact(b.geometry, 0.0):
            return False
        return all((pd.isna(a[c]) and pd.isna(b[c])) or a[c] == b[c] for c in columns)

    occurrence = occurrences(original[key])
    rows = []
    for i in range(len(original)):
        js = before.get(i, []) + ([match[i]] if i in match else []) + after.get(i, [])
        if len(js) == 1 and i in match and same(i, js[0]):
            continue
        edit = {EDIT_OF: keys_a[i], OCCURRENCE: occurrence[i]}
        if len(js) == 0:
            rows.append({**edit, key: keys_a[i], **dict.fromkeys(columns), "geometry": None})
        rows.extend(
            {**edit, key: keys_b[j], **target.loc[j, columns].to_dict(), "geometry": target.geometry.iat[j]} for j in js
        )
    return gpd.GeoDataFrame(
        rows, columns=[EDIT_OF, OCCURRENCE, key, *columns, "geometry"], geometry="geometry", crs=target.crs
    )


def main_water_system(boundaries: gpd.GeoDataFrame, hws: gpd.GeoDataFrame, authority: str) -> gpd.GeoDataFrame:
    """The parts of the main water system (HWS) within a water authority, as in the postprocessing of 2024.

    :param boundaries: the water authorities, `Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg`
    :param hws: the water bodies of the main water system, `Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg`
    :param authority: the name of the water authority, such as "Waterschap Zuiderzeeland"
    """
    boundary = boundaries.loc[boundaries["naam"].str.strip() == authority]
    assert len(boundary) == 1, f"no single boundary named {authority}"
    assert boundary.crs is not None, "the boundaries have no CRS"
    return gpd.overlay(boundary, hws.to_crs(boundary.crs), how="intersection")


def with_main_water_system_levels(
    streefpeil: gpd.GeoDataFrame, hws: gpd.GeoDataFrame, prefix: str = "dummy_globalid_nhws_"
) -> gpd.GeoDataFrame:
    """The streefpeilen with a missing level for each part of the main water system before them, as in 2024.

    The postprocessing of 2024 added the main water system to the peilgebieden with these levels, and removed it again.
    """
    levels = pd.DataFrame(
        {"waterhoogte": np.nan, "globalid": prefix + hws.index.astype(str), "geometry": None}, index=hws.index
    )
    return gpd.GeoDataFrame(pd.concat([levels, streefpeil]), geometry="geometry", crs=streefpeil.crs)


def write_layers(layers: dict[str, gpd.GeoDataFrame], path: Path) -> None:
    """Write the layers to a new GeoPackage, as in 2024."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    for name, df in layers.items():
        assert isinstance(df, gpd.GeoDataFrame), f"{name} is not a GeoDataFrame"
        df.to_file(path, layer=name)

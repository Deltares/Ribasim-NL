"""Filter monitoring observations using per-location quality labels."""

from pathlib import Path

import pandas as pd

LOCATION_COLUMN = "KRW_monitoringslocatie"


def filter_observations_by_quality(
    observations: pd.DataFrame,
    quality_labels_path: Path,
    included_quality_classes: dict[str, set[str]],
) -> pd.DataFrame:
    """Return observations for parameters and locations with allowed quality classes."""
    if not quality_labels_path.is_file():
        raise FileNotFoundError(f"Quality-label CSV not found: {quality_labels_path}")
    if LOCATION_COLUMN not in observations.columns or "parameter" not in observations.columns:
        raise ValueError(f"Observations must contain {LOCATION_COLUMN!r} and 'parameter' columns.")

    quality_labels = pd.read_csv(quality_labels_path, sep=";", dtype={LOCATION_COLUMN: str})
    required_columns = {LOCATION_COLUMN} | {f"kwaliteitsklasse_{parameter}" for parameter in included_quality_classes}
    missing_columns = required_columns.difference(quality_labels.columns)
    if missing_columns:
        raise ValueError(f"Quality-label CSV is missing required columns: {sorted(missing_columns)}")
    if quality_labels[LOCATION_COLUMN].duplicated().any():
        raise ValueError("Quality-label CSV must contain at most one row per monitoring location.")

    keep = ~observations["parameter"].isin(included_quality_classes)
    observation_locations = observations[LOCATION_COLUMN].astype(str)
    for parameter, classes in included_quality_classes.items():
        if not classes:
            continue
        label_column = f"kwaliteitsklasse_{parameter}"
        allowed_locations = quality_labels.loc[
            quality_labels[label_column].astype("string").str.strip().str.upper().isin(classes),
            LOCATION_COLUMN,
        ]
        keep |= (observations["parameter"] == parameter) & observation_locations.isin(allowed_locations)
    return observations.loc[keep].copy()

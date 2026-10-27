"""Compute flows using DM/LSM relations provided by Geert Prinsen."""

from pathlib import Path

import numpy as np
import pandas as pd
from ribasim_nl.transboundary_inflow import _read_timeseries


def _hourly(frame: pd.DataFrame) -> pd.DataFrame:
    """Average measurements into hourly bins."""
    numeric = frame.apply(pd.to_numeric).astype(float)
    return numeric.resample("h").mean().clip(lower=0).round(6).rename_axis("Datum")


def compute_bovenmark(blauwe_kamer: pd.Series, wielhoef: pd.Series) -> pd.DataFrame:
    """Calculate Bovenmark inflow as used in DM: Wielhoef + 0.92 * Blauwe Kamer (m3/s)."""
    bovenmark = (wielhoef + 0.92 * blauwe_kamer).round(6)
    assert bovenmark.notna().any(), "No overlapping usable measurements"
    assert not np.isinf(bovenmark.to_numpy()).any(), "Infinite calculated BovenMark discharges"
    return bovenmark.to_frame("BovenMark")


def add_to_csv(df: pd.DataFrame, column_name: str, csv_path: Path) -> pd.DataFrame:
    """Return the existing CSV with one column updated, aligning values by timestamp."""
    updated = _read_timeseries(csv_path)
    updated[column_name] = df[column_name]
    return updated


if __name__ == "__main__":
    directory = Path("data") / "Basisgegevens" / "BuitenlandseAanvoer" / "aangeleverd"
    jcaratrace = directory / "JCARATRACE_uur.csv"
    wielhoef_path = directory / "BrabantseDelta_uur_Wielhoef.csv"
    measurements = _hourly(_read_timeseries(jcaratrace)[["BlauweKamer", "Wielhoef"]])
    original_wielhoef = _hourly(_read_timeseries(wielhoef_path)[["Aa of Weerijs Belgie"]])["Aa of Weerijs Belgie"]
    bovenmark = compute_bovenmark(
        measurements["BlauweKamer"], measurements["Wielhoef"].combine_first(original_wielhoef)
    )
    updated = add_to_csv(bovenmark, "BovenMark", jcaratrace)
    updated.to_csv(jcaratrace, index_label="Datum")

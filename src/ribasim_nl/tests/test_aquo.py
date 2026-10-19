"""Tests for explicit authority name and Aquo code lookups."""

import pandas as pd
import pytest
from ribasim_nl.aquo import get_id, get_ids, get_name, get_names, waterbeheercode


@pytest.mark.parametrize("name, code", [("BrabantseDelta", 25), ("Rijkswaterstaat", 80), ("RijnenIJssel", 7)])
def test_authority_lookups(name: str, code: int) -> None:
    assert get_id(name=name) == code
    assert get_name(id=code) == name
    assert waterbeheercode[name] == code


def test_unknown_authority_name() -> None:
    with pytest.raises(ValueError, match="authority name"):
        get_id(name="25")
    with pytest.raises(ValueError, match="authority name"):
        get_id(name="Unknown")


@pytest.mark.parametrize("code", [999, "25", 25.0, True], ids=["unknown", "string", "float", "bool"])
def test_invalid_authority_code(code: object) -> None:
    with pytest.raises(ValueError, match="authority code"):
        get_name(id=code)


def test_series_lookups() -> None:
    names = pd.Series(["BrabantseDelta", None, "Rijkswaterstaat"], index=[3, 1, 8], name="authority")
    expected = pd.Series([25, None, 80], index=names.index, name=names.name, dtype="Int64")
    pd.testing.assert_series_equal(get_ids(names), expected)
    pd.testing.assert_series_equal(get_names(expected), names.astype("string"))
    pd.testing.assert_series_equal(get_names(expected.astype(float)), names.astype("string"))


def test_dataframe_lookups() -> None:
    names = pd.DataFrame({"from": ["BrabantseDelta", None], "to": ["Rijkswaterstaat", "RijnenIJssel"]}, index=[5, 2])
    expected = pd.DataFrame({"from": [25, None], "to": [80, 7]}, index=names.index, dtype="Int64")
    pd.testing.assert_frame_equal(get_ids(names), expected)
    pd.testing.assert_frame_equal(get_names(expected), names.astype("string"))


def test_unknown_vector_values_raise() -> None:
    with pytest.raises(ValueError, match="authority name"):
        get_ids(pd.Series(["BrabantseDelta", "Unknown"]))
    with pytest.raises(ValueError, match="authority code"):
        get_names(pd.Series([25, 999]))

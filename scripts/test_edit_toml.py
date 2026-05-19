"""Tests for scripts/edit_toml.py"""

import sys
from datetime import date, datetime
from pathlib import Path

import tomli

sys.path.insert(0, str(Path(__file__).parent))
from edit_toml import main, parse_value


def test_parse_value_bool():
    assert parse_value("true") is True
    assert parse_value("false") is False
    assert parse_value("True") is True
    assert parse_value("FALSE") is False


def test_parse_value_int():
    assert parse_value("42") == 42
    assert parse_value("-1") == -1


def test_parse_value_float():
    assert parse_value("3.14") == 3.14
    assert parse_value("1e-6") == 1e-6


def test_parse_value_datetime():
    assert parse_value("2020-01-01 00:00:00") == datetime(2020, 1, 1)
    assert parse_value('"2020-01-01 00:00:00"') == datetime(2020, 1, 1)
    assert parse_value("2024-01-01") == date(2024, 1, 1)


def test_parse_value_string():
    assert parse_value("hello") == "hello"
    assert parse_value('"quoted"') == "quoted"
    assert parse_value("'single'") == "single"


def test_edit_toml_basic(tmp_path, monkeypatch):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text("starttime = 2017-01-01\nendtime = 2020-01-01\n")

    monkeypatch.setattr("sys.argv", ["edit_toml.py", str(toml_file), "endtime=2024-01-01"])
    main()

    with toml_file.open("rb") as f:
        cfg = tomli.load(f)
    assert cfg["endtime"] == date(2024, 1, 1)
    assert cfg["starttime"] == date(2017, 1, 1)


def test_edit_toml_nested(tmp_path, monkeypatch):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text("[solver]\nabstol = 0.0001\n")

    monkeypatch.setattr("sys.argv", ["edit_toml.py", str(toml_file), "solver.abstol=1e-6"])
    main()

    with toml_file.open("rb") as f:
        cfg = tomli.load(f)
    assert cfg["solver"]["abstol"] == 1e-6


def test_edit_toml_create_nested(tmp_path, monkeypatch):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text('starttime = "2017-01-01"\n')

    monkeypatch.setattr("sys.argv", ["edit_toml.py", str(toml_file), "solver.algorithm=QNDF"])
    main()

    with toml_file.open("rb") as f:
        cfg = tomli.load(f)
    assert cfg["solver"]["algorithm"] == "QNDF"


def test_edit_toml_multiple_overrides(tmp_path, monkeypatch):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text("[solver]\nabstol = 0.0001\nspecialize = true\n")

    monkeypatch.setattr("sys.argv", ["edit_toml.py", str(toml_file), "solver.abstol=1e-5", "solver.specialize=false"])
    main()

    with toml_file.open("rb") as f:
        cfg = tomli.load(f)
    assert cfg["solver"]["abstol"] == 1e-5
    assert cfg["solver"]["specialize"] is False


def test_edit_toml_missing_args(monkeypatch):
    monkeypatch.setattr("sys.argv", ["edit_toml.py"])
    try:
        main()
    except SystemExit as e:
        assert e.code == 1


def test_edit_toml_invalid_override(tmp_path, monkeypatch):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text("x = 1\n")

    monkeypatch.setattr("sys.argv", ["edit_toml.py", str(toml_file), "no_equals_sign"])
    try:
        main()
    except SystemExit as e:
        assert e.code == 1

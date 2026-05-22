"""Edit a TOML file in-place with key=value overrides.

Usage:
    pixi run edit-toml <toml_path> key1=value1 [key2=value2 ...]

Nested keys use dot notation:
    pixi run edit-toml model.toml endtime="2024-01-01 00:00:00" solver.abstol=1e-6
"""

import sys
from datetime import date, datetime
from pathlib import Path

import tomli
import tomli_w


def parse_value(value: str):
    """Parse a string value into the appropriate Python type."""
    # Strip surrounding quotes early
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1]
    # Boolean
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    # Numeric
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    # Datetime / date (try date first since fromisoformat on date strings also works as datetime)
    try:
        return date.fromisoformat(value)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass
    return value


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    toml_path = Path(sys.argv[1])
    overrides = sys.argv[2:]

    with toml_path.open("rb") as f:
        cfg = tomli.load(f)

    for override in overrides:
        key, _, val = override.partition("=")
        if not _:
            print(f"Error: invalid override '{override}', expected key=value")
            sys.exit(1)

        parts = key.split(".")
        d = cfg
        for p in parts[:-1]:
            d = d.setdefault(p, {})
        d[parts[-1]] = parse_value(val)

    with toml_path.open("wb") as f:
        tomli_w.dump(cfg, f)

    print(f"Updated {toml_path}: {', '.join(overrides)}")


if __name__ == "__main__":
    main()

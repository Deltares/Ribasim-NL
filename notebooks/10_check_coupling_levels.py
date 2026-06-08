# %%
import argparse
from pathlib import Path

from ribasim_nl.coupling_levels import CouplingLevelSettings, run_coupling_level_check
from ribasim_nl.settings import settings

TOML_FILE = settings.ribasim_nl_data_dir.joinpath(
    "Rijkswaterstaat",
    "modellen",
    "lhm_sub_models",
    "VrijAfwaterend_DOD_Vechtstromen_coupled",
    "VrijAfwaterend_DOD_Vechtstromen_coupled.toml",
)

UPSTREAM_SUPPLY_OFFSET = -0.04
RWS_PROFILE_OFFSET = 0.1
APPLY_RWS_INLET_MIN_UPSTREAM = False
APPLY_MAX_DOWNSTREAM_LEVEL = False
APPLY_DIRECT_MIN_UPSTREAM_LEVEL = False


def coupling_level_settings() -> CouplingLevelSettings:
    return CouplingLevelSettings(
        upstream_supply_offset=UPSTREAM_SUPPLY_OFFSET,
        rws_profile_offset=RWS_PROFILE_OFFSET,
        apply_rws_inlet_min_upstream=APPLY_RWS_INLET_MIN_UPSTREAM,
        apply_max_downstream_level=APPLY_MAX_DOWNSTREAM_LEVEL,
        apply_direct_min_upstream_level=APPLY_DIRECT_MIN_UPSTREAM_LEVEL,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check and apply coupling-level corrections for a Ribasim model.")
    parser.add_argument("--toml-file", type=Path, default=TOML_FILE, help="Pad naar een bestaand Ribasim TOML-model.")
    parser.add_argument("--tolerance", type=float, default=1e-6, help="Numerical tolerance for level checks.")
    args, _ = parser.parse_known_args()
    return args


def main() -> None:
    args = parse_args()
    if not args.toml_file.exists():
        raise FileNotFoundError(args.toml_file)

    run_coupling_level_check(
        toml_file=args.toml_file,
        settings=coupling_level_settings(),
        tolerance=args.tolerance,
    )


if __name__ == "__main__":
    main()

import argparse
from pathlib import Path

from ribasim_nl.coupling_levels import CouplingLevelSettings, run_coupling_level_report

COUPLING_LEVEL_SETTINGS = CouplingLevelSettings(
    upstream_supply_offset=-0.04,
    rws_profile_offset=0.1,
    apply_rws_inlet_min_upstream=True,
    apply_max_downstream_level=True,
    apply_direct_min_upstream_level=True,
    manning_n=None,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rapporteer en pas expliciet ingestelde coupling-level correcties toe."
    )
    parser.add_argument("--toml-file", type=Path, required=True, help="Pad naar Ribasim TOML-model.")
    parser.add_argument(
        "--output-gpkg",
        type=Path,
        default=None,
        help="Output-GPKG. Relatief pad wordt naast de TOML geplaatst.",
    )
    parser.add_argument(
        "--verdachte-output-gpkg",
        type=Path,
        default=None,
        help="Apart GPKG met alleen verdachte punten. Relatief pad wordt naast de TOML geplaatst.",
    )
    parser.add_argument("--tolerance", type=float, default=1e-6)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_coupling_level_report(
        toml_file=args.toml_file,
        settings=COUPLING_LEVEL_SETTINGS,
        output_gpkg=args.output_gpkg,
        verdachte_output_gpkg=args.verdachte_output_gpkg,
        tolerance=args.tolerance,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

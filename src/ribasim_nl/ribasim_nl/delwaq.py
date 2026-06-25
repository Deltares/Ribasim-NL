"""Run the full Delwaq pipeline (generate -> run -> parse) for a Ribasim model."""

from pathlib import Path

from ribasim.delwaq import generate, parse, run_delwaq

from ribasim_nl.model import Model
from ribasim_nl.settings import settings


def compute_delwaq(
    model: Model | Path | str,
    d3d_home: Path | None = None,
    to_input: bool = False,
) -> Model:
    """Generate, run and parse a Delwaq model for the given Ribasim model.

    Args:
        model: A loaded `Model` or a path to a Ribasim TOML file.
        d3d_home: Path to the Delft3D installation. Defaults to `settings.d3d_home`.
        to_input: Whether to write the parsed results back as model input.

    Returns
    -------
        The `Model` with parsed Delwaq results.
    """
    if not isinstance(model, Model):
        model = Model.read(model)

    if d3d_home is None:
        d3d_home = settings.d3d_home

    delwaq_dir = model.toml_path.with_name("delwaq")

    # print(f"generate DELWAQ model in {delwaq_dir}")
    generate(model, output_path=delwaq_dir)

    # print("run DELWAQ")
    run_delwaq(model_dir=delwaq_dir, d3d_home=d3d_home)

    print("parse DELWAQ results in Ribasim-model")
    parse(model, output_folder=delwaq_dir, to_input=to_input)

    return model


def main() -> None:
    """CLI entry point: `python -m ribasim_nl.delwaq <toml_path>`."""
    import argparse

    parser = argparse.ArgumentParser(description="Run the Delwaq pipeline for a Ribasim model.")
    parser.add_argument("toml_path", type=Path, help="Path to the Ribasim TOML file.")
    args = parser.parse_args()

    compute_delwaq(args.toml_path)
    print("Done.")


if __name__ == "__main__":
    main()

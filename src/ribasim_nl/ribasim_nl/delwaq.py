"""Run the full Delwaq pipeline (generate -> run -> parse) for a Ribasim model."""

import contextlib
from pathlib import Path

from ribasim.delwaq import generate, parse, run_delwaq

from ribasim_nl.model import Model
from ribasim_nl.settings import settings


def compute_delwaq(
    model: Model | Path | str,
    d3d_home: Path | None = None,
    to_input: bool = False,
    output_path: Path | None = None,
) -> Model:
    """Generate, run and parse a Delwaq model for the given Ribasim model.

    Args:
        model: A loaded `Model` or a path to a Ribasim TOML file.
        d3d_home: Root of the Delft3D installation, which holds `bin/run_delwaq.sh` or `bin/run_delwaq.bat`.
            Defaults to `settings.d3d_home`, the `D3D_HOME` setting.
        to_input: Whether to write the parsed results back as model input.
        output_path: Directory for the Delwaq input and output. Defaults to `delwaq` next to the model TOML.

    Returns
    -------
        The `Model` with parsed Delwaq results.
    """
    if not isinstance(model, Model):
        model = Model.read(model)

    if d3d_home is None:
        d3d_home = settings.d3d_home

    if not (d3d_home / "bin").is_dir():
        raise FileNotFoundError(
            f"D3D_HOME should be the root of the Delft3D installation, with a bin directory: {d3d_home}"
        )

    delwaq_dir = (output_path or model.toml_path.with_name("delwaq")).absolute()

    print(f"generate DELWAQ model in {delwaq_dir}")
    generate(model, output_path=delwaq_dir)

    # Delwaq reads delwaq.inp from the working directory, which run_delwaq only sets on Windows.
    # TODO: remove the chdir once we use a Ribasim release with https://github.com/Deltares/Ribasim/pull/3415
    with contextlib.chdir(delwaq_dir):
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

"""Run the full Delwaq pipeline (generate -> run -> parse) for a Ribasim model."""

from pathlib import Path

from ribasim.delwaq import generate, parse, run_delwaq

from ribasim_nl.model import Model
from ribasim_nl.settings import settings

# The concentrations of Delwaq in the results of a model, next to `concentration.nc` of the core
CONCENTRATION_FILENAME = "concentration_delwaq.nc"


def check_d3d_home(d3d_home: Path | None = None) -> Path:
    """Return the root of the Delft3D installation, by default `settings.d3d_home`, after checking it has `bin`."""
    if d3d_home is None:
        d3d_home = settings.d3d_home
    if not (d3d_home / "bin").is_dir():
        raise FileNotFoundError(
            f"D3D_HOME should be the root of the Delft3D installation, with a bin directory: {d3d_home}"
        )
    return d3d_home


def compute_delwaq(
    model: Model | Path | str,
    d3d_home: Path | None = None,
    to_input: bool = False,
    output_path: Path | None = None,
    concentration_path: Path | None = None,
) -> Model:
    """Generate, run and parse a Delwaq model for the given Ribasim model.

    Args:
        model: A loaded `Model` or a path to a Ribasim TOML file.
        d3d_home: Root of the Delft3D installation, which holds `bin/run_delwaq.sh` or `bin/run_delwaq.bat`.
            Defaults to `settings.d3d_home`, the `D3D_HOME` setting.
        to_input: Whether to write the parsed results back as model input.
        output_path: Directory for the Delwaq input and output. Defaults to `delwaq` next to the model TOML.
        concentration_path: Where to write the concentrations of Delwaq. By default `parse` writes them to
            `concentration.nc` in the results of the model, replacing those of the core. Otherwise these are kept.

    Returns
    -------
        The `Model` with parsed Delwaq results.
    """
    ribasim_model = model if isinstance(model, Model) else Model.read(model)
    d3d_home = check_d3d_home(d3d_home)

    delwaq_dir = (output_path or ribasim_model.toml_path.with_name("delwaq")).absolute()

    print(f"generate DELWAQ model in {delwaq_dir}")
    generate(ribasim_model, output_path=delwaq_dir)

    run_delwaq(model_dir=delwaq_dir, d3d_home=d3d_home)

    print("parse DELWAQ results in Ribasim-model")
    parsed = ribasim_model.results_path / "concentration.nc"
    if concentration_path is None or concentration_path.absolute() == parsed.absolute():
        return _parse(ribasim_model, delwaq_dir, to_input)

    # `parse` always writes to `concentration.nc`, so set aside that of the core while it does
    kept = parsed.with_name("concentration_core.nc")
    assert not kept.exists(), f"{kept} exists, from an interrupted earlier run?"
    has_core = parsed.is_file()
    if has_core:
        parsed.rename(kept)
    try:
        parsed_model = _parse(ribasim_model, delwaq_dir, to_input)
        parsed.replace(concentration_path)
    finally:
        if has_core:
            kept.replace(parsed)
    return parsed_model


def _parse(model: Model, delwaq_dir: Path, to_input: bool) -> Model:
    """Parse the results of Delwaq into the results of a model, and with `to_input` into a copy of the model."""
    parsed_model = parse(model, output_folder=delwaq_dir, to_input=to_input)
    # `parse` copies the model it gets
    assert isinstance(parsed_model, Model), f"parse returned a {type(parsed_model)}"
    return parsed_model


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

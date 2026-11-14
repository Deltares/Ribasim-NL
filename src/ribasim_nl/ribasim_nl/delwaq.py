"""Run the full Delwaq pipeline (generate -> run -> parse) for a Ribasim model."""

from pathlib import Path

import numpy as np
import xarray as xr
from ribasim.delwaq import generate, parse, run_delwaq

from ribasim_nl.model import Model
from ribasim_nl.settings import settings

# The attribute in which the core records the bits that its lossy compression keeps
QUANTIZE_BITS = "_QuantizeBitRoundNumberOfSignificantBits"
# The encoding of the core that writing its concentrations again keeps
CORE_ENCODING = ("dtype", "zlib", "complevel", "shuffle", "chunksizes")


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
) -> Model:
    """Generate, run and parse a Delwaq model for the given Ribasim model.

    If the core wrote concentrations, the substances that only Delwaq has, like the nutrients of emissions, are
    added to these, see `add_delwaq_substances`. So the tracers stay those of the core. Otherwise `parse` writes
    the concentrations of Delwaq to `concentration.nc` in the results.

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
    ribasim_model = model if isinstance(model, Model) else Model.read(model)
    d3d_home = check_d3d_home(d3d_home)

    delwaq_dir = (output_path or ribasim_model.toml_path.with_name("delwaq")).absolute()

    print(f"generate DELWAQ model in {delwaq_dir}")
    generate(ribasim_model, output_path=delwaq_dir)

    run_delwaq(model_dir=delwaq_dir, d3d_home=d3d_home)

    print("parse DELWAQ results in Ribasim-model")
    concentration = ribasim_model.results_path / "concentration.nc"
    if not concentration.is_file():
        return _parse(ribasim_model, delwaq_dir, to_input)

    # `parse` writes to `concentration.nc`, so set aside that of the core while it does
    core = concentration.with_name("concentration_core.nc")
    parsed = concentration.with_name("concentration_parsed.nc")
    for path in (core, parsed):
        assert not path.exists(), f"{path} exists, from an interrupted earlier run?"
    concentration.rename(core)
    try:
        parsed_model = _parse(ribasim_model, delwaq_dir, to_input)
        concentration.rename(parsed)
        added = add_delwaq_substances(core, parsed, concentration)
        print(f"added the substances of Delwaq to the concentrations of the core: {', '.join(added)}")
    except BaseException:
        concentration.unlink(missing_ok=True)
        core.rename(concentration)
        raise
    finally:
        parsed.unlink(missing_ok=True)
    core.unlink()
    return parsed_model


def _parse(model: Model, delwaq_dir: Path, to_input: bool) -> Model:
    """Parse the results of Delwaq into the results of a model, and with `to_input` into a copy of the model."""
    parsed_model = parse(model, output_folder=delwaq_dir, to_input=to_input)
    # `parse` copies the model it gets
    assert isinstance(parsed_model, Model), f"parse returned a {type(parsed_model)}"
    return parsed_model


def add_delwaq_substances(core_path: Path, delwaq_path: Path, output_path: Path) -> list[str]:
    """Write the concentrations of the core with the substances that only Delwaq has; return these.

    The core stamps the state at the end of a time step with the start of that time step. `parse` stamps the
    state at its time, starting with the initial state, see Deltares/Ribasim#3445. So the values of Delwaq move
    one time step earlier, and they have none for the last time step. The file keeps the encoding of the core,
    including its lossy compression, so the values of the core stay the same.
    """
    with xr.open_dataset(core_path) as core_ds, xr.open_dataset(delwaq_path) as delwaq_ds:
        core = core_ds["concentration"].load()
        delwaq = delwaq_ds["concentration"].load()
        dims = ("time", "substance", "node_id")
        assert core.dims == dims, f"Unexpected dimensions {core.dims} in {core_path}"
        assert np.array_equal(core["time"], delwaq["time"]), "The time steps of the core and Delwaq differ"
        assert set(core["node_id"].to_numpy()) == set(delwaq["node_id"].to_numpy()), "The Basins differ"
        initial = delwaq.sel(substance="Initial").isel(time=0).to_numpy()
        assert np.allclose(initial, 1.0), f"{delwaq_path} does not start with the initial state, see Ribasim#3445"
        core_substances = set(core["substance"].to_numpy().astype(str))
        added = [str(s) for s in delwaq["substance"].to_numpy() if str(s) not in core_substances]
        values = (
            delwaq.sel(substance=added)
            .reindex(node_id=core["node_id"].to_numpy())
            .shift(time=-1)
            .transpose(*dims)
            .astype(core.dtype)
        )
        values = values.assign_coords(node_id=core["node_id"], time=core["time"])
        merged = xr.concat([core, values], dim="substance", combine_attrs="override")
        attrs = dict(core.attrs)
        encoding = {key: value for key, value in core.encoding.items() if key in CORE_ENCODING}
        if QUANTIZE_BITS in attrs:
            # netCDF4 writes this attribute itself when it quantizes
            encoding |= {"quantize_mode": "BitRound", "significant_digits": int(attrs.pop(QUANTIZE_BITS))}
        merged.attrs = attrs
        out = xr.Dataset({"concentration": merged}, attrs=core_ds.attrs)
        tmp = output_path.with_name(f"{output_path.stem}_tmp.nc")
        try:
            out.to_netcdf(tmp, encoding={"concentration": encoding})
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
    tmp.replace(output_path)
    return added


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

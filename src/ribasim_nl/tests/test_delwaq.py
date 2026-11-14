from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from ribasim_nl import delwaq

TIME = pd.date_range("2020-01-01", periods=4, freq="D")


def write_core(path: Path) -> np.ndarray:
    """Write concentrations like the core: a tracer and a fraction of the LHM, with lossy compression."""
    values = np.random.default_rng(0).random((4, 2, 3))
    da = xr.DataArray(
        values,
        dims=("time", "substance", "node_id"),
        coords={"time": TIME, "substance": ["Initial", "Neerslag"], "node_id": np.array([3, 1, 2], dtype="int32")},
        attrs={"units": "g m-3", "long_name": "mass concentration"},
    )
    encoding = {"zlib": True, "complevel": 1, "quantize_mode": "BitRound", "significant_digits": 20}
    xr.Dataset({"concentration": da}, attrs={"title": "Ribasim results: concentration"}).to_netcdf(
        path, encoding={"concentration": encoding}
    )
    with xr.open_dataset(path) as ds:
        return ds["concentration"].to_numpy()


def write_parsed(path: Path, initial: float = 1.0) -> np.ndarray:
    """Write concentrations like `parse`: from the initial state, sorted by substance and node, in float32."""
    no3 = np.arange(12, dtype="float32").reshape(4, 3) * 1000
    values = np.stack([np.full((4, 3), initial, dtype="float32"), np.zeros((4, 3), dtype="float32"), no3], axis=1)
    xr.Dataset(
        {"concentration": (("time", "substance", "node_id"), values)},
        coords={"time": TIME, "substance": ["Initial", "NO3", "Neerslag"], "node_id": np.array([1, 2, 3])},
    ).to_netcdf(path)
    return values


def test_add_delwaq_substances(tmp_path: Path):
    core, parsed, output = tmp_path / "core.nc", tmp_path / "parsed.nc", tmp_path / "concentration.nc"
    core_values = write_core(core)
    parsed_values = write_parsed(parsed)

    assert delwaq.add_delwaq_substances(core, parsed, output) == ["NO3"]

    with xr.open_dataset(output) as ds:
        da = ds["concentration"]
        assert da.dims == ("time", "substance", "node_id")
        assert da["substance"].to_numpy().tolist() == ["Initial", "Neerslag", "NO3"]
        assert da["node_id"].to_numpy().tolist() == [3, 1, 2]
        assert ds.attrs["title"] == "Ribasim results: concentration"
        assert da.attrs["units"] == "g m-3"
        # The tracers of the core stay as they are, also after compressing them again
        np.testing.assert_array_equal(da.sel(substance=["Initial", "Neerslag"]).to_numpy(), core_values)
        # Node 3 is the third in the file of Delwaq, and its values move one day earlier
        no3 = da.sel(substance="NO3", node_id=3).to_numpy()
        np.testing.assert_allclose(no3[:3], parsed_values[1:, 1, 2], rtol=1e-6)
        assert np.isnan(no3[3])


def test_add_delwaq_substances_needs_the_initial_state(tmp_path: Path):
    """A file of `parse` that does not start with the initial state no longer has the shift of Ribasim#3445."""
    core, parsed = tmp_path / "core.nc", tmp_path / "parsed.nc"
    write_core(core)
    write_parsed(parsed, initial=0.5)
    with pytest.raises(AssertionError, match="initial state"):
        delwaq.add_delwaq_substances(core, parsed, tmp_path / "concentration.nc")


class FakeModel:
    """The attributes of a model that `compute_delwaq` uses."""

    def __init__(self, toml_path: Path):
        self.toml_path = toml_path
        self.results_path = toml_path.parent / "results"


@pytest.fixture
def run(tmp_path: Path, monkeypatch) -> FakeModel:
    """A run whose core wrote concentrations, with Delwaq replaced by a `parse` that writes its own."""
    model = FakeModel(tmp_path / "model.toml")
    model.results_path.mkdir()
    write_core(model.results_path / "concentration.nc")
    (tmp_path / "d3d" / "bin").mkdir(parents=True)
    monkeypatch.setattr(delwaq, "Model", FakeModel)
    monkeypatch.setattr(delwaq, "generate", lambda model, output_path: None)
    monkeypatch.setattr(delwaq, "run_delwaq", lambda model_dir, d3d_home: None)

    def parse(model, output_folder, to_input):
        write_parsed(model.results_path / "concentration.nc")
        return model

    monkeypatch.setattr(delwaq, "parse", parse)
    return model


def test_compute_delwaq_adds_substances(run: FakeModel, tmp_path: Path):
    delwaq.compute_delwaq(run, d3d_home=tmp_path / "d3d")
    assert [path.name for path in run.results_path.iterdir()] == ["concentration.nc"]
    with xr.open_dataset(run.results_path / "concentration.nc") as ds:
        assert ds["substance"].to_numpy().tolist() == ["Initial", "Neerslag", "NO3"]


def test_compute_delwaq_without_core_concentration(run: FakeModel, tmp_path: Path):
    """Without concentrations of the core, those of Delwaq are written as `parse` writes them."""
    (run.results_path / "concentration.nc").unlink()
    delwaq.compute_delwaq(run, d3d_home=tmp_path / "d3d")
    with xr.open_dataset(run.results_path / "concentration.nc") as ds:
        assert ds["substance"].to_numpy().tolist() == ["Initial", "NO3", "Neerslag"]


def test_compute_delwaq_restores_core_concentration(run: FakeModel, tmp_path: Path, monkeypatch):
    """If parsing fails, the concentrations of the core are put back."""
    before = (run.results_path / "concentration.nc").read_bytes()

    def fail(model, output_folder, to_input):
        raise RuntimeError("parse failed")

    monkeypatch.setattr(delwaq, "parse", fail)
    with pytest.raises(RuntimeError, match="parse failed"):
        delwaq.compute_delwaq(run, d3d_home=tmp_path / "d3d")
    assert [path.name for path in run.results_path.iterdir()] == ["concentration.nc"]
    assert (run.results_path / "concentration.nc").read_bytes() == before


def test_check_d3d_home(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="D3D_HOME"):
        delwaq.check_d3d_home(tmp_path)
    (tmp_path / "bin").mkdir()
    assert delwaq.check_d3d_home(tmp_path) == tmp_path

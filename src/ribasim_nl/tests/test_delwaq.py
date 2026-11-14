from pathlib import Path

import pytest

from ribasim_nl import delwaq


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
    (model.results_path / "concentration.nc").write_text("core")
    (tmp_path / "d3d" / "bin").mkdir(parents=True)
    monkeypatch.setattr(delwaq, "Model", FakeModel)
    monkeypatch.setattr(delwaq, "generate", lambda model, output_path: None)
    monkeypatch.setattr(delwaq, "run_delwaq", lambda model_dir, d3d_home: None)

    def parse(model, output_folder, to_input):
        (model.results_path / "concentration.nc").write_text("delwaq")
        return model

    monkeypatch.setattr(delwaq, "parse", parse)
    return model


def test_compute_delwaq_replaces_core_concentration(run: FakeModel, tmp_path: Path):
    delwaq.compute_delwaq(run, d3d_home=tmp_path / "d3d")
    assert (run.results_path / "concentration.nc").read_text() == "delwaq"


def test_compute_delwaq_keeps_core_concentration(run: FakeModel, tmp_path: Path):
    target = run.results_path / delwaq.CONCENTRATION_FILENAME
    delwaq.compute_delwaq(run, d3d_home=tmp_path / "d3d", concentration_path=target)
    assert (run.results_path / "concentration.nc").read_text() == "core"
    assert target.read_text() == "delwaq"
    assert sorted(p.name for p in run.results_path.iterdir()) == ["concentration.nc", delwaq.CONCENTRATION_FILENAME]


def test_compute_delwaq_restores_core_concentration(run: FakeModel, tmp_path: Path, monkeypatch):
    """If parsing fails, the concentrations of the core are put back."""

    def fail(model, output_folder, to_input):
        raise RuntimeError("parse failed")

    monkeypatch.setattr(delwaq, "parse", fail)
    with pytest.raises(RuntimeError, match="parse failed"):
        delwaq.compute_delwaq(run, d3d_home=tmp_path / "d3d", concentration_path=tmp_path / "out.nc")
    assert (run.results_path / "concentration.nc").read_text() == "core"
    assert not (run.results_path / "concentration_core.nc").exists()


def test_check_d3d_home(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="D3D_HOME"):
        delwaq.check_d3d_home(tmp_path)
    (tmp_path / "bin").mkdir()
    assert delwaq.check_d3d_home(tmp_path) == tmp_path

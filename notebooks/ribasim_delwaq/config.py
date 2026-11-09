"""Configuration for the Ribasim-DELWAQ integration workflow."""

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DelwaqConfig:
    """Resolved paths shared by the DELWAQ preparation scripts."""

    model_toml: Path
    delwaq_data: Path

    @property
    def model_path(self) -> Path:
        """Return the directory containing the Ribasim model."""
        return self.model_toml.parent

    @property
    def animo_data(self) -> Path:
        """Return the ANIMO data directory."""
        return self.delwaq_data / "ANIMO"

    @property
    def er_data(self) -> Path:
        """Return the Emissieregistratie data directory."""
        return self.delwaq_data / "Emissieregistratie"

    @property
    def im_data(self) -> Path:
        """Return the IM data directory."""
        return self.delwaq_data / "IM"

    @property
    def zinfo_data(self) -> Path:
        """Return the Zinfo data directory."""
        return self.delwaq_data / "Zinfo"


def load_delwaq_config(config_path: Path) -> DelwaqConfig:
    """Load and resolve the DELWAQ configuration from its TOML path."""
    with config_path.open("rb") as config_file:
        config = tomllib.load(config_file)

    root = config_path.resolve().parents[2]
    model_toml = root / config["model"]["toml_path"]
    delwaq_data = root / config["paths"]["delwaq_data"]
    if not model_toml.is_file():
        raise FileNotFoundError(f"Configured model TOML does not exist: {model_toml}")
    if not delwaq_data.is_dir():
        raise FileNotFoundError(f"Configured DELWAQ data directory does not exist: {delwaq_data}")
    return DelwaqConfig(model_toml=model_toml, delwaq_data=delwaq_data)

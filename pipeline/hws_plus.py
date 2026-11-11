"""Build the hws_plus model from the run of the coupled LHM model, see `ribasim_nl.hws_plus`."""

from ribasim_nl.hws_plus import build_hws_plus
from ribasim_nl.settings import settings

data_dir = settings.ribasim_nl_data_dir
build_hws_plus(
    run_toml=data_dir / "Rijkswaterstaat/runs/lhm_coupled/lhm_coupled.toml",
    output_toml=data_dir / "Rijkswaterstaat/modellen/hws_plus/hws_plus.toml",
)

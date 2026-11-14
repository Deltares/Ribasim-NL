"""Export the coupled LHM model for the documentation model viewer, see `ribasim_nl.webmap`.

The run with Delwaq is a copy of the run of the coupled model, with the emissions and the concentrations of
Delwaq added, so the viewer shows the concentrations of both the core and Delwaq.
"""

from ribasim_nl.settings import settings
from ribasim_nl.webmap import export_webmap

data_dir = settings.ribasim_nl_data_dir
export_webmap(
    toml_path=data_dir / "Rijkswaterstaat/runs/lhm_coupled_delwaq/lhm_coupled.toml",
    waterboards_path=data_dir / "Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg",
    output_dir=data_dir / "Rijkswaterstaat/webmap/lhm_coupled",
)

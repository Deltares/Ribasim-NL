"""Export the coupled LHM model for the documentation model viewer, see `ribasim_nl.webmap`."""

from ribasim_nl.settings import settings
from ribasim_nl.webmap import export_webmap

data_dir = settings.ribasim_nl_data_dir
export_webmap(
    toml_path=data_dir / "Rijkswaterstaat/modellen/lhm_coupled/lhm_coupled.toml",
    waterboards_path=data_dir / "Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg",
    output_dir=data_dir / "Rijkswaterstaat/webmap/lhm_coupled",
)

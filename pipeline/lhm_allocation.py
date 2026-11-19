"""Derive `lhm_allocation`, the coupled LHM model from `koppelen` with allocation, see docs/workflow/allocation.qmd.

See `ribasim_nl.allocation.enable_allocation` for how the subnetworks are chosen. Change the model here, after
`enable_allocation`, to try a variant.
"""

from ribasim_nl.allocation import enable_allocation
from ribasim_nl.settings import settings

from ribasim_nl import Model

modellen_dir = settings.ribasim_nl_data_dir / "Rijkswaterstaat/modellen"
model = Model.read(modellen_dir / "lhm_coupled/lhm_coupled.toml")
enable_allocation(model)
model.write(modellen_dir / "lhm_allocation/lhm_allocation.toml")

import logging

from ribasim import Model

from ribasim_nl import CloudStorage, junctionify

logger = logging.getLogger(__name__)


cloud = CloudStorage()

toml_path_input = cloud.joinpath("Rijkswaterstaat/modellen/lhm_coupled_2025_9_0/lhm.toml")
toml_path_output = cloud.joinpath("Rijkswaterstaat/modellen/lhm_coupled_junction/lhm.toml")

model = Model.read(toml_path_input)
model = junctionify(model)
model.write(toml_path_output)

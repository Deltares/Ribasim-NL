from ribasim_nl.junctions import junctionify

from ribasim_nl import CloudStorage, Model

if __name__ == "__main__":
    cloud = CloudStorage()

    toml_path_input = cloud.joinpath("Rijkswaterstaat/modellen/lhm_coupled_2025_9_0/lhm.toml")
    toml_path_output = cloud.joinpath("Rijkswaterstaat/modellen/lhm_coupled_junction/lhm.toml")

    model = Model.read(toml_path_input)
    model = junctionify(model)
    model.write(toml_path_output)

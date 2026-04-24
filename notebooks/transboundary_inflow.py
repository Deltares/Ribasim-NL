# %% Imports
import logging

import pandas as pd

from ribasim_nl import CloudStorage, Model, add_transboundary_inflow, import_transboundary_inflow

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", force=True)

# %% input
cloud = CloudStorage()
transboundary_data_path = cloud.joinpath("Basisgegevens/BuitenlandseAanvoer/aangeleverd/BuitenlandseAanvoer_V5.xlsx")
cloud.synchronize(filepaths=[transboundary_data_path])

ribasim_toml_input = cloud.joinpath("Vechtstromen/modellen/Vechtstromen_dynamic_model/vechtstromen.toml")
ribasim_toml_output = cloud.joinpath("Vechtstromen/modellen/Vechtstromen_dynamic_model-ba/vechtstromen-ba.toml")

# %% load model
model = Model.read(ribasim_toml_input)

start_time = pd.to_datetime("2017-01-01")
stop_time = pd.to_datetime("2020-01-01")

# %% importeer buitenlandse aanvoer en voeg toe aan model
dict_flow = import_transboundary_inflow(transboundary_data_path, start_time, stop_time, model)
add_transboundary_inflow(model, dict_flow)

model.write(ribasim_toml_output)

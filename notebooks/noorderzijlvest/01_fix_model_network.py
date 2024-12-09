# %%
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, pump

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.reset_static_tables import reset_static_tables

cloud = CloudStorage()

authority = "Noorderzijlvest"
short_name = "nzv"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{short_name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")

# %% read model
model = Model.read(ribasim_toml)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_network", f"{short_name}.toml")
network_validator = NetworkValidator(model)

# %% some stuff we'll need again
manning_data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])
level_data = level_boundary.Static(level=[0])

basin_data = [
    basin.Profile(level=[0.0, 1.0], area=[0.01, 1000.0]),
    basin.Static(
        drainage=[0.0],
        potential_evaporation=[0.001 / 86400],
        infiltration=[0.0],
        precipitation=[0.005 / 86400],
    ),
    basin.State(level=[0]),
]
outlet_data = outlet.Static(flow_rate=[100])
pump_data = pump.Static(flow_rate=[10])

# %%
# %% https://github.com/Deltares/Ribasim-NL/issues/155#issuecomment-2454955046

# 76 edges bij opgeheven nodes verwijderen
mask = model.edge.df.to_node_id.isin(model.node_table().df.index) & model.edge.df.from_node_id.isin(
    model.node_table().df.index
)
missing_edges_df = model.edge.df[~mask]

model.edge.df = model.edge.df[~model.edge.df.index.isin(missing_edges_df.index)]

# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)


#  %% write model
model.use_validation = True
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()
# %%

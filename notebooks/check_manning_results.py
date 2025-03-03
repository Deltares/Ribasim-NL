# %%
from ribasim_nl import CloudStorage, Model
from ribasim_nl.parametrization.manning_resistance_table import manning_flow_rate

cloud = CloudStorage()
ribasim_toml = cloud.joinpath("AaenMaas", "modellen", "AaenMaas_parameterized_model", "aam.toml")

model = Model.read(ribasim_toml)
manning_node_id = 1101
at_timestamp = model.basin_results.df.index.max()

# discharge calculated directly using Manning equation
expected_q = manning_flow_rate(model, manning_node_id=manning_node_id)

# discharge from upstream
edge_id = model.edge.df.reset_index().set_index("to_node_id").at[manning_node_id, "edge_id"]
simulated_q = model.flow_results.df.loc[at_timestamp].set_index("edge_id").at[edge_id, "flow_rate"]

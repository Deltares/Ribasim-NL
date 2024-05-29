# %%
import shutil

import pandas as pd
import ribasim
from ribasim_nl import CloudStorage
from ribasim_nl.results import basin_results

cloud = CloudStorage()


# %%
# Inlezen ribasim model
ribasim_model_dir = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_2024_4_4")
ribasim_toml = ribasim_model_dir / "hws.toml"
model = ribasim.Model.read(ribasim_toml)


# read properties
if hasattr(model, "network"):
    edge_df = model.network.edge.df
    node_df = model.network.node.df
else:
    edge_df = model.edge.df
    node_df = model.node_table().df

dt = model.solver.saveat

basin_results_df = basin_results(model)

# %%
# read results
basin_arrow = ribasim_model_dir.joinpath("results", "basin.arrow")
basin_results_df = pd.read_feather(basin_arrow)
edge_results_df = pd.read_feather(ribasim_model_dir.joinpath("results", "flow.arrow"))
shutil.copy2(
    basin_arrow,
    ribasim_model_dir.joinpath("results", "basin_original.arrow"),
)
# init balance error df
balance_error_df = basin_results_df.set_index(["node_id", "time"])


# %%
# balans per basin
# Let op (!) we sommeren alleen de edges, niet neerslag, verdamping, etc

# flows: filter from and to basin edges
is_basin_edge = (edge_results_df.to_node_type == "Basin") | (
    edge_results_df.from_node_type == "Basin"
)
basin_edge_df = edge_results_df[is_basin_edge]

# flows: revert flows of 'from basin edges' (positive flow is negative flow on balance)
from_basin = basin_edge_df.from_node_type == "Basin"
basin_edge_df.loc[from_basin, "flow_rate"] = -basin_edge_df[from_basin].flow_rate

# flows: set basin_id
basin_edge_df.loc[from_basin, "node_id"] = basin_edge_df[from_basin].from_node_id
basin_edge_df.loc[~from_basin, "node_id"] = basin_edge_df[~from_basin].to_node_id
is_inflow = basin_edge_df.flow_rate > 0

# flows: calculate Vin
basin_inflow_df = basin_edge_df.loc[is_inflow]
basin_inflow_df.set_index(["node_id", "time"], inplace=True)
inflow = basin_inflow_df.groupby(["node_id", "time"]).flow_rate.sum()

balance_error_df.loc[inflow.index, ["inflow"]] = inflow
balance_error_df.loc[balance_error_df["inflow"].isna(), "inflow"] = 0

# flows: calculate Vout
basin_outflow_df = basin_edge_df.loc[~is_inflow]
basin_outflow_df.set_index(["node_id", "time"], inplace=True)
outflow = -basin_outflow_df.groupby(["node_id", "time"]).flow_rate.sum()

balance_error_df.loc[outflow.index, ["outflow"]] = outflow
balance_error_df.loc[balance_error_df["outflow"].isna(), "outflow"] = 0

# %%
# calculate delta-storage
for node_id, df in basin_results_df.groupby("node_id"):
    df.sort_values(by="time", inplace=True)
    df.set_index(["node_id", "time"], inplace=True, drop=False)
    delta_time = df["time"].diff(1).shift(-1).dt.total_seconds()
    storage_change = df["storage"].diff(1).shift(-1) / delta_time
    balance_error_df.loc[storage_change.index, ["storage_change"]] = storage_change

# calculate balance Error: inflow + precipitation + drainage - evaporation - infiltration - outflow - storage_change
if "precipitation" in balance_error_df.columns:
    balance_error_df.loc[:, "balance_error"] = (
        balance_error_df["inflow"]
        + balance_error_df["precipitation"]
        + balance_error_df["drainage"]
        - balance_error_df["evaporation"]
        - balance_error_df["infiltration"]
        - balance_error_df["outflow"]
        - balance_error_df["storage_change"]
    )
else:
    balance_error_df.loc[:, "balance_error"] = (
        balance_error_df["inflow"]
        - balance_error_df["outflow"]
        - balance_error_df["storage_change"]
    )

# possible positive error (if error not 0): inflow is larger than outflow
positive = balance_error_df["inflow"] > balance_error_df["outflow"]

# if positive error, we devide the error by the outflow (x% too much inflow compared to outflow, or x% too little outflow compared to inflow)
balance_error_df.loc[positive, ["relative_balance_error"]] = (
    balance_error_df[positive]["balance_error"] / balance_error_df[positive]["outflow"]
) * 100

# if negative error, we devide the error by the inflow (x% too much outflow compared to outflow, or x% too little inflow compared to outflow)
balance_error_df.loc[~positive, ["relative_balance_error"]] = (
    balance_error_df[~positive]["balance_error"] / balance_error_df[~positive]["inflow"]
) * 100

balance_error_df.reset_index(inplace=True)
balance_error_df.to_feather(basin_arrow)

# %%

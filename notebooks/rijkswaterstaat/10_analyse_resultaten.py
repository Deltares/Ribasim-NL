# %%
import shutil
from pathlib import Path

import pandas as pd
import ribasim

# from ribasim_nl import CloudStorage

# cloud = CloudStorage()

pd.options.mode.chained_assignment = None

# %%
# Inlezen ribasim model
# ribasim_model_dir = cloud.joinpath("Rijkswaterstaat", "modellen", "hws")
ribasim_model_dir = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Rijkswaterstaat\modellen\hws"
)
ribasim_toml = ribasim_model_dir / "hws.toml"
model = ribasim.Model.read(ribasim_toml)


# read properties
edge_df = model.edge.df
# node_df = model.network.node.df
node_df = model.node_table().df

dt = model.solver.saveat


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
inflow_rate = basin_inflow_df.groupby(["node_id", "time"]).flow_rate.sum()
balance_error_df.loc[inflow_rate.index, ["inflow_rate"]] = inflow_rate

# flows: calculate Vout
basin_outflow_df = basin_edge_df.loc[~is_inflow]
basin_outflow_df.set_index(["node_id", "time"], inplace=True)
outflow_rate = -basin_outflow_df.groupby(["node_id", "time"]).flow_rate.sum()
balance_error_df.loc[outflow_rate.index, ["outflow_rate"]] = outflow_rate

# calculate delta-storage
for node_id, df in basin_results_df.groupby("node_id"):
    df.set_index(["node_id", "time"], inplace=True)
    storage_change_rate = df["storage"] - df["storage"].shift(1)
    balance_error_df.loc[storage_change_rate.index, ["dS[m3]"]] = storage_change_rate

# calculate balance Error
balance_error_df.loc[:, ["balance_error"]] = (
    balance_error_df["inflow_rate]"]
    - balance_error_df["outflow_rate"]
    - balance_error_df["storage_change_rate"]
)
positive = balance_error_df["inflow_rate"] > balance_error_df["outflow_rate"]
balance_error_df.loc[positive, ["relative_balance_error"]] = (
    balance_error_df[positive]["balance_error"]
    / balance_error_df[positive]["outflow_rate"]
) * 100

balance_error_df.loc[~positive, ["relative_balance_error"]] = (
    -(
        balance_error_df[~positive]["balance_error"]
        / balance_error_df[~positive]["inflow_rate"]
    )
    * 100
)

balance_error_df.to_feather(basin_arrow)

# %%
from datetime import timedelta

import geopandas as gpd
import pandas as pd
import ribasim
from ribasim_nl import CloudStorage

cloud = CloudStorage()

# %%
# Inlezen ribasim model
ribasim_model_dir = cloud.joinpath("Rijkswaterstaat", "modellen", "hws")
ribasim_toml = ribasim_model_dir / "hws.toml"
model = ribasim.Model.read(ribasim_toml)

edge_df = model.network.edge.df
node_df = model.network.node.df

dt = model.solver.saveat


# %%
# inlezen resultaten
basin_results_df = pd.read_feather(ribasim_model_dir.joinpath("results", "basin.arrow"))
edge_results_df = pd.read_feather(ribasim_model_dir.joinpath("results", "flow.arrow"))

balance_error_df = basin_results_df.set_index(["node_id", "time"])
report_date_time = basin_results_df.time.max() - timedelta(seconds=dt)

# %%
# balans per basin
# Let op (!) we sommeren alleen de edges, niet neerslag, verdamping, etc

is_basin_edge = (edge_results_df.to_node_type == "Basin") | (
    edge_results_df.from_node_type == "Basin"
)
from_basin = edge_results_df.from_node_type == "Basin"

basin_edge_df = edge_results_df[is_basin_edge]
basin_edge_df.loc[from_basin, "flow_rate"] = -basin_edge_df[
    from_basin
].flow_rate  # revert flows from basin (positive is outflow)

# set basin_id
basin_edge_df.loc[from_basin, "node_id"] = basin_edge_df[from_basin].from_node_id
basin_edge_df.loc[~from_basin, "node_id"] = basin_edge_df[~from_basin].to_node_id
is_inflow = basin_edge_df.flow_rate > 0

# calculate Qin
basin_inflow_df = basin_edge_df.loc[is_inflow]
basin_inflow_df.set_index(["node_id", "time"], inplace=True)
Qin = basin_inflow_df.groupby(["node_id", "time"]).flow_rate.sum()
balance_error_df.loc[Qin.index, ["Qin"]] = Qin
balance_error_df.loc[:, ["Vin"]] = balance_error_df["Qin"] * dt

# calculate Qout
basin_outflow_df = basin_edge_df.loc[~is_inflow]
basin_outflow_df.set_index(["node_id", "time"], inplace=True)
Qout = -basin_outflow_df.groupby(["node_id", "time"]).flow_rate.sum()
balance_error_df.loc[Qout.index, ["Qout"]] = Qout
balance_error_df.loc[:, ["Vout"]] = balance_error_df["Qout"] * dt

# calculate Ds
for node_id, df in basin_results_df.groupby("node_id"):
    df.set_index(["node_id", "time"], inplace=True)
    dS = df["storage"] - df["storage"].shift(1)
    balance_error_df.loc[dS.index, ["dS"]] = dS

# calculate balance Error
balance_error_df.loc[:, ["balance_error[m3]"]] = (
    balance_error_df.Vin - balance_error_df.Vout - balance_error_df.dS
)
positive = balance_error_df.Vin > balance_error_df.Vout
balance_error_df.loc[positive, ["balance_error[%]"]] = (
    balance_error_df[positive]["balance_error[m3]"] / balance_error_df[positive].Vout
) * 100

balance_error_df.loc[~positive, ["balance_error[%]"]] = (
    -(
        balance_error_df[~positive]["balance_error[m3]"]
        / balance_error_df[~positive].Vin
    )
    * 100
)

# %% dh/dx
levels_df = basin_results_df[basin_results_df.time == report_date_time][
    ["level", "node_id"]
].set_index("node_id")
manning_resistance_df = node_df[node_df.node_type == "ManningResistance"]
manning_resistance_df.loc[:, ["slope"]] = float("nan")
manning_resistance_df.loc[:, ["time"]] = report_date_time
for row in manning_resistance_df.itertuples():
    to_node_id = edge_df.set_index("from_node_id").at[row.node_id, "to_node_id"]
    to_node_type = edge_df.set_index("from_node_id").at[row.node_id, "to_node_type"]
    from_node_id = edge_df.set_index("to_node_id").at[row.node_id, "from_node_id"]
    if to_node_type == "Basin":
        h_to = levels_df.at[to_node_id, "level"]
    elif to_node_type == "LevelBoundary":
        h_to = model.level_boundary.static.df.set_index("node_id").at[
            to_node_id, "level"
        ]

    dh = abs(h_to - levels_df.at[from_node_id, "level"])
    dx = model.manning_resistance.static.df.set_index("node_id").at[
        row.node_id, "length"
    ]
    slope = dh / dx
    manning_resistance_df.loc[row.Index, ["slope"]] = slope

manning_resistance_df.to_file(
    ribasim_model_dir / "analysis.gpkg", layer="slope", engine="pyogrio"
)


# %%
balance_error_summary_df = balance_error_df.loc[(slice(None), report_date_time), :]
balance_error_summary_df.reset_index(inplace=True)
balance_error_summary_df.loc[:, "geometry"] = balance_error_summary_df.node_id.apply(
    lambda x: node_df.at[x, "geometry"]
)
balance_error_summary_df = gpd.GeoDataFrame(balance_error_summary_df, crs=node_df.crs)

balance_error_summary_df.to_file(
    ribasim_model_dir / "analysis.gpkg", layer="balance_error", engine="pyogrio"
)

# %%
import shutil

import pandas as pd
import ribasim
from ribasim_nl import CloudStorage

cloud = CloudStorage()

pd.options.mode.chained_assignment = None

# %%
# Inlezen ribasim model
ribasim_model_dir = cloud.joinpath("Rijkswaterstaat", "modellen", "hws")
ribasim_toml = ribasim_model_dir / "hws.toml"
model = ribasim.Model.read(ribasim_toml)


# read properties
edge_df = model.network.edge.df
node_df = model.network.node.df

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
Qin = basin_inflow_df.groupby(["node_id", "time"]).flow_rate.sum()
balance_error_df.loc[Qin.index, ["Vin[m3]"]] = Qin * dt

# flows: calculate Vout
basin_outflow_df = basin_edge_df.loc[~is_inflow]
basin_outflow_df.set_index(["node_id", "time"], inplace=True)
Qout = -basin_outflow_df.groupby(["node_id", "time"]).flow_rate.sum()
balance_error_df.loc[Qout.index, ["Vout[m3]"]] = Qout * dt

# calculate delta-storage
for node_id, df in basin_results_df.groupby("node_id"):
    df.set_index(["node_id", "time"], inplace=True)
    dS = df["storage"] - df["storage"].shift(1)
    balance_error_df.loc[dS.index, ["dS[m3]"]] = dS

# calculate balance Error
balance_error_df.loc[:, ["error[m3]"]] = (
    balance_error_df["Vin[m3]"]
    - balance_error_df["Vout[m3]"]
    - balance_error_df["dS[m3]"]
)
positive = balance_error_df["Vin[m3]"] > balance_error_df["Vout[m3]"]
balance_error_df.loc[positive, ["error[%]"]] = (
    balance_error_df[positive]["error[m3]"] / balance_error_df[positive]["Vout[m3]"]
) * 100

balance_error_df.loc[~positive, ["error[%]"]] = (
    -(balance_error_df[~positive]["error[m3]"] / balance_error_df[~positive]["Vin[m3]"])
    * 100
)

balance_error_df.to_feather(basin_arrow)

# balance_error_df = gpd.GeoDataFrame(
#     balance_error_df, geometry=gpd.GeoSeries(), crs=model.network.node.df.crs
# )

# balance_error_df.to_file(
#     ribasim_model_dir.joinpath("results", "post_analysis.gpkg"),
#     layer="balance",
#     engine="pyogrio",
# )


# %% dh/dx
# levels_df = basin_results_df[basin_results_df.time == report_date_time][
#     ["level", "node_id"]
# ].set_index("node_id")
# manning_resistance_df = node_df[node_df.node_type == "ManningResistance"]
# manning_resistance_df.loc[:, ["slope"]] = float("nan")
# manning_resistance_df.loc[:, ["time"]] = report_date_time
# for row in manning_resistance_df.itertuples():
#     to_node_id = edge_df.set_index("from_node_id").at[row.node_id, "to_node_id"]
#     to_node_type = edge_df.set_index("from_node_id").at[row.node_id, "to_node_type"]
#     from_node_id = edge_df.set_index("to_node_id").at[row.node_id, "from_node_id"]
#     if to_node_type == "Basin":
#         h_to = levels_df.at[to_node_id, "level"]
#     elif to_node_type == "LevelBoundary":
#         h_to = model.level_boundary.static.df.set_index("node_id").at[
#             to_node_id, "level"
#         ]

#     dh = abs(h_to - levels_df.at[from_node_id, "level"])
#     dx = model.manning_resistance.static.df.set_index("node_id").at[
#         row.node_id, "length"
#     ]
#     slope = dh / dx
#     manning_resistance_df.loc[row.Index, ["slope"]] = slope

# manning_resistance_df.to_file(
#     ribasim_model_dir / "analysis.gpkg", layer="slope", engine="pyogrio"
# )


# %%
# balance_error_summary_df = balance_error_df.loc[(slice(None), report_date_time), :]
# balance_error_summary_df.reset_index(inplace=True)
# balance_error_summary_df.loc[:, "geometry"] = balance_error_summary_df.node_id.apply(
#     lambda x: node_df.at[x, "geometry"]
# )
# balance_error_summary_df = gpd.GeoDataFrame(balance_error_summary_df, crs=node_df.crs)

# balance_error_summary_df.to_file(
#     ribasim_model_dir / "analysis.gpkg", layer="balance_error", engine="pyogrio"
# )

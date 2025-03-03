# %%
from ribasim_nl import CloudStorage, Model
from ribasim_nl.parametrization.manning_resistance_table import calculate_discharge

cloud = CloudStorage()
ribasim_toml = cloud.joinpath("test_models", "manning_test", "manning.toml")

model = Model.read(ribasim_toml)

manning_node_id = 3

# get upstream and downstream basins
us_basin_node_id = model.upstream_node_id(manning_node_id)
ds_basin_node_id = model.downstream_node_id(manning_node_id)

# get slope
final_timestep = model.basin_results.df.index.max()
df = model.basin_results.df.loc[final_timestep].set_index("node_id")
delta_h = df.at[us_basin_node_id, "level"] - df.at[ds_basin_node_id, "level"]
slope = abs(delta_h / model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "length"])

# get depth as in https://github.com/Deltares/Ribasim/blob/1773acf71857ab05390a60626fbf96dd4ccae740/core/src/solve.jl#L529-L532
water_level = (df.at[us_basin_node_id, "level"] + df.at[ds_basin_node_id, "level"]) / 2
bottom_level = (
    model.basin.profile.df.set_index("node_id").loc[us_basin_node_id, "level"].min()
    + model.basin.profile.df.set_index("node_id").loc[ds_basin_node_id, "level"].min()
) / 2
depth = water_level - bottom_level

q = calculate_discharge(
    depth=depth,
    profile_width=model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "profile_width"],
    profile_slope=model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "profile_slope"],
    manning_n=model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "manning_n"],
    slope=slope,
)

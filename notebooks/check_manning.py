# %%

from ribasim import Node
from ribasim.nodes import basin, flow_boundary, level_boundary, manning_resistance, outlet
from shapely.geometry import Point

from ribasim_nl import CloudStorage, Model

model = Model(starttime="2022-01-01", endtime="2023-01-01", crs="EPSG:28992")
cloud = CloudStorage()
ribasim_toml = cloud.joinpath("test_models", "manning_test", "manning.toml")
ribasim_toml.parent.mkdir(exist_ok=True, parents=True)

expeted_dh = 1
expected_water_level_in_profile = 0.8
error_margin = 0.02

"""
ManningResistance Static:
- profile_slope = 0.5
- profile_width = 10m (bottom width)
- manning_n = 0.04
- profile_length = 1000m

FlowBoundary Static:
flow_rate = 5.213921445 (m3/s)

Should yield a dh of ~1 meter over two basins (slope = 0.001) according to the manning equation

To fully respect manning, we will make sure the model is fully uniform under these conditions. So for basin profiles:
- we shift elevation exactly 1 m
- area table matches the manning-profile * length of canal.
"""

model.flow_boundary.add(node=Node(1, geometry=Point(-10, 0)), tables=[flow_boundary.Static(flow_rate=[5.213921445])])
model.basin.add(
    node=Node(2, geometry=Point(0, 0)),
    tables=[
        basin.Static(precipitation=[0], potential_evaporation=[0], infiltration=[0], drainage=[0]),
        basin.Profile(area=[5000, 6000], level=[0.5, 4.5]),
        basin.State(level=[1.3]),
    ],
)
model.manning_resistance.add(
    node=Node(3, geometry=Point(500, 0)),
    tables=[manning_resistance.Static(profile_width=[10], profile_slope=[0.5], manning_n=0.04, length=[1000])],
)
model.basin.add(
    node=Node(4, geometry=Point(1000, 0)),
    tables=[
        basin.Static(precipitation=[0], potential_evaporation=[0], infiltration=[0], drainage=[0]),
        basin.Profile(area=[5000, 6000], level=[-0.5, 3.5]),
        basin.State(level=[0.3]),
    ],
)
model.outlet.add(node=Node(5, geometry=Point(1010, 0)), tables=[outlet.Static(flow_rate=[7.5])])
model.level_boundary.add(node=Node(6, geometry=Point(1020, 0)), tables=[level_boundary.Static(level=[0.3])])

model.link.add(model.flow_boundary[1], model.basin[2])
model.link.add(model.basin[2], model.manning_resistance[3])
model.link.add(model.manning_resistance[3], model.basin[4])
model.link.add(model.basin[4], model.outlet[5])
model.link.add(model.outlet[5], model.level_boundary[6])

model.write(ribasim_toml)

result = model.run()
assert result.exit_code == 0

final_timestep = model.basin_results.df.index.max()
df = model.basin_results.df.loc[final_timestep].set_index("node_id")
delta_h = df.at[2, "level"] - df.at[4, "level"]
error = abs(expeted_dh - delta_h) / expeted_dh

print(f"dh error: {error}")

assert error < error_margin


level = (df.at[2, "level"] + df.at[4, "level"]) / 2
error = abs(expected_water_level_in_profile - level) / expected_water_level_in_profile

print(f"level error: {error}")

assert error < error_margin

# %%
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import (
    basin,
    discrete_control,
    flow_demand,
    level_boundary,
    manning_resistance,
    outlet,
    pump,
)
from shapely.geometry import Point

from ribasim_nl import Model

# initialize model
model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")


# specify basin time series
time = pd.date_range(model.starttime, model.endtime)
day_of_year = time.day_of_year.to_numpy()
precipitation = np.zeros(day_of_year.size)
precipitation[0:90] = 1e-6
precipitation[90:180] = 0
precipitation[180:270] = 1e-6
precipitation[270:366] = 0
evaporation = np.zeros(day_of_year.size)
evaporation[0:90] = 0
evaporation[90:180] = 1e-6
evaporation[180:270] = 0
evaporation[270:366] = 1e-6

# specify basin data
basin_profile = basin.Profile(area=[0.01, 1000000.0], level=[-10, 1.0])
basin_time = basin.Time(
    time=pd.date_range(model.starttime, model.endtime),
    drainage=0.0,
    potential_evaporation=evaporation,
    infiltration=0.0,
    precipitation=precipitation,
)


# specify basins
# Boezem system is made of 2 basins with a level of 1.2 msl
# Polder system is made of 2 basins with levels of 1.0 msl and 0.9 msl
basin1 = model.basin.add(
    Node(1, Point(2.0, 0.0), name="Boezem (1.2 msl)"), tables=[basin_profile, basin_time, basin.State(level=[1.2])]
)
basin2 = model.basin.add(
    Node(2, Point(2.0, 2.0), name="Polder (1 msl)"), tables=[basin_profile, basin_time, basin.State(level=[1.0])]
)
basin3 = model.basin.add(
    Node(3, Point(4.0, 2.0), name="Polder (0.9 msl)"), tables=[basin_profile, basin_time, basin.State(level=[0.9])]
)
basin4 = model.basin.add(
    Node(4, Point(4.0, 0.0), name="Boezem 1.2 msl"), tables=[basin_profile, basin_time, basin.State(level=[1.2])]
)


# we sturen op Basin #3.
# Als de level > 0.95 msl is, dan gaat het afvoergemaal (pump7) aan en inlaat (outlet6) dicht
# Als de level ≤ 0.90 msl is, dan gaat het afvoergemaal (pump7) uit en inlaat (outlet6) open
dc_variable = discrete_control.Variable(
    compound_variable_id=1,
    listen_node_id=3,
    variable=["level"],
)

dc_condition = discrete_control.Condition(
    compound_variable_id=1,
    condition_id=[1],
    threshold_high=[0.95],  # True als >0.90
    threshold_low=[0.9],  # False als ≤0.89
)


# specify outlets
outlet5 = model.outlet.add(
    Node(5, Point(1.0, 0.0)),
    [outlet.Static(flow_rate=10, min_upstream_level=[1.2], max_downstream_level=[1.2])],
)

outlet6 = model.outlet.add(
    Node(6, Point(2, 1), name="inlaat"),
    [
        outlet.Static(
            control_state=["closed", "open"],
            flow_rate=[0.0, 5.0],  # two states, in case of flow-demand we apply closed-state > 0
            min_upstream_level=[1.1, 1.1],
            max_downstream_level=[1.02, 1.02],  # a bit higher than 1.0 so there will always be floww
        )
    ],
)

outlet7 = model.outlet.add(
    Node(7, Point(3, 2), name="doorlaat (inlaat+uitlaat)"),
    [
        outlet.Static(flow_rate=[5], min_upstream_level=[0.98], max_downstream_level=[0.95])
    ],  # @visr, why doesn't this work with min_upstream_level = [0.99] (max_downstream_level = [1.00] at outlet #6)?
)

outlet8 = model.outlet.add(
    Node(8, Point(5.0, 0)),
    [outlet.Static(flow_rate=[20], min_upstream_level=[1.2])],
)


outlet6_open_closed = model.discrete_control.add(
    Node(60, Point(2.5, 1), name="inlaat open/dicht"),
    [
        dc_condition,
        dc_variable,
        # T (>0.95)  -> closed
        # F (≤0.89)  -> open
        discrete_control.Logic(
            truth_state=["T", "F"],
            control_state=["closed", "open"],
        ),
    ],
)

model.link.add(outlet6_open_closed, outlet6)


# specify pump
# the pump will maintain a level of 0.9 msl in basin 3 in on state
# when pushed off-state by controller, the flow_rate will become 0 m3/s and will only become 5 m3/s when the level in basin 3 exceeds 0.95 msl
pump9 = model.pump.add(
    Node(9, Point(4, 1), name="afvoergemaal"),
    [
        pump.Static(
            control_state=["off", "on"],
            flow_rate=[0.0, 5.0],
            min_upstream_level=[0.91, 0.91],
        )
    ],
)

pump9_on_off = model.discrete_control.add(
    Node(90, Point(4.5, 1), name="afvoergemaal aan/uit"),
    [
        dc_variable,
        dc_condition,
        # T (>0.95)  -> aan
        # F (≤0.89)  -> uit
        discrete_control.Logic(
            truth_state=["T", "F"],  # T: >0.95, F: ≤0.90
            control_state=["on", "off"],
        ),
    ],
)

model.link.add(pump9_on_off, pump9)


# Specify manning resistance:
manning_resistance10 = model.manning_resistance.add(
    Node(10, Point(3, 0.0)),
    [manning_resistance.Static(length=[900], manning_n=[0.04], profile_width=[6.0], profile_slope=[3.0])],
)


# Specify level boundaries:
level_boundary11 = model.level_boundary.add(Node(11, Point(0, 0)), [level_boundary.Static(level=[1.3])])
level_boundary12 = model.level_boundary.add(Node(12, Point(6, 0)), [level_boundary.Static(level=[1])])


# Link nodes:

# links boezem
model.link.add(level_boundary11, outlet5)
model.link.add(outlet5, basin1)
model.link.add(
    basin1,
    outlet6,
)
model.link.add(basin1, manning_resistance10)
model.link.add(manning_resistance10, basin4)
model.link.add(basin4, outlet8)
model.link.add(outlet8, level_boundary12)

# link polder
model.link.add(outlet6, basin2)
model.link.add(basin2, outlet7)
model.link.add(outlet7, basin3)
model.link.add(basin3, pump9)
model.link.add(pump9, basin4)

# % plot to verify
model.plot()

# % run model
toml = Path(__file__).parent.joinpath("level_control", "model.toml")
model.write(toml)
model.run()


# %% plot results

# levels at basins
fig, ax = plt.subplots(figsize=(8, 5))
model.basin_results.df[model.basin_results.df.node_id == 1]["level"].plot(ax=ax, label="Basin 1")
model.basin_results.df[model.basin_results.df.node_id == 2]["level"].plot(ax=ax, label="Basin 2")
model.basin_results.df[model.basin_results.df.node_id == 3]["level"].plot(ax=ax, label="Basin 3")

ax.set_title("Polder basins")
ax.grid(True)
ax.legend()

# flows at links

fig, ax = plt.subplots(figsize=(8, 5))
model.link_results.df[model.link_results.df.link_id == 4]["flow_rate"].plot(ax=ax, label="Boezem boven (#4)")
model.link_results.df[model.link_results.df.link_id == 10]["flow_rate"].plot(ax=ax, label="Beneden inlaat (#10)")
model.link_results.df[model.link_results.df.link_id == 12]["flow_rate"].plot(ax=ax, label="Beneden doorlaat (#12)")
model.link_results.df[model.link_results.df.link_id == 14]["flow_rate"].plot(ax=ax, label="Beneden afvoergemaal (#14)")
model.link_results.df[model.link_results.df.link_id == 9]["flow_rate"].plot(ax=ax, label="Boezem beneden (#9)")


ax.set_title("Links")
ax.grid(True)
ax.legend()


# %%
t = pd.date_range(model.starttime, model.endtime, freq="D")
d = np.zeros(len(t))
d[0:90] = 0.0
d[90:180] = 1
d[180:270] = 0.0
d[270:366] = 1


pump9_alloc = model.flow_demand.add(
    Node(91, Point(4.5, 0.5), name="afvoergemaal (doorspoeling 1 m3/s)"),
    [
        flow_demand.Time(
            time=t,
            demand_priority=[1] * len(t),  # prio constant
            demand=d,  # 0 / 2 / 0 / 2
        )
    ],
)
model.link.add(pump9_alloc, pump9)

# % plot to verify
model.plot()

# %% run model
model.outlet.static.df.loc[model.outlet.static.df.node_id == 7, "min_upstream_level"] = (
    0.97  # @visr, see previous comment, why do we need this adjustment here?
)


toml = Path(__file__).parent.joinpath("level_control_and_flow_demand", "model.toml")
model.write(toml)
model.run()
model._basin_results = None  # force reload of results
model._link_results = None  # force reload of results

# %


model.link_results.df[model.link_results.df.link_id == 14]["flow_rate"].plot(title="Afvoergemaal (link #14)", grid=True)

# %%

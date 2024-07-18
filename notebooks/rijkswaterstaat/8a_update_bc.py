# %%
from datetime import datetime

import numpy as np
import pandas as pd
from ribasim import Model
from ribasim.nodes import flow_boundary
from ribasim_nl import CloudStorage

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_demand", "hws.toml")
model = Model.read(ribasim_toml)

df = pd.read_excel(
    cloud.joinpath(
        "Rijkswaterstaat", "aangeleverd", "debieten_Rijn_Maas_2023_2024.xlsx"
    ),
    skiprows=3,
)
df.set_index("Unnamed: 0", inplace=True)
df.index.name = "time"

# %%lobith series
# "2023-09-15 07:00:00" and "2023-09-15 11:00:00" are outliers
invalid_dt = np.array(
    ["2023-09-15 07:00:00", "2023-09-15 11:00:00"], dtype=np.datetime64
)
lobith_series = df.loc[~df.index.isin(invalid_dt)]["Lobith"]

# interpolate missings
lobith_series.interpolate(inplace=True)

# resample to daily values
lobith_series = lobith_series.resample("D").mean()
lobith_series.name = "flow_rate"

# %% Eijsden series
eijsden_series = df["Eijsden"]

# interpolate missings
eijsden_series.interpolate(inplace=True)

# resample to daily values
eijsden_series = eijsden_series.resample("D").mean()
eijsden_series = eijsden_series[eijsden_series.notna()]
eijsden_series.name = "flow_rate"

# %% Monsin series
df = pd.read_excel(
    cloud.joinpath("Basisgegevens", "metingen", "RWsOS-IWP_debieten_2023_2024.xlsx"),
    sheet_name="Maas",
    skiprows=5,
    index_col=0,
)

monsin_series = df["Monsin"]

# interpolate missings
monsin_series.interpolate(inplace=True)

monsin_series = monsin_series.resample("D").mean()

# resample to daily values
monsin_series = monsin_series.resample("D").mean()
monsin_series = monsin_series[monsin_series.notna()]
monsin_series.name = "flow_rate"

# %% set FlowBoundary / Time

# get node ids
lobith_node_id = model.flow_boundary.node.df.set_index("meta_meetlocatie_code").at[
    "LOBH", "node_id"
]
monsin_node_id = model.flow_boundary.node.df.set_index("meta_meetlocatie_code").at[
    "MONS", "node_id"
]

# set flow boundary timeseries
lobith_df = pd.DataFrame(lobith_series).reset_index()
lobith_df.loc[:, "node_id"] = lobith_node_id

monsin_df = pd.DataFrame(eijsden_series).reset_index()
monsin_df.loc[:, "node_id"] = monsin_node_id

bc_time_df = pd.concat([lobith_df, monsin_df])

model.flow_boundary.time = flow_boundary.Time(**bc_time_df.to_dict(orient="list"))

model.starttime = max(eijsden_series.index.min(), lobith_series.index.min())
model.endtime = min(eijsden_series.index.max(), lobith_series.index.max())

# remove fixed values from static
model.flow_boundary.static.df = model.flow_boundary.static.df[
    ~model.flow_boundary.static.df.node_id.isin([lobith_node_id, monsin_node_id])
]


# %% update LevelBoundary / Time

node_ids = (
    model.node_table()
    .df[model.node_table().df["meta_meetlocatie_code"].isin(["KOBU", "OEBU"])]
    .node_id.to_numpy()
)

time = pd.date_range(model.starttime, model.endtime)

day_of_year = [
    "01-01",
    "03-01",
    "03-11",
    "03-21",
    "04-01",
    "04-10",
    "04-15",
    "08-11",
    "08-15",
    "08-21",
    "08-31",
    "09-11",
    "09-15",
    "09-21",
    "10-01",
    "10-11",
    "10-21",
    "10-31",
    "12-31",
]

level = [
    -0.4,
    -0.1,
    -0.1,
    -0.1,
    -0.2,
    -0.2,
    -0.2,
    -0.2,
    -0.3,
    -0.3,
    -0.3,
    -0.3,
    -0.3,
    -0.3,
    -0.4,
    -0.4,
    -0.4,
    -0.4,
    -0.4,
]
level_cycle_df = pd.DataFrame(
    {
        "dayofyear": [
            datetime.strptime(i, "%m-%d").timetuple().tm_yday for i in day_of_year
        ],
        "level": level,
    }
).set_index("dayofyear")


def get_level(timestamp, level_cycle_df):
    return level_cycle_df.at[
        level_cycle_df.index[level_cycle_df.index <= timestamp.dayofyear].max(), "level"
    ]


time = pd.date_range(model.starttime, model.endtime)
level_df = pd.DataFrame(
    {"time": time, "level": [get_level(i, level_cycle_df) for i in time]}
)

level_df = pd.concat(
    [
        pd.concat(
            [level_df, pd.DataFrame({"node_id": [node_id] * len(level_df)})], axis=1
        )
        for node_id in node_ids
    ],
    ignore_index=True,
)
model.level_boundary.time.df = level_df


# %%
ribasim_toml = cloud.joinpath(
    "Rijkswaterstaat", "modellen", "hws_transient", "hws.toml"
)
model.write(ribasim_toml)

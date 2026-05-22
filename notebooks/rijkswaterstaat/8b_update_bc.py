# %%
from pathlib import Path

import pandas as pd

from ribasim_nl import CloudStorage, Model, merge_rwzi_model

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_demand/hws.toml")
model = Model.read(ribasim_toml)

rwzi_model_path = cloud.joinpath("Rijkswaterstaat/modellen/rwzi/rwzi.toml")
debieten_xlsx = cloud.joinpath("Rijkswaterstaat/aangeleverd/Matroos_debieten_2017_2022_Lobith_Monsin.xlsx")
cloud.synchronize([debieten_xlsx])

time_colname = " Timeseries retrieved from the MATROOS series database"


def read_lobith(debieten_xlsx: Path, starttime, endtime) -> pd.Series:
    df = pd.read_excel(
        debieten_xlsx,
        sheet_name="10min",
        parse_dates=[time_colname],
        date_format="%Y/%m/%d %H:%M",
    )
    df.set_index(time_colname, inplace=True)
    df.index.name = "time"
    df = df.loc[starttime:endtime]

    lobith_series = df["Lobith"]

    if lobith_series.isna().sum():
        raise ValueError("Lobith series contains missings.")
    # resample to daily values
    lobith_series = lobith_series.resample("D").mean()
    lobith_series.name = "flow_rate"
    return lobith_series


def read_monsin(debieten_xlsx: Path, starttime, endtime) -> pd.Series:
    df = pd.read_excel(
        debieten_xlsx,
        sheet_name="hourly",
        parse_dates=[time_colname],
        date_format="%Y/%m/%d %H:%M",
    )
    df.set_index(time_colname, inplace=True)
    df.index.name = "time"
    df = df.loc[starttime:endtime]

    monsin_series = df["Monsin"]

    if monsin_series.isna().sum():
        raise ValueError("Monsin series contains missings.")
    # resample to daily values
    monsin_series = monsin_series.resample("D").mean()
    monsin_series.name = "flow_rate"

    return monsin_series


lobith_series = read_lobith(debieten_xlsx, model.starttime, model.endtime)
monsin_series = read_monsin(debieten_xlsx, model.starttime, model.endtime)

# %% set FlowBoundary / Time

# get node ids
lobith_node_id = (
    model.flow_boundary.node.df.reset_index(drop=False).set_index("meta_meetlocatie_code").at["LOBH", "node_id"]
)
monsin_node_id = (
    model.flow_boundary.node.df.reset_index(drop=False).set_index("meta_meetlocatie_code").at["MONS", "node_id"]
)

# set flow boundary timeseries
lobith_df = pd.DataFrame(lobith_series).reset_index()
lobith_df.loc[:, "node_id"] = lobith_node_id

monsin_df = pd.DataFrame(monsin_series).reset_index()
monsin_df.loc[:, "node_id"] = monsin_node_id

# TODO fill the gaps before so we don't need dropna here
# the resample introduces missing from gaps in the timeseries
bc_time_df = pd.concat([lobith_df, monsin_df]).dropna(subset=["flow_rate"])
model.flow_boundary.time = bc_time_df

# remove fixed values from static
model.flow_boundary.static.df = model.flow_boundary.static.df[
    ~model.flow_boundary.static.df.node_id.isin([lobith_node_id, monsin_node_id])
]

# %% update LevelBoundary / Time

node_ids = model.level_boundary.node.df[
    model.level_boundary.node.df["meta_meetlocatie_code"].isin(["KOBU", "OEBU"])
].index.to_numpy()

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
years = range(model.starttime.year, model.endtime.year + 1)
level_df = pd.DataFrame(
    {
        "time": [pd.Timestamp(f"{year}-{d}") for year in years for d in day_of_year],
        "level": level * len(years),
    }
)
level_df = level_df[(level_df["time"] >= model.starttime) & (level_df["time"] <= model.endtime)]

level_df = pd.concat(
    [pd.concat([level_df, pd.DataFrame({"node_id": [node_id] * len(level_df)})], axis=1) for node_id in node_ids],
    ignore_index=True,
)
model.level_boundary.time.df = level_df

# merge RWZI model
model = merge_rwzi_model(model, rwzi_model_path)

# %%
ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_transient/hws.toml")
model.write(ribasim_toml)

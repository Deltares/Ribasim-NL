"""
Created on Wed Jul  8 13:15:05 2026

@author: kingma
"""

# %%
from pathlib import Path

import xarray as xr

from ribasim_nl import Model

datadir = Path(r"C:\projects\2024\LWKM\06_Ribasim\02_models\LHM\lhm_val_3yr")
model = Model.read(datadir / "lhm_coupled.toml")

# %%
# --- Load node tables and reset index so node_id is a column ---
junction_df = model.junction.node.df.reset_index()
fb_df = model.flow_boundary.node.df.reset_index()

# --- Filter to RWZI-related nodes only ---
junction_rwzi_df = junction_df[junction_df["meta_categorie"] == "RWZI"].copy()
fb_rwzi_df = fb_df[fb_df["meta_categorie"] == "RWZI"].copy()

# --- Match junction name_out to flow_boundary name ---
junction_rwzi_df["name_match"] = junction_rwzi_df["name"].str.removesuffix("_out")

mapping_df = (
    fb_rwzi_df[["node_id", "name"]]
    .rename(columns={"node_id": "from_node_id"})
    .merge(
        junction_rwzi_df[["node_id", "name_match"]].rename(columns={"node_id": "to_node_id"}),
        left_on="name",
        right_on="name_match",
        how="inner",
    )[["from_node_id", "to_node_id", "name"]]
)

print(mapping_df)

# %% Plot the flow from different RWZIs
# --- Select RWZIs to plot by name ---
RWZI_plot = ["BURGUM", "LEEUWARDEN", "ZWOLLE", "HENGELO", "EINDHOVEN"]
RWZI_plot = ["HOOGKERK", "RIJSENHOUT", "GROEDE", "AALST"]  # uit bedrijf in 2017, 2018, 2019, 2020
mapping_selected = mapping_df[mapping_df["name"].isin(RWZI_plot)]
link_plot = list(mapping_selected.itertuples(index=False, name=None))

# --- Load flow results ---
ds_flow = xr.open_dataset(datadir / "results/flow.nc")
df_flow = ds_flow.to_dataframe().reset_index()
df_flow["link"] = list(zip(df_flow.from_node_id, df_flow.to_node_id))
df_flow["flow_m3d"] = df_flow.flow_rate * 86400

link_to_name = {(f, t): name for f, t, name in link_plot}
link_pairs = list(link_to_name.keys())

df_flow_filtered = df_flow[df_flow["link"].isin(link_pairs)].copy()
df_flow_filtered["name"] = df_flow_filtered["link"].map(link_to_name)

# --- Plot ---
ax = df_flow_filtered.pivot_table(index="time", columns="name", values="flow_m3d").plot()
ax.legend(bbox_to_anchor=(1.3, 1), title="RWZI")
ax.set_ylabel("flow [m³day⁻¹]")
ax.set_xlabel("time")

# %% Flow buitenlandse aanvoer
fb_ba_df = fb_df[(fb_df["meta_categorie"] == "buitenlandse aanvoer") | (fb_df["meta_meetlocatie_code"].notna())].copy()

names_plot = ["Roer", "Zoddebeek", "Geul", "Overijsselse Vecht", "Berkel"]  # the specific ones you want
names_plot = ["Monsin", "Lobith"]  # Maas en Rijn

fb_ba_df = fb_ba_df[fb_ba_df["name"].isin(names_plot)]

# mapping from node_id -> name for these flow boundaries
fb_ba_name_map = dict(zip(fb_ba_df["node_id"], fb_ba_df["name"]))

# --- Load flow results ---
ds_flow = xr.open_dataset(datadir / "results/flow.nc")
df_flow = ds_flow.to_dataframe().reset_index()
df_flow["flow_m3d"] = df_flow.flow_rate * 86400

# --- Filter to links originating from these flow boundaries ---
df_flow_filtered = df_flow[df_flow["from_node_id"].isin(fb_ba_name_map.keys())].copy()
df_flow_filtered["name"] = df_flow_filtered["from_node_id"].map(fb_ba_name_map)

# --- Plot ---
ax = df_flow_filtered.pivot_table(index="time", columns="name", values="flow_m3d").plot()
ax.legend(bbox_to_anchor=(1.3, 1), title="Buitenlandse aanvoer")
ax.set_ylabel("flow [m³day⁻¹]")
ax.set_xlabel("time")

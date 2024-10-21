# %%
import json
import pathlib

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from peilbeheerst_model import ParseCrossings

# %%
with open("waterschappen.json") as f:
    waterschap_data = json.load(f)

print_df = {}
for waterschap, waterschap_struct in waterschap_data.items():
    for funcname, func_args in waterschap_struct.items():
        if funcname not in print_df:
            print_df[funcname] = []
        print_df[funcname].append(pd.Series(func_args, name=waterschap))

for funcname, df in print_df.items():
    print(f"Function {funcname}:")
    print(pd.DataFrame(df))

# %%
for waterschap, waterschap_struct in waterschap_data.items():
    print(f"\n{waterschap}...")

    init_settings, crossing_settings = waterschap_struct.values()
    init_settings["logfile"] = pathlib.Path(init_settings["output_path"]).with_suffix("").with_suffix(".log")

    if waterschap not in ["HHNK"]:
        continue

    # if pathlib.Path(init_settings["output_path"]).exists() and "crossings_hydroobject" in fiona.listlayers(init_settings["output_path"]):
    #     continue

    # Crossings class initializeren
    cross = ParseCrossings(**init_settings)

    # Crossings bepalen en wegschrijven
    if crossing_settings["filterlayer"] is None:
        df_hydro = cross.find_crossings_with_peilgebieden("hydroobject", **crossing_settings)
        cross.write_crossings(df_hydro)
    else:
        df_hydro, df_dsf, df_hydro_dsf = cross.find_crossings_with_peilgebieden("hydroobject", **crossing_settings)
        cross.write_crossings(df_hydro, crossing_settings["filterlayer"], df_dsf, df_hydro_dsf)

# %%
plt.close("all")
fig1, ax1 = plt.subplots(figsize=(12, 7.4), dpi=100)
fig2, ax2 = plt.subplots(figsize=(12, 7.4), dpi=100)

for ax in [ax1, ax2]:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color("#dddddd")
    ax.tick_params(bottom=False, left=False)
    ax.yaxis.grid(True, color="#eeeeee")
    ax.xaxis.grid(False)

waterschappen = []
network_results = {"Basins": [], "Edges": [], "Peilgebieden": []}
# reduction_results = {"initial": [], "in_use": [], "agg_links_in_use": [], "agg_areas_in_use": []}
reduction_results = {"in_use": [], "agg_links_in_use": [], "agg_areas_in_use": []}
for waterschap, waterschap_struct in waterschap_data.items():
    init_settings, crossing_settings = waterschap_struct.values()
    df = gpd.read_file(init_settings["output_path"], layer="crossings_hydroobject_filtered")

    basins, edges, peilgebieden = None, None, None
    init_cross, cross_inuse, cross_agglinks, cross_aggareas = None, None, None, None
    try:
        sub_df = df[df.agg_areas_in_use].copy()
        all_nodes = np.hstack([sub_df.agg_area_from.to_numpy(), sub_df.agg_area_to.to_numpy()])
        basins = len(np.unique(all_nodes[~pd.isna(all_nodes)]))
        edges = len(sub_df) * 2
        all_peilgebieden = np.hstack([sub_df.peilgebied_from.to_numpy(), sub_df.peilgebied_to.to_numpy()])
        peilgebieden = len(np.unique(all_peilgebieden[~pd.isna(all_peilgebieden)]))

        init_cross = len(df)
        cross_inuse = len(df[df.in_use])
        cross_agglinks = len(df[df.agg_links_in_use])
        cross_aggareas = len(df[df.agg_areas_in_use])
    except Exception as e:
        print(f"{waterschap=}, {e=}")

    # reduction_results["initial"].append(init_cross)
    reduction_results["in_use"].append(cross_inuse)
    reduction_results["agg_links_in_use"].append(cross_agglinks)
    reduction_results["agg_areas_in_use"].append(cross_aggareas)

    network_results["Basins"].append(basins)
    network_results["Edges"].append(edges)
    network_results["Peilgebieden"].append(peilgebieden)
    waterschappen.append(waterschap)

colours = ["#0C3B5D", "#3EC1CD", "#EF3A4C", "#FCB94D"]

x1 = np.arange(len(waterschappen))
width = 1 / (1 + len(network_results))
multiplier = 0
for multiplier, (attribute, measurement) in enumerate(network_results.items()):
    offset = width * multiplier
    rects = ax1.bar(x1 + offset, measurement, width, label=attribute, color=colours[multiplier])
    # ax1.bar_label(rects, padding=3)
ax1.set_axisbelow(True)
ax1.set_xticks(x1 + width, waterschappen, rotation=45)
ax1.legend(loc="upper left", ncols=len(network_results))


x2 = np.arange(len(waterschappen))
width = 1 / (1 + len(reduction_results))
for multiplier, (attribute, measurement) in enumerate(reduction_results.items()):
    offset = width * multiplier
    rects = ax2.bar(x2 + offset, measurement, width, label=attribute, color=colours[multiplier])
    # ax2.bar_label(rects, padding=3)
ax2.set_axisbelow(True)
ax2.set_xticks(x2 + width, waterschappen, rotation=45)
ax2.legend(loc="upper left", ncols=len(reduction_results))

fig1.tight_layout()
fig2.tight_layout()

fig1.savefig("network_results.jpeg", bbox_inches="tight")
fig2.savefig("reduction_results.jpeg", bbox_inches="tight")

print(pd.DataFrame(reduction_results, index=waterschappen))
print(pd.DataFrame(network_results, index=waterschappen))

from pathlib import Path

import fiona
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from ribasim_lumping.utils.general_functions import remove_holes_from_polygons
from shapely.geometry import Polygon

# locatie van de waterschapsmappen
base_dir = "../Ribasim modeldata/"

# creeer een lijst met alle namen van de waterschappen
waterschappen = {
    "Noorderzijlvest": "Noorderzijlvest",
    "HunzeenAas": "Hunze en Aa's",
    "DrentsOverijsselseDelta": "Drents Overijsselse Delta",
    "Vechtstromen": "Vechtstromen",
    "RijnenIJssel": "Rijn en IJssel",
    "ValleienVeluwe": "Vallei en Veluwe",
    "StichtseRijnlanden": "De Stichtse Rijnlanden",
    "BrabantseDelta": "Brabantse Delta",
    "DeDommel": "De Dommel",
    "AaenMaas": "Aa en Maas",
    "Limburg": "Limburg",
}

# lijst met de benodigde layers
layers = {
    "basins": "ribasim_network.gpkg",
    "basin_areas": "ribasim_network.gpkg",
    "split_nodes": "ribasim_network.gpkg",
    "boundaries": "ribasim_network.gpkg",
    "boundary_connections": "ribasim_network.gpkg",
    "basin_connections": "ribasim_network.gpkg",
    "areas": "areas.gpkg",
    "drainage_areas": "areas.gpkg",
    "foreign_drainage_areas": "foreign_input.gpkg",
    # "gemaal": "hydamo.gpkg",
    # "stuw": "hydamo.gpkg",
    # "onderdoorlaat": "hydamo.gpkg",
    # "afsluitmiddel": "hydamo.gpkg",
    # "duikersifonhevel": "hydamo.gpkg",
    # "hydroobject": "hydamo.gpkg",
}

output_gpkg = "data//alle_waterschappen.gpkg"
# output_gpkg = "data//foreign_input.gpkg"


# waterschappen_geoms = gpd.read_file("data_oud//waterschappen.gpkg").to_crs(28992)
waterschappen_labels = list(waterschappen.keys())


split_nodes = gpd.read_file(
    Path(base_dir, list(waterschappen.keys())[1], "verwerkt", "4_ribasim", layers["split_nodes"])
)


# loop door de verschillende shapefiles die je wilt hebben per waterschap
for layer, gpkg_file in layers.items():
    print(layer)
    layer_totaal = None
    # loop door de directories van de waterschappen
    print(" - ", end="")
    for i, waterschap in enumerate(waterschappen):
        print(waterschap[:3], end=" ")
        # maak de directory
        locatie_gpkg = Path(base_dir, waterschap, "verwerkt", "4_ribasim", gpkg_file)
        if not locatie_gpkg.exists():
            continue
        if layer not in fiona.listlayers(locatie_gpkg):
            continue

        # read the shapefile layers
        layer_data = gpd.read_file(locatie_gpkg, layer=layer, engine="pyogrio")
        if layer == "areas":
            layer_data = layer_data[["code", "geometry"]]
        if layer == "foreign_drainage_areas":
            layer_data = layer_data[["name", "boundary_name", "geometry"]]
        if layer in [
            "drainage_areas",
            "gemaal",
            "stuw",
            "afsluitmiddel",
            "onderdoorlaat",
            "duikersifonhevel",
            "hydroobject",
        ]:
            if "code" not in layer_data.columns:
                layer_data["code"] = None
            layer_data = layer_data[["code", "geometry"]]

        # add waterschap name
        layer_data["waterschap"] = waterschap

        layer_data = layer_data.set_crs(28992, allow_override=True)

        if layer_totaal is None:
            layer_totaal = layer_data.copy()
        else:
            layer_totaal = pd.concat([layer_totaal, layer_data])

    if layer_totaal is not None:
        layer_totaal.to_file(output_gpkg, layer=layer, driver="GPKG")
        print(" -> saved")


# ### Plots


# load the data
areas = gpd.read_file(output_gpkg, layer="areas")
basins = gpd.read_file(output_gpkg, layer="basins")
basin_areas = gpd.read_file(output_gpkg, layer="basin_areas")
split_nodes = gpd.read_file(output_gpkg, layer="split_nodes")
boundaries = gpd.read_file(output_gpkg, layer="boundaries")

drainage_areas = gpd.read_file(output_gpkg, layer="drainage_areas")
foreign_drainage_areas = gpd.read_file(output_gpkg, layer="foreign_drainage_areas")
gemaal = gpd.read_file(output_gpkg, layer="gemaal")
stuw = gpd.read_file(output_gpkg, layer="stuw")
onderdoorlaat = gpd.read_file(output_gpkg, layer="onderdoorlaat")
afsluitmiddel = gpd.read_file(output_gpkg, layer="afsluitmiddel")
duikersifonhevel = gpd.read_file(output_gpkg, layer="duikersifonhevel")
hydroobject = gpd.read_file(output_gpkg, layer="hydroobject")


# BOUNDARIES: FILL TYPE
boundaries["Type"] = (
    boundaries["Type"]
    .fillna(boundaries["quantity"])
    .replace({"dischargebnd": "FlowBoundary", "waterlevelbnd": "LevelBoundary"})
)
# CHECK BOUNDARIES
boundaries[["Type", "quantity", "waterschap"]].fillna("").groupby(
    ["Type", "quantity", "waterschap"]
).size().reset_index()  # .rename(columns={0:'count'})
boundaries.to_file(output_gpkg, layer="boundaries")
# SEPARATE FLOW AND LEVEL BOUNDARIES
flow_boundaries = boundaries[boundaries["Type"] == "FlowBoundary"]
level_boundaries = boundaries[boundaries["Type"] == "LevelBoundary"]


# BASIN AREAS
basin_areas_waterschap = areas.dissolve(by=["waterschap", "basin_node_id"])
basin_areas_waterschap.area = basin_areas_waterschap.geometry.area
rng = np.random.default_rng()
basin_areas_waterschap["color_no"] = rng.choice(np.arange(50), size=len(basin_areas_waterschap))


basin_areas_waterschap = remove_holes_from_polygons(basin_areas_waterschap.explode(), 100_000)


basin_areas_waterschap.to_file(output_gpkg, layer="basin_areas")


basin_areas_waterschap.reset_index().to_file(output_gpkg, layer="basin_areas")


rng = np.random.default_rng()
basin_areas_waterschap["color_no"] = rng.choice(np.arange(50), size=len(basin_areas_waterschap))


# BASIN AREAS
fig, ax = plt.subplots()
basin_areas_waterschap.reset_index(drop=True).plot(ax=ax, column="color_no")
waterschappen.plot(ax=ax, facecolor="None")


# CALCULATE SURFACE AREA OF WATER BOARDS
areas["area"] = areas.geometry.area / 1_000_000
areas[["area", "waterschap"]].groupby("waterschap").sum()


# PLOT FOR SURFACE AREA, BOUNDARIES, SPLIT NODES, BASINS, BASIN AREAS


def addlabels(ax, x, y):
    for _x, _y in zip(x, y):
        ax.text(_x, _y, _y, ha="center", va="bottom", fontsize=7)


# make the plots
fig, axs = plt.subplots(4, 1, figsize=(5, 7), sharex=True, gridspec_kw={"hspace": 0.25, "wspace": 0.3})
# fig.tight_layout()

data_sets = [boundaries, split_nodes, basins, basin_areas]
columns = ["Boundaries", "Split nodes", "Basins", "Basin areas"]
data_labels = ["Boundaries", "Split nodes", "Basins", "Basin areas"]

for data_set, data_label, ax in zip(data_sets, data_labels, axs.flatten()):
    labels, counts = np.unique(data_set.waterschap, return_counts=True)
    counts_def = []
    for w_lab in waterschappen.keys():
        counts_new = 0
        for label, count in zip(labels, counts):
            if label == w_lab:
                counts_new = count
        counts_def += [counts_new]
    ax.bar(waterschappen.values, counts_def, align="center")
    addlabels(ax, waterschappen.values, counts_def)
    ax.set_ylim([0, max(counts_def) * 1.2])
    ax.set_title(data_label, fontsize=10, ha="left", x=-0.1, fontweight="bold")
    ax.tick_params(axis="x", which="major", labelsize=10)
    ax.tick_params(axis="y", which="major", labelsize=9)

basin_areas.area = basin_areas.geometry.area
basin_areas["area_km2"] = basin_areas.geometry.area / 1000000
# basin_areas[basin_areas.waterschap=="Noorderzijlvest", "color_no"] =

ax = axs[-1]  # [-1]
# basin_areas_km2 = basin_areas[["waterschap", "area_km2"]].groupby("waterschap").sum().area_km2
# ax.bar(basin_areas_km2.index, basin_areas_km2.values, align='center')
# addlabels(ax, basin_areas_km2.index, basin_areas_km2.round(0).values)#((basin_areas_km2/1000).round(0)*1000.0).values)
# ax.set_ylim([0, basin_areas_km2.max()*1.2])
# ax.set_ylabel("area [km2]")
ax.tick_params(axis="x", labelrotation=90)
ax.set_xticklabels(waterschappen.values)


# PLOT FOR PUMPS, WEIRS, CULVERTS, HYDROOBJECTS

# make the plots
fig, axs = plt.subplots(4, 1, figsize=(5, 7), sharex=True, gridspec_kw={"hspace": 0.25, "wspace": 0.3})
fig.tight_layout()

waterschap_areas = areas[["area", "waterschap"]].groupby("waterschap").sum()
counts_def = []
for w_lab in waterschappen.keys():
    counts_new = 0
    for label, count in zip(waterschap_areas.index, waterschap_areas.area.round(0).values):
        if label == w_lab:
            counts_new = count
    counts_def += [int(counts_new)]
axs[0].bar(waterschappen_labels, counts_def, align="center")
addlabels(axs[0], waterschappen_labels, counts_def)
axs[0].set_ylim([0, max(counts_def) * 1.2])
axs[0].set_title("Surface area [km2]", fontsize=10, ha="left", x=-0.1, fontweight="bold")
axs[0].tick_params(axis="x", which="major", labelsize=10)
axs[0].tick_params(axis="y", which="major", labelsize=9)

hydroobject["length"] = hydroobject.geometry.length / 1000
hydroobject_length = hydroobject[["length", "waterschap"]].groupby("waterschap").sum()
counts_def = []
for w_lab in waterschappen.keys():
    counts_new = 0
    for label, count in zip(hydroobject_length.index, hydroobject_length.length.round(0).values):
        if label == w_lab:
            counts_new = count
    counts_def += [int(counts_new)]
axs[1].bar(waterschappen_labels, counts_def, align="center")
addlabels(axs[1], waterschappen_labels, counts_def)
axs[1].set_ylim([0, max(counts_def) * 1.2])
axs[1].set_title("Hydro-objects [km]", fontsize=10, ha="left", x=-0.1, fontweight="bold")
axs[1].tick_params(axis="x", which="major", labelsize=10)
axs[1].tick_params(axis="y", which="major", labelsize=9)

afsluitmiddel = pd.concat([afsluitmiddel, onderdoorlaat])

data_sets = [gemaal, stuw]
columns = ["Gemaal", "Stuw"]
data_labels = ["Pumping stations", "Weirs"]


def addlabels(ax, x, y):
    for _x, _y in zip(x, y):
        ax.text(_x, _y, _y, ha="center", va="bottom", fontsize=7)


for data_set, data_label, ax in zip(data_sets, data_labels, axs.flatten()[2:]):
    labels, counts = np.unique(data_set.waterschap, return_counts=True)
    counts_def = []
    for w_lab in waterschappen.keys():
        counts_new = 0
        for label, count in zip(labels, counts):
            if label == w_lab:
                counts_new = count
        counts_def += [int(counts_new)]
    ax.bar(waterschappen_labels, counts_def, align="center")
    addlabels(ax, waterschappen_labels, counts_def)
    ax.set_ylim([0, max(counts_def) * 1.2])
    ax.set_title(data_label, fontsize=10, ha="left", x=-0.1, fontweight="bold")
    ax.tick_params(axis="x", which="major", labelsize=10)
    ax.tick_params(axis="y", which="major", labelsize=9)

basin_areas.area = basin_areas.geometry.area
basin_areas["area_km2"] = basin_areas.geometry.area / 1000000

ax = axs[-1]  # [-1]
ax.tick_params(axis="x", labelrotation=90)
ax.set_xticklabels(waterschappen_labels)


def remove_small_holes_from_areas(gdf, min_area):
    list_geometry = []
    for polygon in gdf.geometry:
        list_interiors = []
        for interior in polygon.interiors:
            p = Polygon(interior)
            if p.area > min_area:
                list_interiors.append(interior)
        temp_pol = Polygon(polygon.exterior.coords, holes=list_interiors)
        list_geometry.append(temp_pol)
    gdf.geometry = list_geometry
    return gdf


drainage_areas = remove_small_holes_from_areas(drainage_areas, 1000.0)


drainage_areas.to_file(Path(base_dir, "areas.gpkg"), layer="drainage_areas", driver="GPKG")

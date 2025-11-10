# %%
import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage

"""
General method used by Kadaster:
- dissolve top10NL layer to naam to an internal (not shared) layer
- read rws waterlines schaallabe per scale
- find intersecting polygons within scale lines
- add other polygons that are large of droogvallend

# Names
standard dissolving doesn't work, e.g.:
- Noordervaart exists in Brabant and Noord Holland.
- Names in Fryslan are scattered

- We make a custom dissolve using this trick: https://stackoverflow.com/questions/67280722/how-to-merge-touching-polygons-with-geopandas
- We keep "fysiekvoorkomen" == "in sluis" separated to avoid problems at Hollandse IJssel (seperate at Gouda) and Overijsselskanaal (seperate at Vecht)

"""


cloud = CloudStorage()


RE_DISSOLVE = False
MAP_SLUIZEN = {"Reevesluis": "Drontermeer"}

top10_nl_water = cloud.joinpath("Basisgegevens/Top10NL/top10nl_Compleet.gpkg")


watervlak_gpkg = cloud.joinpath("Rijkswaterstaat/Verwerkt/categorie_oppervlaktewater.gpkg")


result_gpkg = cloud.joinpath("Rijkswaterstaat/Verwerkt/categorie_oppervlaktewater.gpkg")

print("read")
watervlak_gdf = gpd.read_file(top10_nl_water, layer="top10nl_waterdeel_vlak", engine="pyogrio", fid_as_index=True)

rws_waterlijn_gdf = gpd.read_file(result_gpkg, layer="waterlijnen_rws", engine="pyogrio", fid_as_index=True)

recategorize_xlsx = cloud.joinpath("Rijkswaterstaat/verwerkt/recategorize.xlsx")

# %% generate watervlak_diss_gdf

if RE_DISSOLVE:
    # merge all name-categories
    print("merge name-fields")
    watervlak_gdf.loc[:, ["naam"]] = watervlak_gdf["naamofficieel"]
    watervlak_gdf.loc[watervlak_gdf.naam.isna(), ["naam"]] = watervlak_gdf[watervlak_gdf.naam.isna()]["naamnl"]
    watervlak_gdf.loc[watervlak_gdf.naam.isna(), ["naam"]] = watervlak_gdf[watervlak_gdf.naam.isna()]["naamfries"]

    print("name sluizen")
    for k, v in MAP_SLUIZEN.items():
        watervlak_gdf.loc[watervlak_gdf.sluisnaam == k, ["naam"]] = v

    exclude_names_df = pd.read_excel(recategorize_xlsx, sheet_name="TOP10NL_EXCLUDE_NAMES")

    watervlak_gdf = watervlak_gdf.loc[~watervlak_gdf.naam.isin(exclude_names_df["naam"])]

    print("dissolve to naam, fysiekvoorkomen and hoofdafwatering")
    data = {"naam": [], "geometry": [], "fysiekvoorkomen": []}

    # fill fysiekvoorkomen so polygons don't get dropped
    watervlak_gdf.loc[watervlak_gdf["fysiekvoorkomen"].isna(), "fysiekvoorkomen"] = "overig"

    # custom dissolve section. We dissolve only adjacent polygons with the same name and not over sluices, bridges etc (fysiekvoorkomen)
    for (name, voorkomen), df in watervlak_gdf.groupby(by=["naam", "fysiekvoorkomen"]):
        # dissolve touching polygons (magic!)
        geometry = df.geometry.buffer(0.1).union_all().buffer(-0.1)
        # make sure we have a list of single polygons
        if isinstance(geometry, MultiPolygon):
            geometries = list(geometry.geoms)
        else:
            geometries = [geometry]  # that is 1 Polygon
        # add to data
        data["geometry"] += geometries
        data["naam"] += [name] * len(geometries)
        data["fysiekvoorkomen"] += [voorkomen] * len(geometries)

    # concat with name is NaN (dropped by groupby)
    watervlak_diss_gdf = pd.concat(
        [
            gpd.GeoDataFrame(data, crs=28992),
            watervlak_gdf[watervlak_gdf.naam.isna()][["naam", "fysiekvoorkomen", "geometry"]],
        ]
    ).reset_index(drop=True)

    # fix index and prepare categorie column
    watervlak_diss_gdf.index += 1
    watervlak_diss_gdf.name = "fid"
    watervlak_diss_gdf.loc[:, ["categorie"]] = None

    print("write")
    watervlak_diss_gdf.to_file(result_gpkg, layer="watervlak", engine="pyogrio")
else:
    print("read")
    watervlak_diss_gdf = gpd.read_file(result_gpkg, layer="watervlak", engine="pyogrio", fid_as_index=True)
    # reset categorie column
    watervlak_diss_gdf.loc[:, ["categorie"]] = None

# %% Categorize Nationaal Hoofdwater
print("nationaal hoofdwater")
overlap = 0.05
min_length = 240
category = "nationaal hoofdwater"
ignore_min_length = [
    "Noordervaart",
    "Haringvliet",
    "Voorhaven noordzijde Rozenburgsesluis, Calandkanaal",
    "Calandkanaal",
    "Veluwemeer",
]

# Select lines
# should have scale higher than lower limit
mask = rws_waterlijn_gdf["categorie"] == category
# we only accept lines of minimal length
mask = mask & (rws_waterlijn_gdf.length > min_length)

waterlijn_select_gdf = rws_waterlijn_gdf[mask]

for row in waterlijn_select_gdf.itertuples():
    # get row indexes by spatial index and geometric selection
    idx = watervlak_diss_gdf.sindex.intersection(row.geometry.bounds)
    idx = watervlak_diss_gdf.iloc[idx][watervlak_diss_gdf.iloc[idx].intersects(row.geometry)].index

    if row.Label not in ignore_min_length:
        # should have sigificant overlap with polygon
        mask = watervlak_diss_gdf.loc[idx].intersection(row.geometry).length > (row.geometry.length * overlap)

        # or not being "overig" (sluis, bridge, etc)
        mask = mask | (watervlak_diss_gdf.loc[idx].fysiekvoorkomen != "overig")

        # if category is set, we leave it as is
        mask = mask & (watervlak_diss_gdf.loc[idx].categorie.isna())
    else:
        mask = watervlak_diss_gdf.loc[idx].categorie.isna()

    # write category
    category_idx = watervlak_diss_gdf.loc[idx][mask].index
    watervlak_diss_gdf.loc[category_idx, ["categorie"]] = category

    # give rws-label if Top10NL name is None
    if row.Bron == "NWB Vaarwegen":
        name_idx = watervlak_diss_gdf.loc[idx][watervlak_diss_gdf.loc[idx].naam.isna()].index
        watervlak_diss_gdf.loc[name_idx, ["naam"]] = row.Label

# Recategorize for
# Some mess in RWS-lijnen just ds Eijsden
# Zuid-Willemsvaart at border near Maastricht
reset_fids = [10904, 15254]
reset_names = ["Pietersplas"]
# re-categorize nonames, due to some mess in RWS-lijnen just ds Eijsden
watervlak_diss_gdf.loc[watervlak_diss_gdf.naam.isna(), ["categorie"]] = None
watervlak_diss_gdf.loc[watervlak_diss_gdf.index.isin(reset_fids), ["categorie"]] = None
watervlak_diss_gdf.loc[watervlak_diss_gdf.naam.isin(reset_names), ["categorie"]] = None

to_nationaal_hw_df = pd.read_excel(recategorize_xlsx, sheet_name="TOP10NL_TO_NATIONAL_HW")

watervlak_diss_gdf.loc[watervlak_diss_gdf.naam.isin(to_nationaal_hw_df["naam"]), ["categorie"]] = "nationaal hoofdwater"

# %% Write to disk
print("write")
watervlak_diss_gdf.to_file(result_gpkg, layer="watervlak", engine="pyogrio")

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage

cloud = CloudStorage()

# Ensure the directory for post_processed_path exists
authority = "WetterskipFryslan"
post_processed_dir = Path(cloud.joinpath(authority, "verwerkt/Data_postprocessed"))
os.makedirs(post_processed_dir, exist_ok=True)

# Define the full path for the GeoPackage file
post_processed_path = post_processed_dir / f"{authority}.gpkg"

data_delivery = cloud.joinpath(
    "WetterskipFryslan",
    "aangeleverd",
    "Na_levering",
    "20250404_nieuwe_schematisatie",
    "beheerregister_WF.gdb",
    "beheerregister_WF.gdb",
)

cloud.synchronize(filepaths=[data_delivery])


def read_gpkg_layers(gpkg_path, engine="pyogrio", print_var=False):
    data = {}
    layers = gpd.list_layers(gpkg_path)
    for layer in layers.name:
        if print_var:
            print(layer)
        data_temp = gpd.read_file(gpkg_path, layer=layer, engine=engine)
        data[layer] = data_temp

    return data


def store_data(waterschap, output_gpkg_path):
    for key in waterschap.keys():
        waterschap[str(key)].to_file(output_gpkg_path, layer=str(key), driver="GPKG")


Wetterskip = read_gpkg_layers(gpkg_path=data_delivery)

Wetterskip["peilgebied"] = Wetterskip["peilenbeheerkaart"][
    ["PBHIDENT", "HOOGPEIL", "LAAGPEIL", "WATERSYSTEEM", "ORDECODE", "GLOBALID", "geometry"]
]
Wetterskip["peilgebied"] = Wetterskip["peilgebied"].rename(
    columns={
        "PBHIDENT": "meta_name",
        "HOOGPEIL": "waterhoogte",
        "LAAGPEIL": "meta_laag_peil",
        "WATERSYSTEEM": "polder",
        "GLOBALID": "globalid",
    }
)

Wetterskip["peilgebied"]["code"] = Wetterskip["peilgebied"]["meta_name"].copy()

# change names for correct aggregation
print(len(Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder == "Boezem"]))
Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder == "Boezemland", "polder"] = "Boezem_to_merge"
Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder == "Boezemland beheerst", "polder"] = "Boezem_to_merge"
Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder == "Boezem beheerst", "polder"] = "Boezem_to_merge"
Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder == "Particulier", "polder"] = "Boezem_to_merge"

# identify boezem areas which are tiny. They can be merged as well
Wetterskip["peilgebied"]["area"] = Wetterskip["peilgebied"].area
Wetterskip["peilgebied"].loc[
    (Wetterskip["peilgebied"].area < 100000) & (Wetterskip["peilgebied"].polder == "Boezem"), "polder"
] = "Boezem_to_merge"

# add a number to the Vrijafwaterendegebieden, as they should not be aggregated
boezem_idx = Wetterskip["peilgebied"].polder == "Vrij afwaterend"
Wetterskip["peilgebied"].loc[boezem_idx, "polder"] = "Vrij_afwaterend_" + Wetterskip["peilgebied"].index[
    boezem_idx
].astype(str)

boezem_idx = Wetterskip["peilgebied"].polder == "Vrijafstromend"
Wetterskip["peilgebied"].loc[boezem_idx, "polder"] = "Vrijafstromend_" + Wetterskip["peilgebied"].index[
    boezem_idx
].astype(str)

print(len(Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder == "Boezem"]))

# isolate the boezem (place it back after the aggregation)
boezem = Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder == "Boezem"]
boezem_to_merge = Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder == "Boezem_to_merge"]

# remove from the peilgebieden (will be added later again)
Wetterskip["peilgebied"] = Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder != "Boezem"]
Wetterskip["peilgebied"] = Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].polder != "Boezem_to_merge"]

# aggregate based on the polder
Wetterskip["aggregation_area"] = Wetterskip["peilgebied"].copy()
Wetterskip["aggregation_area"] = Wetterskip["aggregation_area"].dissolve(by="polder", as_index=False, dropna=False)

# place the boezem back in the aggregation areas. A number will be added to it later to have unique values
Wetterskip["aggregation_area"] = gpd.GeoDataFrame(pd.concat([Wetterskip["aggregation_area"], boezem]))
Wetterskip["aggregation_area"] = gpd.GeoDataFrame(pd.concat([Wetterskip["aggregation_area"], boezem_to_merge]))

# add the boezem back in the peilgebieden
Wetterskip["peilgebied"] = gpd.GeoDataFrame(pd.concat([Wetterskip["peilgebied"], boezem]))
Wetterskip["peilgebied"] = gpd.GeoDataFrame(pd.concat([Wetterskip["peilgebied"], boezem_to_merge]))

# merge the tiny Boezem_to_merge polygons with the existing boezem
# Original layers
gdf = Wetterskip["aggregation_area"]
boezem = gdf[gdf["polder"] == "Boezem"].copy()
to_merge = gdf[gdf["polder"] == "Boezem_to_merge"].copy()
others = gdf[~gdf["polder"].isin(["Boezem", "Boezem_to_merge"])].copy()

merged_boezems = []

# For each boezem_to_merge, find ONE boezem it touches
for idx, row in to_merge.iterrows():
    # Create a temporary buffer to enlarge geometry slightly
    buffered_geom = row.geometry.buffer(1)

    # Find boezem polygons that touch within the buffer tolerance
    touching = boezem[boezem.geometry.intersects(buffered_geom)]

    if not touching.empty:
        # Merge with the first touching boezem only
        boezem_geom = touching.iloc[0].geometry
        merged_geom = boezem_geom.union(row.geometry)

        # Replace old boezem geometry
        boezem_idx = touching.index[0]
        boezem.at[boezem_idx, "geometry"] = merged_geom
    else:
        # No merge possible, keep as-is
        merged_boezems.append(row)

print("Following boezems have not been assigned:")

# Combine results
merged_gdf = pd.concat(
    [
        boezem,
        others,
    ]
).reset_index(drop=True)

Wetterskip["aggregation_area"] = merged_gdf.drop(columns="area")

# #add a number to the boezems, as they should not be aggregated but should have an unique number
boezem_idx = Wetterskip["aggregation_area"].polder == "Boezem"
Wetterskip["aggregation_area"].loc[boezem_idx, "Boezem"] = "Boezem_" + Wetterskip["aggregation_area"].index[
    boezem_idx
].astype(str)

# define boezem areas
Wetterskip["peilgebied"]["peilgebied_cat"] = 0  # 0 = regular basin
Wetterskip["peilgebied"].loc[
    Wetterskip["peilgebied"]["polder"].str.lower().str.contains("boezem", na=False), "peilgebied_cat"
] = 1  # 1 = boezem
Wetterskip["peilgebied"] = Wetterskip["peilgebied"].to_crs(crs="EPSG:28992")

# add streefpeilen
Wetterskip["streefpeil"] = Wetterskip["peilgebied"][["globalid", "waterhoogte", "geometry"]].copy()
Wetterskip["streefpeil"]["geometry"] = None

# combine duikers and sifons to duikersifonhevel, and add to the hydroobjecten
Wetterskip["duikersifonhevel"] = gpd.GeoDataFrame(pd.concat([Wetterskip["duikers"], Wetterskip["sifons"]]))
Wetterskip["duikers"] = Wetterskip["duikers"][["GLOBALID", "IDENT_NW", "geometry"]]
Wetterskip["duikers"] = Wetterskip["duikers"].rename(columns={"GLOBALID": "globalid", "IDENT_NW": "code"})
Wetterskip["duikers"]["nen3610id"] = "dummy_nen3610id_duiker_" + Wetterskip["duikers"].index.astype(str)

Wetterskip["sifons"] = Wetterskip["sifons"][["GLOBALID", "KSYIDENT", "geometry"]]
Wetterskip["sifons"] = Wetterskip["sifons"].rename(columns={"GLOBALID": "globalid", "KSYIDENT": "code"})
Wetterskip["sifons"]["nen3610id"] = "dummy_nen3610id_duiker_" + Wetterskip["sifons"].index.astype(str)

Wetterskip["duikersifonhevel"] = pd.concat([Wetterskip["duikers"], Wetterskip["sifons"]])

# prevent duplicate values
gdf = Wetterskip["duikersifonhevel"].copy()
for col in ["globalid", "code"]:
    # Convert None/NaN to string "None" to handle them uniformly
    gdf[col] = gdf[col].fillna("None").astype(str)

    # Identify duplicates
    duplicated = gdf[col].duplicated(keep=False)

    # Create suffixes only for duplicated values
    suffixes = (
        gdf.loc[duplicated, col]
        .groupby(gdf.loc[duplicated, col])
        .cumcount()
        .astype(str)
        .radd("_")
        .replace("_0", "")  # Don't suffix the first occurrence
    )

    gdf.loc[duplicated, col] += suffixes

Wetterskip["duikersifonhevel"] = gdf

# add hydroobjecten
Wetterskip["hydroobject"] = Wetterskip["watergangen"][["OVKIDENT", "GLOBALID", "geometry"]]
Wetterskip["hydroobject"] = Wetterskip["hydroobject"].rename(columns={"GLOBALID": "globalid", "OVKIDENT": "code"})
Wetterskip["hydroobject"]["nen3610id"] = "dummy_nen3610id_hydroobject_" + Wetterskip["hydroobject"].index.astype(str)
Wetterskip["hydroobject"] = gpd.GeoDataFrame(pd.concat([Wetterskip["hydroobject"], Wetterskip["duikersifonhevel"]]))
dup_mask = Wetterskip["hydroobject"]["code"].duplicated(keep=False) | Wetterskip["hydroobject"]["code"].isna()
Wetterskip["hydroobject"].loc[dup_mask, "code"] = (
    Wetterskip["hydroobject"].loc[dup_mask, "code"].astype(str)
    + "_"
    + Wetterskip["hydroobject"].loc[dup_mask].index.astype(str)
)

# Find duplicated code values
df = Wetterskip["hydroobject"]
dupes = df["code"].duplicated()
df.loc[dupes, "code"] = df.loc[dupes, "code"].fillna("None").astype(str) + "_" + df.loc[dupes].index.astype(str)
Wetterskip["hydroobject"] = df

# add gemalen
Wetterskip["gemaal"] = Wetterskip["gemalen"][
    ["KWKNAAM", "KGMIDENT", "IDENT_NW", "GLOBALID", "KGMFUNC", "KGMMACAP", "geometry"]
]
Wetterskip["gemaal"] = Wetterskip["gemaal"].rename(
    columns={
        "GLOBALID": "globalid",
        "IDENT_NW": "nen3610id",
        "KWKNAAM": "meta_name",
        "KGMIDENT": "code",
        "KGMFUNC": "functie",
        "KGMMACAP": "meta_capaciteit",
    }
)

# determine the function of the gemalen
Wetterskip["gemaal"].functie = Wetterskip["gemaal"].functie.astype(int)
Wetterskip["gemaal"].loc[Wetterskip["gemaal"].functie.isin([2, 4, 5, 6, 7, 99]), "func_afvoer"] = True
Wetterskip["gemaal"].loc[Wetterskip["gemaal"].functie.isin([1, 3, 5, 7]), "func_aanvoer"] = True
Wetterskip["gemaal"].loc[Wetterskip["gemaal"].functie == 8, "func_circulatie"] = True
Wetterskip["gemaal"] = Wetterskip["gemaal"].drop(columns="functie")

cols = ["func_aanvoer", "func_afvoer", "func_circulatie"]
for col in cols:  # fill the columns with False if None
    Wetterskip["gemaal"][col] = Wetterskip["gemaal"][col].fillna(False).astype(bool)

# add stuwen
Wetterskip["stuw"] = Wetterskip["stuwen"][["IDENT_NW", "GLOBALID", "geometry"]]
Wetterskip["stuw"] = Wetterskip["stuw"].rename(columns={"GLOBALID": "globalid", "IDENT_NW": "code"})
Wetterskip["stuw"]["nen3610id"] = "dummy_nen3610id_stuw_" + Wetterskip["stuw"].index.astype(str)
Wetterskip["stuw"]["code"] = Wetterskip["stuw"]["code"].fillna("Code_onbekend_").astype(str)
Wetterskip["stuw"].loc[Wetterskip["stuw"]["code"] == "Code_onbekend_", "code"] = Wetterskip["stuw"][
    "code"
] + Wetterskip["stuw"].index.astype(str)

# only select relevant keys
Wetterskip = {
    k: Wetterskip[k]
    for k in ["aggregation_area", "peilgebied", "streefpeil", "hydroobject", "duikersifonhevel", "stuw", "gemaal"]
}


# test whether the codes and globalids are unique
variables = ["aggregation_area", "peilgebied", "stuw", "gemaal", "duikersifonhevel", "hydroobject"]

for var in variables:
    temp = Wetterskip[var]

    for column in temp.keys():
        if column in ["globalid", "code"]:
            temp_column = temp[column]

            if temp_column.duplicated().any():
                print("variable =", var)
                print("column =", column)
                print(temp.loc[temp_column.duplicated(keep=False), [column]])
                print()

# Write the geopackage locally
store_data(waterschap=Wetterskip, output_gpkg_path=post_processed_path)

# ensure the parent directory exists on the cloud
post_processed_path_dir = Path(post_processed_path).parent  # Get the parent directory of the file
cloud.upload_content(dir_path=post_processed_path_dir, overwrite=True)

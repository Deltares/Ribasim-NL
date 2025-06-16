# %%
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage

cloud = CloudStorage()
dir = cloud.joinpath("Basisgegevens", "BGT")

waterdeel_df = gpd.read_file(dir / "bgt_waterdeel_current.gpkg")

# %% [markdown]

# ## Verdeelpunten
# We lezen de verdeelpunten in voor heel Nederland

# %% [markdown]

# verdeelpunten
# * lezen excel
# * eenheden omschrijven naar m3/s

verdeelpunten_df = pd.read_excel(
    cloud.joinpath("Landelijk", "verdelingsnetwerk", "verdeelpunten.xlsx"), sheet_name="verdeelpunten"
)
verdeelpunten_df = gpd.GeoDataFrame(verdeelpunten_df, geometry=gpd.GeoSeries(None, crs=28992))

# l/s -> m3/s
mask = verdeelpunten_df["aanvoer_eenheid"] == "l/s"
verdeelpunten_df.loc[mask, ["aanvoer_winter", "aanvoer_zomer", "aanvoer_min", "aanvoer_max"]] = (
    verdeelpunten_df.loc[mask, ["aanvoer_winter", "aanvoer_zomer", "aanvoer_min", "aanvoer_max"]] / 1000
)
verdeelpunten_df.loc[mask, "aanvoer_eenheid"] = "m3/s"


# %% [markdown]
# Limburg
# * Toevoegen geometrie aan verdeelpunten
# * Hydroobject filteren op supply areas
# * Aanvoergebieden maken op basis van hydroobjecten + BGT

bronhouder = "W0665"
hydroobject_buffer = 0.5
mask = verdeelpunten_df["waterbeheerder"] == "Limburg"
hydamo_gpkg = cloud.joinpath("Limburg", "verwerkt", "4_ribasim", "hydamo.gpkg")

# Toevoegen geometrie aan verdeelpunten
for objecttype in verdeelpunten_df[mask].objecttype.unique():
    geoseries = gpd.read_file(hydamo_gpkg, layer=objecttype).set_index("code")["geometry"].force_2d()
    objecttype_mask = mask & (verdeelpunten_df["objecttype"] == objecttype)
    verdeelpunten_df.loc[objecttype_mask, "geometry"] = geoseries.loc[
        verdeelpunten_df[objecttype_mask].code.to_numpy()
    ].to_numpy()


# hydroobject filteren
hydamo_gpkg = cloud.joinpath("Limburg", "verwerkt", "4_ribasim", "hydamo.gpkg")
areas_gpkg = cloud.joinpath("Limburg", "verwerkt", "4_ribasim", "areas.gpkg")
hydroobject_df = gpd.read_file(hydamo_gpkg, layer="hydroobject")
supply_df = gpd.read_file(areas_gpkg, layer="supply_areas")
hydroobject_select_df = hydroobject_df[hydroobject_df.within(supply_df.union_all().buffer(-90))]

# bgt filteren
waterdeel_select_df = waterdeel_df[waterdeel_df["bronhouder"] == bronhouder]
waterdeel_select_df = waterdeel_select_df[waterdeel_select_df.intersects(hydroobject_select_df.union_all())]


hydroobject_poly_df = hydroobject_select_df.copy()
hydroobject_poly_df["geometry"] = hydroobject_poly_df.buffer(hydroobject_buffer, cap_style="flat", join_style="mitre")
aanvoergebied_combined_df = pd.concat(
    [
        hydroobject_poly_df,
        waterdeel_select_df[["geometry"]],
    ]
)

aanvoergebied_combined_df = aanvoergebied_combined_df.dissolve().explode()[["geometry"]]
aanvoergebied_combined_df["waterbeheerder"] = "Limburg"

# %% [markdown]
# Aa en Maas
# * Hydroobject filteren op supply areas
# * Aanvoergebieden maken op basis van hydroobjecten + BGT

bronhouder = "W0654"

# %% [markdown]
# * wegschrijven laag verdeelpunten


aanvoergebieden_gpkg = cloud.joinpath("Landelijk", "verdelingsnetwerk", "aanvoer.gpkg")
verdeelpunten_df.to_file(aanvoergebieden_gpkg, layer="verdeelpunten")
aanvoergebied_combined_df.to_file(aanvoergebieden_gpkg, layer="aanvoergebieden")
hydroobject_select_df.to_file(aanvoergebieden_gpkg, layer="hydroobject")

# %%

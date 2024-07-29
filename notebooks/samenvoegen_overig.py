# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage
from ribasim_nl.geometry import drop_z
from shapely.geometry import MultiPolygon

# %%
cloud = CloudStorage()


models = [
    {
        "authority": "Rijkswaterstaat",
        "model": "hws",
        "find_toml": False,
        "update": False,
        "zoom_level": 0,
        "area_file": "krw_basins_vlakken.gpkg",
        "area_layer": "krw_basins_vlakken",
    },
    {
        "authority": "AmstelGooienVecht",
        "model": "AmstelGooienVecht_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "AmstelGooienVecht_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
    {
        "authority": "Delfland",
        "model": "Delfland_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "Delfland_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
    {
        "authority": "HollandseDelta",
        "model": "HollandseDelta_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "HollandseDelta_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
    {
        "authority": "HollandsNoorderkwartier",
        "model": "HollandsNoorderkwartier_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "HollandsNoorderkwartier_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
    {
        "authority": "Rijnland",
        "model": "Rijnland_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "Rijnland_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
    {
        "authority": "Rivierenland",
        "model": "Rivierenland_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "Rivierenland_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
    {
        "authority": "Scheldestromen",
        "model": "Scheldestromen_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "Scheldestromen_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
    {
        "authority": "SchielandendeKrimpenerwaard",
        "model": "SchielandendeKrimpenerwaard_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "SchielandendeKrimpenerwaard_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
    {
        "authority": "WetterskipFryslan",
        "model": "WetterskipFryslan_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "WetterskipFryslan_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
    {
        "authority": "Zuiderzeeland",
        "model": "Zuiderzeeland_poldermodel",
        "find_toml": True,
        "update": True,
        "zoom_level": 3,
        "area_file": "Zuiderzeeland_poldermodel_checks.gpkg",
        "area_layer": "peilgebied_met_streefpeil",
    },
]

gdfs = []
for model in models:
    print(model["authority"])
    model_versions = [i for i in cloud.uploaded_models(model["authority"]) if i.model == model["model"]]
    if model_versions:
        model_version = sorted(model_versions, key=lambda x: x.version)[-1]
    else:
        raise ValueError(f"No models with name {model["model"]} in the cloud")

    gpkg_file = cloud.joinpath(model["authority"], "modellen", model_version.path_string, model["area_file"])

    gdf = gpd.read_file(gpkg_file, layer=model["area_layer"], engine="pyogrio")
    if gdf.crs is None:
        gdf.crs = 28992
    elif gdf.crs.to_epsg() != 28992:
        gdf.to_crs(28992, inplace=True)
    gdf.loc[:, ["waterbeheerder"]] = model["authority"]
    gdf.rename(
        columns={"waterhoogte": "level", "owmident": "code", "owmnaam": "name"},
        inplace=True,
    )
    gdfs += [gdf]

# %%
gdf = pd.concat(gdfs, ignore_index=True)

# drop z-coordinates
gdf.loc[gdf.has_z, "geometry"] = gdf.loc[gdf.has_z, "geometry"].apply(lambda x: drop_z(x))

# drop non-polygons
mask = gdf.geom_type == "GeometryCollection"
gdf.loc[mask, "geometry"] = gdf.loc[mask, "geometry"].apply(
    lambda x: MultiPolygon([i for i in x.geoms if i.geom_type in ["Polygon", "MultiPolygon"]])
)

gpkg_file = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "project_data.gpkg")
gdf.to_file(gpkg_file, layer="area", engine="pyogrio")

# %%

# %%
import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point

collect_catalogus = "https://waterwebservices.rijkswaterstaat.nl/" + "METADATASERVICES_DBO/" + "OphalenCatalogus/"

check_waarneming = (
    "https://waterwebservices.rijkswaterstaat.nl/" + "ONLINEWAARNEMINGENSERVICES_DBO/" + "CheckWaarnemingenAanwezig/"
)

laatste_waarneming = (
    "https://waterwebservices.rijkswaterstaat.nl/" + "ONLINEWAARNEMINGENSERVICES_DBO/" + "OphalenLaatsteWaarnemingen/"
)

body = {
    "CatalogusFilter": {
        "Eenheden": True,
        "Grootheden": True,
        "Hoedanigheden": True,
        "Compartimenten": True,
    }
}


resp = requests.post(collect_catalogus, json=body)
result = resp.json()

locaties_df = gpd.GeoDataFrame(result["LocatieLijst"], geometry=gpd.GeoSeries(), crs=25831)
locaties_df.loc[:, "geometry"] = locaties_df.apply((lambda x: Point(x.X, x.Y)), axis=1)
locaties_df.to_crs(28992, inplace=True)
locaties_df.drop(columns="Coordinatenstelsel", inplace=True)


waterinfo_legenda_grenzen_df = pd.read_excel(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Rijkswaterstaat\aangeleverd\waterinfo-legenda-grenzen-20231019.xlsx",
    sheet_name="ParameterLimits",
)


locaties_df = locaties_df[locaties_df.Code.isin(waterinfo_legenda_grenzen_df.Code.unique())]
locaties_df.to_file("meetlocaties.gpkg")
# %%
locatie = locaties_df[(locaties_df.Code == "LOBH") & (locaties_df.Naam == "Lobith")].iloc[0].to_dict()
locatie = {k: v for k, v in locatie.items() if k in ["X", "Y", "Code"]}
body = {
    "LocatieLijst": [locatie],
    "AquoPlusWaarnemingMetadataLijst": [
        {
            "AquoMetadata": {
                "Compartiment": {"Code": "OW"},
                "Grootheid": {"Code": "WATHTE"},
            }
        }
    ],
    # "Periode": {
    #     "Begindatumtijd": "2024-05-20T14:00:00.000+01:00",
    #     "Einddatumtijd": "2024-05-20T16:00:00.000+01:00",
    # },
}

resp = requests.post(laatste_waarneming, json=body)
# result = resp.json()

# %%

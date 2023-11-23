# %%
import geopandas as gpd
from ribasim_nl import CloudStorage

cloud = CloudStorage()


# %% Create file with direction basins
# Load GeoPackage files with explicit geometry column name

poly_column = "owmident"
line_column = "Name"


kunstwerken_gdf = gpd.read_file(
    cloud.joinpath(
        "Rijkswaterstaat", "aangeleverd", "kunstwerken_primaire_waterkeringen.gpkg"
    )
)


# %%
# Load the original geopackage

# Select values
rws_kunstwerken_gdf = kunstwerken_gdf[kunstwerken_gdf["rws"] == "JA"]
kunstwerken_primair_gdf = kunstwerken_gdf[
    (
        kunstwerken_gdf["kd_type_om"].isin(
            [
                "gemaal",
                "stuw",
                "inlaatsluis",
                "keersluis",
                "spuisluis",
                "hevel",
                "duiker",
                "schutsluis",
            ]
        )
    )
]
overige_kunstwerken_primair_gdf = kunstwerken_gdf[
    (
        ~kunstwerken_gdf["kd_type_om"].isin(
            [
                "gemaal",
                "stuw",
                "inlaatsluis",
                "keersluis",
                "spuisluis",
                "hevel",
                "duiker",
                "schutsluis",
            ]
        )
    )
]

kunstwerken_primair_gdf.to_file(
    cloud.joinpath(
        "Rijkswaterstaat", "verwerkt", "kunstwerken_primaire_waterkeringen.gpkg"
    )
)
overige_kunstwerken_primair_gdf.to_file(
    cloud.joinpath(
        "Rijkswaterstaat", "verwerkt", "overige_kunstwerken_primaire_waterkeringen.gpkg"
    )
)
rws_kunstwerken_gdf.to_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "rws_kunstwerken.gpkg")
)

# %%

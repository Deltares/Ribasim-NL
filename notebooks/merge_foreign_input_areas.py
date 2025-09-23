# %%
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage

cloud = CloudStorage()


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


authorities = cloud.water_authorities

dfs = []
for authority in authorities:
    foreign_input_gpkg = cloud.joinpath(authority, "verwerkt", "4_ribasim", "foreign_input.gpkg")
    if foreign_input_gpkg.exists():
        print(authority)
        layer = next(
            (
                i
                for i in gpd.list_layers(foreign_input_gpkg)["name"]
                if i in ["foreign_areas", "foreign_drainage_areas"]
            ),
            None,
        )
        df = gpd.read_file(foreign_input_gpkg, layer=layer)
        df.crs = 28992
        df["waterbeheerder"] = authority
        dfs += [df]

# concat buitenlandse aanvoer
df = pd.concat(dfs)

buitenlandse_aanvoer_dir = cloud.joinpath("Basisgegevens", "buitenlandse_aanvoer")
buitenlandse_aanvoer_dir.mkdir(exist_ok=True)
buitenlandse_aanvoer_gpkg = buitenlandse_aanvoer_dir / "buitenlandse_aanvoer.gpkg"
df.to_file(buitenlandse_aanvoer_dir / "buitenlandse_aanvoer.gpkg")


# upload content
readme_md = buitenlandse_aanvoer_dir / "readme.md"
readme_md.write_text(
    f"Gebieden met buitenlandse aanvoer geleverd door waterschappen {', '.join(df.waterbeheerder.unique())}"
)

cloud.upload_file(buitenlandse_aanvoer_gpkg)
cloud.upload_file(readme_md)

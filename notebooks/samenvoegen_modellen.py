# %%
import sqlite3
from datetime import datetime

import pandas as pd
import ribasim
from ribasim_nl import CloudStorage
from ribasim_nl.concat import concat

# %%
cloud = CloudStorage()
readme = f"""# Model voor het Landelijk Hydrologisch Model

Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""


def fix_rating_curve(database_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)

    tables = ["TabulatedRatingCurve / static", "TabulatedRatingCurve / time"]

    for table in tables:
        # Read the table into a pandas DataFrame
        try:
            df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)
        except:  # noqa: E722
            continue

        # Rename the column in the DataFrame
        df_renamed = df.rename(columns={"discharge": "flow_rate"})

        # Write the DataFrame back to the SQLite table
        df_renamed.to_sql(table, conn, if_exists="replace", index=False)

    # Close the connection
    conn.close()


def add_basin_state(toml):
    # load a model without Basin / state
    model = ribasim.Model(filepath=toml)
    basin = model.basin

    # set initial level to (for example) 1 meter above Basin bottom
    basin.state.df = pd.DataFrame(
        (basin.profile.df.groupby("node_id").min("level").level) + 1.0
    ).reset_index()

    # remove geopackage key in model if exists
    if hasattr(model, "geopackage"):
        model.geopackage = None

    # write it back
    model.write(toml)


models = [
    {
        "authority": "Rijkswaterstaat",
        "model": "hws",
        "find_toml": False,
        "update": False,
        "zoom_level": 0,
    },
    {
        "authority": "AmstelGooienVecht",
        "model": "AmstelGooienVecht_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
    {
        "authority": "Delfland",
        "model": "Delfland_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
    {
        "authority": "HollandseDelta",
        "model": "HollandseDelta_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
    {
        "authority": "HollandsNoorderkwartier",
        "model": "HollandsNoorderkwartier_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
    {
        "authority": "Rijnland",
        "model": "Rijnland_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
    {
        "authority": "Rivierenland",
        "model": "Rivierenland_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
    {
        "authority": "Scheldestromen",
        "model": "Scheldestromen_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
    {
        "authority": "SchielandendeKrimpenerwaard",
        "model": "SchielandendeKrimpenerwaard_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
    {
        "authority": "WetterskipFryslan",
        "model": "WetterskipFryslan_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
    {
        "authority": "Zuiderzeeland",
        "model": "Zuiderzeeland_poldermodel",
        "find_toml": True,
        "update": False,
        "zoom_level": 3,
    },
]

for idx, model in enumerate(models):
    print(f"{model["authority"]} - {model["model"]}")
    model_versions = [
        i
        for i in cloud.uploaded_models(model["authority"])
        if i.model == model["model"]
    ]
    if model_versions:
        model_version = sorted(model_versions, key=lambda x: x.version)[-1]
    else:
        raise ValueError(f"No models with name {model["model"]} in the cloud")

    model_path = cloud.joinpath(
        model["authority"], "modellen", model_version.path_string
    )

    # download model if not yet downloaded
    if not model_path.exists():
        print(f"Downloaden versie: {model_version.version}")
        url = cloud.joinurl(model["authority"], "modellen", model_version.path_string)
        cloud.download_content(url)

    # find toml
    if model["find_toml"]:
        tomls = list(model_path.glob("*.toml"))
        if len(tomls) > 1:
            raise ValueError(
                f"User provided more than one toml-file: {len(tomls)}, remove one!"
            )
        else:
            model_path = tomls[0]
    else:
        model_path = model_path.joinpath(f"{model["model"]}.toml")

    # update model to v0.7.0
    if model["update"] and (not model_path.parent.joinpath("updated").exists()):
        print("updating model")
        # rename db_file if incorrect
        db_file = model_path.parent.joinpath(f"{tomls[0].stem}.gpkg")
        if db_file.exists():
            db_file = db_file.rename(model_path.parent.joinpath("database.gpkg"))
        else:
            db_file = model_path.parent.joinpath("database.gpkg")
            if not db_file.exists():
                raise FileNotFoundError(f"{db_file} doesn't exist")
        # fix rating_curve
        fix_rating_curve(db_file)

        # add basin-state
        add_basin_state(model_path)

        model_path.parent.joinpath("updated").write_text("true")

    # read model
    ribasim_model = ribasim.Model.read(model_path)
    ribasim_model.network.node.df.loc[:, "meta_zoom_level"] = model["zoom_level"]
    ribasim_model.network.edge.df.loc[:, "meta_zoom_level"] = model["zoom_level"]
    if idx == 0:
        lhm_model = ribasim_model
    else:
        cols = [i for i in lhm_model.network.edge.df.columns if i != "meta_index"]
        lhm_model.network.edge.df = lhm_model.network.edge.df[cols]
        ribasim_model.network.node.df.loc[:, "meta_waterbeheerder"] = model["authority"]
        ribasim_model.network.edge.df.loc[:, "meta_waterbeheerder"] = model["authority"]
        lhm_model = concat([lhm_model, ribasim_model])

    readme += f"""
**{model["authority"]}**: {model["model"]} ({model_version.version})"""

# %%
print("write lhm model")
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "lhm.toml")
lhm_model.write(ribasim_toml)
cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "readme.md").write_text(readme)
# %%
cloud.upload_model("Rijkswaterstaat", model="lhm")

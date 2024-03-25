# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage, default_model
from ribasim_nl.default_model import DEFAULTS

pd.options.mode.chained_assignment = None

# TODO: verwijderen duplicated node_ids in Node laag
# TODO: verwijderen duplicated (geometries) in Edge laag
# TODO: oplossen probleem met geometrieen met lengte == 0 in Edge laag
# TODO: zorgen dat node_ids altijd gevuld is, None in set van DrentsOverijsselseDelta

cloud = CloudStorage()
waterschappen = [
    "AaenMaas",
    "BrabantseDelta",
    "DeDommel",
    "DrentsOverijsselseDelta",
    "HunzeenAas",
    "Limburg",
    "Noorderzijlvest",
    "RijnenIJssel",
    "StichtseRijnlanden",
    "ValleienVeluwe",
    "Vechtstromen",
]

for waterschap in waterschappen:
    print(waterschap)
    in_gpkg = cloud.joinpath(waterschap, "modellen", "ribasim_model.gpkg")
    node_in_df = gpd.read_file(
        in_gpkg, layer="Node", engine="pyogrio", fid_as_index=True
    )

    # FIXME: zorgen dat node_id kolom altijd gevuld is
    if pd.to_numeric(node_in_df.node_id).hasnans:
        print("nans in node_id")
        node_in_df.loc[:, ["node_id"]] = (node_in_df.reset_index().index + 1).to_list()

    edge_in_df = gpd.read_file(
        in_gpkg, layer="Edge", engine="pyogrio", fid_as_index=True
    )
    # FIXME: opschonen brongegevens i.p.v. filteren op edge.length > 0

    if (edge_in_df.length == 0).any():
        print("edges met geometry.length == 0")
        edge_in_df = edge_in_df[edge_in_df.length != 0]

    # create default model
    model = default_model(node_in_df, edge_in_df, **DEFAULTS)

    # write model to disk
    ribasim_toml = cloud.joinpath(waterschap, "modellen", "ribasim_model", "model.toml")
    model.write(ribasim_toml)

    # upload model to cloud
    cloud.upload_model(waterschap, "ribasim_model")

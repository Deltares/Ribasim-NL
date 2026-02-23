# %%
# This script is to define the target level of the basin.
# It is taken from the lowest Gemiddeld zomerpeil (GPGZMRPL), located at the structure


from ribasim_nl.streefpeilen import add_streefpeil

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

# List of different parameters for the models, including peilgebieden path for each authority
params_list = [
    (
        "HunzeenAas",
        "hea",
        "verwerkt/1_ontvangen_data/peilgebieden.gpkg",
        None,
        "gpgzmrpl",
        "gpgident",
        "HunzeenAas_fix_model_network",
    ),
    (
        "DrentsOverijsselseDelta",
        "dod",
        "verwerkt/1_ontvangen_data/extra data/Peilgebieden/Peilgebieden.shp",
        None,
        "GPGZMRPL",
        "GPGIDENT",
        "DrentsOverijsselseDelta_fix_model_network",
    ),
    (
        "AaenMaas",
        "aam",
        "verwerkt/downloads/WS_PEILGEBIEDPolygon.shp",
        None,
        "ZOMERPEIL",
        "CODE",
        "AaenMaas_fix_model_area",
    ),
    (
        "BrabantseDelta",
        "wbd",
        "verwerkt/4_ribasim/hydamo.gpkg",
        "peilgebiedpraktijk",
        "WS_ZOMERPEIL",
        "CODE",
        "BrabantseDelta_fix_model_area",
    ),
    (
        "StichtseRijnlanden",
        "hdsr",
        "verwerkt/4_ribasim/peilgebieden.gpkg",
        None,
        "WS_ZP",
        "WS_PGID",
        "StichtseRijnlanden_fix_model_area",
    ),
    (
        "ValleienVeluwe",
        "venv",
        "verwerkt/1_ontvangen_data/Eerste_levering/vallei_en_veluwe.gpkg",
        "peilgebiedpraktijk",
        "ws_max_peil",
        "code",
        "ValleienVeluwe_fix_model_area",
    ),
    (
        "Vechtstromen",
        "vechtstromen",
        "verwerkt/downloads/peilgebieden_voormalig_velt_en_vecht.gpkg",
        None,
        "GPGZMRPL",
        "GPGIDENT",
        "Vechtstromen_fix_model_area",
    ),
]

# Main function
if __name__ == "__main__":
    for params in params_list:
        # parse params
        authority, short_name, peilgebieden_path, layername, targetlevel, code, modelpath = params
        # parse paths
        ribasim_dir = cloud.joinpath(authority, "modellen", modelpath)
        ribasim_toml = ribasim_dir / f"{short_name}.toml"
        peilgebieden_path = cloud.joinpath(authority, peilgebieden_path)

        # sync paths
        cloud.synchronize(filepaths=[peilgebieden_path])

        # read model
        model = Model.read(ribasim_toml)

        # add streefpeil
        result = add_streefpeil(model, peilgebieden_path, layername, targetlevel, code)

        # write model
        model.write(ribasim_toml)

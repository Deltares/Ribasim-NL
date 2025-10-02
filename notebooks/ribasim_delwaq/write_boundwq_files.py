# %%
import json
import logging
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from ribasim_nl import CloudStorage, Model


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )


setup_logging()

# %% Define folder locations and synchronize with the Good Cloud
cloud = CloudStorage()
logging.info("Synchronizing with file on the Good Cloud")

delwaq_folder = cloud.joinpath("Basisgegevens/Delwaq")

IM_metingen_excel_path = cloud.joinpath(delwaq_folder, "verwerkt/data/combined_IM_Metingen_2016_2022.xlsx")

meetlocaties_IM_path = cloud.joinpath(delwaq_folder, "verwerkt/data/IM_ribasim_mapping.geojson")

zinfo_file_path = cloud.joinpath(delwaq_folder, "aangeleverd/Zinfo/zinfo_20160101_20231231_waterkwaliteit.csv")

boundwq_path = cloud.joinpath(delwaq_folder, "verwerkt/delwaq_input")

figures_path = cloud.joinpath(delwaq_folder, "verwerkt/figures")

cloud.synchronize(
    filepaths=[
        IM_metingen_excel_path,
        meetlocaties_IM_path,
        zinfo_file_path,
        boundwq_path,
        figures_path,
    ]
)


# %% Hard coded parameter settings
make_plots = False

NO3_fractie = 0.8
NH4_Nkj_fractie = 0.8
NH4_Ntot_fractie = 0.1
OON_Nkj_fractie = 0.2
OON_Ntot_fractie = 0.1
AAP_fractie = 0.75
AAP_PO4_fractie = 0.4
OOP_fractie = 0.25
OOP_PO4_fractie = 0.1
PO4_Ptot_fractie = 0.5

parameters_IM = ["NKj", "NO3", "NOx", "sNO3NO2", "Ntot", "PO4", "Ptot"]
parameters_Zinfo = ["NKj", "NO3", "NOx", "sNO3NO2", "Ntot", "PO4", "Ptot"]


# %% Dowload gewenste Ribasim model versie

model_spec = {
    "authority": "Rijkswaterstaat",
    "model": "lhm_coupled",
    "find_toml": True,
}

logging.info(f"Download model: {model_spec['authority']} - {model_spec['model']}")


def get_model_path(model, model_version):
    return cloud.joinpath(model["authority"], "modellen", model_version.path_string)


download_latest_model = True

# get version
if "model_version" in model_spec:
    model_version = model_spec["model_version"]
    logging.info("model version: %s", model_version)
else:
    model_versions = [i for i in cloud.uploaded_models(model_spec["authority"]) if i.model == model_spec["model"]]
    if model_versions:
        model_version = sorted(model_versions, key=lambda x: x.sorter)[-1]
    else:
        raise ValueError(f"No models with name {model_spec['model']} in the cloud")
    logging.info("model version not defined, latest is: %s", model_version)

model_path = get_model_path(model_spec, model_version)

# download model if not yet downloaded
if not model_path.exists():
    if download_latest_model:
        logging.info(f"Downloaden versie: {model_version.version}")
        url = cloud.joinurl(model_spec["authority"], "modellen", model_version.path_string)
        cloud.download_content(url)
    else:
        model_versions = sorted(model_versions, key=lambda x: x.version, reverse=True)
        model_paths = (get_model_path(model_spec, i) for i in model_versions)
        model_path = next((i for i in model_paths if i.exists()), None)
        if model_path is None:
            raise ValueError(f"No models with name {model_spec['model']} on local drive")

# find toml
if model_spec["find_toml"]:
    tomls = list(model_path.glob("*.toml"))
    if len(tomls) > 1:
        raise ValueError(f"User provided more than one toml-file: {len(tomls)}, remove one!")
    else:
        model_path = tomls[0]
        logging.info("found 1 toml-file")
else:
    model_path = model_path.joinpath(f"{model_spec['model']}.toml")

# %% Laad het Ribasim model in
model = Model.read(model_path)
logging.info("Ribasim model ingeladen")
df_model_nodes = model.flow_boundary.node.df
logging.info(f"{len(df_model_nodes)} FlowBoundaries in het model")

df_model_nodes_rwzi = df_model_nodes[df_model_nodes["meta_rwzi_codeist"].notna()]
df_rwzi_mapping_table = df_model_nodes_rwzi[["meta_rwzi_codeist", "name"]].reset_index()
df_rwzi_mapping_table["meta_rwzi_codeist"] = (
    df_rwzi_mapping_table["meta_rwzi_codeist"].str.replace("NL.", "NL", regex=False).str.replace(".", "_", regex=False)
)
# print(df_rwzi_mapping_table)
logging.info(f"Waarvan {len(df_rwzi_mapping_table)} RWZIs")


# %% Meetlocaties IM-metingen inladen en linken aan de modeldata
def load_geojson(meetlocaties_path):
    try:
        with open(meetlocaties_path, encoding="utf-8") as geojson_file:
            geojson_data = json.load(geojson_file)
        logging.info(f"IM meetlocaties ingeladen: {meetlocaties_path}")
        return geojson_data
    except FileNotFoundError:
        logging.error(f"GeoJSN bestaat niet: {meetlocaties_path}")
        return None


meetlocaties_IM_data = load_geojson(meetlocaties_IM_path)
df_IM_meetlocaties = pd.DataFrame([feature["properties"] for feature in meetlocaties_IM_data["features"]])


def extract_and_add_node_ids(features_df: pd.DataFrame, df_model_nodes: pd.DataFrame) -> pd.DataFrame:
    required_columns = ["Grenspunt", "ribasim_id"]
    missing_columns = [col for col in required_columns if col not in features_df.columns]

    if missing_columns:
        logging.warning(
            "De volgende vereiste kolommen ontbreken in de geojson file: %s",
            ", ".join(missing_columns),
        )
        return pd.DataFrame(columns=["Meetobject.code", "ribasim_id", "node_id"])

    # Valid ribasim_id = not NaN, not empty
    ribasim_valid = ~features_df["ribasim_id"].isna() & (features_df["ribasim_id"].astype(str).str.strip() != "")
    grenspunt_normalized = features_df["Grenspunt"].astype(str).str.strip().str.lower()
    mask_valid = (grenspunt_normalized == "true") & ribasim_valid

    # Filter de grenspunten die in het model zitten
    filtered_df = features_df.loc[mask_valid, ["Meetobject.code", "ribasim_id"]].drop_duplicates().copy()

    logging.info(
        "Aantal meetlocaties die gebruikt worden als randvoorwaarde: %d",
        len(filtered_df),
    )
    if not filtered_df.empty:
        logging.info("Deze unieke locaties zijn:\n%s", filtered_df.to_string(index=False))

    # Check for missing ribasim_id
    mask_missing = (grenspunt_normalized == "true") & (~ribasim_valid)
    missing_ribasim = features_df.loc[mask_missing, "Meetobject.code"].unique()
    if len(missing_ribasim) > 0:
        logging.warning(
            "Voor de volgende meetobjecten is geen ribasim_id opgegeven:\n%s",
            ", ".join(missing_ribasim),
        )
    else:
        logging.info("Alle grenspunten hebben een geldige ribasim_id.")

    # Merge with node_id
    nodes_lookup = df_model_nodes.reset_index()[["node_id", "name"]]
    merged_df = filtered_df.merge(nodes_lookup, left_on="ribasim_id", right_on="name", how="left").drop(
        columns=["name"]
    )

    # Log missing node_id matches
    missing_nodes = merged_df[merged_df["node_id"].isna()]
    if not missing_nodes.empty:
        logging.warning(
            "%d ribasim_id(s) konden niet worden gekoppeld aan een node_id: %s",
            len(missing_nodes),
            ", ".join(missing_nodes["ribasim_id"].astype(str).unique()),
        )
    else:
        logging.info("Alle ribasim_id's zijn succesvol gekoppeld aan node_id's.")

    return merged_df.reset_index(drop=True)


grenspunt_meetobject_df = extract_and_add_node_ids(df_IM_meetlocaties, df_model_nodes)
# grenspunt_meetobject_codes = grenspunt_meetobject_df["Meetobject.code"]


# %% Lezen excel sheet IM-metingen (langzaam)
def read_excel_file(file_path):
    if not os.path.exists(file_path):
        logging.error(f"Bestand bestaat niet: {file_path}")
        raise FileNotFoundError(f"Het Excel-bestand bestaat niet: {file_path}")

    logging.info(f"Begin met inlezen van Excel-bestand met IM-metingen: {file_path}")
    return pd.read_excel(file_path, sheet_name=None)


sheets_dict = read_excel_file(IM_metingen_excel_path)
logging.info("Excel bestand met IM-metingen ingeladen.")


def filter_grenspunt_locations(sheets_dict, grenspunt_meetobject_codes):
    return {
        sheet_name: df[df["Meetobject.code"].isin(grenspunt_meetobject_codes)] for sheet_name, df in sheets_dict.items()
    }


filtered_sheets_dict = filter_grenspunt_locations(sheets_dict, grenspunt_meetobject_df["Meetobject.code"])


def filter_units(sheets_dict_mgpl):
    deleted_units = {}
    for par in sheets_dict_mgpl.keys():
        before_filtering = set(sheets_dict_mgpl[par]["Eenheid.code"].unique())
        sheets_dict_mgpl[par] = sheets_dict_mgpl[par][sheets_dict_mgpl[par]["Eenheid.code"] == "mg/l"]
        after_filtering = set(sheets_dict_mgpl[par]["Eenheid.code"].unique())
        deleted = before_filtering - after_filtering
        if deleted:
            deleted_units[par] = list(deleted)
    return deleted_units


deleted_units = filter_units(filtered_sheets_dict)


def log_unique_codes(sheets_dict_mgpl):
    parameters = []
    unique_codes = []
    for par in sheets_dict_mgpl.keys():
        parameters.append(par)
        unique_codes.append(list(sheets_dict_mgpl[par]["Eenheid.code"].unique()))
    df_unique_codes = pd.DataFrame({"Parameter": parameters, "Unique Eenheid.code": unique_codes})
    logging.info(f"Geselecteerde parameters uit de IM-metingen: \n{df_unique_codes.to_string(index=False)}")


log_unique_codes(filtered_sheets_dict)

for par, units in deleted_units.items():
    logging.info(f'From parameter "{par}", deleted units: {units}')


def combine_measurement_data(sheets_dict_mgpl):
    combined_df = pd.concat(sheets_dict_mgpl.values(), ignore_index=True)
    locations = combined_df["Meetobject.code"].unique()
    logging.info(f"Unieke meetlocaties uit de IM-metingen: {locations}")
    return combined_df, locations


df_IM_metingen, locations_IM = combine_measurement_data(filtered_sheets_dict)


# %% Read Zinfo data
def read_zinfo_data(file_path):
    if not os.path.exists(file_path):
        logging.error(f"The file does not exist: {file_path}")
        raise FileNotFoundError(f"The CSV file does not exist: {file_path}")
    logging.info(f"Begin met inlezen van Excel-bestand met Z-info metingen: {file_path}")
    df = pd.read_csv(file_path, sep=",")
    return df


def filter_zinfo_data(df):
    combined_df = df[df["ehd"] == "mg/l"].copy()
    return combined_df


def process_zinfo_data(zinfo_file_path):
    df = read_zinfo_data(zinfo_file_path)
    combined_df = filter_zinfo_data(df)
    unique_values = combined_df["par"].unique()
    logging.info(f"Unieke parameters beschikbaar in de Zinfo data zijn: : {unique_values}")

    combined_df.rename(
        columns={
            "d": "Begindatum",
            "ist": "Meetobject.code",
            "par": "Parameter.code",
            "Waarde": "Numeriekewaarde",
        },
        inplace=True,
    )
    combined_df["Meetobject.code"] = combined_df["Meetobject.code"].str.replace(
        r"\.([^.]*)\.(.*)", r"\1_\2", regex=True
    )

    # Convert 'Begindatum' to datetime
    combined_df["Begindatum"] = pd.to_datetime(combined_df["Begindatum"]).dt.date
    combined_df["Begintijd"] = "00:00:00"

    return combined_df


df_Zinfo_full = process_zinfo_data(zinfo_file_path)
logging.info(f"Z-info data ingelezen. Aantal RWZIs in dataset: {len(df_Zinfo_full['Meetobject.code'].unique())}")

df_Zinfo = df_Zinfo_full[df_Zinfo_full["Meetobject.code"].isin(df_rwzi_mapping_table["meta_rwzi_codeist"])]
locations_zinfo = df_Zinfo["Meetobject.code"].unique()
logging.info(
    f"Z-info data gefilterd op aanwezige RWZI's in Ribasim: {len(df_Zinfo_full['Meetobject.code'].unique()) - len(df_Zinfo['Meetobject.code'].unique())} RWZIs niet in het model"
)


# %% Converteer de meetdata naar gewenste Delwaq parameters
def process_measurement_data(combined_df, locations, parameters):
    dict_choosen_method = {}
    dict_delwaq_input = {}

    for loc in locations:
        df_loc = combined_df[combined_df["Meetobject.code"] == loc]
        df_pivot = df_loc.pivot_table(
            index=["Begindatum", "Begintijd"],
            columns="Parameter.code",
            values="Numeriekewaarde",
        ).reset_index()

        df_pivot["NO3_0"] = df_pivot["NO3"] if "NO3" in df_pivot else -999
        df_pivot["NO3_1"] = df_pivot["sNO3NO2"] if "sNO3NO2" in df_pivot else -999
        df_pivot["NO3_2"] = df_pivot["NOx"] if "NOx" in df_pivot else -999
        df_pivot["NO3_3"] = (NO3_fractie * df_pivot["Ntot"]) if "Ntot" in df_pivot else -999

        df_pivot["NH4_0"] = df_pivot["NH4"] if "NH4" in df_pivot else -999
        df_pivot["NH4_1"] = (NH4_Nkj_fractie * df_pivot["NKj"]) if "NKj" in df_pivot else -999
        df_pivot["NH4_2"] = (NH4_Ntot_fractie * df_pivot["Ntot"]) if "Ntot" in df_pivot else -999

        df_pivot["OON_0"] = (
            (df_pivot["NKj"] - df_pivot["NH4"]) if all(col in df_pivot for col in ["NKj", "NH4"]) else -999
        )
        df_pivot["OON_1"] = (OON_Nkj_fractie * df_pivot["NKj"]) if "NKj" in df_pivot else -999
        df_pivot["OON_2"] = (OON_Ntot_fractie * df_pivot["Ntot"]) if "Ntot" in df_pivot else -999

        # Include only if we decide to include Ntot after all
        # df_pivot['Ntot_0'] = df_pivot['Ntot'] if 'Ntot' in df_pivot else -999
        # Now this part works only if NO3 is directly measured, do we want to include computed N03 values as well?
        # df_pivot['Ntot_1'] = (df_pivot['NO3'] + df_pivot['NH4'] + df_pivot['OON']) if all(col in df_pivot for col in ['NO3', 'NH4', 'OON']) else -999

        df_pivot["PO4_0"] = df_pivot["PO4"] if "PO4" in df_pivot else -999
        df_pivot["PO4_1"] = (PO4_Ptot_fractie * df_pivot["Ptot"]) if all(col in df_pivot for col in ["Ptot"]) else -999

        df_pivot["AAP_0"] = (
            AAP_fractie * (df_pivot["Ptot"] - df_pivot["PO4"])
            if all(col in df_pivot for col in ["Ptot", "PO4"])
            else -999
        )

        df_pivot["AAP_1"] = (AAP_PO4_fractie * df_pivot["PO4"]) if all(col in df_pivot for col in ["PO4"]) else -999

        df_pivot["OOP_0"] = (
            OOP_fractie * (df_pivot["Ptot"] - df_pivot["PO4"])
            if all(col in df_pivot for col in ["Ptot", "PO4"])
            else -999
        )

        df_pivot["OOP_1"] = (OOP_PO4_fractie * df_pivot["Ptot"]) if all(col in df_pivot for col in ["Ptot"]) else -999

        # Include only if we decide to include Ptpt after all
        # TO DO: ONLY DO THIS IF AAP AND OOP ARE NOT -999
        # df_pivot['Ptot_0'] = df_pivot['Ptot'] if 'Ptot' in df_pivot else -999
        # df_pivot['Ptot_1'] = (df_pivot['PO4'] + df_pivot['AAP'] + df_pivot['OOP']) if all(col in df_pivot for col in ['PO4', 'AAP', 'OOP']) else -999

        df_pivot_binary = df_pivot.copy()
        df_pivot_binary = df_pivot_binary.map(lambda x: 0 if x == -999 else 1)
        dict_choosen_method[loc] = df_pivot_binary

        # Kies de methode waarbij we data hebben
        df_delwaq_input = df_pivot[["Begindatum", "Begintijd"]].copy()
        df_delwaq_input["NO3"] = np.where(
            df_pivot["NO3_0"] != -999,
            df_pivot["NO3_0"],
            np.where(
                df_pivot["NO3_1"] != -999,
                df_pivot["NO3_1"],
                np.where(
                    df_pivot["NO3_2"] != -999,
                    df_pivot["NO3_2"],
                    np.where(df_pivot["NO3_3"] != -999, df_pivot["NO3_3"], -999),
                ),
            ),
        )

        df_delwaq_input["NH4"] = np.where(
            df_pivot["NH4_0"] != -999,
            df_pivot["NH4_0"],
            np.where(
                df_pivot["NH4_1"] != -999,
                df_pivot["NH4_1"],
                np.where(df_pivot["NH4_2"] != -999, df_pivot["NH4_2"], -999),
            ),
        )

        df_delwaq_input["OON"] = np.where(
            df_pivot["OON_0"] != -999,
            df_pivot["OON_0"],
            np.where(
                df_pivot["OON_1"] != -999,
                df_pivot["OON_1"],
                np.where(df_pivot["OON_2"] != -999, df_pivot["OON_2"], -999),
            ),
        )

        df_delwaq_input["PO4"] = np.where(
            df_pivot["PO4_0"] != -999,
            df_pivot["PO4_0"],
            np.where(df_pivot["PO4_1"] != -999, df_pivot["PO4_1"], -999),
        )

        df_delwaq_input["AAP"] = np.where(
            df_pivot["AAP_0"] != -999,
            df_pivot["AAP_0"],
            np.where(df_pivot["AAP_1"] != -999, df_pivot["AAP_1"], -999),
        )

        df_delwaq_input["OOP"] = np.where(
            df_pivot["OOP_0"] != -999,
            df_pivot["OOP_0"],
            np.where(df_pivot["OOP_1"] != -999, df_pivot["OOP_1"], -999),
        )

        # Only include if we decided to include Ntot and Ptot and CHLFa
        # df_delwaq_input['Ntot'] = np.where(df_pivot['Ntot_0'] != -999, df_pivot['Ntot_0'],
        # np.where(df_pivot['Ntot_1'] != -999, df_pivot['Ntot_1'], -999))
        # df_delwaq_input['Ptot'] = np.where(df_pivot['Ptot_0'] != -999, df_pivot['Ptot_0'],
        #                         np.where(df_pivot['Ptot_1'] != -999, df_pivot['Ptot_1'], -999))
        # df_delwaq_input['CHLFa'] = df_pivot['CHLFa'] if 'CHLFa' in df_pivot else -999

        dict_delwaq_input[loc] = df_delwaq_input
    return dict_choosen_method, dict_delwaq_input


# Converteer de IM-data voor buitenlandse aanvoeren
dict_choosen_method_IM, dict_delwaq_input_IM = process_measurement_data(df_IM_metingen, locations_IM, parameters_IM)

# Converteer de Z-info data voor RWZIs
dict_choosen_method_Zinfo, dict_delwaq_input_Zinfo = process_measurement_data(
    df_Zinfo, locations_zinfo, parameters_Zinfo
)

# Log the parameters
parameter_delwaq_input_IM = list(dict_delwaq_input_IM.values())[0].columns[2:]
parameter_delwaq_input_Zinfo = list(dict_delwaq_input_Zinfo.values())[0].columns[2:]

logging.info(
    f"Delwaq input parameters uit IM-metingen (buitenlandse aanvoeren): \n {list(parameter_delwaq_input_IM[:])} \n voor {len(dict_delwaq_input_IM.keys())} locaties"
)
logging.info(
    f"Delwaq input parameters uit Z-Info (rwzis): \n {list(parameter_delwaq_input_Zinfo[:])} \n voor {len(dict_delwaq_input_Zinfo.keys())} locaties"
)


# %% Schijf de boundarywq.dat files weg
def write_boundwq_file(boundwq_file, dict_delwaq_input, parameter_delwaq_input):
    # gebruik mapping tabellen om de juiste node_ids van Ribasim te vinden
    use_node_id_im = dict_delwaq_input is dict_delwaq_input_IM
    use_node_id_zinfo = dict_delwaq_input is dict_delwaq_input_Zinfo
    locations = []
    if use_node_id_im:
        loc_to_node = dict(
            zip(
                grenspunt_meetobject_df["Meetobject.code"],
                grenspunt_meetobject_df["node_id"],
            )
        )
        loc_to_ribasim = dict(
            zip(
                grenspunt_meetobject_df["Meetobject.code"],
                grenspunt_meetobject_df["ribasim_id"],
            )
        )

    if use_node_id_zinfo:
        # Mapping from df_rwzi_mapping_table: meta_rwzi_codeist -> node_id
        loc_to_node_zinfo = dict(
            zip(
                df_rwzi_mapping_table["meta_rwzi_codeist"],
                df_rwzi_mapping_table["node_id"],
            )
        )
        # Mapping from meta_rwzi_codeist -> name
        loc_to_name_zinfo = dict(
            zip(
                df_rwzi_mapping_table["meta_rwzi_codeist"],
                df_rwzi_mapping_table["name"],
            )
        )

    with open(boundwq_file, "w") as boundwq:

        def write_data(dict_delwaq_input, parameter_delwaq_input):
            for loc in dict_delwaq_input.keys():
                if use_node_id_im:
                    node_id = loc_to_node.get(loc, loc)
                    ribasim_id = loc_to_ribasim.get(loc, "NA")
                elif use_node_id_zinfo:
                    node_id = loc_to_node_zinfo.get(loc, loc)
                    ribasim_id = loc_to_name_zinfo.get(loc, "NA")
                else:
                    node_id = loc
                    ribasim_id = "NA"

                boundwq.write(f"ITEM '{node_id}'; {loc} {ribasim_id}\nABSOLUTE TIME\nCONCENTRATION\n")

                for param in parameter_delwaq_input:
                    boundwq.write(f" '{param}'\n")

                boundwq.write(
                    "LINEAR DATA\t\t\t\t" + "".join(f"'{param}'".ljust(12) for param in parameter_delwaq_input) + "\n"
                )
                for _, row in dict_delwaq_input[loc].fillna(-999).iterrows():
                    begindatum_formatted = str(row["Begindatum"]).replace("-", "/") + "-" + row["Begintijd"]
                    values = "".join(
                        f"{int(row[param]) if row[param] == -999 else round(row[param], 6):<12}"
                        for param in parameter_delwaq_input
                    )
                    boundwq.write(f"'{begindatum_formatted}'    {values}\n")

                boundwq.write("\n")
                # logging.info(f"Data of {loc} written to BOUNDWQ.DAT file")
                locations.append(loc)

        write_data(dict_delwaq_input, parameter_delwaq_input)
    logging.info(f"{len(locations)} tijdseries weggeschreven")


boundwq_file_zinfo = cloud.joinpath(boundwq_path, "BOUNDWQ_rwzi.DAT")
boundwq_file_im = cloud.joinpath(boundwq_path, "BOUNDWQ_ba.DAT")

write_boundwq_file(boundwq_file_zinfo, dict_delwaq_input_Zinfo, parameter_delwaq_input_Zinfo)
logging.info(f"BOUNDWQ_rwzi.DAT file saved in {boundwq_file_zinfo}")

write_boundwq_file(boundwq_file_im, dict_delwaq_input_IM, parameter_delwaq_input_IM)
logging.info(f"BOUNDWQ_ba.DAT file saved in {boundwq_file_im}")

cloud.upload_file(boundwq_file_im)
cloud.upload_file(boundwq_file_zinfo)


# %% Keuze voor parameter methode IM metingen
"""
The rest of the code consists of analyzing how often certain methods are chosen to determine a parameter.
"""


if make_plots:
    plot_colors = ["#83C5BE", "#3A86FF", "#006D77", "#6D9A3D", "#BC6C25", "#E29578"]
    parameters = {
        "NO3": (
            ["NO3_0", "NO3_1", "NO3_2", "NO3_3"],
            ["NO3", "sNO3NO2", "NOx", f"{NO3_fractie}*Ntot", "-999"],
        ),
        "NH4": (
            ["NH4_0", "NH4_1", "NH4_2"],
            ["NH4", f"{NH4_Nkj_fractie}*NKj", f"{NH4_Ntot_fractie}*Ntot", "-999"],
        ),
        "OON": (
            ["OON_0", "OON_1", "OON_2"],
            ["NKj-NH4", f"{OON_Nkj_fractie}*NKj", f"{OON_Ntot_fractie}*Ntot", "-999"],
        ),
        "PO4": (["PO4_0", "PO4_1"], ["PO4", f"{PO4_Ptot_fractie}*Ptot", "-999"]),
        "AAP": (
            ["AAP_0", "AAP_1"],
            [f"{AAP_fractie}*(Ptot-PO4)", f"{AAP_PO4_fractie}*PO4", "-999"],
        ),
        "OOP": (
            ["OOP_0", "OOP_1"],
            [f"{OOP_fractie}*(Ptot-PO4)", f"{OOP_PO4_fractie}*Ptot", "-999"],
        ),
    }

    for group, parameter_keys in zip(["N Parameters", "P Parameters"], [["NO3", "NH4", "OON"], ["PO4", "AAP", "OOP"]]):
        fig, axes = plt.subplots(len(parameter_keys), 1, figsize=(20, 10), dpi=1000)
        print()
        for parameter_key, ax in zip(parameter_keys, axes):
            parsset, parslab = parameters[parameter_key]
            locations = dict_choosen_method_IM.keys()
            location_percentages = {loc: dict.fromkeys(parsset + ["-999"], 0) for loc in locations}

            # Calculate percentages for each parameter and location
            for loc in locations:
                df_loc = dict_choosen_method_IM[loc][parsset]
                total_timesteps = len(df_loc)

                for timestep in df_loc.index:
                    chosen = "-999"  # Default to -999 if none are chosen
                    for par in parsset:
                        if df_loc.loc[timestep, par] == 1:
                            chosen = par
                            break
                    location_percentages[loc][chosen] += 1

                for par in parsset + ["-999"]:
                    location_percentages[loc][par] = (location_percentages[loc][par] / total_timesteps) * 100

            # Bar chart
            parameters_with_fallback = parsset + ["-999"]
            bar_width = 0.5
            locations_list = list(locations)
            bottom = np.zeros(len(locations_list))

            for i, par in enumerate(parameters_with_fallback):
                c = plot_colors[i]
                if par == "-999":
                    c = "#E29578"
                heights = [location_percentages[loc][par] for loc in locations_list]
                ax.bar(
                    locations_list,
                    heights,
                    bottom=bottom,
                    label=parslab[i],
                    width=bar_width,
                    color=c,
                )
                bottom += heights

            ax.set_title(f"{parameter_key}")
            ax.set_ylabel("Percentage")
            ax.legend(title="Parameters", bbox_to_anchor=(1.05, 1), loc="upper left")

        for i in range(len(parameter_keys) - 1):
            axes[i].set_xticks([])
            axes[i].set_xlabel("")

        axes[-1].set_xlabel("Meetlocatie")
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.suptitle(group, fontsize=16, y=1.02)

        file_name = f"{'_'.join(parameter_keys)}_percentage_plot.png"  # Specify a file name
        plt.suptitle(
            f"Procentuele verdeling van gebruikte methodes per locatie voor de bepaling van {', '.join(parameter_keys[:-1]) + ' en ' + parameter_keys[-1]} binnen de IM-Metingen",
            fontsize=16,
            y=1.02,
        )

        file_name = "gecombineerde_plot_percentages.png"
        file_path = cloud.joinpath(figures_path, file_name)

        plt.savefig(file_path, dpi=500, bbox_inches="tight")
        cloud.upload_file(file_path)
        plt.show()


# %%
if make_plots:

    def split_into_chunks(data, num_chunks):
        """Split data (list) into `num_chunks`, with the first chunks potentially having one more item."""
        n = len(data)
        q, r = divmod(n, num_chunks)  # q = quotient, r = remainder
        sizes = [q + (1 if i < r else 0) for i in range(num_chunks)]  # Distribute remainder to the first chunks
        chunks = []
        start = 0
        for size in sizes:
            chunks.append(data[start : start + size])
            start += size
        return chunks

    # Example for 4 subplots:
    for group, parameter_keys in zip(["N Parameters", "P Parameters"], [["NO3", "NH4", "OON"], ["PO4", "AAP", "OOP"]]):
        for parameter_key in parameter_keys:
            parsset, parslab = parameters[parameter_key]
            locations = list(dict_choosen_method_Zinfo.keys())  # Convert to a list to allow slicing
            location_chunks = split_into_chunks(locations, 4)  # Divide locations into 4 chunks
            num_subplots = len(location_chunks)

            fig, axes = plt.subplots(num_subplots, 1, figsize=(20, 12), dpi=1000)
            if num_subplots == 1:
                axes = [axes]  # Ensure axes is always a list for consistent iteration

            for idx, (loc_chunk, ax) in enumerate(zip(location_chunks, axes)):
                location_percentages = {loc: dict.fromkeys(parsset + ["-999"], 0) for loc in loc_chunk}

                # Calculate percentages for each parameter and location
                for loc in loc_chunk:
                    df_loc = dict_choosen_method_Zinfo[loc][parsset]
                    total_timesteps = len(df_loc)

                    for timestep in df_loc.index:
                        chosen = "-999"  # Default to -999 if none are chosen
                        for par in parsset:
                            if df_loc.loc[timestep, par] == 1:
                                chosen = par
                                break
                        location_percentages[loc][chosen] += 1

                    for par in parsset + ["-999"]:
                        location_percentages[loc][par] = (location_percentages[loc][par] / total_timesteps) * 100

                # Bar chart
                parameters_with_fallback = parsset + ["-999"]
                bar_width = 0.5
                locations_list = loc_chunk
                bottom = np.zeros(len(locations_list))

                for i, par in enumerate(parameters_with_fallback):
                    c = plot_colors[i]
                    if par == "-999":
                        c = "#E29578"
                    heights = [location_percentages[loc][par] for loc in locations_list]
                    ax.bar(
                        locations_list,
                        heights,
                        bottom=bottom,
                        label=parslab[i],
                        width=bar_width,
                        color=c,
                    )
                    bottom += heights

                # Add labels and x-tick rotation
                ax.set_ylabel("Percentage")
                ax.set_xticks(range(len(locations_list)))
                ax.set_xticklabels(locations_list, rotation=90)
                if idx == num_subplots - 1:
                    ax.set_xlabel("Meetlocatie")  # Only add x-label for the last subplot

            # Add legend only next to the first subplot
            axes[0].legend(title="Parameters", bbox_to_anchor=(1.05, 1), loc="upper left")

            plt.tight_layout()
            file_name = f"{parameter_key}_percentage_plot.png"  # Specify a file name

            plt.suptitle(
                f"Procentuele verdeling van gebruikte methodes per locatie voor de bepaling van {parameter_key} binnen de Zinfo-metingen",
                fontsize=16,
                y=1.02,
            )  # Add parameter key as a group title
            file_name = "procentuele_verdeling_gebruikte_methode.png"
            file_path = cloud.joinpath(figures_path, file_name)

            plt.savefig(file_path, dpi=500, bbox_inches="tight")
            cloud.upload_file(file_path)
            plt.show()


# %% Plot a summery of the plots above
if make_plots:
    first_methods = {key: values[0][0] for key, values in parameters.items()}

    # Function to calculate percentages for the first method across parameters
    def calculate_percentages(dataset, parameters, first_methods):
        location_percentages = {}
        for parameter_key, first_method in first_methods.items():
            locations = dataset.keys()
            percentage_first_method = []

            for loc in locations:
                df_loc = dataset[loc][parameters[parameter_key][0]]
                total_timesteps = len(df_loc)

                # Count timesteps where the first method is chosen
                first_method_count = (df_loc[first_method] == 1).sum()
                percentage_first_method.append((first_method_count / total_timesteps) * 100)

            # Average percentage across locations for the parameter
            location_percentages[parameter_key] = np.mean(percentage_first_method)
        return location_percentages

    # Calculate percentages for IM dataset
    location_percentages_IM = calculate_percentages(dict_choosen_method_IM, parameters, first_methods)

    # Prepare data for IM plotting
    parameters_list_IM = list(location_percentages_IM.keys())
    percentages_IM = list(location_percentages_IM.values())

    # Calculate percentages for Zinfo dataset
    location_percentages_Zinfo = calculate_percentages(dict_choosen_method_Zinfo, parameters, first_methods)

    # Prepare data for Zinfo plotting
    parameters_list_Zinfo = list(location_percentages_Zinfo.keys())
    percentages_Zinfo = list(location_percentages_Zinfo.values())

    # Create subplots with 2 rows and 1 column (stacked vertically)
    fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=1, figsize=(12, 9), dpi=300)

    # Plotting for IM dataset on the first subplot (ax1)
    bar_colors_IM = plot_colors[: len(parameters_list_IM)]
    bars_IM = ax1.bar(parameters_list_IM, percentages_IM, color=bar_colors_IM, width=0.6)
    ax1.set_xlabel("Parameters")
    ax1.set_ylabel("Percentage")
    ax1.set_title(
        "Percentage van de locaties waar altijd de eerste methode wordt gebruikt om de parameter te bepalen (IM-metingen)"
    )

    ax1.set_xticklabels(parameters_list_IM, rotation=45, ha="right")

    # Annotating percentages on top of the bars for IM
    for bar in bars_IM:
        height = bar.get_height()
        if height == 0:
            # When the bar height is 0, put the text above the bar
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                height + 1,
                f"{height:.1f}%",
                ha="center",
                va="bottom",
                fontsize=10,
                color="black",
                fontweight="bold",
            )
        else:
            # When the bar height is greater than 0, place the text inside the bar
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                height / 2,
                f"{height:.1f}%",
                ha="center",
                va="center",
                fontsize=10,
                color="black",
                fontweight="bold",
            )

    # Plotting for Zinfo dataset on the second subplot (ax2)
    bar_colors_Zinfo = plot_colors[: len(parameters_list_Zinfo)]
    bars_Zinfo = ax2.bar(parameters_list_Zinfo, percentages_Zinfo, color=bar_colors_Zinfo, width=0.6)
    ax2.set_xlabel("Parameters")
    ax2.set_ylabel("Percentage")
    ax2.set_title(
        "Percentage van de locaties waar altijd de eerste methode wordt gebruikt om de parameter te bepalen (Z-info)"
    )
    ax2.set_xticklabels(parameters_list_Zinfo, rotation=45, ha="right")

    # Annotating percentages on top of the bars for Zinfo
    for bar in bars_Zinfo:
        height = bar.get_height()
        if height == 0:
            # When the bar height is 0, put the text above the bar
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                height + 1,
                f"{height:.1f}%",
                ha="center",
                va="bottom",
                fontsize=10,
                color="black",
                fontweight="bold",
            )
        else:
            # When the bar height is greater than 0, place the text inside the bar
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                height / 2,
                f"{height:.1f}%",
                ha="center",
                va="center",
                fontsize=10,
                color="black",
                fontweight="bold",
            )

    # Adjust layout to make sure labels and titles fit
    plt.tight_layout()
    file_name = "percentage_locaties_eerste_methode.png"
    file_path = cloud.joinpath(figures_path, file_name)

    plt.savefig(file_path, dpi=500, bbox_inches="tight")
    cloud.upload_file(file_path)
    plt.show()

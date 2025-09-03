# %%

#######################################################################################################
#######################################################################################################
# Dit script is specifiek gemaakt om de koppeltabel zoals deze wordt gegeneerd door het script
# Transform_koppeltabel.py te updaten als de linkjes in deze koppeltabel nog niet correct zijn gekoppeld.

# In principe doet dit script hetzelfde als de functie die gedefineerd is in UpdateKoppeltabel.py, alleen
# Worden er andere kolom namen gebruikt, omdat het Transform_koppeltabel.py script, andere kolom namen genereerd
# als de nieuwe geometrie van een nieuw model wordt afgeleid.
#######################################################################################################
#######################################################################################################

# %%
# Packages
import ast
import os

import pandas as pd
from Ruimtelijke_koppeling_functies import search_geometry_nodes, search_type_nodes

# %%


def update_koppeltabel_with_feedback(
    input_koppeltabel_path,
    feedback_koppeltabel_path,
    lhm_model,
    output_path,
    cloud_sync=None,
    keep_all_columns=True,
    columns_to_keep=None,
    remove_meetreeksc=None,
    partij=None,
    add_new_data=False,
):
    """
    Updates the input koppeltabel based on the feedback koppeltabel, removes rows based on the specified MeetreeksC values, and saves the result.

    Parameters
    ----------
    - input_koppeltabel_path (str): Path to the input koppeltabel Excel file.
    - feedback_koppeltabel_path (str): Path to the feedback koppeltabel Excel file.
    - lhm_model (object): The LHM model containing link and node data.
    - output_path (str): Directory to save the updated koppeltabel. Default is the current directory.
    - columns_to_keep (list): List of column names to retain in the final output. Default is None.
    - keep_all_columns (bool): Whether to retain all columns in the final output. If False, only columns in `columns_to_keep` are retained.
    - remove_meetreeksc (list): List of "MeetreeksC" values to remove from the updated koppeltabel.
    - waterschap (str): Name of the waterboard to include in the output file name.
    - add_new_data (bool): Whether to append rows from the feedback koppeltabel to the input koppeltabel.

    Returns
    -------
    - pd.DataFrame: Updated koppeltabel DataFrame.
    """
    # Read the input and feedback koppeltabels
    input_koppeltabel = pd.read_excel(input_koppeltabel_path)
    feedback_koppeltabel = pd.read_excel(feedback_koppeltabel_path)

    # Add missing columns from feedback_koppeltabel to input_koppeltabel
    # Toevoegen als we een model voor een specifiek waterschaps model draaien
    # Dan is deze info niet aanwezig tov van een samengevoegd model
    missing_columns = set(input_koppeltabel.columns) - set(feedback_koppeltabel.columns)
    for col in missing_columns:
        feedback_koppeltabel[col] = None

    if add_new_data:
        input_koppeltabel = pd.concat([input_koppeltabel, feedback_koppeltabel], ignore_index=True)
        # input_koppeltabel = pd.concat([input_koppeltabel, feedback_koppeltabel])

    # Subset the feedback koppeltabel on rows where "link_id_correct" is not NaN
    feedback_koppeltabel = feedback_koppeltabel[~feedback_koppeltabel["link_id_correct"].isna()]

    # Extract static model link data
    links_model = lhm_model.link.df.reset_index()

    input_koppeltabel["status"] = None

    # Iterate through the feedback koppeltabel to match and update rows in the input koppeltabel
    for index, feedback_row in feedback_koppeltabel.iterrows():
        meetreeks_c = feedback_row["MeetreeksC"]

        # Find rows in input koppeltabel matching "MeetreeksC"
        matching_rows = input_koppeltabel[input_koppeltabel["MeetreeksC"] == meetreeks_c]

        for input_index, input_row in matching_rows.iterrows():
            link_ids = feedback_row["link_id_correct"]
            if not isinstance(link_ids, list):
                link_ids = (
                    ast.literal_eval(link_ids) if isinstance(link_ids, str) and link_ids.startswith("[") else [link_ids]
                )

            from_node_ids, to_node_ids, edge_ids = [], [], []

            for link_id in link_ids:
                link_data = links_model[links_model["link_id"] == link_id]
                if not link_data.empty:
                    # Alleen toevoegen als de kolom bestaat

                    if "from_node_id" in link_data.columns:
                        from_node_ids.extend(link_data["from_node_id"].tolist())

                    if "to_node_id" in link_data.columns:
                        to_node_ids.extend(link_data["to_node_id"].tolist())

                    if "meta_edge_id_waterbeheerder" in link_data.columns:
                        edge_ids.extend(link_data["meta_edge_id_waterbeheerder"].tolist())

            from_node_geometries = [search_geometry_nodes(lhm_model, node_id) for node_id in from_node_ids]
            to_node_geometries = [search_geometry_nodes(lhm_model, node_id) for node_id in to_node_ids]
            from_node_types = [search_type_nodes(lhm_model, node_id) for node_id in from_node_ids]
            to_node_types = [search_type_nodes(lhm_model, node_id) for node_id in to_node_ids]

            # Update columns in input koppeltabel
            input_koppeltabel.at[input_index, "new_link_id"] = link_ids if link_ids else None

            input_koppeltabel.at[input_index, "new_from_node_geometry"] = (
                from_node_geometries if from_node_geometries else None
            )

            input_koppeltabel.at[input_index, "new_to_node_geometry"] = (
                to_node_geometries if to_node_geometries else None
            )

            input_koppeltabel.at[input_index, "new_from_node_types"] = from_node_types if from_node_types else None

            input_koppeltabel.at[input_index, "new_to_node_types"] = to_node_types if to_node_types else None

            # Als we een update doen dan ook de status en de match_nodes aanpassen als we
            input_koppeltabel.at[input_index, "status"] = "Updated obv feedback"
            # input_koppeltabel.at[input_index, 'match_nodes'] = None

    # Remove rows with specified MeetreeksC values
    if remove_meetreeksc:
        input_koppeltabel = input_koppeltabel[~input_koppeltabel["MeetreeksC"].isin(remove_meetreeksc)]

    # Generate output file name
    folder, filename = os.path.split(input_koppeltabel_path)
    basename, ext = os.path.splitext(filename)
    base_without_feedback = basename.split("_Feedback")[0]

    if partij:
        new_filename = f"{base_without_feedback}_Feedback_Verwerkt_{partij}{ext}"
    else:
        new_filename = f"{base_without_feedback}_Feedback_Verwerkt{ext}"

    if cloud_sync:
        opslaan_path = cloud_sync.joinpath(output_path, new_filename)

    def update_specifiek_column(input_koppeltabel: pd.DataFrame, feedback_koppeltabel: pd.DataFrame) -> pd.DataFrame:
        # Check if "Specifiek" is in feedback
        if "Specifiek" not in feedback_koppeltabel.columns:
            return input_koppeltabel.copy()

        updated_df = input_koppeltabel.copy()

        if "Specifiek" in updated_df.columns:
            # Case 1: Update only matching rows
            updated_df = updated_df.merge(
                feedback_koppeltabel[["MeetreeksC", "Specifiek"]], on="MeetreeksC", how="left", suffixes=("", "_fb")
            )
            updated_df["Specifiek"] = updated_df["Specifiek_fb"].combine_first(updated_df["Specifiek"])
            updated_df = updated_df.drop(columns=["Specifiek_fb"])
        else:
            # Case 2: Add new Specifiek column
            updated_df = updated_df.merge(
                feedback_koppeltabel[["MeetreeksC", "Specifiek"]], on="MeetreeksC", how="left"
            )

        return updated_df

    input_koppeltabel = update_specifiek_column(input_koppeltabel, feedback_koppeltabel)

    # Welke columns behouden we:
    if not keep_all_columns:
        input_koppeltabel = input_koppeltabel[columns_to_keep]

    # # Sort the updated koppeltabel alphabetically by "Waterschap"
    # if 'Waterschap' in input_koppeltabel.columns:
    #     input_koppeltabel = input_koppeltabel.sort_values(by='Waterschap', ascending=True)

    input_koppeltabel.reset_index(drop=True, inplace=True)

    # Save the updated koppeltabel
    input_koppeltabel.to_excel(opslaan_path, index=False)

    if cloud_sync:
        cloud_sync.upload_file(opslaan_path)

    return input_koppeltabel

# %%
"""Script met verschillende functies om de uitvoer van de Ribasim modellen te vergelijken met meetreeksen"""

import ast
import os

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tqdm
from shapely import wkt

from ribasim_nl import CloudStorage
from ribasim_nl.aquo import waterbeheercode


def _parse_to_local(int_val: int, prefix_code: int):
    """The function `parse_to_local` removes a specified prefix code from an integer value."""
    str_val = str(int_val)
    if not str_val.startswith(f"{prefix_code}"):
        raise ValueError(f"Value {int_val} does not start with the specified prefix code {prefix_code}.")

    return int(str_val[len(str(prefix_code)) :])


def ParseList(val: int, prefix_code: int | None) -> list[int] | int:
    """The function `ParseList` checks if a given string represents a list and returns the list or the original value accordingly.

    Parameters
    ----------
    val: int
        The `ParseList` function takes a single parameter `val`, which is expected to be a string. The
    function checks if the string starts and ends with square brackets `[ ]`, indicating a list-like
    structure. If the string meets these conditions, it attempts to parse the string using
    `ast.literal_eval
    prefix_code : int | None
        The `prefix_code`, AQUO prefix. If specified it is to be removed from the integer values in the list.

    Returns
    -------
        The `ParseList` function is designed to parse a string representation of a list. If the input `val`
    is a string that starts with '[' and ends with ']', it attempts to evaluate the string using
    `ast.literal_eval` to convert it into a Python list. If the evaluation is successful and the result
    is a list, it returns the first element of the list if the list has only one element in it.

    """
    parsed = ast.literal_eval(val)

    if prefix_code is not None:
        parsed = [_parse_to_local(v, prefix_code) for v in parsed]

    # val = first item in list
    if len(parsed) == 1:
        return parsed[0]
    else:
        return parsed


def ReadOutputFile(model_folder, filetype) -> pd.DataFrame:
    """The function `ReadOutputFile` reads a specific type of output file from a model folder, adjusts the timing data, and returns the data.

    Parameters
    ----------
    model_folder
        The `model_folder` parameter is the directory where the model results are stored. It should be a
    string representing the path to the folder containing the model results.
    filetype
        The `filetype` parameter specifies the type of file to be read from the specified `model_folder`.
    The available file types are 'basin', 'flow', 'control', 'solver_stats', and 'basin_state'. If the
    provided `filetype` is not one of these options, a ValueError is raised.

    Returns
    -------
        The function `ReadOutputFile` reads a specific file type from the results folder within the
    provided model folder. It then adjusts the time data in the file by subtracting 12 years to correct
    a timing issue. Finally, it returns the data read from the file after the time adjustment.

    """
    possible_filetypes = ["basin", "flow", "control", "solver_stats", "basin_state"]
    if filetype.lower() not in possible_filetypes:
        raise ValueError(f"{filetype} not available. Choose on of the following: {possible_filetypes}")

    else:
        data = pd.read_feather(os.path.join(model_folder, "results", filetype.lower() + ".arrow"))

        # The timing is not yet correct between the measurements and the flow results. Subtract 12 years from the flow data to fix this
        # data['time'] = data['time'] - pd.DateOffset(years=12) #TODO repair!

        return data


def LaadKoppeltabel(loc_koppeltabel, apply_for_water_authority: str | None = None):
    """The function `LaadKoppeltabel` reads an Excel file, parses lists in the 'link_id' column, and converts the 'geometry' column to a geometry object.

    Parameters
    ----------
    loc_koppeltabel
        The `loc_koppeltabel` parameter in the `LaadKoppeltabel` function is expected to be a file location
    pointing to an Excel file that contains data for a koppeltabel (linking table).
    apply_for_water_authority
        Optional specification to read koppeltabel for a specific water authority. Defaults to None

    Returns
    -------
        The function returns the updated koppeltabel dataframe with the
    parsed columns 'link_id_parsed' and 'geometry_parsed'.

    """
    koppeltabel = pd.read_excel(loc_koppeltabel)

    # filter for water authority if specified
    if apply_for_water_authority is not None:
        koppeltabel = koppeltabel[koppeltabel["Waterschap"] == apply_for_water_authority]
        prefix_code = waterbeheercode[apply_for_water_authority]
    else:
        prefix_code = None

    # Convert the lists in link_id to lists if possible
    koppeltabel["link_id_parsed"] = koppeltabel["new_link_id"].apply(ParseList, args=(prefix_code,))

    # Parse the geometry
    koppeltabel["geometry_parsed"] = koppeltabel["geometry"].apply(lambda x: wkt.loads(x))

    return koppeltabel


def LaadSpecifiekeBewerking(loc_specifics) -> pd.DataFrame:
    """
    The function `LaadSpecifiekeBewerking` reads specific modifications from an Excel file and returns it.

    Parameters
    ----------
    loc_specifics:
        The `loc_specifics` parameter in the `LaadSpecifiekeBewerking` function is
        expected to be a file location pointing to an Excel file that contains specific modifications.

    Returns
    -------
    The function `LaadSpecifiekeBewerking` is returning the data read from an Excel file
    located at the path specified by the `loc_specifics` parameter as a pandas DataFrame
    """
    specifics = pd.read_excel(loc_specifics, header=0)

    return specifics


def get_unique(items):
    seen = []
    for item in items:
        if not any(item == s for s in seen):
            seen.append(item)
    return seen


def ApplySpecificOperation(data: pd.DataFrame, link: list | int, spec_op: str):
    """
    The function `ApplySpecificOperation`  performs specific operations on the input data.

    Parameters
    ----------
    data : pd.DataFrame
        The function `ApplySpecificOperation` takes in a pandas DataFrame `data`, a list of link IDs
    `link`, and a specific operation `spec_op` to be applied on the data. The function then performs
    different operations based on the value of `spec_op`.
    link : list | int
        The `link` parameter in the function `ApplySpecificOperation` is used to specify the link or links
    for which the specific operation will be applied. It can be either an integer or a list of integers
    representing the link IDs. If it is an integer, it will be converted to a list containing
    spec_op : str
        The `spec_op` parameter in the `ApplySpecificOperation` function represents a specific operation
    that can be performed on the data based on the provided conditions. The function uses a `match`
    statement to determine the specific operation to be applied. The possible values for `spec_op` and
    their corresponding operations are 'optellen','optellen_en_negatief_maken','negatief_maken', np.nan, or a
    custom formula. A custom formula needs to refer to the specific links as link1, link2 etc.

    Returns
    -------
    A subset of the data with a special operation applied (if any)
    """
    # Make sure the link is a list
    if isinstance(link, int):
        link = [link]

    # There are four options for the specific operation
    match spec_op:
        case "optellen":
            # Tel de links bij elkaar op wanneer de specifieke bewerking hierom vraagt
            subset_links = data[data["link_id"].isin(link)]
            subset_output = subset_links.groupby("time", as_index=False)["flow_rate"].sum()

        case "negatief_maken":
            # Maak de meetreeks negatief
            subset_output = data[data["link_id"].isin(link)].copy()
            subset_output["flow_rate"] = subset_output["flow_rate"] * -1

        case "optellen_en_negatief_maken":
            # Tel op en maak de reeks negatief
            subset_links = data[data["link_id"].isin(link)]
            subset_output = subset_links.groupby("time", as_index=False)["flow_rate"].sum().copy()
            subset_output["flow_rate"] = subset_output["flow_rate"] * -1

        case _ if pd.isna(spec_op):
            # Als er geen specifieke bewerking nodig is, selecteer de link
            subset_output = data[data["link_id"].isin(link)]

        case _:
            # Handel de specifieke formule af
            # Create a mapping between link numbers and link IDs
            link_mapping = {f"link{i + 1}": ID for i, ID in enumerate(link)}

            # Select the dataframe with the specified links
            subset_links = data[data["link_id"].isin(link)]
            subset_pivot = subset_links.pivot(index="time", columns="link_id", values="flow_rate")
            env = {placeholder: subset_pivot[link_id] for placeholder, link_id in link_mapping.items()}

            # Evaluate the formula and add the results to the pivoted dataframe
            subset_pivot["result"] = eval(spec_op, {}, env)

            # Rename the results to 'flow_rate'
            subset_output = subset_pivot["result"].reset_index().rename(columns={"result": "flow_rate"})

    return subset_output


def AddCumulative(data, decade=False):
    """
    Calculates cumulative flow rates and sums for model output and measurements, respectively, with an option to adjust for decadal data.

    Parameters
    ----------
    data
        The `data` parameter is a DataFrame containing the measurement data (column 'sum') and model data (column 'flow_rate')
    decade, optional
        The `decade` parameter is a boolean parameter that determines whether the data should be treated as decadal data. If `decade` is set to `True`, the function will
    multiply the cumulative values by 10 to represent decadal data.

    Returns
    -------
        The function `AddCumulative` returns a new dataset with two additional columns: "flow_rate_cum" and
    "sum_cum". The "flow_rate_cum" column contains the cumulative sum of the "flow_rate" data multiplied
    by a multiplier (which is 86400 by default, or 864000 if the `decade` parameter is set to True). The
    "sum_cum" column contains the cumulative sum of the measurements in the column 'sum'

    """
    # If decadal data is given, multiply by 10 to get the cumulative graph
    multiplier_s_to_d = 86400
    if decade:
        multiplier_s_to_d *= 10

    # Get the cumulative data for both the model output and the measurements
    new_data = data.copy()

    # Get cumulative of model output in
    new_data["flow_rate_cum"] = np.cumsum(new_data["flow_rate"]) * multiplier_s_to_d

    # Get cumulative data of measurements by filling the gaps
    new_data["sum_cum"] = np.cumsum(new_data["sum"].ffill()) * multiplier_s_to_d

    return new_data


def CompareOutputMeasurements(
    loc_koppeltabel,
    loc_specifics,
    meas_folder,
    model_folder,
    filetype="flow",
    apply_for_water_authority: str | None = None,
    save_results_combined: bool = False,
) -> None:
    """Compares model output measurements with actual measurements, calculates statistics, and saves the results in a geopackage per waterboard as well as producing the necessary figures.

    Parameters
    ----------
    loc_koppeltabel
        The location of the koppeltabel (Excel file):
    loc_specifics
        The location of the table with specific operations per measurement (Excel file)
    meas_folder
        The `meas_folder` refers to the folder location where measurements data is stored.
    model_folder
        The `model_folder` refers to the directory path where the model is stored.
    filetype
        The `filetype` parameter specifies the type of output file to be read from the `model_folder`.
        By default, it is set to `'flow'`, but you can change it to
    apply_for_water_authority
        Optional specification to read koppeltabel for a specific water authority. Defaults to None
    save_results_combined
        Optional boolean to check if all the results are to be written to a single layer in a geopackage instead of
        one per water authority. Defaults to False

    Returns
    -------
    The function returns nothing, but saves the results in .png figures and a geopackage

    """
    koppeltabel = LaadKoppeltabel(loc_koppeltabel, apply_for_water_authority=apply_for_water_authority)
    specifics = LaadSpecifiekeBewerking(loc_specifics)
    data = ReadOutputFile(model_folder, filetype)

    measurements = LoadMeasurements(meas_folder)

    results_measurements = {}
    results_measurements_decade = {}
    # Get the unique link ids
    unique_links = get_unique(koppeltabel["link_id_parsed"])

    for link in tqdm.tqdm(unique_links, total=len(unique_links), desc="Verwerken metingen"):
        try:
            if np.isnan(link):
                print("A link with type NaN has been found. Skipped.")
                continue
        except:  # noqa: E722 TODO: do not use bare except
            None

        # for n, meetlocatie in tqdm.tqdm(koppeltabel.iterrows(), total=len(koppeltabel), desc='Verwerken metingen'):
        mask = koppeltabel["link_id_parsed"].apply(lambda x: x == link)
        meetlocaties_link = koppeltabel[mask]

        # Create a result dictionary per waterschap
        if meetlocaties_link.iloc[0]["Waterschap"] not in results_measurements.keys():
            results_measurements[meetlocaties_link.iloc[0]["Waterschap"]] = {
                "koppelinfo": [],
                "waterschap": [],
                "link_id": [],
                "NSE": [],
                "RMSE": [],
                "MAE": [],
                "geometry": [],
                "figure_path": [],
            }

            results_measurements_decade[meetlocaties_link.iloc[0]["Waterschap"]] = {
                "koppelinfo": [],
                "waterschap": [],
                "link_id": [],
                "NSE": [],
                "RMSE": [],
                "MAE": [],
                "geometry": [],
                "figure_path": [],
            }

        # Loop over the locations in case two need to be summed
        # Two locs cannot be summed if the type of discharge differs
        if len(np.unique(meetlocaties_link["Aan/Af"])) > 1:
            continue  # TODO
        else:
            if meetlocaties_link.iloc[0]["Aan/Af"] == "Aanvoer":
                dagmetingen = measurements["aanvoer_dag"]
                # decademetingen = measurements['aanvoer_decade']
            if (meetlocaties_link.iloc[0]["Aan/Af"] == "Afvoer") or (meetlocaties_link.iloc[0]["Aan/Af"] == "Aan&Af"):
                dagmetingen = measurements["afvoer_dag"]
                # decademetingen = measurements['afvoer_decade']

        existing_measurements = [col for col in meetlocaties_link["MeetreeksC"] if col in dagmetingen.columns]
        missing_measurements = [col for col in meetlocaties_link["MeetreeksC"] if col not in dagmetingen.columns]
        if len(missing_measurements) > 0:
            for col in missing_measurements:
                print(f"Cannot find the daily measurements for {col}")

        subset_measurements = dagmetingen[["time"] + existing_measurements].copy()

        # If multiple measurements series refer to the same link, take the sum of the measurements
        subset_measurements["sum"] = subset_measurements[existing_measurements].sum(axis=1, min_count=1)

        # Get the specific operations for the measurements
        subset_specs = specifics[
            (specifics["MeetreeksC"].isin(existing_measurements))
            & (specifics["Aan/Af"] == meetlocaties_link.iloc[0]["Aan/Af"])
        ]

        if link == 8059555:
            None
        if len(pd.Series.unique(subset_specs["Specifiek"])) > 1:
            print(
                f"Two different operations found for the same set of links. A.o. in measurements {existing_measurements}"
            )
            continue

        # Set the specific operation
        spec_op = subset_specs["Specifiek"].iloc[0]

        # If a list of links is present, a specific operation is required.
        if isinstance(link, list) & pd.isna(spec_op):
            print(f"No specific operation found for measurements {existing_measurements}, around link {link}")
            continue

        # Apply the special operation to get the subset of model output
        subset_modeloutput = ApplySpecificOperation(data, link, spec_op)

        # Combine the measurements with the modeloutput in one dataframe
        combined_df = subset_modeloutput.merge(subset_measurements[["time", "sum"]], on=["time"], how="left")

        # Check whether there are any measurements at all during the period
        if combined_df["sum"].isna().all().all():
            print(
                f"No measured data available for the requested time period for link {link}, a.o. location {meetlocaties_link.iloc[0]['MeetreeksC']}"
            )
            continue

        combined_df_decade = ConvertToDecade(combined_df)

        # Calculate the required statistics
        stats = GetStatisticsComparison(combined_df)
        stats_dec = GetStatisticsComparison(combined_df_decade)

        # Add the cumulative discharges to the dataframe
        combined_df_cum = AddCumulative(combined_df)
        combined_df_decade_cum = AddCumulative(combined_df_decade, decade=True)

        # Deal with the daily values
        full_title = " - ".join(existing_measurements)
        fig_name = full_title.split(" - ")[0]
        bron_meting = None if apply_for_water_authority is not None else meetlocaties_link.iloc[0]["Waterschap"]
        PlotAndSave(
            combined_df=combined_df_cum,
            stats=stats,
            koppelinfo=full_title,
            fig_name=fig_name,
            bron_meting=bron_meting,
            output_folder=os.path.join(model_folder, "results", "figures"),
        )
        fig_name_clean = fig_name.replace(" ", "_")
        pop_up_figure = f'<img src="../figures/{meetlocaties_link.iloc[0]["Waterschap"]}/{fig_name_clean}.png" width=400 height=300>'

        # Save the resulting statistics per measurement
        results_measurements[meetlocaties_link.iloc[0]["Waterschap"]]["koppelinfo"].append(fig_name_clean)
        results_measurements[meetlocaties_link.iloc[0]["Waterschap"]]["link_id"].append(link)
        results_measurements[meetlocaties_link.iloc[0]["Waterschap"]]["MAE"].append(stats["MAE"])
        results_measurements[meetlocaties_link.iloc[0]["Waterschap"]]["NSE"].append(stats["NSE"])
        results_measurements[meetlocaties_link.iloc[0]["Waterschap"]]["RMSE"].append(stats["RMSE"])
        results_measurements[meetlocaties_link.iloc[0]["Waterschap"]]["geometry"].append(
            meetlocaties_link.iloc[0]["geometry_parsed"]
        )
        results_measurements[meetlocaties_link.iloc[0]["Waterschap"]]["waterschap"].append(
            meetlocaties_link.iloc[0]["Waterschap"]
        )
        results_measurements[meetlocaties_link.iloc[0]["Waterschap"]]["figure_path"].append(pop_up_figure)

        # Save the results per decade
        full_title = " - ".join(existing_measurements)
        fig_name = full_title.split(" - ")[0] + "_decade"
        PlotAndSave(
            combined_df=combined_df_decade_cum,
            stats=stats_dec,
            koppelinfo=full_title,
            fig_name=fig_name,
            bron_meting=bron_meting,
            output_folder=os.path.join(model_folder, "results", "figures"),
        )
        fig_name_clean = fig_name.replace(" ", "_")
        pop_up_figure = f'<img src="../figures/{meetlocaties_link.iloc[0]["Waterschap"]}/{fig_name_clean}.png" width=400 height=300>'

        # Save the resulting statistics per measurement
        results_measurements_decade[meetlocaties_link.iloc[0]["Waterschap"]]["koppelinfo"].append(fig_name_clean)
        results_measurements_decade[meetlocaties_link.iloc[0]["Waterschap"]]["link_id"].append(link)
        results_measurements_decade[meetlocaties_link.iloc[0]["Waterschap"]]["MAE"].append(stats_dec["MAE"])
        results_measurements_decade[meetlocaties_link.iloc[0]["Waterschap"]]["NSE"].append(stats_dec["NSE"])
        results_measurements_decade[meetlocaties_link.iloc[0]["Waterschap"]]["RMSE"].append(stats_dec["RMSE"])
        results_measurements_decade[meetlocaties_link.iloc[0]["Waterschap"]]["geometry"].append(
            meetlocaties_link.iloc[0]["geometry_parsed"]
        )
        results_measurements_decade[meetlocaties_link.iloc[0]["Waterschap"]]["waterschap"].append(
            meetlocaties_link.iloc[0]["Waterschap"]
        )
        results_measurements_decade[meetlocaties_link.iloc[0]["Waterschap"]]["figure_path"].append(pop_up_figure)

    results_combined = []
    results_dec_combined = []

    # Save the results in a geopackage per waterboard
    for waterschap, results in results_measurements.items():
        results_gdf = gpd.GeoDataFrame(results, geometry="geometry")
        results_gdf.set_crs(epsg="28992", inplace=True)
        results_combined.append(results_gdf)
        results_gdf.to_file(os.path.join(model_folder, "results", "Validatie_resultaten.gpkg"), layer=waterschap)

    for waterschap, results in results_measurements_decade.items():
        results_gdf = gpd.GeoDataFrame(results, geometry="geometry")
        results_gdf.set_crs(epsg="28992", inplace=True)
        results_dec_combined.append(results_gdf)
        results_gdf.to_file(os.path.join(model_folder, "results", "Validatie_resultaten_dec.gpkg"), layer=waterschap)

    # Save all the results in one geopackage to make handling in QGIS or HTML easier
    if save_results_combined:
        final_gdf = pd.concat(results_combined, ignore_index=True)
        final_dec_gdf = pd.concat(results_dec_combined, ignore_index=True)

        final_gdf.to_file(os.path.join(model_folder, "results", "Validatie_resultaten_all.gpkg"), layer="Compleet")
        final_dec_gdf.to_file(
            os.path.join(model_folder, "results", "Validatie_resultaten_dec_all.gpkg"), layer="Compleet"
        )


def ConvertToDecade(combined_df_results):
    def get_decade(ts):
        day = ts.day
        if day <= 10:
            return 1
        elif day <= 20:
            return 2
        else:
            return 3

    combined_df_results["Time"] = pd.to_datetime(combined_df_results["time"])  # ensure Time is datetime

    combined_df_results["year"] = combined_df_results["time"].dt.year
    combined_df_results["month"] = combined_df_results["time"].dt.month
    combined_df_results["decade"] = combined_df_results["time"].apply(get_decade)

    # Group by year-month-decade
    grouped = combined_df_results.groupby(["year", "month", "decade"], as_index=False).agg(
        {
            "flow_rate": "mean",  # or 'sum', depending on what you want
            "sum": "mean",  # adjust aggregation as needed
        }
    )

    # Optional: combine into a proper timestamp (e.g. midpoint of the decade)
    def build_decade_date(row):
        day = {1: 1, 2: 11, 3: 21}[row["decade"]]
        return pd.Timestamp(year=int(row["year"]), month=int(row["month"]), day=day)

    grouped["time"] = grouped.apply(build_decade_date, axis=1)

    # Reorder and set dtypes to match original
    result = grouped[["time", "flow_rate", "sum"]].copy()
    result["time"] = pd.to_datetime(result["time"])
    result["flow_rate"] = result["flow_rate"].astype("float64")
    result["sum"] = result["sum"].astype("float64")

    return result


def LoadMeasurements(meas_folder) -> dict:
    """The function `LoadMeasurements` reads measurement files from a specified folder, parses date columns, and returns a dictionary of measurements.

    Parameters
    ----------
    meas_folder
        The `meas_folder` is a string that represents the folder path where the measurement files are located.

    Returns
    -------
        The function `LoadMeasurements` returns a dictionary `measurements` containing different types of
    measurements loaded from CSV files located in the specified `meas_folder`. The keys in the
    dictionary correspond to the types of measurements (e.g., 'aanvoer_dag', 'aanvoer_decade',
    'afvoer_dag', 'afvoer_decade'), and the values are pandas DataFrames.

    """
    # Define the different measurement files
    meas_files = {
        "aanvoer_dag": "Metingen_aanvoer_dag_totaal.csv",
        #'aanvoer_decade': 'Metingen_aanvoer_decade.csv',
        "afvoer_dag": "Metingen_afvoer_dag_totaal.csv",
        #'afvoer_decade':  'Metingen_afvoer_decade.csv'
    }

    measurements = {}
    for key, file in meas_files.items():
        try:
            measurements[key] = pd.read_csv(os.path.join(meas_folder, file), parse_dates=["Unnamed: 0"])
            measurements[key].rename(columns={"Unnamed: 0": "time"}, inplace=True)
        except:  # noqa: E722 TODO: specify exception
            try:
                measurements[key] = pd.read_csv(os.path.join(meas_folder, file), parse_dates=["Datum"])
                measurements[key].rename(columns={"Datum": "time"}, inplace=True)
            except ValueError:
                print("Cannot identify date/time column. Can be [Unnamed: 0, Datum] ")

    return measurements


def PlotAndSave(combined_df, stats, koppelinfo, fig_name, bron_meting, output_folder):
    """Plots data from a DataFrame, adds statistical information, and saves the plot as an image in a specified folder.

    Parameters
    ----------
    combined_df
        `combined_df` is a DataFrame containing data to be plotted.
    stats
        The `stats` parameter in the `PlotAndSave` function contains statistical information such as
    NSE (Nash-Sutcliffe Efficiency), RMSE (Root Mean Square Error), and MAE (Mean Absolute Error)
    calculated for the data being plotted.
    koppelinfo
        The `koppelinfo` parameter in the `PlotAndSave` function supplies the name of the measurement location. This is used as title and filename.
    bron_meting
        The `bron_meting` parameter in the `PlotAndSave` function represents the origin of the measurement (waterboard).
        It is used to create a folder within the `output_folder` where the plot will be saved.
    output_folder
        The `output_folder` parameter in the `PlotAndSave` function is the directory where the plot will be
        saved. It is the location where the folder for the specific `bron_meting` will be created, and
        within that folder, the plot will be saved as a PNG file with the name supplied by 'koppelinfo'

    """
    # Create the folder where the plot can be saved
    if bron_meting is None:
        os.makedirs(output_folder, exist_ok=True)
    else:
        os.makedirs(os.path.join(output_folder, bron_meting), exist_ok=True)

    font = "Arial"

    # Set up the figure
    fig, ax = plt.subplots(figsize=(6, 6))
    (model_line,) = ax.plot(combined_df["time"], combined_df["flow_rate"], label="Ribasim", color="tab:blue")
    (meas_line,) = ax.plot(combined_df["time"], combined_df["sum"], label="Meting", color="tab:orange")
    ax.grid()
    ax.set_xticks(ticks=ax.get_xticks(), labels=ax.get_xticklabels(), rotation=45, fontname=font)
    # ax.set_xticklabels(labels=ax.get_xticklabels())
    ax.set_ylabel("Debiet [m$^3$/s]", fontdict={"fontsize": 12, "fontname": font})
    ax.set_title(koppelinfo, fontname=font, wrap=True)

    # Set the cumulative axis
    ax2 = ax.twinx()
    (model_cum_line,) = ax2.plot(
        combined_df["time"],
        combined_df["flow_rate_cum"] / 1_000_000,
        linestyle="--",
        color="#0047b3",
        label="Rib. cum.",
    )
    (meas_cum_line,) = ax2.plot(
        combined_df["time"], combined_df["sum_cum"] / 1_000_000, linestyle="--", color="#cc6600", label="Meting cum."
    )
    ax2.set_ylabel("Cumulatief debiet [Mm$^3$]")
    lines = [model_line, meas_line, model_cum_line, meas_cum_line]
    labels = [line.get_label() for line in lines]

    ax.legend(lines, labels, prop={"family": font}, loc="lower left", bbox_to_anchor=(1.10, 0))

    # Add a textbox with the right statistics
    fig_text = rf"""
    Stats:
    $\bf{{NSE}}$:
    {np.round(stats["NSE"], 2)}

    $\bf{{RSME}}$:
    {np.round(stats["RMSE"], 2)}

    $\bf{{MAE}}$:
    {np.round(stats["MAE"], 2)}
    """
    fig.text(
        1.05,
        0.90,
        fig_text,
        ha="left",
        va="top",
        fontdict={"fontsize": 12, "fontname": font},
        bbox={"facecolor": "white", "edgecolor": "darkgrey", "boxstyle": "round"},
    )
    # Save the figure in the right location
    fig_name_clean = fig_name.replace(" ", "_").replace("/", "_")  # remove / as it will raise FileNotFoundError
    if bron_meting is None:
        fig_path = os.path.join(output_folder, fig_name_clean + ".png")
    else:
        fig_path = os.path.join(output_folder, bron_meting, fig_name_clean + ".png")
    fig.savefig(fig_path, bbox_inches="tight", dpi=200)
    plt.close()


def GetStatisticsComparison(combined_df):
    """Calculates and returns statistics such as NSE, RMSE, and MAE based on the input combined dataframe.

    Parameters
    ----------
    combined_df
        The function takes `combined_df` as input, which contains columns for the target values and model output values for a particular task. The target values are
    stored in the last column of the DataFrame, and the model output values are stored in a column named 'flow_rate'

    Returns
    -------
        The function `GetStatisticsComparison` returns a dictionary containing the calculated statistics
    for the given input data in the `combined_df`. The statistics included in the dictionary are NSE
    (Nash-Sutcliffe Efficiency), RMSE (Root Mean Squared Error), and MAE (Mean Absolute Error).

    """
    # Calculate different statistics over the measurements
    # First, initialise a stats dict
    stats = {}

    # NSE
    targets = combined_df["sum"]
    model_output = combined_df["flow_rate"]

    denom = np.sum((targets - np.mean(targets)) ** 2)
    if denom != 0:
        stats["NSE"] = 1 - (np.sum((targets - model_output) ** 2) / denom)
    else:
        stats["NSE"] = -999

    # RMSE
    stats["RMSE"] = np.sqrt(np.mean((model_output - targets) ** 2))

    # MAE
    stats["MAE"] = np.mean(abs(model_output - targets))

    return stats


if __name__ == "__main__":
    # init cloud
    cloud = CloudStorage()

    # # specify koppeltabel and meas_folder
    loc_koppeltabel = cloud.joinpath(
        "Basisgegevens/resultaatvergelijking/koppeltabel/Transformed_koppeltabel_versie_lhm_coupled_2025_9_0_Feedback_Verwerkt_HydroLogic.xlsx"
    )
    loc_specifieke_bewerking = cloud.joinpath(
        "Basisgegevens/resultaatvergelijking/koppeltabel/Specifiek_bewerking_versielhm_coupled_2025_9_0.xlsx"
    )
    meas_folder = cloud.joinpath("Basisgegevens/resultaatvergelijking/meetreeksen")

    # get latest coupled LHM model
    rws_model_versions = cloud.uploaded_models(authority="Rijkswaterstaat")
    latest_lhm_version = sorted(
        [i for i in rws_model_versions if i.model == "lhm_coupled"], key=lambda x: getattr(x, "sorter", "")
    )[-1]
    model_folder = cloud.joinpath("Rijkswaterstaat/modellen", latest_lhm_version.path_string)

    # # synchronize paths
    cloud.synchronize([loc_koppeltabel, loc_specifieke_bewerking, meas_folder, model_folder])

    # run compare output measurements
    CompareOutputMeasurements(
        loc_koppeltabel=loc_koppeltabel,
        loc_specifics=loc_specifieke_bewerking,
        meas_folder=meas_folder,
        model_folder=model_folder,
        filetype="flow",
        save_results_combined=True,
    )

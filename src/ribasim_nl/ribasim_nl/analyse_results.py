# %%
"""Script met verschillende functies om de uitvoer van de Ribasim modellen te vergelijken met meetreeksen"""

import ast
import operator
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import openpyxl
import pandas as pd
import tqdm
import xarray as xr
from openpyxl.styles import PatternFill
from shapely import wkt

from ribasim_nl import CloudStorage
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.html_viewer import CreateHTMLViewer

# from aquo import waterbeheercode


def _sanitize_filename(name: str) -> str:
    r"""Verwijdert of vervangt tekens die niet geldig zijn in bestandsnamen op Windows/Linux.

    Spaties worden underscores, alle overige ongeldige tekens (/ \\ : * ? " < > | ( ) , )
    worden weggelaten.
    """
    name = name.replace(" ", "_").replace("/", "_")
    invalid = r'\:*?"<>|()'
    for ch in invalid:
        name = name.replace(ch, "")
    # Verwijder komma's en eventuele opeenvolgende underscores die achterblijven
    name = name.replace(",", "")
    while "__" in name:
        name = name.replace("__", "_")
    return name.strip("_")


def _parse_to_local(int_val: int, prefix_code: int) -> int:
    """The function `parse_to_local` removes a specified prefix code from an integer value."""
    str_val = str(int_val)
    if not str_val.startswith(f"{prefix_code}"):
        return int_val

    return int(str_val[len(str(prefix_code)) :])


def ParseList(val: str, prefix_code: int | None) -> list[int] | int:
    """The function `ParseList` checks if a given string represents a list and returns the list or the original value accordingly.

    Parameters
    ----------
    val: str
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


def ReadOutputFile(
    model_folder,
    filetype,
    output_is_feather: bool = True,
    output_is_nc: bool = False,
    resample_to_daily: bool = True,
) -> pd.DataFrame:
    """Leest een Ribasim outputbestand en retourneert een DataFrame met daggemiddelden.

    Parameters
    ----------
    model_folder
        Map van het Ribasim-model; outputbestanden worden gelezen uit ``{model_folder}/results/``.
    filetype
        Type outputbestand. Mogelijke waarden: ``'basin'``, ``'flow'``, ``'control'``,
        ``'solver_stats'``, ``'basin_state'``.
    output_is_feather
        Lees het bestand als Arrow/Feather (``.arrow``). Standaard ``True``.
    output_is_nc
        Lees het bestand als NetCDF (``.nc``). Standaard ``False``.
    resample_to_daily
        Als ``True`` (standaard) wordt het tijdstap van de data gedetecteerd en automatisch
        naar daggemiddelden geresamplet als het interval korter dan één dag is. Zet op
        ``False`` als de data al dagwaarden bevat en je de hersampling wilt overslaan.

    Returns
    -------
    pd.DataFrame
        DataFrame met kolommen ``time``, ``link_id`` (of vergelijkbaar) en waarden,
        altijd op dagresolutie wanneer ``resample_to_daily=True``.
    """
    possible_filetypes = ["basin", "flow", "control", "solver_stats", "basin_state"]
    if filetype.lower() not in possible_filetypes:
        raise ValueError(f"{filetype} not available. Choose one of the following: {possible_filetypes}")

    if output_is_feather and output_is_nc:
        raise ValueError("Both output_is_feather and output_is_nc are True. Choose only one.")
    if not output_is_feather and not output_is_nc:
        raise ValueError("Both output_is_feather and output_is_nc are False. Choose one format.")

    results_folder = Path(model_folder, "results")

    if output_is_feather:
        data = pd.read_feather(Path(results_folder, filetype.lower() + ".arrow"))
    else:
        ds = xr.open_dataset(Path(results_folder, filetype.lower() + ".nc"))
        data = ds.to_dataframe().reset_index()

    # Optional: fix timing if needed
    # data['time'] = data['time'] - pd.DateOffset(years=12)

    if resample_to_daily:
        data = _resample_to_daily(data)

    return data


def _resample_to_daily(data: pd.DataFrame) -> pd.DataFrame:
    """Resamplet een lang-formaat DataFrame naar daggemiddelden als het tijdstap sub-dagelijks is.

    Detecteert het mediane tijdstap en doet niets als de data al op dagresolutie staat.
    Werkt voor elk lang-formaat outputbestand: groepeert op alle niet-tijd/niet-waarde kolommen
    en middelt de numerieke waardekolommen per dag.
    """
    if "time" not in data.columns:
        return data

    data["time"] = pd.to_datetime(data["time"])
    unique_times = data["time"].drop_duplicates().sort_values()
    if len(unique_times) < 2:
        return data

    median_step = (unique_times.diff().dropna()).median()
    one_day = pd.Timedelta(days=1)

    if median_step >= one_day:
        # Al dagelijks of grover — niets te doen
        return data

    print(f"  Tijdstap gedetecteerd: {median_step}. Resampling naar daggemiddelden.")

    # Bepaal welke kolommen groepeersleutels zijn (niet-numeriek of identifier) en welke waarden
    id_cols = [
        c for c in data.columns if c != "time" and (data[c].dtype == object or not pd.api.types.is_float_dtype(data[c]))
    ]
    val_cols = [c for c in data.columns if c != "time" and c not in id_cols]

    data = data.groupby([*id_cols, pd.Grouper(key="time", freq="D")])[val_cols].mean().reset_index()

    return data


def LaadKoppeltabel(loc_koppeltabel, apply_for_water_authority: str | None = None) -> pd.DataFrame:
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
    located at the path specified by the `loc_specifics` parameter as a pd.DataFrame
    """
    specifics = pd.read_excel(loc_specifics, header=0)

    return specifics


def get_unique(items):
    seen = []
    for item in items:
        if not any(item == s for s in seen):
            seen.append(item)
    return seen


def _safe_eval_formula(formula: str, env: dict) -> pd.Series:
    """Evaluate a simple arithmetic formula string safely using AST.

    Only allows numeric constants, variables present in `env`, and +/-/* / operators.
    """
    _bin_ops = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
    }
    _unary_ops = {
        ast.USub: operator.neg,
    }

    def _eval(node):
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Constant):
            if not isinstance(node.value, (int, float)):
                raise ValueError(f"Only numeric constants allowed: {node.value}")
            return node.value
        if isinstance(node, ast.Name):
            if node.id not in env:
                raise ValueError(f"Unknown variable: {node.id}")
            return env[node.id]
        if isinstance(node, ast.BinOp):
            op = _bin_ops.get(type(node.op))  # pyrefly: ignore[bad-argument-type]
            if op is None:
                raise ValueError(f"Unsupported operator: {type(node.op)}")
            return op(_eval(node.left), _eval(node.right))
        if isinstance(node, ast.UnaryOp):
            op = _unary_ops.get(type(node.op))  # pyrefly: ignore[bad-argument-type]
            if op is None:
                raise ValueError(f"Unsupported operator: {type(node.op)}")
            return op(_eval(node.operand))
        raise ValueError(f"Unsupported expression type: {type(node)}")

    return _eval(ast.parse(formula, mode="eval"))


def ApplySpecificOperation(data: pd.DataFrame, link: list | int, spec_op: str):
    """
    The function `ApplySpecificOperation`  performs specific operations on the input data.

    Parameters
    ----------
    data : pd.DataFrame
        The function `ApplySpecificOperation` takes in a pd.DataFrame `data`, a list of link IDs
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
            # pyrefly: ignore[bad-assignment]
            subset_output: pd.DataFrame = subset_links.groupby("time", as_index=False)["flow_rate"].sum()

        case "negatief_maken":
            # Maak de meetreeks negatief
            subset_output = data[data["link_id"].isin(link)].copy()
            subset_output["flow_rate"] = subset_output["flow_rate"] * -1

        case "optellen_en_negatief_maken":
            # Tel op en maak de reeks negatief
            subset_links = data[data["link_id"].isin(link)]
            # pyrefly: ignore[bad-assignment]
            subset_output = subset_links.groupby("time", as_index=False)["flow_rate"].sum().copy()
            subset_output["flow_rate"] = subset_output["flow_rate"] * -1

        case _ if pd.isna(spec_op):
            # Als er geen specifieke bewerking nodig is, selecteer de link
            subset_output = data[data["link_id"].isin(link)]

        case _:
            # Handel de specifieke formule af
            link_mapping = {f"link{i + 1}": ID for i, ID in enumerate(link)}
            subset_links = data[data["link_id"].isin(link)]
            subset_pivot = subset_links.pivot(index="time", columns="link_id", values="flow_rate")
            env = {placeholder: subset_pivot[link_id] for placeholder, link_id in link_mapping.items()}
            subset_pivot["result"] = _safe_eval_formula(spec_op, env)
            subset_output = subset_pivot["result"].reset_index().rename(columns={"result": "flow_rate"})

    return subset_output


def AddCumulative(data, decade=False):
    """
    Calculates cumulative flow rates and sums for model output and measurements, respectively, with an option to adjust for decadal data.

    Parameters
    ----------
    data
        The `data` parameter is a pd.DataFrame containing the measurement data (column 'sum') and model data (column 'flow_rate')
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
    new_data["sum_cum"] = new_data["sum"].fillna(0).cumsum() * multiplier_s_to_d

    return new_data


def GetStatisticsComparison(combined_df: pd.DataFrame) -> dict:
    """Berekent NSE, RMSE, MAE, relatieve bias, KGE en percentielen (P10/P25/P75/P90).

    Berekening op basis van de gecombineerde model- en meetreeks DataFrame.

    Parameters
    ----------
    combined_df
        DataFrame met kolommen 'flow_rate' (model) en 'sum' (meting).
        Rijen met NaN in 'sum' worden uitgesloten van de berekening.

    Returns
    -------
    dict met alle statistieken.
    """
    stats: dict[str, float | int] = {}

    # Verwijder tijdstappen zonder meting
    valid = combined_df.dropna(subset=["sum"])
    targets = np.asarray(valid["sum"], dtype=float)
    model_output = np.asarray(valid["flow_rate"], dtype=float)

    if len(targets) == 0:
        return dict.fromkeys(
            [
                "n_obs",
                "NSE",
                "RMSE",
                "MAE",
                "RelBias",
                "KGE",
                "P10_obs",
                "P10_sim",
                "P10_reldev",
                "P25_obs",
                "P25_sim",
                "P25_reldev",
                "P75_obs",
                "P75_sim",
                "P75_reldev",
                "P90_obs",
                "P90_sim",
                "P90_reldev",
            ],
            np.nan,
        )

    # --- Aantal geldige observaties ---
    stats["n_obs"] = len(targets)

    # --- NSE ---
    denom = np.sum((targets - np.mean(targets)) ** 2)
    stats["NSE"] = float(1 - np.sum((targets - model_output) ** 2) / denom) if denom != 0 else -999.0

    # --- RMSE ---
    stats["RMSE"] = float(np.sqrt(np.mean((model_output - targets) ** 2)))

    # --- MAE ---
    stats["MAE"] = float(np.mean(np.abs(model_output - targets)))

    # --- Relatieve bias op het gemiddeld debiet [%] ---
    mean_obs = np.mean(targets)
    if mean_obs != 0:
        stats["RelBias"] = float((np.mean(model_output) - mean_obs) / mean_obs * 100)
    else:
        stats["RelBias"] = np.nan

    # --- KGE (Kling-Gupta Efficiency) ---
    r = float(np.corrcoef(targets, model_output)[0, 1]) if np.std(targets) > 0 and np.std(model_output) > 0 else np.nan
    alpha = float(np.std(model_output) / np.std(targets)) if np.std(targets) != 0 else np.nan
    beta = float(np.mean(model_output) / mean_obs) if mean_obs != 0 else np.nan
    if any(np.isnan(v) for v in [r, alpha, beta]):
        stats["KGE"] = np.nan
    else:
        stats["KGE"] = float(1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2))

    # --- Percentielen P10, P25, P75, P90 ---
    for p in [10, 25, 75, 90]:
        obs_p = float(np.nanpercentile(targets, p))
        sim_p = float(np.nanpercentile(model_output, p))
        stats[f"P{p}_obs"] = obs_p
        stats[f"P{p}_sim"] = sim_p
        stats[f"P{p}_reldev"] = float((sim_p - obs_p) / abs(obs_p) * 100) if obs_p != 0 else np.nan

    return stats


# ── Module-niveau constanten (gebruikt door meerdere functies) ────────────────

CRITERIA_GRENZEN: dict[str, dict] = {
    "Bias": {"Goed": 10.0, "Matig": 15.0},
    "NSE": {"Goed": 0.5},
    "KGE": {"Goed": 0.6},
    "P10": {"Goed": 25.0, "Matig": 30.0},
    "P25": {"Goed": 10.0, "Matig": 15.0},
    "P75": {"Goed": 10.0, "Matig": 15.0},
    "P90": {"Goed": 25.0, "Matig": 30.0},
}

BEOOR_KLEUREN = {
    "Goed": PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),
    "Matig": PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"),
    "Onvoldoende": PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
    "n.v.t.": PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid"),
}

CRITERIA_GROEPEN: dict[str, dict] = {
    "Bias": {"kolommen": ["Beoor_Bias_dec"], "min_goed": 1},
    "NSE_KGE": {"kolommen": ["Beoor_NSE_dec", "Beoor_KGE_dec"], "min_goed": 1},
    "Percentielen": {"kolommen": ["Beoor_P10_dec", "Beoor_P25_dec", "Beoor_P75_dec", "Beoor_P90_dec"], "min_goed": 4},
}

MIN_GROEPEN_VOLDOEN = 2
DREMPEL_HWS = 75.0
DREMPEL_REGIONAAL = 50.0
HWS_WATERSCHAP = "Rijkswaterstaat"


def BeoordeelCriteria(stats: dict) -> dict:
    """Beoordeelt de statistieken op basis van de KRW-toetsingscriteria.

    Parameters
    ----------
    stats
        Dictionary met statistieken zoals teruggegeven door GetStatisticsComparison.

    Returns
    -------
    dict met per criterium de beoordeling: 'Goed', 'Matig', 'Onvoldoende' of 'n.v.t.'
    """
    beoordeling = {}

    def _beoordeel(waarde, criterium, hoger_is_beter=False):
        if waarde is None or np.isnan(waarde):
            return "n.v.t."
        grenzen = CRITERIA_GRENZEN[criterium]
        if hoger_is_beter:
            return "Goed" if waarde > grenzen["Goed"] else "Onvoldoende"
        else:
            abs_val = abs(waarde)
            if "Matig" in grenzen:
                return (
                    "Goed" if abs_val < grenzen["Goed"] else ("Matig" if abs_val < grenzen["Matig"] else "Onvoldoende")
                )
            else:
                return "Goed" if abs_val < grenzen["Goed"] else "Onvoldoende"

    beoordeling["Bias"] = _beoordeel(stats.get("RelBias"), "Bias")
    beoordeling["NSE"] = _beoordeel(stats.get("NSE"), "NSE", hoger_is_beter=True)
    beoordeling["KGE"] = _beoordeel(stats.get("KGE"), "KGE", hoger_is_beter=True)
    beoordeling["P10"] = _beoordeel(stats.get("P10_reldev"), "P10")
    beoordeling["P25"] = _beoordeel(stats.get("P25_reldev"), "P25")
    beoordeling["P75"] = _beoordeel(stats.get("P75_reldev"), "P75")
    beoordeling["P90"] = _beoordeel(stats.get("P90_reldev"), "P90")

    return beoordeling


def GetStatisticsPerPeriod(combined_df: pd.DataFrame) -> dict:
    """Berekent statistieken voor elk uniek jaar in de data én voor de totale periode.

    De perioden worden afgeleid vanuit de Ribasim modeluitvoer zelf, zodat het script
    automatisch werkt voor andere tijdsperioden dan 2017-2019.

    Parameters
    ----------
    combined_df
        DataFrame met kolommen 'time', 'flow_rate' en 'sum'.

    Returns
    -------
    dict met per jaar (als string) en 'totaal' de statistieken.
    """
    results: dict[str, dict | None] = {}
    unique_years = sorted(combined_df["time"].dt.year.unique())

    for year in unique_years:
        df_year = combined_df[combined_df["time"].dt.year == year]
        if df_year.dropna(subset=["sum"]).shape[0] >= 5:
            results[str(year)] = GetStatisticsComparison(df_year)
        else:
            results[str(year)] = None  # te weinig geldige metingen

    results["totaal"] = GetStatisticsComparison(combined_df)
    return results


def SaveTimeseries(
    combined_df: pd.DataFrame,
    combined_df_decade: pd.DataFrame,
    fig_name_clean: str,
    meetreeks_naam: str,
    output_folder: str,
) -> None:
    """Schrijft de tijdsreeksen (dag en decade) weg als Excel-bestand met twee sheets.

    Parameters
    ----------
    combined_df
        DataFrame met dagwaarden (kolommen: time, flow_rate, sum).
    combined_df_decade
        DataFrame met decadewaarden (kolommen: time, flow_rate, sum).
    fig_name_clean
        Bestandsnaam (zonder extensie) afgeleid van de meetreeksnaam.
    meetreeks_naam
        Naam van de meetreeks, gebruikt als kolomnaam in de output.
    output_folder
        Map waar het Excel-bestand wordt opgeslagen.
    """
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    filepath = Path(output_folder) / f"{fig_name_clean}.xlsx"

    # Dag sheet: datum | meetreeks | Ribasim
    dag_df = combined_df[["time", "sum", "flow_rate"]].copy()
    dag_df.columns = ["Datum", meetreeks_naam, "Ribasim [m3/s]"]

    # Decade sheet: datum | meetreeks | Ribasim
    dec_df = combined_df_decade[["time", "sum", "flow_rate"]].copy()
    dec_df.columns = ["Datum", meetreeks_naam, "Ribasim [m3/s]"]

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        dag_df.to_excel(writer, sheet_name="Dag", index=False)
        dec_df.to_excel(writer, sheet_name="Decade", index=False)


def _KleureerBeoordelingen(ws, df: pd.DataFrame) -> None:
    """Kleurt de beoordelingskolommen (kolommen die 'Beoor_' bevatten) in het worksheet.

    Parameters
    ----------
    ws
        openpyxl worksheet object.
    df
        DataFrame waaruit het worksheet is aangemaakt (voor kolomindexen).
    """
    beoor_cols = [i + 1 for i, col in enumerate(df.columns) if col.startswith("Beoor_")]
    for row_idx in range(2, ws.max_row + 1):  # rij 1 = header
        for col_idx in beoor_cols:
            cell = ws.cell(row=row_idx, column=col_idx)
            kleur = BEOOR_KLEUREN.get(cell.value, BEOOR_KLEUREN["n.v.t."])
            cell.fill = kleur


def BerekenModelEindbeoordeling(
    validatie_xlsx: str | Path,
    output_xlsx: str | Path | None = None,
    periode: str = "totaal",
) -> pd.DataFrame:
    """Leest de weggeschreven Validatie_criteria.xlsx en berekent de eindbeoordeling.

    Werkt op de decade-beoordelingskolommen (Beoor_*_dec) die per waterschap-sheet
    aanwezig zijn. De criteria zijn gegroepeerd in drie overkoepelende groepen
    (zie CRITERIA_GROEPEN):

    - Bias         : RelBias_dec
    - NSE/KGE      : NSE_dec en/of KGE_dec  (Goed als ≥ 1 Goed)
    - Percentielen : P10, P25, P75, P90      (Goed als ≥ 2 van 4 Goed)

    Een locatie 'voldoet' als MIN_GROEPEN_VOLDOEN (standaard 2) van de 3 groepen Goed zijn.
    Het model is 'Geschikt' als het % voldoende locaties de drempel haalt:
    - DREMPEL_HWS voor sheet "Rijkswaterstaat" (hoofdwatersysteem)
    - DREMPEL_REGIONAAL voor overige sheets

    Parameters
    ----------
    validatie_xlsx
        Pad naar de weggeschreven Validatie_criteria.xlsx.
    output_xlsx
        Optioneel uitvoerpad voor een nieuwe Excel met de eindbeoordeling.
    periode
        Rij-filter op de kolom 'Periode'; standaard 'totaal'.

    Returns
    -------
    pd.DataFrame met de eindbeoordeling per waterschap + samenvattingsrijen.
    """
    # Open de validatie-Excel zonder alle sheets tegelijk in geheugen te laden
    xl = pd.ExcelFile(validatie_xlsx)
    detail_rijen: list[dict] = []
    samenvatting_rijen: list[dict] = []

    # Vertaaltabel van beoordelingswaarden naar Excel-opvulkleuren
    _kleur_map = {
        "Geschikt": BEOOR_KLEUREN["Goed"],
        "Ongeschikt": BEOOR_KLEUREN["Onvoldoende"],
        "Ja": BEOOR_KLEUREN["Goed"],
        "Nee": BEOOR_KLEUREN["Onvoldoende"],
        "Goed": BEOOR_KLEUREN["Goed"],
        "Onvoldoende": BEOOR_KLEUREN["Onvoldoende"],
    }

    # ── Stap 1: doorloop elke waterschap-sheet ────────────────────────────────
    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        # Sla sheets over die geen statistieken-structuur hebben (bijv. extra info-sheets)
        if "Periode" not in df.columns:
            continue
        # Filter op de gewenste periode (standaard "totaal" = statistieken over de gehele simulatieperiode)
        df_p = df[df["Periode"] == periode].copy()
        if df_p.empty:
            continue

        # Rijkswaterstaat krijgt een strengere drempel dan regionale waterschappen
        is_hws = sheet == HWS_WATERSCHAP
        drempel = DREMPEL_HWS if is_hws else DREMPEL_REGIONAAL
        n_voldoet = 0

        # ── Stap 2: beoordeel elke locatie ───────────────────────────────────
        for _, rij in df_p.iterrows():
            groep_beoor: dict[str, str] = {}
            for groep, cfg in CRITERIA_GROEPEN.items():
                # Alleen kolommen meenemen die ook echt in de sheet aanwezig zijn
                aanwezig = [k for k in cfg["kolommen"] if k in df_p.columns]
                n_goed = sum(rij.get(k, "n.v.t.") == "Goed" for k in aanwezig)
                # Groep is "Goed" als het minimum aantal "Goed"-kolommen is gehaald
                groep_beoor[groep] = "Goed" if (aanwezig and n_goed >= cfg["min_goed"]) else "Onvoldoende"

            # Locatie voldoet als minimaal MIN_GROEPEN_VOLDOEN van de 3 groepen "Goed" zijn
            n_groepen_goed = sum(v == "Goed" for v in groep_beoor.values())
            voldoet = n_groepen_goed >= MIN_GROEPEN_VOLDOEN
            if voldoet:
                n_voldoet += 1

            # Sla het locatie-detailresultaat op voor de Details_per_locatie-sheet
            detail_rijen.append(
                {
                    "Waterschap": sheet,
                    "Locatie": rij.get("Locatie", "onbekend"),
                    **{f"Groep_{g}": v for g, v in groep_beoor.items()},
                    "N_groepen_Goed": n_groepen_goed,
                    "Voldoet": "Ja" if voldoet else "Nee",
                }
            )

        # ── Stap 3: samenvatting per waterschap ──────────────────────────────
        n_totaal = len(df_p)
        pct = round(n_voldoet / n_totaal * 100, 1) if n_totaal > 0 else np.nan
        samenvatting_rijen.append(
            {
                "Waterschap": sheet,
                "Type": "HWS" if is_hws else "Regionaal",
                "N_locaties": n_totaal,
                "N_voldoen": n_voldoet,
                "Pct_voldoen": pct,
                "Drempel [%]": drempel,
                # Model "Geschikt" als het percentage voldoende locaties de drempel haalt
                "Beoordeling": "Geschikt" if (not np.isnan(pct) and pct >= drempel) else "Ongeschikt",
            }
        )

    if not samenvatting_rijen:
        print("Geen bruikbare sheets gevonden in de validatie-Excel.")
        return pd.DataFrame()

    df_sam = pd.DataFrame(samenvatting_rijen)
    df_det = pd.DataFrame(detail_rijen)

    # ── Stap 4: voeg totaalrijen toe voor HWS en Regionaal als geheel ────────
    for type_naam, type_drempel in [("HWS", DREMPEL_HWS), ("Regionaal", DREMPEL_REGIONAAL)]:
        sub = df_sam[df_sam["Type"] == type_naam]
        if sub.empty:
            continue
        n_tot = int(sub["N_locaties"].sum())
        n_vol = int(sub["N_voldoen"].sum())
        pct = round(n_vol / n_tot * 100, 1) if n_tot > 0 else np.nan
        df_sam = pd.concat(
            [
                df_sam,
                pd.DataFrame(
                    [
                        {
                            "Waterschap": f"── Totaal {type_naam}",
                            "Type": type_naam,
                            "N_locaties": n_tot,
                            "N_voldoen": n_vol,
                            "Pct_voldoen": pct,
                            "Drempel [%]": type_drempel,
                            "Beoordeling": "Geschikt" if (not np.isnan(pct) and pct >= type_drempel) else "Ongeschikt",
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    # ── Stap 5: wegschrijven naar Excel (optioneel) ───────────────────────────
    if output_xlsx is not None:
        Path(output_xlsx).resolve().parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
            # Sheet 1: samenvatting per waterschap met eindbeoordeling
            df_sam.to_excel(writer, sheet_name="Eindbeoordeling", index=False)
            ws = writer.sheets["Eindbeoordeling"]
            # Kleureer de Beoordeling-kolom (Geschikt = groen, Ongeschikt = rood)
            beoor_col = df_sam.columns.tolist().index("Beoordeling") + 1
            for row_idx in range(2, ws.max_row + 1):
                cell = ws.cell(row=row_idx, column=beoor_col)
                if cell.value in _kleur_map:
                    cell.fill = _kleur_map[cell.value]
            # Notitieregel met de gebruikte instellingen voor reproduceerbaarheid
            ws.cell(row=ws.max_row + 2, column=1).value = (
                f"Instellingen: periode={periode}  |  tijdreeks=decade  |  "
                f"min. groepen voldoen={MIN_GROEPEN_VOLDOEN}/3  |  "
                f"drempel HWS={DREMPEL_HWS}%  |  drempel regionaal={DREMPEL_REGIONAAL}%"
            )

            # Sheet 2: per locatie de drie groepsbeoordelingen en of de locatie voldoet
            if not df_det.empty:
                df_det.to_excel(writer, sheet_name="Details_per_locatie", index=False)
                ws_det = writer.sheets["Details_per_locatie"]
                # Kleureer de Groep_*- en Voldoet-kolommen
                kleur_cols = [i + 1 for i, c in enumerate(df_det.columns) if c.startswith("Groep_") or c == "Voldoet"]
                for row_idx in range(2, ws_det.max_row + 1):
                    for col_idx in kleur_cols:
                        cell = ws_det.cell(row=row_idx, column=col_idx)
                        if cell.value in _kleur_map:
                            cell.fill = _kleur_map[cell.value]

        print(f"Eindbeoordeling opgeslagen: {output_xlsx}")

    return df_sam


def ExportToExcel(results_excel: dict, output_path: str | Path) -> None:
    """Schrijft per waterschap een sheet naar een Excel-bestand.

    Bevat alle statistieken en beoordelingen voor dag- en decadewaarden,
    uitgesplitst per periode.

    Parameters
    ----------
    results_excel
        Dict met structuur:
        {waterschap: [{"locatie": str,
                       "stats_dag":  {periode: stats_dict | None},
                       "stats_dec":  {periode: stats_dict | None}}, ...]}
    output_path
        Pad naar het te schrijven Excel-bestand.
    """
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for waterschap, records in results_excel.items():
            rijen = []
            for record in records:
                perioden = [*sorted(p for p in record["stats_dag"] if p != "totaal"), "totaal"]

                for periode in perioden:
                    stats_d = record["stats_dag"].get(periode)
                    stats_dc = record["stats_dec"].get(periode)

                    def _fmt(stats, key):
                        if stats is None or stats.get(key) is None:
                            return np.nan
                        val = stats[key]
                        return round(float(val), 3) if not np.isnan(val) else np.nan

                    beoor_d = (
                        BeoordeelCriteria(stats_d)
                        if stats_d is not None
                        else dict.fromkeys(["NSE", "KGE", "Bias", "P10", "P25", "P75", "P90"], "n.v.t.")
                    )
                    beoor_dc = (
                        BeoordeelCriteria(stats_dc)
                        if stats_dc is not None
                        else dict.fromkeys(["NSE", "KGE", "Bias", "P10", "P25", "P75", "P90"], "n.v.t.")
                    )

                    rij = {
                        "Locatie": record["locatie"],
                        "Periode": periode,
                        # --- Dagwaarden: statistieken ---
                        "n_obs_dag": _fmt(stats_d, "n_obs"),
                        "NSE_dag": _fmt(stats_d, "NSE"),
                        "KGE_dag": _fmt(stats_d, "KGE"),
                        "RelBias_dag [%]": _fmt(stats_d, "RelBias"),
                        "P10_reldev_dag [%]": _fmt(stats_d, "P10_reldev"),
                        "P25_reldev_dag [%]": _fmt(stats_d, "P25_reldev"),
                        "P75_reldev_dag [%]": _fmt(stats_d, "P75_reldev"),
                        "P90_reldev_dag [%]": _fmt(stats_d, "P90_reldev"),
                        # --- Dagwaarden: beoordelingen ---
                        "Beoor_NSE_dag": beoor_d["NSE"],
                        "Beoor_KGE_dag": beoor_d["KGE"],
                        "Beoor_Bias_dag": beoor_d["Bias"],
                        "Beoor_P10_dag": beoor_d["P10"],
                        "Beoor_P25_dag": beoor_d["P25"],
                        "Beoor_P75_dag": beoor_d["P75"],
                        "Beoor_P90_dag": beoor_d["P90"],
                        # --- Decadewaarden: statistieken ---
                        "n_obs_dec": _fmt(stats_dc, "n_obs"),
                        "NSE_dec": _fmt(stats_dc, "NSE"),
                        "KGE_dec": _fmt(stats_dc, "KGE"),
                        "RelBias_dec [%]": _fmt(stats_dc, "RelBias"),
                        "P10_reldev_dec [%]": _fmt(stats_dc, "P10_reldev"),
                        "P25_reldev_dec [%]": _fmt(stats_dc, "P25_reldev"),
                        "P75_reldev_dec [%]": _fmt(stats_dc, "P75_reldev"),
                        "P90_reldev_dec [%]": _fmt(stats_dc, "P90_reldev"),
                        # --- Decadewaarden: beoordelingen ---
                        "Beoor_NSE_dec": beoor_dc["NSE"],
                        "Beoor_KGE_dec": beoor_dc["KGE"],
                        "Beoor_Bias_dec": beoor_dc["Bias"],
                        "Beoor_P10_dec": beoor_dc["P10"],
                        "Beoor_P25_dec": beoor_dc["P25"],
                        "Beoor_P75_dec": beoor_dc["P75"],
                        "Beoor_P90_dec": beoor_dc["P90"],
                    }
                    rijen.append(rij)

            if not rijen:
                continue

            df_sheet = pd.DataFrame(rijen)
            sheet_name = waterschap[:31]  # Excel beperkt sheetnamen tot 31 tekens
            df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)

            # Kleurcodering op beoordelingskolommen
            ws = writer.sheets[sheet_name]
            _KleureerBeoordelingen(ws, df_sheet)

            # Kolombreedte automatisch aanpassen
            for col_idx, col_name in enumerate(df_sheet.columns, start=1):
                max_len = max(len(str(col_name)), df_sheet[col_name].astype(str).str.len().max())
                ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = min(max_len + 2, 30)


def CompareOutputMeasurements(
    loc_koppeltabel,
    loc_specifics,
    meas_folder,
    model_folder,
    filetype="flow",
    apply_for_water_authority: str | None = None,
    save_results_combined: bool = False,
    output_is_feather: bool = True,
    output_is_nc: bool = False,
    resample_to_daily: bool = True,
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
    The function returns nothing, but saves the results in .png figures, geopackages,
    tijdsreeksen per meetlocatie en een Excel-bestand met criteria en beoordelingen.

    """
    koppeltabel = LaadKoppeltabel(loc_koppeltabel, apply_for_water_authority=apply_for_water_authority)
    specifics = LaadSpecifiekeBewerking(loc_specifics)
    data = ReadOutputFile(
        model_folder,
        filetype,
        output_is_feather=output_is_feather,
        output_is_nc=output_is_nc,
        resample_to_daily=resample_to_daily,
    )

    measurements = LoadMeasurements(meas_folder)

    results_measurements: dict[str, dict[str, list[object]]] = {}
    results_measurements_decade: dict[str, dict[str, list[object]]] = {}
    results_excel = {}  # voor het Excel-criteriabestand

    # Get the unique link ids
    unique_links = get_unique(koppeltabel["link_id_parsed"])

    for link in tqdm.tqdm(unique_links, total=len(unique_links), desc="Verwerken metingen"):
        try:
            if np.isnan(link):
                print("A link with type NaN has been found. Skipped.")
                continue
        except:  # noqa: E722 S110 TODO: do not use bare except
            pass

        # for n, meetlocatie in tqdm.tqdm(koppeltabel.iterrows(), total=len(koppeltabel), desc='Verwerken metingen'):
        mask = koppeltabel["link_id_parsed"].apply(lambda x, _link=link: x == _link)
        meetlocaties_link = koppeltabel[mask]
        waterschap = meetlocaties_link.iloc[0]["Waterschap"]

        # Create a result dictionary per waterschap
        if waterschap not in results_measurements:

            def _leeg_results():
                return {
                    "koppelinfo": [],
                    "waterschap": [],
                    "link_id": [],
                    "MeetreeksC": [],
                    "Aan/Af": [],
                    "NSE": [],
                    "RMSE": [],
                    "MAE": [],
                    "RelBias": [],
                    "KGE": [],
                    "P10_reldev": [],
                    "P25_reldev": [],
                    "P75_reldev": [],
                    "P90_reldev": [],
                    "geometry": [],
                    "figure_path": [],
                }

            results_measurements[waterschap] = _leeg_results()
            results_measurements_decade[waterschap] = _leeg_results()
            results_excel[waterschap] = []

        # Loop over the locations in case two need to be summed
        # Two locs cannot be summed if the type of discharge differs
        unieke_aanaf = np.unique(meetlocaties_link["Aan/Af"])
        if len(unieke_aanaf) > 1:
            # HARDCODED uitzondering: AmstelGooienVecht mag metingen met hetzelfde
            # Aan/Af-type optellen ook als de link meerdere Aan/Af-waarden heeft.
            # Rijen met een afwijkend Aan/Af worden weggelaten; de rest wordt gesommeerd.
            if waterschap == "AmstelGooienVecht":
                meest_voorkomend = meetlocaties_link["Aan/Af"].mode()[0]
                meetlocaties_link = meetlocaties_link[meetlocaties_link["Aan/Af"] == meest_voorkomend]
                print(
                    f"[HARDCODED] Link {link} ({waterschap}): meerdere Aan/Af-waarden gevonden "
                    f"({list(unieke_aanaf)}). Rijen met Aan/Af='{meest_voorkomend}' worden opgeteld, "
                    f"overige rijen weggelaten."
                )
            else:
                dominant_aanaf = meetlocaties_link.iloc[0]["Aan/Af"]
                details = ", ".join(
                    f"{row['MeetreeksC']} ({row['Aan/Af']})"
                    for _, row in meetlocaties_link[["MeetreeksC", "Aan/Af"]].iterrows()
                )
                weggelaten = [
                    row["MeetreeksC"] for _, row in meetlocaties_link.iterrows() if row["Aan/Af"] != dominant_aanaf
                ]
                print(
                    f"Link {link} ({waterschap}): meerdere Aan/Af-waarden gevonden "
                    f"({list(unieke_aanaf)}). Doorgaan met Aan/Af='{dominant_aanaf}'. "
                    f"Meetreeksen: {details}." + (f" Weggelaten (andere Aan/Af): {weggelaten}." if weggelaten else "")
                )
                meetlocaties_link = meetlocaties_link[meetlocaties_link["Aan/Af"] == dominant_aanaf]

        if meetlocaties_link.iloc[0]["Aan/Af"] == "Aanvoer":
            dagmetingen = measurements["aanvoer_dag"]
        elif (meetlocaties_link.iloc[0]["Aan/Af"] == "Afvoer") or (meetlocaties_link.iloc[0]["Aan/Af"] == "Aan&Af"):
            dagmetingen = measurements["afvoer_dag"]
        else:
            print(f"Onbekend Aan/Af-type '{meetlocaties_link.iloc[0]['Aan/Af']}' voor link {link}, overgeslagen.")
            continue

        existing_measurements = [col for col in meetlocaties_link["MeetreeksC"] if col in dagmetingen.columns]
        missing_measurements = [col for col in meetlocaties_link["MeetreeksC"] if col not in dagmetingen.columns]
        if len(missing_measurements) > 0:
            for col in missing_measurements:
                print(f"Cannot find the daily measurements for {col}")

        subset_measurements = dagmetingen[["time", *existing_measurements]].copy()

        # If multiple measurements series refer to the same link, take the sum of the measurements
        subset_measurements["sum"] = subset_measurements[existing_measurements].sum(axis=1, min_count=1)

        # Get the specific operations for the measurements
        subset_specs = specifics[
            (specifics["MeetreeksC"].isin(existing_measurements))
            & (specifics["Aan/Af"] == meetlocaties_link.iloc[0]["Aan/Af"])
        ]

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

        # Bereken statistieken voor totale periode en per jaar (afgeleid uit de data)
        stats_dag_per_periode = GetStatisticsPerPeriod(combined_df)
        stats_dec_per_periode = GetStatisticsPerPeriod(combined_df_decade)

        # Totaalstatistieken voor gebruik in de plot en het geopackage
        stats = stats_dag_per_periode["totaal"]
        stats_dec = stats_dec_per_periode["totaal"]

        # Add the cumulative discharges to the dataframe
        combined_df_cum = AddCumulative(combined_df)
        combined_df_decade_cum = AddCumulative(combined_df_decade, decade=True)

        # --- Deal with the daily values ---
        full_title = " - ".join(existing_measurements)
        fig_name = full_title.split(" - ")[0]
        meetreeks_naam = fig_name  # kolomnaam in tijdsreeks-Excel
        bron_meting = None if apply_for_water_authority is not None else waterschap
        PlotAndSave(
            combined_df=combined_df_cum,
            stats=stats,
            koppelinfo=full_title,
            fig_name=fig_name,
            bron_meting=bron_meting,
            output_folder=Path(model_folder, "results", "figures"),
        )
        fig_name_clean = _sanitize_filename(fig_name)
        pop_up_figure = f'<img src="../figures/{waterschap}/{fig_name_clean}.png" width=400 height=300>'

        # Save the resulting statistics per measurement
        for key, val in [
            ("koppelinfo", fig_name_clean),
            ("link_id", link),
            ("MeetreeksC", existing_measurements[0]),
            ("Aan/Af", meetlocaties_link.iloc[0]["Aan/Af"]),
            ("NSE", stats["NSE"]),
            ("RMSE", stats["RMSE"]),
            ("MAE", stats["MAE"]),
            ("RelBias", stats["RelBias"]),
            ("KGE", stats["KGE"]),
            ("P10_reldev", stats["P10_reldev"]),
            ("P25_reldev", stats["P25_reldev"]),
            ("P75_reldev", stats["P75_reldev"]),
            ("P90_reldev", stats["P90_reldev"]),
            ("geometry", meetlocaties_link.iloc[0]["geometry_parsed"]),
            ("waterschap", waterschap),
            ("figure_path", pop_up_figure),
        ]:
            results_measurements[waterschap][key].append(val)

        # --- Save the results per decade ---
        fig_name_dec = fig_name + "_decade"
        PlotAndSave(
            combined_df=combined_df_decade_cum,
            stats=stats_dec,
            koppelinfo=full_title,
            fig_name=fig_name_dec,
            bron_meting=bron_meting,
            output_folder=Path(model_folder) / "results" / "figures",
        )
        fig_name_dec_clean = _sanitize_filename(fig_name) + "_decade"
        pop_up_figure_dec = f'<img src="../figures/{waterschap}/{fig_name_dec_clean}.png" width=400 height=300>'

        for key, val in [
            ("koppelinfo", fig_name_dec_clean),
            ("link_id", link),
            ("MeetreeksC", existing_measurements[0]),
            ("Aan/Af", meetlocaties_link.iloc[0]["Aan/Af"]),
            ("NSE", stats_dec["NSE"]),
            ("RMSE", stats_dec["RMSE"]),
            ("MAE", stats_dec["MAE"]),
            ("RelBias", stats_dec["RelBias"]),
            ("KGE", stats_dec["KGE"]),
            ("P10_reldev", stats_dec["P10_reldev"]),
            ("P25_reldev", stats_dec["P25_reldev"]),
            ("P75_reldev", stats_dec["P75_reldev"]),
            ("P90_reldev", stats_dec["P90_reldev"]),
            ("geometry", meetlocaties_link.iloc[0]["geometry_parsed"]),
            ("waterschap", waterschap),
            ("figure_path", pop_up_figure_dec),
        ]:
            results_measurements_decade[waterschap][key].append(val)

        # --- Tijdsreeksen wegschrijven per meetlocatie ---
        ts_folder = Path(model_folder) / "results" / "tijdreeksen" / waterschap
        SaveTimeseries(
            combined_df=combined_df,
            combined_df_decade=combined_df_decade,
            fig_name_clean=fig_name_clean,
            meetreeks_naam=meetreeks_naam,
            output_folder=ts_folder,
        )

        # --- Sla record op voor het Excel-criteriabestand ---
        results_excel[waterschap].append(
            {
                "locatie": fig_name_clean,
                "stats_dag": stats_dag_per_periode,
                "stats_dec": stats_dec_per_periode,
            }
        )

    results_combined = []
    results_dec_combined = []

    # Save the results in a geopackage per waterboard
    for waterschap, results in results_measurements.items():
        results_gdf = gpd.GeoDataFrame(results, geometry="geometry")
        results_gdf.set_crs(epsg="28992", inplace=True)
        results_combined.append(results_gdf)
        results_gdf.to_file(Path(model_folder) / "results" / "Validatie_resultaten.gpkg", layer=waterschap)

    for waterschap, results in results_measurements_decade.items():
        results_gdf = gpd.GeoDataFrame(results, geometry="geometry")
        results_gdf.set_crs(epsg="28992", inplace=True)
        results_dec_combined.append(results_gdf)
        results_gdf.to_file(Path(model_folder) / "results" / "Validatie_resultaten_dec.gpkg", layer=waterschap)

    # Save all the results in one geopackage to make handling in QGIS or HTML easier
    if save_results_combined:
        final_gdf = pd.concat(results_combined, ignore_index=True)
        final_dec_gdf = pd.concat(results_dec_combined, ignore_index=True)

        final_gdf.to_file(Path(model_folder) / "results" / "Validatie_resultaten_all.gpkg", layer="Compleet")
        final_dec_gdf.to_file(Path(model_folder) / "results" / "Validatie_resultaten_dec_all.gpkg", layer="Compleet")

    # Schrijf het Excel-criteriabestand weg
    excel_path = Path(model_folder) / "results" / "Validatie_criteria.xlsx"
    ExportToExcel(results_excel, excel_path)
    print(f"Criteria Excel opgeslagen: {excel_path}")


def ConvertToDecade(combined_df_results):
    def get_decade(ts) -> int:
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
    def build_decade_date(row) -> pd.Timestamp:
        day = {1: 1, 2: 11, 3: 21}[row["decade"]]
        return pd.Timestamp(year=int(row["year"]), month=int(row["month"]), day=day)

    grouped["time"] = grouped.apply(build_decade_date, axis=1)

    # Reorder and set dtypes to match original
    result = grouped[["time", "flow_rate", "sum"]].copy()
    result["time"] = pd.to_datetime(result["time"])
    result["flow_rate"] = result["flow_rate"].astype("float64")
    result["sum"] = result["sum"].astype("float64")

    return result


def LoadMeasurements(meas_folder) -> dict[str, pd.DataFrame]:
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
            measurements[key] = pd.read_csv(Path(meas_folder) / file, parse_dates=["Unnamed: 0"])
            measurements[key].rename(columns={"Unnamed: 0": "time"}, inplace=True)
        except:  # noqa: E722 TODO: specify exception
            try:
                measurements[key] = pd.read_csv(Path(meas_folder) / file, parse_dates=["Datum"])
                measurements[key].rename(columns={"Datum": "time"}, inplace=True)
            except ValueError:
                print("Cannot identify date/time column. Can be [Unnamed: 0, Datum] ")

    return measurements


def PlotAndSave(combined_df, stats, koppelinfo, fig_name, bron_meting, output_folder):
    """Plots data from a pd.DataFrame, adds statistical information, and saves the plot as an image in a specified folder.

    Parameters
    ----------
    combined_df
        `combined_df` is a pd.DataFrame containing data to be plotted.
    stats
        The `stats` in the `PlotAndSave` function parameter contains statistical information (NSE, KGE, RelBias, RMSE, MAE,
        percentielen) calculated for the data being plotted.
    koppelinfo
        The `koppelinfo` parameter in the `PlotAndSave` function supplies the name of the measurement location. Used as title.
    bron_meting
        The `bron_meting` parameter in the `PlotAndSave` function represents the origin of the measurement (waterboard).
    output_folder
        The `output_folder` parameter in the `PlotAndSave` function is the directory where the plot will be
        saved. It is the location where the folder for the specific `bron_meting` will be created, and
        within that folder, the plot will be saved as a PNG file with the name supplied by 'koppelinfo'
    """
    if bron_meting is None:
        Path(output_folder).mkdir(parents=True, exist_ok=True)
    else:
        (Path(output_folder) / bron_meting).mkdir(parents=True, exist_ok=True)

    font = "Arial"

    _BEOOR_KLEUR = {
        "Goed": "#2a7a2a",
        "Matig": "#b8860b",
        "Onvoldoende": "#c0392b",
        "n.v.t.": "gray",
    }

    def _fmt(val, decimals=3):
        return f"{round(val, decimals)}" if val is not None and not np.isnan(val) else "n.v.t."

    beoordeling = BeoordeelCriteria(stats)

    fig = plt.figure(figsize=(11, 5))
    gs = fig.add_gridspec(1, 2, width_ratios=[4, 1.1], wspace=0.20)
    ax = fig.add_subplot(gs[0])
    ax_leg = fig.add_subplot(gs[1])
    ax_leg.axis("off")

    (model_line,) = ax.plot(
        combined_df["time"], combined_df["flow_rate"], label="Ribasim", color="tab:blue", linewidth=2.0
    )
    (meas_line,) = ax.plot(combined_df["time"], combined_df["sum"], label="Meting", color="tab:orange", linewidth=2.0)
    ax.grid()
    ax.set_xticks(ticks=ax.get_xticks(), labels=ax.get_xticklabels(), rotation=45, fontname=font)
    ax.tick_params(axis="both", labelsize=11)
    for lbl in ax.get_xticklabels() + ax.get_yticklabels():
        lbl.set_fontweight("bold")
    ax.set_ylabel("Debiet [m$^3$/s]", fontdict={"fontsize": 13, "fontname": font, "fontweight": "bold"})
    ax.set_title(koppelinfo, fontname=font, wrap=True, fontsize=11)

    ax2 = ax.twinx()
    (model_cum_line,) = ax2.plot(
        combined_df["time"],
        combined_df["flow_rate_cum"] / 1_000_000,
        linestyle="--",
        color="#0047b3",
        label="Rib. cum.",
        linewidth=2.0,
    )
    (meas_cum_line,) = ax2.plot(
        combined_df["time"],
        combined_df["sum_cum"] / 1_000_000,
        linestyle="--",
        color="#cc6600",
        label="Meting cum.",
        linewidth=2.0,
    )
    ax2.set_ylabel("Cum. debiet [Mm$^3$]", fontsize=13, fontweight="bold")
    ax2.tick_params(axis="y", labelsize=11)
    for lbl in ax2.get_yticklabels():
        lbl.set_fontweight("bold")
    lines = [model_line, meas_line, model_cum_line, meas_cum_line]
    labels = [line.get_label() for line in lines]
    ax.legend(lines, labels, prop={"family": font}, fontsize=10, loc="best")

    # ── Statistieken-panel rechts ─────────────────────────────────────────────
    ax_leg.add_patch(
        plt.matplotlib.patches.FancyBboxPatch(
            (0.02, 0.02),
            0.96,
            0.96,
            transform=ax_leg.transAxes,
            boxstyle="round,pad=0.02",
            facecolor="whitesmoke",
            edgecolor="lightgray",
            linewidth=0.8,
            zorder=0,
        )
    )

    LINE_HEIGHT = 0.050  # regelafstand voor score-tekst
    VERDICT_STEP = 0.020  # beoordeling dicht onder de score
    GAP_AFTER = 0.030  # ruimte na beoordeling, vóór de volgende criteria
    SEP = 0.028  # sectie-scheiding
    y = 0.96

    def _regel(tekst, kleur="black", bold=False, suffix=None, suffix_kleur="black"):
        """Score-tekst op huidige y; beoordeling direct daarna (VERDICT_STEP).

        Gevolgd door GAP_AFTER als witruimte vóór de volgende criteria.
        """
        nonlocal y
        gewicht = "bold" if bold else "normal"
        ax_leg.text(
            0.06,
            y,
            tekst,
            transform=ax_leg.transAxes,
            fontsize=10,
            fontfamily=font,
            color=kleur,
            fontweight=gewicht,
            verticalalignment="top",
            clip_on=True,
        )
        y -= LINE_HEIGHT
        if suffix:
            ax_leg.text(
                0.12,
                y,
                suffix,
                transform=ax_leg.transAxes,
                fontsize=9.5,
                fontfamily=font,
                color=suffix_kleur,
                fontweight="bold",
                verticalalignment="top",
                clip_on=True,
            )
            y -= VERDICT_STEP
            y -= GAP_AFTER

    _regel("Statistieken (totaal)", bold=True)

    # Criteria met kleurcode
    for sleutel, label, fmt in [
        ("NSE", "NSE", ".2f"),
        ("KGE", "KGE", ".2f"),
        ("RelBias", "Bias", ".1f"),
    ]:
        val = stats.get(sleutel)
        val_str = f"{val:{fmt}}" if val is not None and not np.isnan(val) else "n.v.t."
        beoor = beoordeling.get("Bias" if sleutel == "RelBias" else sleutel, "n.v.t.")
        kleur = _BEOOR_KLEUR.get(beoor, "gray")
        eenheid = " %" if sleutel == "RelBias" else ""
        _regel(f"{label}: {val_str}{eenheid}", suffix=beoor, suffix_kleur=kleur)

    y -= SEP

    _regel("Percentielen (sim / obs)", bold=True)
    for p, sleutel in [("P10", "P10"), ("P25", "P25"), ("P75", "P75"), ("P90", "P90")]:
        sim_str = _fmt(stats.get(f"{sleutel}_sim"), 2)
        obs_str = _fmt(stats.get(f"{sleutel}_obs"), 2)
        beoor = beoordeling.get(sleutel, "n.v.t.")
        kleur = _BEOOR_KLEUR.get(beoor, "gray")
        _regel(f"{p}: {sim_str} / {obs_str}", suffix=beoor, suffix_kleur=kleur)

    fig_name_clean = _sanitize_filename(fig_name)
    if bron_meting is None:
        fig_path = Path(output_folder) / (fig_name_clean + ".png")
    else:
        fig_path = Path(output_folder) / bron_meting / (fig_name_clean + ".png")
    fig.savefig(fig_path, bbox_inches="tight", dpi=300)
    plt.close()


def ExtraInfoToevoegenAllData(
    loc_koppeltabel,
    meas_folder,
    model_folder,
    apply_for_water_authority=None,
    stat_cols=("abs_q95", "abs_q05"),
    threshold=5,
    new_col="P95_P05_alle_beschikbare_data",
    mode="any",
    above_operator=">=",
):
    """Voegt P95_P05_alle_beschikbare_data toe aan bestaande validatie-geopackages.

    Stap 1 — classificeer per (MeetreeksC, Aan/Af) op basis van de volledige meetreeks:
      - '{above_operator}{threshold}' als de conditie geldt (mode='any'/'all')
      - '<{threshold}' anders
      - 'geen_data' als de meetreeks niet in de CSV staat
    Stap 2 — voeg classificatie toe aan koppeltabel en merge met geopackages.
    """
    if isinstance(stat_cols, str):
        stat_cols = list(stat_cols)

    label_above = f"{above_operator}{threshold}"
    label_below = f"<{threshold}" if above_operator == ">=" else f"<={threshold}"

    koppeltabel = LaadKoppeltabel(loc_koppeltabel, apply_for_water_authority)
    measurements = LoadMeasurements(meas_folder)

    # --- Bereken q05 / q95 per MeetreeksC over de volledige meetreeks ---
    def _calc_stats(df):
        cols = [c for c in df.columns if c != "time"]
        q = df[cols].quantile([0.05, 0.95]).T
        q.columns = ["q05", "q95"]
        q["abs_q05"] = q["q05"].abs()
        q["abs_q95"] = q["q95"].abs()
        return q

    stats_aanvoer = _calc_stats(measurements["aanvoer_dag"])
    stats_afvoer = _calc_stats(measurements["afvoer_dag"])

    # --- Stap 1: voeg classificatie toe aan koppeltabel op (MeetreeksC, Aan/Af) ---
    def _classify(row):
        mc, aan_af = row["MeetreeksC"], row["Aan/Af"]
        stats = stats_afvoer if aan_af in ("Afvoer", "Aan&Af") else stats_aanvoer
        if mc not in stats.index:
            return "geen_data"
        available = [c for c in stat_cols if c in stats.columns]
        if not available:
            return "geen_data"
        vals = stats.loc[mc, available]
        hits = vals >= threshold if above_operator == ">=" else vals > threshold
        return label_above if (hits.any() if mode == "any" else hits.all()) else label_below

    koppeltabel[new_col] = koppeltabel.apply(_classify, axis=1)

    # --- Stap 2: merge koppeltabel (met classificatie) naar geopackages op (MeetreeksC, Aan/Af) ---
    merge_cols = koppeltabel[["MeetreeksC", "Aan/Af", new_col]]

    results_path = Path(model_folder) / "results"

    def _update_gpkg(gpkg_path: Path):
        if not gpkg_path.exists():
            print(f"Geopackage niet gevonden, overgeslagen: {gpkg_path}")
            return
        layers = gpd.list_layers(gpkg_path)["name"].tolist()
        for layer in layers:
            gdf = gpd.read_file(gpkg_path, layer=layer)
            gdf = gdf.merge(merge_cols, on=["MeetreeksC", "Aan/Af"], how="left")
            gdf[new_col] = gdf[new_col].fillna("geen_data")
            gdf.to_file(gpkg_path, layer=layer)
        print(f"Bijgewerkt: {gpkg_path.name} ({len(layers)} lagen)")

    for gpkg_name in [
        "Validatie_resultaten.gpkg",
        "Validatie_resultaten_dec.gpkg",
        "Validatie_resultaten_all.gpkg",
        "Validatie_resultaten_dec_all.gpkg",
    ]:
        _update_gpkg(results_path / gpkg_name)

    print("ExtraInfoToevoegenAllData voltooid.")


# %%
if __name__ == "__main__":
    ########################################################################################################################################
    # Implementatie lokaal voor testen
    # base = r"C:\Users\micha.veenendaal\Data\Ribasim LHM validatie\Sync_GoodCloud\Rijkswaterstaat\modellen"
    # loc_koppeltabel = Path(base) / "lhm_coupled_2025_9_0_test" / "results" / "koppeltabel" / "Transformed_koppeltabel_versie_lhm_coupled_2025_9_0_Feedback_Verwerkt_HydroLogic.xlsx"
    # loc_specifieke_bewerking = Path(base) / "lhm_coupled_2025_9_0_test" / "results" / "koppeltabel" / "Specifiek_bewerking_versielhm_coupled_2025_9_0.xlsx"
    # meas_folder = r"C:\Users\micha.veenendaal\Data\Ribasim LHM validatie\Sync_GoodCloud\Landelijk\resultaatvergelijking\meetreeksen"

    # #Nieuwe plot functies even op een kopie testen
    # model_folder = os.path.join(base, "lhm_coupled_2025_9_0_test")
    # base = r"C:\Users\micha.veenendaal\Data\HL-P26004\Modellen\Limburg_2026_4_0 - Copy"
    # loc_koppeltabel = os.path.join(base, "results", "koppeltabel", "Transformed_koppeltabel_versie_Limburg_HL_test_Feedback_Verwerkt_HydroLogic.xlsx")
    # loc_specifieke_bewerking = os.path.join(base, "results", "koppeltabel", "Specifiek_bewerking_versieHL_test_Limburg.xlsx")
    # meas_folder = r"C:\Users\micha.veenendaal\Data\Ribasim LHM validatie\Sync_GoodCloud\Landelijk\resultaatvergelijking\meetreeksen"

    # #Nieuwe plot functies even op een kopie testen
    # # model_folder = base

    # model_folder = Path(base) / "lhm_coupled_2025_9_0_test"

    cloud = CloudStorage()
    base_koppeltabel = cloud.joinpath("Basisgegevens/resultaatvergelijking/koppeltabel_2026")

    loc_koppeltabel = (
        base_koppeltabel / "Transformed_koppeltabel_versie_Limburg_2026_4_0_Feedback_Verwerkt_HydroLogic.xlsx"
    )
    loc_specifieke_bewerking = base_koppeltabel / "Specifiek_bewerking_versieLimburg_2026_4_0.xlsx"
    waterboard = "Limburg"
    waterboard_model_versions = cloud.uploaded_models(authority=waterboard)

    latest_model_version = sorted(
        [i for i in waterboard_model_versions if i.model == waterboard], key=lambda x: getattr(x, "sorter", "")
    )[-1]

    model_folder = cloud.joinpath(f"{waterboard}/modellen", latest_model_version.path_string)
    # toml_naam = "lhm-coupled.toml"
    toml_naam = "limburg.toml"

    meas_folder = cloud.joinpath("Basisgegevens/resultaatvergelijking/meetreeksen_2026")

    # synchronize paths
    cloud.synchronize([loc_koppeltabel, loc_specifieke_bewerking, meas_folder, model_folder])

    # validatie_lhm4_1 = os.path.join(base, "Toetsing LHM 4.1", )
    ########################################################################################################################################

    RUN_COMPARE = True
    RUN_EXTRA_INFO = True
    RUN_EINDBEOORDELING = True
    RUN_HTML_VIEWER = True
    RUN_UPLOAD = False

    # ── Verwerk meetreeksen, bereken statistieken, schrijf figuren/geopackages/Excel ──
    if RUN_COMPARE:
        CompareOutputMeasurements(
            loc_koppeltabel=loc_koppeltabel,
            loc_specifics=loc_specifieke_bewerking,
            meas_folder=meas_folder,
            model_folder=model_folder,
            filetype="flow",
            save_results_combined=True,
            output_is_feather=False,
            output_is_nc=True,
            resample_to_daily=True,
        )

    # ── Voeg debiet_klasse_extreem toe aan bestaande geopackages ──
    if RUN_EXTRA_INFO:
        ExtraInfoToevoegenAllData(
            loc_koppeltabel=loc_koppeltabel,
            meas_folder=meas_folder,
            model_folder=model_folder,
            stat_cols=("abs_q95", "abs_q05"),
            threshold=5,
            new_col="P95_P05_alle_beschikbare_data",
            mode="any",
            above_operator=">=",
        )

    # ── Eindbeoordeling op basis van weggeschreven Validatie_criteria.xlsx ──
    if RUN_EINDBEOORDELING:
        validatie_xlsx = Path(model_folder) / "results" / "Validatie_criteria.xlsx"
        eindbeoordeling_xlsx = Path(model_folder) / "results" / "Eindbeoordeling_model.xlsx"
        BerekenModelEindbeoordeling(
            validatie_xlsx=validatie_xlsx,
            output_xlsx=eindbeoordeling_xlsx,
        )

    # ── HTML-viewer genereren op basis van weggeschreven geopackages ──
    if RUN_HTML_VIEWER:
        CreateHTMLViewer(model_folder=model_folder, model_gpkg=None, include_lhm41=False)
        # CreateHTMLViewer(model_folder=model_folder, model_gpkg=None, include_lhm41=True)

    # ── Upload resultaten naar de cloud ──
    if RUN_UPLOAD:
        results = Path(model_folder) / "results"
        for gpkg in [
            "Validatie_resultaten_all.gpkg",
            "Validatie_resultaten_dec_all.gpkg",
            "Validatie_resultaten.gpkg",
            "Validatie_resultaten_dec.gpkg",
        ]:
            gpkg_path = results / gpkg
            if gpkg_path.exists():
                cloud.upload_file(gpkg_path)
        for xlsx in ["Validatie_criteria.xlsx", "Eindbeoordeling_model.xlsx"]:
            xlsx_path = results / xlsx
            if xlsx_path.exists():
                cloud.upload_file(xlsx_path)
        figures_path = results / "figures"
        if figures_path.exists():
            cloud.upload_content(figures_path)
        html_path = results / "Validatieresultaten_HTML"
        if html_path.exists():
            cloud.upload_content(html_path)

# %%

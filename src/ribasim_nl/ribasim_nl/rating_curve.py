# %%
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from pandas import DataFrame, Series


def read_rating_curve(file_path: Path, node_index: Series) -> DataFrame:
    """Concat sheets in a verdeelsleutel.xlsx to 1 pandas dataframe."""
    wb = load_workbook(file_path)
    sheet_names = wb.sheetnames
    dfs = []
    for sheet_name in sheet_names:
        if sheet_name != "disclaimer":
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            df["code_waterbeheerder"] = sheet_name
            df["node_id"] = node_index.loc[sheet_name]
            dfs += [df]

    return pd.concat(dfs)[["node_id", "level", "discharge", "code_waterbeheerder"]]


# %%

import os
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Border
from pydantic import BaseModel

from ribasim_nl import Model
from ribasim_nl.parametrization.empty_table import empty_table_df

defaults = {
    "Afvoergemaal": {
        "upstream_level_offset": 0.0,
        "downstream_level_offset": 0.2,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 15,
        "function": "outlet",
    },
    "Aanvoergemaal": {
        "upstream_level_offset": 0.2,
        "downstream_level_offset": 0.0,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 4,
        "function": "inlet",
    },
    "Uitlaat": {
        "upstream_level_offset": 0.0,
        "downstream_level_offset": 0.3,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 50,
        "function": "outlet",
    },
    "Inlaat": {
        "upstream_level_offset": 0.2,
        "downstream_level_offset": 0.0,
        "flow_rate": pd.NA,
        "flow_rate_mm_per_day": 4,
        "function": "inlet",
    },
}


class StaticData(BaseModel):
    xlsx_path: os.PathLike
    model: Model
    defaults: dict = defaults

    def __post_init__(self):
        self.xlsx_path = Path(self.xlsx_path)

    @property
    def defaults_df(self):
        df = pd.DataFrame.from_dict(self.defaults, orient="index")
        df.index.name = "categorie"
        return df

    def write(self):
        if self.xlsx_path.exists():
            self.xlsx_path.unlink()
        else:
            self.xlsx_path.parent.mkdir(exist_ok=True, parents=True)

        # write defaults
        self.defaults_df.to_excel(self.xlsx_path, sheet_name="defaults")

        # write sheets
        with pd.ExcelWriter(self.xlsx_path, mode="a", if_sheet_exists="replace") as xlsx_writer:
            columns = ["node_id", "name", "code", "flow_rate", "min_upstream_level", "max_downstream_level"]
            extra_columns = ["categorie", "opmerking_waterbeheerder"]
            for node_type in ["Pump", "Outlet"]:
                df = empty_table_df(
                    model=self.model,
                    table_type="Static",
                    node_type=node_type,
                    meta_columns=["meta_code_waterbeheerder", "name"],
                )
                df.rename(columns={"meta_code_waterbeheerder": "code"}, inplace=True)
                if node_type == "Pump":
                    df["categorie"] = "Afvoergemaal"
                if node_type == "Outlet":
                    df["categorie"] = "Uitlaat"
                df["opmerking_waterbeheerder"] = ""
                df[columns + extra_columns].to_excel(xlsx_writer, sheet_name=node_type, index=False)

        # fix formatting
        wb = load_workbook(self.xlsx_path)
        for sheet_name in pd.ExcelFile(self.xlsx_path).sheet_names:
            ws = wb[sheet_name]

            # Remove cell borders
            for row in ws.iter_rows():
                for cell in row:
                    cell.border = Border()  # Removes borders

            # Auto-adjust column width
            for col in ws.columns:
                max_length = 0
                col_letter = col[0].column_letter  # Get column letter (e.g., 'A', 'B', etc.)

                for cell in col:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))

                ws.column_dimensions[col_letter].width = max_length + 2  # Add some padding

            # Save the modified file
            wb.save(self.xlsx_path)

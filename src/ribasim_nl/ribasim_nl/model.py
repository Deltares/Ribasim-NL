from pathlib import Path

import pandas as pd
from pydantic import BaseModel
from ribasim import Model


def read_arrow(filepath: Path) -> pd.DataFrame:
    df = pd.read_feather(filepath)
    if "time" in df.columns:
        df.set_index("time", inplace=True)
    return df


class BasinResults(BaseModel):
    filepath: Path
    _df = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._basin_df = read_arrow(self.filepath)
        return self._basin_df


class Model(Model):
    _basin_results: BasinResults | None = None

    @property
    def basin_results(self):
        if self._basin_results is None:
            filepath = (
                self.filepath.parent.joinpath(self.results_dir, "basin.arrow")
                .absolute()
                .resolve()
            )
            self._basin_results = BasinResults(filepath=filepath)
        return self._basin_results

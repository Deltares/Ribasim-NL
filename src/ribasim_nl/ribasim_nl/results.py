import pandas as pd
from pandas import DataFrame
from ribasim import Model


def basin_results(model: Model) -> DataFrame:
    """Read basin-results into a pandas DataFrame."""
    basin_arrow = model.filepath.parent.joinpath(model.results_dir, "basin.arrow")

    return pd.read_feather(basin_arrow)

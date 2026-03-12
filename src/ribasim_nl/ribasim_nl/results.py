import xarray as xr
from pandas import DataFrame

from ribasim_nl.model import Model


def basin_results(model: Model) -> DataFrame:
    """Read basin-results into a pandas DataFrame."""
    basin_nc = model.filepath.parent.joinpath(model.results_dir, "basin.nc")

    return xr.open_dataset(basin_nc).to_dataframe().reset_index()

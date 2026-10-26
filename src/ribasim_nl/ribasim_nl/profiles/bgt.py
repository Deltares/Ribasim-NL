"""Flag the BGT water that is coupled to the main route, for the intermediate output of the profile generation."""

import itertools
from pathlib import Path

import geopandas as gpd


def save_bgt_coupling(
    hydro_objects: gpd.GeoDataFrame,
    bgt_data: gpd.GeoDataFrame,
    wd: Path,
    *,
    fn: str = "int_output.gpkg",
    layer: str = "bgt",
) -> None:
    """Save BGT-data including flag on main-routing.

    The BGT-data is coupled to hydro-objects. This function transfers the main-route flag from the hydro-objects to the
    BGT-data based on the BGT-coupling with the hydro-objects. This flagging is intended as part of the intermediate
    output options, and therefore for debugging purposes.

    :param hydro_objects: hydro-objects
    :param bgt_data: BGT-data
    :param wd: working directory to save the BGT-data to
    :param fn: filename, defaults to "int_output.gpkg"
    :param layer: layer, defaults to "bgt"

    :type hydro_objects: geopandas.GeoDataFrame
    :type bgt_data: geopandas.GeoDataFrame
    :type wd: Path
    :type fn: str, optional
    :type layer: str, optional

    :raises AssertionError
    """
    # validate hydro-objects
    _req_cols = "main-route", "index_bgt"
    if not all(c in hydro_objects.columns for c in _req_cols):
        msg = f"Required columns missing in `hydro_objects` ({_req_cols=}); not all required processing is executed"
        raise ValueError(msg)

    # flag BGT-data
    bgt_data = bgt_data.copy()
    bgt_index = list(itertools.chain.from_iterable(hydro_objects.loc[hydro_objects["main-route"], "index_bgt"]))
    bgt_data["main-route"] = bgt_data.index.isin(bgt_index)

    # save flagged BGT-data
    bgt_data.to_file(wd / fn, layer=layer)

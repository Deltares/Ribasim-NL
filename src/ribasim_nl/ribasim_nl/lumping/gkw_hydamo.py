"""The HyDAMO data of the water authorities from the national GKW-HyDAMO package, prepared to lump a network from.

The water authorities deliver their HyDAMO data to the Gegevensknooppunt waterschappen (GkW) of Het Waterschapshuis,
which passes on the objects that pass its checks. NHI publishes the result as one GeoPackage of all authorities, zipped
at `PACKAGE_URL`. It has no versions: the file is replaced in place. `scripts/download_gkw_hydamo.py` therefore
downloads it to `GKW_HYDAMO`, which DVC tracks, and `read_authority` reads the objects of one authority from that.

`prepare_hydroobjects` makes the hydroobjects of an authority into the lines that a network is lumped from. The
lumping connects lines only where their ends have the same coordinates, see
`ribasim_nl.lumping.hydamo.read_hydamo_network`, so the ends that lie close together get one coordinate, and a line is
split where another one ends on it.
"""

from collections.abc import Collection
from pathlib import Path

import geopandas as gpd
import pyogrio

from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.lumping.connect import connect_lines, join_parts
from ribasim_nl.settings import settings

PACKAGE_URL = "https://nhipackages.blob.core.windows.net/packages/gkw-hydamo-package.zip"
# the GeoPackage in the zip file of the package, and where it is kept
PACKAGE_GPKG = "LHM_HyDAMO.gpkg"
GKW_HYDAMO = settings.ribasim_nl_data_dir / "Basisgegevens/GKW-HyDAMO" / PACKAGE_GPKG
# the column with the category of a hydroobject: primair, secundair, tertiair or overig
CATEGORY_COLUMN = "categorieoppwaterlichaam"
# the distance in m within which hydroobjects that end near each other are connected. In the package of November 2026,
# 9 junctions of Brabantse Delta miss each other by 0.5 to 0.95 m. Further apart are also the watercourses on either
# side of a pumping station, which the pumping station connects.
CONNECT_DISTANCE = 1.0


def read_authority(authority: str, layer: str, package: Path = GKW_HYDAMO) -> gpd.GeoDataFrame:
    """The objects of a water authority in `layer` of the national GeoPackage `package`.

    The `nen3610id` of an object is `NL.WBHCODE.<waterbeheercode>.<object type>.<objectid>`. The `code` of an object
    does not tell its authority: codes are only unique within an authority.
    """
    where = f"nen3610id LIKE 'NL.WBHCODE.{waterbeheercode[authority]}.%'"
    gdf = pyogrio.read_dataframe(package, layer=layer, where=where)
    assert isinstance(gdf, gpd.GeoDataFrame), f"layer {layer} of {package} has no geometries"
    return gdf


def select_hydroobjects(
    hydroobject: gpd.GeoDataFrame, categories: Collection[str], codes: Collection[str] = ()
) -> gpd.GeoDataFrame:
    """The hydroobjects of `categories`, with those with a code in `codes`."""
    missing = set(codes) - set(hydroobject["code"])
    assert not missing, f"no hydroobjects with codes {sorted(missing)}"
    selected = hydroobject[hydroobject[CATEGORY_COLUMN].isin(categories) | hydroobject["code"].isin(codes)]
    already = set(codes) & set(hydroobject.loc[hydroobject[CATEGORY_COLUMN].isin(categories), "code"])
    assert not already, f"hydroobjects {sorted(already)} are of categories {sorted(categories)} already"
    assert not selected.empty, f"no hydroobjects of categories {sorted(categories)}"
    assert selected["code"].notna().all(), "hydroobjects without code"
    return selected


def prepare_hydroobjects(
    hydroobject: gpd.GeoDataFrame,
    categories: Collection[str],
    codes: Collection[str] = (),
    connect_distance: float = CONNECT_DISTANCE,
) -> gpd.GeoDataFrame:
    """The hydroobjects of `categories` and with `codes`, as lines that connect.

    Every line gets its own code, see `ribasim_nl.lumping.connect.join_parts`, and the lines that end within
    `connect_distance` of each other are connected, see `ribasim_nl.lumping.connect.connect_lines`.
    """
    lines = join_parts(select_hydroobjects(hydroobject, categories, codes))
    lines = connect_lines(lines, connect_distance)
    assert lines["code"].is_unique
    return lines

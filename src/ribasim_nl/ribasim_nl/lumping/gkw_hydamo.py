"""The HyDAMO data of a water authority from the national GKW-HyDAMO package, prepared to lump a network from.

The water authorities deliver their HyDAMO data to the Gegevensknooppunt waterschappen (GkW) of Het Waterschapshuis,
which passes on the objects that pass its checks. NHI publishes the result as one GeoPackage of all authorities,
`PACKAGE_URL`. It has no versions: the file is replaced in place. `extract_authority` therefore takes the layers of one
authority out of it, to keep as source data under the date of the package.

`prepare_hydroobjects` makes the hydroobjects of such an extract into the lines that a network is lumped from. The
lumping connects lines only where their ends have the same coordinates, see
`ribasim_nl.lumping.hydamo.read_hydamo_network`, so the ends that lie close together get one coordinate, and a line is
split where another one ends on it.
"""

import logging
from collections.abc import Collection, Sequence
from pathlib import Path

import geopandas as gpd
import pyogrio

from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.lumping.connect import connect_lines, join_parts
from ribasim_nl.lumping.hydamo import STRUCTURE_LAYERS

logger = logging.getLogger(__name__)

PACKAGE_URL = "https://nhipackages.blob.core.windows.net/packages/gkw-hydamo-package.zip"
# the GeoPackage in the package
PACKAGE_GPKG = "LHM_HyDAMO.gpkg"
# the layers that a network is lumped from
LAYERS = ["hydroobject", *STRUCTURE_LAYERS]
# the column with the category of a hydroobject: primair, secundair, tertiair or overig
CATEGORY_COLUMN = "categorieoppwaterlichaam"
# the distance in m within which hydroobjects that end near each other are connected. In the package of November 2026,
# 9 junctions of Brabantse Delta miss each other by 0.5 to 0.95 m. Further apart are also the watercourses on either
# side of a pumping station, which the pumping station connects.
CONNECT_DISTANCE = 1.0


def authority_filter(authority: str) -> str:
    """The SQL filter of the objects of a water authority in the package.

    The `nen3610id` of an object is `NL.WBHCODE.<waterbeheercode>.<object type>.<objectid>`. The `code` of an object
    does not tell its authority: codes are only unique within an authority.
    """
    return f"nen3610id LIKE 'NL.WBHCODE.{waterbeheercode[authority]}.%'"


def extract_authority(package: Path, authority: str, target: Path, layers: Sequence[str] = LAYERS) -> None:
    """Write the objects of a water authority in `layers` of the national GeoPackage `package` to `target`.

    Layers without objects of the authority are left out.
    """
    assert package != target
    available = set(pyogrio.list_layers(package)[:, 0])
    missing = set(layers) - available
    assert not missing, f"{package} has no layers {sorted(missing)}"
    target.unlink(missing_ok=True)
    for layer in layers:
        gdf = pyogrio.read_dataframe(package, layer=layer, where=authority_filter(authority))
        logger.info(f"{layer}: {len(gdf)} objects of {authority}")
        if len(gdf) > 0:
            pyogrio.write_dataframe(gdf, target, layer=layer)
    assert target.exists(), f"{package} has no objects of {authority} in {list(layers)}"


def select_hydroobjects(
    hydroobject: gpd.GeoDataFrame,
    categories: Collection[str],
    codes: Collection[str] = (),
    excluded: Collection[str] = (),
) -> gpd.GeoDataFrame:
    """The hydroobjects of `categories`, with those with a code in `codes`, and without those in `excluded`."""
    missing = (set(codes) | set(excluded)) - set(hydroobject["code"])
    assert not missing, f"no hydroobjects with codes {sorted(missing)}"
    both = set(codes) & set(excluded)
    assert not both, f"hydroobjects {sorted(both)} are both selected and excluded"
    selected = hydroobject[hydroobject[CATEGORY_COLUMN].isin(categories) | hydroobject["code"].isin(codes)]
    not_selected = set(excluded) - set(selected["code"])
    assert not not_selected, f"excluded hydroobjects {sorted(not_selected)} are not of categories {sorted(categories)}"
    selected = selected[~selected["code"].isin(excluded)]
    assert not selected.empty, f"no hydroobjects of categories {sorted(categories)}"
    assert selected["code"].notna().all(), "hydroobjects without code"
    return selected


def prepare_hydroobjects(
    hydroobject: gpd.GeoDataFrame,
    categories: Collection[str],
    codes: Collection[str] = (),
    excluded: Collection[str] = (),
    connect_distance: float = CONNECT_DISTANCE,
) -> gpd.GeoDataFrame:
    """The hydroobjects of `categories` and with `codes`, without `excluded`, as lines that connect.

    Every line gets its own code, see `ribasim_nl.lumping.connect.join_parts`, and the lines that end within
    `connect_distance` of each other are connected, see `ribasim_nl.lumping.connect.connect_lines`.
    """
    lines = join_parts(select_hydroobjects(hydroobject, categories, codes, excluded))
    lines = connect_lines(lines, connect_distance)
    assert lines["code"].is_unique
    return lines

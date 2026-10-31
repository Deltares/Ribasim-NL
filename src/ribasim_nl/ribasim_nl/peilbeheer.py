"""The level management (peilbeheer) of the peilgebieden of the water boards, in summer and winter.

The water boards deliver their peilgebieden in their own formats. `read_peilbeheer` reads them with the attribute
mappings of `SOURCES`, see https://github.com/Deltares/Ribasim-NL/issues/893, into one table with per peilgebied the
type of level management, the target level (streefpeil) and its lower and upper bound (ondergrens and bovengrens) in
summer and winter, and the dates of the transitions between them. Where a dataset does not give these, they follow
from the rules of `complete_peilbeheer`:

- Without a target level, the target level is the mean of the bounds.
- A season without a target level gets the levels of the other season, as for a fixed level.
- Without a bound, or with a bound that does not lie within `MAX_BOUND_DISTANCE` beyond the target level, the bound
  is `DEFAULT_BOUND` from the target level, which is the band of the DiscreteControl thresholds of the model.
- The transition from the winter to the summer level is from 1 to 10 April, and back from 1 to 10 October. Datasets
  with only the date at which the level changes start a transition of `TRANSITION_DAYS` days on that date. The models
  change their levels at once, at the begin of a transition, see `ribasim_nl.seasonal_levels`.

Levels of 99 m from NAP or more are unknown values, and so are levels of 0 in datasets that use 0 for unknown values,
see `valid_levels`. Peilgebieden of which the summer and winter level differ more than `MAX_SEASONAL_DIFFERENCE` are
data errors, and skipped. Most peilgebieden are areas, but they can also be points, such as weirs with their target
levels.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from ribasim_nl.control_layout import DEFAULT_LEVEL_THRESHOLD_RANGE

LOG = logging.getLogger(__name__)

SEASONS = ("zomer", "winter")
LEVELS = ("streefpeil", "ondergrens", "bovengrens")
LEVEL_COLUMNS = [f"{level}_{season}" for season in SEASONS for level in LEVELS]
TRANSITION_COLUMNS = [
    "begin_overgang_winterpeil_zomerpeil",
    "eind_overgang_winterpeil_zomerpeil",
    "begin_overgang_zomerpeil_winterpeil",
    "eind_overgang_zomerpeil_winterpeil",
]
COLUMNS = ["authority", "code", "soort_peilbeheer", *LEVEL_COLUMNS, *TRANSITION_COLUMNS]

# The bounds of a target level without them, the band of the DiscreteControl thresholds
DEFAULT_BOUND = DEFAULT_LEVEL_THRESHOLD_RANGE / 2  # m
# Bounds further from the target level than this are data errors
MAX_BOUND_DISTANCE = 0.5  # m
# Summer and winter levels further apart than this are data errors
MAX_SEASONAL_DIFFERENCE = 1.0  # m
# The transitions between the winter and summer levels, as day-month
DEFAULT_TRANSITIONS = ("01-04", "10-04", "01-10", "10-10")
# The duration of a transition of which only the date is known, from its first to its last day
TRANSITION_DAYS = 10
# A year without 29 February, to convert day-month to a day of the year
_YEAR = 2021

CRS = "EPSG:28992"


def valid_levels(values: pd.Series, zero_is_unknown: bool = False) -> pd.Series:
    """Return the levels as floats, with NaN for unknown levels.

    Datasets use 99 or 9999 m NAP for unknown levels, so levels of 99 m from NAP or more are unknown. Some datasets
    also use 0, which is otherwise a level like any other.
    """
    levels = pd.to_numeric(values, errors="coerce").astype(float)
    unknown = (levels.abs() >= 99) | ((levels == 0) & zero_is_unknown)
    return levels.where(~unknown)


def _multiple_levels(values: pd.Series, level: str, zero_is_unknown: bool = False) -> pd.Series:
    """Return the levels of attributes that give one level per structure, separated by semicolons.

    The band of the bounds of all structures counts, so the lowest lower bound, and the highest upper bound. Target
    levels must be the same for all structures.
    """
    if values.dtype != object and not pd.api.types.is_string_dtype(values):
        return valid_levels(values, zero_is_unknown)
    parts = values.astype("string").str.split(";").explode().str.strip()
    levels = valid_levels(parts, zero_is_unknown).groupby(level=0)
    if level == "ondergrens":
        return levels.min().reindex(values.index)
    if level == "bovengrens":
        return levels.max().reindex(values.index)
    return levels.max().where(levels.min() == levels.max()).reindex(values.index)


def _categories(values: pd.Series) -> pd.Series:
    """Return the values as strings, with integral numbers without decimals, e.g. 1.0 as "1"."""
    if pd.api.types.is_numeric_dtype(values) and values.dropna().mod(1).eq(0).all():
        values = values.astype("Int64")
    return values.astype("string").str.strip()


def month_day(day_month: object) -> tuple[int, int] | None:
    """Return the month and day of a date as "dd-mm" or "ddmm", or None if it is not a date of every year."""
    if not isinstance(day_month, str):
        return None
    digits = day_month.replace("-", "").strip()
    if len(digits) != 4 or not digits.isdigit():
        return None
    try:
        return date(_YEAR, int(digits[2:]), int(digits[:2])).timetuple()[1:3]
    except ValueError:
        return None


def day_of_year(day_month: object) -> int | None:
    """Return the day of the year, from 1 to 365, of a date as "dd-mm" or "ddmm", or None if it is not a date."""
    parsed = month_day(day_month)
    return None if parsed is None else date(_YEAR, *parsed).timetuple().tm_yday


def day_month(day: int) -> str:
    """Return a day of the year as "dd-mm", the inverse of `day_of_year`."""
    return (date(_YEAR, 1, 1) + timedelta(days=day - 1)).strftime("%d-%m")


@dataclass(frozen=True)
class Source:
    """A dataset with the peilgebieden of a water board, and the attributes of their levels.

    `mapping` maps a value of attribute `by` to the attributes of the levels of the peilgebieden with that value, and
    `"*"` to those of all peilgebieden. The levels are `streefpeil`, `ondergrens` and `bovengrens` for both seasons,
    and these with the suffix `_zomer` or `_winter` for one season. Peilgebieden with other values of `by` are skipped.

    :param path: dataset, relative to the data directory
    :param code: attribute with the code of a peilgebied
    :param mapping: attributes of the levels of the peilgebieden, by value of `by`
    :param layer: layer of the dataset
    :param member: path of the dataset in the zip archive `path`
    :param read: function that reads the dataset from the data directory, instead of `path`, `layer` and `member`
    :param by: attribute that selects the mapping
    :param soort_peilbeheer: attribute with the type of level management
    :param labels: the types of level management of the values of `soort_peilbeheer` that are codes
    :param transitions: attributes with the dates of the transitions, as "dd-mm", by their column of `COLUMNS`
    :param zero_is_unknown: whether the dataset uses levels of 0 for unknown levels
    """

    path: str
    code: str
    mapping: dict[str, dict[str, str]]
    layer: str | None = None
    member: str | None = None
    read: Callable[[Path], gpd.GeoDataFrame] | None = None
    by: str | None = None
    soort_peilbeheer: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    transitions: dict[str, str] = field(default_factory=dict)
    zero_is_unknown: bool = False

    def __post_init__(self) -> None:
        levels = {*LEVELS, *LEVEL_COLUMNS}
        for key, attributes in self.mapping.items():
            assert (key == "*") == (self.by is None), (
                f"Map all peilgebieden with '*' if and only if `by` is None: {key}"
            )
            assert set(attributes) <= levels, f"Unknown levels {set(attributes) - levels} in {self.path}"
        assert set(self.transitions) <= set(TRANSITION_COLUMNS), f"Unknown transitions in {self.path}"

    def read_dataset(self, data_dir: Path) -> gpd.GeoDataFrame:
        """Read the dataset, in `CRS`."""
        if self.read is not None:
            gdf = self.read(data_dir)
        else:
            path = (data_dir / self.path).as_posix()
            if self.member is not None:
                path = f"/vsizip/{path}/{self.member}"
            gdf = gpd.read_file(path, layer=self.layer)
        assert gdf.crs is not None, f"No CRS in {self.path}"
        # Some datasets have RD New with NAP heights, which is the same in the plane
        if gdf.crs.to_epsg() == 7415:
            gdf = gdf.set_crs(CRS, allow_override=True)
        return gdf.to_crs(CRS)

    def peilbeheer(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Return the levels and transitions of the peilgebieden in `gdf`, the dataset, with the columns of `COLUMNS`."""
        df = gpd.GeoDataFrame(
            {
                "code": _categories(gdf[self.code]),
                "soort_peilbeheer": pd.Series(pd.NA, index=gdf.index, dtype="string"),
                **dict.fromkeys(LEVEL_COLUMNS, np.nan),
                **{column: pd.Series(pd.NA, index=gdf.index, dtype="string") for column in TRANSITION_COLUMNS},
            },
            geometry=gdf.geometry.values,
            crs=gdf.crs,
            index=gdf.index,
        )
        if self.soort_peilbeheer is not None:
            soort = _categories(gdf[self.soort_peilbeheer])
            df["soort_peilbeheer"] = soort.replace(self.labels)

        by = _categories(gdf[self.by]) if self.by is not None else pd.Series("*", index=gdf.index)
        for key, attributes in self.mapping.items():
            rows = by == key
            for level, attribute in attributes.items():
                columns = [level] if level in LEVEL_COLUMNS else [f"{level}_{season}" for season in SEASONS]
                values = _multiple_levels(gdf.loc[rows, attribute], level.split("_")[0], self.zero_is_unknown)
                for column in columns:
                    # a level of one season goes before that of both seasons
                    if level in LEVEL_COLUMNS or column not in attributes:
                        df.loc[rows, column] = values
        for column, attribute in self.transitions.items():
            df[column] = gdf[attribute].astype("string")
        return df.loc[by.isin(list(self.mapping))]


def complete_peilbeheer(df: pd.DataFrame) -> pd.DataFrame:
    """Complete the levels and transitions of the peilgebieden with the rules of this module, see its docstring.

    :param df: peilgebieden with the columns of `COLUMNS`
    :return: the peilgebieden with a target level, with all their levels and transitions
    """
    df = df.copy()
    for season in SEASONS:
        bounds_mean = (df[f"ondergrens_{season}"] + df[f"bovengrens_{season}"]) / 2
        df[f"streefpeil_{season}"] = df[f"streefpeil_{season}"].fillna(bounds_mean)
    for season, other in (SEASONS, SEASONS[::-1]):
        fixed = df[f"streefpeil_{season}"].isna()
        for level in LEVELS:
            df.loc[fixed, f"{level}_{season}"] = df.loc[fixed, f"{level}_{other}"]
    df = df.loc[df["streefpeil_zomer"].notna() & df["streefpeil_winter"].notna()].copy()
    authorities = ", ".join(map(str, df["authority"].dropna().unique()))
    excessive = (df["streefpeil_zomer"] - df["streefpeil_winter"]).abs() > MAX_SEASONAL_DIFFERENCE
    if excessive.any():
        LOG.warning(
            "%s: skipped %s with summer and winter levels too far apart", authorities, list(df.loc[excessive, "code"])
        )
        df = df.loc[~excessive].copy()

    for season in SEASONS:
        target = df[f"streefpeil_{season}"]
        lower = df[f"ondergrens_{season}"]
        upper = df[f"bovengrens_{season}"]
        valid_lower = (lower < target) & (lower >= target - MAX_BOUND_DISTANCE)
        valid_upper = (upper > target) & (upper <= target + MAX_BOUND_DISTANCE)
        df[f"ondergrens_{season}"] = lower.where(valid_lower, target - DEFAULT_BOUND)
        df[f"bovengrens_{season}"] = upper.where(valid_upper, target + DEFAULT_BOUND)
        invalid = (lower.notna() & ~valid_lower) | (upper.notna() & ~valid_upper)
        if invalid.any():
            LOG.warning("%s: replaced the invalid %s bounds of %d peilgebieden", authorities, season, invalid.sum())

    transitions = [_complete_transitions(*row) for row in df[TRANSITION_COLUMNS].itertuples(index=False)]
    df[TRANSITION_COLUMNS] = pd.DataFrame(transitions, index=df.index, columns=TRANSITION_COLUMNS, dtype="string")
    return df


def _complete_transitions(*day_months: object) -> tuple[str, ...]:
    """Complete the begin and end of the transitions to summer and to winter, as "dd-mm".

    A transition with only its begin or end lasts `TRANSITION_DAYS` days. Without dates of the transitions, or if they
    are not in the order of the year after 1 January, the transitions are those of `DEFAULT_TRANSITIONS`.
    """
    days = []
    for begin, end in (day_months[:2], day_months[2:]):
        begin_day, end_day = day_of_year(begin), day_of_year(end)
        if begin_day is not None and end_day is None:
            end_day = begin_day + TRANSITION_DAYS - 1
        elif begin_day is None and end_day is not None:
            begin_day = end_day - TRANSITION_DAYS + 1
        if begin_day is None or end_day is None:
            return DEFAULT_TRANSITIONS
        days += [begin_day, end_day]
    begin_summer, end_summer, begin_winter, end_winter = days
    if not 1 < begin_summer <= end_summer < begin_winter <= end_winter <= 365:
        LOG.warning("Transitions %s are not in the order of the year, the defaults are used", day_months)
        return DEFAULT_TRANSITIONS
    return tuple(day_month(day) for day in days)


def _read_drents_overijsselse_delta(data_dir: Path) -> gpd.GeoDataFrame:
    """Read the peilgebieden of Drents Overijsselse Delta, with their summer and winter levels.

    The levels are in the `streefpeil` table, in which `soortstreefpeil` 901 is the summer level and 902 the winter
    level, with the date from which they apply as `beginperiode`.
    """
    path = data_dir / "DrentsOverijsselseDelta/verwerkt/4_ribasim/hydamo.gpkg"
    peilgebied = gpd.read_file(path, layer="peilgebiedpraktijk")
    streefpeil = gpd.read_file(path, layer="streefpeil", ignore_geometry=True)
    soort = _categories(streefpeil["soortstreefpeil"])
    for code, season in (("901", "zomer"), ("902", "winter")):
        levels = streefpeil.loc[soort == code].drop_duplicates("peilgebiedpraktijkid").set_index("peilgebiedpraktijkid")
        peilgebied[f"{season}peil"] = peilgebied["globalid"].map(levels["waterhoogte"])
        peilgebied[f"begin_{season}peil"] = peilgebied["globalid"].map(levels["beginperiode"])
    return peilgebied


SEASONAL = {"streefpeil_zomer": "ZOMERPEIL", "streefpeil_winter": "WINTERPEIL"}
# The coded types of level management of the IWS standard
IWS_LABELS = {"1": "vast peil", "2": "flexibel peil", "3": "zomer- en winterpeil"}

# The peilgebieden of the water boards, see https://github.com/Deltares/Ribasim-NL/issues/893
SOURCES: dict[str, list[Source]] = {
    "AaenMaas": [Source("AaenMaas/verwerkt/downloads/WS_PEILGEBIEDPolygon.shp", code="CODE", mapping={"*": SEASONAL})],
    "AmstelGooienVecht": [
        Source(
            "AmstelGooienVecht/aangeleverd/Na_levering/vigerende_peilgebieden.gpkg",
            code="GPGIDENT",
            by="IWS_GPGBEHER",
            soort_peilbeheer="IWS_GPGBEHER",
            labels=IWS_LABELS,
            mapping={
                "1": {"streefpeil": "IWS_GPGVASTP"},
                "2": {"ondergrens_zomer": "IWS_GPGONDP", "bovengrens_zomer": "IWS_GPGBOVP"},
                "3": {"streefpeil_zomer": "GPGZMRPL", "streefpeil_winter": "GPGWNTPL"},
            },
            transitions={
                "begin_overgang_winterpeil_zomerpeil": "OV_WP_ZP_B",
                "eind_overgang_winterpeil_zomerpeil": "OV_WP_ZP_E",
                "begin_overgang_zomerpeil_winterpeil": "OV_ZP_WP_B",
                "eind_overgang_zomerpeil_winterpeil": "OV_ZP_WP_E",
            },
        )
    ],
    "BrabantseDelta": [
        Source(
            "BrabantseDelta/verwerkt/4_ribasim/hydamo.gpkg",
            layer="peilgebiedpraktijk",
            code="CODE",
            by="WS_TYPE_PEILBEHEER",
            soort_peilbeheer="WS_TYPE_PEILBEHEER",
            mapping={
                "Vast peil": {"streefpeil": "WS_VAST_PEIL"},
                "Zomer- en winterpeil": {"streefpeil_zomer": "WS_ZOMERPEIL", "streefpeil_winter": "WS_WINTERPEIL"},
            },
        )
    ],
    # The target levels of the weirs, of which the model takes WS_STREEFPEILHOOG, see pipeline/de_dommel
    "DeDommel": [
        Source(
            "DeDommel/verwerkt/4_ribasim/hydamo.gpkg",
            layer="stuw",
            code="CODE",
            mapping={"*": {"streefpeil_zomer": "WS_STREEFPEILHOOG", "streefpeil_winter": "WS_STREEFPEILLAAG"}},
            # all levels are far above NAP
            zero_is_unknown=True,
        )
    ],
    "Delfland": [
        Source(
            "Delfland/aangeleverd/Na_levering/Watersysteem.gdb",
            layer="PeilgebiedPraktijk",
            code="CODE",
            by="WS_SOORTSTREEFPEIL",
            soort_peilbeheer="WS_SOORTSTREEFPEIL",
            mapping={
                "Vast peil": {"streefpeil": "WS_LAAGPEIL"},
                "Zomer- en winterpeil": {"streefpeil_zomer": "WS_HOOGPEIL", "streefpeil_winter": "WS_LAAGPEIL"},
                "Getrapt peil (van/tot)": {"streefpeil_zomer": "WS_HOOGPEIL", "streefpeil_winter": "WS_LAAGPEIL"},
                "Flexibel peil": {"ondergrens": "WS_LAAGPEIL", "bovengrens": "WS_HOOGPEIL"},
            },
        )
    ],
    "DrentsOverijsselseDelta": [
        Source(
            "DrentsOverijsselseDelta/verwerkt/4_ribasim/hydamo.gpkg",
            read=_read_drents_overijsselse_delta,
            code="code",
            mapping={"*": {"streefpeil_zomer": "zomerpeil", "streefpeil_winter": "winterpeil"}},
            transitions={
                "begin_overgang_winterpeil_zomerpeil": "begin_zomerpeil",
                "begin_overgang_zomerpeil_winterpeil": "begin_winterpeil",
            },
        )
    ],
    "HollandseDelta": [
        Source(
            "HollandseDelta/aangeleverd/Na_levering/Peilgebieden_HD_nalevering/Praktijkpeilgebieden.shp",
            code="CODE",
            soort_peilbeheer="SOORT_PEIL",
            mapping={
                "*": {
                    "streefpeil_zomer": "REKENPEIL_",
                    "ondergrens_zomer": "ONDERGRENS",
                    "bovengrens_zomer": "BOVENGRENS",
                    "streefpeil_winter": "REKENPEIL1",
                    "ondergrens_winter": "ONDERGRE_1",
                    "bovengrens_winter": "BOVENGRE_1",
                }
            },
        )
    ],
    "HollandsNoorderkwartier": [
        Source(
            "HollandsNoorderkwartier/aangeleverd/Na_levering/20240618_peilgebieden_en_polders/Peilgebieden_export_2024-06-18.shp",
            code="CODE",
            by="PEILBEHEER",
            soort_peilbeheer="PEILBEHEER",
            # the attributes that do not apply are 0
            zero_is_unknown=True,
            mapping={
                "Dynamisch seizoengebonden": {
                    "streefpeil_zomer": "STREEFPE_2",
                    "ondergrens_zomer": "ONDERGRE_2",
                    "bovengrens_zomer": "BOVENGRE_2",
                    "streefpeil_winter": "STREEFPE_1",
                    "ondergrens_winter": "ONDERGRE_1",
                    "bovengrens_winter": "BOVENGRE_1",
                },
                "Dynamisch": {"streefpeil": "STREEFPEIL", "ondergrens": "ONDERGRENS", "bovengrens": "BOVENGRENS"},
                "Vast": {"streefpeil": "VAST"},
                "Flexibel": {"ondergrens": "ONDERGRENS", "bovengrens": "BOVENGRENS"},
                "Seizoengebonden": {"streefpeil_zomer": "ZOMER", "streefpeil_winter": "WINTER"},
            },
        )
    ],
    "HunzeenAas": [
        Source(
            "HunzeenAas/verwerkt/1_ontvangen_data/peilgebieden.gpkg",
            code="gpgident",
            mapping={"*": {"streefpeil_zomer": "gpgzmrpl", "streefpeil_winter": "gpgwntpl"}},
        )
    ],
    "Noorderzijlvest": [
        Source(
            "Noorderzijlvest/verwerkt/5_D_HYDRO_export/peilgebieden/HydrologischeEenheden_v38_modelselectie2.shp",
            code="GPGIDENT",
            mapping={"*": {"streefpeil_zomer": "OPVAFWZP", "streefpeil_winter": "OPVAFWWP"}},
        )
    ],
    "Rijnland": [
        Source(
            "Rijnland/aangeleverd/PeilgebiedPraktijk Afwatering/PeilgebiedPraktijkRijnland.gdb.zip",
            member="PeilgebiedPraktijkRijnland.gdb",
            layer="PeilgebiedPraktijk",
            code="CODE",
            by="SOORTPEILBEHEER",
            soort_peilbeheer="SOORTPEILBEHEER",
            mapping={
                "vast peil": {"streefpeil": "VASTPEIL"},
                "dynamisch peil": {
                    "ondergrens_zomer": "FLEXZOMERPEILONDERGRENS",
                    "bovengrens_zomer": "FLEXZOMERPEILBOVENGRENS",
                    "ondergrens_winter": "FLEXWINTERPEILONDERGRENS",
                    "bovengrens_winter": "FLEXWINTERPEILBOVENGRENS",
                },
                "seizoensgebonden peil (flexibel)": {
                    "ondergrens_zomer": "FLEXZOMERPEILONDERGRENS",
                    "bovengrens_zomer": "FLEXZOMERPEILBOVENGRENS",
                    "ondergrens_winter": "FLEXWINTERPEILONDERGRENS",
                    "bovengrens_winter": "FLEXWINTERPEILBOVENGRENS",
                },
                "seizoensgebonden peil (vast)": SEASONAL,
                "seizoensgebonden peil (winter vast en zomer flexibel)": {
                    "ondergrens_zomer": "FLEXZOMERPEILONDERGRENS",
                    "bovengrens_zomer": "FLEXZOMERPEILBOVENGRENS",
                    "streefpeil_winter": "WINTERPEIL",
                },
            },
        )
    ],
    "Rivierenland": [
        Source(
            "Rivierenland/aangeleverd/Eerste_levering/OverigeGegevens.zip",
            member="OverigeGegevens/OverigeGegevens.gdb",
            layer="PeilgebiedenPraktijk",
            code="CODE",
            by="TYPE_PEILBEHEER",
            soort_peilbeheer="TYPE_PEILBEHEER",
            mapping={
                "flexibel": {"ondergrens": "MIN_PEIL", "bovengrens": "MAX_PEIL"},
                "seizoensgebonden": SEASONAL,
                "streef": {"streefpeil": "STREEFPEIL"},
                "vast": {"streefpeil": "VASTPEIL"},
            },
        )
    ],
    "Scheldestromen": [
        Source(
            "Scheldestromen/aangeleverd/Na_levering/Praktijkpeilgebieden_20231204/Praktijkpeilgebieden_20231204.shp",
            code="GPGIDENT",
            by="GPGBEHER",
            soort_peilbeheer="GPGBEHER",
            labels=IWS_LABELS,
            mapping={
                "1": {"streefpeil_zomer": "GPGZP", "streefpeil_winter": "GPGWP"},
                "2": {
                    "streefpeil_zomer": "GPGZP",
                    "streefpeil_winter": "GPGWP",
                    "bovengrens_winter": "GPGFPWB",
                    "ondergrens_zomer": "GPGFPZO",
                },
                "3": {"streefpeil_zomer": "GPGZP", "streefpeil_winter": "GPGWP", "ondergrens_winter": "GPGOWP"},
            },
        )
    ],
    "StichtseRijnlanden": [
        Source(
            "StichtseRijnlanden/verwerkt/4_ribasim/peilgebieden.gpkg",
            code="WS_PGID",
            by="WS_PBSOORT",
            soort_peilbeheer="WS_PBSOORT",
            labels=IWS_LABELS,
            mapping={
                "1": {"streefpeil": "WS_VP"},
                "2": {"ondergrens": "WS_OP", "bovengrens": "WS_BP"},
                "3": {"streefpeil_zomer": "WS_ZP", "streefpeil_winter": "WS_WP"},
            },
        )
    ],
    "Vechtstromen": [
        Source(
            "Vechtstromen/verwerkt/1_ontvangen_data/aanvulling feb 24/Peilgebied_(met_peilregister_peilen).shp",
            code="GPGIDENT",
            by="GPGSOORT",
            zero_is_unknown=True,
            mapping={
                "afvoerpeilgebied": {"streefpeil_zomer": "PEILBEPALE", "streefpeil_winter": "PEILBEPA_1"},
                # not in issue #893, but with the same levels as an afvoerpeilgebied
                "aanvoerpeilgebied": {"streefpeil_zomer": "PEILBEPALE", "streefpeil_winter": "PEILBEPA_1"},
                "aan- en afvoerpeilgebied": {
                    "streefpeil_zomer": "PEILBEPALE",
                    "streefpeil_winter": "PEILBEPA_1",
                    "bovengrens_zomer": "AANSLAGPEI",
                    "bovengrens_winter": "AANSLAGP_1",
                    "ondergrens_zomer": "AFSLAGPEIL",
                    "ondergrens_winter": "AFSLAGPE_1",
                },
                "bemalen": {
                    "streefpeil_zomer": "PEILBEPALE",
                    "streefpeil_winter": "PEILBEPA_1",
                    "ondergrens_zomer": "AFSLAGPEIL",
                    "ondergrens_winter": "AFSLAGPE_1",
                },
            },
        )
    ],
    "WetterskipFryslan": [
        Source(
            f"WetterskipFryslan/aangeleverd/Na_levering/20241205_verwerkte_peilgebieden_en_boezem/{name}.shp",
            code="GPGIDENT",
            by="IWS_GPGBEH",
            soort_peilbeheer="IWS_GPGBEH",
            labels=IWS_LABELS,
            mapping={
                "1": {"streefpeil": "IWS_GPGVAS"},
                "2": {"ondergrens": "IWS_GPGOND", "bovengrens": "IWS_GPGBOV"},
                "3": {"streefpeil_zomer": "GPGZMRPL", "streefpeil_winter": "GPGWNTPL"},
            },
        )
        for name in ("peilgebieden", "boezem")
    ],
    "Zuiderzeeland": [
        Source(
            "Zuiderzeeland/aangeleverd/Na_levering/peilgebieden.gpkg",
            code="GPGIDENT",
            soort_peilbeheer="IWS_GPGBEHER",
            labels=IWS_LABELS,
            mapping={
                "*": {
                    "streefpeil_zomer": "GPGZMRPL",
                    "streefpeil_winter": "GPGWNTPL",
                    "ondergrens_zomer": "IWS_ONDERGRENS_ZOMERPEIL",
                    "bovengrens_zomer": "IWS_BOVENGRENS_ZOMERPEIL",
                }
            },
        )
    ],
}


def read_peilbeheer(authority: str, data_dir: Path) -> gpd.GeoDataFrame:
    """Read the level management of the peilgebieden of a water board, with the columns of `COLUMNS`.

    :param authority: water board, a key of `SOURCES`
    :param data_dir: the data directory
    :return: the peilgebieden with a target level, completed with `complete_peilbeheer`
    """
    parts = []
    for source in SOURCES[authority]:
        gdf = source.read_dataset(data_dir)
        parts.append(source.peilbeheer(gdf))
    df = pd.concat(parts, ignore_index=True)
    df["authority"] = authority
    df = gpd.GeoDataFrame(complete_peilbeheer(df), geometry="geometry", crs=CRS)
    geometry = gpd.GeoSeries(shapely.force_2d(df.geometry.values), index=df.index, crs=CRS)
    # Areas are made valid, without the lines and points that this may leave
    is_area = geometry.geom_type.isin(["Polygon", "MultiPolygon"])
    geometry[is_area] = shapely.buffer(shapely.make_valid(geometry[is_area].to_numpy()), 0)
    df["geometry"] = geometry
    valid = ~df.geometry.is_empty & (df.geometry.geom_type.eq("Point") | df.geometry.area.gt(0))
    return df.loc[valid, [*COLUMNS, "geometry"]].reset_index(drop=True)

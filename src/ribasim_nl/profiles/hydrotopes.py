"""Interface for hydrotopes."""

import dataclasses
import typing

import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage


class HydrotopeTable:
    """Table with hydrotope-entries."""

    collector: dict[int, "Hydrotope"]

    def __init__(self, *hydrotope: "Hydrotope") -> None:
        """Initiation.

        :param hydrotope: hydrotope-definition
        :type hydrotope: Hydrotope
        """
        self.collector = {h.fid: h for h in hydrotope}

    def __str__(self) -> str:
        """String representation of the table, returning all entries as a table-format.

        :return: tabulated hydrotopes
        :rtype: str
        """
        return "\n".join(map(str, self.collector.values()))

    def __getitem__(self, fid: int) -> "Hydrotope":
        """Getter-function: Get hydrotope by its ID.

        :param fid: hydrotope ID
        :type fid: int

        :return: referenced hydrotope
        :rtype: Hydrotope

        :raises KeyError: if no hydrotope is stored in the table with `fid`
        """
        return self.collector[fid]

    def __len__(self) -> int:
        """Length of the table, i.e., number of hydrotopes in the table

        :return: table length
        :rtype: int
        """
        return len(self.collector)

    def __iter__(self) -> typing.Iterable[int]:
        """Iterator-function: Iterating over table `collector`-attribute (`dict`)

        :return: dictionary-iterator
        :rtype: iterable[int]
        """
        return iter(self.collector)

    def add_hydrotope(self, hydrotope: "Hydrotope") -> None:
        """Add hydrotope to the table.

        :param hydrotope: hydrotope-definition
        :type hydrotope: Hydrotope

        :raises ValueError: if hydrotope ID is already used
        """
        if hydrotope.fid in self.collector:
            msg = f"{hydrotope.fid=} already used: {self.collector[hydrotope.fid]}"
            raise ValueError(msg)

        self.collector.update({hydrotope.fid: hydrotope})

    def add_from_specs(self, fid: int, name: str, depths: tuple[float, float, float, float]) -> "Hydrotope":
        """Add hydrotope to the table by its definition

        :param fid: hydrotope ID
        :param name: hydrotope name
        :param depths: hydrotope depth-classes

        :type fid: int
        :type name: str
        :type depths: tuple[float, float, float, float]

        :return: hydrotope
        :rtype: Hydrotope
        """
        hydrotope = Hydrotope(fid, name, depths)
        self.add_hydrotope(hydrotope)
        return hydrotope

    def __add__(self, other: "HydrotopeTable") -> "HydrotopeTable":
        """Add-function: Add hydrotope-entries of two tables together.

        Overlapping hydrotope IDs cause the hydrotopes as defined in `other` to be used over them in `self`.

        :param other: other hydrotope-table
        :type other: HydrotopeTable

        :return: collective hydrotope-table
        :rtype: HydrotopeTable
        """
        collection = {**self.collector, **other.collector}
        return self.__init__(*collection.values())

    @classmethod
    def from_csv(cls, fn: str, **kwargs) -> "HydrotopeTable":
        """Initiate a hydrotope-table from a *.csv-file, containing the required information per hydrotope.

        :param fn: filename
        :param kwargs: optional arguments

        :key read_kw: kwargs passed to `pandas.read_csv()`, defaults to {}
        :key col_fid: column-name with hydrotope IDs, defaults to first column of *.csv-file
        :key col_name: column-name with hydrotope names, defaults to second column of *.csv-file
        :key cols_depths: column-names with hydrotope depth-classes, defaults to third to sixth columns of *.csv-file

        :type fn: str

        :return: initiate hydrotope-table
        :rtype: HydrotopeTable

        :raises AssertionError: if data contains less than six columns
        :raises AssertionError: if `cols_depths` does not contain four (4) column-names
        """
        # read data
        read_kw: dict[str, typing.Any] = kwargs.get("read_kw", {})
        data = pd.read_csv(fn, **read_kw)
        assert len(data.columns) >= 6

        # column-names
        col_fid: str = kwargs.get("col_fid", data.columns[0])
        col_name: str = kwargs.get("col_name", data.columns[1])
        cols_depths: list[str] = kwargs.get("cols_depths", data.columns[2:6])
        assert len(cols_depths) == 4

        # ensure data-types
        data.astype({col_fid: int, col_name: str, **dict.fromkeys(cols_depths, float)})

        # initiate hydrotopes
        hydrotypes = [
            Hydrotope(fid, name, depths)
            for _, (fid, name, *depths) in data[[col_fid, col_name, *cols_depths]].iterrows()
        ]

        # initiate tabe
        table = cls(*hydrotypes)

        # return table
        return table

    @property
    def hydrotopes(self) -> list["Hydrotope"]:
        """List of hydrotopes.

        :return: hydrotopes
        :rtype: list[Hydrotope]
        """
        return [*self.collector.values()]

    def get_by_fid(self, fid: int, *, default: typing.Any = None) -> "Hydrotope":
        """Get hydrotope from table by its ID.

        :param fid: hydrotope ID
        :param default: default value when no hydrotope matches the ID, defaults to None

        :type fid: int
        :type default: any, optional

        :return: hydrotope
        :rtype: Hydrotope
        """
        return self.collector.get(fid, default)

    def get_by_name(self, name: str, *, default: typing.Any = None) -> "Hydrotope":
        """Get hydrotope from by its name.

        :param name: hydrotope name
        :param default: default value when no hydrotope matches the name, defaults to None

        :type name: str
        :type default: any, optional

        :return: hydrotope
        :rtype: Hydrotope
        """
        return next((h for h in self.collector.values() if h.name == name), default)


@dataclasses.dataclass
class Hydrotope:
    """Hydrotope definition."""

    fid: int
    name: str
    depths: tuple[float, float, float, float]

    def __post_init__(self):
        """Post-initiation input validation."""
        assert len(self.depths) == 4

    def depth(self, width: float) -> float:
        """Representative depth of hydrotope based on the width.

        :param width: width of hydro-object
        :type width: float

        :return: representative depth
        :rtype: float
        """
        if width < 1:
            return self.depths[0]
        elif width < 3:
            return self.depths[1]
        elif width < 6:
            return self.depths[2]
        return self.depths[3]


def get_hydrotopes_map(
    cloud: CloudStorage = CloudStorage(), *, sync: bool = True, crs: str = "epsg:28992"
) -> gpd.GeoDataFrame:
    """Get map with hydrotope data from the GoodCloud.

    :param cloud: the GoodCloud, defaults to CloudStorage
    :param sync: sync the GoodCloud prior to reading the map, defaults to True
    :param crs: set CRS of hydrotope map (missing), defaults to 'epsg:28992'

    :type cloud: CloudStorage
    :type sync: bool, optional
    :type crs: str, optional

    :return: hydrotope map
    :rtype: geopandas.GeoDataFrame
    """
    # sync with the GoodCloud
    if sync:
        cloud.download_basisgegevens(["Hydrotypen"])

    # get hydrotope map
    fn = cloud.joinpath("Basisgegevens", "Hydrotypen", "hydrotype.shp")
    gdf = gpd.read_file(fn)
    gdf.crs = crs
    return gdf

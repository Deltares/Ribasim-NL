"""Interface for hydrotopes."""

import dataclasses
import typing

import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage


class HydrotopeTable:
    def __init__(self, *hydrotope: "Hydrotope"):
        self.collector: dict[int, Hydrotope] = {h.fid: h for h in hydrotope}

    def __str__(self) -> str:
        return "\n".join(map(str, self.collector.values()))

    def __getitem__(self, fid: int) -> "Hydrotope":
        return self.collector[fid]

    def __len__(self) -> int:
        return len(self.collector)

    def __iter__(self) -> typing.Iterable[int]:
        return iter(self.collector)

    def add_hydrotope(self, hydrotope: "Hydrotope") -> None:
        if hydrotope.fid in self.collector:
            msg = f"{hydrotope.fid=} already used: {self.collector[hydrotope.fid]}"
            raise ValueError(msg)

        self.collector.update({hydrotope.fid: hydrotope})

    def add_from_specs(self, fid: int, name: str, depths: tuple[float, float, float, float]) -> "Hydrotope":
        hydrotope = Hydrotope(fid, name, depths)
        self.add_hydrotope(hydrotope)
        return hydrotope

    def __add__(self, other: "HydrotopeTable") -> "HydrotopeTable":
        collection = {**self.collector, **other.collector}
        return self.__init__(*collection.values())

    @classmethod
    def from_csv(cls, fn: str, **kwargs) -> "HydrotopeTable":
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
        return [*self.collector.values()]

    def get_by_fid(self, fid: int, *, default: typing.Any = None) -> "Hydrotope":
        return self.collector.get(fid, default)

    def get_by_name(self, name: str, *, default: typing.Any = None) -> "Hydrotope":
        return next((h for h in self.collector.values() if h.name == name), default)


@dataclasses.dataclass
class Hydrotope:
    fid: int
    name: str
    depths: tuple[float, float, float, float]

    def __post_init__(self):
        assert len(self.depths) == 4

    def depth(self, width: float | None = None) -> tuple[float, float, float, float] | float:
        if width is None:
            return self.depths

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
    if sync:
        cloud.download_basisgegevens(["Hydrotypen"])

    fn = cloud.joinpath("Basisgegevens", "Hydrotypen", "hydrotype.shp")

    gdf = gpd.read_file(fn)
    gdf.crs = crs

    return gdf

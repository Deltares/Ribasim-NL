"""Interface for hydrotopes."""

import dataclasses
import typing

import pandas as pd


class HydrotopeTable:
    def __init__(self, *hydrotope: "Hydrotope"):
        self.collector: dict[int, Hydrotope] = {h.fid: h for h in hydrotope}

    def __getitem__(self, fid: int) -> "Hydrotope":
        if fid not in self.collector:
            raise KeyError
        return self.collector[fid]

    def __str__(self) -> str:
        return "\n".join(map(str, self.collector.values()))

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

    def get_by_fid(self, fid: int) -> "Hydrotope":
        return self.collector[fid]

    def get_by_name(self, name: str) -> "Hydrotope":
        hydrotope = next(h for h in self.collector.values() if h.name == name)
        return hydrotope


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

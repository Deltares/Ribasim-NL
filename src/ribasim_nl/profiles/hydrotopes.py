"""Interface for hydrotopes."""

import dataclasses
import typing

import pandas as pd


class HydrotopeTable:
    collector: dict[int, "Hydrotope"]

    def __getitem__(self, fid: int) -> "Hydrotope":
        if fid not in self.collector:
            raise KeyError
        return self.collector[fid]

    def __str__(self) -> str:
        return "\n".join(map(str, self.collector.values()))

    @classmethod
    def add(cls, hydrotope: "Hydrotope") -> "Hydrotope":
        if hydrotope.fid in cls.collector:
            msg = f"{hydrotope.fid=} already used: {cls.collector[hydrotope.fid]}"
            raise KeyError(msg)

        cls.collector.update({hydrotope.fid: hydrotope})

        return hydrotope

    @classmethod
    def remove(cls, *, fid: int | None = None, hydrotope: "Hydrotope" | None = None) -> "Hydrotope":
        assert (fid is None) ^ (hydrotope is None)
        if hydrotope is not None:
            fid = hydrotope.fid

        if fid not in cls.collector:
            msg = f"{fid=} not found; choose one of {(*cls.collector.keys(),)}"
            raise KeyError(msg)

        return cls.collector.pop(fid)

    @classmethod
    def reset(cls) -> None:
        cls.collector.clear()

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
        data.apply(lambda row: Hydrotope(row[col_fid], row[col_name], (*row[cols_depths].values,)), axis=1)

        # return table
        return cls()

    @classmethod
    def get_by_fid(cls, fid: int) -> "Hydrotope":
        return cls.collector[fid]

    @classmethod
    def get_by_name(cls, name: str) -> "Hydrotope":
        (hydrotope,) = [h for h in cls.collector.values() if h.name == name]
        return hydrotope


@dataclasses.dataclass
class Hydrotope:
    fid: int
    name: str
    depths: tuple[float, float, float, float]

    def __post_init__(self):
        assert len(self.depths) == 4
        HydrotopeTable.add(self)

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


def depth_by_name(name: str, width: float) -> float:
    if name not in [h.name for h in HydrotopeTable.collector.values()]:
        msg = f"Unknown {name=}"
        raise KeyError(msg)

    return HydrotopeTable.get_by_name(name).depth(width=width)


def depth_by_fid(fid: int, width: float) -> float:
    if fid not in HydrotopeTable.collector.keys():
        msg = f"Unknown {fid=}"
        raise KeyError(msg)

    return HydrotopeTable.get_by_fid(fid).depth(width=width)

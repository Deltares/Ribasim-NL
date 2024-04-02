# %%
import math
import numbers
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal

from bokeh.models import DatetimeTickFormatter, Slider


def get_formatter(format="%Y-%m-%d %H:%M:%S"):
    return DatetimeTickFormatter(
        years=format, days=format, hours=format, minutes=format
    )


def round_seconds(ts_seconds, td_seconds, method):
    if method in ["floor", "ceil"]:
        rounder = getattr(math, method)
    else:
        rounder = round

    return rounder(ts_seconds / td_seconds) * td_seconds


def round_datetime(
    dt, timedelta_round, method: Literal["floor", "ceil", "round"] = "round"
):
    ts_seconds = dt.timestamp()
    td_seconds = timedelta_round.total_seconds()
    rounded_seconds = round_seconds(ts_seconds, td_seconds, method)

    return datetime.fromtimestamp(rounded_seconds)


@dataclass
class DatetimeSlider:
    start: datetime | int
    end: datetime | int
    value: datetime | int
    step: timedelta | int
    format: str = "%Y-%m-%d %H:%M:%S"
    kwargs: dict = field(default_factory=dict)
    widget: Slider | None = None

    def __post_init__(self):
        # validate values
        if self.start >= self.end:
            raise ValueError(f"{self.start} >= {self.end}")

        if self.value < self.start:
            raise ValueError(f"{self.value} < {self.start}")

        if self.value > self.end:
            raise ValueError(f"{self.value} > {self.end}")

        if isinstance(self.start, datetime):
            start = self.start.timestamp() * 1000
        else:
            start = self.start

        if isinstance(self.end, datetime):
            end = self.end.timestamp() * 1000
        else:
            end = self.end

        if isinstance(self.value, datetime):
            value = self.value.timestamp() * 1000
        else:
            value = self.value

        if isinstance(self.step, timedelta):
            step = self.step.total_seconds() * 1000
        else:
            step = self.step

        self.widget = Slider(
            start=round_seconds(start, step, "floor"),
            end=round_seconds(end, step, "ceil"),
            value=round_seconds(value, step, "round"),
            step=step,
            format=get_formatter(self.format),
            **self.kwargs,
        )

    @property
    def value_as_datetime(self):
        if isinstance(self.widget.value, numbers.Number):
            return datetime.fromtimestamp(self.widget.value / 1000)
        else:
            return self.widget.value

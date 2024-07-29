import numbers
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from bokeh.models import Slider

from bokeh_helpers.widgets.shared_functions import get_formatter, round_seconds


@dataclass
class DatetimeSlider:
    start: datetime | int
    end: datetime | int
    value: datetime | int
    step: datetime | int
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

    @property
    def steps(self):
        step = int(self.widget.step)
        start = int(self.widget.start)
        stop = int(self.widget.end + step)
        return list(range(start, stop, step))

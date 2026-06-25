import numbers
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from bokeh.models import Slider

# pyrefly: ignore[missing-import]
from bokeh_helpers.widgets.shared_functions import get_formatter, round_seconds


@dataclass
class DatetimeSlider:
    start: datetime | int
    end: datetime | int
    value: datetime | int
    step: datetime | int
    format: str = "%Y-%m-%d %H:%M:%S"
    kwargs: dict[str, object] = field(default_factory=dict)
    widget: Slider | None = None

    def __post_init__(self) -> None:
        # validate values
        if self.start >= self.end:
            raise ValueError(f"{self.start} >= {self.end}")

        if self.value < self.start:
            raise ValueError(f"{self.value} < {self.start}")

        if self.value > self.end:
            raise ValueError(f"{self.value} > {self.end}")

        start = self.start.timestamp() * 1000 if isinstance(self.start, datetime) else self.start

        end = self.end.timestamp() * 1000 if isinstance(self.end, datetime) else self.end

        value = self.value.timestamp() * 1000 if isinstance(self.value, datetime) else self.value

        step = self.step.total_seconds() * 1000 if isinstance(self.step, timedelta) else self.step

        self.widget = Slider(
            start=round_seconds(start, step, "floor"),
            end=round_seconds(end, step, "ceil"),
            value=round_seconds(value, step, "round"),
            step=step,
            format=get_formatter(self.format),
            **self.kwargs,
        )

    @property
    def value_as_datetime(self) -> datetime | float | object:
        if isinstance(self.widget.value, numbers.Number):
            return datetime.fromtimestamp(self.widget.value / 1000)
        else:
            return self.widget.value

    @property
    def steps(self) -> list[int]:
        step = int(self.widget.step)
        start = int(self.widget.start)
        stop = int(self.widget.end + step)
        return list(range(start, stop, step))

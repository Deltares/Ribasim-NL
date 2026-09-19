"""Shared DiscreteControl layouts used by full-control setup and level syncs."""

DEFAULT_CONTROL_THRESHOLD_OFFSET = 0.02
DEFAULT_LEVEL_THRESHOLD_RANGE = 0.05
DEFAULT_FLOW_RATE_THRESHOLD_FRACTION = 0.1
DEFAULT_FLOW_RATE_THRESHOLD_MARGIN = 0.001
ControlLayout = tuple[dict[int, int], dict[int, int], set[tuple[str, str]]]

SINGLE_BASIN_LOGIC = (("F", "aanvoer"), ("T", "afvoer"))
FLOW_CONTROL_LOGIC = (
    ("FFF", "aanvoer"),
    ("FFT", "aanvoer"),
    ("TFF", "afvoer"),
    ("TFT", "aanvoer"),
    ("TTF", "afvoer"),
    ("TTT", "afvoer"),
)
FLOW_DEMAND_LOGIC = (
    ("FF", "afvoer"),
    ("FT", "aanvoer"),
    ("TF", "afvoer"),
    ("TT", "afvoer"),
)


def level_threshold_pair(
    threshold_high: float,
    threshold_low: float | None = None,
    threshold_range: float = DEFAULT_LEVEL_THRESHOLD_RANGE,
) -> tuple[float, float]:
    """Return low and high thresholds, preserving an explicitly provided pair."""
    threshold_high = float(threshold_high)
    if threshold_low is not None:
        threshold_low = float(threshold_low)
        if threshold_low > threshold_high:
            raise ValueError("threshold_low must not exceed threshold_high")
        return threshold_low, threshold_high
    if threshold_range < 0:
        raise ValueError("threshold_range must be non-negative")
    half_range = float(threshold_range) / 2
    return threshold_high - half_range, threshold_high + half_range


def flow_rate_threshold_pair(
    threshold: float,
    threshold_fraction: float = DEFAULT_FLOW_RATE_THRESHOLD_FRACTION,
    minimum_margin: float = DEFAULT_FLOW_RATE_THRESHOLD_MARGIN,
) -> tuple[float, float]:
    """Return thresholds using the larger of a relative or absolute margin."""
    threshold = float(threshold)
    if threshold_fraction < 0 or minimum_margin < 0:
        raise ValueError("Flow-rate threshold margins must be non-negative")
    margin = max(abs(threshold) * float(threshold_fraction), float(minimum_margin))
    return threshold - margin, threshold + margin


def control_layout_key(function: str, flow_demand_controlled: bool = False, control_name: str | None = None) -> str:
    """Return the control layout that should be used for a controlled node."""
    if flow_demand_controlled:
        return "flow_demand"

    if control_name is not None and ":" in control_name:
        control_name_function = control_name.split(":", 1)[0].strip().lower()
        if control_name_function in {"inlaat", "uitlaat", "doorlaat"}:
            return control_name_function

    return str(function).lower()


def control_logic(layout_key: str) -> tuple[tuple[str, str], ...]:
    """Return the truth-state to control-state mapping for a control.py layout."""
    if layout_key in {"inlaat", "uitlaat"}:
        return SINGLE_BASIN_LOGIC
    if layout_key == "doorlaat":
        return FLOW_CONTROL_LOGIC
    if layout_key == "flow_demand":
        return FLOW_DEMAND_LOGIC
    return ()


def control_layouts() -> dict[str, ControlLayout]:
    """Return expected variable, condition and logic counts per control.py layout."""
    single_basin_logic = set(SINGLE_BASIN_LOGIC)
    return {
        "inlaat": ({1: 1}, {1: 1}, single_basin_logic),
        "uitlaat": ({1: 1}, {1: 1}, single_basin_logic),
        "doorlaat": ({1: 1, 2: 1}, {1: 2, 2: 1}, set(FLOW_CONTROL_LOGIC)),
        "flow_demand": ({1: 1, 2: 1}, {1: 1, 2: 1}, set(FLOW_DEMAND_LOGIC)),
    }


def control_condition_thresholds(
    *,
    layout_key: str,
    compound_variable_id: int,
    variable_name: str,
    level_value: float,
    weight: float,
    level_difference_threshold: float = DEFAULT_CONTROL_THRESHOLD_OFFSET,
) -> list[float]:
    """Return threshold values for a level or flow-rate update in a control.py layout."""
    layout_key = str(layout_key).lower()
    variable_name = str(variable_name).lower()
    compound_variable_id = int(compound_variable_id)
    weight = float(weight)

    if layout_key == "doorlaat":
        if compound_variable_id == 1:
            if variable_name != "level" or weight != 1.0:
                raise ValueError("Doorlaat compound_variable_id=1 moet level met weight=1 zijn volgens control.py.")
            return [float(level_value), float(level_value) + float(level_difference_threshold)]
        if compound_variable_id == 2:
            if variable_name != "level" or weight != -1.0:
                raise ValueError("Doorlaat compound_variable_id=2 moet level met weight=-1 zijn volgens control.py.")
            return [-float(level_value)]

    if layout_key in {"inlaat", "uitlaat"} and compound_variable_id == 1:
        if variable_name != "level" or weight != 1.0:
            raise ValueError(f"{layout_key.capitalize()} compound_variable_id=1 moet level met weight=1 zijn.")
        return [float(level_value)]

    if layout_key == "flow_demand":
        if compound_variable_id == 1:
            if variable_name != "level" or weight != 1.0:
                raise ValueError("FlowDemand compound_variable_id=1 moet level met weight=1 zijn volgens control.py.")
            return [float(level_value)]
        if compound_variable_id == 2:
            if variable_name != "flow_rate" or weight != -1.0:
                raise ValueError(
                    "FlowDemand compound_variable_id=2 moet flow_rate met weight=-1 zijn volgens control.py."
                )
            return [-float(level_value)]

    return [float(level_value) * weight]

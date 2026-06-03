"""Shared DiscreteControl layouts used by full-control setup and level syncs."""

from __future__ import annotations

DEFAULT_CONTROL_THRESHOLD_OFFSET = 0.02
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

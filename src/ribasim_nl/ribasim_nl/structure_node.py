from math import pi


def calculate_area(width: float, shape: str, height: float | None = None) -> float:
    """Calculate flow-area of a cross-section"""
    # shapes that only use width
    if shape == "round":
        return pi * (width / 2) ** 2

    # shapes that need height
    elif height is None:
        raise ValueError(f"for shape {shape} height cannot be None")
    elif shape == "rectangle":
        return width * height
    elif shape == "ellipse":
        return pi * (width / 2) * (height / 2)

    # shapes not implemented
    else:
        raise ValueError(f"shape {shape} not implemented")


def calculate_velocity(
    level: float,
    crest_level,
):
    """Calculate velocity over a weir-type structure."""
    if crest_level > level:
        return 0
    else:
        return ((2 / 3) * 9.81 * (level - crest_level)) ** (1 / 2)


def calculate_flow_rate(
    level: float,
    crest_level: float,
    width: float,
    height: float | None = None,
    loss_coefficient: float = 0.63,
    shape: str = "rectangle",
) -> float:
    velocity = calculate_velocity(level=level, crest_level=crest_level)
    area = width * ((2 / 3) * (level - crest_level))
    if height is not None:
        area = min(area, calculate_area(width=width, shape=shape, height=height))

    return round(loss_coefficient * area * velocity, 2)

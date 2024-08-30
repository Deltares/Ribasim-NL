from math import pi

from ribasim.nodes import outlet, tabulated_rating_curve


def calculate_area(width: float, shape: str, height: float | None = None):
    """Calculate flow-area of a cross-section"""
    # shapes that only use width
    if shape == "round":
        return pi * (width / 2) ** 2

    # shapes that need height
    elif height is None:
        raise ValueError(f"for shape {shape} height cannot be None")
    elif shape in ["rectangle"]:
        return width * height
    elif shape in ["ellipse"]:
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
):
    velocity = calculate_velocity(level=level, crest_level=crest_level)
    area = width * ((2 / 3) * (level - crest_level))
    if height is not None:
        area = min(area, calculate_area(width=width, shape=shape, height=height))

    return round(loss_coefficient * area * velocity, 2)


def get_outlet(
    crest_level: float, width: float, shape: str = "rectangle", height: float | None = None, max_velocity: float = 1
) -> outlet.Static:
    """Return an outlet curve from structure-data"""
    area = calculate_area(width=width, shape=shape, height=height)
    flow_rate = round(area * max_velocity, 2)

    return outlet.Static(flow_rate=[flow_rate], min_crest_level=crest_level)


def get_tabulated_rating_curve(
    crest_level,
    width,
    loss_coefficient: float = 0.63,
    height: float | None = None,
    shape: str = "rectangle",
    levels: list[float] = [0, 0.05, 0.1, 0.25, 0.5],
) -> tabulated_rating_curve.Static:
    """Return a tabulated-rating curve from structure-data"""
    level = [round(crest_level, 2) + i for i in levels]
    flow_rate = [
        calculate_flow_rate(
            level=i,
            crest_level=crest_level,
            width=width,
            height=height,
            shape=shape,
            loss_coefficient=loss_coefficient,
        )
        for i in level
    ]

    return tabulated_rating_curve.Static(level=level, flow_rate=flow_rate)

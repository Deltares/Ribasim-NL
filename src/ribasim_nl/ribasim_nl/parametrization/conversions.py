# %%
import math
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd


def mm_per_day_to_m3_per_second(mm_per_day: float, area_m2: float, precision: float | int = 0.001):
    """
    Convert rainfall rate from mm/day to m³/s based on the given area.

    Parameters
    ----------
        mm_per_day (float): Millimeters per day (net rainfall).
        area_m2 (float): Area in square meters.
        precision (float): Optional rounding precision (e.g., 10, 100, 0.1). Defaults t= 0.001

    Returns
    -------
        float: Flow rate in cubic meters per second (m³/s).
    """
    # Convert mm/day to m³/day
    volume_m3_per_day = mm_per_day * area_m2 * 1e-3

    # Convert m³/day to m³/s
    flow_rate_m3_per_s = volume_m3_per_day / 86400

    return round_to_precision(flow_rate_m3_per_s, precision)


def round_to_precision(number: float, precision: float | int):
    """
    Round a number to the nearest multiple of the specified precision.

    Parameters
    ----------
        number (float): The number to round.
        precision (float): The rounding precision (e.g., 10, 100, 0.1).

    Returns
    -------
        float: The rounded number.
    """
    if pd.isna(number):  # can't round nans
        return number

    number = Decimal(str(number))
    if precision == 0:
        rounded = (number).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    else:
        precision = Decimal(str(precision))
        rounded = (number / precision).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * precision

    return float(rounded)


def round_to_significant_digits(number: float, significant_digits: int = 3) -> float:
    """
    Rounds a number to a specified maximum number of significant digits.

    Parameters
    ----------
        number (float): The input number.
        significant_digits (int): The maximum number of significant digits.

    Returns
    -------
        float: The rounded number.
    """
    if (number == 0) | pd.isna(number):
        return number  # Zero or nan remains zero or nan
    if significant_digits <= 0:
        raise ValueError("max_significant_digits must be a positive integer.")

    # Determine the order of magnitude of the number
    exponent = int(math.floor(math.log10(abs(number))))
    # Calculate the rounding precision
    precision = 10 ** (exponent - significant_digits + 1)
    # Round the number to the nearest significant digit
    return round_to_precision(number, precision)

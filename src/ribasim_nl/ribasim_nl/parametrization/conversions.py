# %%
import math
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd


def round_to_precision(number: float, precision: float | int) -> float:
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

    dec_number = Decimal(str(number))
    if precision == 0:
        rounded = dec_number.quantize(Decimal(1), rounding=ROUND_HALF_UP)
    else:
        dec_precision = Decimal(str(precision))
        rounded = (dec_number / dec_precision).quantize(Decimal(1), rounding=ROUND_HALF_UP) * dec_precision

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
    exponent = math.floor(math.log10(abs(number)))
    # Calculate the rounding precision
    precision = 10 ** (exponent - significant_digits + 1)
    # Round the number to the nearest significant digit
    return round_to_precision(number, precision)

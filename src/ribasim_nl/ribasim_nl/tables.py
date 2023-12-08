import pandas as pd
from scipy import interpolate


def average_width(df_left: pd.DataFrame, df_right: pd.DataFrame) -> pd.DataFrame:
    """Combine two DataFrames with width(level) to one average width(level) relation

    DataFrames should have columns 'level' and 'width'. Resulting DataFrame will contain all unique levels
    and the average width of both dataframes. Widths will be linear interpolated if only in one of the two
    DataFrames.

    Parameters
    ----------
    df_left : pd.DataFrame
        One DataFrame with width(level) relation
    df_right : pd.DataFrame
        Other DataFrame with width(level) relation

    Returns
    -------
    pd.DataFrame
        resulting DataFrame with width(level) relation
    """
    # get unique levels
    level = list(set(df_left["level"].to_list() + df_right["level"].to_list()))

    f_left = interpolate.interp1d(
        df_left["level"].to_numpy(), df_left["width"].to_numpy(), bounds_error=False
    )
    f_right = interpolate.interp1d(
        df_right["level"].to_numpy(), df_right["width"].to_numpy(), bounds_error=False
    )

    width = (f_left(level) + f_right(level)) / 2

    df = pd.DataFrame({"level": level, "width": width})

    # where width is NaN, its out of bounds of bounds of left or right. We'll replace it with left or right
    df_single = (
        pd.concat([df_left, df_right])
        .sort_values("level")
        .drop_duplicates("level")
        .reset_index(drop=True)
    )
    df[df.width.isna()] = df_single[df.width.isna()]

    return df


def cumulative_area(df: pd.DataFrame) -> pd.Series:
    """Calculate the cumulative_area from a DataFrame with width(level).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with level and width columns

    Returns
    -------
    pd.Series
        Series with cumulative area
    """

    df.reset_index(drop=True, inplace=True)
    df.sort_values("level", inplace=True)

    dx = df["level"] - df["level"].shift(fill_value=0)
    dy = df["width"] - df["width"].shift(fill_value=0)
    area = (df["width"].shift(fill_value=0) * dx) + (dy * dx) / 2
    area.loc[0] = 0
    return area.cumsum()


def manning_profile(df: pd.DataFrame) -> tuple[float, float]:
    """Convert a DataFrame with a Width(level) relation to a manning profile_width and slope.

    DataFrame should have columns 'level' and 'width'

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with level and width columns

    Returns
    -------
    Tuple[int, int]
        Tuple with (profile_width, slope) values
    """

    dz = df["level"].max() - df["level"].min()
    tw = df["width"].max()

    area = cumulative_area(df)
    A = area.max()
    bw = max(2 * A / (dz) - tw, 0)
    dy = (tw - bw) / 2
    S = dy / dz

    return bw, S

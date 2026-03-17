"""Implement profiles in model generation."""

import pandas as pd

from ribasim_nl import CloudStorage


def get_tables(
    water_authority: str, cloud: CloudStorage = CloudStorage(), overwrite: bool = False
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Get profile tables

    :param water_authority: water authority
    :param cloud: the GoodCloud-storage, defaults to CloudStorage()
    :param overwrite: overwrite GoodCloud-data, defaults to False

    :type water_authority: str
    :type cloud: CloudStorage, optional
    :type overwrite: bool, optional

    :return: profile tables (flowing/'doorgaand' and storing/'bergend')
    :rtype: tuple[pandas.DataFrame, pandas.DataFrame]

    :raises FileNotFoundError: if profile *.csv-file(s) cannot be found
    """
    # sync GoodCloud
    cloud.download_verwerkt(water_authority, overwrite=overwrite)

    # verify existence of tables
    wd = cloud.joinpath(water_authority, "verwerkt", "profielen")
    fn_flowing = wd / "profielen_doorgaand.csv"
    fn_storing = wd / "profielen_bergend.csv"
    if not fn_flowing.exists() or not fn_storing.exists():
        msg = "Profiles not (yet) preprocessed"
        raise FileNotFoundError(msg)

    # read profile data
    df_flowing = pd.read_csv(wd / fn_flowing)
    df_storing = pd.read_csv(wd / fn_storing)

    # return profile tables
    return df_flowing, df_storing

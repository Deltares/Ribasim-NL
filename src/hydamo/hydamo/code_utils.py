"""Utilities for generating unique codes."""
from pathlib import Path

import pandas as pd

BGT_CSV = Path(__file__).parent.joinpath("data", "bgt_codes.csv")
WBH_CSV = Path(__file__).parent.joinpath("data", "wbh_codes.csv")
BGT_DF = None
WBH_DF = None
BGT_ORGANIZATIONS = ["gemeente", "waterschap", "landelijke_organisatie"]

WBH_CODE_TEMPLATE = "NL.WBHCODE.{wbh_code}.{layer}.{code}"
BGT_CODE_TEMPLATE = "NL.BGTCODE.{bgt_code}.{layer}.{code}"
NEN3610_ID_TEMPLATE = "NL.WBHCODE.{wbh_code}.{layer}.{objectid}"


def get_bgt_df():
    """Get (and set) the global bgt_df.

    Returns
    -------
    BGT_DF : DataFrame

    """
    global BGT_DF

    # read BGT_CSV if not yet in memory
    if BGT_DF is None:
        BGT_DF = pd.read_csv(BGT_CSV)
        BGT_DF.set_index(BGT_DF.naam.str.lower(), inplace=True)
    return BGT_DF


def get_wbh_df():
    """Get (and set) the global wbh_df.

    Returns
    -------
    WBH_DF : DataFrame

    """
    global WBH_DF

    # read BGT_CSV if not yet in memory
    if WBH_DF is None:
        WBH_DF = pd.read_csv(WBH_CSV)
        WBH_DF.set_index(WBH_DF.naam.str.lower(), inplace=True)

    return WBH_DF


def bgt_to_wbh_code(bgt_code: str) -> int:
    """Return a wbh_code from a given bgt_code.

    Parameters
    ----------
    bgt_code : str
        DESCRIPTION.

    Returns
    -------
    str
        DESCRIPTION.

    """
    bgt_df = get_bgt_df()
    organization = bgt_df.set_index("bgt_code").loc[bgt_code].naam
    return find_wbh_code(organization)


def find_bgt_code(organisatie: str, type_organisatie: str = None) -> dict:
    """Find a dictionary with bgt-code(s) based on a (part of) an organisation name.

    Parameters
    ----------
    organisatie : str
        (Part of) an (case insensitive) organization name. E.g. "Groningen", or
        "ministerie"
    type_organisatie : str, optional
        Optional filter for types of organization. Either, "gemeente", "waterschap" or
        "landelijke_organisatie". The default is None.

    Returns
    -------
    dict
        dictionary with bgt-code of organization in the form {bgt_code: naam}.

    """
    bgt_df = get_bgt_df()

    # filter on type_organisatie
    if type_organisatie:
        if type_organisatie.lower() in BGT_ORGANIZATIONS:
            bgt_df = bgt_df.loc[bgt_df.type_organisatie == type_organisatie.lower()]

    # try to get an exact match
    if organisatie.lower() in bgt_df.index:
        df = bgt_df.loc[organisatie.lower()]

    # get a dictionary with non-exact matches. Empty if no match is found
    else:
        df = bgt_df.loc[bgt_df.naam.apply(lambda x: organisatie.lower() in x.lower())]

    # return as dictionary
    return df.set_index("bgt_code").naam.to_dict()


def find_wbh_code(organisatie: str = None) -> dict:
    """Find a dictionary with wbh-code(s) based on a (part of) an organisation name.

    Parameters
    ----------
    organisatie : str
        (Part of) an (case insensitive) organization name. E.g. "Groningen", or
        "ministerie"

    Returns
    -------
    dict
        dictionary with wbh-code of organization in the form {wbh_code: naam}.

    """
    wbh_df = get_wbh_df()

    # try to get an exact match
    if organisatie.lower() in wbh_df.index:
        result = wbh_df.loc[organisatie.lower()]

    # get a dictionary with non-exact matches. Empty if no match is found
    else:
        result = wbh_df.loc[
            wbh_df.naam.apply(lambda x: organisatie.lower() in x.lower())
        ]

    # get either the value or a dict
    if isinstance(result, pd.Series):
        result = int(result.wbh_code)
    else:
        result = result.set_index("wbh_code").naam.to_dict()

    return result


def generate_model_id(code, wbh_code, layer, bgt_code=None, x=None, y=None):
    # generate a code
    if code is None:
        code = f"loc={int(x)},{int(y)}"

    if wbh_code:
        return WBH_CODE_TEMPLATE.format(wbh_code=wbh_code, layer=layer, code=code)
    elif bgt_code:
        return BGT_CODE_TEMPLATE.format(bgt_code=bgt_code, layer=layer, code=code)
    else:
        raise TypeError(
            """
                        Failed to generate model_id. Provide wbh_code or bgt_code'
                        """
        )

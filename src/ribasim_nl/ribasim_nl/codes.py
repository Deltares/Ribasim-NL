"""Utilities for generating unique codes."""
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from shapely.geometry import Point

CODES_CSV = Path(__file__).parent.joinpath("data", "codes.csv")
CODES_DF = None

WBH_CODE_TEMPLATE = "NL.WBHCODE.{wbh_code}.{layer}.{code}"
BGT_CODE_TEMPLATE = "NL.BGTCODE.{bgt_code}.{layer}.{code}"
NEN3610_ID_TEMPLATE = "NL.BGTCODE.{bgt_code}.{layer}.{nen3610_id}"


def get_codes_df() -> DataFrame:
    """Get (and set) the global CODES_DF.

    Returns
    -------
    CODES_DF : DataFrame

    """
    global CODES_DF

    # read BGT_CSV if CODES_DF is None
    if CODES_DF is None:
        CODES_DF = pd.read_csv(CODES_CSV)
        CODES_DF.set_index(CODES_DF.name.str.lower(), inplace=True)
        CODES_DF["wbh_code"] = CODES_DF.wbh_code.apply(
            lambda x: f"{int(x):02}" if not pd.isna(x) else None
        )
    return CODES_DF.copy()


def bgt_code_exists(bgt_code: str) -> bool:
    """Check if bgt_code exists in CODES_DF"""
    codes_df = get_codes_df()
    return bgt_code in codes_df.bgt_code.to_numpy()


def wbh_code_exists(wbh_code) -> bool:
    """Check if wbh_code exists in CODES_DF"""
    codes_df = get_codes_df()
    return wbh_code in codes_df.wbh_code.to_numpy()


def bgt_to_wbh_code(bgt_code) -> str | None:
    """Convert bgt_code to wbh_code if bgt_code exists"""
    wbh_code = None
    if bgt_code_exists(bgt_code):
        codes_df = get_codes_df()
        wbh_code = (
            codes_df.reset_index(drop=True).set_index("bgt_code").loc[bgt_code].wbh_code
        )

    return wbh_code


def find_codes(
    organization: str,
    administration_category: str | None = None,
    to_dict: bool = True,
) -> dict[str, list[dict[str, str]]] | DataFrame:
    codes = {}
    """Find codes associated with an organization"""
    codes_df = get_codes_df()

    # filter on administration_category
    if administration_category:
        if (
            administration_category.lower()
            in codes_df.administration_category.to_numpy()
        ):
            codes_df = codes_df.loc[
                codes_df.administration_category == administration_category.lower()
            ]
        else:
            raise (
                ValueError(
                    f"invalid value for`administration_category`. {administration_category}` not in {codes_df.administration_category.unique()}"
                )
            )

    # if 1 exact match we return it
    if organization.lower() in codes_df.index:
        df = codes_df.loc[organization.lower()]
    # if more matches we return all matches
    else:
        df = codes_df.loc[
            codes_df.name.apply(lambda x: organization.lower() in x.lower())
        ]
    if to_dict:
        if isinstance(df, DataFrame):
            codes = df.to_dict(orient="records")
        else:
            codes = df.to_dict()

    return codes


def code_from_geometry(geometry: Point) -> str:
    """Generate a code from a geometry x/y location

    Parameters
    ----------
    geometry : Point
        Input shapely.geometry.Point

    Returns
    -------
    str
        output code-string
    """
    # make sure we have 1 point even if we haven't
    point = geometry.centroid

    # return code based on x/y location
    return f"loc={int(point.x+ 0.5)},{int(point.y + 0.5)}"


def generate_model_id(code, layer, wbh_code=None, bgt_code=None, geometry=None) -> str:
    """Generate a model_id from wbh_code or bgt_code and code or x/y coordinate"""
    if code is None:
        if geometry is not None:
            code = code_from_geometry(geometry)
        else:
            raise ValueError(
                f"""
                             Specify 'code' ({code}), you have to specify 'geometry' ({geometry}).
                """
            )

    if layer is None:
        raise ValueError(f" Specify 'layer' ({layer}) to generate a model_id")

    result = None
    if wbh_code:
        if wbh_code_exists(wbh_code):
            result = WBH_CODE_TEMPLATE.format(wbh_code=wbh_code, layer=layer, code=code)
    elif bgt_code:
        if bgt_code_exists(bgt_code):
            wbh_code = bgt_to_wbh_code(bgt_code)
            if wbh_code:
                result = WBH_CODE_TEMPLATE.format(
                    wbh_code=wbh_code, layer=layer, code=code
                )
            else:
                result = BGT_CODE_TEMPLATE.format(
                    bgt_code=bgt_code, layer=layer, code=code
                )
    if result is None:
        raise ValueError(
            f"""
                         Specify a valid 'wbh_code' ({wbh_code}) or 'bgt_code' ({bgt_code}) to generate model_id.
                         """
        )
    else:
        return result

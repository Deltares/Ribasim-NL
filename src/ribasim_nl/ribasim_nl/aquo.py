import pandas as pd

# https://www.aquo.nl/index.php/Imwa_sim_1.0/doc/codelijst/waterbeheerder
waterbeheercode = {
    "HollandseDelta": 40,
    "Zuiderzeeland": 37,
    "HollandsNoorderkwartier": 12,
    "Rivierenland": 9,
    "Delfland": 15,
    "AaenMaas": 38,
    "WetterskipFryslan": 2,
    "Noorderzijlvest": 34,
    "BrabantseDelta": 25,
    "HunzeenAas": 33,
    "Scheldestromen": 42,
    "Vechtstromen": 44,
    "RijnenIJssel": 7,
    "ValleienVeluwe": 43,
    "SchielandendeKrimpenerwaard": 39,
    "StichtseRijnlanden": 14,
    "DeDommel": 27,
    "Limburg": 60,
    "DrentsOverijsselseDelta": 59,
    "Rijnland": 13,
    "AmstelGooienVecht": 11,
    "Rijkswaterstaat": 80,
}


def get_id(*, name: str) -> int:
    """Return the Aquo code for an authority name."""
    if name not in waterbeheercode:
        raise ValueError(f"Unknown water authority name: {name!r}")
    return waterbeheercode[name]


def get_name(*, id: int) -> str:
    """Return the authority name for an integer Aquo code."""
    if isinstance(id, int) and not isinstance(id, bool):
        for name, code in waterbeheercode.items():
            if code == id:
                return name
    raise ValueError(f"Unknown water authority code: {id!r}")


def get_ids(names: pd.Series | pd.DataFrame) -> pd.Series | pd.DataFrame:
    """Map names to nullable integer codes, preserving axes and missing values."""
    ids = (
        names.map(waterbeheercode)
        if isinstance(names, pd.Series)
        else names.apply(lambda column: column.map(waterbeheercode))
    )
    if (names.notna() & ids.isna()).to_numpy().any():
        raise ValueError("Unknown water authority name in input")
    return ids.astype("Int64")


def get_names(ids: pd.Series | pd.DataFrame) -> pd.Series | pd.DataFrame:
    """Map codes to names, preserving axes and missing values."""
    lookup = {code: name for name, code in waterbeheercode.items()}
    names = ids.map(lookup) if isinstance(ids, pd.Series) else ids.apply(lambda column: column.map(lookup))
    if (ids.notna() & names.isna()).to_numpy().any():
        raise ValueError("Unknown water authority code in input")
    return names.astype("string")

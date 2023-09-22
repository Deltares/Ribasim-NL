from pathlib import Path
import pandas as pd

BGT_CSV = Path(__file__).parent.joinpath("data", "bgt_codes.csv")
BGT_DF = None
BGT_ORGANIZATIONS = ["gemeente", "waterschap", "landelijke_organisatie"]

def find_bgt_code(organisatie:str, type_organisatie:str=None) -> dict:
    """
    Find a dictionary with bgt-code(s) based on a (part of) an organisation
    name.

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
    global BGT_DF
    
    # read BGT_CSV if not yet in memory
    if BGT_DF is None:
        BGT_DF = pd.read_csv(BGT_CSV)
        BGT_DF.set_index(BGT_DF.naam.str.lower(), inplace=True)

    bgt_df = BGT_DF.copy()

    # filter on type_organisatie
    if type_organisatie:
        if type_organisatie.lower() in BGT_ORGANIZATIONS:
            bgt_df = bgt_df.loc[bgt_df.type_organisatie == type_organisatie.lower()]
      
    # try to get an exact match
    if organisatie.lower() in bgt_df.index:
        df = bgt_df.loc[organisatie.lower()]
        
    # get a dictionary with non-exact matches. Empty if no match is found
    else:
        df = bgt_df.loc[
            bgt_df.naam.apply(lambda x: organisatie.lower() in x.lower())
            ]

    # return as dictionary
    return df.set_index("bgt_code").naam.to_dict()
    
    
import numpy as np
import numpy.typing as npt

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

# The name of the authority of each waterbeheercode
authority_by_code = {code: name for name, code in waterbeheercode.items()}
# The node IDs of the coupled model are the waterbeheercode of their authority times this, plus their own node ID
NODE_ID_FACTOR = 100_000


def authority_code(node_ids: npt.ArrayLike) -> np.ndarray:
    """The waterbeheercode of the authority of nodes of the coupled model, also of negated node IDs."""
    return np.abs(np.asarray(node_ids)) // NODE_ID_FACTOR

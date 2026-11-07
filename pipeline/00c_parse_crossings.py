# %%
"""Find the crossings of the hydro-objects of a level controlled water authority with the boundaries of its peilgebieden.

Writes `verwerkt/crossings.gpkg` from the postprocessed data in `verwerkt/postprocessed.gpkg`, see
`ribasim_nl.crossings.parse`. Besides the crossings, it holds the layers of the postprocessed data.

Usage: pixi run python pipeline/00c_parse_crossings.py <authority>
"""

import sys

from ribasim_nl.crossings.parse import ParseCrossings

from ribasim_nl import CloudStorage

cloud = CloudStorage()

# the search radius for structures near a crossing, as in 2024, otherwise 60 m
SEARCH_RADIUS_STRUCTURE = {"HollandseDelta": 300.0, "SchielandendeKrimpenerwaard": 300.0}

# crossings that were not found in 2024, as the peilgebied they cross from and to, and their location
EXCLUDED_CROSSINGS = {
    # this hydro-object is almost parallel to the boundary of the peilgebieden where it crosses it, so the direction
    # of the crossing depends on rounding: GEOS 3.12 found it from 95 to 94, GEOS 3.14 also from 94 to 95
    "HollandseDelta": [("dummy_globalid_peilgebied_94", "dummy_globalid_peilgebied_95", 67333.545, 423481.513)],
}

authorities = sys.argv[1:]
assert authorities, "pass the authorities as arguments"

for authority in authorities:
    cloud.validate_authority(authority)
    print(authority)
    verwerkt = cloud.joinpath(authority, "verwerkt")
    cross = ParseCrossings(
        verwerkt / "postprocessed.gpkg",
        search_radius_structure=SEARCH_RADIUS_STRUCTURE.get(authority, 60.0),
        agg_peilgebieden_layer="aggregation_area",
        agg_peilgebieden_column="code",
        excluded_crossings=EXCLUDED_CROSSINGS.get(authority, []),
    )
    df_hydro, df_culverts, df_hydro_culverts = cross.find_crossings_with_peilgebieden("hydroobject", "duikersifonhevel")
    cross.write_crossings(verwerkt / "crossings.gpkg", df_hydro, "duikersifonhevel", df_culverts, df_hydro_culverts)

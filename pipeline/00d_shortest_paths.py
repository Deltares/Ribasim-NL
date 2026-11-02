# %%
"""The shortest paths along the hydro-objects through the boezems of a level controlled water authority.

Writes `verwerkt/shortest_path.gpkg` from `verwerkt/crossings.gpkg`, see `ribasim_nl.crossings.shortest_path`.

Usage: pixi run python pipeline/00d_shortest_paths.py <authority>
"""

import sys

from ribasim_nl.crossings.shortest_path import shortest_paths

from ribasim_nl import CloudStorage

cloud = CloudStorage()

# the length of the segments the hydro-objects are cut into, as in 2024, otherwise 50 m
DISTANCE_INTERVAL = {"AmstelGooienVecht": 5.0}

authorities = sys.argv[1:]
assert authorities, "pass the authorities as arguments"

for authority in authorities:
    cloud.validate_authority(authority)
    print(authority)
    verwerkt = cloud.joinpath(authority, "verwerkt")
    paths = shortest_paths(verwerkt / "crossings.gpkg", distance_interval=DISTANCE_INTERVAL.get(authority, 50.0))
    path = verwerkt / "shortest_path.gpkg"
    path.unlink(missing_ok=True)
    paths.to_file(path, layer="shortest_path")

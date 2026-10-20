# %%
import sys

import geopandas as gpd
from ribasim_nl.berging import VdGaastBerging, supply_area_basin_ids
from ribasim_nl.capacity import bound_supply_inflows

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

FIND_POST_FIXES = ["full_control_model"]
# pass authorities as arguments, or edit list here
SELECTION: set = {"RijnenIJssel"}
REBUILD = True
RUN_MODEL: bool = False

# authorities provided as arguments, else in SELECTION, else all
authorities = cloud.select_authorities(sys.argv[1:], fallback=SELECTION)

# %%
for authority in authorities:
    model_dir = cloud.find_model_dir(authority, FIND_POST_FIXES)
    if model_dir is None:
        raise FileNotFoundError(f"No model of {authority} found with post fixes {FIND_POST_FIXES}")
    print(authority)
    toml_file = next(model_dir.glob("*.toml"))
    model = Model.read(toml_file)

    # write model with berging
    dst_toml_file = cloud.model_dir(authority, "bergend_model") / toml_file.name

    if (not dst_toml_file.exists()) or REBUILD:
        # limit supply to the target level, before adding storing basins
        bound_supply_inflows(model)

        # storing basins in supplied areas (meta_aanvoer) get a ManningResistance, so they can be supplied too
        supply_areas = gpd.read_file(cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg"))
        manning_basin_ids = supply_area_basin_ids(model, supply_areas)
        print(f"{len(manning_basin_ids)} storing basins in supplied areas are connected with a ManningResistance")

        # add berging
        add_berging = VdGaastBerging(model=model, cloud=cloud, manning_basin_ids=manning_basin_ids)
        add_berging.add()

        # run model
        model.write(dst_toml_file)
        if RUN_MODEL:
            result = model.run()
            assert result.exit_code == 0

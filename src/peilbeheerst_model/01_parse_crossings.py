import json
import pathlib

import pandas as pd

from peilbeheerst_model import ParseCrossings, waterschap_data

with open("waterschappen.json") as f:
    waterschap_data = json.load(f)

print_df = {}
for waterschap, waterschap_struct in waterschap_data.items():
    for funcname, func_args in waterschap_struct.items():
        if waterschap != "Wetterskip":
            continue
        if funcname not in print_df:
            print_df[funcname] = []
        print_df[funcname].append(pd.Series(func_args, name=waterschap))

for waterschap, waterschap_struct in waterschap_data.items():
    print(f"\n{waterschap}...")

    init_settings, crossing_settings = waterschap_struct.values()
    init_settings["logfile"] = pathlib.Path(init_settings["output_path"]).with_suffix("").with_suffix(".log")

    if waterschap not in ["Wetterskip"]:
        continue

    # Crossings class initializeren
    cross = ParseCrossings(**init_settings)

    # Crossings bepalen en wegschrijven
    if crossing_settings["filterlayer"] is None:
        df_hydro = cross.find_crossings_with_peilgebieden("hydroobject", **crossing_settings)
        cross.write_crossings(df_hydro)
    else:
        df_hydro, df_dsf, df_hydro_dsf = cross.find_crossings_with_peilgebieden("hydroobject", **crossing_settings)
        cross.write_crossings(df_hydro, crossing_settings["filterlayer"], df_dsf, df_hydro_dsf)

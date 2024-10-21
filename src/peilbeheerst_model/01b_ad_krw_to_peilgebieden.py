import pathlib

import geopandas as gpd
import pandas as pd
from IPython.core.display import HTML

from peilbeheerst_model import ParseCrossings, waterschap_data

print_df = {}
for waterschap, waterschap_struct in waterschap_data.items():
    for funcname, func_args in waterschap_struct.items():
        if funcname not in print_df:
            print_df[funcname] = []
        print_df[funcname].append(pd.Series(func_args, name=waterschap))

for funcname, df in print_df.items():
    print(HTML(f"<h2>Function {funcname}:</h2>"))
    print(pd.DataFrame(df))


for waterschap, waterschap_struct in waterschap_data.items():
    print(f"\n{waterschap}...")

    crossing_settings = waterschap_struct["find_crossings_with_peilgebieden"]
    init_settings = waterschap_struct["init"]

    gpkg = pathlib.Path(init_settings["output_path"])
    if not gpkg.exists():
        raise ValueError(gpkg)

    df_peilgebieden = gpd.read_file(gpkg, layer="peilgebied")
    org_shape = df_peilgebieden.shape
    df_peilgebieden = ParseCrossings._make_valid_2dgeom(df_peilgebieden)

    df_peilgebieden = ParseCrossings.add_krw_to_peilgebieden(
        df_peilgebieden,
        init_settings["krw_path"],
        init_settings["krw_column_id"],
        init_settings["krw_column_name"],
        init_settings["krw_min_overlap"],
        ",",
    )

    assert df_peilgebieden.shape[0] == org_shape[0]
    df_peilgebieden.to_file(gpkg, layer="peilgebied")
    print(gpkg)

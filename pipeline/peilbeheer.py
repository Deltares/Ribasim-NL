"""Read the level management (peilbeheer) of the peilgebieden of the water boards into one GeoPackage.

See `ribasim_nl.peilbeheer` for the datasets and rules. `09_koppelen_modellen.py` applies it to the coupled model
with `ribasim_nl.seasonal_levels.apply_seasonal_levels`, see https://github.com/Deltares/Ribasim-NL/issues/893.
"""

import geopandas as gpd
import pandas as pd
from ribasim_nl.peilbeheer import CRS, SOURCES, read_peilbeheer
from ribasim_nl.settings import settings

data_dir = settings.ribasim_nl_data_dir
output = data_dir / "Rijkswaterstaat/verwerkt/peilbeheer.gpkg"

parts = []
for authority in SOURCES:
    df = read_peilbeheer(authority, data_dir)
    seasonal = (df["streefpeil_zomer"] != df["streefpeil_winter"]).sum()
    print(f"{authority}: {len(df)} peilgebieden, {seasonal} with a summer and winter level")
    parts.append(df)

peilbeheer = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), geometry="geometry", crs=CRS)
output.unlink(missing_ok=True)
peilbeheer.to_file(output, layer="peilgebied")

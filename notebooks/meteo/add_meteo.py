# %%
import geopandas as gpd
import xarray as xr
from meteo_functions import (
    AddMeteoModel,
    GetFractionalGridPerBasin,
    GetMeteoPerBasin,
    SyncModelFromCloud,
    SyncWiwbMeteoFromCloud,
)

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

############# SET THE DESIRED MODEL AND TIME PERIOD ###################
authority = "Rijkswaterstaat"  # Water authority folder that is used on the Cloud Storage
model = "lhm_coupled_2025_5_0"  # Model that is selected on the Cloud Storage
shortname = "lhm"  # Name of the .toml file in the specified model
startdate = "2017-01-01"  # Startdate of the modelrun
enddate = "2017-12-31"  # Enddate of the modelrun
new_model_name = "lhm_coupled_2025_5_0_2017"  # New model name
######################################################################


# Sync the meteo data and the required model from the Cloud Storage
SyncWiwbMeteoFromCloud(cloud)
SyncModelFromCloud(cloud, authority, model)

# Load the precipitation, evaporation and basins
precip = xr.open_dataset(cloud.joinpath("Basisgegevens", "WIWB", "Meteobase.Precipitation.nc"))
evp = xr.open_dataset(cloud.joinpath("Basisgegevens", "WIWB", "Meteobase.Evaporation.Makkink.nc"))

# Load the model and the basins
model_dir = cloud.joinpath(authority, "modellen", model)
ribasim_gpkg = model_dir / "database.gpkg"
ribasim_toml = model_dir / f"{shortname}.toml"

model = Model.read(ribasim_toml)
basins = gpd.read_file(ribasim_gpkg, layer="Basin / area")

# Extract arrays of x and y coordinates from the meteo grids
xll_coords = precip["x"].values
yll_coords = precip["y"].values

# Get a dictionary with fractional coverage for each basin
fraction_map = GetFractionalGridPerBasin(xll_coords, yll_coords, basins)

# Get the meteo input per basin as pd.DataFrame
meteo_time_df = GetMeteoPerBasin(startdate, enddate, precip, evp, fraction_map)

# Add the meteo information to the selected model
new_model = AddMeteoModel(meteo_time_df, model, startdate, enddate)

# Write the model to the designated folder
new_model_toml = cloud.joinpath(authority, "modellen", new_model_name, f"{shortname}.toml")
new_model.write(new_model_toml)
print(f"New model written to: {new_model_toml}")
# %%

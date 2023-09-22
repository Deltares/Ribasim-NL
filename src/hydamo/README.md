# HyDAMO
Module to work with the Dutch Hydrological Data Model (HyDAMO). The original class is taken from [D-Hydamo](https://github.com/Deltares/HYDROLIB/tree/main/hydrolib/dhydamo) developed by [HKV](https://www.hkv.nl/) and further developed in the [ValidatieModule](https://github.com/HetWaterschapshuis/HyDAMOValidatieModule) developed by [D2Hydro](https://d2hydro.nl/). Here we will further improve it during the construction of the Landelijk Hydrologisch Model in Ribasim.

## Install a copy
- Get a local clone of this repository.
- Build an environment as in [environment](./environment.yml).
- Mak sure your environment is activated
- Go to the root of your repository (where you find setup.py) and install the package in edit-mode by:

```
pip install -e .
```

## Get Started
To start working, import the hydamo datamodel:
```
from hydamo.datamodel import HyDAMO

hydamo = HyDAMO()
```

The hydamo-object will have have GeoPandas.GeoDataFrame for every layer defined by HyDAMO, e.g. HydroObject. You can access this layer by calling the layer as hydamo-property:

```
hydamo.hydroobject
```

You can set data from an existing feature-file, e.g. an ESRI shapefile by calling the method `set_data`:

```
hydroobject_gdf = gpd.read_file(`hydroobject.shp`)

# map all your columns  (and dtypes!) in `hydroobject_gdf` to HyDAMO

hydamo.hydroobject.set_data(hydroobject_gdf)
```

Finally you can write the entire model into one GeoPackage by calling the `to_geopackage` method on the hydamo-class:

```
hydamo.to_geopackage("hydamo.gpkg")
```
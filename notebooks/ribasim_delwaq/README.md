# Ribasim-Delwaq Integration Guide

The main script used in this workflow is `delwaq_tests/combined_testing.py`. This script runs as a jupyter notebook. It requires the pixi environment of Ribasim-NL to be active. This script does the following:

1. Couple emission data to Ribasim model
2. Set up DELWAQ simulation automatically using generate.py
3. Run DELWAQ simulation
4. Parse and save simulation results

### Ribasim model (hydrology)

This script requires a Ribasim model with results, located under ``model_path``. This model may be obtained by downloading it from TheGoodCloud or by running `notebooks/rwzi/add_rwzi_model.py`. **check if these are currently still the main methods**

Results may already be included in a downloaded Ribasim model, otherwise, in a CLI:
```
<path_to_ribasim.exe> <path_to_lhm.toml>
```

The simulation period can be adjusted in the model's `.toml` file

For each step a detailed description is provided below.

## Couple emission data to Ribasim model

The following four emission data sources are coupled to the LHM:

- Z-info: Measured concentration in WWTP effluent (assigned to water flux)
- IM: Measured concentration in transboundary inflows into the water system (assigned to water flux)
- ANIMO: Diffuse emission estimates from agriculture (dry waste load)
- EmissieRegistratie (ER): Diffuse emission estimates of other origins (dry waste load)

For each data source, a separate folder containing raw data and a coupling script was created. Each script creates a dataframe that follows the ribasim data structure (node_id, datetime, substance, value). This dataframe is saved to a parquet file in the folder specific to each data source. Each parquet file containing the emissions from each data source is loaded into combined_testing.py and coupled to the Ribasim model.

**For each data source, a detailed description of the raw data, the conversion scripts, and the decisions/assumptions is provided in a separate tab**
**Each script should contain a config section with variables that can also be defined through combined_testing.py**
**An unresolved issue is how we combine loads and concentrations from two sources with (possibly) different temporal resolutions to the same ribasim nodes**

## Set up DELWAQ simulation automatically using generate.py

The ``generate`` function takes a ribasim model, along with emissions that were coupled in the previous step, to create all input files that are necessary to run a delwaq simulation. ``generate`` also creates two variables, `graph` (ribasim and associated delwaq network) and `substances` (list of substances that are simulated), which are necessary to run the ``parse`` function later. Right now, ``generate`` can take around 30 minutes to run.

## Run DELWAQ simulation

The delwaq simulation is run from python using the ``subprocess`` package, which takes a delwaq dimrset (which points to the right executable) and the input files that were created with ``generate.py``

### DIMR_PATH Environment Variable

To run delwaq from the notebook, ensure that the environment variable `DIMR_PATH` is set to the path of the DIMR executable (in .env).

Example:
```
DIMR_PATH=c:\Program Files\Deltares\Delft3D FM Suite 2025.02 HMWQ\plugins\DeltaShell.Dimr\kernels\x64\bin\run_dimr.bat
```

### Running the simulation

The delwaq simulation is run for the period spanned by the LHM results, but can be adjusted. Keep in mind that any adjustments are overturned when running ``generate`` with the same output_path

## Parse results and save simulation results

The ``parse`` function takes the delwaq output, the concentration of each substance over time per delwaq segment, in netCDF format and assigns it to ribasim basin nodes. By specifying `to_input=True`, the results are also written per basin node.

Using ``plot_fraction``, the concentration for a specific basin node is plotted.


# Documentation of each emission source

## ER

### Data Sources

File locations in Python scripts need to be adjusted, corresponding to cloud storage or DVC

The conversion script `ER_data_conversion_delwaq.py` is an adaptation of:
`p:/krw-verkenner/01_landsdekkende_schematisatie/LKM25 schematisatie/OverigeEmissies/KRW_Tussenevaluatie_2024/Convert_ER_Emissions_To_KRW_input_tusseneval.py`

- Modified to use spatial coupling with new LHM schematisation, called via function from Python script `ER_GAF_fractions_func.py`
- GAF polygons sourced from `P:/11210327-lwkm2/01_data/Emissieregistratie/gaf_90.shp`

### settings

- frac_bergend: fraction of emissions in a basin that is assigned to its **bergende node**, where the remainder (1 - frac_bergend) is assigned to the **doorgaande node**.

- make_plots: tells the script whether to create plots (currently not saved)

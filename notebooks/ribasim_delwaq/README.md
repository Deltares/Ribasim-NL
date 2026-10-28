# Ribasim-Delwaq Integration Guide

The DELWAQ workflow is split into three notebook-style Python scripts. They use
the active Pixi environment of Ribasim-NL and can be run as Jupyter notebooks
in VS Code because they contain `# %%` cells.

Run the scripts in this order:

1. `01_prepare_and_verify_inputs.py`
2. `02_build_delwaq_model.py`
3. `03_run_and_analyse_delwaq.py`

The scripts communicate through prepared Parquet files in:

```
data/Rijkswaterstaat/modellen/lhm_coupled_full/delwaq_input/
```

### 1. Prepare and verify inputs

`01_prepare_and_verify_inputs.py`:

- Reads the ER, ANIMO, IM, and Zinfo input data.
- Optionally runs the source-data conversion scripts.
- Verifies ER and ANIMO mass loads against basin polygons.
- Processes IM and Zinfo independently:
  - removes invalid concentration and node values;
  - interpolates concentrations and flow rates to the daily time grid;
  - calculates daily boundary mass loads;
  - verifies each source against boundary nodes and links.
- Combines IM and Zinfo only after their separate processing and verification.
- Combines ANIMO and ER after their separate verification.
- Writes the prepared load and concentration data for the next script.

The boundary verification plots show boundary-node loads with the Ribasim
network links in the background. The basin verification plots use basin areas
to calculate loads in `kg/ha/year`.

### 2. Build the DELWAQ model

`02_build_delwaq_model.py` reads the prepared Parquet files, assigns:

- combined ANIMO/ER loads to `model.basin.mass_load`;
- combined IM/Zinfo concentrations to `model.flow_boundary.concentration`.

It then writes the updated Ribasim model TOML file.

### 3. Run and analyse DELWAQ

`03_run_and_analyse_delwaq.py`:

1. Creates DELWAQ input files with `generate`.
2. Runs DELWAQ through DIMR.
3. Parses the results with `parse`.

The output is written below the model's `delwaq_all_sources` directory.

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

For each data source, a separate folder containing raw data and a coupling script was created. Each conversion script creates a dataframe that follows the Ribasim data
structure (`node_id`, `time`, `substance`, and `load` or `concentration`).
The preparation script reads these Parquet files, verifies each source, and
writes the combined prepared inputs used by the model-building script.

**For each data source, a detailed description of the raw data, the conversion scripts, and the decisions/assumptions is provided in a separate tab**
**Each script should contain a config section with variables that can also be defined through combined_testing.py**
**An unresolved issue is how we combine loads and concentrations from two sources with (possibly) different temporal resolutions to the same ribasim nodes**

## Set up DELWAQ simulation automatically using `generate`

The `generate` function takes the written Ribasim model and creates all input
files necessary to run a DELWAQ simulation. It can take around 30 minutes.

## Run DELWAQ simulation

The DELWAQ simulation is run from Python using the `subprocess` package. The
run script uses the DIMR executable and the input files created by `generate`.

### D3D_HOME Environment Variable

To run DELWAQ, ensure that `D3D_HOME` points to the DIMR executable used by
the run script.

Example:
```
D3D_HOME=c:\Program Files\Deltares\Delft3D FM Suite 2025.02 HMWQ\plugins\DeltaShell.Dimr\kernels\x64\bin\run_dimr.bat
```

### Running the simulation

The DELWAQ simulation is run for the period configured in the generated
inputs. Any manual changes to generated files can be overwritten when
`generate` is run again with the same output path.

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

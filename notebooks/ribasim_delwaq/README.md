# Ribasim-Delwaq Integration Guide

The DELWAQ workflow is split into notebook-style Python scripts. They use
the active Pixi environment of Ribasim-NL and can be run as Jupyter notebooks
in VS Code because they contain `# %%` cells.

Run the scripts in this order:

1. `00_copy_lhm_run.py`
2. `ANIMO_to_delwaq/get_basin_polygons.py`
3. `01_prepare_and_verify_inputs.py`
4. `02_build_delwaq_model.py`
5. `03_run_and_analyse_delwaq.py`

`04_visualise_delwaq_results.py` is a template for post-processing the results
afterwards.

## DVC stage

The `delwaq` stage of the DVC pipeline in `dvc.yaml` runs these five scripts in
this order, on the latest run of the coupled model. It is not part of the full
reproductions of the pipeline, since it needs a Delft3D installation, see
[D3D_HOME](#d3d_home-environment-variable). Run it on demand, which also pulls
the run and the input data:

```
pixi run dvc repro --pull --single-item delwaq
```

Commit `dvc.lock` and push the outputs with `pixi run dvc push delwaq`
afterwards. The `delwaq` entry in `dvc.lock` belongs to the run of the coupled
model it was made on, so it lags behind when that run is reproduced;
`pixi run dvc status delwaq` shows whether it is up to date. Get the results of
that entry without running Delwaq with `pixi run dvc pull delwaq`.

The stage reads and writes:

| Path | Content | In DVC |
|:-----|:--------|:-------|
| `data/Rijkswaterstaat/runs/lhm_coupled` | The run of the coupled model | Output of `run_lhm_coupled` |
| `data/Basisgegevens/Delwaq/<source>/aangeleverd` | The delivered data per emission source, of which only the files that the scripts read are in DVC | Input, `.dvc` files |
| `data/Basisgegevens/Delwaq/ANIMO/interim/LHM_fluxen/LHM_433` | The drainage fluxes of LHM 4.3.3 per decade, made by `Process_LHM_hydrology.py` outside the pipeline | Input, `.dvc` file |
| `data/Basisgegevens/Delwaq/<source>/output` | The loads and concentrations per source | Output, not cached |
| `data/Basisgegevens/Delwaq/ANIMO/aangeleverd/shapes/lhm_coupled_delwaq` | The basin polygons of the model | Output, not cached |
| `data/Rijkswaterstaat/runs/lhm_coupled_delwaq` | The copy of the run, with the emissions, the DELWAQ input and the results | Output |
| `data/Basisgegevens/Delwaq/<source>/figures`, `data/Basisgegevens/Delwaq/ANIMO/interim` | Figures and other intermediate files | No |

To add or replace an input file, add it with `pixi run dvc add <file>`, list it
in the `deps` of the stage in `dvc.yaml`, and push it with `pixi run dvc push`.

The stage takes nearly two hours for a run of two years, and its output is 8 GB:

| Step | Time |
|:-----|:-----|
| `00_copy_lhm_run.py` | 3 seconds |
| `get_basin_polygons.py` | 8 to 13 minutes, nearly all for the two `dissolve` calls of its printed diagnostics |
| `01_prepare_and_verify_inputs.py` | 20 minutes: 4 for ER, 13 for ANIMO and 3 for the boundaries and the verification |
| `02_build_delwaq_model.py` | 4 minutes |
| `03_run_and_analyse_delwaq.py` | 75 minutes: 23 for `generate`, 5 for the input processing of DELWAQ, 41 for its simulation and 5 for `parse` |

## Configuration

`config.toml` has the run of the coupled model, `run_toml_path`, and the copy
of it that the workflow changes and adds its results to, `toml_path`.
`00_copy_lhm_run.py` makes that copy, replacing an earlier one. The
preparation, build, run, and source conversion scripts all read the model
directory and TOML filename from `toml_path`. The basin polygon script must be
rerun when changing the configured model so that its output is available to the
ANIMO conversion script.

`02_build_delwaq_model.py` adds the concentrations to those that are in the
model, so make a new copy before running it again.

### 4. Visualise existing DELWAQ results

`04_visualise_delwaq_results.py` only reads an existing DELWAQ run. By
default, it uses the configured model's `delwaq_all_sources` directory. Set
`OUTPUT_PATH_OVERRIDE` in the script to a copied model's result directory when
analysing results outside the repository.

The scripts communicate through prepared Parquet files in:

```
data/Rijkswaterstaat/runs/lhm_coupled_delwaq/delwaq_input/
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
2. Runs DELWAQ with `run_delwaq`.
3. Parses the results with `parse`.

These steps are combined in `ribasim_nl.delwaq.compute_delwaq`.

The output is written below the model's `delwaq_all_sources` directory.

### Ribasim model (hydrology)

The workflow requires a Ribasim model with results: the copy that `00_copy_lhm_run.py` makes of the run of the coupled model. Get that run with `pixi run dvc pull run_lhm_coupled`.

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

The DELWAQ simulation is run with `run_delwaq` from Ribasim, on the input
files created by `generate`. It calls `bin/run_delwaq.bat` on Windows and
`bin/run_delwaq.sh` on Linux.

### D3D_HOME Environment Variable

To run DELWAQ, set `D3D_HOME` in `.env` to the root of the Delft3D
installation, the directory that holds `bin`.

Example:
```
D3D_HOME=c:\Program Files\Deltares\Delft3D FM Suite 2025.02 HMWQ\plugins\DeltaShell.Dimr\kernels\x64
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

The input files are in DVC, under `data/Basisgegevens/Delwaq/Emissieregistratie/aangeleverd`.

The conversion script `ER_data_conversion_delwaq.py` is an adaptation of:
`p:/krw-verkenner/01_landsdekkende_schematisatie/LKM25 schematisatie/OverigeEmissies/KRW_Tussenevaluatie_2024/Convert_ER_Emissions_To_KRW_input_tusseneval.py`

- Modified to use spatial coupling with new LHM schematisation, called via function from Python script `ER_GAF_fractions_func.py`
- GAF polygons sourced from `P:/11210327-lwkm2/01_data/Emissieregistratie/gaf_90.shp`

### settings

- frac_bergend: fraction of emissions in a basin that is assigned to its **bergende node**, where the remainder (1 - frac_bergend) is assigned to the **doorgaande node**.

- make_plots: tells the script whether to create plots (currently not saved)

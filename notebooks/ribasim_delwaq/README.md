# Ribasim-Delwaq Integration Guide

The main script used in this workflow is `delwaq_tests/combined_testing.py`, which does the following:

- loads a Ribasim model
- sets up the delwaq simulation
- runs the delwaq simulation
- parses the results
- writes the results for visualization in QGIS

The final two steps are still a work in progress, providing some handles for a more robust analysis that will be performed in the near future. In its testing phase, this script has only been run as a jupyter notebook, using the interactive python extension in VSCode.

## DIMR_PATH Environment Variable

To execute the conversion from ER (Emission Registry) to Delwaq, ensure that the
environment variable `DIMR_PATH` is set to the path of the DIMR executable.
This is required to run the conversion scripts.

Example:
```
DIMR_PATH=c:\Program Files\Deltares\Delft3D FM Suite 2025.02 HMWQ\plugins\DeltaShell.Dimr\kernels\x64\bin\run_dimr.bat
```

## Data Sources

File locations in Python scripts need to be adjusted.

The conversion script `ER_data_conversion_delwaq.py` is an adaptation of:
`p:/krw-verkenner/01_landsdekkende_schematisatie/LKM25 schematisatie/OverigeEmissies/KRW_Tussenevaluatie_2024/Convert_ER_Emissions_To_KRW_input_tusseneval.py`

- Modified to use spatial coupling with new LHM schematisation, called via function from Python script `ER_GAF_fractions_func.py`
- GAF polygons sourced from `P:/11210327-lwkm2/01_data/Emissieregistratie/gaf_90.shp`
- Script otherwise unchanged, processes dataframe 'Diffuse_emissions_OE' to generate Delwaq input

## Step-by-Step ER Coupling Process

1. **Download Ribasim model**

   Directly via good cloud or by running `notebooks/rwzi/add_rwzi_model.py`

   Ensure that the environment variable `RIBASIM_NL_DATA_DIR` is used as the location

2. **Run Ribasim model** for the desired period (can be short for tests)

   Results may already be included in a downloaded Ribasim model, otherwise:

   In a CLI: <path_to_ribasin.exe> <path_to_lhm.toml>

   Simulation period can be adjusted in the model's `.toml` file

3. **Run** `delwaq_tests/combined_testing.py`

   Only the part until the comment: 'pause here and proceed with steps in README.md'

   Requires Ribasim-NL uv environment to be active

   This provides most input files for Delwaq via `generate.py`, which live in the output_folder specified in this `combined_testing.py`

   Generates separate `delwaq_bndlist.inc`

4. **Run** `ER_data_conversion_delwaq.py`

   This produces `B6_loads.inc`

5. **Run** `ANIMO2Delwaq.py`

   This produces `include_ANIMO.inc`

6. **Obtain concentration data for FlowBoundary nodes**

   These are obtained by running scripts from a separate commit, producing BOUNDWQ_rwzi.DAT and BOUNDWQ_ba.dat. So far these were manually moved into the delwaq folder. **could skip this step until commit is merged**

   (While saving them in the right location automatically would be an improvement, I suggest skipping this step for now and moving straight to including the dataframes in the ribasim model structure)

7. **Manually adjust** `delwaq.inp`:

   **Block 1:**
      - Add substances NO3, NH4, OON, PO4, AAP, OOP to the list of substances
      - Change number of substances accordingly (adding 6 to the number that is there for six substances)

   **Block 5:**
      - keep: INCLUDE 'B5_bounddata.inc'    (already present, existing substances/tracers)
      - add:  INCLUDE 'BOUNDWQ_rwzi.DAT'    (timeseries for new substances on FlowBoundary nodes)
      - add:  INCLUDE 'BOUNDWQ_ba.DAT'      (timeseries for new substances on FlowBoundary nodes)

   **Block 6:**
      - add:  INCLUDE 'loadswq.id'           (links ribasim basins to delwaq segments)
      - add:  INCLUDE 'B6_loads.inc'         (timeseries loads for new substances on Basin nodes)
      - add:  INCLUDE 'include_ANIMO.inc'    (load Agriculture and Nature, from conversion ANIMO model results)

   **Block 8:**
      - Change contents to (without indentation):
               INITIALS Continuity Drainage FlowBoundary Initial LevelBoundary Precipitation SurfaceRunoff UserDemand NO3 NH4 OON PO4 AAP OOP
               DEFAULTS 1.0 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0

      (instead of specifying the initials for every node separately)


8. **Run Delwaq** via `delwaq_tests/combined_testing.py` (or optionally via cmd)

      Within the python script, the output folder is specified again before running the model

      This enables the user to specify a folder where all manual adjustments to delwaq.inp have already been made

      Otherwise, the user would have to make the manual adjustments to delwaq.inp in the specified output_folder each time the script is run

      This step will not be necessary once generate.py does not require manual changes to delwaq.inp anymore

9. **Inspect results** via `delwaq_tests/combined_testing.py`

   By running the remainder of this script, some results can be seen. This section is still a work in progress and forms the starting point of a visualization and validation workflow that is to be setup.

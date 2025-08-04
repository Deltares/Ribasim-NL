# Ribasim-Delwaq Integration Guide

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

- Modified to use new coupling script, called via function from Python script `ER_GAF_fractions_func.py`
- GAF polygons sourced from `P:/11210327-lwkm2/01_data/Emissieregistratie/gaf_90.shp`
- Script otherwise unchanged, processes dataframe 'Diffuse_emissions_OE' to generate Delwaq input

## Step-by-Step ER Coupling Process

1. **Download Ribasim model** via `notebooks/rwzi/add_rwzi_model.py`

   Ensure that the environment variable `RIBASIM_NL_DATA_DIR` is used as the location

2. **Run Ribasim model** for the desired period (can be short for tests)

   Simulation period can be adjusted in the model's `.toml` file

3. **Run** `ER_setup_delwaq.py`

   This provides most input files for Delwaq via `generate.py`

   Generates separate `delwaq_bndlist.inc`

4. **Run** `ER_data_conversion_delwaq.py`

   This produces `B6_loads.inc`

5. **Manually adjust** `delwaq.inp`:

   **B1:** Add 'N' and 'P' as substances
          Total number of substances +2

   **B6:** Remove `0; number of loads`
          Add:
          ```
          INCLUDE delwaq_bndlist.inc
          INCLUDE B6_loads.inc
          ```

   **B8:** Remove everything
          Add:
          ```
          INITIALS {all substances without quotes, separated by spaces}
          DEFAULTS {IC values separated by spaces}
          ```
          Example:
          ```
          INITIALS Continuity Drainage FlowBoundary Initial LevelBoundary Precipitation Terminal UserDemand N P
          DEFAULTS 1.0 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 0.0
          ```

6. **Run Delwaq** via cmd or Python

7. **Run** `ER_run_parse_inspect.py` for postprocessing/validation:

   Check substances and optionally run:
   ```python
   substances.add 'N'
   substances.add 'P'
   ```

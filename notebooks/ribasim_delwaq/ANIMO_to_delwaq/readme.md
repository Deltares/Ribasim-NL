### ANIMO to delwaq data conversion scripts

#### usage:

scripts are run sequentially:

1 Process_LHM_hydrology.py --> creates the intermediate files (based on subsurface LHM) to massively reduce DVC usage (currently not run inside the workflow)
2 get_basin_polygons.py --> extracts shapefile of basin polygons based on specified LHM (right now using 'bergend' polygons)
3 ANIMO_coupling_Delwaq.py --> couples ANIMO loads to delwaq basins (currently couples to Bergend only)

Script 2 and 3 only need to be re-run when the LHM schematisation changes. Script 1 may be re-run locally when the ANIMO input from WEnR is updated or the subsurface LHM that is used changes

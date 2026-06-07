@echo off
setlocal

set "ROOT=%~dp0.."
for %%I in ("%ROOT%") do set "ROOT=%%~fI"

cd /d "%ROOT%"
set "CONDA_PREFIX=%ROOT%\.pixi\envs\default"
set "GDAL_DATA=%CONDA_PREFIX%\Library\share\gdal"
set "PROJ_DATA=%CONDA_PREFIX%\Library\share\proj"
set "PROJ_LIB=%PROJ_DATA%"
set "RIBASIM_HOME=%ROOT%\bin\ribasim"
set "PATH=%CONDA_PREFIX%;%CONDA_PREFIX%\Library\mingw-w64\bin;%CONDA_PREFIX%\Library\usr\bin;%CONDA_PREFIX%\Library\bin;%CONDA_PREFIX%\Scripts;%PATH%"

if not exist "%CONDA_PREFIX%\python.exe" (
    echo Python niet gevonden: %CONDA_PREFIX%\python.exe
    echo Draai eerst: pixi install
    exit /b 1
)

"%CONDA_PREFIX%\python.exe" "%ROOT%\scripts\run_coupled_pipeline.py" %*
exit /b %ERRORLEVEL%

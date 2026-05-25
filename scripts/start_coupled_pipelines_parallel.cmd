@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=%~dp0.."
for %%I in ("%ROOT%") do set "ROOT=%%~fI"
set "RUNNER=%ROOT%\scripts\run_coupled_pipeline.cmd"

if /I "%~1"=="--dry-run" (
    set "DRY_RUN=1"
    shift
) else (
    set "DRY_RUN="
)

if not exist "%RUNNER%" (
    echo Runner niet gevonden: %RUNNER%
    exit /b 1
)

if "%~1"=="" goto all
if /I "%~1"=="all" goto all
goto selected

:all
set "PIPELINES=hdsr-rws venv-rws dod-vecht-hunze-rws wf-nzv-hunze-rws rij-rws aam-limburg-rws dommel-aam-rws dommel-aam-limburg-rws brabantse-delta-rws"
goto start_pipelines

:selected
set "PIPELINES="
:collect_selected
if "%~1"=="" goto start_pipelines
set "PIPELINES=!PIPELINES! %~1"
shift
goto collect_selected

goto start_pipelines

:start_pipelines
cd /d "%ROOT%"
for %%P in (%PIPELINES%) do (
    if defined DRY_RUN (
        echo start "%%P" cmd /k "%RUNNER%" %%P
    ) else (
        start "%%P" cmd /k "%RUNNER%" %%P
    )
)

exit /b 0

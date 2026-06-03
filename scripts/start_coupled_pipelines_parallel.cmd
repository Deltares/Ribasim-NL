@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=%~dp0.."
for %%I in ("%ROOT%") do set "ROOT=%%~fI"
set "RUNNER=%ROOT%\scripts\run_coupled_pipeline.cmd"

set "DRY_RUN="
set "PIPELINES="
set "EXTRA_ARGS="
set "USE_ALL="
set "EXPECT_OPTION_VALUE="
set "HUNZE_PIPELINES="
set "OTHER_PIPELINES="

if not exist "%RUNNER%" (
    echo Runner niet gevonden: %RUNNER%
    exit /b 1
)

:parse_args
if "%~1"=="" goto parsed_args
set "ARG=%~1"

if defined EXPECT_OPTION_VALUE (
    set "EXTRA_ARGS=!EXTRA_ARGS! !ARG!"
    set "EXPECT_OPTION_VALUE="
    shift
    goto parse_args
)

if /I "!ARG!"=="--dry-run" (
    set "DRY_RUN=1"
    shift
    goto parse_args
)

if /I "!ARG!"=="all" (
    set "USE_ALL=1"
    shift
    goto parse_args
)

if "!ARG:~0,2!"=="--" (
    set "EXTRA_ARGS=!EXTRA_ARGS! !ARG!"
    if /I "!ARG!"=="--start-at" set "EXPECT_OPTION_VALUE=1"
    if /I "!ARG!"=="--parallel-workers" set "EXPECT_OPTION_VALUE=1"
    shift
    goto parse_args
)

set "PIPELINES=!PIPELINES! !ARG!"
shift
goto parse_args

:parsed_args
if defined EXPECT_OPTION_VALUE (
    echo Ontbrekende waarde na optie.
    exit /b 1
)

if defined USE_ALL set "PIPELINES="
if not defined PIPELINES (
    set "PIPELINES=hdsr-rws venv-rws dod-vecht-hunze-rws wf-nzv-hunze-rws rij-rws dommel-aam-limburg-rws brabantse-delta-rws"
)

:start_pipelines
cd /d "%ROOT%"
for %%P in (%PIPELINES%) do (
    if /I "%%P"=="dod-vecht-hunze-rws" (
        set "HUNZE_PIPELINES=!HUNZE_PIPELINES! %%P"
    ) else if /I "%%P"=="wf-nzv-hunze-rws" (
        set "HUNZE_PIPELINES=!HUNZE_PIPELINES! %%P"
    ) else if /I "%%P"=="dod-vecht-wf-nzv-hunze-rws" (
        set "HUNZE_PIPELINES=!HUNZE_PIPELINES! %%P"
    ) else if /I "%%P"=="alle-regionaal-rws" (
        set "HUNZE_PIPELINES=!HUNZE_PIPELINES! %%P"
    ) else if /I "%%P"=="alle-regionaal-zonder-wf-rws" (
        set "HUNZE_PIPELINES=!HUNZE_PIPELINES! %%P"
    ) else (
        set "OTHER_PIPELINES=!OTHER_PIPELINES! %%P"
    )
)

for %%P in (%OTHER_PIPELINES%) do (
    set "PIPELINE_ARGS=!EXTRA_ARGS!"
    if /I "%%P"=="alle-regionaal-zonder-wf-rws" set "PIPELINE_ARGS=!PIPELINE_ARGS! --parallel-until-samenvoegen --parallel-new-windows"
    if defined DRY_RUN (
        echo start "%%P" cmd /k ""%RUNNER%" %%P !PIPELINE_ARGS!"
    ) else (
        start "%%P" cmd /k ""%RUNNER%" %%P !PIPELINE_ARGS!"
    )
)

if defined HUNZE_PIPELINES (
    set "HUNZE_COMMAND="
    for %%P in (%HUNZE_PIPELINES%) do (
        set "PIPELINE_ARGS=!EXTRA_ARGS!"
        if /I "%%P"=="alle-regionaal-zonder-wf-rws" set "PIPELINE_ARGS=!PIPELINE_ARGS! --parallel-until-samenvoegen --parallel-new-windows"
        if defined HUNZE_COMMAND (
            set "HUNZE_COMMAND=!HUNZE_COMMAND! ^&^& "%RUNNER%" %%P !PIPELINE_ARGS!"
        ) else (
            set "HUNZE_COMMAND="%RUNNER%" %%P !PIPELINE_ARGS!"
        )
    )

    if defined DRY_RUN (
        echo start "hunze-sequentieel" cmd /k "!HUNZE_COMMAND!"
    ) else (
        start "hunze-sequentieel" cmd /k "!HUNZE_COMMAND!"
    )
)

exit /b 0

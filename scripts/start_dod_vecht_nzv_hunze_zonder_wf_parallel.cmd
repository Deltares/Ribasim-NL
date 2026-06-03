@echo off
setlocal

call "%~dp0run_coupled_pipeline.cmd" dod-vecht-nzv-hunze-rws --start-at full-control --parallel-until-samenvoegen --parallel-new-windows %*
exit /b %ERRORLEVEL%

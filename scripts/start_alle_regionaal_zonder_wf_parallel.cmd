@echo off
setlocal

call "%~dp0run_coupled_pipeline.cmd" alle-regionaal-zonder-wf-rws --start-at full-control --parallel-until-samenvoegen --parallel-new-windows %*
exit /b %ERRORLEVEL%

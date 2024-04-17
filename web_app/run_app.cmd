SET VIRTUAL_ENV=../.pixi/envs/default
SET PATH=%VIRTUAL_ENV%;%VIRTUAL_ENV%\Library\mingw-w64\bin;%VIRTUAL_ENV%\Library\usr\bin;%VIRTUAL_ENV%\Library\bin;%VIRTUAL_ENV%\Scripts;%VIRTUAL_ENV%\bin;%PATH%
SET PROJ_LIB=%VIRTUAL_ENV%\Library\share\proj

python -m bokeh serve app --show --dev --port 5007 --allow-websocket-origin=* --args %1

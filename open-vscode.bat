REM make sure python env in sync with uv.lock
call uv sync

REM activate env
call .\\.venv\\Scripts\\activate.bat

REM open using ribasim python release
code . | exit

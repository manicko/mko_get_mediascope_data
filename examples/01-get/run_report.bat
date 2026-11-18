@echo off
setlocal EnableExtensions

cd /d "%~dp0"
if errorlevel 1 (
    echo Failed to change directory to script folder.
    pause
    exit /b 1
)

chcp 65001 >nul 2>&1

set "PYTHON_CMD="

where py >nul 2>&1
if not errorlevel 1 (
    py -3 -c "import sys" >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=py -3"
        goto :run
    )
)

where python >nul 2>&1
if not errorlevel 1 (
    python -c "import sys" >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=python"
        goto :run
    )
)

where uv >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=uv run python"
    goto :run
)

echo No suitable Python interpreter found (tried: py -3, python, uv run python).
echo Ensure Python 3 and project dependencies are available in the active environment.
pause
exit /b 1

:run
%PYTHON_CMD% "%~dp0run_report.py"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
echo Script finished with exit code %EXIT_CODE%.
pause
exit /b %EXIT_CODE%

@echo off
setlocal
title Whatnot Scanner — Setup

REM pushd handles UNC paths (\\wsl.localhost\...) by mapping to a temp
REM drive; plain `cd /d` would silently fail and land in C:\Windows.
pushd "%~dp0" 2>nul
if errorlevel 1 (
    echo ERROR: Could not enter script folder.
    pause
    exit /b 1
)

echo.
echo === Whatnot Scanner — Setup ===
echo.

REM ── Pick a Python ───────────────────────────────────────────────────────────
REM install.bat passes the interpreter it resolved (or auto-installed) as %1.
REM Run standalone? Fall back to `python` then the `py` launcher on PATH.
set "PYEXE=%~1"
if not defined PYEXE (
    where python >nul 2>nul && set "PYEXE=python"
)
if not defined PYEXE (
    where py >nul 2>nul && set "PYEXE=py"
)
if not defined PYEXE (
    echo ERROR: Python not found on PATH.
    echo Install Python 3.10+ from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during install,
    echo or just run install.bat which can install it for you.
    echo.
    pause
    exit /b 1
)

REM Show which Python we'll use
for /f "tokens=*" %%v in ('"%PYEXE%" --version 2^>^&1') do echo Using %%v

REM ── Create virtualenv if missing ────────────────────────────────────────────
if not exist ".venv\Scripts\python.exe" (
    echo.
    echo Creating virtual environment in .venv\ ...
    "%PYEXE%" -m venv .venv
    if errorlevel 1 (
        echo.
        echo Failed to create virtualenv. See errors above.
        pause
        exit /b 1
    )
) else (
    echo Virtualenv already exists, reusing .venv\
)

REM ── Install / upgrade dependencies ─────────────────────────────────────────
echo.
echo Installing dependencies from requirements.txt ...
".venv\Scripts\python.exe" -m pip install --upgrade pip
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
    echo.
    echo pip install failed. See errors above.
    pause
    exit /b 1
)

echo.
echo === Setup complete ===
echo.
echo Next steps:
echo   1. Make sure .env is in this folder ^(optional — the extension feeds
echo      tokens to the server once you open a Whatnot stream^).
echo   2. Load the extension\ folder as an unpacked extension in your browser
echo      ^(Chrome / Edge / Brave: Extensions page -^> Developer mode -^> Load unpacked^).
echo   3. Run start.bat to launch the scanner, then open http://localhost:5000
echo.
pause

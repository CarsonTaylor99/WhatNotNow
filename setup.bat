@echo off
setlocal
cd /d "%~dp0"
title Whatnot Scanner — Setup

echo.
echo === Whatnot Scanner — Setup ===
echo.

REM ── Check Python is on PATH ─────────────────────────────────────────────────
where python >nul 2>nul
if errorlevel 1 (
    echo ERROR: Python not found on PATH.
    echo Install Python 3.10+ from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during install.
    echo.
    pause
    exit /b 1
)

REM Show which Python we'll use
for /f "tokens=*" %%v in ('python --version') do echo Using %%v

REM ── Create virtualenv if missing ────────────────────────────────────────────
if not exist ".venv\Scripts\python.exe" (
    echo.
    echo Creating virtual environment in .venv\ ...
    python -m venv .venv
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
echo   1. Make sure .env is in this folder (copy from your old PC).
echo   2. Load the extension\ folder as an unpacked extension in Chrome.
echo   3. Run start.bat to launch the scanner.
echo.
pause

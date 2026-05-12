@echo off
title Whatnot Scanner

REM Run from this script's own folder. pushd also copes with the script
REM living on a UNC path (\\server\share\...) by mapping a temp drive.
pushd "%~dp0" 2>nul
if errorlevel 1 (
    echo ERROR: Could not enter the script folder.
    pause
    exit /b 1
)

REM ── Make sure the install actually happened ────────────────────────────────
if not exist ".venv\Scripts\python.exe" (
    echo.
    echo The Python environment is missing ^(no .venv\ folder^).
    echo Run install.bat first ^(or setup.bat if you already have the files^).
    echo.
    popd
    pause
    exit /b 1
)

echo.
echo === Whatnot Scanner ===
echo Dashboard: http://localhost:5000
echo Press Ctrl+C in this window to stop the scanner.
echo.

REM Open the dashboard in the default browser a few seconds after the server
REM starts. Runs in a separate, minimized PowerShell so it doesn't block here.
start "WhatNotNow" /min powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 3; Start-Process 'http://localhost:5000'"

REM ── Launch the server with the project's own Python ────────────────────────
".venv\Scripts\python.exe" main.py

echo.
echo === Server stopped ===
popd
pause

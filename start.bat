@echo off
setlocal EnableDelayedExpansion
title Whatnot Scanner

REM Resolve the script's folder, stripping the trailing backslash. The
REM trailing \ is what was breaking the previous version: when the path
REM ended up inside quotes (e.g. ...\"), cmd parsed it as an escaped quote,
REM scrambled the if-block paren matching, and aborted the whole script
REM silently before any pause could run.
set "SCRIPT_DIR=%~dp0"
if "!SCRIPT_DIR:~-1!"=="\" set "SCRIPT_DIR=!SCRIPT_DIR:~0,-1!"

echo.
echo === Whatnot Scanner ===
echo Folder: !SCRIPT_DIR!
echo.

REM pushd handles UNC paths (\\wsl.localhost\...) by mapping to a temp drive.
pushd "!SCRIPT_DIR!"
if errorlevel 1 (
    echo ERROR: Could not enter folder.
    pause
    exit /b 1
)

REM ── Linux venv (WSL setup) — checked first because that's the common case ─
if exist ".venv\bin\python" (
    echo Detected WSL/Linux venv. Routing through WSL...

    REM Convert this folder to its Linux path. Pipe the trimmed SCRIPT_DIR
    REM (no trailing slash) so wslpath -u sees a clean argument.
    set "WSL_PATH="
    for /f "usebackq delims=" %%i in (`wsl wslpath -u "!SCRIPT_DIR!"`) do set "WSL_PATH=%%i"

    if "!WSL_PATH!"=="" (
        echo.
        echo Could not get WSL path. Open a WSL terminal and run:
        echo   cd ~/WhatNotNow ^&^& .venv/bin/python main.py
        echo.
        goto :stopped
    )

    echo WSL path: !WSL_PATH!
    echo Dashboard: http://localhost:5000
    echo Press Ctrl+C to stop.
    echo.
    wsl --cd "!WSL_PATH!" .venv/bin/python main.py
    goto :stopped
)

REM ── Windows-native venv ────────────────────────────────────────────────────
if exist ".venv\Scripts\python.exe" (
    echo Starting Whatnot Scanner (native Windows venv)...
    echo Dashboard: http://localhost:5000
    echo Press Ctrl+C to stop.
    echo.
    ".venv\Scripts\python.exe" main.py
    goto :stopped
)

echo.
echo No virtualenv found in !SCRIPT_DIR!\.venv
echo Looked for both .venv\bin\python (WSL) and .venv\Scripts\python.exe (Windows).
echo Run setup.bat to create one, or check that you're in the right folder.

:stopped
echo.
echo === Server stopped ===
popd
pause

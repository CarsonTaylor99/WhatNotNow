@echo off
setlocal EnableDelayedExpansion
title Whatnot Scanner

REM Save the script's folder BEFORE we shift directories. Use pushd because
REM it handles UNC paths (\\wsl.localhost\...) by mapping them to a temp
REM drive letter — plain `cd /d` can't do that and would land in C:\Windows.
set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%" 2>nul
if errorlevel 1 (
    echo ERROR: Could not enter script folder.
    echo Path: %SCRIPT_DIR%
    pause
    exit /b 1
)

REM ── Native Windows venv ────────────────────────────────────────────────────
if exist ".venv\Scripts\python.exe" (
    echo Starting Whatnot Scanner (native Windows venv)...
    echo Dashboard: http://localhost:5000
    echo Press Ctrl+C to stop.
    echo.
    ".venv\Scripts\python.exe" main.py
    goto :stopped
)

REM ── WSL/Linux venv — invoke through WSL ────────────────────────────────────
if exist ".venv\bin\python" (
    echo Detected Linux virtualenv. Project is in WSL — invoking via WSL...

    REM Convert script path to WSL path (e.g.,
    REM \\wsl.localhost\Ubuntu\home\claude\WhatNotNow\  →  /home/claude/WhatNotNow)
    for /f "delims=" %%i in ('wsl wslpath -u "%SCRIPT_DIR%" 2^>nul') do set "WSL_PATH=%%i"

    if "!WSL_PATH!"=="" (
        echo Could not convert path to WSL format.
        echo Open a WSL terminal and run manually:
        echo   cd ~/WhatNotNow
        echo   .venv/bin/python main.py
        goto :stopped
    )

    REM Strip trailing slash
    if "!WSL_PATH:~-1!"=="/" set "WSL_PATH=!WSL_PATH:~0,-1!"

    echo WSL working dir: !WSL_PATH!
    echo Dashboard: http://localhost:5000
    echo Press Ctrl+C to stop.
    echo.
    wsl --cd "!WSL_PATH!" .venv/bin/python main.py
    goto :stopped
)

echo No virtualenv found.
echo Looked for: .venv\Scripts\python.exe (Windows) or .venv\bin\python (WSL).
echo Run setup.bat to create one.

:stopped
echo.
echo Server stopped.
popd
pause

@echo off
setlocal
cd /d "%~dp0"
title Whatnot Scanner

REM ── Sanity checks ───────────────────────────────────────────────────────────
if not exist ".venv\Scripts\python.exe" (
    echo No virtualenv found in .venv\
    echo Run setup.bat first.
    echo.
    pause
    exit /b 1
)

if not exist ".env" (
    echo WARNING: No .env file in this folder.
    echo The server will start but won't have any seed auth tokens.
    echo Copy .env from your old PC, or rely on the Chrome extension to push tokens.
    echo.
)

echo.
echo  Starting Whatnot Scanner...
echo  Dashboard: http://localhost:5000
echo  Press Ctrl+C in this window to stop.
echo.

".venv\Scripts\python.exe" main.py

echo.
echo  Server stopped.
pause

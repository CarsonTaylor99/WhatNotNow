@echo off
setlocal EnableDelayedExpansion
title Whatnot Scanner — Installer

echo.
echo ========================================
echo   Whatnot Scanner Installer
echo ========================================
echo.
echo This installer will:
echo   - download the latest project from GitHub
echo   - create a Python virtualenv and install dependencies
echo.
echo You'll handle these manually after:
echo   - copy your .env file into the install folder
echo   - load the extension\ folder in Chrome
echo.

REM ── Pick install folder ─────────────────────────────────────────────────────
set "DEFAULT_DIR=%USERPROFILE%\WhatNotNow"
echo Default install folder: %DEFAULT_DIR%
set "INSTALL_DIR="
set /p "INSTALL_DIR=Press Enter to accept, or type a different path: "
if "%INSTALL_DIR%"=="" set "INSTALL_DIR=%DEFAULT_DIR%"

echo.
echo Installing to: %INSTALL_DIR%
echo.

REM ── Check Python is on PATH ─────────────────────────────────────────────────
where python >nul 2>nul
if errorlevel 1 (
    echo ERROR: Python is not installed or not on PATH.
    echo.
    echo 1. Download Python 3.10+ from https://www.python.org/downloads/
    echo 2. During install, CHECK the "Add Python to PATH" box
    echo 3. Open a NEW terminal/explorer and re-run this installer
    echo.
    pause
    exit /b 1
)
for /f "tokens=*" %%v in ('python --version') do echo Found %%v

REM ── Check curl + powershell are available (they are on Win10+ by default) ──
where curl >nul 2>nul
if errorlevel 1 (
    echo ERROR: curl not found. Windows 10+ ships with curl. Update Windows.
    pause
    exit /b 1
)

REM ── Create install folder ──────────────────────────────────────────────────
if not exist "%INSTALL_DIR%" (
    mkdir "%INSTALL_DIR%"
    if errorlevel 1 (
        echo ERROR: Could not create %INSTALL_DIR%
        pause
        exit /b 1
    )
)
cd /d "%INSTALL_DIR%"

REM ── Download project zip from GitHub ───────────────────────────────────────
echo.
echo Downloading project from GitHub...
curl -fL -o "%TEMP%\wnn-install.zip" https://github.com/CarsonTaylor99/WhatNotNow/archive/refs/heads/master.zip
if errorlevel 1 (
    echo ERROR: Download failed. Check your internet connection.
    pause
    exit /b 1
)

REM ── Extract ─────────────────────────────────────────────────────────────────
echo Extracting...
if exist "%TEMP%\wnn-install-extract" rmdir /S /Q "%TEMP%\wnn-install-extract"
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "Expand-Archive -Path '%TEMP%\wnn-install.zip' -DestinationPath '%TEMP%\wnn-install-extract' -Force"
if errorlevel 1 (
    echo ERROR: Extraction failed.
    pause
    exit /b 1
)

REM Copy contents (the zip extracts into a WhatNotNow-master folder)
xcopy /E /I /Y /Q "%TEMP%\wnn-install-extract\WhatNotNow-master\*" "%INSTALL_DIR%\" >nul
if errorlevel 1 (
    echo ERROR: Copy failed.
    pause
    exit /b 1
)

REM Cleanup
del "%TEMP%\wnn-install.zip" >nul 2>nul
rmdir /S /Q "%TEMP%\wnn-install-extract" >nul 2>nul

REM ── Run setup.bat to create venv + install deps ────────────────────────────
echo.
echo Running setup.bat (creates virtualenv, installs Python packages)...
echo.
call "%INSTALL_DIR%\setup.bat"
if errorlevel 1 (
    echo.
    echo Setup step reported an error. See messages above.
    pause
    exit /b 1
)

REM ── Done ────────────────────────────────────────────────────────────────────
echo.
echo ========================================
echo   Install complete
echo ========================================
echo.
echo Final manual steps:
echo.
echo   1. Copy your .env file from your old PC into:
echo      %INSTALL_DIR%
echo.
echo   2. Load the Chrome extension:
echo      - Open chrome://extensions
echo      - Toggle Developer mode (top-right)
echo      - Click "Load unpacked"
echo      - Select: %INSTALL_DIR%\extension
echo.
echo   3. Double-click start.bat to launch.
echo.
echo Opening the install folder now...
start "" "%INSTALL_DIR%"
echo.
pause

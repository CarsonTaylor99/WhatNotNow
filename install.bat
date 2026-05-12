@echo off
setlocal EnableDelayedExpansion
title Whatnot Scanner — Installer

echo.
echo ========================================
echo   Whatnot Scanner Installer  (Windows)
echo ========================================
echo.
echo This installer will:
echo   - install Python for you if it isn't already on this PC
echo   - download the latest project from GitHub
echo   - create a Python virtualenv and install dependencies
echo   - drop a starter .env file in the folder (you only edit it if you want to)
echo.
echo After it finishes you'll:
echo   - load the extension\ folder in your browser (it feeds login tokens to the scanner)
echo   - open whatnot.com in that browser and leave a tab running in the background
echo   - (optional) edit .env for a token fallback or email notifications
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

REM ── Ensure Python is available (auto-install via winget if missing) ─────────
set "PYEXE="
where python >nul 2>nul && set "PYEXE=python"
if not defined PYEXE (
    where py >nul 2>nul && set "PYEXE=py"
)
if defined PYEXE goto :have_python

echo Python was not found on this PC.
where winget >nul 2>nul
if errorlevel 1 goto :python_manual

echo Installing Python 3.12 with winget. This can take a minute...
echo.
winget install -e --id Python.Python.3.12 --scope user --silent --accept-source-agreements --accept-package-agreements
echo.
REM winget can't refresh THIS window's PATH, so look where it installs Python.
for /d %%P in ("%LOCALAPPDATA%\Programs\Python\Python3*") do (
    if exist "%%P\python.exe" set "PYEXE=%%P\python.exe"
)
if not defined PYEXE (
    for /d %%P in ("%ProgramFiles%\Python3*") do (
        if exist "%%P\python.exe" set "PYEXE=%%P\python.exe"
    )
)
if defined PYEXE goto :have_python

echo.
echo Couldn't confirm the Python install automatically.
echo Please install Python 3.10+ from https://www.python.org/downloads/
echo  - CHECK the "Add Python to PATH" box during install
echo  - then open a NEW window and run install.bat again
start "" https://www.python.org/downloads/
pause
exit /b 1

:python_manual
echo winget isn't available on this PC, so Python can't be installed automatically.
echo.
echo  1. Download Python 3.10+ from https://www.python.org/downloads/
echo  2. During install, CHECK the "Add Python to PATH" box
echo  3. Open a NEW window and re-run this installer
echo.
start "" https://www.python.org/downloads/
pause
exit /b 1

:have_python
for /f "tokens=*" %%v in ('"%PYEXE%" --version 2^>^&1') do echo Using %%v

REM ── Check curl is available (Windows 10 1803+ ships it) ────────────────────
where curl >nul 2>nul
if errorlevel 1 (
    echo ERROR: curl not found. Windows 10/11 ships with curl — please update Windows.
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
call "%INSTALL_DIR%\setup.bat" "%PYEXE%"
if errorlevel 1 (
    echo.
    echo Setup step reported an error. See messages above.
    pause
    exit /b 1
)

REM ── Seed a starter .env from the template if one isn't there yet ────────────
if not exist "%INSTALL_DIR%\.env" (
    if exist "%INSTALL_DIR%\env.example" (
        copy /Y "%INSTALL_DIR%\env.example" "%INSTALL_DIR%\.env" >nul 2>nul
        echo Created a starter .env from env.example.
    )
)

REM ── Done ────────────────────────────────────────────────────────────────────
echo.
echo ========================================
echo   Install complete
echo ========================================
echo.
echo Final steps:
echo.
echo   1. Load the browser extension — this is what keeps the scanner logged in:
echo      - Open your browser's extensions page:
echo          Chrome:  chrome://extensions
echo          Edge:    edge://extensions
echo          Brave:   brave://extensions
echo      - Toggle "Developer mode" on
echo      - Click "Load unpacked"
echo      - Select: %INSTALL_DIR%\extension
echo.
echo   2. Open whatnot.com in that browser (any livestream) and leave that tab
echo      open in the background. The extension pushes fresh tokens from there,
echo      so the scanner stays connected without you copying anything by hand.
echo.
echo   3. (Optional) Edit %INSTALL_DIR%\.env if you want:
echo        - an initial token fallback, so scanning works before you open Whatnot
echo        - the email "send to phone" buttons (needs a Gmail app password)
echo      env.example explains every field — copy values in as needed.
echo.
echo   4. Double-click start.bat. The dashboard opens at http://localhost:5000
echo      Pick your categories on the left, then click Start.
echo.
echo To update later, just re-run this installer — it re-downloads the latest
echo version and leaves your .env in place. (Reload the extension in your
echo browser afterward to pick up extension changes.)
echo.

REM Offer to launch the scanner right now.
set "RUNNOW="
set /p "RUNNOW=Start the scanner now? [Y/n]: "
if /I "%RUNNOW%"=="n" goto :open_folder
if exist "%INSTALL_DIR%\start.bat" (
    start "" "%INSTALL_DIR%\start.bat"
    goto :end
)

:open_folder
echo Opening the install folder...
start "" "%INSTALL_DIR%"

:end
echo.
pause

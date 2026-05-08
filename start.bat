@echo off
title Whatnot Scanner

REM Move off any UNC-mapped temp drive (Z:\) before launching wsl.
REM cmd auto-pushd's to Z:\ when this .bat lives on \\wsl.localhost\...,
REM and that confuses subsequent path resolution.
cd /d "%USERPROFILE%"

echo.
echo === Whatnot Scanner ===
echo Dashboard: http://localhost:5000
echo Press Ctrl+C to stop.
echo.

wsl -d Ubuntu --cd /home/claude/WhatNotNow .venv/bin/python main.py

echo.
echo === Server stopped ===
pause

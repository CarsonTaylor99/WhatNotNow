@echo off
title Whatnot Scanner
echo.
echo  Starting Whatnot Scanner...
echo  Dashboard: http://localhost:5000
echo  Press Ctrl+C in this window to stop.
echo.
wsl -d Ubuntu --cd /home/claude/WhatNotNow .venv/bin/python main.py
echo.
echo  Server stopped.
pause

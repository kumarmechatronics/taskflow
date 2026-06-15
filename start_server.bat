@echo off
title TaskFlow Auto-Backup Server
echo ================================================
echo   TaskFlow Server - Hourly Auto-Backup Active
echo ================================================
echo.
echo   Server   → http://localhost:3000
echo   Backups  → %~dp0backups\
echo   Archive  → %~dp0archive\
echo.
echo Minimise to taskbar — do NOT close this window.
echo.

:loop
python "%~dp0server.py"
echo.
echo [!] Server stopped. Restarting in 5 seconds...
timeout /t 5 /nobreak >nul
goto loop

@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
rem Kill any running instances and start fresh (hidden)
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%scripts\restart_all.ps1"
endlocal

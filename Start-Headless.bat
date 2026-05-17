@echo off
setlocal
set PSARG=-NoProfile -ExecutionPolicy Bypass -File "%~dp0setup_headless.ps1"
powershell %PSARG%
endlocal
exit /b 0

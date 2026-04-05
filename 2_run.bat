@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PY=C:\users\rlaau\appdata\local\programs\python\python310\python.exe

echo.
echo ==================================================
echo   S-RIM v4
echo ==================================================

if "%~1"=="" (
    set /p STOCK=Stock name: 
) else (
    set STOCK=%~1
)

"%PY%" srim_runner_v4.py "%STOCK%"
pause

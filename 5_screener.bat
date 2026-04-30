@echo off
chcp 65001 > nul
cd /d "%~dp0"

REM Python auto-detect: try known paths then fall back to PATH
set PY=
if exist "C:\Users\rlaau\AppData\Local\Programs\Python\Python310\python.exe" (
    set PY=C:\Users\rlaau\AppData\Local\Programs\Python\Python310\python.exe
) else if exist "C:\Python310\python.exe" (
    set PY=C:\Python310\python.exe
) else if exist "C:\Python311\python.exe" (
    set PY=C:\Python311\python.exe
) else if exist "C:\Python312\python.exe" (
    set PY=C:\Python312\python.exe
) else (
    where python >nul 2>&1
    if %ERRORLEVEL%==0 (
        set PY=python
    ) else (
        echo [ERROR] Python not found. Check path in 5_screener.bat
        pause
        exit /b 1
    )
)

echo.
echo ================================================
echo   S-RIM Screener v3
echo ================================================
echo.
echo   [1] Full scan  (Stage1:Finance + Stage2:RIM + Stage3:Supply)
echo   [2] Stage 1 only  (Finance filter, fast)
echo.
echo   Press Enter for [1]
echo.
set /p CHOICE=Select (1 or 2, Enter=1):

if "%CHOICE%"=="2" (
    "%PY%" rim_screener.py --stage1
) else (
    "%PY%" rim_screener.py
)

echo.
echo Screening done. Type EXIT to close this window.
cmd /k

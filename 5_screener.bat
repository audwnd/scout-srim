@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PY=C:\Users\rlaau\AppData\Local\Programs\Python\Python310\python.exe

echo.
echo ==================================================
echo   RIM Screening
echo ==================================================
echo.
echo  [1] All undervalued
echo  [2] More than 10%% undervalued
echo  [3] More than 20%% undervalued
echo  [4] Stage 1 only (fast)
echo  [5] Strict mode (consensus + ROE improving)
echo.
set /p CHOICE=Select (1~5):

if "%CHOICE%"=="2" (
    "%PY%" rim_screener.py --pct -10
) else if "%CHOICE%"=="3" (
    "%PY%" rim_screener.py --pct -20
) else if "%CHOICE%"=="4" (
    "%PY%" rim_screener.py --pct 0 --stage1
) else if "%CHOICE%"=="5" (
    "%PY%" rim_screener.py --pct 0 --strict
) else (
    "%PY%" rim_screener.py --pct 0
)

echo.
pause

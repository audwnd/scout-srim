@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PY=C:\Users\rlaau\AppData\Local\Programs\Python\Python310\python.exe

echo.
echo ==================================================
echo   S-RIM 스크리닝 v3
echo ==================================================
echo.
echo  [1] 전체 실행 (1단계:재무 + 2단계:RIM + 3단계:수급)  (default)
echo  [2] 1단계만 빠르게 (재무필터만, RIM 계산 생략)
echo.
echo  Press Enter for [1] default
echo.
set /p CHOICE=Select (1~2, Enter=1):

if "%CHOICE%"=="2" (
    "%PY%" rim_screener.py --stage1
) else (
    "%PY%" rim_screener.py
)

echo.
echo Screening done. Type EXIT to close this window.
cmd /k

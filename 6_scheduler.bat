@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PY=C:\users\rlaau\appdata\local\programs\python\python310\python.exe
set DIR=%~dp0

echo.
echo ==================================================
echo   RIM 스크리닝 자동 스케줄 등록
echo ==================================================
echo.
echo  매일 오후 4:30 자동 실행으로 등록합니다.
echo  (장마감 16:00 이후 최신 데이터 반영)
echo.

REM 기존 작업 삭제 (있으면)
schtasks /delete /tn "RIM_Screener" /f >nul 2>&1

REM 새 작업 등록
schtasks /create /tn "RIM_Screener" /tr ""%PY%" "%DIR%rim_screener.py" --pct 0" /sc daily /st 16:30 /f

if %ERRORLEVEL%==0 (
    echo  등록 성공: 매일 16:30 자동 실행
    echo  결과는 OUTPUT 폴더에 저장됩니다.
) else (
    echo  등록 실패 - 관리자 권한으로 실행해주세요.
)

echo.
pause

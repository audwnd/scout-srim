@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PY=C:\users\rlaau\appdata\local\programs\python\python310\python.exe

echo.
echo ==================================================
echo   패키지 설치
echo ==================================================
echo.

"%PY%" -m pip install --upgrade pip

echo  [1/6] requests 설치...
"%PY%" -m pip install requests --quiet

echo  [2/6] flask 설치...
"%PY%" -m pip install flask --quiet

echo  [3/6] openpyxl 설치...
"%PY%" -m pip install openpyxl --quiet

echo  [4/6] beautifulsoup4 + lxml 설치...
"%PY%" -m pip install beautifulsoup4 lxml --quiet

echo  [5/6] pykrx 설치...
"%PY%" -m pip install pykrx --quiet

echo  [6/6] pandas 설치...
"%PY%" -m pip install pandas --quiet

echo.
echo  설치 완료!
pause

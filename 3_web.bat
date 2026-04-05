@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PY=C:\Users\rlaau\AppData\Local\Programs\Python\Python310\python.exe

"%PY%" -c "import flask" 2>nul || "%PY%" -m pip install flask --quiet

REM VBScript로 창 없이 브라우저 오픈 (3초 후)
echo Set ws=CreateObject("WScript.Shell") > %temp%\open_browser.vbs
echo WScript.Sleep 3000 >> %temp%\open_browser.vbs
echo ws.Run "http://localhost:5000" >> %temp%\open_browser.vbs
start /min wscript %temp%\open_browser.vbs

"%PY%" app.py

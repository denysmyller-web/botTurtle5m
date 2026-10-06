@echo off
setlocal
cd /d "%~dp0"
if exist "%~dp0.venv-1\Scripts\python.exe" (
    "%~dp0.venv-1\Scripts\python.exe" app.py
) else if exist "%~dp0.venv\Scripts\python.exe" (
    "%~dp0.venv\Scripts\python.exe" app.py
) else (
    where py >nul 2>nul
    if errorlevel 1 (
        python app.py
    ) else (
        py -3 app.py
    )
)
if errorlevel 1 pause
endlocal

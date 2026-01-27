@echo off
setlocal
cd /d "%~dp0"

echo ==========================================
echo   Amazon Economics Pipeline Setup Helper
echo ==========================================
echo.

:: 1. Check for Python
echo [1/4] Checking for Python...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    py --version >nul 2>&1
    if %errorlevel% neq 0 (
        echo.
        echo [ERROR] Python not found!
        echo.
        echo Please install Python from: https://www.python.org/downloads/
        echo Make sure to check "Add Python to PATH" during installation.
        echo.
        echo Once installed, close this window and run this script again.
        pause
        exit /b 1
    ) else (
        echo Found 'py' launcher. Using 'py'.
        set PYTHON_CMD=py
    )
) else (
    echo Found 'python' command.
    set PYTHON_CMD=python
)

:: 2. Create Virtual Environment
echo.
echo [2/4] Checking/Creating Virtual Environment...
if not exist ".venv" (
    echo Creating virtual environment in .venv...
    %PYTHON_CMD% -m venv .venv
) else (
    echo Virtual environment already exists.
)

:: 3. Install Dependencies
echo.
echo [3/4] Installing dependencies...
call .venv\Scripts\activate.bat
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Failed to install dependencies.
    pause
    exit /b 1
)

:: 4. Run Ingestion Script
echo.
echo [4/4] Running Ingestion Script...
%PYTHON_CMD% execution/convert_excel_to_json.py

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Script failed. Check the output above.
) else (
    echo.
    echo [SUCCESS] Script completed successfully.
)

pause

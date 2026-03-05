@echo off
REM ============================================================
REM Weather Data Update Script for Windows
REM ============================================================
REM
REM This batch file updates weather data from the Open-Meteo API.
REM It creates timestamped log files in the logs directory.
REM
REM Usage:
REM   - Double-click to run manually
REM   - Or schedule via Windows Task Scheduler for automatic updates
REM
REM Task Scheduler Setup:
REM   1. Open Task Scheduler (taskschd.msc)
REM   2. Create Basic Task > Name: "Weather Data Update"
REM   3. Trigger: Daily or as needed
REM   4. Action: Start a program
REM   5. Program: Full path to this .bat file
REM   6. Start in: Full path to project directory
REM
REM ============================================================

setlocal enabledelayedexpansion

REM Get the directory where this script is located
set "SCRIPT_DIR=%~dp0"
REM Remove trailing backslash
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

REM Get project root directory (parent of scripts folder)
for %%i in ("%SCRIPT_DIR%") do set "PROJECT_DIR=%%~dpi"
REM Remove trailing backslash
set "PROJECT_DIR=%PROJECT_DIR:~0,-1%"

REM Set paths
set "LOGS_DIR=%PROJECT_DIR%\logs"
set "PYTHON_SCRIPT=%SCRIPT_DIR%\update_weather.py"

REM Generate timestamp for log file name
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "TIMESTAMP=%dt:~0,4%-%dt:~4,2%-%dt:~6,2%_%dt:~8,2%%dt:~10,2%%dt:~12,2%"
set "LOG_FILE=%LOGS_DIR%\weather_update_%TIMESTAMP%.log"
set "LATEST_LOG=%LOGS_DIR%\weather_update_latest.log"

REM Create logs directory if it doesn't exist
if not exist "%LOGS_DIR%" (
    echo Creating logs directory: %LOGS_DIR%
    mkdir "%LOGS_DIR%"
)

REM Start logging
echo ============================================================ >> "%LOG_FILE%"
echo Weather Data Update >> "%LOG_FILE%"
echo Started: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"
echo Project Directory: %PROJECT_DIR% >> "%LOG_FILE%"
echo Python Script: %PYTHON_SCRIPT% >> "%LOG_FILE%"
echo Log File: %LOG_FILE% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM Display start message
echo ============================================================
echo Weather Data Update
echo ============================================================
echo Started: %date% %time%
echo Log file: %LOG_FILE%
echo.

REM Check if Python is available (try python first, then py launcher)
set "PYTHON_CMD="
where python >nul 2>&1
if %ERRORLEVEL% equ 0 (
    set "PYTHON_CMD=python"
) else (
    where py >nul 2>&1
    if %ERRORLEVEL% equ 0 (
        set "PYTHON_CMD=py"
    )
)

if "%PYTHON_CMD%"=="" (
    echo ERROR: Python not found in PATH >> "%LOG_FILE%"
    echo ERROR: Python not found in PATH
    echo Please install Python and add it to your PATH
    echo Or install the Python Launcher ^(py^) for Windows
    goto :error
)

echo Using Python: %PYTHON_CMD% >> "%LOG_FILE%"

REM Check if script exists
if not exist "%PYTHON_SCRIPT%" (
    echo ERROR: Python script not found: %PYTHON_SCRIPT% >> "%LOG_FILE%"
    echo ERROR: Python script not found: %PYTHON_SCRIPT%
    goto :error
)

REM Change to project directory
cd /d "%PROJECT_DIR%"

REM Run the Python script and capture output
echo Running weather update script... >> "%LOG_FILE%"
echo Running weather update script...
echo. >> "%LOG_FILE%"

%PYTHON_CMD% "%PYTHON_SCRIPT%" >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo. >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

if %EXIT_CODE% equ 0 (
    echo [SUCCESS] Weather update completed successfully >> "%LOG_FILE%"
    echo [SUCCESS] Weather update completed successfully
    echo Exit Code: %EXIT_CODE% >> "%LOG_FILE%"
) else (
    echo [ERROR] Weather update failed with exit code: %EXIT_CODE% >> "%LOG_FILE%"
    echo [ERROR] Weather update failed with exit code: %EXIT_CODE%
)

echo Ended: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

REM Copy to latest log for easy access
copy /y "%LOG_FILE%" "%LATEST_LOG%" >nul 2>&1

echo.
echo Log saved to: %LOG_FILE%
echo.

REM Exit with the Python script's exit code
exit /b %EXIT_CODE%

:error
echo. >> "%LOG_FILE%"
echo [ERROR] Script terminated with errors >> "%LOG_FILE%"
echo Ended: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

copy /y "%LOG_FILE%" "%LATEST_LOG%" >nul 2>&1

exit /b 1

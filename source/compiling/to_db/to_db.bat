@echo off
set SCRIPT_NAME=to_db.py
set EXE_NAME=universal-converter
set ICON_NAME=db.ico

echo ===========================================
echo  Starting %EXE_NAME% Build Process
echo ===========================================

:: 1. Check for PyInstaller installation
echo Checking for PyInstaller...
pip show pyinstaller > nul
if errorlevel 1 (
echo PyInstaller not found. Installing now...
pip install pyinstaller
if errorlevel 1 (
echo ERROR: Failed to install PyInstaller. Please check your Python/pip setup.
goto :eof
)
echo PyInstaller successfully installed.
) else (
echo PyInstaller is already installed.
)

:: 2. Check for the icon file
if not exist "%ICON_NAME%" (
echo.
echo WARNING: Icon file "%ICON_NAME%" not found!
echo The executable will be built without a custom icon.
set ICON_FLAG=
) else (
echo Icon "%ICON_NAME%" found.
set ICON_FLAG=--icon=%ICON_NAME%
)

:: 3. Run PyInstaller command
echo.
echo Running PyInstaller... This may take a few moments...

pyinstaller --onefile ^
--name "%EXE_NAME%" ^
%ICON_FLAG% ^
--hidden-import=pandas._libs.tslibs.np_datetime ^
--hidden-import=pandas._libs.tslibs.nattype ^
--hidden-import=sqlalchemy.sql.default_comparator ^
"%SCRIPT_NAME%"

:: Check if PyInstaller succeeded
if errorlevel 1 (
echo.
echo ===========================================
echo  BUILD FAILED! Check the console output above for errors.
echo ===========================================
) else (
echo.
echo ===========================================
echo  BUILD SUCCESSFUL!
echo ===========================================
echo Your executable is ready in the "dist" folder:
echo  dist%EXE_NAME%.exe
)

pause
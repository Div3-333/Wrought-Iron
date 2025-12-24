@echo off
echo Building Wrought Iron Distribution...

:: Ensure static files are up to date
echo Collecting static files...
python manage.py collectstatic --noinput

echo Running PyInstaller with WroughtIron.spec...
python -m PyInstaller WroughtIron.spec --clean

echo Build complete. Check the 'dist' folder.
@echo off
setlocal

pushd "%~dp0"

echo [1/3] Checking tools...
where python >nul 2>nul
if errorlevel 1 (
  echo Python was not found in PATH.
  echo Install Python or add it to PATH, then try again.
  pause
  exit /b 1
)

where npm >nul 2>nul
if errorlevel 1 (
  echo npm was not found in PATH.
  echo Install Node.js or add it to PATH, then try again.
  pause
  exit /b 1
)

if not exist "webui\package.json" (
  echo Could not find webui\package.json
  pause
  exit /b 1
)

echo [2/3] Building web UI...
call npm --prefix webui run build
if errorlevel 1 (
  echo.
  echo Build failed.
  pause
  exit /b 1
)

echo [3/3] Starting sandbox app...
echo The browser should open automatically.
echo Close this window or press Ctrl+C to stop the app.
echo.
python runner.py

set EXIT_CODE=%ERRORLEVEL%
if not "%EXIT_CODE%"=="0" (
  echo.
  echo App exited with code %EXIT_CODE%.
  pause
)

popd
endlocal

@echo off
echo ============================================
echo  SEC Ledger — Windows Build Script
echo ============================================
echo.
echo  Normally you don't need this: GitHub builds SEC-Ledger.exe
echo  automatically (see README). Use it to build on your own PC.
echo.

echo [1/3] Installing dependencies...
pip install -r requirements.txt pyinstaller
if errorlevel 1 (echo ERROR: pip install failed & pause & exit /b 1)

echo.
echo [2/3] Building executable...
pyinstaller --noconfirm sec_ledger.spec
if errorlevel 1 (echo ERROR: Build failed & pause & exit /b 1)

echo.
echo [3/3] Checking the build...
dist\SEC-Ledger.exe --self-test
if errorlevel 1 (echo ERROR: Self-test failed & pause & exit /b 1)

echo.
echo ============================================
echo  Done!
echo  Your executable: dist\SEC-Ledger.exe
echo  Share that single file with others.
echo  They just double-click it — no Python needed.
echo ============================================
pause

@echo off
echo ============================================
echo  SEC Filing Collator — Windows Build Script
echo ============================================
echo.

echo [1/3] Installing dependencies...
pip install -r requirements.txt
if errorlevel 1 (echo ERROR: pip install failed & pause & exit /b 1)

echo.
echo [2/3] Installing PyInstaller...
pip install pyinstaller
if errorlevel 1 (echo ERROR: PyInstaller install failed & pause & exit /b 1)

echo.
echo [3/3] Building executable...
pyinstaller --onefile ^
    --collect-all streamlit ^
    --add-data "app.py;." ^
    --hidden-import streamlit ^
    --hidden-import pandas ^
    --hidden-import requests ^
    --hidden-import bs4 ^
    --hidden-import lxml ^
    --name sec-collator ^
    launcher.py
if errorlevel 1 (echo ERROR: Build failed & pause & exit /b 1)

echo.
echo ============================================
echo  Done!
echo  Your executable: dist\sec-collator.exe
echo  Share that single file with others.
echo  They just double-click it — no Python needed.
echo ============================================
pause

#!/bin/bash
set -e

echo "============================================"
echo " SEC Filing Collator — Mac/Linux Build Script"
echo "============================================"
echo

echo "[1/3] Installing dependencies..."
pip install -r requirements.txt

echo
echo "[2/3] Installing PyInstaller..."
pip install pyinstaller

echo
echo "[3/3] Building executable..."
pyinstaller --onefile \
    --collect-all streamlit \
    --add-data "app.py:." \
    --hidden-import streamlit \
    --hidden-import pandas \
    --hidden-import requests \
    --hidden-import bs4 \
    --hidden-import lxml \
    --name sec-collator \
    launcher.py

echo
echo "============================================"
echo " Done!"
echo " Your executable: dist/sec-collator"
echo " Share that single file with others."
echo " They just run it — no Python needed."
echo "============================================"

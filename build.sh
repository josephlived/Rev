#!/bin/bash
set -e

echo "============================================"
echo " SEC Ledger — Mac/Linux Build Script"
echo "============================================"
echo
echo " The supported desktop app is the Windows .exe that GitHub"
echo " builds automatically (see README). This builds a local binary."
echo

echo "[1/3] Installing dependencies..."
pip install -r requirements.txt pyinstaller

echo
echo "[2/3] Building executable..."
pyinstaller --noconfirm sec_ledger.spec

echo
echo "[3/3] Checking the build..."
dist/SEC-Ledger --self-test

echo
echo "============================================"
echo " Done!"
echo " Your executable: dist/SEC-Ledger"
echo "============================================"

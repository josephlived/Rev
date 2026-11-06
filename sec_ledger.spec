# PyInstaller build recipe for the SEC Ledger desktop app (dist/SEC-Ledger.exe).
# Used by build.bat / build.sh and .github/workflows/windows-app.yml:
#     pyinstaller --noconfirm sec_ledger.spec
from PyInstaller.utils.hooks import collect_all

datas = [("app.py", ".")]
binaries = []
# Streamlit runs app.py from a file, so PyInstaller can't follow its imports:
# list the app's own modules and the libraries only app.py/edgar.py use
hiddenimports = [
    "config",
    "edgar",
    "russell",
    "settings",
    "anthropic",
    "bs4",
    "lxml",
    "lxml.etree",
    "openpyxl",
]
# Streamlit needs its frontend files and package metadata, not just its code
st_datas, st_binaries, st_hiddenimports = collect_all("streamlit")
datas += st_datas
binaries += st_binaries
hiddenimports += st_hiddenimports

a = Analysis(
    ["launcher.py"],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="SEC-Ledger",
    icon="assets/icon.ico",
    console=True,  # the console window is how users quit the app
    upx=False,  # UPX-packed executables trigger more antivirus false positives
)

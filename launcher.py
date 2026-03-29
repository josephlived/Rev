"""
PyInstaller entry point.

This file is what PyInstaller wraps into the .exe / binary.
It starts the Streamlit server and opens the app in the browser.

Do NOT run this directly during development — use:
    streamlit run app.py
"""
import os
import sys


def main():
    from streamlit.web import cli as stcli

    if getattr(sys, "frozen", False):
        # Running inside a PyInstaller bundle
        app_path = os.path.join(sys._MEIPASS, "app.py")
    else:
        app_path = os.path.join(os.path.dirname(__file__), "app.py")

    sys.argv = [
        "streamlit", "run", app_path,
        "--server.port=8501",
        "--server.headless=true",
        "--browser.serverAddress=localhost",
        "--browser.gatherUsageStats=false",
    ]
    sys.exit(stcli.main())


if __name__ == "__main__":
    main()

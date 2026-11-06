"""
Desktop entry point: this is what PyInstaller wraps into SEC-Ledger.exe.

It starts the Streamlit server on this computer only and opens the app in the
default browser. Closing the console window stops the app.

Do NOT run this directly during development — use:
    streamlit run app.py

    SEC-Ledger.exe --self-test   imports everything the app needs and exits;
                                 the build workflow uses it to catch modules
                                 missing from the bundle.
    SEC_LEDGER_NO_BROWSER=1      starts the server without opening a browser
                                 (used by the smoke test).
"""
import os
import socket
import sys
import threading
import time
import urllib.request
import webbrowser

APP_NAME = "SEC Ledger"
DEFAULT_PORT = 8501
STARTUP_TIMEOUT_S = 180  # first launch can be slow while antivirus scans the .exe

# Never send localhost requests through a corporate proxy
_local_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def main():
    if "--self-test" in sys.argv[1:]:
        sys.exit(_self_test())

    port = DEFAULT_PORT
    if _is_app_running(port):
        # Already open (e.g. double-clicked twice): just show it again
        print(f"{APP_NAME} is already running at http://localhost:{port}", flush=True)
        _open_browser(port)
        return
    if not _port_is_free(port):
        port = _find_free_port()

    _set_console_title(f"{APP_NAME} - close this window to quit")
    print(f"Starting {APP_NAME}... your browser will open in a moment.", flush=True)
    print("Keep this window open while you use the app. Close it to quit.\n", flush=True)
    threading.Thread(target=_open_browser_when_ready, args=(port,), daemon=True).start()

    from streamlit.web import cli as stcli

    sys.argv = [
        "streamlit", "run", _app_path(),
        # Streamlit assumes development mode when it isn't pip-installed, and
        # then refuses to start with an explicit port
        "--global.developmentMode=false",
        # Only this computer can reach the app (and the saved API key)
        "--server.address=localhost",
        f"--server.port={port}",
        "--server.headless=true",
        "--server.fileWatcherType=none",
        "--browser.gatherUsageStats=false",
        # Hide Streamlit's developer toolbar ("Deploy" button etc.)
        "--client.toolbarMode=viewer",
        "--theme.base=dark",
    ]
    sys.exit(stcli.main())


def _app_path() -> str:
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "app.py")


def _is_app_running(port: int) -> bool:
    try:
        with _local_opener.open(f"http://localhost:{port}/_stcore/health", timeout=2) as resp:
            return resp.read().strip() == b"ok"
    except OSError:
        return False


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if os.name != "nt":
            # Match the server's own bind, so a port still cooling down from
            # the last run (TIME_WAIT) counts as free. On Windows this option
            # would allow binding a port that is genuinely in use.
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _open_browser(port: int) -> None:
    if os.environ.get("SEC_LEDGER_NO_BROWSER") != "1":
        webbrowser.open(f"http://localhost:{port}")


def _open_browser_when_ready(port: int) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_S
    while time.monotonic() < deadline:
        if _is_app_running(port):
            print(f"{APP_NAME} is running at http://localhost:{port}", flush=True)
            _open_browser(port)
            return
        time.sleep(0.5)
    print(
        f"\n{APP_NAME} is taking longer than expected to start. "
        f"If no error appears above, open http://localhost:{port} in your browser.",
        flush=True,
    )


def _set_console_title(title: str) -> None:
    if os.name == "nt":
        import ctypes

        ctypes.windll.kernel32.SetConsoleTitleW(title)


def _self_test() -> int:
    """Exercise every library the app uses; return a process exit code."""
    failures = []

    def check(name, fn):
        try:
            fn()
            print(f"ok    {name}", flush=True)
        except Exception as exc:  # report every failure, not just the first
            failures.append(name)
            print(f"FAIL  {name}: {type(exc).__name__}: {exc}", flush=True)

    def app_script():
        if not os.path.isfile(_app_path()):
            raise FileNotFoundError(_app_path())

    def streamlit_frontend():
        import streamlit

        index = os.path.join(os.path.dirname(streamlit.__file__), "static", "index.html")
        if not os.path.isfile(index):
            raise FileNotFoundError(index)

    def app_modules():
        import config, edgar, russell, settings  # noqa: F401,E401

    def anthropic_client():
        import anthropic

        anthropic.Anthropic(api_key="self-test")  # builds the client, no network call

    def excel_round_trip():
        import io

        import pandas as pd

        buf = io.BytesIO()
        pd.DataFrame({"CIK": ["320193"]}).to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        pd.read_excel(buf, dtype=str)

    def html_parser():
        from bs4 import BeautifulSoup

        BeautifulSoup("<p>ok</p>", "lxml")

    check("app.py bundled", app_script)
    check("streamlit frontend", streamlit_frontend)
    check("app modules", app_modules)
    check("anthropic client", anthropic_client)
    check("excel read/write", excel_round_trip)
    check("html parser", html_parser)

    print("\nSELF-TEST " + ("FAILED: " + ", ".join(failures) if failures else "PASSED"), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    main()

"""
Desktop entry point: start_sec_ledger.py runs this with the PC's own Python.

It starts the Streamlit server on this computer only and opens the app in the
default browser. Closing the console window stops the app.

During development, use:
    streamlit run app.py

    SEC_LEDGER_NO_BROWSER=1   starts the server without opening a browser
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
STARTUP_TIMEOUT_S = 180  # first launch can be slow while antivirus scans the libraries

# Never send localhost requests through a corporate proxy
_local_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def main():
    # Before anything opens an HTTPS connection
    _use_system_certificates()

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


def _use_system_certificates() -> None:
    """Trust the certificates the operating system trusts, as browsers do.

    Company networks that inspect HTTPS re-sign traffic with an internal
    certificate that IT installs into Windows. Python's own certificate list
    (certifi) doesn't include it, so every SEC and iShares request failed with
    CERTIFICATE_VERIFY_FAILED. Certificates are still verified,
    just against the Windows store instead of certifi.
    """
    import truststore

    truststore.inject_into_ssl()


def _app_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.py")


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


if __name__ == "__main__":
    main()

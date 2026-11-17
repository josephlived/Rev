"""
Start SEC Ledger with the Python already installed on this PC (no .exe needed).

    python start_sec_ledger.py                first run: sets everything up
                                              (3-5 min), adds a desktop
                                              shortcut, then opens the app
    python start_sec_ledger.py --shortcut     re-create the desktop shortcut
    python start_sec_ledger.py --setup-only   install/update, don't start
    python start_sec_ledger.py --no-shortcut  don't add the shortcut on setup

The app's libraries go in a private ".venv" folder next to this file, so
nothing else on the PC changes; delete that folder to undo. It lives here and
not in AppData because Microsoft Store Python redirects AppData writes to a
private copy, which breaks virtual environments created there.

Only the standard library is used: this runs before anything is installed.
"""
# Lets older Pythons reach the friendly version check instead of failing on
# the "str | None" annotations
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import venv

APP_DIR = os.path.dirname(os.path.abspath(__file__))
VENV_DIR = os.path.join(APP_DIR, ".venv")
REQUIREMENTS = os.path.join(APP_DIR, "requirements.txt")
STAMP_PATH = os.path.join(VENV_DIR, "sec-ledger-installed.txt")
SHORTCUT_NAME = "SEC Ledger.lnk"
MIN_PYTHON = (3, 10)


def main() -> int:
    args = set(sys.argv[1:])
    if sys.version_info < MIN_PYTHON:
        return _fail(
            f"SEC Ledger needs Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]} or newer; "
            f"this is Python {sys.version.split()[0]}."
        )

    try:
        fresh = _ensure_venv()
        _ensure_packages()
    except subprocess.CalledProcessError:
        return _fail(
            "Setup could not finish (see the messages above). This usually means the "
            "network blocked the download of Python packages from pypi.org. "
            "Run this again; if it keeps failing, ask IT whether pypi.org is allowed."
        )

    if os.name == "nt" and (
        "--shortcut" in args or (fresh and "--no-shortcut" not in args)
    ):
        _create_desktop_shortcut()

    if "--setup-only" in args:
        print("Setup complete.", flush=True)
        return 0

    env = dict(os.environ, SEC_LEDGER_DESKTOP="1")
    try:
        code = subprocess.call(
            [_venv_python(), os.path.join(APP_DIR, "launcher.py")], env=env
        )
    except KeyboardInterrupt:  # Ctrl+C in the console window
        return 0
    if code != 0:
        return _fail(f"SEC Ledger stopped with an error (code {code}). See the messages above.")
    return 0


def _venv_python() -> str:
    if os.name == "nt":
        return os.path.join(VENV_DIR, "Scripts", "python.exe")
    return os.path.join(VENV_DIR, "bin", "python")


def _venv_version() -> str | None:
    try:
        out = subprocess.run(
            [_venv_python(), "-c", "import sys; print('%d.%d' % sys.version_info[:2])"],
            capture_output=True, text=True, timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


def _ensure_venv() -> bool:
    """Create .venv if it is missing, broken, or from another Python version.
    Returns True when it was (re)created."""
    wanted = "%d.%d" % sys.version_info[:2]
    if os.path.isfile(_venv_python()) and _venv_version() == wanted:
        return False
    print("First-time setup: preparing SEC Ledger (one time only)...", flush=True)
    venv.EnvBuilder(with_pip=True, clear=True).create(VENV_DIR)
    return True


def _requirements_stamp() -> str:
    with open(REQUIREMENTS, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    return f"{digest} python-{sys.version_info[0]}.{sys.version_info[1]}"


def _ensure_packages() -> None:
    """Install requirements.txt into .venv when it is new or has changed."""
    stamp = _requirements_stamp()
    try:
        with open(STAMP_PATH) as f:
            if f.read().strip() == stamp:
                return
    except OSError:
        pass
    print(
        "Installing the app's libraries. This takes 3-5 minutes the first time "
        "(and after updates); please keep this window open...\n",
        flush=True,
    )
    subprocess.check_call(
        [_venv_python(), "-m", "pip", "install", "--disable-pip-version-check",
         "-r", REQUIREMENTS]
    )
    with open(STAMP_PATH, "w") as f:
        f.write(stamp)


def _create_desktop_shortcut() -> None:
    """Put a 'SEC Ledger' shortcut on the desktop that runs this file with this
    Python. Best effort: on failure, explain how to start the app instead."""

    def ps_quote(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    script = os.path.abspath(__file__)
    command = "; ".join([
        # GetFolderPath follows a desktop moved into OneDrive
        "$desktop = [Environment]::GetFolderPath('Desktop')",
        "if (-not (Test-Path $desktop)) { New-Item -ItemType Directory -Path $desktop | Out-Null }",
        f"$s = (New-Object -ComObject WScript.Shell).CreateShortcut((Join-Path $desktop {ps_quote(SHORTCUT_NAME)}))",
        f"$s.TargetPath = {ps_quote(sys.executable)}",
        f"$s.Arguments = {ps_quote(chr(34) + script + chr(34))}",
        f"$s.WorkingDirectory = {ps_quote(APP_DIR)}",
        f"$s.IconLocation = {ps_quote(os.path.join(APP_DIR, 'assets', 'icon.ico'))}",
        "$s.Description = 'Start SEC Ledger'",
        "$s.Save()",
    ])
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", command],
            check=True, capture_output=True, timeout=60,
        )
        print("Added a 'SEC Ledger' shortcut to your desktop.", flush=True)
    except (OSError, subprocess.SubprocessError):
        print(
            "Could not add a desktop shortcut. To start SEC Ledger later, open this "
            f"folder in Terminal and run:  python start_sec_ledger.py\n  ({APP_DIR})",
            flush=True,
        )


def _fail(message: str) -> int:
    print(f"\nERROR: {message}", flush=True)
    # Started from the desktop shortcut, the window would close before the
    # message can be read
    if sys.stdin is not None and sys.stdin.isatty():
        try:
            input("\nPress Enter to close this window...")
        except EOFError:
            pass
    return 1


if __name__ == "__main__":
    sys.exit(main())

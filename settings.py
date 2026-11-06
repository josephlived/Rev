"""
Per-user settings for the desktop app, stored as JSON in the user's own
app-data folder (config.SETTINGS_PATH).

Only used when config.IS_DESKTOP is true: on Streamlit Cloud the file would be
shared by every visitor.
"""
import json
import os
import tempfile

from config import SETTINGS_PATH

_API_KEY = "anthropic_api_key"


def get_api_key() -> str:
    value = _load().get(_API_KEY, "")
    return value if isinstance(value, str) else ""


def set_api_key(api_key: str) -> None:
    data = _load()
    data[_API_KEY] = api_key.strip()
    _save(data)


def clear_api_key() -> None:
    data = _load()
    if data.pop(_API_KEY, None) is not None:
        _save(data)


def _load() -> dict:
    try:
        with open(SETTINGS_PATH, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _save(data: dict) -> None:
    folder = os.path.dirname(SETTINGS_PATH)
    os.makedirs(folder, exist_ok=True)
    # Write to a temp file then swap it in, so a crash can't leave half a file
    fd, tmp_path = tempfile.mkstemp(dir=folder, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, SETTINGS_PATH)
    except BaseException:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise

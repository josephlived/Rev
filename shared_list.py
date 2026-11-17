"""
Shared constituent list for the desktop app.

The team keeps its FactSet list in a Teams/SharePoint folder named
config.SHARED_LIST_FOLDER_NAME, synced to each PC by OneDrive, and the app uses
the newest .xlsx/.csv file in it instead of an upload on every run.

The synced folder's path differs per PC (it contains the user and company
names), so it is found automatically under the OneDrive folders and the user
profile, or pasted once, and remembered in settings. The last list that loaded
successfully is kept as a local backup for when the shared file can't be read
(offline, not synced yet, open in Excel, or not a valid list).
"""
import io
import json
import os
import tempfile
import time
from collections import deque
from datetime import datetime
from typing import Callable, NamedTuple

import requests

import russell
import settings
from config import (
    SHARED_LIST_BACKUP_DIR,
    SHARED_LIST_FOLDER_NAME,
    SHARED_LIST_MAX_AGE_DAYS,
)

LIST_EXTENSIONS = (".xlsx", ".csv")
_BACKUP_META = os.path.join(SHARED_LIST_BACKUP_DIR, "last_good.json")

# Bounds for the automatic search, so a large profile can't stall the app
_PROFILE_SEARCH_DEPTH = 4  # <profile>\Company\Site - Documents\Channel\<folder>
_ONEDRIVE_SEARCH_DEPTH = 3  # <OneDrive - Company>\Channel\<folder>
_MAX_DIRS_VISITED = 20_000
_SKIP_DIRS = {"appdata", "node_modules", ".venv", "venv", "__pycache__", "$recycle.bin"}


class SharedListError(RuntimeError):
    """The shared list (and its backup) could not be used."""


class SharedFile(NamedTuple):
    path: str
    modified: float  # timestamp of the file's last save

    @property
    def name(self) -> str:
        return os.path.basename(self.path)

    @property
    def age_days(self) -> float:
        return (time.time() - self.modified) / 86400


def format_date(ts: float) -> str:
    d = datetime.fromtimestamp(ts)
    return f"{d:%b} {d.day}, {d.year}"


def is_stale(modified: float) -> bool:
    return (time.time() - modified) / 86400 > SHARED_LIST_MAX_AGE_DAYS


# ── Finding the folder ────────────────────────────────────────────────────────

def set_folder(raw_path: str) -> str:
    """Validate and remember a pasted folder path ("Copy as path" adds quotes)."""
    cleaned = raw_path.strip().strip('"').strip("'").strip()
    path = os.path.normpath(os.path.expanduser(cleaned)) if cleaned else ""
    if not path or not os.path.isdir(path):
        raise SharedListError(f"Folder not found: {cleaned or '(empty)'}")
    settings.set_shared_folder(path)
    return path


def search_folder() -> str:
    """Breadth-first search for SHARED_LIST_FOLDER_NAME under the OneDrive
    folders and the user profile. Returns its path, or '' if not found."""
    target = SHARED_LIST_FOLDER_NAME.casefold()
    visited = 0
    for root, max_depth in _search_roots():
        queue = deque([(root, 0)])
        while queue:
            path, depth = queue.popleft()
            try:
                entries = list(os.scandir(path))
            except OSError:  # no access, or the folder vanished
                continue
            for entry in entries:
                try:
                    # Junctions such as "My Documents" point back into the profile
                    if entry.is_symlink() or getattr(entry, "is_junction", lambda: False)():
                        continue
                    if not entry.is_dir():
                        continue
                except OSError:
                    continue
                if entry.name.casefold() == target:
                    return entry.path
                visited += 1
                if visited >= _MAX_DIRS_VISITED:
                    return ""
                if (
                    depth + 1 < max_depth
                    and not entry.name.startswith(".")
                    and entry.name.casefold() not in _SKIP_DIRS
                ):
                    queue.append((entry.path, depth + 1))
    return ""


def _search_roots() -> list[tuple[str, int]]:
    roots = []
    for var in ("OneDriveCommercial", "OneDrive", "OneDriveConsumer"):
        path = os.environ.get(var)
        if path and os.path.isdir(path) and all(path != r for r, _ in roots):
            roots.append((path, _ONEDRIVE_SEARCH_DEPTH))
    # SharePoint libraries synced with "Sync" land in <profile>\<Company name>
    profile = os.environ.get("USERPROFILE") or os.path.expanduser("~")
    if os.path.isdir(profile):
        roots.append((profile, _PROFILE_SEARCH_DEPTH))
    return roots


# ── Reading the list ──────────────────────────────────────────────────────────

def newest_file(folder: str) -> SharedFile | None:
    """Newest .xlsx/.csv in folder, ignoring Excel's "~$" lock files.
    Raises OSError if the folder can't be read."""
    files = []
    for entry in os.scandir(folder):
        name = entry.name
        if name.startswith(("~$", ".")) or not name.lower().endswith(LIST_EXTENSIONS):
            continue
        if entry.is_file():
            files.append(SharedFile(entry.path, entry.stat().st_mtime))
    return max(files, key=lambda f: f.modified, default=None)


def backup_info() -> dict | None:
    """{'name', 'modified'} of the saved backup copy, or None."""
    try:
        with open(_BACKUP_META, encoding="utf-8") as f:
            meta = json.load(f)
        if os.path.isfile(_backup_path(meta["name"])):
            return meta
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


def load_constituents(
    folder: str,
    warn_cb: Callable[[str], None] | None = None,
) -> tuple[set[str], dict[str, str], dict[str, str], str]:
    """Load the newest list in folder, falling back to the backup copy.
    Returns (cik_set, ticker_from_cik, index_from_cik, source_label)."""
    problem = ""
    try:
        newest = newest_file(folder)
        if newest is None:
            problem = f"there is no .xlsx or .csv file in {folder}"
    except OSError as exc:
        newest = None
        problem = f"the shared folder can't be reached ({exc})"

    if newest is not None:
        try:
            with open(newest.path, "rb") as f:
                data = f.read()
            result = _parse(data, newest.name, warn_cb)
        except requests.RequestException:
            raise  # SEC unreachable: the backup would fail the same way
        except Exception as exc:  # locked, not synced yet, or not a valid list
            problem = f"{newest.name} couldn't be used ({exc})"
        else:
            _warn_if_stale(newest.name, newest.modified, warn_cb)
            _save_backup(data, newest)
            return (*result, newest.name)

    meta = backup_info()
    if meta is None:
        raise SharedListError(f"{problem[0].upper()}{problem[1:]}, and there is no saved copy yet.")
    with open(_backup_path(meta["name"]), "rb") as f:
        data = f.read()
    _log(
        warn_cb,
        f"Using your last saved copy of {meta['name']} "
        f"(file dated {format_date(meta['modified'])}) because {problem}.",
    )
    result = _parse(data, meta["name"], warn_cb)
    _warn_if_stale(meta["name"], meta["modified"], warn_cb)
    return (*result, f"{meta['name']} (saved copy)")


def _parse(data: bytes, name: str, warn_cb) -> tuple[set[str], dict[str, str], dict[str, str]]:
    buf = io.BytesIO(data)
    buf.name = name  # load_from_excel picks CSV vs Excel by extension
    cik_set, ticker_from_cik, index_from_cik = russell.load_from_excel(buf, warn_cb=warn_cb)
    if not cik_set:
        raise ValueError("no companies found in it")
    return cik_set, ticker_from_cik, index_from_cik


def _warn_if_stale(name: str, modified: float, warn_cb) -> None:
    if is_stale(modified):
        days = int((time.time() - modified) / 86400)
        _log(
            warn_cb,
            f"The constituent list {name} is {days} days old. "
            "Has there been a rebalance? Save a fresh FactSet export to the shared folder.",
        )


def _backup_path(name: str) -> str:
    return os.path.join(SHARED_LIST_BACKUP_DIR, "last_good" + os.path.splitext(name)[1].lower())


def _save_backup(data: bytes, source: SharedFile) -> None:
    os.makedirs(SHARED_LIST_BACKUP_DIR, exist_ok=True)
    _atomic_write(_backup_path(source.name), data)
    meta = {"name": source.name, "modified": source.modified}
    _atomic_write(_BACKUP_META, json.dumps(meta).encode("utf-8"))
    # Drop the copy of the other format, if the team switched between CSV/Excel
    for ext in LIST_EXTENSIONS:
        other = os.path.join(SHARED_LIST_BACKUP_DIR, "last_good" + ext)
        if other != _backup_path(source.name) and os.path.exists(other):
            os.remove(other)


def _atomic_write(path: str, data: bytes) -> None:
    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _log(cb, msg):
    if cb:
        cb(msg)

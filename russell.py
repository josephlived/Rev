"""
Russell 3000 index management.

Downloads the iShares IWV ETF holdings CSV to get tickers, cross-references
with the SEC company_tickers.json to resolve CIKs, and caches the result
locally so we don't hammer remote servers on every run.
"""
import io
import json
import os
import time
from datetime import datetime, timezone
from typing import Callable

import pandas as pd
import requests

from config import (
    CACHE_DIR,
    COMPANY_TICKERS_URL,
    ISHARES_IWV_URL,
    RUSSELL_CACHE_MAX_AGE_DAYS,
    RUSSELL_CACHE_PATH,
    SEC_USER_AGENT,
)


# ── Public API ────────────────────────────────────────────────────────────────

def load_from_excel(
    file_obj,
    progress_cb: Callable[[str], None] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    """
    Load a constituent list from an uploaded Excel or CSV file.

    The file must have a Ticker column (matched case-insensitively against
    'ticker' or 'symbol').  An optional Index column ('index', 'index name',
    'group', 'etf') is used to populate the index_name display column.

    Returns (ticker_cik_map, ticker_index_map) where both are {TICKER: str}.
    """
    name = getattr(file_obj, "name", "")
    if name.lower().endswith(".csv"):
        df = pd.read_csv(file_obj, dtype=str)
    else:
        df = pd.read_excel(file_obj, dtype=str)

    # Normalise column names for lookup
    col_lower = {c.lower().strip(): c for c in df.columns}

    # Find ticker column
    ticker_col = next(
        (col_lower[k] for k in ("ticker", "symbol") if k in col_lower), None
    )
    if ticker_col is None:
        raise ValueError(
            "Could not find a 'Ticker' or 'Symbol' column in the uploaded file. "
            f"Columns found: {list(df.columns)}"
        )

    # Find optional index column
    index_col = next(
        (col_lower[k] for k in ("index", "index name", "group", "etf") if k in col_lower),
        None,
    )

    tickers = set(df[ticker_col].dropna().str.upper().str.strip())
    tickers.discard("")

    _log(progress_cb, "Downloading SEC company → CIK map…")
    cik_map = _fetch_sec_cik_map()

    ticker_cik_map = {}
    missed = []
    for ticker in sorted(tickers):
        cik = cik_map.get(ticker)
        if cik:
            ticker_cik_map[ticker] = cik
        else:
            missed.append(ticker)
    if missed:
        print(f"[excel] {len(missed)} tickers not found in SEC map: {missed[:10]}")

    ticker_index_map: dict[str, str] = {}
    if index_col:
        for _, row in df.iterrows():
            t = str(row[ticker_col]).upper().strip() if pd.notna(row[ticker_col]) else ""
            idx = str(row[index_col]).strip() if pd.notna(row[index_col]) else ""
            if t:
                ticker_index_map[t] = idx

    return ticker_cik_map, ticker_index_map


def load_russell_ciks(
    force_refresh: bool = False,
    progress_cb: Callable[[str], None] | None = None,
) -> dict[str, str]:
    """
    Return a dict mapping ticker (uppercase) → zero-padded 10-digit CIK string
    for all companies in the Russell 3000.

    Results are cached in RUSSELL_CACHE_PATH for RUSSELL_CACHE_MAX_AGE_DAYS days.
    Pass force_refresh=True to bypass the cache.
    """
    if not force_refresh and _cache_is_fresh():
        return _read_cache()

    _log(progress_cb, "Downloading Russell 3000 holdings from iShares IWV\u2026")
    russell_tickers = _fetch_russell_tickers()

    _log(progress_cb, "Downloading SEC company \u2192 CIK map\u2026")
    cik_map = _fetch_sec_cik_map()

    _log(progress_cb, "Merging ticker lists and writing cache\u2026")
    mapping = _merge(russell_tickers, cik_map)
    _write_cache(mapping)

    return mapping


def cache_info() -> dict:
    """Return cache metadata for display in the UI."""
    if not os.path.exists(RUSSELL_CACHE_PATH):
        return {"exists": False, "age_days": None, "ticker_count": 0}
    with open(RUSSELL_CACHE_PATH) as f:
        data = json.load(f)
    ts = data.get("timestamp", 0)
    age_days = (time.time() - ts) / 86400
    return {
        "exists": True,
        "age_days": round(age_days, 1),
        "ticker_count": len(data.get("mapping", {})),
        "updated_at": datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        ),
    }


# ── Internal helpers ──────────────────────────────────────────────────────────

def _log(cb, msg):
    if cb:
        cb(msg)


def _cache_is_fresh() -> bool:
    if not os.path.exists(RUSSELL_CACHE_PATH):
        return False
    with open(RUSSELL_CACHE_PATH) as f:
        data = json.load(f)
    age_days = (time.time() - data.get("timestamp", 0)) / 86400
    return age_days < RUSSELL_CACHE_MAX_AGE_DAYS


def _read_cache() -> dict[str, str]:
    with open(RUSSELL_CACHE_PATH) as f:
        data = json.load(f)
    return data["mapping"]


def _write_cache(mapping: dict[str, str]) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(RUSSELL_CACHE_PATH, "w") as f:
        json.dump({"timestamp": time.time(), "mapping": mapping}, f)


def _fetch_russell_tickers() -> set[str]:
    """Download iShares IWV holdings and return a set of uppercase tickers."""
    headers = {"User-Agent": SEC_USER_AGENT}
    resp = requests.get(ISHARES_IWV_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    lines = resp.text.splitlines()
    # The CSV has fund metadata rows before the real column header.
    # The real header row starts with "Ticker".
    try:
        header_idx = next(
            i for i, line in enumerate(lines) if line.startswith("Ticker")
        )
    except StopIteration:
        raise ValueError(
            "Could not find 'Ticker' header row in iShares IWV CSV. "
            "The file format may have changed."
        )

    df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])))

    # Drop cash/futures/non-equity rows (Ticker is blank, "-", or "CASH")
    df = df[df["Ticker"].notna()]
    df = df[~df["Ticker"].str.strip().isin(["-", "CASH", ""])]

    return set(df["Ticker"].str.upper().str.strip())


def _fetch_sec_cik_map() -> dict[str, str]:
    """
    Download SEC's company_tickers.json and return {TICKER: zero-padded CIK}.
    """
    headers = {"User-Agent": SEC_USER_AGENT}
    resp = requests.get(COMPANY_TICKERS_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    # Structure: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "..."}, ...}
    return {
        entry["ticker"].upper(): str(entry["cik_str"]).zfill(10)
        for entry in raw.values()
    }


def _merge(russell_tickers: set[str], cik_map: dict[str, str]) -> dict[str, str]:
    """Inner-join Russell tickers with SEC CIK map."""
    mapping = {}
    missed = []
    for ticker in sorted(russell_tickers):
        cik = cik_map.get(ticker)
        if cik:
            mapping[ticker] = cik
        else:
            missed.append(ticker)
    if missed:
        print(
            f"[russell] {len(missed)} tickers not found in SEC map "
            f"(likely non-equity/cash rows): {missed[:10]}{'\u2026' if len(missed)>10 else ''}"
        )
    return mapping

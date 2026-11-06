"""
Russell 3000 / constituent list management.

All public functions return the same 3-tuple so callers can treat the
iShares fallback and a user-uploaded file identically:

    (cik_set, ticker_from_cik, index_from_cik)

    cik_set          – set of str(int(cik)) strings for filtering
    ticker_from_cik  – {str(int(cik)): ticker_symbol}
    index_from_cik   – {str(int(cik)): index_name}  (empty when not available)
"""
import io
import json
import os
import re
import tempfile
import time
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from config import (
    CACHE_DIR,
    COMPANY_TICKERS_URL,
    ISHARES_IWV_URL,
    ISHARES_USER_AGENT,
    RUSSELL_CACHE_MAX_AGE_DAYS,
    RUSSELL_CACHE_PATH,
    RUSSELL_MIN_COMPANIES,
    SEC_USER_AGENT,
)

_PH_TZ = ZoneInfo("Asia/Manila")


class RussellSourceError(RuntimeError):
    """The iShares holdings download could not be turned into a usable list."""


def normalize_ticker(raw) -> str:
    """
    Normalize share-class separators to SEC's spelling so lookups match:
    'BRK B' (iShares), 'BRK.B' and 'BRK/B' (other vendors) → 'BRK-B'.
    """
    return re.sub(r"[\s./]+", "-", str(raw).strip().upper())


# ── Public API ────────────────────────────────────────────────────────────────

def load_from_excel(
    file_obj,
    progress_cb: Callable[[str], None] | None = None,
    warn_cb: Callable[[str], None] | None = None,
) -> tuple[set[str], dict[str, str], dict[str, str]]:
    """
    Load a constituent list from an uploaded Excel or CSV file.

    Column detection (all case-insensitive):
      - CIK column  : "cik", "entity id", "entity_id"  ← preferred
      - Ticker column: "ticker", "symbol"               ← fallback
      - Index column : "index name", "index", "group", "etf"  ← optional

    Returns (cik_set, ticker_from_cik, index_from_cik).
    """
    name = getattr(file_obj, "name", "")
    if name.lower().endswith(".csv"):
        df = pd.read_csv(file_obj, dtype=str)
    else:
        df = pd.read_excel(file_obj, dtype=str)

    col_lower = {c.lower().strip(): c for c in df.columns}

    # ── Find identifier column ────────────────────────────────────────────────
    cik_col = next(
        (col_lower[k] for k in ("cik", "entity id", "entity_id") if k in col_lower),
        None,
    )
    ticker_col = next(
        (col_lower[k] for k in ("ticker", "symbol") if k in col_lower),
        None,
    )

    if cik_col is None and ticker_col is None:
        raise ValueError(
            "Could not find a 'CIK', 'Ticker', or 'Symbol' column in the uploaded file. "
            f"Columns found: {list(df.columns)}"
        )

    # ── Find optional index column ────────────────────────────────────────────
    index_col = next(
        (col_lower[k] for k in ("index name", "index", "group", "etf") if k in col_lower),
        None,
    )

    _log(progress_cb, "Downloading SEC company → CIK map…")
    sec_cik_map = _fetch_sec_cik_map()                    # {TICKER: padded_cik}
    sec_ticker_map = {v: k for k, v in sec_cik_map.items()}  # {padded_cik: TICKER}

    if cik_col is not None:
        # ── CIK-first path ────────────────────────────────────────────────────
        raw_ciks = df[cik_col].dropna().astype(str).str.strip()
        cik_set: set[str] = set()
        index_from_cik: dict[str, str] = {}
        ticker_from_cik: dict[str, str] = {}

        for idx, row in df.iterrows():
            raw = str(row[cik_col]).strip() if pd.notna(row[cik_col]) else ""
            if not raw or raw.lower() == "nan":
                continue
            try:
                norm = str(int(float(raw)))  # "0000712034" or "712034.0" → "712034"
            except ValueError:
                continue
            cik_set.add(norm)
            # index name
            if index_col and pd.notna(row.get(index_col, None)):
                index_from_cik[norm] = str(row[index_col]).strip()
            # ticker — prefer file's ticker column, else reverse-lookup
            padded = norm.zfill(10)
            if ticker_col and pd.notna(row.get(ticker_col, None)):
                ticker_from_cik[norm] = normalize_ticker(row[ticker_col])
            elif padded in sec_ticker_map:
                ticker_from_cik[norm] = sec_ticker_map[padded]

        return cik_set, ticker_from_cik, index_from_cik

    else:
        # ── Ticker-first path ─────────────────────────────────────────────────
        tickers = {normalize_ticker(t) for t in df[ticker_col].dropna()} - {""}
        cik_set = set()
        ticker_from_cik = {}
        index_from_cik = {}
        missed = []

        for ticker in sorted(tickers):
            padded = sec_cik_map.get(ticker)
            if not padded:
                missed.append(ticker)
                continue
            norm = str(int(padded))
            cik_set.add(norm)
            ticker_from_cik[norm] = ticker

        if missed:
            _log(
                warn_cb,
                f"{len(missed)} ticker(s) in your file were not found in SEC's ticker list "
                f"and were skipped: {', '.join(missed[:20])}{'…' if len(missed) > 20 else ''}",
            )

        if index_col:
            for _, row in df.iterrows():
                t = normalize_ticker(row[ticker_col]) if pd.notna(row[ticker_col]) else ""
                padded = sec_cik_map.get(t)
                if not padded:
                    continue
                norm = str(int(padded))
                idx_val = str(row[index_col]).strip() if pd.notna(row.get(index_col)) else ""
                index_from_cik[norm] = idx_val

        return cik_set, ticker_from_cik, index_from_cik


def load_russell_ciks(
    force_refresh: bool = False,
    progress_cb: Callable[[str], None] | None = None,
    warn_cb: Callable[[str], None] | None = None,
) -> tuple[set[str], dict[str, str], dict[str, str]]:
    """
    Download / load from cache the iShares IWV (Russell 3000) constituent list.
    Returns (cik_set, ticker_from_cik, {}).

    A failed download never replaces the cache. On a forced refresh the error
    is raised; otherwise a stale cache is used and the reason sent to warn_cb.
    """
    cached = _load_cache_data()
    if cached and not force_refresh and _cache_age_days(cached) < RUSSELL_CACHE_MAX_AGE_DAYS:
        mapping = cached["mapping"]      # {ticker: padded_cik}
    else:
        try:
            mapping = _download_and_cache(progress_cb)
        except (requests.RequestException, RussellSourceError, ValueError) as exc:
            if force_refresh or not cached:
                raise
            _log(
                warn_cb,
                f"Could not refresh the Russell 3000 list ({exc}). Using the saved list "
                f"from {_format_ts(cached['timestamp'])} "
                f"({_cache_age_days(cached):.0f} days old) instead.",
            )
            mapping = cached["mapping"]

    cik_set: set[str] = set()
    ticker_from_cik: dict[str, str] = {}
    for ticker, padded in mapping.items():
        norm = str(int(padded))
        cik_set.add(norm)
        ticker_from_cik[norm] = ticker

    return cik_set, ticker_from_cik, {}


def cache_info() -> dict:
    """Return cache metadata for display in the UI."""
    data = _load_cache_data()
    if data is None:
        return {"exists": False, "age_days": None, "ticker_count": 0, "unmatched": []}
    return {
        "exists": True,
        "age_days": round(_cache_age_days(data), 1),
        "ticker_count": len(data["mapping"]),
        "updated_at": _format_ts(data["timestamp"]),
        # [[ticker, name], ...] — absent in caches written before this was tracked
        "unmatched": data.get("unmatched", []),
    }


# ── Internal helpers ──────────────────────────────────────────────────────────

def _log(cb, msg):
    if cb:
        cb(msg)


def _format_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=_PH_TZ).strftime("%Y-%m-%d %H:%M PHT")


def _cache_age_days(data: dict) -> float:
    return (time.time() - data["timestamp"]) / 86400


def _load_cache_data() -> dict | None:
    """Return the parsed cache, or None if it is missing or unreadable."""
    try:
        with open(RUSSELL_CACHE_PATH) as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or not isinstance(data.get("mapping"), dict):
        return None
    if not isinstance(data.get("timestamp"), (int, float)):
        data["timestamp"] = 0  # unknown age → treat as stale
    return data


def _write_cache(mapping: dict[str, str], unmatched: list[list[str]]) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    # Write to a temp file then swap it in, so a crash or a second user
    # refreshing at the same time can never leave a half-written cache
    fd, tmp_path = tempfile.mkstemp(dir=CACHE_DIR, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(
                {"timestamp": time.time(), "mapping": mapping, "unmatched": unmatched}, f
            )
        os.replace(tmp_path, RUSSELL_CACHE_PATH)
    except BaseException:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _download_and_cache(progress_cb) -> dict[str, str]:
    _log(progress_cb, "Downloading Russell 3000 holdings from iShares IWV…")
    holdings = _fetch_russell_holdings()
    _log(progress_cb, "Downloading SEC company → CIK map…")
    cik_map = _fetch_sec_cik_map()
    _log(progress_cb, "Merging and caching…")
    mapping, unmatched = _merge(holdings, cik_map)
    company_count = len(set(mapping.values()))
    if company_count < RUSSELL_MIN_COMPANIES:
        raise RussellSourceError(
            f"only {company_count:,} companies could be matched to an SEC CIK "
            f"(expected at least {RUSSELL_MIN_COMPANIES:,}), so the download looks "
            "incomplete. The previously saved list was kept."
        )
    _write_cache(mapping, unmatched)
    return mapping


def _fetch_russell_holdings() -> dict[str, str]:
    """Return {normalized_ticker: company_name} for IWV's equity holdings."""
    resp = requests.get(
        ISHARES_IWV_URL, headers={"User-Agent": ISHARES_USER_AGENT}, timeout=30
    )
    resp.raise_for_status()
    text = resp.text.lstrip("﻿")
    # When the link moved, iShares answered 200 with its HTML product page while
    # still labelling it text/csv, so check the body rather than the headers
    if "<html" in text[:2000].lower():
        raise RussellSourceError(
            "iShares returned a web page instead of the holdings CSV. The download "
            "link has probably changed; update ISHARES_IWV_URL in config.py."
        )
    lines = text.splitlines()
    header_idx = next(
        (i for i, line in enumerate(lines) if line.startswith("Ticker,")), None
    )
    if header_idx is None:
        raise RussellSourceError(
            "Could not find the 'Ticker' header row in the iShares IWV CSV. "
            "The file format may have changed."
        )
    df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])), dtype=str)
    missing = {"Ticker", "Name", "Asset Class"} - set(df.columns)
    if missing:
        raise RussellSourceError(
            f"The iShares IWV CSV is missing expected column(s): {', '.join(sorted(missing))}."
        )
    # Keep only stock holdings: drops futures, cash, money-market rows and
    # placeholder rows with no ticker ("-")
    df = df[df["Asset Class"].str.strip() == "Equity"]
    df = df[df["Ticker"].notna()]
    df = df[~df["Ticker"].str.strip().isin(["-", ""])]
    return {
        normalize_ticker(ticker): str(name).strip()
        for ticker, name in zip(df["Ticker"], df["Name"])
    }


def _fetch_sec_cik_map() -> dict[str, str]:
    """Return {TICKER: zero-padded-10-digit-CIK}."""
    headers = {"User-Agent": SEC_USER_AGENT}
    resp = requests.get(COMPANY_TICKERS_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    return {
        normalize_ticker(entry["ticker"]): str(entry["cik_str"]).zfill(10)
        for entry in raw.values()
    }


def _merge(
    holdings: dict[str, str], cik_map: dict[str, str]
) -> tuple[dict[str, str], list[list[str]]]:
    """Return ({ticker: padded_cik}, [[ticker, name], ...] with no SEC match)."""
    mapping = {}
    unmatched = []
    for ticker in sorted(holdings):
        cik = cik_map.get(ticker)
        if cik:
            mapping[ticker] = cik
        else:
            unmatched.append([ticker, holdings[ticker]])
    return mapping, unmatched

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
    RUSSELL_CACHE_MAX_AGE_DAYS,
    RUSSELL_CACHE_PATH,
    SEC_USER_AGENT,
)

_PH_TZ = ZoneInfo("Asia/Manila")


# ── Public API ────────────────────────────────────────────────────────────────

def load_from_excel(
    file_obj,
    progress_cb: Callable[[str], None] | None = None,
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
                ticker_from_cik[norm] = str(row[ticker_col]).strip().upper()
            elif padded in sec_ticker_map:
                ticker_from_cik[norm] = sec_ticker_map[padded]

        return cik_set, ticker_from_cik, index_from_cik

    else:
        # ── Ticker-first path ─────────────────────────────────────────────────
        tickers = set(df[ticker_col].dropna().str.upper().str.strip()) - {""}
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
            print(f"[excel] {len(missed)} tickers not found in SEC map: {missed[:10]}")

        if index_col:
            for _, row in df.iterrows():
                t = str(row[ticker_col]).upper().strip() if pd.notna(row[ticker_col]) else ""
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
) -> tuple[set[str], dict[str, str], dict[str, str]]:
    """
    Download / load from cache the iShares IWV (Russell 3000) constituent list.
    Returns (cik_set, ticker_from_cik, {}).
    """
    if not force_refresh and _cache_is_fresh():
        mapping = _read_cache()          # {ticker: padded_cik}
    else:
        _log(progress_cb, "Downloading Russell 3000 holdings from iShares IWV…")
        russell_tickers = _fetch_russell_tickers()
        _log(progress_cb, "Downloading SEC company → CIK map…")
        cik_map = _fetch_sec_cik_map()
        _log(progress_cb, "Merging and caching…")
        mapping = _merge(russell_tickers, cik_map)
        _write_cache(mapping)

    cik_set: set[str] = set()
    ticker_from_cik: dict[str, str] = {}
    for ticker, padded in mapping.items():
        norm = str(int(padded))
        cik_set.add(norm)
        ticker_from_cik[norm] = ticker

    return cik_set, ticker_from_cik, {}


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
        "updated_at": datetime.fromtimestamp(ts, tz=_PH_TZ).strftime(
            "%Y-%m-%d %H:%M PHT"
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
    headers = {"User-Agent": SEC_USER_AGENT}
    resp = requests.get(ISHARES_IWV_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    lines = resp.text.splitlines()
    try:
        header_idx = next(i for i, line in enumerate(lines) if line.startswith("Ticker"))
    except StopIteration:
        raise ValueError(
            "Could not find 'Ticker' header row in iShares IWV CSV. "
            "The file format may have changed."
        )
    df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])))
    df = df[df["Ticker"].notna()]
    df = df[~df["Ticker"].str.strip().isin(["-", "CASH", ""])]
    return set(df["Ticker"].str.upper().str.strip())


def _fetch_sec_cik_map() -> dict[str, str]:
    """Return {TICKER: zero-padded-10-digit-CIK}."""
    headers = {"User-Agent": SEC_USER_AGENT}
    resp = requests.get(COMPANY_TICKERS_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    return {
        entry["ticker"].upper(): str(entry["cik_str"]).zfill(10)
        for entry in raw.values()
    }


def _merge(russell_tickers: set[str], cik_map: dict[str, str]) -> dict[str, str]:
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
            f"[russell] {len(missed)} tickers not in SEC map "
            f"(non-equity rows): {missed[:10]}{'…' if len(missed) > 10 else ''}"
        )
    return mapping

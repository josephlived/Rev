"""
EDGAR data access and DEF 14A parsing.

Covers:
  - Fetching the daily filing index for a specific date
  - Filtering by form type and Russell 3000 CIK set
  - Fetching DEF 14A primary documents
  - Parsing meeting type and date from proxy statement text
"""
import io
import re
import time
from datetime import date
from typing import Callable

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import (
    DEF14A_FETCH_BYTES,
    EDGAR_ARCHIVE_BASE,
    EDGAR_DAILY_INDEX_URL,
    RATE_LIMIT_RPS,
    SEC_USER_AGENT,
    TARGET_FORMS,
)

# ── Regex patterns for meeting type / date detection ──────────────────────────

_ANNUAL_SPECIAL_RE = re.compile(
    r"annual\s+and\s+special\s+meeting|special\s+and\s+annual\s+meeting",
    re.IGNORECASE,
)
_ANNUAL_RE = re.compile(r"\bannual\s+(?:general\s+)?meeting\b", re.IGNORECASE)
_SPECIAL_RE = re.compile(r"\bspecial\s+(?:general\s+)?meeting\b", re.IGNORECASE)

# Matches "May 12, 2026", "June 3, 2026", etc.
_MONTH_DATE_RE = re.compile(
    r"(?:January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+\d{1,2},?\s+\d{4}",
    re.IGNORECASE,
)

# Tries to find a date that closely follows "annual meeting … held on …"
_MEETING_DATE_CONTEXT_RE = re.compile(
    r"annual\s+meeting\b[^.]{0,150}?"
    r"(?:to\s+be\s+held\s+(?:on\s+)?|scheduled\s+for\s+(?:\w+,\s+)?|on\s+)"
    r"((?:January|February|March|April|May|June|July|August|"
    r"September|October|November|December)"
    r"\.?\s+\d{1,2},?\s+\d{4})",
    re.IGNORECASE,
)


# ── Rate limiter ──────────────────────────────────────────────────────────────

_last_request_ts: float = 0.0
_min_interval: float = 1.0 / RATE_LIMIT_RPS  # seconds between requests


def _get(session: requests.Session, url: str, **kwargs) -> requests.Response:
    """GET with per-request rate limiting and the required User-Agent header."""
    global _last_request_ts
    elapsed = time.monotonic() - _last_request_ts
    if elapsed < _min_interval:
        time.sleep(_min_interval - elapsed)
    resp = session.get(
        url,
        headers={"User-Agent": SEC_USER_AGENT},
        timeout=30,
        **kwargs,
    )
    _last_request_ts = time.monotonic()
    resp.raise_for_status()
    return resp


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_daily_index(filing_date: date, session: requests.Session) -> pd.DataFrame:
    """
    Download the EDGAR daily filing index for filing_date and return a
    DataFrame with columns:
        form_type, company_name, cik, date_filed, filename
    Raises a user-friendly ValueError if no index exists (e.g. weekend/holiday).
    """
    quarter = (filing_date.month - 1) // 3 + 1
    url = EDGAR_DAILY_INDEX_URL.format(
        year=filing_date.year,
        quarter=quarter,
        date=filing_date.strftime("%Y%m%d"),
    )
    try:
        resp = _get(session, url)
    except requests.HTTPError as exc:
        if exc.response.status_code == 404:
            raise ValueError(
                f"No EDGAR filing index found for {filing_date}. "
                "The market may have been closed (weekend/holiday) or "
                "no filings were submitted."
            ) from exc
        raise

    # The .idx file has a variable-length header (metadata + blanks + two-line
    # column header + dashed separator).  Locate the separator dynamically so
    # we never depend on a hardcoded line count.
    lines = resp.text.splitlines()
    sep_idx = next(
        (i for i, line in enumerate(lines) if line.startswith("---")), None
    )
    if sep_idx is None:
        raise ValueError(
            "Could not parse EDGAR index file: separator line not found. "
            "The file format may have changed."
        )
    data_lines = [line for line in lines[sep_idx + 1:] if line.strip()]
    content = io.StringIO("\n".join(data_lines))
    try:
        df = pd.read_fwf(
            content,
            header=None,
            names=["form_type", "company_name", "cik", "date_filed", "filename"],
            dtype=str,
        )
    except Exception as exc:
        raise ValueError(f"Failed to parse EDGAR index file: {exc}") from exc

    df = df.dropna(subset=["form_type", "cik"])
    df["form_type"] = df["form_type"].str.strip()
    df["cik"] = df["cik"].str.strip()
    df["company_name"] = df["company_name"].str.strip()
    df["filename"] = df["filename"].str.strip()
    return df


def filter_filings(
    df: pd.DataFrame,
    russell_cik_set: set[str],
    target_forms: set[str] | None = None,
) -> pd.DataFrame:
    """
    Keep rows where form_type is in target_forms AND the CIK (stripped of
    leading zeros) belongs to the Russell 3000 set.
    """
    if target_forms is None:
        target_forms = TARGET_FORMS

    def _strip(cik: str) -> str:
        try:
            return str(int(cik))
        except (ValueError, TypeError):
            return cik

    df = df.copy()
    df["_cik_norm"] = df["cik"].apply(_strip)
    russell_norm = {_strip(c) for c in russell_cik_set}

    mask = df["form_type"].isin(target_forms) & df["_cik_norm"].isin(russell_norm)
    return df[mask].drop(columns=["_cik_norm"]).reset_index(drop=True)


def enrich_with_ticker(
    df: pd.DataFrame,
    ticker_from_cik: dict[str, str],
    index_from_cik: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Add 'ticker' and 'index_name' columns using CIK-keyed lookup dicts."""
    df = df.copy()
    norm = df["cik"].apply(lambda c: str(int(c)))
    df["ticker"] = norm.map(ticker_from_cik or {}).fillna("")
    df["index_name"] = norm.map(index_from_cik or {}).fillna("")
    return df


def parse_def14a_filings(
    df: pd.DataFrame,
    session: requests.Session,
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> pd.DataFrame:
    """
    For every DEF 14A row in df, fetch the primary document and extract
    meeting_type and meeting_date.  Returns df with those two columns added
    (empty strings for non-DEF14A rows).
    """
    df = df.copy()
    df["meeting_type"] = ""
    df["meeting_date"] = ""

    def14a_mask = df["form_type"] == "DEF 14A"
    total = def14a_mask.sum()

    for i, idx in enumerate(df[def14a_mask].index):
        row = df.loc[idx]
        company = row["company_name"]
        filename = row["filename"]

        if progress_cb:
            progress_cb(f"Parsing DEF 14A: {company}", i + 1, total)

        try:
            html = _fetch_def14a_text(filename, row["cik"], session)
            info = _parse_meeting_info(html)
            df.at[idx, "meeting_type"] = info["meeting_type"]
            df.at[idx, "meeting_date"] = info["meeting_date"]
        except Exception as exc:
            df.at[idx, "meeting_type"] = "Error"
            df.at[idx, "meeting_date"] = str(exc)[:80]

    return df


# ── DEF 14A fetch ─────────────────────────────────────────────────────────────

def _fetch_def14a_text(filename: str, cik: str, session: requests.Session) -> str:
    stem = filename.rsplit("/", 1)[-1].replace(".txt", "")
    accession_dashes = stem
    accession_nodash = accession_dashes.replace("-", "")
    cik_clean = str(int(cik))

    index_url = (
        f"{EDGAR_ARCHIVE_BASE}edgar/data/{cik_clean}/{accession_nodash}/"
        f"{accession_dashes}-index.htm"
    )
    try:
        idx_resp = _get(session, index_url)
    except requests.HTTPError:
        raw_url = EDGAR_ARCHIVE_BASE + filename
        return _fetch_partial(raw_url, session)

    soup = BeautifulSoup(idx_resp.text, "lxml")
    doc_url = _find_primary_doc_url(soup, cik_clean, accession_nodash)

    if doc_url:
        return _fetch_partial(doc_url, session)

    raw_url = EDGAR_ARCHIVE_BASE + filename
    return _fetch_partial(raw_url, session)


def _find_primary_doc_url(
    soup: BeautifulSoup, cik: str, accession_nodash: str
) -> str | None:
    # Index table columns: Seq | Description | Document | Type | Size
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        doc_type = cells[3].get_text(strip=True).upper()
        if doc_type in ("DEF 14A", "DEF14A"):
            link = cells[2].find("a")
            if link and link.get("href"):
                href = link["href"]
                # Unwrap inline XBRL viewer: /ix?doc=/Archives/edgar/.../file.htm
                if "/ix?doc=" in href:
                    href = href.split("doc=", 1)[1]
                if href.startswith("http"):
                    return href
                # Root-relative path e.g. /Archives/edgar/data/.../file.htm
                return "https://www.sec.gov" + href
    return None


def _fetch_partial(url: str, session: requests.Session) -> str:
    """Fetch only the first DEF14A_FETCH_BYTES of a URL (streaming)."""
    resp = session.get(
        url,
        headers={"User-Agent": SEC_USER_AGENT},
        stream=True,
        timeout=30,
    )
    _last_request_ts_update()
    resp.raise_for_status()
    chunks = []
    total = 0
    for chunk in resp.iter_content(chunk_size=8192):
        chunks.append(chunk)
        total += len(chunk)
        if total >= DEF14A_FETCH_BYTES:
            break
    resp.close()
    raw = b"".join(chunks)[:DEF14A_FETCH_BYTES]
    return raw.decode("utf-8", errors="replace")


def _last_request_ts_update():
    global _last_request_ts
    _last_request_ts = time.monotonic()


# ── Meeting info parsing ──────────────────────────────────────────────────────

def _parse_meeting_info(html_text: str) -> dict:
    try:
        text = BeautifulSoup(html_text, "lxml").get_text(" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html_text)
    text = re.sub(r"\s+", " ", text)

    has_annual_special = bool(_ANNUAL_SPECIAL_RE.search(text))
    has_annual = bool(_ANNUAL_RE.search(text))
    has_special = bool(_SPECIAL_RE.search(text))

    if has_annual_special or (has_annual and has_special):
        meeting_type = "Annual + Special"
    elif has_annual:
        meeting_type = "Annual"
    elif has_special:
        meeting_type = "Special"
    else:
        meeting_type = "Other"

    meeting_date = ""
    if "Annual" in meeting_type:
        meeting_date = _extract_annual_meeting_date(text)

    return {"meeting_type": meeting_type, "meeting_date": meeting_date}


def _extract_annual_meeting_date(text: str) -> str:
    m = _MEETING_DATE_CONTEXT_RE.search(text)
    if m:
        return m.group(1).strip().title()

    annual_match = _ANNUAL_RE.search(text)
    if annual_match:
        snippet = text[annual_match.start(): annual_match.start() + 400]
        m2 = _MONTH_DATE_RE.search(snippet)
        if m2:
            return m2.group(0).strip().title()

    return ""

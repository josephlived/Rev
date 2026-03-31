"""
EDGAR data access and DEF 14A parsing.

Covers:
  - Fetching the daily filing index for a specific date
  - Filtering by form type and Russell 3000 CIK set
  - Fetching DEF 14A primary documents
  - Parsing meeting type and date from proxy statement text
"""
import json as _json
import re
import time
from datetime import date, datetime
from typing import Callable

try:
    import anthropic as _anthropic
    _ANTHROPIC_AVAILABLE = True
except ImportError:
    _ANTHROPIC_AVAILABLE = False

_ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
PARSING_MODE_REGEX = "Regex Parsing"
PARSING_MODE_API = "API Parsing"
PARSING_MODE_HYBRID = "Hybrid"
PARSING_MODES = [PARSING_MODE_REGEX, PARSING_MODE_API, PARSING_MODE_HYBRID]
_CLAUDE_SNIPPET_TOTAL_CHARS = 5_000
_CLAUDE_SNIPPET_BEFORE_CHARS = 2_250
_CLAUDE_SNIPPET_AFTER_CHARS = 2_750
_CLAUDE_FALLBACK_CHARS = 12_000

_CLAUDE_PROMPT = """\
You are parsing a SEC DEF 14A proxy statement. Extract exactly two fields:

1. meeting_type — choose exactly one of: "Annual", "Special", "Annual + Special", "Extraordinary", "Other"
2. meeting_date — the shareholder meeting date as "Month D, YYYY" (e.g. "May 14, 2026"). Use "" if not found.

Reply with JSON only, no explanation or extra text:
{"meeting_type": "...", "meeting_date": "..."}

Proxy statement text:
"""

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
_EXTRAORDINARY_RE = re.compile(
    r"\bextraordinary\s+(?:general\s+)?meeting\b", re.IGNORECASE
)

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

_CLAUDE_SNIPPET_PATTERNS = [
    re.compile(r"annual\s+and\s+special\s+meeting", re.IGNORECASE),
    re.compile(r"special\s+and\s+annual\s+meeting", re.IGNORECASE),
    re.compile(r"notice\s+of\s+annual\s+meeting", re.IGNORECASE),
    re.compile(r"notice\s+of\s+special\s+meeting", re.IGNORECASE),
    re.compile(r"annual\s+meeting", re.IGNORECASE),
    re.compile(r"special\s+meeting", re.IGNORECASE),
    re.compile(r"extraordinary\s+meeting", re.IGNORECASE),
    re.compile(r"date,\s*time\s+and\s+place", re.IGNORECASE),
    re.compile(r"proxy\s+statement", re.IGNORECASE),
]

_IDX_TAIL_RE = re.compile(
    r"^(?P<company_name>.*?)\s+"
    r"(?P<cik>\d{1,10})\s+"
    r"(?P<date_filed>\d{8}|\d{4}-\d{2}-\d{2})\s+"
    r"(?P<filename>\S+)$"
)

# ── Rate limiter ──────────────────────────────────────────────────────────────

_last_request_ts: float = 0.0
_min_interval: float = 1.0 / RATE_LIMIT_RPS  # seconds between requests


class SecAccessError(RuntimeError):
    """Raised when SEC blocks access to a requested resource."""

    def __init__(self, url: str, status_code: int):
        self.url = url
        self.status_code = status_code
        super().__init__(f"SEC returned HTTP {status_code} for {url}")


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
    return fetch_daily_index_from_url(url, filing_date, session)


def fetch_daily_index_from_url(
    url: str,
    filing_date: date | None,
    session: requests.Session,
) -> pd.DataFrame:
    """Download and parse a daily index file from SEC."""
    try:
        resp = _get(session, url)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code
        if status_code == 403:
            raise SecAccessError(url, status_code) from exc
        if status_code == 404:
            label = f" for {filing_date}" if filing_date is not None else ""
            raise ValueError(
                f"No EDGAR filing index found{label}. "
                "The market may have been closed (weekend/holiday) or "
                "no filings were submitted."
            ) from exc
        raise
    return parse_daily_index_text(resp.text)


def parse_daily_index_text(raw_text: str) -> pd.DataFrame:
    """Parse the contents of a SEC form daily index file."""
    lines = raw_text.splitlines()
    sep_idx = next(
        (i for i, line in enumerate(lines) if line.startswith("---")), None
    )
    if sep_idx is None:
        raise ValueError(
            "Could not parse EDGAR index file: separator line not found. "
            "The file format may have changed."
        )
    rows = []
    for line in lines[sep_idx + 1:]:
        if not line.strip():
            continue
        form_type = line[:12].strip()
        tail = line[12:].strip()
        match = _IDX_TAIL_RE.match(tail)
        if not match:
            continue
        row = {"form_type": form_type, **match.groupdict()}
        if not all(row.values()):
            continue
        rows.append(row)

    if not rows:
        raise ValueError(
            "Failed to parse EDGAR index file: no filing rows matched the expected format."
        )

    df = pd.DataFrame(rows, columns=["form_type", "company_name", "cik", "date_filed", "filename"])
    df["form_type"] = df["form_type"].fillna("").astype(str).str.strip()
    df["form_type_normalized"] = df["form_type"].map(normalize_form_type)
    df["cik"] = df["cik"].str.strip()
    df["company_name"] = df["company_name"].str.strip()
    df["filename"] = df["filename"].str.strip()
    return df


def parse_daily_index_file(file_obj) -> pd.DataFrame:
    """Parse an uploaded SEC .idx file into the standard dataframe shape."""
    raw_bytes = file_obj.getvalue() if hasattr(file_obj, "getvalue") else file_obj.read()
    if isinstance(raw_bytes, str):
        raw_text = raw_bytes
    else:
        raw_text = raw_bytes.decode("utf-8", errors="replace")
    return parse_daily_index_text(raw_text)


def normalize_form_type(form_type: str) -> str:
    """Normalize form types for robust comparisons while preserving display values."""
    return re.sub(r"\s+", " ", str(form_type or "").strip()).upper()


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
    if "form_type_normalized" not in df.columns:
        df["form_type_normalized"] = df["form_type"].map(normalize_form_type)
    target_forms_norm = {normalize_form_type(form) for form in target_forms}

    mask = df["form_type_normalized"].isin(target_forms_norm) & df["_cik_norm"].isin(russell_norm)
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


def enrich_with_filing_url(
    df: pd.DataFrame,
    session: requests.Session,
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> pd.DataFrame:
    """Add a filing_url column that prefers HTML documents over raw .txt archives."""
    df = df.copy()
    filing_urls = []
    total = len(df)
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        if progress_cb:
            progress_cb(f"Resolving filing link: {row['company_name']}", i, total)
        filing_urls.append(resolve_filing_url(row["filename"], row["cik"], session))
    df["filing_url"] = filing_urls
    return df


def parse_def14a_filings(
    df: pd.DataFrame,
    session: requests.Session,
    progress_cb: Callable[[str, int, int], None] | None = None,
    parsing_mode: str = PARSING_MODE_HYBRID,
    api_key: str | None = None,
) -> pd.DataFrame:
    """
    For every DEF 14A row in df, fetch the primary document and extract
    meeting_type and meeting_date.  Returns df with those two columns added
    (empty strings for non-DEF14A rows).

    Parsing mode controls whether regex, API, or hybrid fallback logic is used.
    """
    df = df.copy()
    df["meeting_type"] = ""
    df["meeting_date"] = ""
    df["claude_error"] = ""
    df["parsing_method"] = ""

    def14a_mask = df["form_type_normalized"] == normalize_form_type("DEF 14A")
    total = def14a_mask.sum()
    api_enabled = bool(api_key and _ANTHROPIC_AVAILABLE)

    for i, idx in enumerate(df[def14a_mask].index):
        row = df.loc[idx]
        company = row["company_name"]
        filename = row["filename"]

        if progress_cb:
            progress_cb(f"Parsing DEF 14A: {company}", i + 1, total)

        try:
            html = _fetch_def14a_text(filename, row["cik"], session)
            regex_info = _parse_meeting_info(html)
            filing_date = _parse_filing_date(row.get("date_filed", ""))

            if parsing_mode == PARSING_MODE_REGEX or not api_enabled:
                info = regex_info
                method = "regex"
            elif parsing_mode == PARSING_MODE_API:
                claude_result, claude_err = _parse_with_claude(html, api_key)
                if claude_result:
                    info = claude_result
                    method = "api"
                else:
                    df.at[idx, "claude_error"] = claude_err or "unknown error"
                    info = regex_info
                    method = "regex-fallback"
            else:
                if _should_fallback_to_api(regex_info, filing_date):
                    claude_result, claude_err = _parse_with_claude(html, api_key)
                    if claude_result:
                        info = claude_result
                        method = "api-fallback"
                    else:
                        df.at[idx, "claude_error"] = claude_err or "unknown error"
                        info = regex_info
                        method = "regex-fallback"
                else:
                    info = regex_info
                    method = "regex"
            df.at[idx, "meeting_type"] = info["meeting_type"]
            df.at[idx, "meeting_date"] = info["meeting_date"]
            df.at[idx, "parsing_method"] = method
        except Exception as exc:
            df.at[idx, "meeting_type"] = "Error"
            df.at[idx, "meeting_date"] = str(exc)[:80]
            df.at[idx, "parsing_method"] = "error"

    return df


# ── DEF 14A fetch ─────────────────────────────────────────────────────────────

def _fetch_def14a_text(filename: str, cik: str, session: requests.Session) -> str:
    cik_clean, accession_nodash = _extract_accession_parts(filename, cik)
    index_url = _build_index_url(filename, cik)
    try:
        idx_resp = _get(session, index_url)
    except requests.HTTPError:
        raw_url = build_raw_filing_url(filename)
        return _fetch_partial(raw_url, session)

    soup = BeautifulSoup(idx_resp.text, "lxml")
    doc_url = _find_primary_doc_url(soup, cik_clean, accession_nodash)

    if doc_url:
        return _fetch_partial(doc_url, session)

    raw_url = build_raw_filing_url(filename)
    return _fetch_partial(raw_url, session)


def resolve_filing_url(filename: str, cik: str, session: requests.Session) -> str:
    """Resolve a filing URL that prefers the filing's primary HTML document."""
    raw_url = build_raw_filing_url(filename)
    try:
        index_url = _build_index_url(filename, cik)
    except Exception:
        return raw_url

    try:
        idx_resp = _get(session, index_url)
    except Exception:
        return raw_url

    soup = BeautifulSoup(idx_resp.text, "lxml")
    cik_clean, accession_nodash = _extract_accession_parts(filename, cik)
    doc_url = _find_primary_doc_url(soup, cik_clean, accession_nodash)
    return doc_url or raw_url


def build_raw_filing_url(filename: str) -> str:
    return EDGAR_ARCHIVE_BASE + filename


def _extract_accession_parts(filename: str, cik: str) -> tuple[str, str]:
    stem = filename.rsplit("/", 1)[-1].replace(".txt", "")
    accession_dashes = stem
    accession_nodash = accession_dashes.replace("-", "")
    cik_clean = str(int(cik))
    return cik_clean, accession_nodash


def _build_index_url(filename: str, cik: str) -> str:
    cik_clean, accession_nodash = _extract_accession_parts(filename, cik)
    accession_dashes = filename.rsplit("/", 1)[-1].replace(".txt", "")
    return (
        f"{EDGAR_ARCHIVE_BASE}edgar/data/{cik_clean}/{accession_nodash}/"
        f"{accession_dashes}-index.htm"
    )


def _find_primary_doc_url(
    soup: BeautifulSoup, cik: str, accession_nodash: str
) -> str | None:
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        doc_type = cells[3].get_text(strip=True).upper()
        if doc_type in ("DEF 14A", "DEF14A"):
            link = cells[2].find("a")
            if link and link.get("href"):
                href = link["href"]
                if "/ix?doc=" in href:
                    href = href.split("doc=", 1)[1]
                if href.startswith("http"):
                    return href
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
    has_extraordinary = bool(_EXTRAORDINARY_RE.search(text))

    type_is_ambiguous = False

    if has_annual_special or (has_annual and has_special):
        meeting_type = "Annual + Special"
    elif has_annual:
        meeting_type = "Annual"
    elif has_extraordinary:
        meeting_type = "Extraordinary"
    elif has_special:
        meeting_type = "Special"
    else:
        meeting_type = "Other"
        type_is_ambiguous = True

    if sum([has_annual, has_special, has_extraordinary]) > 1 and not has_annual_special:
        type_is_ambiguous = True

    meeting_date = ""
    if "Annual" in meeting_type or meeting_type == "Extraordinary":
        meeting_date = _extract_annual_meeting_date(text)

    return {
        "meeting_type": meeting_type,
        "meeting_date": meeting_date,
        "type_is_ambiguous": type_is_ambiguous,
    }


def test_api_key(api_key: str) -> str | None:
    """
    Validate an Anthropic API key by making a minimal real API call.
    Returns None if the key is valid, or an error string if it is not.
    """
    if not _ANTHROPIC_AVAILABLE:
        return "anthropic package not installed"
    try:
        client = _anthropic.Anthropic(api_key=api_key)
        client.messages.create(
            model=_ANTHROPIC_MODEL,
            max_tokens=1,
            messages=[{"role": "user", "content": "hi"}],
        )
        return None
    except Exception as exc:
        return str(exc)


def _parse_with_claude(html_text: str, api_key: str) -> tuple[dict | None, str | None]:
    """
    Use Claude Haiku 4.5 to extract meeting_type and meeting_date from DEF 14A
    text.  Returns (result, None) on success or (None, error_str) on failure
    so the caller can fall back to regex and surface the error.
    """
    try:
        try:
            clean = BeautifulSoup(html_text, "lxml").get_text(" ", strip=True)
        except Exception:
            clean = re.sub(r"<[^>]+>", " ", html_text)
        clean = re.sub(r"\s+", " ", clean)
        snippet = _build_claude_snippet(clean)

        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=_ANTHROPIC_MODEL,
            max_tokens=100,
            messages=[{"role": "user", "content": _CLAUDE_PROMPT + snippet}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if Claude wraps the response despite instructions
        raw = re.sub(r"^```(?:json)?\s*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw).strip()
        result = _json.loads(raw)
        mtype = result.get("meeting_type", "Other").strip()
        mdate = result.get("meeting_date", "").strip()
        if mtype not in {"Annual", "Special", "Annual + Special", "Extraordinary", "Other"}:
            mtype = "Other"
        return {
            "meeting_type": mtype,
            "meeting_date": mdate,
            "type_is_ambiguous": False,
        }, None
    except Exception as exc:
        return None, str(exc)


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


def _build_claude_snippet(clean_text: str) -> str:
    if not clean_text:
        return ""

    hit_index = _find_best_claude_anchor(clean_text)
    if hit_index is None:
        return clean_text[:_CLAUDE_FALLBACK_CHARS]

    start = max(0, hit_index - _CLAUDE_SNIPPET_BEFORE_CHARS)
    end = min(len(clean_text), hit_index + _CLAUDE_SNIPPET_AFTER_CHARS)

    snippet = clean_text[start:end]
    if len(snippet) < _CLAUDE_SNIPPET_TOTAL_CHARS:
        needed = _CLAUDE_SNIPPET_TOTAL_CHARS - len(snippet)
        extra_after = min(needed, len(clean_text) - end)
        end += extra_after
        needed -= extra_after
        if needed > 0:
            start = max(0, start - needed)
        snippet = clean_text[start:end]

    return snippet


def _find_best_claude_anchor(clean_text: str) -> int | None:
    for pattern in _CLAUDE_SNIPPET_PATTERNS:
        match = pattern.search(clean_text)
        if match:
            return match.start()
    return None


def _parse_filing_date(raw_date: str) -> date | None:
    try:
        return datetime.strptime(str(raw_date).strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_meeting_date(raw_date: str) -> date | None:
    if not raw_date:
        return None
    for fmt in ("%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"):
        try:
            return datetime.strptime(raw_date.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _should_fallback_to_api(regex_info: dict, filing_date: date | None) -> bool:
    if regex_info.get("type_is_ambiguous"):
        return True

    meeting_date_raw = regex_info.get("meeting_date", "").strip()
    if not meeting_date_raw:
        return True

    if filing_date is None:
        return False

    meeting_date = _parse_meeting_date(meeting_date_raw)
    if meeting_date is None:
        return True

    return meeting_date < filing_date

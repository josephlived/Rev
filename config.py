"""
Central configuration: all URLs, file paths, and constants.
"""
import os
import sys

# ── Base directory (works both in dev and when frozen by PyInstaller) ─────────
if getattr(sys, "frozen", False):
    # Running as a PyInstaller bundle — write cache next to the .exe
    _BASE_DIR = os.path.dirname(sys.executable)
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── EDGAR URLs ────────────────────────────────────────────────────────────────
EDGAR_DAILY_INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/form.{date}.idx"
)
EDGAR_ARCHIVE_BASE = "https://www.sec.gov/Archives/"
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# iShares Russell 3000 ETF (IWV) holdings CSV
ISHARES_IWV_URL = (
    "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/"
    "1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund"
)

# ── Local cache ───────────────────────────────────────────────────────────────
CACHE_DIR = os.path.join(_BASE_DIR, "cache")
RUSSELL_CACHE_PATH = os.path.join(CACHE_DIR, "russell3000_ciks.json")
RUSSELL_CACHE_MAX_AGE_DAYS = 7

# ── SEC request headers (required by SEC; swap for real contact info) ─────────
SEC_USER_AGENT = "Joseph Lumbihao jc4892790@gmail.com"

# ── Filing types to collate ───────────────────────────────────────────────────
TARGET_FORMS = {"10-K", "10-K/A", "10-KT", "DEF 14A", "20-F", "40-F", "NT 10-K", "DEF 14C", "DEF 14M"}

# ── Rate limiting ─────────────────────────────────────────────────────────────
RATE_LIMIT_RPS = 8  # stay safely under SEC's 10 req/s limit

# ── DEF 14A parsing ───────────────────────────────────────────────────────────
DEF14A_FETCH_BYTES = 150_000  # 150 KB — covers cover page even for large iXBRL filings

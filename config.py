"""
Central configuration: all URLs, file paths, and constants.
"""
import os

# ── Base directory ────────────────────────────────────────────────────────────
# True when the app runs on the user's own PC, started by start_sec_ledger.py
# (which sets SEC_LEDGER_DESKTOP=1). Per-user features such as saving the API
# key are limited to it: on Streamlit Cloud every visitor would share the same
# files.
IS_DESKTOP = os.environ.get("SEC_LEDGER_DESKTOP") == "1"

if IS_DESKTOP:
    # The user's own app-data folder, e.g. C:\Users\<you>\AppData\Local\SEC Ledger,
    # so settings survive replacing the app folder with a new version.
    _BASE_DIR = os.path.join(
        os.environ.get("LOCALAPPDATA") or os.path.expanduser("~"), "SEC Ledger"
    )
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Desktop app settings (API key, shared folder); see settings.py
SETTINGS_PATH = os.path.join(_BASE_DIR, "settings.json")

# ── EDGAR URLs ────────────────────────────────────────────────────────────────
EDGAR_DAILY_INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/form.{date}.idx"
)
EDGAR_ARCHIVE_BASE = "https://www.sec.gov/Archives/"
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# iShares Russell 3000 ETF (IWV) holdings CSV
# (the old ".ajax?fileType=csv" link now returns the HTML product page)
ISHARES_IWV_URL = (
    "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/"
    "latest-holdings.csv"
)
# iShares needs no contact details, so don't send it the SEC user agent
ISHARES_USER_AGENT = "SEC-Ledger/1.0"

# ── Local cache ───────────────────────────────────────────────────────────────
CACHE_DIR = os.path.join(_BASE_DIR, "cache")
RUSSELL_CACHE_PATH = os.path.join(CACHE_DIR, "russell3000_ciks.json")
RUSSELL_CACHE_MAX_AGE_DAYS = 7
# A refresh that maps fewer companies than this is treated as a bad download
# and the previous cache is kept (IWV normally maps ~2,550)
RUSSELL_MIN_COMPANIES = 2000

# ── Shared constituent list (desktop only; see shared_list.py) ────────────────
# Teams/SharePoint folder, synced through OneDrive, holding the team's list
SHARED_LIST_FOLDER_NAME = "SEC Ledger Constituents"
SHARED_LIST_MAX_AGE_DAYS = 35  # older than this → "has there been a rebalance?"
# Last list that loaded successfully, used when the shared file can't be read
SHARED_LIST_BACKUP_DIR = os.path.join(CACHE_DIR, "shared_list")

# ── SEC request headers (required by SEC; swap for real contact info) ─────────
SEC_USER_AGENT = "Joseph Lumbihao jc4892790@gmail.com"

# ── Filing types to collate ───────────────────────────────────────────────────
TARGET_FORMS = {"10-K", "10-K/A", "10-KT", "DEF 14A", "20-F", "40-F", "NT 10-K"}

# ── Rate limiting ─────────────────────────────────────────────────────────────
RATE_LIMIT_RPS = 8  # stay safely under SEC's 10 req/s limit

# ── DEF 14A parsing ───────────────────────────────────────────────────────────
DEF14A_FETCH_BYTES = 500_000  # 500 KB: large-cap proxies can put the meeting notice past 150 KB

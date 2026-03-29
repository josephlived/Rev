"""
SEC Filing Collator — Streamlit app entry point.

Run with:
    streamlit run app.py
"""
import datetime

import pandas as pd
import requests
import streamlit as st

import edgar
import russell
from config import TARGET_FORMS

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SEC Filing Collator",
    page_icon="📋",
    layout="wide",
)

st.title("📋 SEC Filing Collator — Russell 3000")
st.caption(
    "Collates 10-K · 10-K/A · DEF 14A · 20-F · 40-F · NT 10-K filings "
    "filed on a given date, filtered to Russell 3000 companies."
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Settings")

    filing_date = st.date_input(
        "Filing Date",
        value=datetime.date.today() - datetime.timedelta(days=1),
        min_value=datetime.date(1996, 1, 1),
        max_value=datetime.date.today(),
        help="Date filings were submitted to SEC EDGAR.",
    )

    fetch_btn = st.button("🔍 Fetch Filings", use_container_width=True, type="primary")

    st.divider()

    st.subheader("Russell 3000 Cache")
    info = russell.cache_info()
    if info["exists"]:
        st.success(
            f"✅ Cached — {info['ticker_count']} tickers\n\n"
            f"Updated: {info['updated_at']}\n\n"
            f"Age: {info['age_days']} days"
        )
    else:
        st.warning("⚠️ No cache — will download on first fetch.")

    refresh_btn = st.button(
        "🔄 Refresh Russell 3000 List",
        use_container_width=True,
        help="Force re-download of the iShares IWV holdings. Useful after June reconstitution.",
    )

    st.divider()
    st.caption(
        "Data sources: SEC EDGAR · iShares IWV ETF\n\n"
        "Rate-limited to ≤8 req/s per SEC guidelines."
    )

# ── Handle Russell 3000 refresh ───────────────────────────────────────────────
if refresh_btn:
    with st.spinner("Downloading Russell 3000 list\u2026"):
        status_box = st.empty()

        def _log(msg):
            status_box.info(msg)

        russell.load_russell_ciks(force_refresh=True, progress_cb=_log)
        status_box.empty()

    st.success("Russell 3000 cache refreshed.")
    st.rerun()

# ── Main fetch logic ──────────────────────────────────────────────────────────
if fetch_btn:
    session = requests.Session()

    with st.status("Loading Russell 3000 list\u2026", expanded=False) as status:
        r3k_log = st.empty()

        def _r3k_log(msg):
            r3k_log.write(msg)

        try:
            ticker_cik_map = russell.load_russell_ciks(progress_cb=_r3k_log)
            russell_cik_set = set(ticker_cik_map.values())
            status.update(
                label=f"✅ Russell 3000 loaded — {len(ticker_cik_map)} tickers",
                state="complete",
            )
        except Exception as exc:
            status.update(label="❌ Failed to load Russell 3000", state="error")
            st.error(f"Could not load Russell 3000 list: {exc}")
            st.stop()

    with st.status(
        f"Fetching EDGAR daily index for {filing_date}\u2026", expanded=False
    ) as status:
        try:
            raw_df = edgar.fetch_daily_index(filing_date, session)
            status.update(
                label=f"✅ Daily index fetched — {len(raw_df):,} total filings",
                state="complete",
            )
        except ValueError as exc:
            status.update(label="⚠️ No filings found", state="error")
            st.warning(str(exc))
            st.stop()
        except Exception as exc:
            status.update(label="❌ Failed to fetch index", state="error")
            st.error(f"EDGAR fetch error: {exc}")
            st.stop()

    filtered_df = edgar.filter_filings(raw_df, russell_cik_set)
    filtered_df = edgar.enrich_with_ticker(filtered_df, ticker_cik_map)

    if filtered_df.empty:
        st.info(
            f"No filings of types {sorted(TARGET_FORMS)} found for Russell 3000 "
            f"companies on {filing_date}."
        )
        st.stop()

    def14a_count = (filtered_df["form_type"] == "DEF 14A").sum()

    if def14a_count > 0:
        progress_bar = st.progress(0, text="Parsing DEF 14A filings\u2026")

        def _def14a_progress(msg, current, total):
            pct = int(current / total * 100)
            progress_bar.progress(pct, text=f"{msg} ({current}/{total})")

        filtered_df = edgar.parse_def14a_filings(
            filtered_df, session, progress_cb=_def14a_progress
        )
        progress_bar.empty()
    else:
        filtered_df["meeting_type"] = ""
        filtered_df["meeting_date"] = ""

    display_df = _build_display(filtered_df)

    form_counts = filtered_df["form_type"].value_counts().to_dict()
    cols = st.columns(len(form_counts) + 1)
    cols[0].metric("Total Filings", len(filtered_df))
    for i, (form, count) in enumerate(sorted(form_counts.items()), start=1):
        cols[i].metric(form, count)

    st.divider()

    st.subheader(f"Results for {filing_date}")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    csv_bytes = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download CSV",
        data=csv_bytes,
        file_name=f"sec_filings_{filing_date}.csv",
        mime="text/csv",
    )


# ── Helper ────────────────────────────────────────────────────────────────────

def _build_display(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder and rename columns for the results table."""
    out = pd.DataFrame()
    out["Form Type"] = df["form_type"]
    out["Company"] = df["company_name"]
    out["Ticker"] = df["ticker"]
    out["CIK"] = df["cik"]
    out["Meeting Type"] = df["meeting_type"].replace("", "\u2014")
    out["Meeting Date"] = df["meeting_date"].replace("", "\u2014")
    out["Filing"] = df["filename"].apply(
        lambda f: f"https://www.sec.gov/Archives/{f}" if pd.notna(f) else ""
    )
    return out

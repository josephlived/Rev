"""
SEC Ledger — Streamlit app entry point.

Run with:
    streamlit run app.py
"""
import datetime
import os

import pandas as pd
import requests
import streamlit as st

import edgar
import russell
from config import TARGET_FORMS


def _build_display(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder and rename columns for the results table."""
    out = pd.DataFrame()
    out["Date Filed"] = pd.to_datetime(df["date_filed"], errors="coerce").dt.strftime("%-m/%-d/%y")
    out["Form Type"] = df["form_type"]
    out["Company"] = df["company_name"]
    out["Ticker"] = df["ticker"]
    out["Index"] = df["index_name"].replace("", "—")
    out["CIK"] = df["cik"]
    out["Meeting Type"] = df["meeting_type"].replace("", "—")
    out["Meeting Date"] = df["meeting_date"].replace("", "—")
    out["Filing"] = df["filename"].apply(
        lambda f: f"https://www.sec.gov/Archives/{f}" if pd.notna(f) else ""
    )
    return out


# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SEC Ledger",
    page_icon="📋",
    layout="wide",
)

st.title("📋 SEC Ledger")
st.caption(
    "Collates 10-K · 10-K/A · 10-KT · DEF 14A · 20-F · 40-F · NT 10-K filings "
    "filed on a given date, filtered to Russell 3000 companies."
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Settings")

    _yesterday = datetime.date.today() - datetime.timedelta(days=1)
    _col1, _col2 = st.columns(2)
    start_date = _col1.date_input(
        "From",
        value=_yesterday,
        min_value=datetime.date(1996, 1, 1),
        max_value=datetime.date.today(),
    )
    end_date = _col2.date_input(
        "To",
        value=_yesterday,
        min_value=datetime.date(1996, 1, 1),
        max_value=datetime.date.today(),
    )
    if end_date < start_date:
        st.warning("'To' date must be on or after 'From' date.")
        end_date = start_date

    st.divider()

    # ── Form type selector ────────────────────────────────────────────────────
    st.subheader("Form Types")
    selected_forms = st.multiselect(
        "Select form types to collate",
        options=sorted(TARGET_FORMS),
        default=sorted(TARGET_FORMS),
        help="Choose which filing types to include. Deselect any you don't need.",
        label_visibility="collapsed",
    )
    if not selected_forms:
        st.warning("Select at least one form type.")

    selected_forms_set = set(selected_forms)

    fetch_btn = st.button(
        "🔍 Fetch Filings",
        use_container_width=True,
        type="primary",
        disabled=not selected_forms,
    )

    st.divider()

    # ── Custom constituent list upload ────────────────────────────────────────
    st.subheader("Constituent List")
    uploaded_file = st.file_uploader(
        "Upload your Excel/CSV list",
        type=["xlsx", "xls", "csv"],
        help=(
            "Upload a file with a 'CIK' or 'Ticker' column and optionally an 'Index' "
            "column (e.g. 'Russell 1000', 'S&P 500'). "
            "If no file is uploaded, the app uses the iShares IWV (Russell 3000) list."
        ),
    )
    if uploaded_file:
        st.success(f"✅ Using uploaded file: {uploaded_file.name}")
    else:
        st.caption("No file uploaded — using iShares IWV (Russell 3000).")

    st.divider()

    # ── iShares cache controls (shown only when no file is uploaded) ──────────
    if not uploaded_file:
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
            help="Force re-download of the iShares IWV holdings.",
        )
    else:
        refresh_btn = False

    st.divider()

    # ── AI meeting date parsing ───────────────────────────────────────────────
    st.subheader("AI Parsing")
    _api_key = st.secrets.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_API_KEY", "")
    if _api_key:
        _key_error = edgar.test_api_key(_api_key)
        if _key_error:
            st.error(f"Claude API key error: {_key_error}")
            _api_key = ""
        else:
            st.success("✅ Claude Haiku 4.5 active")
    else:
        st.caption(
            "No API key found. Add `ANTHROPIC_API_KEY` to Streamlit secrets "
            "for AI-powered meeting date parsing."
        )

    st.divider()
    st.caption("Data sources: SEC EDGAR · iShares IWV ETF\n\nRate-limited to ≤8 req/s per SEC guidelines.")

# ── Handle Russell 3000 refresh ───────────────────────────────────────────────
if refresh_btn:
    with st.spinner("Downloading Russell 3000 list…"):
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

    # ── Step 1: Load constituent list ─────────────────────────────────────────
    if uploaded_file:
        source_label = uploaded_file.name
        with st.status(f"Loading constituent list from {uploaded_file.name}…", expanded=False) as status:
            try:
                cik_set, ticker_from_cik, index_from_cik = russell.load_from_excel(uploaded_file)
                status.update(
                    label=f"✅ Loaded {len(cik_set)} companies from {uploaded_file.name}",
                    state="complete",
                )
            except Exception as exc:
                status.update(label="❌ Failed to load uploaded file", state="error")
                st.error(f"Could not read constituent file: {exc}")
                st.stop()
    else:
        source_label = "iShares IWV (Russell 3000)"
        with st.status("Loading Russell 3000 list…", expanded=False) as status:
            r3k_log = st.empty()

            def _r3k_log(msg):
                r3k_log.write(msg)

            try:
                cik_set, ticker_from_cik, index_from_cik = russell.load_russell_ciks(
                    progress_cb=_r3k_log
                )
                status.update(
                    label=f"✅ Russell 3000 loaded — {len(cik_set)} companies",
                    state="complete",
                )
            except Exception as exc:
                status.update(label="❌ Failed to load Russell 3000", state="error")
                st.error(f"Could not load Russell 3000 list: {exc}")
                st.stop()

    # ── Step 2: Build list of weekdays in the date range ──────────────────────
    dates_to_fetch = []
    d = start_date
    while d <= end_date:
        if d.weekday() < 5:  # Mon–Fri only; EDGAR has no index on weekends
            dates_to_fetch.append(d)
        d += datetime.timedelta(days=1)

    if not dates_to_fetch:
        st.warning("The selected date range contains no weekdays. Please pick a range that includes at least one weekday.")
        st.stop()

    # ── Step 3: Fetch EDGAR daily index for each date ─────────────────────────
    date_label = str(start_date) if start_date == end_date else f"{start_date} – {end_date}"

    all_raw: list[pd.DataFrame] = []
    skipped_dates: list[datetime.date] = []

    fetch_label = (
        f"Fetching EDGAR daily index for {date_label}…"
        if len(dates_to_fetch) == 1
        else f"Fetching EDGAR daily index ({len(dates_to_fetch)} days)…"
    )
    with st.status(fetch_label, expanded=False) as status:
        for i, day in enumerate(dates_to_fetch, 1):
            if len(dates_to_fetch) > 1:
                status.update(label=f"Fetching {day} ({i}/{len(dates_to_fetch)})…")
            try:
                day_df = edgar.fetch_daily_index(day, session)
                all_raw.append(day_df)
            except ValueError:
                skipped_dates.append(day)  # no index (holiday, etc.) — skip silently
            except Exception as exc:
                status.update(label="❌ Fetch error", state="error")
                st.error(f"EDGAR fetch error on {day}: {exc}")
                st.stop()

        if not all_raw:
            status.update(label="⚠️ No filings found", state="error")
            st.warning(
                f"No EDGAR filing index found for any date in the selected range ({date_label}). "
                "This may be a holiday or market closure."
            )
            st.stop()

        raw_df = pd.concat(all_raw, ignore_index=True).drop_duplicates()
        complete_label = f"✅ Daily index fetched — {len(raw_df):,} total filings"
        if skipped_dates:
            complete_label += f" ({len(skipped_dates)} date(s) skipped — no index)"
        status.update(label=complete_label, state="complete")

    # ── Step 4: Filter + enrich ───────────────────────────────────────────────
    filtered_df = edgar.filter_filings(raw_df, cik_set, target_forms=selected_forms_set)
    filtered_df = edgar.enrich_with_ticker(filtered_df, ticker_from_cik, index_from_cik)

    if filtered_df.empty:
        type_matched = raw_df[raw_df["form_type"].isin(selected_forms_set)]
        st.info(
            f"No filings matched your constituent list for **{date_label}**.\n\n"
            f"- **{len(raw_df):,}** total filings found\n"
            f"- **{len(type_matched):,}** matched the selected form types "
            f"({', '.join(sorted(selected_forms_set))})\n"
            f"- **0** of those had a CIK in your **{len(cik_set):,}**-company list"
        )
        if not type_matched.empty:
            st.caption(
                "These filings matched the form types but were not in your constituent list — "
                "check if any of these companies should be included:"
            )
            st.dataframe(
                type_matched[["form_type", "company_name", "cik"]].reset_index(drop=True),
                hide_index=True,
                use_container_width=True,
            )
        st.stop()

    # ── Step 5: Parse DEF 14A filings ─────────────────────────────────────────
    def14a_count = (filtered_df["form_type"] == "DEF 14A").sum()

    if def14a_count > 0:
        progress_bar = st.progress(0, text="Parsing DEF 14A filings…")

        def _def14a_progress(msg, current, total):
            pct = int(current / total * 100)
            progress_bar.progress(pct, text=f"{msg} ({current}/{total})")

        filtered_df = edgar.parse_def14a_filings(
            filtered_df, session, progress_cb=_def14a_progress,
            api_key=_api_key or None,
        )
        progress_bar.empty()
        if _api_key:
            errors = filtered_df.loc[
                filtered_df["claude_error"].str.len() > 0, "claude_error"
            ]
            if not errors.empty:
                st.warning(
                    f"Claude API failed on {len(errors)} filing(s), fell back to regex. "
                    f"First error: {errors.iloc[0]}"
                )
    else:
        filtered_df["meeting_type"] = ""
        filtered_df["meeting_date"] = ""

    # ── Step 6: Display ───────────────────────────────────────────────────────
    display_df = _build_display(filtered_df)

    form_counts = filtered_df["form_type"].value_counts().to_dict()
    cols = st.columns(len(form_counts) + 1)
    cols[0].metric("Total Filings", len(filtered_df))
    for i, (form, count) in enumerate(sorted(form_counts.items()), start=1):
        cols[i].metric(form, count)

    st.divider()
    st.subheader(f"Results for {date_label}")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    if start_date == end_date:
        csv_filename = f"sec_filings_{start_date}.csv"
    else:
        csv_filename = f"sec_filings_{start_date}_to_{end_date}.csv"

    csv_bytes = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download CSV",
        data=csv_bytes,
        file_name=csv_filename,
        mime="text/csv",
    )

"""
Temporary DEF 14A accuracy test. Not part of SEC Ledger: it lives on a test
branch and runs as its own Streamlit Cloud app.

Streamlit Cloud: main file "accuracy_test_app.py", with secrets
    ANTHROPIC_API_KEY = "sk-ant-..."
    TEST_PASSCODE = "something only you know"

On the same random sample of Russell 3000 DEF 14As it compares:
  - Regex       the app's Regex Parsing, unchanged
  - Haiku 4.5   the app's API Parsing, unchanged (incl. its date checks)
  - Haiku 5.5   the same prompt and checks on claude-haiku-5-5
  - SEC date    the "report date" companies enter when filing
"""
import hmac
import io
import json
import random
import re

import anthropic
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

import edgar
import russell

HAIKU_55 = "claude-haiku-5-5"
IVV_URL = "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/latest-holdings.csv"
PRICE_PER_M = {"Haiku 4.5": (1.00, 5.00), "Haiku 5.5": (0.10, 0.50)}  # $ in / out
METHODS = ["Regex", "Haiku 4.5", "Haiku 5.5"]
SEASON = ("2026-01-01", "2026-06-30")

st.set_page_config(page_title="DEF 14A accuracy test", page_icon="🧪", layout="wide")
st.title("🧪 DEF 14A accuracy test")
st.caption("Temporary test for SEC Ledger: Regex vs Haiku 4.5 vs Haiku 5.5 on the same proxies.")

passcode = st.secrets.get("TEST_PASSCODE", "")
api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
if not passcode or not api_key:
    st.error("Add TEST_PASSCODE and ANTHROPIC_API_KEY to this app's secrets first.")
    st.stop()
if not hmac.compare_digest(st.text_input("Passcode", type="password"), passcode):
    st.info("Enter the passcode to use this test.")
    st.stop()


def _clean_text(html: str) -> str:
    try:
        text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text)


def _iso(raw: str) -> str:
    parsed = edgar._parse_meeting_date(raw) if raw else None
    return parsed.isoformat() if parsed else ""


def parse_haiku_45(html: str, filing_date) -> tuple[str, str, str, float]:
    """The app's API Parsing exactly as it runs today."""
    result, err = edgar._parse_with_claude(html, api_key)
    reason = edgar._get_invalid_meeting_reason(result, filing_date) if result else None
    # _parse_with_claude doesn't expose usage: estimate tokens from characters
    chars = len(edgar._CLAUDE_PROMPT) + len(edgar._build_claude_snippet(_clean_text(html)))
    cost = (chars / 4 * PRICE_PER_M["Haiku 4.5"][0] + 40 * PRICE_PER_M["Haiku 4.5"][1]) / 1e6
    if result is None:
        return "", "", f"error: {err}", 0.0  # failed calls aren't billed
    return (result.get("meeting_type", ""), "" if reason else _iso(result.get("meeting_date", "")),
            f"rejected: {reason}" if reason else "", cost)


def parse_haiku_55(html: str, filing_date, client) -> tuple[str, str, str, float]:
    """Same prompt and date checks on Haiku 5.5. Its thinking is on by default:
    read the text block (not content[0]), allow room for thinking, keep effort low."""
    snippet = edgar._build_claude_snippet(_clean_text(html))
    try:
        msg = client.messages.create(
            model=HAIKU_55,
            max_tokens=1024,
            output_config={"effort": "low"},
            messages=[{"role": "user", "content": edgar._CLAUDE_PROMPT + snippet}],
        )
    except anthropic.APIError as exc:
        return "", "", f"error: {exc}", 0.0
    cost = (msg.usage.input_tokens * PRICE_PER_M["Haiku 5.5"][0]
            + msg.usage.output_tokens * PRICE_PER_M["Haiku 5.5"][1]) / 1e6
    if msg.stop_reason == "refusal":
        return "", "", "refused", cost
    raw = next((b.text for b in msg.content if b.type == "text"), "").strip()
    raw = re.sub(r"^```(?:json)?\s*\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw).strip()
    try:
        result = json.loads(raw)
    except ValueError:
        return "", "", f"error: not JSON ({msg.stop_reason}): {raw[:60]}", cost
    reason = edgar._get_invalid_meeting_reason(result, filing_date)
    return (str(result.get("meeting_type", "")), "" if reason else _iso(str(result.get("meeting_date", ""))),
            f"rejected: {reason}" if reason else "", cost)


def load_sp500_ciks() -> set[str]:
    """S&P 500 companies, from the iShares Core S&P 500 ETF (IVV) holdings."""
    resp = requests.get(IVV_URL, headers={"User-Agent": "SEC-Ledger/1.0"}, timeout=30)
    resp.raise_for_status()
    lines = resp.text.lstrip("﻿").splitlines()
    header = next(i for i, line in enumerate(lines) if line.startswith("Ticker,"))
    df = pd.read_csv(io.StringIO("\n".join(lines[header:])), dtype=str)
    df = df[(df["Asset Class"].str.strip() == "Equity") & df["Ticker"].notna()]
    cik_map = russell._fetch_sec_cik_map()
    tickers = {russell.normalize_ticker(t) for t in df["Ticker"]}
    return {str(int(cik_map[t])) for t in tickers if t in cik_map}


def run(sample_size: int, seed: int, universe: str) -> None:
    state = st.session_state
    if state.get("config") != (sample_size, seed, universe):
        state.config, state.rows, state.tried = (sample_size, seed, universe), [], set()
    session = requests.Session()
    client = anthropic.Anthropic(api_key=api_key)
    with st.spinner(f"Loading the {universe} list..."):
        if universe == "S&P 500":
            cik_set = load_sp500_ciks()
        else:
            cik_set, _, _ = russell.load_russell_ciks()
    ciks = sorted(cik_set)
    random.Random(seed).shuffle(ciks)
    bar = st.progress(len(state.rows) / sample_size, text="Starting...")
    for cik in ciks:
        if len(state.rows) >= sample_size:
            break
        if cik in state.tried:
            continue
        state.tried.add(cik)
        try:
            sub = edgar._get(session, f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json").json()
            recent = sub["filings"]["recent"]
            i = next((i for i, f in enumerate(recent["form"]) if f == "DEF 14A"
                      and SEASON[0] <= recent["filingDate"][i] <= SEASON[1]), None)
            if i is None:
                continue
            acc, filed = recent["accessionNumber"][i], recent["filingDate"][i]
            html = edgar._fetch_def14a_text(f"edgar/data/{cik}/{acc}.txt", cik, session)
        except Exception as exc:  # SEC throttling or an odd filing: skip it
            st.toast(f"Skipped CIK {cik}: {type(exc).__name__}")
            continue
        filing_date = edgar._parse_filing_date(filed)
        regex = edgar._parse_meeting_info(html)
        h45 = parse_haiku_45(html, filing_date)
        h55 = parse_haiku_55(html, filing_date, client)
        sec_date = recent["reportDate"][i] or ""
        state.rows.append({
            "Company": sub["name"], "CIK": cik, "Filed": filed,
            "Regex date": _iso(regex["meeting_date"]), "Haiku 4.5 date": h45[1], "Haiku 5.5 date": h55[1],
            "SEC date": sec_date if sec_date > filed else "",
            "Regex type": regex["meeting_type"], "Haiku 4.5 type": h45[0], "Haiku 5.5 type": h55[0],
            "Haiku 4.5 note": h45[2], "Haiku 5.5 note": h55[2],
            "Haiku 4.5 cost": h45[3], "Haiku 5.5 cost": h55[3],
            "Filing": f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/"
                      f"{recent['primaryDocument'][i]}",
        })
        bar.progress(len(state.rows) / sample_size, text=f"{len(state.rows)} / {sample_size}: {sub['name']}")
        # A bad key fails every call: stop instead of working through the whole sample
        if any(n.startswith("error") and ("401" in n or "authentication" in n.lower())
               for n in (h45[2], h55[2])):
            bar.empty()
            st.error(
                "Anthropic rejected the API key, so the test stopped. Check ANTHROPIC_API_KEY "
                f"in this app's secrets. Details: {h45[2] or h55[2]}"
            )
            return
    bar.empty()


def report(rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    dates = df[[f"{m} date" for m in METHODS]]
    filled = dates.ne("")
    # Rows where two methods gave different (non-blank) dates
    df["Disagree"] = dates.apply(lambda r: len({v for v in r if v}) > 1, axis=1)

    st.subheader(f"Results: {len(df)} proxies")
    summary = []
    for m in METHODS:
        col = df[f"{m} date"]
        sec = df["SEC date"]
        both = col.ne("") & sec.ne("")
        summary.append({
            "Method": m,
            "Date filled": f"{filled[f'{m} date'].sum()} ({filled[f'{m} date'].mean():.0%})",
            "Matches SEC date (where both exist)": f"{(col[both] == sec[both]).sum()} / {both.sum()}",
            "Cost": f"${df[f'{m} cost'].sum():.3f}" if f"{m} cost" in df else "free",
        })
    st.dataframe(pd.DataFrame(summary), hide_index=True, width="stretch")
    st.caption(
        "SEC date = the report date the company entered when filing, counted only when it's after "
        "the filing date. It's a hint, not the truth: check disagreements with the Filing link."
    )

    review = df[df["Disagree"]]
    st.subheader(f"Please review: {len(review)} proxies where the methods disagree")
    st.dataframe(
        review[["Company", "Filed", "Regex date", "Haiku 4.5 date", "Haiku 5.5 date", "SEC date", "Filing"]],
        hide_index=True, width="stretch",
        column_config={"Filing": st.column_config.LinkColumn("Filing", display_text="open")},
    )
    with st.expander("All results"):
        st.dataframe(df, hide_index=True, width="stretch",
                     column_config={"Filing": st.column_config.LinkColumn("Filing", display_text="open")})
    st.download_button("Download all results (CSV)", df.to_csv(index=False).encode("utf-8"),
                       "def14a_accuracy_test.csv", "text/csv")


universe = st.radio("Companies", ["S&P 500", "Russell 3000"], horizontal=True)
col1, col2 = st.columns(2)
size = col1.number_input("Number of proxies", min_value=5, max_value=300, value=150, step=5)
seed = col2.number_input("Sample seed (same seed = same companies)", min_value=1, value=500)
st.caption(f"Estimated API cost: about ${size * 0.0025:.2f} in total. Keep this tab open while it runs.")
if st.button("Run test (or continue an interrupted run)", type="primary"):
    run(int(size), int(seed), universe)
if st.session_state.get("rows"):
    report(st.session_state.rows)

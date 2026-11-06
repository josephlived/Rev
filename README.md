# SEC Ledger

Tracks key SEC filings (10-K, DEF 14A, 20-F, …) for US market coverage: pulls
the SEC EDGAR daily index, keeps filings from your constituent list (your own
Excel/CSV, or the iShares IWV Russell 3000 holdings), and extracts annual
meeting dates from DEF 14A proxies.

## Using the desktop app (Windows)

1. Open the repository's **Releases** page on GitHub and download
   `SEC-Ledger.exe` from the latest release.
2. Double-click it. The first time, Windows may show *"Windows protected your
   PC"*, because the app isn't signed: click **More info → Run anyway**.
3. A small black window opens, then the app opens in your browser.
   **Keep the black window open while you work; close it to quit.**
4. For **API Parsing**, paste your Anthropic API key in the sidebar once. It is
   saved only on your computer, in `%LOCALAPPDATA%\SEC Ledger`.

Starting can take 10–30 seconds. Double-clicking again while it's running just
reopens the browser tab.

## Publishing a new version of the app

GitHub builds the `.exe` for you; no setup needed on your PC.

1. On GitHub: **Releases → Draft a new release**.
2. **Choose a tag** → type a new version such as `v1.0.1` → **Create new tag**.
3. Give it a title, then **Publish release**.
4. After about 10 minutes, `SEC-Ledger.exe` appears under the release's
   **Assets** (progress is on the **Actions** tab).

Every push is also built and tested on the Actions tab, so a broken build shows
up before you release it.

## Developing

```
pip install -r requirements.txt
streamlit run app.py
```

To build the `.exe` on your own PC instead of GitHub, run `build.bat`.

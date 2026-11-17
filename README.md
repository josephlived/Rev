# SEC Ledger

Tracks key SEC filings (10-K, DEF 14A, 20-F, …) for US market coverage: pulls
the SEC EDGAR daily index, keeps filings from your constituent list (your own
Excel/CSV, or the iShares IWV Russell 3000 holdings), and extracts annual
meeting dates from DEF 14A proxies.

## Running it on your PC with Python (recommended on work PCs)

Use this when your PC blocks `SEC-Ledger.exe`. It runs the same app with the
Python already on your PC, so there is nothing new to install as a program.

**Check your PC first (2 minutes).** Open **PowerShell** (Windows key → type
`powershell` → Enter) and run:

```
python --version
python -m pip download streamlit --no-deps -d $env:TEMP\piptest
```

You need `Python 3.10` or newer, and the second command must end with
`Successfully downloaded streamlit`. If `python` isn't found, install
**Python 3.13** from the Microsoft Store (or ask IT). If the download fails,
ask IT whether `pypi.org` is allowed.

**First-time setup (5–10 minutes, once).**

1. Download the code:
   <https://github.com/josephlived/Rev/archive/refs/heads/main.zip>
2. Right-click `Rev-main.zip` → **Extract All…** → extract to your
   **Documents** folder.
3. Open the extracted folder until you see `start_sec_ledger.py`, right-click
   an empty space in it → **Open in Terminal**.
4. Run:
   ```
   python start_sec_ledger.py
   ```
   It installs the app's libraries (3–5 minutes), adds a **SEC Ledger**
   shortcut to your desktop, and opens the app in your browser.

**Every day after that:** double-click **SEC Ledger** on your desktop.

- A black window opens, then the app opens in your browser. **Keep the black
  window open while you work; close it to quit.**
- For **API Parsing**, paste your Anthropic API key in the sidebar once and
  click **Save key on this computer**. It is stored only on your PC.
- Don't move or rename the folder after setup, or the shortcut stops working.
  (If you do, run `python start_sec_ledger.py --shortcut` in its new place.)

**Updating:** download and extract the new zip over the old folder, then
start as usual; it updates its libraries by itself if needed.

**Uninstalling:** delete the folder and the desktop shortcut.

## Using the desktop app (`SEC-Ledger.exe`)

On PCs that allow it:

1. Open the repository's **Releases** page on GitHub and download
   `SEC-Ledger.exe` from the latest release.
2. Double-click it. The first time, Windows may show *"Windows protected your
   PC"*, because the app isn't signed: click **More info → Run anyway**. If
   there is no **Run anyway** button, your company blocks unsigned apps; use
   the Python method above instead.
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

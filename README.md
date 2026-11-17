# SEC Ledger

Tracks key SEC filings (10-K, DEF 14A, 20-F, …) for US market coverage: pulls
the SEC EDGAR daily index, keeps filings from your constituent list (your own
Excel/CSV, or the iShares IWV Russell 3000 holdings), and extracts annual
meeting dates from DEF 14A proxies.

## Running it on your PC

SEC Ledger runs with the Python already on your PC, so there is no program to
install.

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

### Shared constituent list (Teams folder)

The team keeps its FactSet list in one Teams folder; everyone's app uses the
newest file in it automatically, with no upload on each run.

**Once, for the team (list owner):**

1. In the team's Teams channel, open **Files** → **+ New → Folder** and name it
   exactly **`SEC Ledger Constituents`**.
2. Save the FactSet export there (`.xlsx` or `.csv`, with a **CIK** or
   **Ticker** column, optionally **Index**).
3. Each month (or after a rebalance), save the new export into the same folder.
   Keep old files if you like; the app always uses the newest one.

**Once, for each person:**

1. In Teams, open the channel's **Files**, select the **SEC Ledger
   Constituents** folder and click **Add shortcut to OneDrive** (or **Sync**).
2. Start SEC Ledger. It finds the folder by itself and shows
   *"Shared list: <file name>"* in the sidebar under **Constituent List**.
   If it doesn't, open **Shared folder settings** in the sidebar, then either
   click **Search again** or paste the folder's path (in File Explorer:
   Shift + right-click the folder → **Copy as path**).

**Good to know:**

- The sidebar shows which file is used and when it was saved, and reminds you
  when it is more than 35 days old.
- If the shared file can't be read (offline, still syncing, or not a valid
  list), the app uses the last list that worked and says so.
- Uploading a file in the sidebar overrides the shared list for that run.

**Uninstalling:** delete the folder and the desktop shortcut.

## Releasing a change to the team

Every change merged into `main` is tested on Windows automatically (the
**Actions** tab on GitHub shows a green check or a red cross). Once it's
green, tell the team to update: download the zip again and extract it over
their app folder, as described under **Updating** above.

## Developing

```
pip install -r requirements.txt
streamlit run app.py
```

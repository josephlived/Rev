"""
Smoke test for the built desktop app: start it, load it in a real browser and
fail if the app crashes or shows an error.

    pip install playwright
    python scripts/smoke_test.py dist/SEC-Ledger.exe

On Windows it drives the preinstalled Microsoft Edge; elsewhere Playwright's
Chromium. The app runs with a throwaway settings/cache folder, so a real saved
API key or Russell cache is never touched.
"""
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request

from playwright.sync_api import expect, sync_playwright

STARTUP_TIMEOUT_S = 240
_local = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def main(exe: str) -> int:
    data_dir = tempfile.mkdtemp(prefix="sec-ledger-smoke-")
    env = dict(os.environ, SEC_LEDGER_NO_BROWSER="1", LOCALAPPDATA=data_dir, HOME=data_dir)
    log_path = os.path.join(data_dir, "app.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [exe], env=env, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=(os.name != "nt"),
        )
    try:
        url = _wait_for_server(proc, log_path)
        _check_ui(url)
        print("SMOKE TEST PASSED")
        return 0
    except Exception as exc:
        print(f"SMOKE TEST FAILED: {exc}")
        return 1
    finally:
        _stop(proc)
        print("\n--- app console output ---")
        with open(log_path, errors="replace") as f:
            print(f.read())


def _wait_for_server(proc, log_path: str) -> str:
    """Return the app's URL once it answers. The launcher moves off port 8501
    when that is taken, so read the real address from its output."""
    deadline = time.monotonic() + STARTUP_TIMEOUT_S
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"app exited during startup with code {proc.returncode}")
        with open(log_path, errors="replace") as f:
            match = re.search(r"running at (http://localhost:\d+)", f.read())
        if match:
            url = match.group(1)
            with _local.open(f"{url}/_stcore/health", timeout=5) as resp:
                if resp.read().strip() == b"ok":
                    print(f"server up at {url} after {STARTUP_TIMEOUT_S - (deadline - time.monotonic()):.0f}s")
                    return url
        time.sleep(1)
    raise TimeoutError(f"server not up after {STARTUP_TIMEOUT_S}s")


def _check_ui(url: str) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge" if os.name == "nt" else None)
        page = browser.new_page()
        page.set_default_timeout(60_000)
        page.goto(url)

        expect(page.get_by_role("heading", name="SEC Ledger")).to_be_visible()
        expect(page.get_by_role("button", name="Fetch Filings")).to_be_visible()
        _assert_no_exception(page)
        if page.get_by_role("button", name="Deploy").count():
            raise AssertionError("Streamlit's developer 'Deploy' button is showing")
        print("ok    main page renders")

        # Desktop-only: choosing API parsing with no saved key shows the key form
        page.get_by_role(
            "combobox", name=re.compile("Choose how DEF 14A meeting details are parsed")
        ).click()
        page.keyboard.press("ArrowDown")  # a click only focuses the box
        page.get_by_role("option", name="API Parsing").click()
        expect(page.get_by_role("button", name="Save key on this computer")).to_be_visible()
        _assert_no_exception(page)
        print("ok    API key form shown (desktop mode)")

        # A fake key must be rejected by Anthropic and not saved. This makes a
        # real API call, which also proves the anthropic library is bundled.
        page.get_by_label("Anthropic API key").fill("sk-ant-smoke-test-invalid")
        page.get_by_role("button", name="Save key on this computer").click()
        expect(page.get_by_text("didn't work, so it wasn't saved")).to_be_visible()
        _assert_no_exception(page)
        print("ok    invalid API key rejected")

        # Full Russell download inside the .exe. iShares/SEC are outside our
        # control, so a failure here is reported but doesn't fail the build.
        page.get_by_role("button", name="Refresh Russell 3000 List").click()
        try:
            expect(page.get_by_text("holdings skipped (no SEC match)")).to_be_visible(timeout=120_000)
            print("ok    Russell 3000 refresh works inside the .exe")
        except AssertionError:
            print("WARN  Russell 3000 refresh did not complete (network or source issue)")
        _assert_no_exception(page)
        browser.close()


def _assert_no_exception(page) -> None:
    page.wait_for_timeout(1500)  # let the current script run finish
    errors = page.locator('[data-testid="stException"]')
    if errors.count():
        raise AssertionError("app shows an exception:\n" + errors.first.inner_text())


def _stop(proc) -> None:
    if proc.poll() is not None:
        return
    if os.name == "nt":
        # The one-file .exe runs the app in a child process: kill the whole tree
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
    else:
        os.killpg(proc.pid, 9)
    proc.wait(timeout=30)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))

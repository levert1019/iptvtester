# -*- coding: utf-8 -*-
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# How often to print probe progress (every N finished URLs)
_PROGRESS_EVERY = 250

def _ffprobe_ok(url: str, timeout_s: int):
    """
    Run ffprobe quickly to verify there's a playable video stream.
    Returns (ok: bool, note: str).
    """
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name,width,height",
        "-show_entries", "format=duration",
        "-of", "json",
        url,
    ]
    creationflags = 0x08000000 if os.name == "nt" else 0  # no console window on Windows
    try:
        p = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(1, min(timeout_s, 10)),
            check=False,
            creationflags=creationflags,
        )
        if p.returncode != 0:
            reason = (p.stderr.decode(errors="ignore") or "ffprobe error").strip()
            return False, reason
        data = json.loads(p.stdout.decode("utf-8", errors="ignore") or "{}")
        if not (data.get("streams") and data["streams"][0].get("codec_name")):
            return False, "No video stream"
        return True, "OK"
    except subprocess.TimeoutExpired:
        return False, "ffprobe timeout"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def _print_progress(done: int, total: int, quiet: bool):
    if quiet:
        return
    width = 30
    frac = 0 if total == 0 else done / total
    filled = int(frac * width)
    bar = "█" * filled + "·" * (width - filled)
    print(f"   • Probe: [{bar}] {done}/{total}")

def probe_streams(con, items, workers: int, timeout: int, quiet: bool):
    """
    Probes the given list of playlist items in parallel and updates the DB.

    Returns: list of tuples (item, ok: bool, note: str)
    """
    total = len(items)
    results = []

    if not quiet:
        print(f"🧪 Probing now: {total} (parallel={workers})")

    if workers <= 1:
        done = 0
        for it in items:
            ok, note = _ffprobe_ok(it["url"], timeout)
            results.append((it, ok, note))
            done += 1
            _print_progress(done, total, quiet)
    else:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            futs = {ex.submit(_ffprobe_ok, it["url"], timeout): it for it in items}
            done = 0
            for fut in as_completed(futs):
                it = futs[fut]
                ok, note = fut.result()
                results.append((it, ok, note))
                done += 1
                if done == total or (done % _PROGRESS_EVERY == 0):
                    _print_progress(done, total, quiet)

    # Persist results to DB using REAL epoch seconds
    ok_rows, fail_rows = [], []
    now = time.time()
    for (it, ok, note) in results:
        if ok:
            ok_rows.append((now, now, it["url"]))
        else:
            fail_rows.append((now, it["url"]))

    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE;")
    if ok_rows:
        cur.executemany(
            "UPDATE streams SET status='OK', last_checked=?, last_ok=?, fail_count=0 WHERE url=?",
            ok_rows,
        )
    if fail_rows:
        cur.executemany(
            "UPDATE streams SET status='FAIL', last_checked=?, fail_count=COALESCE(fail_count,0)+1 WHERE url=?",
            fail_rows,
        )
    con.commit()

    if not quiet:
        okc = len(ok_rows)
        failc = len(fail_rows)
        print(f"💾 Saved probe results: {okc} OK, {failc} FAIL")

    return results

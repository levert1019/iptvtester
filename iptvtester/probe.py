# -*- coding: utf-8 -*-
import os, json, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import tick_progress

def ffprobe_ok(url: str, timeout_s: int):
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name,width,height",
        "-show_entries", "format=duration",
        "-of", "json",
        url
    ]
    creationflags = 0x08000000 if os.name == "nt" else 0
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=min(timeout_s, 10), check=False, creationflags=creationflags)
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

def run_probe(items, parallelism, timeout_s, show_each=False, quiet=False):
    results = []
    total = len(items)
    if parallelism <= 1:
        for i, it in enumerate(items, 1):
            ok, note = ffprobe_ok(it['url'], timeout_s)
            if show_each and not quiet:
                print(f"▶ [{i}/{total}] {it.get('group-title','')} | {it.get('title')}: {'OK' if ok else 'FAIL'} ({note})")
            results.append((it, ok, note))
            tick_progress(i, total, prefix="   • Probe:", quiet=quiet)
        return results

    with ThreadPoolExecutor(max_workers=parallelism) as ex:
        futs = {ex.submit(ffprobe_ok, it['url'], timeout_s): it for it in items}
        done = 0
        for fut in as_completed(futs):
            ok, note = fut.result()
            it = futs[fut]
            results.append((it, ok, note))
            done += 1
            tick_progress(done, total, prefix="   • Probe:", quiet=quiet)
    return results

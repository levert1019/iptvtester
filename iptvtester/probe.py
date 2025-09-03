# -*- coding: utf-8 -*-
"""
Probe streams with ffprobe.
Annotates each item with:
  status = "OK"/"FAIL"
  last_ok = epoch (if OK)
  last_checked = epoch
  probe_error = string (if FAIL)

Features:
- Parallel probing (PROBE.PARALLELISM)
- Periodic progress logs (every N items)
- Graceful Ctrl+C: returns partial results gathered so far
"""

from __future__ import annotations
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple


def _probe_one(item: Dict, cfg: Dict) -> Dict:
    url = item.get("url")
    ffprobe_path = cfg.get("PROBE", {}).get("FFPROBE_PATH", "ffprobe")
    timeout = int(cfg.get("PROBE", {}).get("TIMEOUT", 10))
    now = int(time.time())

    try:
        # Keep it light: check first video stream codec_name; errors imply not playable
        cmd = [
            ffprobe_path,
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=codec_name",
            "-of", "default=nokey=1:noprint_wrappers=1",
            url,
        ]
        subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=True,
        )
        item["status"] = "OK"
        item["last_ok"] = now
        item["last_checked"] = now
        item["probe_error"] = ""
    except subprocess.TimeoutExpired:
        item["status"] = "FAIL"
        item["last_checked"] = now
        item["probe_error"] = "Timeout"
    except subprocess.CalledProcessError as e:
        item["status"] = "FAIL"
        item["last_checked"] = now
        msg = ""
        try:
            msg = e.stderr.decode(errors="ignore").strip()
        except Exception:
            pass
        item["probe_error"] = msg or "ffprobe error"
    except Exception as e:
        item["status"] = "FAIL"
        item["last_checked"] = now
        item["probe_error"] = str(e)

    return item


def probe_streams(items: List[Dict], cfg: Dict) -> Tuple[List[Dict], List[Dict]]:
    total = len(items)
    if total == 0:
        return [], []

    parallelism = max(1, int(cfg.get("PROBE", {}).get("PARALLELISM", 16)))
    tick = max(100, int(cfg.get("PROBE", {}).get("PROGRESS_EVERY", 1000)))  # print every N completions

    ok: List[Dict] = []
    fail: List[Dict] = []

    completed = 0
    last_print = 0

    print(f"🧪 Probing now: {total} (parallel={parallelism})")

    # We’ll handle Ctrl+C and return partial results
    try:
        with ThreadPoolExecutor(max_workers=parallelism) as ex:
            futs = [ex.submit(_probe_one, it, cfg) for it in items]
            for fut in as_completed(futs):
                try:
                    item = fut.result()
                except KeyboardInterrupt:
                    # Main thread interrupt — cancel remaining futures
                    ex.shutdown(wait=False, cancel_futures=True)
                    break
                except Exception as e:
                    # Should be rare; mark as FAIL
                    item = None

                if item is not None:
                    if item.get("status") == "OK":
                        ok.append(item)
                    else:
                        fail.append(item)

                completed += 1
                if completed - last_print >= tick or completed == total:
                    last_print = completed
                    # Simple progress line
                    print(f"   • Probe: [{completed}/{total}]")

    except KeyboardInterrupt:
        # Ctrl+C during scheduling/as_completed setup
        print("⏹️  Probing interrupted by user (Ctrl+C). Returning partial results…")

    return ok, fail

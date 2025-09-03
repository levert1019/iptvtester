# -*- coding: utf-8 -*-
from __future__ import annotations
import fnmatch
import os
import re
import sys
import time
from collections import Counter
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import requests

from .m3u import parse as parse_m3u, write as write_m3u
from .tmdb import enrich as tmdb_enrich
from .excel import export as export_excel

# ---- probe import is REQUIRED when PROBE.ENABLED = True
try:
    from .probe import probe_streams  # signature: probe_streams(items, cfg) -> (ok_items, fail_items)
except Exception as _e:
    probe_streams = None
    _probe_import_error = _e
else:
    _probe_import_error = None


def _is_url(s: str) -> bool:
    try:
        p = urlparse(s or "")
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def _download_text(url: str, cfg: Dict) -> str:
    """Download M3U text using config HTTP settings, with UA rotation."""
    verify = bool(cfg.get("HTTP_VERIFY_TLS", True))
    timeout = int(cfg.get("HTTP_TIMEOUT", 10))
    base_ua = cfg.get("HTTP_UA") or "VLC/3.0.18 LibVLC/3.0.18"

    uas = [
        base_ua,
        "Dalvik/2.1.0 (Linux; U; Android 9; IPTV Smarters Pro)",
        "Kodi/20.0 (Windows NT 10.0; Win64; x64)",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0 Safari/537.36",
    ]
    sess = requests.Session()
    sess.headers.update({"Accept": "*/*", "Accept-Encoding": "gzip, deflate"})
    last_status = None
    last_exc = None
    for ua in uas:
        try:
            sess.headers["User-Agent"] = ua
            r = sess.get(url, timeout=timeout, verify=verify, allow_redirects=True)
            if r.status_code != 200:
                last_status = r.status_code
                print(f"HTTP {r.status_code} with UA={ua}")
                continue
            return r.text or ""
        except Exception as e:
            last_exc = e
            continue
    if last_exc:
        raise OSError(f"Download failed: {last_exc}")
    raise OSError(f"Download failed (last status {last_status})")


def _load_m3u_text(cfg: Dict, src: str) -> str:
    if _is_url(src):
        return _download_text(src, cfg)
    if not os.path.isfile(src):
        raise FileNotFoundError(f"M3U file not found: {src}")
    with open(src, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


# -------------------- GROUP FILTERING --------------------

_PREFIX_RE = re.compile(r'^\s*\|?([A-Z]{2,4})\|?\s*[-|]\s*', re.IGNORECASE)

def _detect_prefix(group_title: str) -> str:
    if not group_title:
        return ""
    m = _PREFIX_RE.match(group_title)
    return (m.group(1).upper() if m else "").strip()

def _norm(s: str) -> str:
    # normalize whitespace, casing, and fancy dashes/pipes
    s = (s or "").strip()
    s = s.replace("—", "-").replace("–", "-").replace("│", "|").replace("¦", "|")
    s = re.sub(r"\s+", " ", s)
    return s

def _match_group(g: str, patterns: List[str], mode: str) -> bool:
    gl = _norm(g).lower()
    pats = [_norm(p).lower() for p in patterns]
    if mode == "equals":
        return any(gl == p for p in pats)
    if mode == "substring":
        return any(p in gl for p in pats)
    if mode == "regex":
        return any(re.search(p, gl) for p in pats)
    if mode == "glob":
        return any(fnmatch.fnmatch(gl, p) for p in pats)
    return any(p in gl for p in pats)

def _filter_items_by_groups(items: List[Dict], cfg: Dict) -> List[Dict]:
    filt = cfg.get("FILTER", {}) or {}
    include_groups = filt.get("INCLUDE_GROUPS") or []
    include_prefixes = [p.upper() for p in (filt.get("INCLUDE_PREFIXES") or [])]
    mode = (filt.get("MODE") or "substring").strip().lower()
    process_only = bool(filt.get("PROCESS_ONLY_INCLUDED_GROUPS", False))

    if not include_groups and not include_prefixes:
        print("🧰 Filter: no INCLUDE_GROUPS or INCLUDE_PREFIXES configured → processing all parsed entries.")
        return items

    kept, dropped = [], 0
    for it in items:
        g = it.get("group-title") or it.get("raw_group") or ""
        pref = _detect_prefix(g)
        ok = False
        if include_prefixes and pref:
            if pref in include_prefixes:
                ok = True
        if not ok and include_groups:
            if _match_group(g, include_groups, mode):
                ok = True
        if ok:
            kept.append(it)
        else:
            dropped += 1

    if not kept:
        uniq = Counter(_norm(x.get("group-title") or x.get("raw_group") or "") for x in items)
        print("⚠️  Filter kept 0 items. Here are the top 20 group-title samples I saw:")
        for name, count in uniq.most_common(20):
            print(f"   • {name}   (x{count})")
        print("   Tip: set FILTER.MODE to 'substring' or 'glob', or use INCLUDE_PREFIXES: ['EN','EX','EXYU','DE']")

    if process_only:
        print(f"🧰 Filter: active (process_only=True) → kept {len(kept)}, dropped {dropped}.")
        return kept
    else:
        print(f"🧰 Filter: present (process_only=False) → keeping ALL ({len(items)}). Matched={len(kept)}, non-matching={dropped}.")
        return items


# -------------------- PROBING / ENRICH / EXPORT --------------------

def _split_ok_fail_after_probe(items: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    ok, fail = [], []
    for it in items:
        st = (it.get("status") or "").upper()
        if st == "OK":
            ok.append(it)
        else:
            fail.append(it)
    return ok, fail


def run_once(cfg: Dict) -> None:
    print("🚀 Start IPTV-Tester")

    try:
        # 1) Load playlist text
        src = cfg.get("SOURCE_M3U") or "input/playlist.m3u"
        text = _load_m3u_text(cfg, src)

        # 2) Parse
        items = parse_m3u(text)
        print(f"🔎 Entries detected: {len(items)}")

        # 3) Filter by groups (deterministic & logged)
        items = _filter_items_by_groups(items, cfg)
        print(f"🔎 After filter: {len(items)}")

        # 4) Probe (ffprobe), if enabled
        probe_enabled = bool(cfg.get("PROBE", {}).get("ENABLED", True))
        if probe_enabled:
            if probe_streams is None:
                raise RuntimeError(
                    "Probe is enabled but iptvtester.probe could not be imported.\n"
                    f"Import error: {_probe_import_error}\n"
                    "→ Ensure iptvtester/probe.py exists and defines probe_streams(items, cfg)."
                )
            ok_items, fail_items = probe_streams(items, cfg)
        else:
            print("🧪 Probe disabled by config (PROBE.ENABLED=false). Marking nothing as OK.")
            ok_items, fail_items = [], items[:]  # everything untested remains FAIL domain until proved

        # 5) Stats
        ok_n = len(ok_items)
        fail_n = len(fail_items)
        print(f"💾 Saved probe results: {ok_n} OK, {fail_n} FAIL")
        print(f"📌 Summary (DB state): {ok_n} OK / {fail_n} FAIL (within filtered playlist).")

        # 6) TMDB enrichment for BOTH OK and FAIL (series-level, no episode API)
        ok_items = tmdb_enrich(ok_items, cfg)
        fail_items = tmdb_enrich(fail_items, cfg)

        # 7) Write M3Us
        out_ok = cfg.get("OUTPUT_OK_M3U") or cfg.get("OUTPUT", {}).get("OK_M3U") or "output/ok.m3u"
        out_fail = cfg.get("OUTPUT_FAIL_M3U") or cfg.get("OUTPUT", {}).get("FAIL_M3U") or "output/fail.m3u"
        os.makedirs(os.path.dirname(out_ok) or ".", exist_ok=True)
        os.makedirs(os.path.dirname(out_fail) or ".", exist_ok=True)

        print("📝 Writing M3Us …")
        write_m3u(out_ok, ok_items)
        write_m3u(out_fail, fail_items)

        # 8) Excel export
        print("📗 Writing Excel …")
        export_excel(
            ok_items,
            fail_items,
            {
                "OUTPUT_XLSX_OK": cfg.get("OUTPUT_XLSX_OK") or cfg.get("OUTPUT", {}).get("XLSX_OK") or "output/ok.xlsx",
                "OUTPUT_XLSX_FAIL": cfg.get("OUTPUT_XLSX_FAIL") or cfg.get("OUTPUT", {}).get("XLSX_FAIL") or "output/fail.xlsx",
            },
        )

        print("✅ Done. ✨")

    except KeyboardInterrupt:
        # Allow graceful exit on Ctrl+C anywhere in the pipeline
        print("\n⏹️  Interrupted by user (Ctrl+C). Exiting cleanly.")
        return

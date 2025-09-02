# -*- coding: utf-8 -*-
import hashlib, os, re, time
from datetime import datetime
from dateutil.tz import tzlocal

_last_tick = [0.0]

def now_local_iso():
    return datetime.now(tzlocal()).isoformat(timespec="seconds")

def human_dt(s):
    if not s:
        return ""
    try:
        dt = datetime.fromisoformat(s)
        return dt.strftime("%d-%m-%Y | %H:%M")
    except Exception:
        return s

def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", "ignore")).hexdigest()

def print_progress(done: int, total: int, prefix: str = "", quiet: bool = False):
    if quiet:
        return
    width = 30
    frac = 0 if total == 0 else done/total
    filled = int(frac*width)
    bar = "█"*filled + "·"*(width-filled)
    print(f"{prefix} [{bar}] {done}/{total}")

def tick_progress(done, total, prefix="", quiet=False, every_sec=1.0):
    now = time.perf_counter()
    if now - _last_tick[0] >= every_sec or done == total:
        _last_tick[0] = now
        print_progress(done, total, prefix, quiet)

def clean_title(t: str) -> str:
    t = (t or "").strip()
    return re.sub(r"\s+", " ", t)

def group_match(name: str, needles):
    n = (name or "").lower()
    return any(s.lower() in n for s in (needles or []))

def lang_to_prefix(lang: str) -> str:
    # "de-DE" -> "DE", "en" -> "EN"
    if not lang:
        return "EN"
    return (lang.split("-")[0] or "en").upper()

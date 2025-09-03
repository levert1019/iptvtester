# -*- coding: utf-8 -*-
"""
TMDB enrichment with strict pre-grouping and stable keys.

- Phase A: Build series groups BEFORE any network call:
    • title_key: aggressively normalized series title
    • url_fingerprint: host + parent path segment
    • series_key = f"{prefix}::{title_key}" (stable across runs)
    • Persist groups to SQLite (series_groups) for visibility and reuse

- Phase B: One TMDB lookup per series_key; apply to all members.

- Guarantees: a show can't split into different categories within the same prefix.
"""

from __future__ import annotations
import json
import os
import re
import sqlite3
import time
import unicodedata
from urllib.parse import urlparse
from typing import Dict, List, Optional, Tuple

import requests

# ----------------------- Genre maps -----------------------
_TV_GENRES = {
    10759: "Action & Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
    99: "Documentary", 18: "Drama", 10751: "Family", 10762: "Kids",
    9648: "Mystery", 10763: "News", 10764: "Reality", 10765: "Sci-Fi & Fantasy",
    10766: "Soap", 10767: "Talk", 10768: "War & Politics", 37: "Western",
}
_MOVIE_GENRES = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
    99: "Documentary", 18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History",
    27: "Horror", 10402: "Music", 9648: "Mystery", 10749: "Romance",
    878: "Science Fiction", 10770: "TV Movie", 53: "Thriller",
    10752: "War", 37: "Western",
}

# ----------------------- Regex helpers -----------------------
_PREFIX_RE = re.compile(r'^\s*\|?([A-Z]{2,4})\|?\s*[-|]\s+', re.IGNORECASE)
_SE_RE = re.compile(r'(?i)\bS\s*(\d{1,2})\s*E\s*(\d{1,2})\b')
_RESJUNK_RE = re.compile(r'(?i)\b(?:4k|uhd|2160p|3840p|1440p|qhd|1080p|720p|hdr|dolby(?:\s*vision)?)\b')
_PARENS_YEARS_RE = re.compile(r'\((?:\d{4})(?:\s*[-/]\s*\d{2,4})?\)')
_MULTI_SPACE_RE = re.compile(r'\s+')
_PUNCT_RE = re.compile(r"[^a-z0-9]+")

_ARTICLES = ("the ", "a ", "an ", "der ", "die ", "das ", "le ", "la ", "el ", "los ", "las ")

def _detect_prefix(s: str) -> str:
    if not s:
        return ""
    m = _PREFIX_RE.match(s)
    return (m.group(1).upper() if m else "").strip()

def _strip_prefix(s: str) -> str:
    return _PREFIX_RE.sub("", s or "").strip()

def _clean_base_title(raw_title: str) -> str:
    t = _RESJUNK_RE.sub(" ", raw_title or "")
    t = _PARENS_YEARS_RE.sub(" ", t)
    t = _MULTI_SPACE_RE.sub(" ", t).strip(" -–—")
    return t

def _ascii_fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")

def _title_key(title: str) -> str:
    """Stable, aggressive key for grouping the *same* show."""
    t = _ascii_fold(title).lower()
    t = t.replace("&", " and ")
    t = _RESJUNK_RE.sub(" ", t)
    t = _PARENS_YEARS_RE.sub(" ", t)
    t = _MULTI_SPACE_RE.sub(" ", t).strip()
    # drop leading articles
    for art in _ARTICLES:
        if t.startswith(art):
            t = t[len(art):]
            break
    # keep only a-z0-9, collapse
    t = _PUNCT_RE.sub(" ", t)
    t = _MULTI_SPACE_RE.sub(" ", t).strip()
    return t

def _split_series_tokens(title_wo_prefix: str) -> Tuple[str, Optional[int], Optional[int], str]:
    m = _SE_RE.search(title_wo_prefix or "")
    if not m:
        base = _clean_base_title(title_wo_prefix)
        return base, None, None, ""
    s = int(m.group(1))
    e = int(m.group(2))
    left = title_wo_prefix[:m.start()].strip()
    right = title_wo_prefix[m.end():].strip(" -")
    base = _clean_base_title(left)
    return base, s, e, right

def _url_fingerprint(url: str) -> str:
    """
    Try to bind episodes from the same provider/series together:
    host + parent path segment (the dir that contains episode file/id).
    """
    if not url:
        return ""
    try:
        u = urlparse(url)
        parts = [p for p in (u.path or "").split("/") if p]
        parent = parts[-2] if len(parts) >= 2 else ""
        return f"{u.netloc}/{parent}".lower()
    except Exception:
        return ""

def _series_cache_key(prefix: str, title_key: str) -> str:
    return f"{prefix}::{title_key}".lower().strip()

# ----------------------- SQLite -----------------------
_SQL_INIT = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA page_size=32768;

CREATE TABLE IF NOT EXISTS tmdb_series_cache (
  series_key    TEXT PRIMARY KEY,
  media_type    TEXT NOT NULL,
  tmdb_id       INTEGER,
  name          TEXT,
  poster_path   TEXT,
  genres_json   TEXT,
  lang          TEXT,
  updated_at    INTEGER,
  negative      INTEGER DEFAULT 0
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS series_groups (
  series_key    TEXT PRIMARY KEY,
  title_key     TEXT NOT NULL,
  prefix        TEXT NOT NULL,
  fingerprints  TEXT NOT NULL,     -- JSON set(list) of url fingerprints
  sample_names  TEXT NOT NULL,     -- JSON list of a few example raw titles
  last_seen     INTEGER NOT NULL
) WITHOUT ROWID;
"""

def _ensure_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    con = sqlite3.connect(path, timeout=30, isolation_level=None)
    con.execute("PRAGMA busy_timeout=5000")
    con.executescript(_SQL_INIT)
    return con

def _cache_get(con: sqlite3.Connection, key: str) -> Optional[dict]:
    cur = con.execute(
        "SELECT media_type, tmdb_id, name, poster_path, genres_json, lang, updated_at, negative "
        "FROM tmdb_series_cache WHERE series_key = ?",
        (key,)
    )
    row = cur.fetchone()
    if not row:
        return None
    media_type, tmdb_id, name, poster_path, genres_json, lang, updated_at, negative = row
    try:
        genres = json.loads(genres_json or "[]")
    except Exception:
        genres = []
    return {
        "media_type": media_type, "tmdb_id": tmdb_id, "name": name or "",
        "poster_path": poster_path or "", "genres": genres, "lang": lang or "",
        "updated_at": updated_at or 0, "negative": int(negative or 0),
    }

def _cache_put(con: sqlite3.Connection, key: str, data: dict) -> None:
    con.execute(
        "INSERT INTO tmdb_series_cache(series_key, media_type, tmdb_id, name, poster_path, genres_json, lang, updated_at, negative) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(series_key) DO UPDATE SET "
        "  media_type=excluded.media_type, tmdb_id=excluded.tmdb_id, name=excluded.name, "
        "  poster_path=excluded.poster_path, genres_json=excluded.genres_json, lang=excluded.lang, "
        "  updated_at=excluded.updated_at, negative=excluded.negative",
        (
            key, data.get("media_type") or "", data.get("tmdb_id"),
            data.get("name") or "", data.get("poster_path") or "",
            json.dumps(data.get("genres") or []), data.get("lang") or "",
            int(time.time()), int(data.get("negative") or 0),
        ),
    )

def _groups_put(con: sqlite3.Connection, series_key: str, title_key: str, prefix: str,
                fingerprints: List[str], sample_names: List[str]) -> None:
    try:
        cur = con.execute("SELECT fingerprints, sample_names FROM series_groups WHERE series_key=?", (series_key,))
        row = cur.fetchone()
        if row:
            old_fps = set(json.loads(row[0] or "[]"))
            old_names = list(dict.fromkeys(json.loads(row[1] or "[]")))
        else:
            old_fps, old_names = set(), []
        new_fps = list(sorted(old_fps.union(set(fingerprints))))
        new_names = old_names + [n for n in sample_names if n not in old_names]
        new_names = new_names[:8]
        con.execute(
            "INSERT INTO series_groups(series_key, title_key, prefix, fingerprints, sample_names, last_seen) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(series_key) DO UPDATE SET "
            "  title_key=excluded.title_key, prefix=excluded.prefix, "
            "  fingerprints=excluded.fingerprints, sample_names=excluded.sample_names, "
            "  last_seen=excluded.last_seen",
            (series_key, title_key, prefix, json.dumps(new_fps), json.dumps(new_names), int(time.time()))
        )
    except Exception:
        # grouping metadata is best-effort; don't crash pipeline
        pass

# ----------------------- HTTP -----------------------
def _requests_session(timeout: int) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "iptvtester/0.2",
    })
    s.request = _wrap_timeout(s.request, timeout)  # type: ignore
    return s

def _wrap_timeout(orig, timeout: int):
    def _req(method, url, **kw):
        if "timeout" not in kw:
            kw["timeout"] = timeout
        return orig(method, url, **kw)
    return _req

def _tmdb_search(session: requests.Session, api_key: str, query: str, media_type: str, lang: str, year: Optional[int]) -> Optional[dict]:
    if not query:
        return None
    endpoint = "https://api.themoviedb.org/3/search/tv" if media_type == "tv" else "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": api_key, "query": query, "language": lang}
    if media_type == "tv" and year:
        params["first_air_date_year"] = year
    if media_type == "movie" and year:
        params["year"] = year
    try:
        r = session.get(endpoint, params=params)
        if r.status_code != 200:
            return None
        data = r.json()
        results = data.get("results") or []
        if not results:
            return None
        return results[0]
    except Exception:
        return None

def _genre_names(media_type: str, genre_ids: List[int]) -> List[str]:
    table = _TV_GENRES if media_type == "tv" else _MOVIE_GENRES
    names = [table.get(int(g), None) for g in genre_ids]
    return [n for n in names if n]

# ----------------------- Public API -----------------------
def enrich(items: List[Dict], cfg: Dict) -> List[Dict]:
    """
    Strict series grouping before any TMDB call, then single lookup per group.
    """
    tmdb = cfg.get("TMDB", {}) or {}
    api_key = (tmdb.get("API_KEY") or "").strip()
    lang = (tmdb.get("LANGUAGE") or "en-US").strip()
    timeout = int(tmdb.get("TIMEOUT", 10))
    cache_path = tmdb.get("CACHE_DB_PATH") or "output/tmdb_cache.sqlite"
    debug_stats = bool(tmdb.get("DEBUG_STATS", False))

    if not api_key:
        print("⚠️  TMDB.API_KEY is empty — enrichment skipped.")
        return items

    con = _ensure_db(cache_path)

    # -------- Phase A: build strict groups --------
    meta_list: List[Dict] = []
    groups: Dict[str, Dict] = {}  # series_key -> {members:[], prefix, media_hint}

    for it in items:
        raw_disp = it.get("display_title") or it.get("tvg-name") or it.get("title") or ""
        raw_group = it.get("group-title") or it.get("raw_group") or ""
        url = it.get("url") or ""

        pref = _detect_prefix(raw_group) or _detect_prefix(raw_disp) or "EN"

        disp_wo_pref = _strip_prefix(raw_disp)
        base, s, e, tail = _split_series_tokens(disp_wo_pref)

        # title key is the backbone
        tkey = _title_key(base) or _title_key(disp_wo_pref) or _title_key(raw_disp)
        fp = _url_fingerprint(url)
        is_series = s is not None and e is not None

        # fallbacks: if no S/E and title is super short, lean on fingerprint to keep episodes together
        if not tkey and fp:
            tkey = fp.replace("/", " ")

        skey = _series_cache_key(pref, tkey)
        g = groups.get(skey)
        if not g:
            g = {"members": [], "prefix": pref, "title_key": tkey, "fingerprints": set(), "media_hint_tv": False}
            groups[skey] = g
        g["members"].append({
            "item": it, "prefix": pref, "base": base, "season": s, "episode": e, "tail": tail, "fp": fp
        })
        if fp:
            g["fingerprints"].add(fp)
        if is_series:
            g["media_hint_tv"] = True

        meta_list.append(g["members"][-1])

    # persist grouping metadata
    for skey, g in groups.items():
        sample_names = []
        for m in g["members"]:
            if len(sample_names) >= 5:
                break
            nm = m["item"].get("display_title") or m["item"].get("tvg-name") or ""
            if nm and nm not in sample_names:
                sample_names.append(nm)
        _groups_put(con, skey, g["title_key"], g["prefix"], sorted(g["fingerprints"]), sample_names)

    total_groups = len(groups)
    if total_groups == 0:
        return items

    print(f"🎬 TMDB enrichment (strict groups) for {total_groups} keys …")

    # -------- Phase B: one lookup per series_key --------
    sess = _requests_session(timeout)
    hits = misses = 0
    tick = 500
    done = 0

    for skey, g in groups.items():
        done += 1
        if (done % tick == 0) or (done == total_groups):
            print(f"   • TMDB [{done}/{total_groups}]")

        want_media = "tv" if g["media_hint_tv"] else "movie"
        cached = _cache_get(con, skey)
        if cached and cached.get("negative"):
            result = None
        elif cached and cached.get("media_type") == want_media and cached.get("name"):
            result = cached
        else:
            # Query is the *clean* version of the most common base
            # pick the longest base we saw (often most informative)
            bases = [m["base"] for m in g["members"] if m["base"]]
            query = sorted(bases, key=len, reverse=True)[0] if bases else g["title_key"]
            # crude year hint if present in any display
            year = None
            for m in g["members"]:
                rd = m["item"].get("display_title") or ""
                ym = re.search(r'\b(19|20)\d{2}\b', rd)
                if ym:
                    year = int(ym.group(0)); break

            result_raw = _tmdb_search(sess, api_key, query, want_media, lang, year)
            if result_raw:
                genre_ids = [int(x) for x in (result_raw.get("genre_ids") or []) if str(x).isdigit()]
                genres = _genre_names(want_media, genre_ids)
                result = {
                    "media_type": want_media,
                    "tmdb_id": result_raw.get("id"),
                    "name": (result_raw.get("name") or result_raw.get("title") or "").strip(),
                    "poster_path": (result_raw.get("poster_path") or "").strip(),
                    "genres": genres,
                    "lang": lang,
                    "negative": 0,
                }
            else:
                result = None

            _cache_put(con, skey, result or {
                "media_type": want_media, "tmdb_id": None, "name": "",
                "poster_path": "", "genres": [], "lang": lang, "negative": 1,
            })

        if result:
            hits += 1
            genre_name = (result.get("genres") or ["Uncategorized"])[0]
            proper = (result.get("name") or "").strip()

            for m in g["members"]:
                it = m["item"]; pref = m["prefix"]; s = m["season"]; e = m["episode"]; tail = m["tail"]
                it["group-title"] = f"|{pref}| - {genre_name}"
                p = result.get("poster_path") or ""
                if p:
                    it["tvg-logo"] = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{p}"

                base = m["base"] or proper or g["title_key"]
                if s is not None and e is not None:
                    it["display_title"] = f"{pref} - {proper or base} S{int(s):02d} E{int(e):02d}" + (f" {tail}" if tail else "")
                else:
                    it["display_title"] = f"{pref} - {proper or base}"
                if s is not None:
                    it["season"] = int(s)
                if e is not None:
                    it["episode"] = int(e)
        else:
            misses += 1
            for m in g["members"]:
                it = m["item"]; pref = m["prefix"]; s = m["season"]; e = m["episode"]; tail = m["tail"]
                it["group-title"] = f"|{pref}| - Uncategorized"
                base = m["base"] or g["title_key"]
                if s is not None and e is not None:
                    it["display_title"] = f"{pref} - {base} S{int(s):02d} E{int(e):02d}" + (f" {tail}" if tail else "")
                else:
                    it["display_title"] = f"{pref} - {base}"
                if s is not None:
                    it["season"] = int(s)
                if e is not None:
                    it["episode"] = int(e)

    if debug_stats:
        print(f"TMDB: {hits}/{total_groups} resolved; {misses} missed.")

    return items

def tmdb_enrich(items: List[Dict], cfg: Dict) -> List[Dict]:  # backwards-compat
    return enrich(items, cfg)

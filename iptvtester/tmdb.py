# -*- coding: utf-8 -*-
"""
TMDB enrichment (series-level only, no per-episode lookups)

• We normalize a "series key" from provider titles:
  - remove prefix (e.g., "EN -")
  - strip all Sxx Exx tokens
  - drop tech tags (4K/1080p/etc), country tags "(GB)", year noise, generic "Episode/Folge/Part" labels
  - lowercase + squeeze spaces

• We build an occurrence key to keep series separate when:
  - language/prefix differs (EN/DE/EX/EXYU…)
  - provider base (scheme + host + path without the last segment) differs
  - the cleaned series key differs

    Occurrence key: series:{lang}:{prefix}:{provider_base}:{series_key}

• ONE TMDB search per occurrence (first try /search/tv with progressive simplification;
  if no hit → /search/multi with the same candidates). We NEVER hit episode endpoints.

• Season/Episode + episode name are extracted from the provider title and:
  - stored in tmdb_series_members (persistent cache DB)
  - used when formatting the final display:    PREFIX - Series Sxx Eyy EpisodeName

• Group is overwritten to:  |{PREFIX}| - {TMDB Primary Genre}   (or "Uncategorized" if no hit)

This file only touches the TMDB cache DB (output/tmdb_cache.sqlite). No change to the main
inventory DB is required for persistence; the per-URL episode info is kept in tmdb_series_members.
"""

import json
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, List, Optional
from urllib.parse import urlparse

import requests

# ---------- prefix / title helpers ----------

def detect_prefix(group_title: str) -> str:
    if not group_title:
        return "EN"
    g = (group_title or "").strip()
    m = re.match(r'^\s*\|?([A-Z]{2,4})\|?\s*[\|\-]', g)  # allow EXYU
    if m: return m.group(1).upper()
    m = re.match(r'^\|?([A-Z]{2,4})\|?\b', g)
    if m: return m.group(1).upper()
    return "EN"

_PREFIX_LEAD_RE = re.compile(r'^\s*\|?([A-Z]{2,4})\|?\s*[-|]\s*', re.IGNORECASE)
def strip_leading_prefix(title: str) -> str:
    return _PREFIX_LEAD_RE.sub("", title or "").strip()

# tech/resolution/etc
_TECH_RE = re.compile(
    r'(?i)\b(4k|8k|2160p|1440p|1080p|720p|480p|uhd|hdr10\+?|hdr|dolby\s+vision|dv|h\.?264|x264|h\.?265|x265|hevc|aac|ddp(?:\.\d+)?|atmos|truehd|bd(?:rip)?|web[- ]?dl|web[- ]?rip|hdtv|remux)\b'
)
# country + year noise
_COUNTRY_TAG_RE        = re.compile(r'\(([A-Z]{2,4})\)')
_YEAR_RANGE_PAREN_RE   = re.compile(r'\(\s*(?:19|20)\d{2}\s*[-– ]\s*(?:19|20)\d{2}\s*\)')
_SINGLE_YEAR_PAREN_RE  = re.compile(r'[\(\[\{]\s*(19|20)\d{2}\s*[\)\]\}]')
_SINGLE_YEAR_TRAIL_RE  = re.compile(r'\b(19|20)\d{2}\b(?=\s*$)')
_SINGLE_YEAR_DASH_RE   = re.compile(r'\s*[-–—]\s*(?=(19|20)\d{2}\b)')
# generic episode descriptors (multi languages)
_EP_LABELS_RE = re.compile(
    r'(?i)\b(?:episode|folge|chapter|cap[ií]tulo|capitolo|episodio|part|teil)\s*[#:\-]?\s*[IVXLC\d]+\b'
)
# season/episode tokens; strip ALL, remember the first we saw
_SE_TOKEN_RE = re.compile(r'(?ix)\b(?:S\s*?(\d{1,2}))\s*(?:E\s*?(\d{1,2}))\b')

def _base_noise_strip(t: str) -> str:
    s = " " + (t or "") + " "
    s = _TECH_RE.sub(' ', s)
    s = _COUNTRY_TAG_RE.sub(' ', s)
    s = _YEAR_RANGE_PAREN_RE.sub(' ', s)
    s = _SINGLE_YEAR_PAREN_RE.sub(' ', s)
    s = _SINGLE_YEAR_TRAIL_RE.sub(' ', s)
    s = _SINGLE_YEAR_DASH_RE.sub(' ', s)
    s = _EP_LABELS_RE.sub(' ', s)
    s = re.sub(r'\s+', ' ', s).strip(' -–—\t\n\r')
    return s

def clean_display_title(t: str) -> str:
    return _base_noise_strip(t)

def _strip_all_se_tokens(text: str):
    """
    Remove *all* Sxx Exx tokens, while capturing the first pair we see.
    """
    if not text:
        return "", None, None
    s = " " + text + " "
    first_s = first_e = None
    def repl(m):
        nonlocal first_s, first_e
        if first_s is None and first_e is None:
            try:
                first_s = int(m.group(1)); first_e = int(m.group(2))
            except Exception:
                pass
        return ' '
    s = _SE_TOKEN_RE.sub(repl, s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s, first_s, first_e

def _series_key(title: str) -> str:
    """
    Robust *series-level* key:
      - strip leading prefix
      - remove ALL Sxx Exx tokens
      - remove tech tags, country tags, years, generic ep labels
      - lowercase, squeeze spaces
    """
    base = strip_leading_prefix(title or "")
    base, _, _ = _strip_all_se_tokens(base)
    base = _base_noise_strip(base)
    return re.sub(r'\s+', ' ', base).strip().lower()

def _normalize_series_and_episode(raw_title: str):
    """
    From a raw provider title:
      -> series_query (clean)
      -> season, episode
      -> ep_name (tail after series + S/E), cleaned (no S/E, no tech/year/country/labels)
    """
    no_prefix = strip_leading_prefix(raw_title or "")
    no_se, s, e = _strip_all_se_tokens(no_prefix)
    cleaned = _base_noise_strip(no_se)

    # ep_name guess: take the part after the series head if titles are like:
    #   "Friends S06 E05 The One with Joey's Porsche"
    # We try to split on ' Sxx Exx ' first.
    ep_name = ""
    parts = re.split(r'(?ix)\sS\d{1,2}\sE\d{1,2}\b', cleaned, maxsplit=1)
    series_query = cleaned
    if len(parts) == 2:
        series_query = parts[0].strip()
        ep_name     = parts[1].strip()

    # extra scrub on ep_name (remove any leftover S/E just in case)
    if ep_name:
        ep_name, _, _ = _strip_all_se_tokens(ep_name)
        ep_name = ep_name.strip()

    series_query = re.sub(r'\s+', ' ', series_query).strip()
    return series_query, s, e, ep_name

def _provider_base(url: str) -> str:
    try:
        p = urlparse(url)
        parts = (p.path or "/").rstrip("/").split("/")
        base_path = "/".join(parts[:-1]) if len(parts) > 1 else p.path
        return f"{p.scheme}://{p.netloc}{base_path}"
    except Exception:
        return url

# ---------- title simplification for TMDB search ----------

_DASH_SPLIT_RE = re.compile(r'\s[-–—]\s')

def _simplify_candidates(q: str, max_cands: int = 6) -> List[str]:
    """
    Generate progressively simpler queries:
      - original q
      - before first dash
      - before first parenthesis
      - before first colon (keep full as well)
      - token backoff down to 2 tokens
    """
    cands = []
    t = (q or "").strip()
    if not t:
        return cands

    cands.append(t)

    # before dash
    m = _DASH_SPLIT_RE.split(t, 1)
    if len(m) > 1 and m[0].strip():
        cands.append(m[0].strip())

    # before parenthesis
    p = t.split('(', 1)[0].strip()
    if p and p not in cands:
        cands.append(p)

    # before colon
    if ':' in t:
        pre = t.split(':', 1)[0].strip()
        if pre and pre not in cands:
            cands.append(pre)

    # token backoff down to 2 tokens
    toks = t.split()
    for keep in range(len(toks)-1, 1, -1):
        c = ' '.join(toks[:keep]).strip()
        if c and c not in cands:
            cands.append(c)
        if len(cands) >= max_cands:
            break

    return cands[:max_cands]

# ---------- tmdb cache (own sqlite) ----------

def _open_cache(path: str):
    con = sqlite3.connect(path, isolation_level=None, timeout=60.0)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("""
    CREATE TABLE IF NOT EXISTS tmdb_series_occ (
        key TEXT PRIMARY KEY,      -- series:{lang}:{prefix}:{provider_base}:{series_key}
        tmdb_id INTEGER,
        media_type TEXT,           -- 'tv' or 'movie'
        title TEXT,
        poster_path TEXT,
        lang TEXT,
        prefix TEXT,
        provider_base TEXT,
        series_key TEXT,
        primary_genre TEXT,        -- NAME
        created_at REAL,
        last_error TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS tmdb_series_members (
        key TEXT,                  -- occurrence key
        url TEXT,
        season INTEGER,
        episode INTEGER,
        ep_name TEXT,              -- parsed from provider title; persisted for later writes
        orig_title TEXT,
        PRIMARY KEY (key, url)
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS tmdb_genres (
        lang TEXT PRIMARY KEY,
        movie_json TEXT,
        tv_json TEXT,
        created_at REAL
    )
    """)
    # migrations
    try:
        con.execute("ALTER TABLE tmdb_series_members ADD COLUMN ep_name TEXT")
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE tmdb_series_occ ADD COLUMN last_error TEXT")
    except Exception:
        pass
    return con

def _get_genres(session: requests.Session, api_key: str, lang: str, con):
    cur = con.cursor()
    row = cur.execute("SELECT movie_json, tv_json FROM tmdb_genres WHERE lang=?", (lang,)).fetchone()
    if row:
        try:
            movie = json.loads(row[0] or "[]")
            tv = json.loads(row[1] or "[]")
            return {g["id"]: g["name"] for g in movie}, {g["id"]: g["name"] for g in tv}
        except Exception:
            pass
    base = "https://api.themoviedb.org/3/genre"
    def fetch(path):
        r = session.get(f"{base}/{path}", params={"api_key": api_key, "language": lang}, timeout=8)
        if r.status_code != 200:
            return []
        return r.json().get("genres") or []
    movie = fetch("movie/list")
    tv = fetch("tv/list")
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO tmdb_genres(lang, movie_json, tv_json, created_at) VALUES (?,?,?,?)",
                (lang, json.dumps(movie), json.dumps(tv), time.time()))
    return {g["id"]: g["name"] for g in movie}, {g["id"]: g["name"] for g in tv}

# ---------- TMDB HTTP ----------

def _session():
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "Accept-Encoding": "gzip, deflate"})
    return s

def _search_tv(session: requests.Session, api_key: str, lang: str, query: str) -> Optional[dict]:
    url = "https://api.themoviedb.org/3/search/tv"
    try:
        r = session.get(url, params={"api_key": api_key, "language": lang, "query": query, "include_adult": "true"}, timeout=8)
    except Exception as e:
        return {"error": f"request-error: {e}"}
    if r.status_code == 401:
        return {"error": "401 unauthorized (bad API key?)"}
    if r.status_code == 429:
        return {"error": "429 rate limited"}
    if r.status_code >= 500:
        return {"error": f"{r.status_code} server error"}
    if r.status_code != 200:
        return {"error": f"http {r.status_code}"}
    try:
        j = r.json()
    except Exception:
        return {"error": "non-json response"}
    results = j.get("results") or []
    if not results:
        return {"error": "no results"}
    best = results[0]
    return {
        "tmdb_id": best.get("id"),
        "media_type": "tv",
        "title": best.get("name"),
        "poster_path": best.get("poster_path") or "",
        "genre_ids": best.get("genre_ids") or [],
    }

def _search_multi(session: requests.Session, api_key: str, lang: str, query: str) -> Optional[dict]:
    url = "https://api.themoviedb.org/3/search/multi"
    try:
        r = session.get(url, params={"api_key": api_key, "language": lang, "query": query, "include_adult": "true"}, timeout=8)
    except Exception as e:
        return {"error": f"request-error: {e}"}
    if r.status_code == 401:
        return {"error": "401 unauthorized (bad API key?)"}
    if r.status_code == 429:
        return {"error": "429 rate limited"}
    if r.status_code >= 500:
        return {"error": f"{r.status_code} server error"}
    if r.status_code != 200:
        return {"error": f"http {r.status_code}"}
    try:
        j = r.json()
    except Exception:
        return {"error": "non-json response"}
    results = j.get("results") or []
    if not results:
        return {"error": "no results"}
    # prefer TV, else movie/other
    best = None
    for item in results:
        if item.get("media_type") == "tv":
            best = item; break
        if best is None:
            best = item
    if not best:
        return {"error": "empty selection"}
    return {
        "tmdb_id": best.get("id"),
        "media_type": best.get("media_type"),
        "title": best.get("name") or best.get("title"),
        "poster_path": best.get("poster_path") or "",
        "genre_ids": best.get("genre_ids") or [],
    }

def _fetch_detail_genre(session, api_key, lang, media_type, tmdb_id) -> Optional[str]:
    if not tmdb_id or media_type not in ("movie", "tv"):
        return None
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
    try:
        r = session.get(url, params={"api_key": api_key, "language": lang}, timeout=8)
    except Exception:
        return None
    if r.status_code != 200:
        return None
    try:
        g = (r.json() or {}).get("genres") or []
    except Exception:
        return None
    return (g[0]["name"].strip() if g and g[0].get("name") else None)

# ---------- public: enrich (series-occurrence level, no ep lookups) ----------

def enrich(items: List[dict], cfg, con_main=None) -> List[dict]:
    """
    ONE TMDB lookup per series occurrence (prefix+provider base+series key).
    No episode-name API calls. We only use provider ep_name (parsed) when formatting.
    """
    if not items:
        return items

    api_key = (cfg["TMDB"]["API_KEY"] or "").strip()
    lang = cfg["TMDB"]["LANGUAGE"]
    debug = bool(cfg["TMDB"].get("DEBUG_STATS", False))
    if not api_key:
        return items  # pipeline will warn

    sess = _session()
    con = _open_cache(cfg["TMDB"]["CACHE_DB_PATH"])
    cur = con.cursor()
    movie_gmap, tv_gmap = _get_genres(sess, api_key, lang, con)

    # Build occurrences + remember per-item meta
    occ_index: Dict[str, Dict] = {}
    for idx, it in enumerate(items):
        raw = it.get("title") or it.get("tvg-name") or it.get("orig_title") or ""
        series_query, season, episode, ep_name = _normalize_series_and_episode(raw)
        sk = re.sub(r'\s+', ' ', series_query).strip().lower()

        pf = (it.get("orig_prefix") or detect_prefix(it.get("orig_group") or it.get("group-title") or "")).upper()
        base = _provider_base(it.get("url") or "")
        occ_key = f"series:{lang}:{pf}:{base}:{sk}"

        d = occ_index.setdefault(occ_key, {
            "prefix": pf, "provider_base": base, "series_key": sk, "indices": [],
            "sample_q": series_query
        })
        d["indices"].append((idx, season, episode, ep_name))

    # Save members BEFORE edits (persist ep_name here)
    cur.execute("BEGIN IMMEDIATE;")
    now = time.time()
    for occ_key, meta in occ_index.items():
        cur.execute("""INSERT OR IGNORE INTO tmdb_series_occ
            (key, tmdb_id, media_type, title, poster_path, lang, prefix, provider_base, series_key, primary_genre, created_at, last_error)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (occ_key, None, None, None, None, lang, meta["prefix"], meta["provider_base"], meta["series_key"], None, now, None)
        )
        for (idx, s, e, ep_name) in meta["indices"]:
            it = items[idx]
            cur.execute("""INSERT OR REPLACE INTO tmdb_series_members
                           (key, url, season, episode, ep_name, orig_title)
                           VALUES (?,?,?,?,?,?)""",
                        (occ_key, it.get("url"), int(s) if s else None, int(e) if e else None,
                         ep_name or "", it.get("title") or it.get("tvg-name") or it.get("orig_title") or ""))
    con.commit()

    # Load cached positives
    cached_occ: Dict[str, dict] = {}
    for occ_key in occ_index.keys():
        row = cur.execute("""SELECT tmdb_id,media_type,title,poster_path,lang,prefix,provider_base,series_key,primary_genre,created_at,last_error
                               FROM tmdb_series_occ WHERE key=?""", (occ_key,)).fetchone()
        if row and row[1] in ("tv","movie") and row[0]:
            cached_occ[occ_key] = {
                "tmdb_id": row[0], "media_type": row[1], "title": row[2], "poster_path": row[3],
                "lang": row[4], "prefix": row[5], "provider_base": row[6], "series_key": row[7],
                "primary_genre": row[8], "created_at": row[9], "last_error": row[10]
            }

    to_fetch = [k for k in occ_index.keys() if k not in cached_occ]
    fetched_occ: Dict[str, dict] = {}

    # Search helper
    def _try_tmdb(series_q: str) -> Optional[dict]:
        # TV first, then multi, with progressive simplification
        cands = _simplify_candidates(series_q)
        for q in cands:
            res = _search_tv(sess, api_key, lang, q)
            if res and "error" not in res:
                return res
        for q in cands:
            res = _search_multi(sess, api_key, lang, q)
            if res and "error" not in res:
                return res
        return None

    if to_fetch:
        def worker(occ_key: str):
            parts   = occ_key.split(":", 4)
            lang_p, pf_p, base_p, sk_part = parts[1], parts[2], parts[3], parts[4]
            # pretty cased query from sk_part
            sk_pretty = " ".join([w.capitalize() if len(w) > 2 else w for w in sk_part.split()])
            first_q   = occ_index[occ_key].get("sample_q") or sk_pretty
            first_q   = re.sub(r'\s+', ' ', first_q).strip()
            first_q   = " ".join([w.capitalize() if len(w) > 2 else w for w in first_q.split()])

            res = _try_tmdb(first_q) or _try_tmdb(sk_pretty)
            if not res:
                return occ_key, {"negative": 1, "last_error": "no results"}

            media_type = res.get("media_type")
            tmdb_id    = res.get("tmdb_id")
            poster     = res.get("poster_path") or ""
            title      = res.get("title") or first_q

            # genre
            genre_name = None
            ids = res.get("genre_ids") or []
            if media_type == "tv":
                for gid in ids:
                    if gid in tv_gmap: genre_name = tv_gmap[gid]; break
            elif media_type == "movie":
                for gid in ids:
                    if gid in movie_gmap: genre_name = movie_gmap[gid]; break
            if not genre_name:
                genre_name = _fetch_detail_genre(sess, api_key, lang, media_type, tmdb_id)

            return occ_key, {
                "tmdb_id": tmdb_id,
                "media_type": media_type,
                "title": title,
                "poster_path": poster,
                "lang": lang_p,
                "prefix": pf_p,
                "provider_base": base_p,
                "series_key": sk_part,
                "primary_genre": genre_name,
                "negative": 0,
                "last_error": None
            }

        maxw = max(1, int(cfg["TMDB"]["PARALLELISM"]))
        with ThreadPoolExecutor(max_workers=maxw) as ex:
            futs = [ex.submit(worker, k) for k in to_fetch]
            for fut in as_completed(futs):
                k, row = fut.result()
                fetched_occ[k] = row

        # persist all (including negatives)
        rows = []
        for k, v in fetched_occ.items():
            rows.append((k, v.get("tmdb_id"), v.get("media_type"), v.get("title"), v.get("poster_path"),
                         v.get("lang"), v.get("prefix"), v.get("provider_base"), v.get("series_key"),
                         v.get("primary_genre"), time.time(), v.get("last_error")))
        cur.executemany("""INSERT OR REPLACE INTO tmdb_series_occ
            (key, tmdb_id, media_type, title, poster_path, lang, prefix, provider_base, series_key, primary_genre, created_at, last_error)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
        con.commit()

    # Resolve usable rows
    all_occ = {}
    for k in occ_index.keys():
        row = cur.execute("""SELECT tmdb_id,media_type,title,poster_path,lang,prefix,provider_base,series_key,primary_genre,last_error
                               FROM tmdb_series_occ WHERE key=?""", (k,)).fetchone()
        if row and row[1] in ("tv","movie") and row[0]:
            all_occ[k] = {
                "tmdb_id": row[0], "media_type": row[1], "title": row[2], "poster_path": row[3],
                "lang": row[4], "prefix": row[5], "provider_base": row[6], "series_key": row[7],
                "primary_genre": row[8], "last_error": row[9]
            }

    # Apply to items (use provider ep_name; no TMDB episode calls)
    for occ_key, meta in occ_index.items():
        occ_row = all_occ.get(occ_key)  # None on miss
        pf = meta["prefix"]
        for (idx, s, e, ep_name) in meta["indices"]:
            it = items[idx]
            raw = it.get("title") or it.get("tvg-name") or it.get("orig_title") or ""
            # Fallback base series name from provider
            series_query, _, _, _ = _normalize_series_and_episode(raw)

            media_type = None
            poster = ""
            genre_name = None
            series_title = series_query  # fallback

            if occ_row:
                media_type   = occ_row.get("media_type")
                series_title = _base_noise_strip(strip_leading_prefix(occ_row.get("title") or series_query))
                poster       = occ_row.get("poster_path") or ""
                genre_name   = occ_row.get("primary_genre")

            # Assemble final display: PREFIX - Series Sxx Eyy EpisodeName
            se_part = f" S{s:02d} E{e:02d}" if isinstance(s, int) and isinstance(e, int) else ""
            ep_tail = f" {ep_name}" if ep_name else ""
            display = f"{pf} - {series_title}{se_part}{ep_tail}".strip()

            it["display_title"] = display
            it["title"] = display
            it["tvg-name"] = display
            it["season"] = s
            it["episode"] = e
            if poster:
                it["tvg-logo"] = f"https://image.tmdb.org/t/p/w154{poster}"

            # Overwrite group using TMDB genre if available
            if media_type in ("movie", "tv"):
                gname = genre_name or "Uncategorized"
                it["group-title"] = f"|{pf}| - {gname}"
            else:
                it["group-title"] = f"|{pf}| - Uncategorized"

    if debug:
        total = len(occ_index)
        resolved = len(all_occ)
        print(f"TMDB occurrences: {resolved}/{total} resolved; {total - resolved} missed.")
        if total and resolved == 0:
            samples = []
            for k, meta in list(occ_index.items())[:5]:
                samples.append(meta.get("sample_q"))
            print("TMDB sample queries (first 5):", "; ".join([s or "" for s in samples]))
    return items

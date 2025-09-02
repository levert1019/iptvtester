# -*- coding: utf-8 -*-
import os, sqlite3, threading, json, requests, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .utils import now_local_iso, tick_progress, lang_to_prefix

# in-run negative cache
_NEG = set()
_httpx = None

def _normalize_title(q: str) -> str:
    q = (q or "").strip()
    q = re.sub(r"\s+", " ", q)
    q = re.sub(r"\bS(?:taffel)?\s*\d+\b.*", "", q, flags=re.I)
    q = re.sub(r"\bS\d{1,2}E\d{1,2}\b", "", q, flags=re.I)
    return q.strip()

def _requests_session():
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "Accept-Encoding": "gzip, deflate"})
    retry = Retry(total=2, backoff_factor=0.4, status_forcelist=(429,500,502,503,504),
                  allowed_methods=frozenset(["GET"]), raise_on_status=False)
    ad = HTTPAdapter(max_retries=retry, pool_connections=128, pool_maxsize=128)
    s.mount("http://", ad); s.mount("https://", ad)
    return s

def _open_cache_db(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path, isolation_level=None, timeout=60.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA mmap_size=3000000000;")
    con.execute("""
    CREATE TABLE IF NOT EXISTS tmdb_cache (
      key TEXT PRIMARY KEY,
      tmdb_id INTEGER,
      media_type TEXT,
      title TEXT,
      poster_path TEXT,
      lang TEXT,
      created_at TEXT,
      negative INTEGER DEFAULT 0,
      genre_ids TEXT,              -- NEW: JSON list of genre ids
      primary_genre TEXT           -- NEW: resolved genre name
    )""")
    # Safe schema upgrades if previous file exists:
    try: con.execute("ALTER TABLE tmdb_cache ADD COLUMN genre_ids TEXT")
    except Exception: pass
    try: con.execute("ALTER TABLE tmdb_cache ADD COLUMN primary_genre TEXT")
    except Exception: pass

    con.execute("""
    CREATE TABLE IF NOT EXISTS tmdb_genres (
      lang TEXT PRIMARY KEY,
      movie_json TEXT,
      tv_json TEXT,
      created_at TEXT
    )""")
    return con

class RateLimiter:
    def __init__(self, rps: float):
        self.lock = threading.Lock()
        self.min_interval = 1.0 / max(1.0, float(rps))
        self.next_time = 0.0
    def acquire(self):
        with self.lock:
            import time
            now = time.perf_counter()
            if self.next_time <= now:
                self.next_time = now + self.min_interval; return
            sleep_for = self.next_time - now
        if sleep_for > 0:
            import time; time.sleep(sleep_for)
        with self.lock:
            import time; self.next_time = time.perf_counter() + self.min_interval

def _tmdb_search_multi(session, api_key, q, lang, limiter: RateLimiter):
    base = "https://api.themoviedb.org/3/search/multi"
    from requests.utils import quote
    url = f"{base}?api_key={api_key}&language={lang}&query={quote(q)}"
    limiter.acquire()
    r = session.get(url, timeout=8)
    if r.status_code != 200:
        return None
    j = r.json()
    res = j.get("results") or []
    if not res: return None
    best = None
    for item in res:
        if item.get("media_type") == "tv":
            best = item; break
        if best is None:
            best = item
    if not best: return None
    media_type = best.get("media_type")
    title = best.get("name") or best.get("title") or q
    poster = best.get("poster_path") or ""
    genre_ids = best.get("genre_ids") or []
    return {"media_type": media_type, "title": title, "poster_path": poster, "genre_ids": genre_ids, "tmdb_id": best.get("id")}

def _fetch_genres(session, api_key, lang, con_cache):
    # cache by language
    row = con_cache.execute("SELECT * FROM tmdb_genres WHERE lang=?", (lang,)).fetchone()
    if row:
        try:
            movie = json.loads(row["movie_json"] or "[]"); tv = json.loads(row["tv_json"] or "[]")
            return movie, tv
        except Exception:
            pass
    base = "https://api.themoviedb.org/3/genre"
    def get(path):
        url = f"{base}/{path}?api_key={api_key}&language={lang}"
        r = session.get(url, timeout=8)
        if r.status_code != 200:
            return []
        return r.json().get("genres", []) or []
    movie = get("movie/list")
    tv    = get("tv/list")
    con_cache.execute("INSERT OR REPLACE INTO tmdb_genres(lang, movie_json, tv_json, created_at) VALUES (?,?,?,?)",
                      (lang, json.dumps(movie), json.dumps(tv), now_local_iso()))
    return movie, tv

def _resolve_primary_genre(genre_ids, movie_genres, tv_genres, media_type):
    if not genre_ids:
        return None
    table = movie_genres if media_type == "movie" else tv_genres
    gmap = {g["id"]: g["name"] for g in table if "id" in g and "name" in g}
    for gid in genre_ids:
        name = gmap.get(gid)
        if name:
            return name
    return None

def enrich(items, cfg):
    tm = cfg["TMDB"]
    if not tm["ENABLE"] or not tm["API_KEY"] or not items:
        return items

    lang = tm["LANGUAGE"]
    prefix = lang_to_prefix(lang)
    http2 = bool(tm["HTTP2"])
    rps = min(int(tm["RATE_LIMIT_RPS"]), 50)
    par = max(1, int(tm["PARALLELISM"]))
    limiter = RateLimiter(rps)

    # session
    session = None
    if http2:
        try:
            import httpx  # noqa
            global _httpx; _httpx = httpx
        except Exception:
            _httpx = None
    if _httpx is None:
        session = _requests_session()

    # dedupe jobs
    jobs = []
    key_to_indices = {}
    for idx, it in enumerate(items):
        q = _normalize_title(it.get("title") or it.get("tvg-name") or "")
        if not q: continue
        key = f"multi:{lang}:{q}"
        key_to_indices.setdefault(key, []).append(idx)
        if key not in _NEG:
            jobs.append((key, q))
    # open cache DB
    con_cache = _open_cache_db(tm["CACHE_DB_PATH"])

    # warm cache
    cached, to_fetch = {}, []
    for key, q in jobs:
        row = con_cache.execute("SELECT * FROM tmdb_cache WHERE key=?", (key,)).fetchone()
        if row is not None:
            row = dict(row)
            cached[key] = row
            if row.get("negative"):
                _NEG.add(key)
        else:
            to_fetch.append((key, q))

    # genre lists (once)
    movie_genres, tv_genres = _fetch_genres(session or _httpx.Client(http2=True), tm["API_KEY"], lang, con_cache) if tm["APPLY_GENRE_GROUPING"] else ([], [])

    # fetch missing
    fetched = {}
    def worker(job):
        key, q = job
        if tm["USE_MULTI_SEARCH"]:
            if _httpx is not None:
                with _httpx.Client(http2=True, timeout=tm["TIMEOUT_SECONDS"]) as client:
                    limiter.acquire()
                    url = f"https://api.themoviedb.org/3/search/multi?api_key={tm['API_KEY']}&language={lang}&query={requests.utils.quote(q)}"
                    r = client.get(url)
                    if r.status_code != 200:
                        res = None
                    else:
                        j = r.json(); reslist = j.get("results") or []
                        best = None
                        for item in reslist:
                            if item.get("media_type") == "tv":
                                best = item; break
                            if best is None:
                                best = item
                        if best:
                            res = {"media_type": best.get("media_type"),
                                   "title": best.get("name") or best.get("title") or q,
                                   "poster_path": best.get("poster_path") or "",
                                   "genre_ids": best.get("genre_ids") or [],
                                   "tmdb_id": best.get("id")}
                        else:
                            res = None
            else:
                res = _tmdb_search_multi(session, tm["API_KEY"], q, lang, limiter)
        else:
            res = None

        if not res:
            row = {"key": key, "tmdb_id": None, "media_type": None, "title": None,
                   "poster_path": "", "lang": lang, "created_at": now_local_iso(),
                   "negative": 1, "genre_ids": "[]", "primary_genre": None}
            _NEG.add(key)
        else:
            primary = None
            if tm["APPLY_GENRE_GROUPING"]:
                primary = _resolve_primary_genre(res.get("genre_ids") or [], movie_genres, tv_genres, res.get("media_type"))
            row = {"key": key, "tmdb_id": res["tmdb_id"], "media_type": res["media_type"],
                   "title": res["title"], "poster_path": res["poster_path"], "lang": lang,
                   "created_at": now_local_iso(), "negative": 0,
                   "genre_ids": json.dumps(res.get("genre_ids") or []), "primary_genre": primary}
        return key, row

    if to_fetch:
        with ThreadPoolExecutor(max_workers=par) as ex:
            futures = [ex.submit(worker, j) for j in to_fetch]
            done = 0; total = len(futures)
            for fut in as_completed(futures):
                key, row = fut.result(); fetched[key] = row
                done += 1; tick_progress(done, total, prefix="   • TMDB", quiet=cfg["MISC"]["QUIET"])

    # bulk write fetched
    if fetched:
        con_cache.execute("BEGIN IMMEDIATE;")
        con_cache.executemany(
            "INSERT OR REPLACE INTO tmdb_cache(key, tmdb_id, media_type, title, poster_path, lang, created_at, negative, genre_ids, primary_genre) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(
                r["key"], r.get("tmdb_id"), r.get("media_type"), r.get("title"),
                r.get("poster_path",""), r.get("lang", lang), r.get("created_at", now_local_iso()),
                r.get("negative", 0), r.get("genre_ids","[]"), r.get("primary_genre")
            ) for r in fetched.values()]
        )
        con_cache.commit()

    all_rows = {**{k: dict(v) for k,v in cached.items()}, **fetched}

    # apply to items (+ genre-based group naming)
    for key, idx_list in key_to_indices.items():
        row = all_rows.get(key)
        if not row or row.get("negative"):
            continue
        poster = row.get("poster_path") or ""
        title  = row.get("title")
        media_type = row.get("media_type")
        primary_genre = row.get("primary_genre")
        for idx in idx_list:
            it = items[idx]
            if title:  it["title"] = title
            if poster: it["tvg-logo"] = f"https://image.tmdb.org/t/p/w500{poster}"
            # genre grouping only for movies/series
            if cfg["TMDB"]["APPLY_GENRE_GROUPING"] and media_type in ("movie", "tv"):
                genre = primary_genre or "Uncategorized"
                it["group-title"] = f"{prefix} - {genre}"
    return items

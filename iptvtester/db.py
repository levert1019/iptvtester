# -*- coding: utf-8 -*-
import sqlite3
from typing import Iterable, Dict, List, Tuple

def ensure_db(path: str):
    con = sqlite3.connect(path, isolation_level=None, timeout=60.0)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA cache_size=-200000;")  # ~200MB shared cache if available

    # Main inventory table
    con.execute("""
    CREATE TABLE IF NOT EXISTS streams (
        url TEXT PRIMARY KEY,
        title TEXT,
        group_title TEXT,
        tvg_id TEXT,
        tvg_name TEXT,
        tvg_logo TEXT,
        last_checked REAL,
        status TEXT,              -- 'OK' or 'FAIL'
        last_ok REAL,
        fail_count INTEGER DEFAULT 0,
        -- preserved originals
        orig_group TEXT,
        orig_prefix TEXT,
        orig_title TEXT,
        -- series occurrence mapping (filled by TMDB enrichment)
        series_occ_key TEXT,
        series_key TEXT,
        occ_prefix TEXT,
        provider_base TEXT,
        season INTEGER,
        episode INTEGER
    )
    """)

    # Backfill columns for older DBs
    def _add(col_def: str):
        try: con.execute(f"ALTER TABLE streams ADD COLUMN {col_def}")
        except Exception: pass

    _add("orig_group TEXT")
    _add("orig_prefix TEXT")
    _add("orig_title TEXT")
    _add("series_occ_key TEXT")
    _add("series_key TEXT")
    _add("occ_prefix TEXT")
    _add("provider_base TEXT")
    _add("season INTEGER")
    _add("episode INTEGER")

    return con

def bulk_stage_and_upsert(con, items: List[Dict], chunk: int = 50000, quiet: bool = False):
    cur = con.cursor()
    # ensure row existence
    cur.executemany("INSERT OR IGNORE INTO streams(url) VALUES (?)",
                    [(it["url"],) for it in items])
    # upsert metadata (preserve originals)
    rows = []
    for it in items:
        rows.append((
            it.get("title") or it.get("tvg-name") or it.get("orig_title") or "",
            it.get("group-title") or "",
            it.get("tvg-id") or "",
            it.get("tvg-name") or "",
            it.get("tvg-logo") or "",
            it.get("orig_group") or "",
            it.get("orig_prefix") or "",
            it.get("orig_title") or "",
            it["url"],
        ))
    cur.executemany("""
        UPDATE streams
           SET title=?,
               group_title=?,
               tvg_id=?,
               tvg_name=?,
               tvg_logo=?,
               orig_group=?,
               orig_prefix=?,
               orig_title=?
         WHERE url=?""", rows)
    con.commit()

def fetch_all_streams(con) -> List[Dict]:
    cur = con.cursor()
    cur.execute("""SELECT url,title,group_title,tvg_id,tvg_name,tvg_logo,
                          last_checked,status,last_ok,fail_count,
                          orig_group,orig_prefix,orig_title,
                          series_occ_key,series_key,occ_prefix,provider_base,season,episode
                     FROM streams""")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def due_for_probe(con, ok_hours: float, fail_minutes: float, limit: int = None) -> List[Dict]:
    """
    Return only rows that should be ffprobed based on recheck windows:
      - OK rows: recheck if now >= last_checked + ok_hours
      - FAIL rows: retry if now >= last_checked + fail_minutes
      - Never-checked rows: include
    """
    cur = con.cursor()
    q = f"""
    SELECT url,title,group_title,tvg_id,tvg_name,tvg_logo,
           last_checked,status,last_ok,fail_count,
           orig_group,orig_prefix,orig_title,
           series_occ_key,series_key,occ_prefix,provider_base,season,episode
      FROM streams
     WHERE last_checked IS NULL
        OR (status='OK'   AND strftime('%s','now') - last_checked >= ?)
        OR (status='FAIL' AND strftime('%s','now') - last_checked >= ?)
    """
    if limit and limit > 0:
        q += " LIMIT ?"
        cur.execute(q, (ok_hours*3600.0, fail_minutes*60.0, int(limit)))
    else:
        cur.execute(q, (ok_hours*3600.0, fail_minutes*60.0))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def update_series_mapping(con, mappings: List[Tuple[str, str, str, str, int, int]]):
    """
    Update series occurrence mapping columns on streams for a bunch of URLs.
    mappings: list of tuples (url, series_occ_key, series_key, occ_prefix, season, episode)
              provider_base is derivable from series_occ_key but we keep a column too.
    """
    if not mappings:
        return
    cur = con.cursor()
    for url, occ_key, skey, pf, season, episode in mappings:
        # provider_base is everything between the 3rd and 4th colon in occ_key: series:{lang}:{prefix}:{provider_base}:{series_key}
        provider_base = None
        try:
            parts = occ_key.split(":", 4)
            if len(parts) == 5:
                provider_base = parts[3]
        except Exception:
            provider_base = None
        cur.execute("""
            UPDATE streams
               SET series_occ_key=?,
                   series_key=?,
                   occ_prefix=?,
                   provider_base=?,
                   season=?,
                   episode=?
             WHERE url=?""",
            (occ_key, skey, pf, provider_base, season, episode, url)
        )
    con.commit()

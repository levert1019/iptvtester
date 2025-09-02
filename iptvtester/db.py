# -*- coding: utf-8 -*-
import os, sqlite3
from .utils import now_local_iso, clean_title, tick_progress

def open_main(path, defer_indexes=False):
    new_db = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path, isolation_level=None)
    con.row_factory = sqlite3.Row
    if new_db:
        con.execute("PRAGMA page_size=32768;")
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=OFF;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA mmap_size=30000000000;")
    con.execute("PRAGMA cache_size=-160000;")
    con.execute("PRAGMA locking_mode=EXCLUSIVE;")
    con.execute("PRAGMA foreign_keys=OFF;")

    con.execute("""
    CREATE TABLE IF NOT EXISTS streams (
      url TEXT PRIMARY KEY,
      title TEXT,
      group_title TEXT,
      tvg_id TEXT,
      tvg_name TEXT,
      tvg_logo TEXT,
      last_checked TEXT,
      status TEXT,
      last_ok TEXT,
      fail_count INTEGER DEFAULT 0
    )""")

    con.execute("""
    CREATE TABLE IF NOT EXISTS title_registry (
      url TEXT PRIMARY KEY,
      base_title TEXT,
      unique_title TEXT,
      updated_at TEXT
    )""")

    con.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")

    if new_db:
        con.execute("VACUUM;")
    if not defer_indexes:
        con.execute("CREATE INDEX IF NOT EXISTS idx_streams_status ON streams(status);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_streams_group  ON streams(group_title);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_title_unique   ON title_registry(unique_title);")
    return con

def meta_get(con, k):
    r = con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return r[0] if r else None

def meta_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (k, v))
    con.commit()

def drop_indexes(con):
    con.execute("DROP INDEX IF EXISTS idx_streams_status;")
    con.execute("DROP INDEX IF EXISTS idx_streams_group;")
    con.execute("DROP INDEX IF EXISTS idx_title_unique;")

def create_indexes(con):
    con.execute("CREATE INDEX IF NOT EXISTS idx_streams_status ON streams(status);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_streams_group  ON streams(group_title);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_title_unique   ON title_registry(unique_title);")

def migrate_without_rowid(con):
    con.execute("BEGIN IMMEDIATE;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS streams_new (
            url TEXT PRIMARY KEY,
            title TEXT,
            group_title TEXT,
            tvg_id TEXT,
            tvg_name TEXT,
            tvg_logo TEXT,
            last_checked TEXT,
            status TEXT,
            last_ok TEXT,
            fail_count INTEGER DEFAULT 0
        ) WITHOUT ROWID;
    """)
    con.execute("INSERT OR REPLACE INTO streams_new SELECT * FROM streams;")
    con.execute("DROP TABLE streams;")
    con.execute("ALTER TABLE streams_new RENAME TO streams;")
    con.commit()

def ingest_stage_merge(con, items, chunk=50000, quiet=False):
    con.execute("DROP TABLE IF EXISTS streams_stage;")
    con.execute("""
        CREATE TEMP TABLE streams_stage (
            url TEXT PRIMARY KEY,
            title TEXT,
            group_title TEXT,
            tvg_id TEXT,
            tvg_name TEXT,
            tvg_logo TEXT
        ) WITHOUT ROWID;
    """)

    seen = set(); rows=[]
    for it in items:
        u = it["url"]
        if u in seen: continue
        seen.add(u)
        rows.append((
            u, it.get("title",""), it.get("group-title",""),
            it.get("tvg-id",""), it.get("tvg-name",""), it.get("tvg-logo",""),
        ))

    sql_ins = "INSERT OR REPLACE INTO streams_stage(url,title,group_title,tvg_id,tvg_name,tvg_logo) VALUES (?,?,?,?,?,?)"
    cur = con.cursor(); cur.execute("BEGIN IMMEDIATE;")
    total = len(rows)
    for i in range(0, total, chunk):
        cur.executemany(sql_ins, rows[i:i+chunk])
        if not quiet:
            done = min(i+chunk, total)
            tick_progress(done, total, prefix="   • Stage:", quiet=quiet)
    con.commit()

    if not quiet: print("   • Merging stage → streams …")
    con.execute("BEGIN IMMEDIATE;")
    con.execute("""
        UPDATE streams
           SET title       = (SELECT s.title       FROM streams_stage s WHERE s.url = streams.url),
               group_title = (SELECT s.group_title FROM streams_stage s WHERE s.url = streams.url),
               tvg_id      = (SELECT s.tvg_id      FROM streams_stage s WHERE s.url = streams.url),
               tvg_name    = (SELECT s.tvg_name    FROM streams_stage s WHERE s.url = streams.url),
               tvg_logo    = (SELECT s.tvg_logo    FROM streams_stage s WHERE s.url = streams.url)
         WHERE url IN (SELECT url FROM streams_stage);
    """)
    con.commit()

    con.execute("BEGIN IMMEDIATE;")
    con.execute("""
        INSERT INTO streams (url, title, group_title, tvg_id, tvg_name, tvg_logo)
        SELECT s.url, s.title, s.group_title, s.tvg_id, s.tvg_name, s.tvg_logo
          FROM streams_stage s
         WHERE NOT EXISTS (SELECT 1 FROM streams t WHERE t.url = s.url);
    """)
    con.commit()
    con.execute("DROP TABLE IF EXISTS streams_stage;")

def assign_unique_titles(con, items, chunk=50000, quiet=False):
    counts = {}; reg_rows=[]
    for it in items:
        url = it["url"]
        base = clean_title(it.get("title") or it.get("tvg-name") or it.get("raw_title") or url)
        n = counts.get(base, 0) + 1; counts[base] = n
        uniq = base if n == 1 else f"{base} #{n}"
        it["title"] = it["tvg-name"] = uniq
        reg_rows.append((url, base, uniq, now_local_iso()))
    sql = "INSERT OR REPLACE INTO title_registry(url, base_title, unique_title, updated_at) VALUES (?,?,?,?)"
    cur = con.cursor(); cur.execute("BEGIN IMMEDIATE;")
    total = len(reg_rows)
    for i in range(0, total, chunk):
        cur.executemany(sql, reg_rows[i:i+chunk])
        if not quiet:
            done = min(i+chunk, total)
            tick_progress(done, total, prefix="   • Titles:", quiet=quiet)
    con.commit()

def fetch_all_streams(con):
    cur = con.execute("SELECT url,title,group_title,tvg_name,tvg_logo,last_checked,status,last_ok,fail_count FROM streams")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def save_probe_results(con, results, quiet=False):
    ok_rows, fail_rows = [], []
    nowiso = now_local_iso()
    for (it, ok, note) in results:
        if ok:   ok_rows.append((nowiso, nowiso, it['url']))
        else:    fail_rows.append((nowiso, it['url']))
    cur = con.cursor(); cur.execute("BEGIN IMMEDIATE;")
    if ok_rows:
        cur.executemany("UPDATE streams SET status='OK', last_checked=?, last_ok=?, fail_count=0 WHERE url=?", ok_rows)
    if fail_rows:
        cur.executemany("UPDATE streams SET status='FAIL', last_checked=?, fail_count=COALESCE(fail_count,0)+1 WHERE url=?", fail_rows)
    con.commit()
    if not quiet: print(f"💾 Saved: {len(ok_rows)} OK, {len(fail_rows)} FAIL")

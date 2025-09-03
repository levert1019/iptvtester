# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from dateutil.tz import tzlocal
from .http_client import load_m3u_text
from .m3u import parse, write
from .db import (open_main, ingest_stage_merge, assign_unique_titles, fetch_all_streams,
                 save_probe_results, meta_get, meta_set, drop_indexes, create_indexes,
                 migrate_without_rowid)
from .probe import run_probe
from .tmdb import enrich
from .excel import export, classify_error
from .utils import sha1_text

def _passes_filters(it, cfg):
    grp = it.get("group-title","")
    gf = cfg["GROUP_FILTERS"]
    inc = gf.get("INCLUDE_GROUPS") or []
    exc = gf.get("EXCLUDE_GROUPS") or []
    def match_any(name, needles):
        n = (name or "").lower()
        return any(s.lower() in n for s in needles)
    if inc and not match_any(grp, inc): return False
    if exc and match_any(grp, exc):     return False
    return True

def _needs_check(row):
    if not row: return True
    last = row.get("last_checked")
    if not last: return True
    try:
        dt = datetime.fromisoformat(last)
        if datetime.now(tzlocal()) - dt > timedelta(days=1):
            return True
    except Exception:
        return True
    return row.get("status") != "OK"

def run_once(cfg):
    if not cfg["MISC"]["QUIET"]:
        print("🚀 Start IPTV-Tester")

    # Load playlist text
    text = load_m3u_text(cfg, cfg["SOURCE_M3U"], cfg["_CLI_HTTP"])

    # DB
    con = open_main(cfg["OUTPUTS"]["DB_PATH"], defer_indexes=cfg["ADVANCED"]["REBUILD_INDEXES"])
    if cfg["ADVANCED"]["MIGRATE_STREAMS_NO_ROWID"]:
        print("🔧 Migrating streams → WITHOUT ROWID …")
        migrate_without_rowid(con)

    # SHA-1
    last_hash = meta_get(con, "last_playlist_sha1")
    curr_hash = sha1_text(text)

    # Parse & filter
    items = parse(text)
    if not cfg["MISC"]["QUIET"]:
        print(f"🔎 Entries detected: {len(items)}")

    gf = cfg["GROUP_FILTERS"]
    if gf["PROCESS_ONLY_INCLUDED_GROUPS"]:
        items = [it for it in items if _passes_filters(it, cfg)]
        if not cfg["MISC"]["QUIET"]:
            print(f"✅ After group filter: {len(items)}")

    # Ingest gate
    if last_hash != curr_hash:
        if cfg["ADVANCED"]["REBUILD_INDEXES"]:
            drop_indexes(con)
        if not cfg["MISC"]["QUIET"]:
            print("🗂️  Updating inventory DB …")
        ingest_stage_merge(con, items, chunk=50000, quiet=cfg["MISC"]["QUIET"])
        assign_unique_titles(con, items, chunk=50000, quiet=cfg["MISC"]["QUIET"])
        if cfg["ADVANCED"]["REBUILD_INDEXES"]:
            if not cfg["MISC"]["QUIET"]:
                print("🧱 Rebuilding indexes …")
            create_indexes(con)
        meta_set(con, "last_playlist_sha1", curr_hash)
        if not cfg["MISC"]["QUIET"]:
            print("✅ Inventory updated.")
    else:
        if not cfg["MISC"]["QUIET"]:
            print("⏭️  Playlist unchanged — skipping inventory update.")

    # Decide probe set
    rows = fetch_all_streams(con)
    status_map = {r['url']: r for r in rows}
    to_check = []
    for it in items:
        r = status_map.get(it['url'])
        if r is None or _needs_check(r): to_check.append(it)

    if not cfg["MISC"]["QUIET"]:
        print(f"🧪 Probing now: {len(to_check)} (parallel={cfg['PROBE']['PARALLELISM']})")

    results = run_probe(
        to_check,
        parallelism=cfg["PROBE"]["PARALLELISM"],
        timeout_s=cfg["PROBE"]["TIMEOUT_SECONDS"],
        show_each=cfg["PROBE"]["SHOW_EACH_PROBE"],
        quiet=cfg["MISC"]["QUIET"],
    )

    if not cfg["MISC"]["QUIET"]:
        print("💾 Saving results …")
    save_probe_results(con, results, quiet=cfg["MISC"]["QUIET"])
    if not cfg["MISC"]["QUIET"]:
        print("✅ Probe results saved.")

    # Build sets (current playlist only)
    rows = fetch_all_streams(con)
    status_map = {r['url']: r for r in rows}
    in_playlist = {it['url'] for it in items}
    ok_set   = {r['url'] for r in rows if r.get('status') == 'OK'   and r['url'] in in_playlist}
    fail_set = {r['url'] for r in rows if r.get('status') == 'FAIL' and r['url'] in in_playlist}
    if not cfg["MISC"]["QUIET"]:
        print(f"📌 Summary: {len(ok_set)} OK / {len(fail_set)} FAIL in current playlist.")

    items_map = {it['url']: it for it in items}
    ok_items = [items_map[u] for u in ok_set if u in items_map]
    ok_items = [it for it in ok_items if _passes_filters(it, cfg)]

    # TMDB enrichment + auto grouping to PREFIX - Genre (or Uncategorized)
    if cfg["TMDB"]["ENABLE"] and cfg["TMDB"]["API_KEY"] and ok_items:
        if not cfg["MISC"]["QUIET"]:
            print(f"🎬 TMDB enrichment (parallel) for {len(ok_items)} items …")
        ok_items = enrich(ok_items, cfg)

    # Write M3Us
    if not cfg["MISC"]["QUIET"]:
        print("📝 Writing M3Us …")
    write(cfg["OUTPUTS"]["OK_M3U"],   ok_items)
    fail_items_m3u = [items_map[u] for u in fail_set if u in items_map]
    write(cfg["OUTPUTS"]["FAIL_M3U"], fail_items_m3u)

    # Excel rows
    note_map = {it['url']: note for (it, ok, note) in results if not ok}
    ok_rows = [{
        "Group": items_map[u].get("group-title",""),
        "Title": items_map[u].get("title",""),
        "Last OK": status_map[u].get("last_ok","") or "",
        "Last Checked": status_map[u].get("last_checked","") or "",
        "Fail Count": status_map[u].get("fail_count",0) or 0,
        "Error Category": "",
        "Reason": "",
        "URL": u,
        "TMDB Logo": items_map[u].get("tvg-logo",""),
    } for u in ok_set if u in items_map]

    fail_rows = []
    for u in fail_set:
        it = items_map.get(u, {}); r = status_map.get(u, {})
        note = note_map.get(u, "") or ""
        fail_rows.append({
            "Group": it.get("group-title",""),
            "Title": it.get("title",""),
            "Last OK": r.get("last_ok","") or "",
            "Last Checked": r.get("last_checked","") or "",
            "Fail Count": r.get("fail_count",0) or 0,
            "Error Category": classify_error(note),
            "Reason": note,
            "URL": u,
            "TMDB Logo": it.get("tvg-logo",""),
        })

    if not cfg["MISC"]["QUIET"]:
        print("📗 Writing Excel …")
    export(ok_rows, fail_rows, cfg)
    if not cfg["MISC"]["QUIET"]:
        print("✅ Done. ✨")

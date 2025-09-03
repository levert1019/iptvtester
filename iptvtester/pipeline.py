# -*- coding: utf-8 -*-
from collections import Counter

from .http_client import load_m3u_text
from .m3u import parse, write
from .db import ensure_db, bulk_stage_and_upsert, fetch_all_streams, due_for_probe
from .probe import probe_streams
from .tmdb import enrich, detect_prefix, clean_display_title, strip_leading_prefix
from .excel import export, classify_error

def _match_any(hay: str, needles):
    h = (hay or "").lower()
    return any((n or "").lower() in h for n in (needles or []))

def _passes_filters(item, cfg):
    grp = item.get("group-title", "")
    gf = cfg.get("GROUP_FILTERS", {})
    includes = gf.get("INCLUDE_GROUPS") or []
    excludes = gf.get("EXCLUDE_GROUPS") or []
    if excludes and _match_any(grp, excludes):
        return False
    if gf.get("PROCESS_ONLY_INCLUDED_GROUPS", True):
        if includes:
            return _match_any(grp, includes)
    return True

def _prepare_items_with_orig(items):
    for it in items:
        og = it.get("group-title") or ""
        it["orig_group"] = og
        it["orig_prefix"] = detect_prefix(og)
        it["orig_title"] = it.get("title") or it.get("tvg-name") or it.get("raw_title") or ""
    return items

def run_once(cfg):
    quiet = bool(cfg.get("MISC", {}).get("QUIET", False))
    if not quiet:
        print("🚀 Start IPTV-Tester")

    # 1) Load & parse playlist
    text = load_m3u_text(cfg, cfg["SOURCE_M3U"], cfg.get("_CLI_HTTP", {}))
    items = parse(text)
    if not quiet:
        print(f"🔎 Entries detected: {len(items)}")

    # 2) Filter by group
    filtered = [it for it in items if _passes_filters(it, cfg)]
    if not quiet:
        gf = cfg.get("GROUP_FILTERS", {})
        mode = "includes-only" if gf.get("PROCESS_ONLY_INCLUDED_GROUPS", True) else "all-minus-excludes"
        print(f"🧰 Group filter mode: {mode}")
        print(f"✅ After group filter: {len(filtered)}")

    # 3) Preserve originals, ingest into DB
    _prepare_items_with_orig(filtered)
    if not quiet:
        print("🗂️  Updating inventory DB …")
    con = ensure_db(cfg["OUTPUTS"]["DB_PATH"])
    bulk_stage_and_upsert(con, filtered, chunk=50000, quiet=quiet)
    if not quiet:
        print("✅ Inventory updated.")

    # 4) Decide which to probe (OK vs FAIL cadences)
    ok_hours  = float(cfg["PROBE"].get("OK_RECHECK_HOURS", 24))
    fail_mins = float(cfg["PROBE"].get("FAIL_RETRY_MINUTES", 30))
    max_probe = int(cfg["PROBE"].get("MAX_PER_RUN", 0))  # 0 = unlimited
    due_rows = due_for_probe(con, ok_hours, fail_mins, limit=(max_probe or None))

    f_by_url = {it["url"]: it for it in filtered}
    due_items = [f_by_url[r["url"]] for r in due_rows if r["url"] in f_by_url]
    if not quiet:
        print(f"🧪 Probing now: {len(due_items)} due (parallel={cfg['PROBE']['PARALLELISM']})")

    results = []
    if due_items:
        results = probe_streams(
            con,
            due_items,
            workers=cfg["PROBE"]["PARALLELISM"],
            timeout=cfg["PROBE"]["TIMEOUT_SECONDS"],
            quiet=quiet
        )

    # 5) Build OK/FAIL sets within filtered, from DB
    rows_all = fetch_all_streams(con)
    status_map = {r['url']: r for r in rows_all}
    in_filtered = set(f_by_url.keys())
    ok_set   = {r['url'] for r in rows_all if r.get('status') == 'OK'   and r['url'] in in_filtered}
    fail_set = {r['url'] for r in rows_all if r.get('status') == 'FAIL' and r['url'] in in_filtered}
    if not quiet:
        print(f"📌 Summary (DB state): {len(ok_set)} OK / {len(fail_set)} FAIL (within filtered playlist).")

    ok_items   = [f_by_url[u] for u in ok_set]
    fail_items = [f_by_url[u] for u in fail_set]

    # 6) Pre-clean titles (strip provider prefixes) for both OK+FAIL (so TMDB sees clean)
    for it in ok_items + fail_items:
        base = it.get("title") or it.get("tvg-name") or it.get("orig_title") or ""
        base = strip_leading_prefix(base)
        base = clean_display_title(base)
        it["title"] = base
        it["tvg-name"] = base

    # 7) TMDB enrichment (series-occurrence level) on OK + (optionally) FAIL
    tm = cfg.get("TMDB", {}) or {}
    tm_enabled = bool(tm.get("ENABLE"))
    tm_key = (tm.get("API_KEY") or "").strip()
    enrich_fails = bool(tm.get("ENRICH_FAILS", True))

    to_enrich = ok_items + (fail_items if enrich_fails else [])
    if tm_enabled and not tm_key:
        print("❌ TMDB is enabled but no API key is configured.")
        print("   → Set TMDB.API_KEY in config.yaml. Skipping TMDB; grouping to '|PREFIX| - Uncategorized'.")
        for it in to_enrich:
            prefix = (it.get("orig_prefix") or "EN").upper()
            it["display_title"] = f"{prefix} - {it.get('title','').strip()}"
            it["title"] = it["display_title"]
            it["tvg-name"] = it["display_title"]
            it["group-title"] = f"|{prefix}| - Uncategorized"
    elif tm_enabled and tm_key and to_enrich:
        if not quiet:
            print(f"🎬 TMDB enrichment (series-level) for {len(to_enrich)} items …")
        enrich(to_enrich, cfg, con_main=con)
        if not quiet and tm.get("DEBUG_STATS", False):
            from collections import Counter
            c = Counter([it.get("group-title","").split(" - ", 1)[-1] or "Uncategorized" for it in to_enrich])
            top = ", ".join(f"{k}:{v}" for k,v in c.most_common(10))
            print(f"🧭 Genre buckets (top): {top}")
    else:
        # TMDB disabled
        for it in to_enrich:
            prefix = (it.get("orig_prefix") or "EN").upper()
            it["display_title"] = f"{prefix} - {it.get('title','').strip()}"
            it["title"] = it["display_title"]
            it["tvg-name"] = it["display_title"]
            it["group-title"] = f"|{prefix}| - Uncategorized"

    # 8) Write outputs
    if not quiet:
        print("📝 Writing M3Us …")
    write(cfg["OUTPUTS"]["OK_M3U"],   ok_items)
    write(cfg["OUTPUTS"]["FAIL_M3U"], fail_items)

    # 9) Excel
    note_map = {it['url']: note for (it, ok, note) in (results or []) if not ok}
    ok_rows = [{
        "Group": it.get("group-title",""),
        "Title": it.get("display_title") or it.get("title",""),
        "Last OK": status_map[it['url']].get("last_ok","") if it['url'] in status_map else "",
        "Last Checked": status_map[it['url']].get("last_checked","") if it['url'] in status_map else "",
        "Fail Count": status_map[it['url']].get("fail_count",0) if it['url'] in status_map else 0,
        "URL": it['url'],
        "TMDB Logo": it.get("tvg-logo",""),
    } for it in ok_items]

    fail_rows = []
    for it in fail_items:
        url = it['url']; st = status_map.get(url, {})
        note = note_map.get(url, "") or ""
        fail_rows.append({
            "Group": it.get("group-title",""),
            "Title": it.get("display_title") or it.get("title",""),
            "Last OK": st.get("last_ok","") or "",
            "Last Checked": st.get("last_checked","") or "",
            "Fail Count": st.get("fail_count",0) or 0,
            "Error Category": classify_error(note),
            "Reason": note,
            "URL": url,
            "TMDB Logo": it.get("tvg-logo",""),
        })

    if not quiet:
        print("📗 Writing Excel …")
    export(ok_rows, fail_rows, cfg)

    if not quiet:
        print("✅ Done. ✨")

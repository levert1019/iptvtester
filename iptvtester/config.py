# -*- coding: utf-8 -*-
import os, subprocess, sys, argparse

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

DEFAULTS = {
    "SOURCE_M3U": os.path.join("input", "playlist.m3u"),
    "OUTPUTS": {
        "OK_M3U":   os.path.join("output", "ok.m3u"),
        "FAIL_M3U": os.path.join("output", "fail.m3u"),
        "OK_XLSX":  os.path.join("output", "ok.xlsx"),
        "FAIL_XLSX":os.path.join("output", "fail.xlsx"),
        "DB_PATH":  os.path.join("output", "inventory.sqlite3"),
    },
    "PROBE": {"PARALLELISM": 48, "TIMEOUT_SECONDS": 12, "SHOW_EACH_PROBE": False},
    "GROUP_FILTERS": {"PROCESS_ONLY_INCLUDED_GROUPS": True, "INCLUDE_GROUPS": [], "EXCLUDE_GROUPS": []},
    "TMDB": {
        "ENABLE": True, "API_KEY": "", "LANGUAGE": "en-US", "USE_MULTI_SEARCH": True,
        "PARALLELISM": 24, "TIMEOUT_SECONDS": 8, "RETRIES": 2, "RATE_LIMIT_RPS": 50,
        "HTTP2": False, "CACHE_DB_PATH": os.path.join("output", "tmdb_cache.sqlite3"),
        "APPLY_GENRE_GROUPING": True,
    },
    "EXCEL": {"ENGINE": "xlsxwriter", "MINIMAL_STYLE": True, "AUTOFIT_MAX": 60, "ADD_CSV_TOO": False},
    "DOWNLOAD": {"SAVE_COPY": True, "DIR": "input", "FILENAME_PREFIX": "playlist_dl_"},
    "HTTP": {
        "TIMEOUT_SECONDS": 25, "RETRIES": 3, "VERIFY_TLS": True,
        "DEFAULT_UA": "VLC/3.0.18 LibVLC/3.0.18", "ACCEPT_LANGUAGE": "en-US,en;q=0.9",
        "UA_ROTATE": [
            "VLC/3.0.18 LibVLC/3.0.18",
            "Dalvik/2.1.0 (Linux; U; Android 9; IPTV Smarters Pro)",
            "Kodi/20.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0 Safari/537.36",
        ],
        "ACCEPT_STATUSES": [200, 206, 884],
        "QUERY_AUTOVARIANTS": [
            "type=m3u_plus&output=ts",
            "type=m3u_plus&output=m3u8",
            "type=m3u&output=ts",
            "type=m3u&output=m3u8",
        ],
        "DEBUG_DUMP": True,
    },
    "MISC": {"QUIET": False},
    "ADVANCED": {"REBUILD_INDEXES": False, "MIGRATE_STREAMS_NO_ROWID": False},
}

def _maybe_install_pyyaml():
    try:
        import yaml  # noqa
        return True
    except Exception:
        py = sys.executable or "python"
        try:
            print("ℹ️  Installing PyYAML …")
            subprocess.run([py, "-m", "pip", "install", "--disable-pip-version-check", "-q", "PyYAML"],
                           check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            import yaml  # noqa
            return True
        except Exception:
            return False

def _deep_update(dst, src):
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_update(dst[k], v)
        else:
            dst[k] = v

def _load_yaml_overrides():
    cfg_path = None
    for name in ("config.yaml", "config.yml"):
        p = os.path.join(os.getcwd(), name)
        if os.path.isfile(p):
            cfg_path = p
            break
    if not cfg_path:
        return {}
    if not _maybe_install_pyyaml():
        print("⚠️  No PyYAML. Using defaults."); return {}
    import yaml
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"⚠️  Could not parse {cfg_path}: {e}")
        return {}

def _parse_cli_overrides():
    p = argparse.ArgumentParser(description="IPTV Tester")
    p.add_argument("--source")
    p.add_argument("--ok-xlsx"); p.add_argument("--fail-xlsx")
    p.add_argument("--ok-m3u");  p.add_argument("--fail-m3u")
    p.add_argument("--db")
    p.add_argument("--include-groups", nargs="+")
    p.add_argument("--exclude-groups", nargs="+")
    p.add_argument("--probe-workers", type=int)
    p.add_argument("--quiet", action="store_true")
    # HTTP overrides
    p.add_argument("--header", action="append")
    p.add_argument("--cookie"); p.add_argument("--referer")
    p.add_argument("--host-header"); p.add_argument("--append-query")
    p.add_argument("--ua"); p.add_argument("--insecure", action="store_true")
    # TMDB
    p.add_argument("--no-tmdb", action="store_true")
    p.add_argument("--tmdb-parallelism", type=int)
    p.add_argument("--tmdb-rps", type=int)
    p.add_argument("--tmdb-http2", action="store_true")
    # Advanced
    p.add_argument("--rebuild-indexes", action="store_true")
    p.add_argument("--migrate-no-rowid", action="store_true")
    return p.parse_args()

def load_config():
    cfg = {}
    _deep_update(cfg, DEFAULTS)
    _deep_update(cfg, _load_yaml_overrides())

    args = _parse_cli_overrides()

    if args.source: cfg["SOURCE_M3U"] = args.source
    out = cfg["OUTPUTS"]
    if args.ok_xlsx:  out["OK_XLSX"] = args.ok_xlsx
    if args.fail_xlsx:out["FAIL_XLSX"] = args.fail_xlsx
    if args.ok_m3u:   out["OK_M3U"] = args.ok_m3u
    if args.fail_m3u: out["FAIL_M3U"] = args.fail_m3u
    if args.db:       out["DB_PATH"] = args.db

    gf = cfg["GROUP_FILTERS"]
    if args.include_groups is not None: gf["INCLUDE_GROUPS"] = args.include_groups
    if args.exclude_groups is not None: gf["EXCLUDE_GROUPS"] = args.exclude_groups

    pr = cfg["PROBE"]
    if args.probe_workers is not None: pr["PARALLELISM"] = max(1, args.probe_workers)
    if args.quiet: cfg["MISC"]["QUIET"] = True

    # CLI HTTP overrides bag (used by downloader)
    http = cfg["HTTP"]
    cfg["_CLI_HTTP"] = {
        "header_list": args.header, "cookie": args.cookie, "referer": args.referer,
        "host_header": args.host_header, "append_query": args.append_query,
        "ua": args.ua, "verify_tls": http["VERIFY_TLS"] and (not args.insecure),
    }

    tm = cfg["TMDB"]
    if args.no_tmdb: tm["ENABLE"] = False
    if args.tmdb_parallelism is not None: tm["PARALLELISM"] = max(1, args.tmdb_parallelism)
    if args.tmdb_rps is not None: tm["RATE_LIMIT_RPS"] = max(1, args.tmdb_rps)
    if args.tmdb_http2: tm["HTTP2"] = True

    adv = cfg["ADVANCED"]
    if args.rebuild_indexes: adv["REBUILD_INDEXES"] = True
    if args.migrate_no_rowid: adv["MIGRATE_STREAMS_NO_ROWID"] = True

    return cfg

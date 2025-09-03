# -*- coding: utf-8 -*-
"""
Central config loader.

• Reads config.yaml from:
    1) ENV IPTVTESTER_CONFIG
    2) ./config.yaml (repo root / working dir)
• Merges with sane defaults.
• Light validation (e.g., TMDB key presence).
"""

from __future__ import annotations
import os
import sys
from typing import Any, Dict

try:
    import yaml  # PyYAML
except Exception:
    yaml = None

# ----------------- defaults -----------------

_DEFAULTS: Dict[str, Any] = {
    "SOURCE_M3U": "input/playlist.m3u",        # path or URL
    "HTTP": {
        "VERIFY_TLS": True,
        "TIMEOUT": 10,
        "UA": "VLC/3.0.18 LibVLC/3.0.18",      # rotating UAs can be in your downloader
    },
    "FILTER": {
        # if non-empty, only process entries whose import group matches one of these
        "INCLUDE_GROUPS": [],
        # if true, only process included groups; if false and INCLUDE_GROUPS non-empty, we still parse all but mark non-included as skipped
        "PROCESS_ONLY_INCLUDED_GROUPS": False,
    },
    "PROBE": {
        "ENABLED": True,
        "PARALLELISM": 48,
        "RECHECK_OK_AFTER_DAYS": 7,            # don’t re-probe OK too often
        "RECHECK_FAIL_AFTER_HOURS": 6,         # retry failed more aggressively
        "FFPROBE_PATH": "ffprobe",             # in PATH
        "TIMEOUT": 10,
    },
    "TMDB": {
        "API_KEY": "",                         # REQUIRED for enrichment
        "LANGUAGE": "en-US",
        "PARALLELISM": 32,
        "CACHE_DB_PATH": "output/tmdb_cache.sqlite",
        "DEBUG_STATS": False,
    },
    "OUTPUT": {
        "DIR": "output",
        "OK_M3U": "output/ok.m3u",
        "FAIL_M3U": "output/fail.m3u",
        "XLSX_OK": "output/ok.xlsx",
        "XLSX_FAIL": "output/fail.xlsx",
    },
}

# ----------------- helpers -----------------

def _deep_merge(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst

def get_config_path() -> str:
    """Return the config.yaml path to use."""
    env = os.environ.get("IPTVTESTER_CONFIG")
    if env and os.path.isfile(env):
        return env
    # working directory fallback
    wd = os.getcwd()
    p = os.path.join(wd, "config.yaml")
    return p if os.path.isfile(p) else ""

def _load_yaml(path: str) -> Dict[str, Any]:
    if not path:
        return {}
    if yaml is None:
        print("⚠️  PyYAML not installed; using defaults only. Install with: pip install pyyaml", file=sys.stderr)
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            return {}
        return data
    except Exception as e:
        print(f"⚠️  Could not read config file '{path}': {e}", file=sys.stderr)
        return {}

def _post_validate(cfg: Dict[str, Any]) -> None:
    # Ensure output dir exists (don’t create here; pipeline may do it)
    # Basic sanity checks:
    tmdb_key = (cfg.get("TMDB", {}).get("API_KEY") or "").strip()
    if not tmdb_key:
        # We don’t hard-exit here so you can still probe/write raw;
        # the pipeline should warn/fail before enrichment.
        print("ℹ️  TMDB.API_KEY is empty — enrichment will be skipped.", file=sys.stderr)

    # Normalize booleans/ints that might have been strings
    try:
        cfg["PROBE"]["PARALLELISM"] = int(cfg["PROBE"].get("PARALLELISM", 48))
    except Exception:
        cfg["PROBE"]["PARALLELISM"] = 48
    try:
        cfg["TMDB"]["PARALLELISM"] = int(cfg["TMDB"].get("PARALLELISM", 32))
    except Exception:
        cfg["TMDB"]["PARALLELISM"] = 32

# ----------------- public -----------------

def load_config() -> Dict[str, Any]:
    """
    Load config.yaml (if present), merge with defaults, return config dict.
    """
    cfg = {}
    cfg.update(_DEFAULTS)
    user_cfg = _load_yaml(get_config_path())
    if user_cfg:
        _deep_merge(cfg, user_cfg)
    _post_validate(cfg)

    # Flatten a few convenience keys
    out = cfg.get("OUTPUT", {})
    cfg["OUTPUT_OK_M3U"] = out.get("OK_M3U", "output/ok.m3u")
    cfg["OUTPUT_FAIL_M3U"] = out.get("FAIL_M3U", "output/fail.m3u")
    cfg["OUTPUT_XLSX_OK"] = out.get("XLSX_OK", "output/ok.xlsx")
    cfg["OUTPUT_XLSX_FAIL"] = out.get("XLSX_FAIL", "output/fail.xlsx")

    # HTTP convenience
    http = cfg.get("HTTP", {})
    cfg["HTTP_VERIFY_TLS"] = bool(http.get("VERIFY_TLS", True))
    cfg["HTTP_TIMEOUT"] = int(http.get("TIMEOUT", 10))
    cfg["HTTP_UA"] = http.get("UA", "VLC/3.0.18 LibVLC/3.0.18")

    return cfg

__all__ = ["load_config", "get_config_path"]

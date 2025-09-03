# -*- coding: utf-8 -*-
import math
import os
from typing import List, Dict

import pandas as pd

# --------- helpers ---------

def classify_error(note: str) -> str:
    n = (note or "").lower()
    if not n:
        return ""
    if "timeout" in n or "timed out" in n:
        return "Timeout"
    if "403" in n:
        return "HTTP 403"
    if "401" in n:
        return "HTTP 401"
    if "404" in n:
        return "HTTP 404"
    if "connection refused" in n or "could not connect" in n:
        return "Connect error"
    if "decode" in n or "codec" in n:
        return "Probe decode"
    return "Other"

def _safe_epoch_series(ser):
    """
    Coerce to numeric *seconds* since epoch; drop NaN/inf; clamp to sane range.
    Range used: [0, 4102444800] (year 2100).
    Anything outside → NaT.
    """
    s = pd.to_numeric(ser, errors="coerce")
    # replace +/-inf with NaN
    s = s.where(~(s == float("inf")), other=pd.NA)
    s = s.where(~(s == float("-inf")), other=pd.NA)
    # clamp range
    s = s.where((s >= 0) & (s <= 4102444800), other=pd.NA)
    return s

def _format_time_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            s = _safe_epoch_series(df[col])
            dt = pd.to_datetime(s, unit="s", errors="coerce", utc=True)
            # display as naive local time (no tz) for Excel compatibility
            df[col] = dt.dt.tz_convert(None)
    return df

def build_ok_df(rows: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=[
        "Group", "Title", "Last OK", "Last Checked", "Fail Count", "URL", "TMDB Logo"
    ])
    df = _format_time_cols(df, ["Last OK", "Last Checked"])
    return df

def build_fail_df(rows: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=[
        "Group", "Title", "Last OK", "Last Checked", "Fail Count",
        "Error Category", "Reason", "URL", "TMDB Logo"
    ])
    df = _format_time_cols(df, ["Last OK", "Last Checked"])
    return df

# --------- export ---------

def export(ok_rows: List[Dict], fail_rows: List[Dict], cfg: Dict):
    out_ok   = cfg["OUTPUTS"]["OK_XLSX"]
    out_fail = cfg["OUTPUTS"]["FAIL_XLSX"]
    excel_cfg = cfg.get("EXCEL", {}) or {}
    add_csv = bool(excel_cfg.get("ADD_CSV_TOO", False))

    os.makedirs(os.path.dirname(out_ok) or ".", exist_ok=True)

    df_ok = build_ok_df(ok_rows)
    df_fail = build_fail_df(fail_rows)

    # XlsxWriter (fast, robust). Avoid unsupported kwargs.
    with pd.ExcelWriter(out_ok, engine="xlsxwriter") as writer:
        df_ok.to_excel(writer, sheet_name="Working Streams", index=False)
        # minimal formatting
        ws = writer.sheets["Working Streams"]
        for i, col in enumerate(df_ok.columns):
            width = min(max(12, int(df_ok[col].astype(str).str.len().quantile(0.95)) + 2),
                        int(cfg.get("EXCEL", {}).get("AUTOFIT_MAX", 60)))
            ws.set_column(i, i, width)

    with pd.ExcelWriter(out_fail, engine="xlsxwriter") as writer:
        df_fail.to_excel(writer, sheet_name="Failed Streams", index=False)
        ws = writer.sheets["Failed Streams"]
        for i, col in enumerate(df_fail.columns):
            width = min(max(12, int(df_fail[col].astype(str).str.len().quantile(0.95)) + 2),
                        int(cfg.get("EXCEL", {}).get("AUTOFIT_MAX", 60)))
            ws.set_column(i, i, width)

    if add_csv:
        df_ok.to_csv(out_ok.replace(".xlsx", ".csv"), index=False, encoding="utf-8")
        df_fail.to_csv(out_fail.replace(".xlsx", ".csv"), index=False, encoding="utf-8")

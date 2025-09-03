# -*- coding: utf-8 -*-
from __future__ import annotations
import math
import os
from typing import List, Dict

import pandas as pd


# -------- helpers --------

def _safe_epoch_series(ser: pd.Series) -> pd.Series:
    """
    Guard against None/NaN/inf/huge values before to_datetime(unit='s').
    """
    def clamp(v):
        try:
            if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                return None
            iv = int(v)
            # sensible bounds: [1970 .. 2100-01-01]
            if iv < 0 or iv > 4102444800:
                return None
            return iv
        except Exception:
            return None
    return ser.map(clamp)


def _format_time_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            ser = _safe_epoch_series(df[c])
            dts = pd.to_datetime(ser, unit="s", errors="coerce", utc=True)
            df[c] = dts.dt.tz_convert(None)  # naive local time
    return df


# -------- optional error bucketing --------

def classify_error(reason: str) -> str:
    """
    Classify probe error strings into friendlier buckets for Excel.
    """
    if not reason:
        return "Unknown"

    r = reason.lower()
    if "timeout" in r:
        return "Timeout"
    if "not found" in r or "404" in r:
        return "Not Found"
    if "connection" in r or "refused" in r or "reset" in r:
        return "Connection Error"
    if "codec" in r or "format" in r:
        return "Unsupported Format"
    return "Other"


# -------- builders --------

def build_ok_df(rows: List[Dict]) -> pd.DataFrame:
    """
    rows: list of dicts describing OK streams (after enrichment), at least:
      url, display_title, group-title, tvg-logo, last_ok, last_checked
    """
    recs = []
    for r in rows:
        recs.append({
            "Title": r.get("display_title") or r.get("tvg-name") or r.get("title") or "",
            "Group": r.get("group-title") or "",
            "Logo": r.get("tvg-logo") or "",
            "URL": r.get("url") or "",
            "Last OK": r.get("last_ok"),
            "Last Checked": r.get("last_checked"),
        })
    df = pd.DataFrame(recs)
    df = _format_time_cols(df, ["Last OK", "Last Checked"])
    return df


def build_fail_df(rows: List[Dict]) -> pd.DataFrame:
    """
    rows: list of dicts describing FAIL streams, with probe errors if available.
    """
    recs = []
    for r in rows:
        reason = r.get("probe_error") or r.get("error") or ""
        recs.append({
            "Title": r.get("display_title") or r.get("tvg-name") or r.get("title") or "",
            "Group": r.get("group-title") or "",
            "URL": r.get("url") or "",
            "Reason": reason,
            "Bucket": classify_error(reason),
            "Last OK": r.get("last_ok"),
            "Last Checked": r.get("last_checked"),
        })
    df = pd.DataFrame(recs)
    df = _format_time_cols(df, ["Last OK", "Last Checked"])
    return df


# -------- writer --------

def export(ok_rows: List[Dict], fail_rows: List[Dict], cfg: Dict):
    """
    Write Excel files for OK and FAIL inventories.
      cfg["OUTPUT_XLSX_OK"], cfg["OUTPUT_XLSX_FAIL"]
    """
    x_ok = cfg.get("OUTPUT_XLSX_OK", "output/ok.xlsx")
    x_fail = cfg.get("OUTPUT_XLSX_FAIL", "output/fail.xlsx")
    os.makedirs(os.path.dirname(x_ok) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(x_fail) or ".", exist_ok=True)

    df_ok = build_ok_df(ok_rows)
    df_fail = build_fail_df(fail_rows)

    # xlsxwriter; avoid unsupported 'options=' argument on some pandas versions
    with pd.ExcelWriter(x_ok, engine="xlsxwriter") as writer:
        df_ok.to_excel(writer, sheet_name="Working Streams", index=False)

    with pd.ExcelWriter(x_fail, engine="xlsxwriter") as writer:
        df_fail.to_excel(writer, sheet_name="Failed Streams", index=False)


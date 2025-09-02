# -*- coding: utf-8 -*-
import os, pandas as pd
from .utils import human_dt

def classify_error(reason: str) -> str:
    r = (reason or "").lower()
    if "401" in r or "403" in r or "unauthor" in r or "forbidden" in r: return "Auth/Forbidden"
    if "404" in r or "not found" in r: return "Not Found"
    if "timeout" in r: return "Timeout"
    if "connection reset" in r or "eof" in r: return "Network/IO"
    if "ssl" in r or "certificate" in r: return "TLS/SSL"
    if "codec" in r or "invalid data" in r or "malformed" in r: return "Stream Data"
    return "Other"

def _auto_widths(df: pd.DataFrame, max_width=60):
    widths=[]
    for col in df.columns:
        h=len(str(col))
        try:
            s=df[col].astype(str).str.len().max(); s=0 if pd.isna(s) else int(s)
        except Exception: s=20
        widths.append(min(max_width, max(h,s)+2))
    return widths

def _write_xlsxwriter(df: pd.DataFrame, path: str, sheetname: str, max_width: int, minimal_style: bool):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    widths=_auto_widths(df, max_width=max_width)
    with pd.ExcelWriter(
        path,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}}
    ) as writer:
        df.to_excel(writer, index=False, sheet_name=sheetname)
        wb=writer.book; ws=writer.sheets[sheetname]
        if minimal_style:
            header_fmt=wb.add_format({"bold":True,"valign":"vcenter"})
            for colx,col_name in enumerate(df.columns):
                ws.write(0,colx,col_name,header_fmt)
        for colx,w in enumerate(widths): ws.set_column(colx,colx,w)
        ws.freeze_panes(1,0); ws.autofilter(0,0, df.shape[0], df.shape[1]-1)

def export(ok_rows, fail_rows, cfg):
    def build_df(rows):
        if not rows:
            return pd.DataFrame(columns=["Group","Title","Last OK","Last Checked","Fail Count","Error Category","Reason","URL","Logo"])
        df=pd.DataFrame(rows).rename(columns={"TMDB Logo":"Logo"})
        for col in ["Group","Title","Last OK","Last Checked","Fail Count","Error Category","Reason","URL","Logo"]:
            if col not in df.columns: df[col]=""
        df["Last OK"]=df["Last OK"].map(human_dt); df["Last Checked"]=df["Last Checked"].map(human_dt)
        return df[["Group","Title","Last OK","Last Checked","Fail Count","Error Category","Reason","URL","Logo"]]

    df_ok=build_df(ok_rows)
    df_fail=build_df(fail_rows)
    if not df_ok.empty: df_ok=df_ok.sort_values(by=["Group","Title"], kind="stable")
    if not df_fail.empty:
        df_fail=df_fail.sort_values(by=["Error Category","Fail Count","Group","Title"],
                                    ascending=[True,False,True,True], kind="stable")

    out = cfg["OUTPUTS"]; ex = cfg["EXCEL"]
    if ex.get("ADD_CSV_TOO", False):
        okb,_ = os.path.splitext(out["OK_XLSX"]); failb,_ = os.path.splitext(out["FAIL_XLSX"])
        df_ok.to_csv(okb + ".csv", index=False, encoding="utf-8")
        df_fail.to_csv(failb + ".csv", index=False, encoding="utf-8")

    _write_xlsxwriter(df_ok, out["OK_XLSX"], "OK", ex["AUTOFIT_MAX"], ex["MINIMAL_STYLE"])
    _write_xlsxwriter(df_fail, out["FAIL_XLSX"], "FAIL", ex["AUTOFIT_MAX"], ex["MINIMAL_STYLE"])
    print(f"📊 Excel (OK)   → {out['OK_XLSX']}")
    print(f"📊 Excel (FAIL) → {out['FAIL_XLSX']}")

# -*- coding: utf-8 -*-
import re
from typing import List, Dict
from urllib.parse import urlparse

_ATTR_RE = re.compile(r'([a-zA-Z0-9\-\_]+)="([^"]*)"')
_EXTINF_RE = re.compile(r'^\s*#EXTINF\s*:\s*[-0-9]+\s*(.*)$', re.IGNORECASE)

def _parse_extinf_attrs(attr_blob: str) -> Dict[str, str]:
    attrs = {}
    for m in _ATTR_RE.finditer(attr_blob):
        attrs[m.group(1)] = m.group(2)
    return attrs

def parse(text: str) -> List[Dict]:
    """
    Parse M3U into a list of dicts.
    Keys we fill:
      url, title, tvg-name, tvg-id, tvg-logo, group-title, raw_title
    """
    items = []
    if not text:
        return items

    lines = [ln.rstrip("\n\r") for ln in text.splitlines()]
    cur_meta = None
    for ln in lines:
        if ln.startswith("#EXTINF"):
            m = _EXTINF_RE.match(ln)
            if not m:
                cur_meta = {"raw_title": ""}
                continue
            blob = m.group(1)
            if "," in blob:
                attr_blob, title = blob.split(",", 1)
            else:
                attr_blob, title = blob, ""
            attrs = _parse_extinf_attrs(attr_blob)
            cur_meta = {
                "raw_title": title.strip(),
                "title": title.strip(),
                "tvg-name": (attrs.get("tvg-name") or title).strip(),
                "tvg-id": attrs.get("tvg-id") or "",
                "tvg-logo": attrs.get("tvg-logo") or "",
                "group-title": attrs.get("group-title") or "",
            }
        elif ln.startswith("#"):
            continue
        else:
            url = ln.strip()
            if not url:
                continue
            if cur_meta is None:
                cur_meta = {"raw_title": "", "title": "", "tvg-name": "", "tvg-id": "", "tvg-logo": "", "group-title": ""}
            it = dict(cur_meta)
            it["url"] = url
            items.append(it)
            cur_meta = None
    return items

def _ensure_ts_extension(url: str) -> str:
    """
    If URL ends with .m3u8/.m3u, force .ts (your requirement).
    """
    try:
        p = urlparse(url)
        path = p.path or ""
        if path.endswith(".m3u8") or path.endswith(".m3u"):
            new_path = path.rsplit(".", 1)[0] + ".ts"
            return url.replace(path, new_path, 1)
        return url
    except Exception:
        return url

def write(path: str, items: List[Dict]):
    """
    Write M3U like your example:
    #EXTM3U
    #EXTINF:0 CUID="n" tvg-name="..." tvg-logo="..." group-title="...",Title
    http://.../12345.ts
    """
    cuid = 0
    with open(path, "w", encoding="utf-8", errors="ignore") as f:
        f.write("#EXTM3U\n")
        for it in items:
            cuid += 1
            # final display comes from enrichment/pipeline under "display_title"
            display_title = (it.get("display_title") or it.get("title") or it.get("tvg-name") or it.get("raw_title") or "").strip()
            tvg_name = display_title
            tvg_logo = (it.get("tvg-logo") or "").strip()
            group = (it.get("group-title") or "").strip()
            url = _ensure_ts_extension(it.get("url") or "")

            attrs = [
                f'CUID="{cuid}"',
                f'tvg-name="{tvg_name}"',
                f'tvg-logo="{tvg_logo}"' if tvg_logo else 'tvg-logo=""',
                f'group-title="{group}"',
            ]
            f.write(f'#EXTINF:0 {" ".join(attrs)},{display_title}\n')
            f.write(url + "\n")

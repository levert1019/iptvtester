# -*- coding: utf-8 -*-
import os
import re
from typing import List, Dict, Tuple

# -------- parsing --------
#
# Robust attribute parsing:
# - allow hyphens in keys: group-title, tvg-name, tvg-id, tvg-logo
# - support "double-quoted" and 'single-quoted' values
# - be tolerant of stray spaces

# Matches key="value" or key='value'
_ATTR_DQ = re.compile(r'([A-Za-z0-9_-]+)\s*=\s*"([^"]*)"')
_ATTR_SQ = re.compile(r"([A-Za-z0-9_-]+)\s*=\s*'([^']*)'")

# Title after the comma in #EXTINF
_EXTINF_TITLE_RE = re.compile(r'^#EXTINF:[^,]*,(.*)$')

def _parse_extinf_attrs(line: str) -> Dict[str, str]:
    attrs: Dict[str, str] = {}
    for m in _ATTR_DQ.finditer(line):
        attrs[m.group(1)] = m.group(2)
    for m in _ATTR_SQ.finditer(line):
        # don't overwrite double-quoted hits; but usually keys won’t repeat
        attrs.setdefault(m.group(1), m.group(2))
    return attrs


def parse(text: str) -> List[Dict]:
    """
    Parse an M3U into a list of dicts with keys (when present):
      url, title, tvg-name, tvg-id, tvg-logo, group-title, raw_title, raw_group
    """
    items: List[Dict] = []
    if not text:
        return items

    lines = [ln.rstrip("\n\r") for ln in text.splitlines()]
    i = 0
    while i < len(lines):
        ln = lines[i].strip()
        if ln.startswith("#EXTINF:"):
            attrs = _parse_extinf_attrs(ln)
            m = _EXTINF_TITLE_RE.search(ln)
            disp = (m.group(1).strip() if m else "")

            # read next non-comment, non-empty as URL
            url = ""
            j = i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                if not nxt:
                    j += 1
                    continue
                if nxt.startswith("#"):
                    j += 1
                    continue
                url = nxt
                break
            i = j

            if url:
                # Some lists don’t include group-title but encode it into the display/title itself;
                # we still store whatever attribute we saw. Also keep a raw_group mirror of it.
                grp = attrs.get("group-title", "")
                item = {
                    "url": url,
                    "title": disp,
                    "raw_title": disp,
                    "tvg-name": attrs.get("tvg-name") or disp,
                    "tvg-id": attrs.get("tvg-id") or "",
                    "tvg-logo": attrs.get("tvg-logo") or "",
                    "group-title": grp,
                    "raw_group": grp,
                }
                items.append(item)
        i += 1
    return items


# -------- sorting + writing --------

_SERIES_PREFIX_RE = re.compile(r'^\s*\|?([A-Z]{2,4})\|?\s*[-|]\s*', re.IGNORECASE)

def _strip_series_prefix(s: str) -> str:
    return _SERIES_PREFIX_RE.sub("", s or "").strip()

_SE_TOKEN_RE = re.compile(r'(?ix)\b(?:S\s*?(\d{1,2}))\s*(?:E\s*?(\d{1,2}))\b')

def _clean_spaces(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or "").strip())

def _series_base_key(display_title: str) -> str:
    """
    Build a series title key from a final display title like:
      "EN - Friends S01 E02 The One ..."
    """
    t = _strip_series_prefix(display_title)
    # split at " Sxx Eyy"
    m = re.split(r'(?ix)\sS\d{1,2}\sE\d{1,2}\b', t, maxsplit=1)
    head = m[0] if m else t
    # also split at ' - ' just in case, keep left
    head = head.split(' - ', 1)[0]
    return _clean_spaces(head).lower()

def _sort_key(it: Dict) -> Tuple:
    grp = (it.get("group-title") or "~").lower()
    series_key = _series_base_key(it.get("display_title") or it.get("tvg-name") or it.get("title") or "")
    s = it.get("season") or 0
    e = it.get("episode") or 0
    return (grp, series_key, int(s or 0), int(e or 0), _clean_spaces(it.get("display_title") or it.get("tvg-name") or it.get("title") or ""))

def write(path: str, items: List[Dict]):
    """
    Write an M3U, sorted by Group → Series → Season → Episode.
    Uses 'display_title' if present, else tvg-name/title.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    items_sorted = sorted(items, key=_sort_key)

    out = ["#EXTM3U"]
    cuid = 1
    for it in items_sorted:
        name = it.get("display_title") or it.get("tvg-name") or it.get("title") or ""
        logo = it.get("tvg-logo") or ""
        group = it.get("group-title") or ""
        url = it.get("url") or ""

        # EXTINF with CUID + tvg-name/logo/group-title
        ext = f'#EXTINF:0 CUID="{cuid}" tvg-name="{name}"'
        if logo:
            ext += f' tvg-logo="{logo}"'
        if group:
            ext += f' group-title="{group}"'
        ext += f",{name}"
        out.append(ext)
        out.append(url)
        cuid += 1

    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(out) + "\n")

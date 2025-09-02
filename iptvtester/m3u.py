# -*- coding: utf-8 -*-
import os, re

def parse(text: str):
    items = []
    get_attr = re.compile(r'(\w[\w-]*)\s*=\s*"([^"]*)"').findall
    lines = text.splitlines()
    i = 0; n = len(lines)
    while i < n:
        line = lines[i]
        if line.startswith("#EXTINF:"):
            try: header, title = line.split(",", 1)
            except ValueError: header, title = line, ""
            attrs = dict((k.lower(), v) for k, v in get_attr(header))
            j = i + 1
            while j < n and (not lines[j] or lines[j].startswith("#")):
                j += 1
            if j < n:
                url = lines[j].strip()
                title_clean = (title or "").strip()
                items.append({
                    "url": url,
                    "title": title_clean or url,
                    "group-title": attrs.get("group-title",""),
                    "tvg-name": attrs.get("tvg-name",""),
                    "tvg-id": attrs.get("tvg-id",""),
                    "tvg-logo": attrs.get("tvg-logo",""),
                    "raw_title": title_clean or url,
                })
            i = j + 1
        else:
            i += 1
    return items

def write(path, items):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        for it in items:
            attrs=[]
            def add(k,v):
                if v: attrs.append(f'{k}="{v}"')
            add("tvg-id", it.get("tvg-id",""))
            add("tvg-name", it.get("title") or it.get("tvg-name",""))
            add("tvg-logo", it.get("tvg-logo",""))
            add("group-title", it.get("group-title",""))
            f.write(f"#EXTINF:-1 {' '.join(attrs)},{it.get('title')}\n")
            f.write(f"{it.get('url')}\n")

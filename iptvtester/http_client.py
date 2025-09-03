# -*- coding: utf-8 -*-
import os, re
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def _session(cfg):
    s = requests.Session()
    s.headers.update({
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": cfg["HTTP"]["ACCEPT_LANGUAGE"],
        "User-Agent": cfg["HTTP"]["DEFAULT_UA"],
    })
    retry = Retry(total=cfg["HTTP"]["RETRIES"], backoff_factor=0.5,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=frozenset(["GET","HEAD"]),
                  raise_on_status=False)
    ad = HTTPAdapter(max_retries=retry, pool_connections=64, pool_maxsize=64)
    s.mount("http://", ad); s.mount("https://", ad)
    return s

def _is_url(s): return isinstance(s, str) and re.match(r"^https?://", s, re.I) is not None

def _append_query(url: str, extra: str) -> str:
    if not extra: return url
    return f"{url}{'&' if '?' in url else '?'}{extra}"

def _apply_overrides(base_headers: dict, header_list, cookie, referer, ua, host_header):
    headers = dict(base_headers or {})
    if ua: headers["User-Agent"] = ua
    if referer: headers["Referer"] = referer
    if cookie: headers["Cookie"] = cookie
    if host_header: headers["Host"] = host_header
    if header_list:
        for h in header_list:
            if ":" in h:
                k,v = h.split(":",1); headers[k.strip()] = v.strip()
    return headers

def _dump_debug(cfg, content: bytes, suffix: str = "html"):
    if not cfg["HTTP"]["DEBUG_DUMP"]:
        return None
    os.makedirs(cfg["DOWNLOAD"]["DIR"], exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(cfg["DOWNLOAD"]["DIR"], f"m3u_error_{ts}.{suffix}")
    try:
        with open(path, "wb") as f: f.write(content)
        return path
    except Exception:
        return None

def load_m3u_text(cfg, path_or_url: str, cli_http):
    if not _is_url(path_or_url):
        with open(path_or_url, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()

    s = _session(cfg)
    accept = set(cfg["HTTP"]["ACCEPT_STATUSES"])
    base = path_or_url

    variants = []
    if cli_http["append_query"]:
        variants.append(_append_query(base, cli_http["append_query"]))
    else:
        variants.append(base)
        for q in cfg["HTTP"]["QUERY_AUTOVARIANTS"]:
            variants.append(_append_query(base, q))

    ua_rot = [cli_http["ua"]] if cli_http["ua"] else []
    ua_rot += [u for u in cfg["HTTP"]["UA_ROTATE"] if u not in ua_rot]

    last_err = None
    for url in variants:
        for ua in (ua_rot or [None]):
            headers = _apply_overrides(s.headers.copy(), cli_http["header_list"], cli_http["cookie"],
                                       cli_http["referer"], ua, cli_http["host_header"])
            try:
                r = s.get(url, timeout=cfg["HTTP"]["TIMEOUT_SECONDS"], allow_redirects=True,
                          headers=headers, verify=cli_http["verify_tls"])
                status = r.status_code
                body = r.content or b""
                ok = (status in accept) or (b"#EXTM3U" in body)
                if ok:
                    enc = r.encoding or r.apparent_encoding or "utf-8"
                    text = body.decode(enc, errors="ignore")
                    if cfg["DOWNLOAD"]["SAVE_COPY"]:
                        os.makedirs(cfg["DOWNLOAD"]["DIR"], exist_ok=True)
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        p = os.path.join(cfg["DOWNLOAD"]["DIR"],
                                         f"{cfg['DOWNLOAD']['FILENAME_PREFIX']}{ts}.m3u")
                        with open(p, "w", encoding="utf-8", errors="ignore") as f:
                            f.write(text)
                        if not cfg["MISC"]["QUIET"]:
                            print(f"⬇️  M3U downloaded and saved: {p} (status {status})")
                    return text
                else:
                    if not cfg["MISC"]["QUIET"]:
                        print(f"HTTP {status} UA={headers.get('User-Agent')} URL={url}")
                        print("Content-Type:", r.headers.get("Content-Type"))
                        snippet = (body[:200] or b"").decode("utf-8", errors="ignore")
                        print("Body snippet:", snippet)
                    _dump_debug(cfg, body, "html")
                    last_err = OSError(f"Download failed: HTTP {status}")
            except Exception as e:
                last_err = e
    raise last_err or OSError("Download failed")

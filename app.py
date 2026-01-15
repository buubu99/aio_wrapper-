from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import requests
from flask import Flask, jsonify, g, has_request_context
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------
# Logging
# ---------------------------

class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.rid = getattr(g, "rid", "-") if has_request_context() else "-"
        return True

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(rid)s] %(message)s",
)
logging.getLogger().addFilter(RequestIdFilter())

# ---------------------------
# Env + normalization
# ---------------------------

def _normalize_aio_base(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    raw = raw.rstrip("/")
    # allow user to paste manifest url
    if raw.endswith("/manifest.json"):
        raw = raw[: -len("/manifest.json")].rstrip("/")
    return raw

# Accept BOTH names so you don't lose time again.
AIO_BASE = _normalize_aio_base(os.environ.get("AIO_BASE", "") or os.environ.get("AIO_URL", "") or "")
AIOSTREAMS_AUTH = os.environ.get("AIOSTREAMS_AUTH", "")  # optional "user:pass"
AIO_TIMEOUT = float(os.environ.get("AIO_TIMEOUT", os.environ.get("REQUEST_TIMEOUT", "25")))

REFORMAT_STREAMS = os.environ.get("REFORMAT_STREAMS", "true").lower() == "true"
MAX_DELIVER = int(os.environ.get("MAX_DELIVER", "60"))
INPUT_CAP = int(os.environ.get("INPUT_CAP", "4500"))

# ---------------------------
# App
# ---------------------------

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False  # preserve emojis
CORS(app, resources={r"/*": {"origins": "*"}})

@app.before_request
def _rid():
    g.rid = uuid.uuid4().hex[:10]

# requests session with retries (prevents random empty)
_session = requests.Session()
_retry = Retry(total=3, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
_session.mount("http://", HTTPAdapter(max_retries=_retry))
_session.mount("https://", HTTPAdapter(max_retries=_retry))

# ---------------------------
# Small utilities
# ---------------------------

_CONTROL_RE = re.compile(r'[\x00-\x1F\x7F-\x9F\u200E\u200F\u202A-\u202E]')

def _truncate(t: str, max_len: int) -> str:
    t = (t or "").strip()
    if max_len <= 0 or len(t) <= max_len:
        return t
    return t[: max(0, max_len - 1)].rstrip() + "…"

_PUNCT_MAP = {"\u2019":"'", "\u2018":"'", "\u201c":'"', "\u201d":'"', "\u2013":"-", "\u2014":"-", "\u00a0":" "}

def _ascii_fold(t: str) -> str:
    try:
        from unidecode import unidecode  # type: ignore
        return unidecode(t)
    except Exception:
        return t

def _ui_clean(t: str, max_len: int) -> str:
    t = (t or "")
    for k, v in _PUNCT_MAP.items():
        t = t.replace(k, v)
    t = _ascii_fold(t)
    t = re.sub(r"\.(mkv|mp4|avi|ts|m2ts)$", "", t, flags=re.I)
    t = t.replace("_", " ").replace(".", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return _truncate(t, max_len)

def _human_size_bytes(n: int) -> str:
    if n <= 0:
        return "? GB"
    gb = n / (1024 ** 3)
    if gb >= 10:
        return f"{gb:.0f} GB"
    s = f"{gb:.1f}"
    if s.endswith(".0"):
        s = s[:-2]
    return f"{s} GB"

def _is_playable_stream(s: Dict[str, Any]) -> bool:
    if s.get("url") or s.get("externalUrl"):
        return True
    bh = s.get("behaviorHints")
    if isinstance(bh, dict) and bh.get("infoHash") and (bh.get("fileIdx") is not None or bh.get("fileIndex") is not None):
        return True
    sd = s.get("streamData")
    if isinstance(sd, dict) and sd.get("infoHash") and (sd.get("fileIdx") is not None or sd.get("fileIndex") is not None):
        return True
    return False

def normalize_stream_inplace(s: Dict[str, Any]) -> bool:
    if not isinstance(s.get("name"), str):
        s["name"] = ""
    if not isinstance(s.get("description"), str):
        s["description"] = ""
    s["name"] = _CONTROL_RE.sub("", s.get("name", ""))
    s["description"] = _CONTROL_RE.sub("", s.get("description", ""))
    if not isinstance(s.get("behaviorHints"), dict):
        s["behaviorHints"] = {}
    return _is_playable_stream(s)

# very light classify (format-only)
_RES_RE = re.compile(r"\b(2160p|1080p|720p|480p|4k)\b", re.I)
_SIZE_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*GB\b", re.I)

def classify(s: Dict[str, Any]) -> Dict[str, Any]:
    name = s.get("name", "") or ""
    desc = (s.get("description", "") or "").strip()
    provider = desc.upper() if 0 < len(desc) <= 3 else "UNK"
    m = _RES_RE.search(name)
    res = (m.group(1) if m else "SD").upper().replace("4K", "2160P")
    ms = _SIZE_RE.search(name)
    size_bytes = int(float(ms.group(1)) * (1024 ** 3)) if ms else int((s.get("behaviorHints") or {}).get("videoSize") or 0)
    title_raw = re.sub(r"\b(2160p|1080p|720p|480p|4k)\b", "", name, flags=re.I)
    title_raw = re.sub(r"\b\d+(?:\.\d+)?\s*GB\b", "", title_raw, flags=re.I)
    title_raw = re.sub(r"\s+", " ", title_raw).strip()
    return {"provider": provider, "res": res, "size": size_bytes, "seeders": 0, "title_raw": title_raw}

def get_expected_metadata(type_: str, id_: str) -> Dict[str, Any]:
    # format testing: no external calls
    return {"title": "", "year": None, "type": type_}

def format_stream_inplace(s: Dict[str, Any], meta: Dict[str, Any], expected: Dict[str, Any]) -> None:
    """Stremio-safe formatting: ONLY use name + description (title field is ignored by Stremio UI).

    Left column (name): 2 lines
      1) provider
      2) ⏳ resolution

    Right column (description): 3 lines
      1) 📄 normalized English title
      2) 💿 tech line (res • source • codec)
      3) 📦 size • 👥 seeders
    """
    provider = meta.get("provider", "UNK")
    res = meta.get("res", "SD")
    s["name"] = f"{provider}
⏳ {res}".strip()

    base_title = (expected.get("title") or "").strip() or (meta.get("title_raw") or "Unknown Title")
    base_title = _ui_clean(base_title, 72)
    year = expected.get("year")
    title_line = f"📄 {base_title}" + (f" ({year})" if year else "")

    tech_parts = []
    src = meta.get("source") or ""
    codec = meta.get("codec") or ""
    if res:
        tech_parts.append(res)
    if src:
        tech_parts.append(src)
    if codec:
        tech_parts.append(codec)
    tech_line = "💿 " + " • ".join(tech_parts) if tech_parts else "💿 SD"

    size_str = _human_size_bytes(int(meta.get("size") or 0))
    seeders = int(meta.get("seeders") or 0)
    size_line = f"📦 {size_str} • 👥 {seeders if seeders>0 else '?'}"

    # 3 lines on the right
    s["description"] = _truncate(title_line, 92) + "
" + _truncate(tech_line, 92) + "
" + _truncate(size_line, 92)


# ---------------------------
# Fetch streams (THIS is where your empties came from)
# ---------------------------

def _headers() -> Dict[str, str]:
    h: Dict[str, str] = {"Accept": "application/json"}
    if AIOSTREAMS_AUTH and ":" in AIOSTREAMS_AUTH:
        user, pw = AIOSTREAMS_AUTH.split(":", 1)
        h["Authorization"] = "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()
    return h

def get_streams(type_: str, id_: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (streams, debug_info)
    debug_info is included in JSON so you can see what was called.
    """
    if not AIO_BASE:
        return [], {"reason": "AIO_BASE_EMPTY", "aio_base": ""}

    url = f"{AIO_BASE}/stream/{type_}/{id_}.json"
    logging.info("AIO GET %s", url)
    try:
        resp = _session.get(url, headers=_headers(), timeout=AIO_TIMEOUT)
        info = {"aio_base": AIO_BASE, "url": url, "status": resp.status_code, "len": len(resp.content)}
        logging.info("AIO status=%s len=%s", resp.status_code, len(resp.content))
        if resp.status_code != 200:
            # keep a small snippet to debug auth/404
            info["text_head"] = resp.text[:200]
            return [], info
        data = resp.json()
        streams = (data.get("streams") or [])[:INPUT_CAP]
        info["aio_streams"] = len(streams)
        return streams, info
    except Exception as e:
        logging.exception("AIO fetch error")
        return [], {"aio_base": AIO_BASE, "url": url, "error": str(e)}

# ---------------------------
# Pipeline
# ---------------------------

@dataclass
class PipeStats:
    aio_in: int = 0
    dropped_badshape: int = 0
    dropped_error: int = 0
    delivered: int = 0

def process_streams(type_: str, id_: str, streams: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], PipeStats]:
    stats = PipeStats(aio_in=len(streams))
    expected = get_expected_metadata(type_, id_)
    out: List[Dict[str, Any]] = []
    for s in streams:
        if not normalize_stream_inplace(s):
            stats.dropped_badshape += 1
            continue
        if isinstance(s.get("streamData"), dict) and s["streamData"].get("type") == "error":
            stats.dropped_error += 1
            continue
        if REFORMAT_STREAMS:
            meta = classify(s)
            format_stream_inplace(s, meta, expected)
        out.append(s)
        if len(out) >= MAX_DELIVER:
            break
    stats.delivered = len(out)
    return out, stats

# ---------------------------
# Endpoints
# ---------------------------

@app.get("/health")
def health():
    # show exactly what the service is using
    return jsonify({"ok": True, "ts": int(time.time()), "aio_base": AIO_BASE, "has_auth": bool(AIOSTREAMS_AUTH)}), 200

@app.get("/manifest.json")
def manifest():
    return jsonify(
        {
            "id": "org.wrapper.format-test",
            "version": "0.2.1",
            "name": "Format Test Wrapper (fixed)",
            "description": "2-left + 3-right formatting. Accepts AIO_BASE or AIO_URL; strips /manifest.json; optional Basic auth.",
            "resources": ["stream"],
            "types": ["movie", "series"],
            "idPrefixes": ["tt", "tmdb", "kitsu", "anidb"],
            "catalogs": [],
        }
    )

@app.get("/stream/<type_>/<path:id_>.json")
def stream(type_: str, id_: str):
    streams, upstream = get_streams(type_, id_)
    processed, stats = process_streams(type_, id_, streams)
    # Include upstream debug so you can see WHY it's empty.
    return jsonify({"streams": processed}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))

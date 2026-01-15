from __future__ import annotations

import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, g, has_request_context
from flask_cors import CORS

# ---------------------------
# Config
# ---------------------------

def _normalize_aio_base(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    raw = raw.rstrip("/")
    # If user pasted full manifest URL, auto-fix it:
    if raw.endswith("/manifest.json"):
        raw = raw[: -len("/manifest.json")]
        raw = raw.rstrip("/")
    return raw

# Accept old/new env var names (this is the BIG fix)
AIO_BASE = _normalize_aio_base(
    os.environ.get("AIO_BASE", "") or
    os.environ.get("AIO_URL", "") or
    os.environ.get("AIO_MANIFEST_URL", "")
)

AIO_TIMEOUT = float(os.environ.get("AIO_TIMEOUT", "15"))

# Validation/verification toggles (keep OFF while format-testing)
VALIDATE_OFF = os.environ.get("VALIDATE_OFF", "1") == "1"
VERIFY_TB_CACHE_OFF = os.environ.get("VERIFY_TB_CACHE_OFF", "1") == "1"

# Formatting toggles
REFORMAT_STREAMS = os.environ.get("REFORMAT_STREAMS", "1") == "1"
MAX_DELIVER = int(os.environ.get("MAX_DELIVER", "200"))

# ---------------------------
# Logging with request id
# ---------------------------
class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.rid = getattr(g, "rid", "-") if has_request_context() else "-"
        return True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(rid)s] %(message)s",
)
logging.getLogger().addFilter(RequestIdFilter())

logging.info("BOOT: AIO_BASE=%r (set AIO_BASE or AIO_URL; may be manifest url too)", AIO_BASE)

app = Flask(__name__)
CORS(app)

@app.before_request
def _before_request():
    g.rid = uuid.uuid4().hex[:10]

# ---------------------------
# Helpers: normalize & ui cleaning
# ---------------------------
_CONTROL_RE = re.compile(r'[\x00-\x1F\x7F-\x9F\u200E\u200F\u202A-\u202E]')

def _is_playable_stream(s: Dict[str, Any]) -> bool:
    """
    Stremio supports:
      - url / externalUrl
      - OR torrent-style: infoHash + fileIdx (sometimes in behaviorHints)
    We allow a few variants to be robust.
    """
    if s.get("url") or s.get("externalUrl"):
        return True

    bh = s.get("behaviorHints")
    if isinstance(bh, dict):
        if bh.get("infoHash") and (bh.get("fileIdx") is not None or bh.get("fileIndex") is not None):
            return True

    # Some add-ons embed infoHash/fileIdx in streamData
    sd = s.get("streamData")
    if isinstance(sd, dict):
        if sd.get("infoHash") and (sd.get("fileIdx") is not None or sd.get("fileIndex") is not None):
            return True

    return False

def normalize_stream_inplace(s: Dict[str, Any]) -> bool:
    """
    Hard gate: ensure fields are well-typed and safe.
    Returns False if stream should be dropped.
    """
    if not isinstance(s.get("name"), str):
        s["name"] = ""
    if not isinstance(s.get("description"), str):
        s["description"] = ""

    s["name"] = _CONTROL_RE.sub("", s.get("name", ""))
    s["description"] = _CONTROL_RE.sub("", s.get("description", ""))

    bh = s.get("behaviorHints")
    if not isinstance(bh, dict):
        s["behaviorHints"] = {}

    return _is_playable_stream(s)

# Basic transliteration fallback (keeps emojis)
_CYR_MAP = {
    "А":"A","Б":"B","В":"V","Г":"G","Д":"D","Е":"E","Ё":"E","Ж":"Zh","З":"Z","И":"I","Й":"Y","К":"K","Л":"L","М":"M","Н":"N",
    "О":"O","П":"P","Р":"R","С":"S","Т":"T","У":"U","Ф":"F","Х":"Kh","Ц":"Ts","Ч":"Ch","Ш":"Sh","Щ":"Shch","Ъ":"","Ы":"Y","Ь":"","Э":"E","Ю":"Yu","Я":"Ya",
    "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n",
    "о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"kh","ц":"ts","ч":"ch","ш":"sh","щ":"shch","ъ":"","ы":"y","ь":"","э":"e","ю":"yu","я":"ya",
}
_PUNCT_MAP = {"\u2019":"'", "\u2018":"'", "\u201c":'"', "\u201d":'"', "\u2013":"-", "\u2014":"-", "\u00a0":" "}

def _truncate(t: str, max_len: int) -> str:
    t = (t or "").strip()
    if max_len <= 0 or len(t) <= max_len:
        return t
    return t[: max(0, max_len - 1)].rstrip() + "…"

def _ascii_fold(t: str) -> str:
    t = (t or "")
    try:
        from unidecode import unidecode  # type: ignore
        return unidecode(t)
    except Exception:
        pass
    t = "".join(_CYR_MAP.get(ch, ch) for ch in t)
    import unicodedata as _ud
    t = _ud.normalize("NFKD", t)
    t = "".join(ch for ch in t if not _ud.combining(ch))
    return t

def _ui_clean(t: str, max_len: int) -> str:
    t = (t or "")
    for k, v in _PUNCT_MAP.items():
        t = t.replace(k, v)
    # Keep emojis; remove file endings / junk separators
    t = _ascii_fold(t)
    t = re.sub(r"\.(mkv|mp4|avi|ts|m2ts)$", "", t, flags=re.I)
    t = t.replace("_", " ").replace(".", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return _truncate(t, max_len)

# ---------------------------
# Classification contract (meta)
# ---------------------------
_RES_RE = re.compile(r"\b(2160p|1080p|720p|480p|4k)\b", re.I)
_SIZE_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*GB\b", re.I)
_SRC_TOKENS = ["REMUX", "BLURAY", "WEB-DL", "WEBDL", "WEBRIP", "HDTV", "DVDRIP", "CAM", "TS"]
_CODEC_TOKENS = ["AV1", "X264", "H264", "X265", "H265", "HEVC"]

def classify(s: Dict[str, Any]) -> Dict[str, Any]:
    name = s.get("name", "")
    desc = (s.get("description", "") or "").strip()

    provider = "UNK"
    if len(desc) <= 3 and desc:
        provider = desc.upper()
    else:
        u = (s.get("url") or s.get("externalUrl") or "").lower()
        if "torbox" in u or "tb" in desc.upper():
            provider = "TB"
        elif "real-debrid" in u or "rd" in desc.upper():
            provider = "RD"

    url = (s.get("url") or "").lower()
    kind = "usenet" if (".nzb" in url or "usenet" in url) else "torrent"

    m = _RES_RE.search(name or "")
    res = (m.group(1) if m else "SD").upper()
    if res == "4K":
        res = "2160P"

    size_bytes = 0
    ms = _SIZE_RE.search(name or "")
    if ms:
        gb = float(ms.group(1))
        size_bytes = int(gb * (1024 ** 3))

    up = (name or "").upper()
    source = ""
    for tok in _SRC_TOKENS:
        if tok in up:
            source = tok.replace("WEBDL", "WEB-DL")
            break

    codec = ""
    for tok in _CODEC_TOKENS:
        if tok in up:
            codec = tok.replace("H265", "HEVC").replace("X265", "HEVC").replace("H264", "H.264").replace("X264", "H.264")
            break

    seeders = 0

    infohash = None
    u2 = (s.get("url") or "")
    parsed = urlparse(u2)
    if "magnet:" in u2:
        hm = re.search(r"btih:([0-9a-fA-F]{40})", u2, re.I)
        infohash = hm.group(1).lower() if hm else None
    else:
        hm = re.search(r"([0-9a-fA-F]{40})", (parsed.query or "") + (parsed.path or ""), re.I)
        infohash = hm.group(1).lower() if hm else None

    title_raw = name or ""
    title_raw = re.sub(r"^\s*(2160p|1080p|720p|480p)\s+", "", title_raw, flags=re.I)
    title_raw = re.sub(r"\s+\d+(?:\.\d+)?\s*GB\s*$", "", title_raw, flags=re.I)
    title_raw = title_raw.strip()

    return {
        "provider": provider,
        "kind": kind,
        "res": res,
        "size": size_bytes,
        "seeders": seeders,
        "infohash": infohash,
        "source": source,
        "codec": codec,
        "title_raw": title_raw,
        "lang": "EN",
    }

def ensure_meta_defaults(meta: Dict[str, Any]) -> Dict[str, Any]:
    meta = dict(meta)
    meta["provider"] = meta.get("provider") or "UNK"
    meta["res"] = meta.get("res") or "SD"
    meta["size"] = int(meta.get("size") or 0)
    meta["seeders"] = int(meta.get("seeders") or 0)
    meta["kind"] = meta.get("kind") or "torrent"
    meta["title_raw"] = meta.get("title_raw") or ""
    return meta

# ---------------------------
# Expected metadata (stub for format testing)
# ---------------------------
def get_expected_metadata(type_: str, id_: str) -> Dict[str, Any]:
    return {"title": "", "year": None, "type": type_, "season": None, "episode": None}

# ---------------------------
# Validation (pass-through while testing formatting)
# ---------------------------
def validate_stream(_s: Dict[str, Any], _meta: Dict[str, Any], _expected: Dict[str, Any]) -> bool:
    if VALIDATE_OFF:
        return True
    return True

# ---------------------------
# Formatting (2-left + 3-right)
# ---------------------------
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

def format_stream_inplace(s: Dict[str, Any], meta: Dict[str, Any], expected: Dict[str, Any]) -> None:
    provider = meta.get("provider", "UNK")
    res = meta.get("res", "SD")
    s["name"] = f"{provider}\n⏳ {res}".strip()

    base_title = (expected.get("title") or "").strip() or (meta.get("title_raw") or "Unknown Title")
    base_title = _ui_clean(base_title, 62)
    year = expected.get("year")
    s["title"] = _truncate(f"📄 {base_title}" + (f" ({year})" if year else ""), 92)

    tech_parts = []
    src = meta.get("source") or ""
    codec = meta.get("codec") or ""
    if res:
        tech_parts.append(res)
    if src:
        tech_parts.append(src)
    if codec:
        tech_parts.append(codec)
    line1 = "💿 " + " • ".join(tech_parts) if tech_parts else "💿 SD"

    size_str = _human_size_bytes(int(meta.get("size") or 0))
    seeders = int(meta.get("seeders") or 0)
    line2 = f"📦 {size_str} 👥 {seeders if seeders>0 else '?'}"

    s["description"] = _truncate(line1, 92) + "\n" + _truncate(line2, 92)

# ---------------------------
# Fetch streams
# ---------------------------
def get_streams(type_: str, id_: str) -> List[Dict[str, Any]]:
    if not AIO_BASE:
        return []
    url = f"{AIO_BASE}/stream/{type_}/{id_}.json"
    logging.info("UPSTREAM GET %s", url)
    r = requests.get(url, timeout=AIO_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return data.get("streams", []) or []

# ---------------------------
# Main pipeline
# ---------------------------
@dataclass
class PipeStats:
    in_count: int = 0
    dropped_badshape: int = 0
    dropped_error: int = 0
    dropped_validate: int = 0
    delivered: int = 0

def process_streams(type_: str, id_: str, streams: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], PipeStats]:
    stats = PipeStats(in_count=len(streams))
    expected = get_expected_metadata(type_, id_)

    out: List[Dict[str, Any]] = []
    for s in streams:
        if not normalize_stream_inplace(s):
            stats.dropped_badshape += 1
            continue
        if isinstance(s.get("streamData"), dict) and s["streamData"].get("type") == "error":
            stats.dropped_error += 1
            continue

        meta = ensure_meta_defaults(classify(s))

        if not validate_stream(s, meta, expected):
            stats.dropped_validate += 1
            continue

        if REFORMAT_STREAMS:
            format_stream_inplace(s, meta, expected)

        out.append(s)
        if len(out) >= MAX_DELIVER:
            break

    stats.delivered = len(out)
    logging.info("PIPE stats=%s", stats)
    return out, stats

# ---------------------------
# Endpoints
# ---------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "ts": int(time.time()), "aio_base": AIO_BASE}), 200

@app.get("/manifest.json")
def manifest():
    return jsonify(
        {
            "id": "org.wrapper.format-test",
            "version": "0.2.0",
            "name": "Format Test Wrapper",
            "description": "Wrapper forces 2-left + 3-right formatting; accepts AIO_BASE/AIO_URL (manifest ok)",
            "resources": ["stream"],
            "types": ["movie", "series"],
            "idPrefixes": ["tt", "tmdb", "kitsu", "anidb"],
            "catalogs": [],
        }
    )

@app.get("/stream/<type_>/<path:id_>.json")
def stream(type_: str, id_: str):
    try:
        streams = get_streams(type_, id_)
        processed, stats = process_streams(type_, id_, streams)
        # Stremio ignores extra fields, but this helps you debug in Chrome:
        return jsonify({"streams": processed, "stats": stats.__dict__})
    except Exception as e:
        logging.exception("stream error")
        return jsonify({"streams": [], "error": str(e), "aio_base": AIO_BASE}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "7000")))

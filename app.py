from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import unicodedata
import uuid
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request, g, has_request_context
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------
# App + logging
# ---------------------------

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
CORS(app, resources={r"/*": {"origins": "*"}})

def _log_level(v: str) -> int:
    return {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }.get((v or "INFO").upper(), logging.INFO)

class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.rid = g.request_id if has_request_context() and hasattr(g, "request_id") else "NO_REQ"
        return True

LOG_LEVEL = _log_level(os.environ.get("LOG_LEVEL", "INFO"))
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(rid)s | %(message)s")
logging.getLogger().addFilter(RequestIdFilter())

@app.before_request
def _before_request() -> None:
    g.request_id = str(uuid.uuid4())[:8]

def _rid() -> str:
    return g.request_id if has_request_context() and hasattr(g, "request_id") else "NO_REQ"


# ---------------------------
# HTTP session
# ---------------------------

# NOTE: REQUEST_TIMEOUT is seconds. If you previously used milliseconds, reduce it (e.g. 20-40).
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "20"))
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.6, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET", "POST"])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


# ---------------------------
# ENV
# ---------------------------

AIO_BASE = (os.environ.get("AIO_URL") or "").rstrip("/")
AIOSTREAMS_AUTH = (os.environ.get("AIOSTREAMS_AUTH") or "").strip()  # "user:pass" optional

MAX_DELIVER = int(os.environ.get("MAX_DELIVER", "60"))
INPUT_CAP = int(os.environ.get("INPUT_CAP", "4500"))
WRAPPER_DEDUP = os.environ.get("WRAPPER_DEDUP", "true").lower() in ("1", "true", "yes", "on")

# Usenet survival
MIN_USENET_KEEP = int(os.environ.get("MIN_USENET_KEEP", "6"))

# Safety toggles
USE_BLACKLISTS = os.environ.get("USE_BLACKLISTS", "true").lower() in ("1", "true", "yes", "on")
USE_FAKES_DB = os.environ.get("USE_FAKES_DB", "true").lower() in ("1", "true", "yes", "on")
USE_SIZE_MISMATCH = os.environ.get("USE_SIZE_MISMATCH", "true").lower() in ("1", "true", "yes", "on")
USE_AGE_HEURISTIC = os.environ.get("USE_AGE_HEURISTIC", "false").lower() in ("1", "true", "yes", "on")

# Torrent-only knobs
MIN_TORRENT_SEEDERS = int(os.environ.get("MIN_TORRENT_SEEDERS", "1"))

# TorBox verify cached (torrent-only)
VERIFY_STREAM = os.environ.get("VERIFY_STREAM", "false").lower() in ("1", "true", "yes", "on")
REFORMAT_STREAMS = os.environ.get("REFORMAT_STREAMS", "true").lower() in ("1", "true", "yes", "on")
MAX_DESC_CHARS = int(os.environ.get("MAX_DESC_CHARS", "180"))

# Friendly provider labels for Stremio UI (keeps original code in behaviorHints)
PROVIDER_LABEL = {
    "TB": "TorBox",
    "RD": "Real-Debrid",
    "AD": "AllDebrid",
    "ND": "Usenet",
    "": "Unknown",
}
TB_API_KEY = (os.environ.get("TB_API_KEY") or "").strip()
TB_BASE = (os.environ.get("TB_API_BASE") or "https://api.torbox.app").rstrip("/")
TB_API_VERSION = (os.environ.get("TB_API_VERSION") or "v1").strip("/")
TB_CHECKCACHED_BATCH = int(os.environ.get("TB_CHECKCACHED_BATCH", "25"))  # hashes per call

# Trakt validation
TRAKT_VALIDATE_TITLES = os.environ.get("TRAKT_VALIDATE_TITLES", "true").lower() in ("1", "true", "yes", "on")
TRAKT_STRICT_YEAR = os.environ.get("TRAKT_STRICT_YEAR", "true").lower() in ("1", "true", "yes", "on")
TRAKT_TITLE_MIN_RATIO = float(os.environ.get("TRAKT_TITLE_MIN_RATIO", "0.72"))
TRAKT_CLIENT_ID = (os.environ.get("TRAKT_CLIENT_ID") or "").strip()

# Blacklists / heuristics (same style as your originals)
BLACKLISTED_EXTS = [x.strip().lower() for x in os.environ.get("BLACKLISTED_EXTS", ".rar,.exe,.zip").split(",") if x.strip()]
BLACKLISTED_GROUPS = [x.strip().lower() for x in os.environ.get("BLACKLISTED_GROUPS", "GalaxyRG,beAst,COLLECTiVE,EPiC,iVy,KiNGDOM,Scene,SUNSCREEN,TGx,TVU").split(",") if x.strip()]
MIN_AGE_DAYS = int(os.environ.get("MIN_AGE_DAYS", "7"))
SIZE_MISMATCH_THRESHOLDS = {
    "2160p": int(os.environ.get("MIN_2160P_BYTES", "4000000000")),
    "1080p": int(os.environ.get("MIN_1080P_BYTES", "1000000000")),
    "720p": int(os.environ.get("MIN_720P_BYTES", "500000000")),
    "unknown": int(os.environ.get("MIN_UNKNOWN_BYTES", "200000000")),
}

FAKE_DB_FILE = os.environ.get("FAKE_DB_FILE", "fakes.json")

if not AIO_BASE:
    logging.warning("AIO_URL is not set; wrapper will return empty streams.")


# ---------------------------
# Parsing + classification
# ---------------------------

_RES_RE = re.compile(r"\b(2160p|1080p|720p|480p)\b", re.I)
_SIZE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(gb|mb)\b", re.I)
_SEED_RE = re.compile(r"(?:👥|seeders?|seeds?|peers?)\s*[:\-]?\s*(\d+)\b", re.I)

HEX40_RE = re.compile(r"\b[a-f0-9]{40}\b", re.I)

def _norm(*parts: str) -> str:
    txt = " ".join([p for p in parts if isinstance(p, str) and p])
    return unicodedata.normalize("NFKD", txt).lower()

def _bytes_from_name(name: str) -> int:
    m = _SIZE_RE.search(name or "")
    if not m:
        return 0
    val = float(m.group(1))
    unit = m.group(2).lower()
    if unit == "mb":
        val = val / 1024.0
    return int(val * 1024 * 1024 * 1024)

def _res(blob: str) -> str:
    m = _RES_RE.search(blob)
    return (m.group(1).lower() if m else "unknown")

def _seeders(blob: str) -> int:
    m = _SEED_RE.search(blob)
    return int(m.group(1)) if m else 0

def _provider(stream: Dict[str, Any]) -> str:
    # AIOStreams uses description as provider short-code ("TB","RD","AD","ND", ...)
    return (stream.get("description") or "").strip().upper()

def _link(stream: Dict[str, Any]) -> str:
    return stream.get("url") or stream.get("externalUrl") or ""

def _title_candidate(stream: Dict[str, Any]) -> str:
    bh = stream.get("behaviorHints") or {}
    return (bh.get("filename") or stream.get("name") or "").strip()

def _is_error_stream(stream: Dict[str, Any]) -> bool:
    # AIOStreams sometimes returns addon errors as "streams" (no URL, error text)
    url = _link(stream)
    if not url:
        return True
    d = (stream.get("description") or "").lower()
    n = (stream.get("name") or "").lower()
    bad = ("timed out" in d) or ("internal server error" in d) or ("unexpected end of json" in d) or ("timed out" in n)
    return bad

def extract_infohash(url: str) -> Optional[str]:
    """
    Extract torrent infohash from URLs commonly seen in AIOStreams output:
    - Comet: .../playback/<40hex>/...
    - Torrentio: .../resolve/torbox/<apikey>/<40hex>/...
    - Any URL containing a standalone 40-hex token
    """
    if not url:
        return None
    u = url.lower()

    # Comet playback pattern
    m = re.search(r"/playback/([a-f0-9]{40})(?:/|$)", u)
    if m:
        return m.group(1)

    # Torrentio resolve pattern
    m = re.search(r"/resolve/torbox/[^/]+/([a-f0-9]{40})(?:/|$)", u)
    if m:
        return m.group(1)

    # Fallback: any 40-hex in the URL
    m = HEX40_RE.search(u)
    if m:
        return m.group(0)

    return None

def classify(stream: Dict[str, Any]) -> Dict[str, Any]:
    prov = _provider(stream)
    url = _link(stream)
    blob = _norm(stream.get("name", ""), _title_candidate(stream), prov, url)
    kind = "usenet" if prov == "ND" else "torrent" if prov in ("TB", "RD", "AD") else "other"
    return {
        "provider": prov,
        "kind": kind,
        "url": url,
        "blob": blob,
        "res": _res(blob),
        "size": _bytes_from_name(blob),
        "seeders": _seeders(blob),
        "infohash": extract_infohash(url) if kind == "torrent" else None,
    }

def dedup_key(stream: Dict[str, Any], meta: Dict[str, Any]) -> Tuple[str, str]:
    # Dedup torrents by infohash when available; otherwise by (provider, url) to avoid collapsing different providers.
    if meta["kind"] == "torrent" and meta.get("infohash"):
        return ("torrent", meta["infohash"])
    if meta["url"]:
        return (meta["kind"], meta["provider"] + "|" + meta["url"])
    return (meta["kind"], meta["provider"] + "|" + (stream.get("name") or ""))



# ---------------------------
# Optional stream reformatting (keeps behaviorHints intact)
# ---------------------------

_TAGS = [
    ("REMUX", r"\bremux\b"),
    ("BluRay", r"\bbluray\b|\bblu-ray\b"),
    ("WEB-DL", r"\bweb[- ]?dl\b"),
    ("WEBRip", r"\bwebrip\b"),
    ("HDTV", r"\bhdtv\b"),
    ("HDR", r"\bhdr\b"),
    ("DV", r"\bdv\b|\bdolby[ ._-]?vision\b"),
    ("HEVC", r"\bhevc\b|\bx265\b|\bh\.?265\b"),
    ("AVC", r"\bx264\b|\bh\.?264\b"),
    ("DD+", r"\bddp\b|\bdd\+\b"),
    ("DTS", r"\bdts\b"),
    ("AAC", r"\baac\b"),
    ("Atmos", r"\batmos\b"),
]

def _safe_one_line(s: str) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _human_size(n_bytes: int) -> str:
    if not n_bytes:
        return ""
    gb = n_bytes / (1024**3)
    if gb >= 100:
        return f"{gb:.0f} GB"
    if gb >= 10:
        return f"{gb:.1f} GB"
    return f"{gb:.2f} GB"

def _pick_tags(blob: str) -> str:
    blob = blob or ""
    tags = []
    for label, rx in _TAGS:
        if re.search(rx, blob, flags=re.I):
            tags.append(label)
    # Keep a stable, compact order and avoid duplicates
    seen = set()
    tags2 = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            tags2.append(t)
    return " • ".join(tags2[:5])

def _expected_title(expected: Optional[Dict[str, Any]]) -> str:
    if not expected:
        return ""
    t = (expected.get("title") or "").strip()
    y = expected.get("year")
    return f"{t} ({y})" if t and y else t

def format_stream_inplace(stream: Dict[str, Any], meta: Dict[str, Any], expected: Optional[Dict[str, Any]], cached_hint: str = "") -> None:
    """
    Rewrites stream['name'] and stream['description'] to be consistent + compact.
    Leaves URLs + behaviorHints untouched.
    """
    prov = meta.get("provider") or ""
    kind = meta.get("kind") or "other"
    res = meta.get("res") or ""
    size = meta.get("size") or 0
    seeders = meta.get("seeders") or 0

    # Left column label in Stremio is typically stream.name
    label = PROVIDER_LABEL.get(prov, prov or "Source")
    stream["name"] = _safe_one_line(f"{label} {('• ' + res) if res else ''}".strip(" •"))

    # Build one-line description (avoid multi-line blowups)
    title = _expected_title(expected) or ""
    tags = _pick_tags(meta.get("blob") or "")
    size_s = _human_size(size)
    parts = []
    if title:
        parts.append(title)
    if tags:
        parts.append(tags)
    if size_s:
        parts.append(f"📦 {size_s}")
    if kind == "torrent" and seeders:
        parts.append(f"👥 {seeders}")
    if cached_hint:
        parts.append(cached_hint)

    desc = " • ".join(parts) if parts else _safe_one_line(stream.get("name", ""))

    # Hard cap for UI cleanliness
    if MAX_DESC_CHARS and len(desc) > MAX_DESC_CHARS:
        desc = desc[: MAX_DESC_CHARS - 1].rstrip() + "…"

    stream["description"] = _safe_one_line(desc) or prov or ""

# ---------------------------
# Fakes DB persistence (same as v2, but safe for json)
# ---------------------------

def load_fakes() -> Dict[str, set]:
    if not USE_FAKES_DB:
        return {"hashes": set(), "groups": set()}
    if os.path.exists(FAKE_DB_FILE):
        try:
            with open(FAKE_DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            return {"hashes": set(data.get("hashes") or []), "groups": set(data.get("groups") or [])}
        except Exception as e:
            logging.warning(f"Failed to load fakes DB: {e}")
    return {"hashes": set(), "groups": set()}

def save_fakes(fakes: Dict[str, set]) -> None:
    if not USE_FAKES_DB:
        return
    try:
        with open(FAKE_DB_FILE, "w", encoding="utf-8") as f:
            json.dump({"hashes": sorted(list(fakes.get("hashes") or set())),
                       "groups": sorted(list(fakes.get("groups") or set()))}, f)
    except Exception as e:
        logging.warning(f"Failed to save fakes DB: {e}")

FAKES = load_fakes()


# ---------------------------
# Trakt validator (read-only, client id only)
# ---------------------------

def _norm_title(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[\[\]\(\)\{\}]", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\b(the|a|an|and|of|to|in|on|for|with)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _title_ratio(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    a_n, b_n = _norm_title(a), _norm_title(b)
    if not a_n or not b_n:
        return 0.0
    return SequenceMatcher(None, a_n, b_n).ratio()

@lru_cache(maxsize=4096)
def trakt_lookup_imdb(imdb: str, type_: str) -> Optional[Dict[str, Any]]:
    if not (TRAKT_CLIENT_ID and imdb):
        return None
    type_param = "movie" if type_ == "movie" else "show"
    url = f"https://api.trakt.tv/search/imdb/{imdb}"
    headers = {"Content-Type": "application/json", "trakt-api-version": "2", "trakt-api-key": TRAKT_CLIENT_ID}
    try:
        r = session.get(url, headers=headers, params={"type": type_param, "extended": "full"}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json() or []
        if not data:
            return None
        obj = data[0].get(type_param) or {}
        return {"title": obj.get("title"), "year": obj.get("year")}
    except Exception as e:
        logging.warning(f"Trakt lookup failed: {e}")
        return None


@lru_cache(maxsize=4096)
def trakt_lookup_tmdb(tmdb_id: str, type_: str) -> Optional[Dict[str, Any]]:
    if not (TRAKT_CLIENT_ID and tmdb_id):
        return None
    type_param = "movie" if type_ == "movie" else "show"
    url = f"https://api.trakt.tv/search/tmdb/{tmdb_id}"
    headers = {"Content-Type": "application/json", "trakt-api-version": "2", "trakt-api-key": TRAKT_CLIENT_ID}
    try:
        r = session.get(url, headers=headers, params={"type": type_param, "extended": "full"}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json() or []
        if not data:
            return None
        obj = data[0].get(type_param) or {}
        return {"title": obj.get("title"), "year": obj.get("year")}
    except Exception as e:
        logging.warning(f"Trakt TMDB lookup failed: {e}")
        return None

def trakt_lookup_id(id_: str, type_: str) -> Optional[Dict[str, Any]]:
    """
    Prefer IMDB ids (tt...), fallback to tmdb:12345
    """
    if not id_:
        return None
    if id_.startswith("tt"):
        return trakt_lookup_imdb(id_, type_)
    if id_.startswith("tmdb:"):
        tmdb_id = id_.split(":", 1)[1]
        if tmdb_id.isdigit():
            return trakt_lookup_tmdb(tmdb_id, type_)
    return None

def trakt_title_ok(candidate: str, expected: Optional[Dict[str, Any]]) -> bool:
    if not TRAKT_VALIDATE_TITLES or not expected:
        return True
    exp_title = expected.get("title") or ""
    exp_year = expected.get("year")
    cand = candidate or ""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", cand)
    if TRAKT_STRICT_YEAR and exp_year and m:
        try:
            if int(m.group(1)) != int(exp_year):
                return False
        except Exception:
            pass
    if exp_title:
        return _title_ratio(exp_title, cand) >= TRAKT_TITLE_MIN_RATIO
    return True


# ---------------------------
# TorBox cached verification (torrent-only, batch)
# Docs: /v1/api/torrents/checkcached (GET/POST)
# ---------------------------

@lru_cache(maxsize=2048)
def torbox_checkcached_batch(hashes_csv: str) -> Dict[str, bool]:
    """
    hashes_csv: comma-separated 40-hex hashes (lowercase)
    Returns {hash: True/False} for those hashes we could determine.
    """
    if not (VERIFY_STREAM and TB_API_KEY):
        return {}
    hashes = [h for h in hashes_csv.split(",") if h]
    if not hashes:
        return {}

    url = f"{TB_BASE}/{TB_API_VERSION}/api/torrents/checkcached"
    headers = {"Authorization": f"Bearer {TB_API_KEY}"}
    # TorBox supports GET and POST; POST is safer for batches.
    try:
        r = session.post(url, headers=headers, json={"hashes": hashes, "format": "object", "list_files": False},
                         timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        j = r.json() or {}
        data = j.get("data")
        out: Dict[str, bool] = {}
        # API has had breaking changes; handle dict/list/None. See TorBox changelog.
        if data is None:
            return {h: False for h in hashes}
        if isinstance(data, dict):
            # format=object: keys are hashes that are cached
            for h in hashes:
                out[h] = bool(data.get(h))
            return out
        if isinstance(data, list):
            # format=list variant: contains cached objects
            cached_set = set()
            for item in data:
                if isinstance(item, dict) and item.get("hash"):
                    cached_set.add(str(item["hash"]).lower())
            for h in hashes:
                out[h] = h in cached_set
            return out
        return {}
    except Exception as e:
        logging.warning(f"TorBox checkcached failed (kept streams, fail-open): {e}")
        return {}


def torbox_is_cached(infohash: str) -> Optional[bool]:
    """
    Returns True/False if determinable. None if cannot verify (missing key, etc.).
    """
    if not infohash:
        return None
    if not (VERIFY_STREAM and TB_API_KEY):
        return None
    # Use cached batch function with single element (still cached by lru_cache)
    res = torbox_checkcached_batch(infohash.lower())
    return res.get(infohash.lower())


# ---------------------------
# Fetch AIOStreams
# ---------------------------

def _aio_auth() -> Optional[Tuple[str, str]]:
    if not AIOSTREAMS_AUTH or ":" not in AIOSTREAMS_AUTH:
        return None
    u, p = AIOSTREAMS_AUTH.split(":", 1)
    return (u, p)

def get_streams(type_: str, id_: str) -> Tuple[List[Dict[str, Any]], int]:
    if not AIO_BASE:
        return [], 0
    aio_url = f"{AIO_BASE}/stream/{type_}/{id_}.json"
    headers = {"X-Request-Id": _rid(), "User-Agent": "AIOWrapper/4.0"}
    try:
        r = session.get(aio_url, headers=headers, auth=_aio_auth(), timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        streams = data.get("streams") or []
        if len(streams) > INPUT_CAP:
            streams = streams[:INPUT_CAP]
        return streams, len(streams)
    except Exception as e:
        logging.warning(f"AIO fetch failed: {e}")
        return [], 0


# ---------------------------
# Pipeline + stats
# ---------------------------

@dataclass
class PipeStats:
    aio_in: int = 0
    merged_in: int = 0
    dropped_error: int = 0
    dropped_blacklist: int = 0
    dropped_fake: int = 0
    dropped_size: int = 0
    dropped_age: int = 0
    dropped_trakt: int = 0
    dropped_verify: int = 0
    dropped_seeders: int = 0
    deduped: int = 0
    delivered: int = 0
    delivered_usenet: int = 0
    delivered_torrent: int = 0
    verify_checked: int = 0
    verify_missing_hash: int = 0


def filter_streams(type_: str, id_: str, streams: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], PipeStats]:
    st = PipeStats(merged_in=len(streams), aio_in=len(streams))
    expected = trakt_lookup_id(id_, type_) if TRAKT_VALIDATE_TITLES else None

    kept: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []

    # Precompute hashes for TorBox verification (torrent-only, provider TB only)
    # We check TB only; RD/AD won't be verified.
    tb_hashes: List[str] = []
    metas: List[Dict[str, Any]] = []
    for s in streams:
        meta = classify(s)
        metas.append(meta)
        if VERIFY_STREAM and meta["kind"] == "torrent" and meta["provider"] == "TB":
            ih = meta.get("infohash")
            if ih:
                tb_hashes.append(ih.lower())

    # Batch check in chunks
    tb_cached: Dict[str, bool] = {}
    if VERIFY_STREAM and TB_API_KEY and tb_hashes:
        # de-dup hashes before querying
        uniq = sorted(set(tb_hashes))
        for i in range(0, len(uniq), TB_CHECKCACHED_BATCH):
            chunk = uniq[i:i+TB_CHECKCACHED_BATCH]
            res = torbox_checkcached_batch(",".join(chunk))
            tb_cached.update(res)

    for s, meta in zip(streams, metas):
        if _is_error_stream(s):
            st.dropped_error += 1
            continue

        blob = meta["blob"]

        # Blacklists
        if USE_BLACKLISTS:
            if any(ext in blob for ext in BLACKLISTED_EXTS) or any(g in blob for g in BLACKLISTED_GROUPS):
                st.dropped_blacklist += 1
                continue

        # Fakes DB
        if USE_FAKES_DB and meta["kind"] == "torrent":
            ih = meta.get("infohash")
            if ih and ih in FAKES["hashes"]:
                st.dropped_fake += 1
                continue
            if any(g in blob for g in FAKES["groups"]):
                st.dropped_fake += 1
                continue

        # Size mismatch (optional)
        if USE_SIZE_MISMATCH and meta["size"]:
            if meta["size"] < SIZE_MISMATCH_THRESHOLDS.get(meta["res"], SIZE_MISMATCH_THRESHOLDS["unknown"]):
                st.dropped_size += 1
                continue

        # Age heuristic (optional)
        if USE_AGE_HEURISTIC:
            m = re.search(r"(\d{4}-\d{2}-\d{2})", blob)
            if m:
                try:
                    age_days = (time.time() - time.mktime(time.strptime(m.group(1), "%Y-%m-%d"))) / 86400
                    if age_days > MIN_AGE_DAYS:
                        st.dropped_age += 1
                        continue
                except Exception:
                    pass

        # Trakt validation
        cand = _title_candidate(s)
        if expected and not trakt_title_ok(cand, expected):
            st.dropped_trakt += 1
            continue

        # TorBox cached verify (TB torrents only)
        if VERIFY_STREAM and meta["kind"] == "torrent" and meta["provider"] == "TB":
            ih = meta.get("infohash")
            if not ih:
                st.verify_missing_hash += 1
            else:
                st.verify_checked += 1
                cached = tb_cached.get(ih.lower())
                if cached is False:
                    st.dropped_verify += 1
                    # optionally record as fake
                    if USE_FAKES_DB:
                        FAKES["hashes"].add(ih.lower())
                        save_fakes(FAKES)
                    continue

        # Seeder rule (torrent-only; does not apply to usenet)
        if meta["kind"] == "torrent" and meta["seeders"] < MIN_TORRENT_SEEDERS:
            st.dropped_seeders += 1
            continue

        kept.append((s, meta))

    # Dedup
    if WRAPPER_DEDUP:
        seen = set()
        out: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for s, meta in kept:
            k = dedup_key(s, meta)
            if k in seen:
                st.deduped += 1
                continue
            seen.add(k)
            out.append((s, meta))
        kept = out

    # Sort: simple quality-first, then provider
    def sort_key(pair: Tuple[Dict[str, Any], Dict[str, Any]]):
        s, meta = pair
        bh = s.get("behaviorHints") or {}
        cached_hint = bool(bh.get("isCached", False))
        res_weight = {"2160p": 4, "1080p": 3, "720p": 2, "480p": 1}.get(meta["res"], 0)
        return (cached_hint, res_weight, meta["size"], meta["seeders"], 1 if meta["kind"]=="usenet" else 0)

    kept.sort(key=sort_key, reverse=True)

    # Usenet reserve
    usenet = [p for p in kept if p[1]["kind"] == "usenet"]
    chosen: List[Dict[str, Any]] = []
    if MIN_USENET_KEEP > 0 and usenet:
        chosen.extend([s for s, _ in usenet[:MIN_USENET_KEEP]])

    chosen_ids = {id(x) for x in chosen}
    for s, _ in kept:
        if len(chosen) >= MAX_DELIVER:
            break
        if id(s) in chosen_ids:
            continue
        chosen.append(s)

    st.delivered = len(chosen)
    st.delivered_usenet = sum(1 for s in chosen if classify(s)["kind"] == "usenet")
    st.delivered_torrent = sum(1 for s in chosen if classify(s)["kind"] == "torrent")

    if REFORMAT_STREAMS:
        for s in chosen:
            meta2 = classify(s)
            ch = ""
            if meta2.get("kind") == "torrent" and meta2.get("provider") == "TB" and VERIFY_STREAM:
                ch = "✅ Cached"
            elif meta2.get("kind") == "usenet":
                ch = "📰 NZB"
            format_stream_inplace(s, meta2, expected, cached_hint=ch)

    return chosen, st


# ---------------------------
# Routes
# ---------------------------

@app.get("/health")
def health():
    return jsonify(ok=True, ts=int(time.time())), 200

@app.get("/manifest.json")
def manifest():
    return jsonify(
        {
            "id": "org.buubuu.aiostreams.wrapper",
            "version": "4.0.0",
            "name": "AIOStreams Wrapper (v5 TB verify + format)",
            "description": "Validates titles via Trakt, filters junk, preserves Usenet, and (optionally) verifies TorBox cached torrents by extracting infohash from AIOStreams URLs.",
            "resources": ["stream"],
            "types": ["movie", "series"],
            "catalogs": [],
            "idPrefixes": ["tt", "tmdb:"],
        }
    )

@app.get("/stream/<type_>/<id_>.json")
def stream(type_: str, id_: str):
    t0 = time.time()
    stats = PipeStats()
    try:
        streams, aio_in = get_streams(type_, id_)
        stats.aio_in = aio_in
        out, stats = filter_streams(type_, id_, streams)
        return jsonify({"streams": out}), 200
    except Exception:
        logging.exception("Unhandled exception in stream endpoint")
        return jsonify({"streams": []}), 200
    finally:
        logging.info(
            "WRAP_STATS rid=%s type=%s id=%s aio_in=%s merged_in=%s dropped_error=%s dropped_blacklist=%s dropped_fake=%s dropped_size=%s dropped_age=%s dropped_trakt=%s dropped_verify=%s dropped_seeders=%s deduped=%s verify_checked=%s verify_missing_hash=%s delivered=%s usenet=%s torrent=%s ms=%s",
            _rid(), type_, id_,
            stats.aio_in, stats.merged_in,
            stats.dropped_error, stats.dropped_blacklist, stats.dropped_fake,
            stats.dropped_size, stats.dropped_age, stats.dropped_trakt,
            stats.dropped_verify, stats.dropped_seeders, stats.deduped,
            stats.verify_checked, stats.verify_missing_hash,
            stats.delivered, stats.delivered_usenet, stats.delivered_torrent,
            int((time.time() - t0)*1000)
        )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)

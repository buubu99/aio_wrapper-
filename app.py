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
from difflib import SequenceMatcher  # For similarity in trakt_validate

# ---------------------------
# Config from env (defaults updated)
# ---------------------------
AIO_URL = os.environ.get("AIO_URL", "")
AIOSTREAMS_AUTH = os.environ.get("AIOSTREAMS_AUTH", "")
INPUT_CAP = int(os.environ.get("INPUT_CAP", "4500"))
MAX_DELIVER = int(os.environ.get("MAX_DELIVER", "60"))
MIN_USENET_KEEP = int(os.environ.get("MIN_USENET_KEEP", "6"))
MIN_USENET_DELIVER = int(os.environ.get("MIN_USENET_DELIVER", "6"))
USE_BLACKLISTS = os.environ.get("USE_BLACKLISTS", "true").lower() == "true"
USE_FAKES_DB = os.environ.get("USE_FAKES_DB", "true").lower() == "true"
USE_SIZE_MISMATCH = os.environ.get("USE_SIZE_MISMATCH", "true").lower() == "true"
USE_AGE_HEURISTIC = os.environ.get("USE_AGE_HEURISTIC", "false").lower() == "true"
TRAKT_VALIDATE_TITLES = os.environ.get("TRAKT_VALIDATE_TITLES", "true").lower() == "true"
TRAKT_TITLE_MIN_RATIO = float(os.environ.get("TRAKT_TITLE_MIN_RATIO", "0.50"))
TRAKT_STRICT_YEAR = os.environ.get("TRAKT_STRICT_YEAR", "true").lower() == "true"
TRAKT_CLIENT_ID = os.environ.get("TRAKT_CLIENT_ID", "")
VERIFY_STREAM = os.environ.get("VERIFY_STREAM", "true").lower() == "true"
TB_API_KEY = os.environ.get("TB_API_KEY", "")
TB_BASE = "https://api.torbox.app"
TB_BATCH_SIZE = 50  # Increased for Pro plan
REFORMAT_STREAMS = os.environ.get("REFORMAT_STREAMS", "true").lower() == "true"
MAX_DESC_CHARS = int(os.environ.get("MAX_DESC_CHARS", "180"))
WRAPPER_DEDUP = os.environ.get("WRAPPER_DEDUP", "true").lower() == "true"
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "30"))  # Increased default

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

session = requests.Session()
retry = Retry(total=3, backoff_factor=0.6, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET", "POST"])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

# ---------------------------
# Data classes
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
    verify_checked: int = 0
    verify_missing_hash: int = 0
    delivered: int = 0
    delivered_usenet: int = 0
    delivered_torrent: int = 0

# ---------------------------
# Blacklists / Fakes (assume from original code - add your lists)
# ---------------------------

BLACKLISTS = []  # List of regex or strings for blacklisted terms
FAKES_DB = {}  # Dict of known fake hashes or patterns

# ---------------------------
# Trakt cache
# ---------------------------

@lru_cache(maxsize=1000)
def get_trakt_expected(type_: str, id_: str) -> Dict[str, Any]:
    # Original logic to fetch from Trakt API using TRAKT_CLIENT_ID
    # For brevity, assume it returns {"title": "Title", "year": year, "aliases": [...]}
    # Implement full fetch here if not in original truncated part
    slug = id_.split(':')[0] if ':' in id_ else id_
    url = f"https://api.trakt.tv/{type_}s/{slug}?extended=full"
    headers = {"trakt-api-version": "2", "trakt-api-key": TRAKT_CLIENT_ID}
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return {"title": data.get("title", ""), "year": data.get("year", None), "aliases": data.get("aliases", [])}
    except Exception as e:
        logging.warning(f"Trakt fetch failed for {type_}/{id_}: {e}")
        return {}

# ---------------------------
# Stream classification
# ---------------------------

def classify(s: Dict[str, Any]) -> Dict[str, Any]:
    meta = {}
    # Provider from description
    desc = s.get("description", "").strip()
    if len(desc) <= 3:  # Short codes like "TB", "RD"
        meta["provider"] = desc.upper()
    else:
        meta["provider"] = ""
    
    # Kind (torrent vs usenet)
    url = s.get("url", "").lower()
    if ".nzb" in url or "usenet" in url:
        meta["kind"] = "usenet"
    else:
        meta["kind"] = "torrent"
    
    # Resolution, size, seeders from name
    name = s.get("name", "").lower()
    res_match = re.search(r'(\d{3,4}p)', name)
    meta["res"] = res_match.group(1) if res_match else "unknown"
    
    size_match = re.search(r'(\d+\.?\d*)\s*(gb|mb)', name)
    if size_match:
        size = float(size_match.group(1))
        meta["size"] = size * (1024**3 if "gb" in size_match.group(2) else 1024**2)
    else:
        meta["size"] = s.get("behaviorHints", {}).get("videoSize", 0)
    
    seeders_match = re.search(r'(\d+)\s*seeders?', name)
    meta["seeders"] = int(seeders_match.group(1)) if seeders_match else 0
    
    # Infohash extraction (expanded patterns)
    parsed = urlparse(url)
    if "magnet:" in url:
        hash_match = re.search(r'btih:([0-9a-fA-F]{40})', url, re.I)
    elif "comet" in url or "torrentio" in url:
        hash_match = re.search(r'([0-9a-fA-F]{40})', parsed.query or parsed.path, re.I)
    else:
        hash_match = re.search(r'([0-9a-fA-F]{40})', url, re.I)
    
    meta["infohash"] = hash_match.group(1).lower() if hash_match else None
    if meta["provider"] == "TB" and not meta["infohash"]:
        logging.debug(f"Missing hash for TB stream: {url[:50]}...")
        meta["verify_missing_hash"] = 1  # For stats
    
    return meta

# ---------------------------
# Dedup key
# ---------------------------

def dedup_key(s: Dict[str, Any], meta: Dict[str, Any]) -> str:
    url = s.get("url", "")
    if not meta.get("infohash"):
        return url  # Fallback to full URL for non-torrents
    return f"{meta['infohash']}:{meta['size']}:{meta.get('seeders', 0)}"

# ---------------------------
# TorBox batch verify (updated)
# ---------------------------

TB_BATCH_SIZE = 50  # Increased for Pro plan

def tb_get_cached(hashes: List[str]) -> Dict[str, bool]:
    if not hashes or not TB_API_KEY:
        return {}
    
    results: Dict[str, bool] = {}
    for i in range(0, len(hashes), TB_BATCH_SIZE):
        chunk = [h for h in hashes[i:i + TB_BATCH_SIZE] if h]  # Skip None
        if not chunk:
            continue
        try:
            resp = session.post(
                f"{TB_BASE}/v1/api/torrents/checkcached",
                json={"hashes": chunk},
                headers={"Authorization": f"Bearer {TB_API_KEY}"},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            results.update({h.lower(): data.get(h, False) for h in chunk})
        except Exception as e:
            logging.warning(f"TB checkcached failed: {e} - treating chunk as uncached")
            results.update({h.lower(): False for h in chunk})
    
    return results

# ---------------------------
# Trakt validate (updated with cleaning and SequenceMatcher)
# ---------------------------

def trakt_validate(s: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    if not TRAKT_VALIDATE_TITLES:
        return True
    
    candidate = s.get("behaviorHints", {}).get("filename", s.get("name", ""))
    if not candidate:
        logging.debug(f"Dropping stream due to no valid title candidate: {s.get('url', 'unknown')[:50]}...")
        return False
    
    # Clean candidate: remove year and tech specs after title
    candidate = re.sub(r'\s*\(\d{4}\).*', '', candidate).strip()
    candidate = re.sub(r'[\._-]', ' ', candidate)  # Replace dots, underscores, hyphens with spaces
    candidate_clean = re.sub(r'(1080p|720p|2160p|4k|uhd|web-?dl|web-?rip|blu-?ray|bdrip|hdtv|dvdrip|hdrip|hdcam|cam|ts|hdts|x264|x265|h264|h265|hevc|avc|vp9|av1|aac|ac3|ddp|dd|dts|truehd|atmos|5\.1|7\.1|2\.0|mkv|mp4|avi|srt|multi|dub|eng|fr|es|de|it|ja|hi|kor|hdr|dv|hdr10|hdr10p|remux|hybrid|surcode|playbd|frame?stor|bhys|ourbits|diyhdhome|tmt)', '', candidate, flags=re.I).strip()
    candidate_clean = re.sub(r'-[a-zA-Z0-9]+$', '', candidate_clean).strip()
    candidate_clean = re.sub(r'\s+', ' ', candidate_clean).lower()
    candidate_norm = unicodedata.normalize("NFKD", candidate_clean).encode("ascii", "ignore").decode()
    
    expected_title = expected.get("title", "").lower()
    expected_norm = unicodedata.normalize("NFKD", expected_title).encode("ascii", "ignore").decode()
    
    # Extract year (improved to avoid 1080p etc.)
    candidate_year_match = re.search(r'[\.\-_ ](19\d{2}|20\d{2})[\.\-_ ]?(?!p)', candidate, re.I)
    candidate_year = int(candidate_year_match.group(1)) if candidate_year_match else None
    
    # Similarity
    ratio = SequenceMatcher(None, candidate_norm, expected_norm).ratio()
    if ratio >= TRAKT_TITLE_MIN_RATIO:
        if not TRAKT_STRICT_YEAR or not candidate_year or candidate_year == expected.get("year"):
            return True
    
    # Check aliases
    if "aliases" in expected:
        for alias in expected["aliases"]:
            alias_title = alias.get("title", "").lower()
            alias_norm = unicodedata.normalize("NFKD", alias_title).encode("ascii", "ignore").decode()
            alias_ratio = SequenceMatcher(None, candidate_norm, alias_norm).ratio()
            if alias_ratio >= TRAKT_TITLE_MIN_RATIO:
                # Optional: Check country-specific year if available, but assume same
                return True
    
    logging.debug(f"Dropping low ratio {ratio}: {candidate} vs {expected_title}")
    if candidate_year and candidate_year != expected.get("year"):
        logging.debug(f"Dropping year mismatch: {candidate_year} vs {expected['year']}")
    return False

# ---------------------------
# Format stream (updated)
# ---------------------------

def format_stream_inplace(s: Dict[str, Any], meta: Dict[str, Any], expected: Dict[str, Any], cached_hint: str = "") -> None:
    source_text = s.get("behaviorHints", {}).get("filename", s.get("name", ""))
    if not source_text and expected.get("title"):
        source_text = f"{expected['title']} ({expected.get('year', '')})"
    
    # Expanded patterns for robustness, using findall for multiples where applicable
    res_matches = re.findall(r'(\d{3,4}p|4k|uhd|hd|sd)', source_text, re.I)
    source_matches = re.findall(r'(webrip|blu[- ]?ray|web[- ]?dl|web|ts|hd[- ]?cam|dvd[- ]?rip|cam|hdcam|hdrip|bdrip|hdtv|dvd)', source_text, re.I)
    codec_matches = re.findall(r'(x264|x265|h264|h265|hevc|avc|vp9|av1)', source_text, re.I)
    audio_matches = re.findall(r'(ddp|truehd|atmos|aac|dd|ac3|dts|5\.1|7\.1|2\.0)', source_text, re.I)
    lang_matches = re.findall(r'(kor|eng|esub|multi|dub|fr|es|de|it|ja|hi)', source_text, re.I)  # Expand langs
    
    parts = []
    parts.extend(m.upper() for m in set(res_matches))  # Dedup
    parts.extend(m.upper().replace(' ', '') for m in set(source_matches))
    parts.extend(m.upper() for m in set(codec_matches))
    parts.extend(m.upper() for m in set(audio_matches))
    parts.extend(m.upper() for m in set(lang_matches))
    
    provider = meta.get("provider", s.get("description", ""))
    if provider: parts.append(provider)
    
    size = meta.get("size", 0)
    size_str = f" {size / 1024**3:.2f} GB" if size > 1024**3 else f" {size / 1024**2:.2f} MB" if size > 0 else ""
    
    formatted_name = " + ".join([p for p in parts if p]) + size_str
    if cached_hint: formatted_name = f"{cached_hint} {formatted_name}"
    
    if not formatted_name.strip():
        formatted_name = s.get("name", "Unknown Stream")
    
    s["name"] = formatted_name.strip()
    
    if "originalName" not in s.get("behaviorHints", {}):
        s.setdefault("behaviorHints", {})["originalName"] = s.get("name", "")
    
    if "description" in s and len(s["description"]) > MAX_DESC_CHARS:
        s["description"] = s["description"][:MAX_DESC_CHARS] + "..."

# ---------------------------
# Get streams from AIO
# ---------------------------

def get_streams(type_: str, id_: str) -> Tuple[List[Dict[str, Any]], int]:
    if not AIO_URL:
        raise ValueError("AIO_URL not set")
    
    aio_base = AIO_URL.rsplit('/manifest.json', 1)[0] if '/manifest.json' in AIO_URL else AIO_URL
    url = f"{aio_base}/stream/{type_}/{id_}.json"
    
    headers = {}
    if AIOSTREAMS_AUTH:
        user, pw = AIOSTREAMS_AUTH.split(':', 1)
        headers["Authorization"] = "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()
    
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        streams = data.get("streams", [])[:INPUT_CAP]
        return streams, len(streams)
    except Exception as e:
        logging.error(f"AIO fetch failed: {e}")
        return [], 0

# ---------------------------
# Filter pipeline (updated with drops for no hash TB)
# ---------------------------

def filter_streams(type_: str, id_: str, streams: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], PipeStats]:
    stats = PipeStats()
    stats.merged_in = len(streams)
    
    expected = get_trakt_expected(type_, id_.split(':')[0]) if TRAKT_VALIDATE_TITLES else {}
    
    # Classify all
    classified = [(s, classify(s)) for s in streams]
    
    # Drop errors
    classified = [(s, m) for s, m in classified if not s.get("streamData", {}).get("type") == "error" and not '[❌]' in s.get("name", "")]
    stats.dropped_error = stats.merged_in - len(classified)
    
    # Blacklists
    if USE_BLACKLISTS:
        classified = [(s, m) for s, m in classified if not any(re.search(bl, s.get("name", "") + s.get("description", "")) for bl in BLACKLISTS)]
        stats.dropped_blacklist = stats.merged_in - len(classified) - stats.dropped_error  # Adjust count
    
    # Fakes
    if USE_FAKES_DB:
        classified = [(s, m) for s, m in classified if m.get("infohash") not in FAKES_DB]
        stats.dropped_fake = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist
    
    # Size mismatch (original heuristic - assume implemented)
    if USE_SIZE_MISMATCH:
        # Add your size check logic here
        pass
    
    # Age heuristic
    if USE_AGE_HEURISTIC:
        # Add age check
        pass
    
    # Trakt validate
    classified = [(s, m) for s, m in classified if trakt_validate(s, expected)]
    stats.dropped_trakt = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist - stats.dropped_fake
    
    # TorBox verify (batch all at once)
    if VERIFY_STREAM:
        tb_hashes = [m["infohash"] for s, m in classified if m["provider"] == "TB" and m["infohash"]]
        stats.verify_checked = len(tb_hashes)
        stats.verify_missing_hash = sum(1 for s, m in classified if m["provider"] == "TB" and not m["infohash"])
        cached_map = tb_get_cached(tb_hashes)
        
        new_classified = []
        for s, m in classified:
            if m["provider"] != "TB":
                new_classified.append((s, m))
                continue
            if not m["infohash"]:
                stats.dropped_verify += 1
                continue
            is_cached = cached_map.get(m["infohash"], False)
            if is_cached:
                new_classified.append((s, m))
            else:
                stats.dropped_verify += 1
                # Optional: Log misclassification if AIO hinted cached but not
                if "Cached" in s.get("name", "") or "cached" in s.get("description", "").lower():
                    logging.warning(f"AIO misclassified uncached TB stream as cached: {s.get('url', 'unknown')[:50]}...")
        classified = new_classified
    
    # Seeders filter (assume min from env)
    min_seeders = int(os.environ.get("MIN_SEEDERS", "0"))
    classified = [(s, m) for s, m in classified if m["kind"] != "torrent" or m["seeders"] >= min_seeders]
    stats.dropped_seeders = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist - stats.dropped_fake - stats.dropped_trakt - stats.dropped_verify
    
    # Dedup
    if WRAPPER_DEDUP:
        seen = set()
        new_classified = []
        for s, m in classified:
            key = dedup_key(s, m)
            if key not in seen:
                seen.add(key)
                new_classified.append((s, m))
            else:
                stats.deduped += 1
        classified = new_classified
    
    # Sort with boosts
    def sort_key(item):
        s, m = item
        cached = 2 if m["provider"] == "TB" and "Cached" in s.get("name", "") else 1 if "Cached" in s.get("name", "") else 0  # Boost TB cached
        res_val = {'4k': 4, '2160p': 4, '1080p': 3, '720p': 2, '480p': 1}.get(m["res"], 0)
        usenet_boost = 1 if m["kind"] == "usenet" and res_val >= 3 and m["size"] > 2*1024**3 else 0  # Boost good Usenet
        non_tb_penalty = -1 if m["provider"] in ["RD", "AD"] and any(im[1]["provider"] == "TB" for im in classified) else 0  # Deprioritize if TB available
        return (-cached - usenet_boost, -res_val, -m["size"], -m["seeders"], non_tb_penalty)
    
    classified.sort(key=sort_key)
    
    # Preserve/Insert Usenet at top if valid
    usenet = [item for item in classified if item[1]["kind"] == "usenet"][:MIN_USENET_KEEP]
    torrent = [item for item in classified if item[1]["kind"] != "usenet"]
    out = usenet + torrent[:MAX_DELIVER - len(usenet)]  # Usenet first, then fill with torrents
    
    # Format final
    if REFORMAT_STREAMS:
        for s, m in out:
            cached_hint = "✅ Cached" if m["provider"] == "TB" and cached_map.get(m["infohash"], False) else ""
            format_stream_inplace(s, m, expected, cached_hint)
    
    stats.delivered = len(out)
    stats.delivered_usenet = len([s for s, m in out if m["kind"] == "usenet"])
    stats.delivered_torrent = stats.delivered - stats.delivered_usenet
    
    return [s for s, m in out], stats

# ---------------------------
# Endpoints
# ---------------------------

@app.get("/health")
def health():
    return jsonify({"ok": True, "ts": int(time.time())}), 200

@app.get("/manifest.json")
def manifest():
    return jsonify(
        {
            "id": "org.jason.wrapper",
            "version": "1.1.0",
            "name": "AIO Wrapper",
            "description": "Wraps AIOStreams to filter and format streams w usenet",
            "resources": ["stream"],
            "types": ["movie", "series"],
            "catalogs": [],
            "idPrefixes": ["tt"]
        }
    )

@app.get("/stream/<type_>/<id_>.json")
def stream(type_: str, id_: str):
    if type_ not in ["movie", "series"]:
        return jsonify({"streams": []}), 400
    if not id_:
        return jsonify({"streams": []}), 400
    
    t0 = time.time()
    stats = PipeStats()
    try:
        streams, stats.aio_in = get_streams(type_, id_)
        out, stats = filter_streams(type_, id_, streams)
        return jsonify({"streams": out}), 200
    except Exception:
        logging.exception("Unhandled exception in stream endpoint")
        return jsonify({"streams": []}), 200
    finally:
        # Log stats (original)
        logging.info(
            "WRAP_STATS rid=%s type=%s id=%s aio_in=%s merged_in=%s dropped_error=%s dropped_blacklist=%s dropped_fake=%s dropped_size=%s dropped_age=%s dropped_trakt=%s dropped_verify=%s dropped_seeders=%s deduped=%s verify_checked=%s verify_missing_hash=%s delivered=%s usenet=%s torrent=%s ms=%s",
            _rid(), type_, id_,
            stats.aio_in, stats.merged_in,
            stats.dropped_error, stats.dropped_blacklist, stats.dropped_fake,
            stats.dropped_size, stats.dropped_age, stats.dropped_trakt,
            stats.dropped_verify, stats.dropped_seeders, stats.deduped,
            stats.verify_checked, stats.verify_missing_hash,
            stats.delivered, stats.delivered_usenet, stats.delivered_torrent,
            int((time.time() - t0) * 1000)
        )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=LOG_LEVEL == logging.DEBUG, use_reloader=False)

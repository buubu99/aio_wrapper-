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
import threading  # For racing metadata fetches

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
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
PREFERRED_SOURCE = os.environ.get("PREFERRED_METADATA_SOURCE", "tmdb")  # Default to TMDB for speed
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
        record.rid = g.request_id if has_request_context() and hasattr(g, "request_id") else "GLOBAL"  # Default to GLOBAL if no context
        return True

LOG_LEVEL = _log_level(os.environ.get("LOG_LEVEL", "INFO"))
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(rid)s | %(message)s")
logging.getLogger().addFilter(RequestIdFilter())

@app.before_request
def _before_request() -> None:
    g.request_id = str(uuid.uuid4())[:8]

def _rid() -> str:
    return g.request_id if has_request_context() and hasattr(g, "request_id") else "GLOBAL"

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
# Transliteration mapping (for non-English normalization)
# ---------------------------

CYRILLIC_MAPPING = {
    'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'Yo', 'Ж': 'Zh',
    'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M', 'Н': 'N', 'О': 'O',
    'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U', 'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts',
    'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch', 'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu',
    'Я': 'Ya',
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh',
    'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
    'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts',
    'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu',
    'я': 'ya'
}

def transliterate_to_latin(text: str, mapping: Dict[str, str]) -> str:
    return ''.join(mapping.get(char, char) for char in text)

# ---------------------------
# Metadata cache (Trakt + TMDB with racing for speed)
# ---------------------------

TMDB_BASE = "https://api.themoviedb.org/3"

@lru_cache(maxsize=1000)
def get_expected_metadata(type_: str, id_: str) -> Dict[str, Any]:
    slug = id_.split(':')[0] if ':' in id_ else id_
    expected = {}  # Final result
    latencies = {"trakt": None, "tmdb": None}  # Track speeds

    def fetch_trakt():
        if not TRAKT_CLIENT_ID:
            return {}
        url = f"https://api.trakt.tv/{type_}s/{slug}?extended=full"
        headers = {"trakt-api-version": "2", "trakt-api-key": TRAKT_CLIENT_ID}
        start = time.time()
        try:
            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            latencies["trakt"] = time.time() - start
            return {"title": data.get("title", ""), "year": data.get("year", None), "aliases": data.get("aliases", [])}
        except Exception as e:
            logging.warning(f"Trakt fetch failed for {type_}/{id_}: {e}")
            latencies["trakt"] = float('inf')
            return {}

    def fetch_tmdb():
        if not TMDB_API_KEY:
            return {}
        tmdb_type = "movie" if type_ == "movie" else "tv"
        find_url = f"{TMDB_BASE}/find/{slug}?api_key={TMDB_API_KEY}&external_source=imdb_id"
        start = time.time()
        try:
            find_resp = session.get(find_url, timeout=REQUEST_TIMEOUT)
            find_resp.raise_for_status()
            find_data = find_resp.json()
            results = find_data.get(f"{tmdb_type}_results", [])
            if not results:
                raise ValueError("No TMDB results")
            tmdb_id = results[0]["id"]
            details_url = f"{TMDB_BASE}/{tmdb_type}/{tmdb_id}?api_key={TMDB_API_KEY}"
            details_resp = session.get(details_url, timeout=REQUEST_TIMEOUT)
            details_resp.raise_for_status()
            details = details_resp.json()
            aliases = []
            trans_url = f"{TMDB_BASE}/{tmdb_type}/{tmdb_id}/translations?api_key={TMDB_API_KEY}"
            trans_resp = session.get(trans_url, timeout=REQUEST_TIMEOUT).json()
            for trans in trans_resp.get("translations", []):
                if title := trans.get("data", {}).get("title") or trans.get("data", {}).get("name"):
                    aliases.append({"title": title})
            year_str = details.get("release_date", "")[:4] or details.get("first_air_date", "")[:4] or "0"
            year = int(year_str) if year_str.isdigit() else 0  # Safe int, default 0
            latencies["tmdb"] = time.time() - start
            return {"title": details.get("title") or details.get("name", ""), "year": year, "aliases": aliases}
        except Exception as e:
            logging.warning(f"TMDB fetch failed for {type_}/{id_}: {e}")
            latencies["tmdb"] = float('inf')
            return {}

    # Race in threads
    trakt_result = {}
    tmdb_result = {}
    trakt_thread = threading.Thread(target=lambda: trakt_result.update(fetch_trakt()))
    tmdb_thread = threading.Thread(target=lambda: tmdb_result.update(fetch_tmdb()))

    if PREFERRED_SOURCE == "tmdb":
        tmdb_thread.start()
        tmdb_thread.join()
        expected = tmdb_result
        if not expected:
            trakt_thread.start()
            trakt_thread.join()
            expected = trakt_result
    else:
        trakt_thread.start()
        trakt_thread.join()
        expected = trakt_result
        if not expected:
            tmdb_thread.start()
            tmdb_thread.join()
            expected = tmdb_result

    # Log latencies
    logging.info(f"Metadata latencies for {type_}/{id_}: Trakt={latencies['trakt']}s, TMDB={latencies['tmdb']}s")

    if not expected:
        logging.error(f"Both metadata fetches failed for {type_}/{id_} - skipping validation")
        return {}  # Empty: Will skip trakt_validate checks

    return expected

# ---------------------------
# Stream classification
# ---------------------------

def classify(s: Dict[str, Any]) -> Dict[str, Any]:
    meta = {}
    desc = s.get("description", "").strip()
    if len(desc) <= 3:
        meta["provider"] = desc.upper()
    else:
        meta["provider"] = ""

    url = s.get("url", "").lower()
    if ".nzb" in url or "usenet" in url:
        meta["kind"] = "usenet"
    else:
        meta["kind"] = "torrent"

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

    # Broader hash search: full URL
    hash_match = re.search(r'([0-9a-fA-F]{40})', url, re.I)
    meta["infohash"] = hash_match.group(1).lower() if hash_match else None
    if meta["provider"] == "TB" and not meta["infohash"]:
        logging.warning(f"Missing hash for TB stream (assuming uncached): {url[:50]}...")  # Warning instead of debug for visibility
        meta["verify_missing_hash"] = 1

    return meta

# ---------------------------
# Dedup key
# ---------------------------

def dedup_key(s: Dict[str, Any], meta: Dict[str, Any]) -> str:
    url = s.get("url", "")
    if not meta.get("infohash"):
        return url
    return f"{meta['infohash']}:{meta['size']}:{meta.get('seeders', 0)}"

# ---------------------------
# TorBox batch verify
# ---------------------------

def tb_get_cached(hashes: List[str]) -> Dict[str, bool]:
    if not hashes or not TB_API_KEY:
        return {}

    results: Dict[str, bool] = {}
    for i in range(0, len(hashes), TB_BATCH_SIZE):
        chunk = [h for h in hashes[i:i + TB_BATCH_SIZE] if h]
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
# Trakt validate (with transliteration)
# ---------------------------

def trakt_validate(s: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    if not TRAKT_VALIDATE_TITLES:
        return True

    candidate = s.get("behaviorHints", {}).get("filename", s.get("name", ""))
    if not candidate:
        logging.debug(f"Dropping stream due to no valid title candidate: {s.get('url', 'unknown')[:50]}...")
        return False

    # Clean candidate
    candidate = re.sub(r'\s*$$   \d{4}   $$.*', '', candidate).strip()
    candidate = re.sub(r'[\._-]', ' ', candidate)
    candidate_clean = re.sub(r'(1080p|720p|2160p|4k|uhd|web-?dl|web-?rip|blu-?ray|bdrip|hdtv|dvdrip|hdrip|hdcam|cam|ts|hdts|x264|x265|h264|h265|hevc|avc|vp9|av1|aac|ac3|ddp|dd|dts|truehd|atmos|5\.1|7\.1|2\.0|mkv|mp4|avi|srt|multi|dub|eng|fr|es|de|it|ja|hi|kor|hdr|dv|hdr10|hdr10p|remux|hybrid|surcode|playbd|frame?stor|bhys|ourbits|diyhdhome|tmt)', '', candidate, flags=re.I).strip()
    candidate_clean = re.sub(r'-[a-zA-Z0-9]+$', '', candidate_clean).strip()
    candidate_clean = re.sub(r'\s+', ' ', candidate_clean).lower()

    # Transliterate for non-English
    candidate_translit = transliterate_to_latin(candidate_clean, CYRILLIC_MAPPING)
    candidate_norm = unicodedata.normalize("NFKD", candidate_translit).encode("ascii", "ignore").decode()

    expected_title = expected.get("title", "").lower()
    expected_norm = unicodedata.normalize("NFKD", expected_title).encode("ascii", "ignore").decode()

    candidate_year_match = re.search(r'[\.\-_ ](19\d{2}|20\d{2})[\.\-_ ]?(?!p)', candidate, re.I)
    candidate_year = int(candidate_year_match.group(1)) if candidate_year_match else None

    ratio = SequenceMatcher(None, candidate_norm, expected_norm).ratio()
    if ratio >= TRAKT_TITLE_MIN_RATIO:
        if not TRAKT_STRICT_YEAR or not candidate_year or candidate_year == expected.get("year"):
            return True

    if "aliases" in expected:
        for alias in expected["aliases"]:
            alias_title = alias.get("title", "").lower()
            alias_translit = transliterate_to_latin(alias_title, CYRILLIC_MAPPING)
            alias_norm = unicodedata.normalize("NFKD", alias_translit).encode("ascii", "ignore").decode()
            alias_ratio = SequenceMatcher(None, candidate_norm, alias_norm).ratio()
            if alias_ratio >= TRAKT_TITLE_MIN_RATIO and (not TRAKT_STRICT_YEAR or not candidate_year or candidate_year == expected.get("year")):
                return True

    logging.debug(f"Dropping stream due to low Trakt similarity: {candidate_norm} vs {expected_norm} (ratio {ratio:.2f})")
    return False

# ---------------------------
# Format stream (detailed from Version A)
# ---------------------------

def format_stream_inplace(s: Dict[str, Any], meta: Dict[str, Any], expected: Dict[str, Any], cached_hint: str = "") -> None:
    # Extract provider, res, filename
    provider = meta.get("provider", "Unknown").upper()
    provider_map = {"RD": "Real-Debrid", "TB": "TorBox", "AD": "AllDebrid"}
    provider_label = provider_map.get(provider, provider)
    res_label = meta.get("res", "unknown").upper()
    filename = s.get("behaviorHints", {}).get("filename", s.get("name", ""))
    source_text = filename.lower()

    # Clean source_text
    source_text_clean = re.sub(r'[\._-]', ' ', source_text)
    source_text_clean = re.sub(r'\s+', ' ', source_text_clean).strip()

    # Tags extraction
    source_tags = {"webrip": "WEBRip", "web-dl": "WEB-DL", "bluray": "BluRay", "bdrip": "BDRip", "hdtv": "HDTV", "dvdrip": "DVDRip", "hdrip": "HDRip", "hdcam": "HDCAM", "cam": "CAM", "ts": "TS", "hdts": "HDTS"}
    source_tag = next((tag for pat, tag in source_tags.items() if re.search(rf'\b{pat}\b', source_text_clean, re.I)), "")

    codec_tags = {"x265": "HEVC", "h265": "HEVC", "hevc": "HEVC", "x264": "H.264", "h264": "H.264", "avc": "AVC", "vp9": "VP9", "av1": "AV1"}
    codec_tag = next((tag for pat, tag in codec_tags.items() if re.search(rf'\b{pat}\b', source_text_clean, re.I)), "")

    audio_tags = {"truehd": "TrueHD", "atmos": "Atmos", "dts": "DTS", "ddp": "DDP", "dd": "DD", "ac3": "AC3", "aac": "AAC", "5.1": "5.1", "7.1": "7.1", "2.0": "2.0"}
    audio_tag = next((tag for pat, tag in audio_tags.items() if re.search(rf'\b{pat}\b', source_text_clean, re.I)), "")

    lang_tags = {"eng": "Eng", "fr": "Fr", "es": "Es", "de": "De", "it": "It", "ja": "Ja", "hi": "Hi", "kor": "Kor", "multi": "Multi", "dub": "Dub"}
    lang_tag = next((tag for pat, tag in lang_tags.items() if re.search(rf'\b{pat}\b', source_text_clean, re.I)), "")

    # Group (last -XXX)
    group_match = re.search(r'-([a-zA-Z0-9]+)$', source_text_clean)
    group = group_match.group(1).upper() if group_match else ""

    # --- Name line (left column) ---
    s["name"] = f"{provider_label}\n⏳ {res_label}"

    # --- Title line (right column header) ---
    base_title = expected.get("title", "").strip()
    year = expected.get("year")
    if not year:
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', source_text_clean)
        year = int(year_match.group(1)) if year_match else None

    if not base_title:
        cut = re.split(r'\b(\d{3,4}p|4k|uhd|webrip|web[- ]?dl|blu[- ]?ray|bluray|x264|x265|h264|h265|hevc|av1|vp9)\b', source_text_clean, maxsplit=1, flags=re.I)[0]
        base_title = cut.strip().title()

    title_parts = []
    if base_title:
        title_parts.append(f"{base_title}({year})" if year else base_title)
    if res_label != "unknown":
        title_parts.append(f"[{res_label}]")
    if source_tag:
        title_parts.append(f"[{source_tag}]")

    s["title"] = "📄 " + " ".join(title_parts) if title_parts else "📄 " + (source_text_clean[:80] + "..." if len(source_text_clean) > 80 else "")

    # --- Description lines ---
    disc_parts = [res_label] if res_label != "unknown" else []
    if source_tag:
        disc_parts.append(source_tag)
    if codec_tag:
        disc_parts.append(codec_tag)
    if audio_tag:
        disc_parts.append(audio_tag)
    disc_line = "💿 " + " • ".join(disc_parts) if disc_parts else ""

    size_bytes = int(meta.get("size", 0))
    def _human_size(n: int) -> str:
        if n <= 0:
            return ""
        gb = n / (1024 ** 3)
        return f"{gb:.1f} GB" if gb >= 1 else f"{n / (1024 ** 2):.0f} MB"

    size_str = _human_size(size_bytes)
    seeders = int(meta.get("seeders", 0))

    box_bits = []
    if size_str:
        box_bits.append(size_str)
    if seeders:
        box_bits.append(f"👥 {seeders}")
    if group:
        box_bits.append("⛓ 💥 " + group)
    box_line = "📦 " + " ".join(box_bits) if box_bits else ""

    lang_line = f"🌐 {lang_tag}" if lang_tag else ""

    desc_lines = [ln for ln in [disc_line, box_line, lang_line] if ln]
    s["description"] = "\n".join(desc_lines) if desc_lines else ""

    if len(s["description"]) > MAX_DESC_CHARS:
        s["description"] = s["description"][:MAX_DESC_CHARS] + "..."

    # Preserve originalName if needed
    if "behaviorHints" not in s:
        s["behaviorHints"] = {}
    s["behaviorHints"]["originalName"] = s.get("name", "")

# ---------------------------
# Get streams from AIO
# ---------------------------

def get_streams(type_: str, id_: str) -> Tuple[List[Dict[str, Any]], int]:
    if not AIO_URL:
        raise ValueError("AIO_URL not set")

    aio_base = AIO_URL.rsplit('/manifest.json', 1)[0] if '/manifest.json' in AIO_URL else AIO_URL
    url = f"{aio_base}/stream/{type_}/{id_}.json"
    logging.info(f"AIO fetch URL: {url}")  # Log the constructed URL for debug

    headers = {}
    if AIOSTREAMS_AUTH:
        user, pw = AIOSTREAMS_AUTH.split(':', 1)
        headers["Authorization"] = "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()

    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        logging.info(f"AIO fetch status: {resp.status_code}, content_len: {len(resp.content)}")  # Log response details
        resp.raise_for_status()
        try:
            data = resp.json()
            logging.info(f"AIO JSON keys: {list(data.keys())}")  # Log keys to see structure
        except json.JSONDecodeError as e:
            logging.error(f"AIO JSON parse failed: {e}, response_text: {resp.text[:200]}...")  # Catch bad JSON
            return [], 0
        streams = data.get("streams", [])[:INPUT_CAP]
        logging.info(f"AIO streams found: {len(streams)}")  # Log actual streams len
        return streams, len(streams)
    except Exception as e:
        logging.error(f"AIO fetch failed: {e}")
        return [], 0

# ---------------------------
# Filter pipeline
# ---------------------------

def filter_streams(type_: str, id_: str, streams: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], PipeStats]:
    stats = PipeStats()
    stats.merged_in = len(streams)

    expected = get_expected_metadata(type_, id_.split(':')[0]) if TRAKT_VALIDATE_TITLES else {}

    classified = [(s, classify(s)) for s in streams]

    classified = [(s, m) for s, m in classified if not s.get("streamData", {}).get("type") == "error" and not '[❌]' in s.get("name", "")]
    stats.dropped_error = stats.merged_in - len(classified)

    if USE_BLACKLISTS:
        classified = [(s, m) for s, m in classified if not any(re.search(bl, s.get("name", "") + s.get("description", "")) for bl in BLACKLISTS)]
        stats.dropped_blacklist = stats.merged_in - len(classified) - stats.dropped_error

    if USE_FAKES_DB:
        classified = [(s, m) for s, m in classified if m.get("infohash") not in FAKES_DB]
        stats.dropped_fake = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist

    # Size/age placeholders
    if USE_SIZE_MISMATCH:
        pass
    if USE_AGE_HEURISTIC:
        pass

    classified = [(s, m) for s, m in classified if trakt_validate(s, expected)]
    stats.dropped_trakt = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist - stats.dropped_fake

    cached_map = {}
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
                # Don't drop: Assume uncached but deliver
                cached_map[m.get("url", "unknown")] = False  # Fake entry to proceed
                new_classified.append((s, m))
                stats.dropped_verify += 1  # Still count as "dropped" for stats, but actually deliver
                continue
            is_cached = cached_map.get(m["infohash"], False)
            if is_cached:
                new_classified.append((s, m))
            else:
                stats.dropped_verify += 1
        classified = new_classified

    min_seeders = int(os.environ.get("MIN_SEEDERS", "0"))
    classified = [(s, m) for s, m in classified if m["kind"] != "torrent" or m["seeders"] >= min_seeders]
    stats.dropped_seeders = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist - stats.dropped_fake - stats.dropped_trakt - stats.dropped_verify

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

    def sort_key(item):
        s, m = item
        cached = 2 if m["provider"] == "TB" and "Cached" in s.get("name", "") else 1 if "Cached" in s.get("name", "") else 0
        res_val = {'4k': 4, '2160p': 4, '1080p': 3, '720p': 2, '480p': 1}.get(m["res"], 0)
        usenet_boost = 1 if m["kind"] == "usenet" and res_val >= 3 and m["size"] > 2*1024**3 else 0
        non_tb_penalty = -1 if m["provider"] in ["RD", "AD"] and any(im[1]["provider"] == "TB" for im in classified) else 0
        return (-cached - usenet_boost, -res_val, -m["size"], -m["seeders"], non_tb_penalty)

    classified.sort(key=sort_key)

    usenet = [item for item in classified if item[1]["kind"] == "usenet"][:MIN_USENET_KEEP]
    torrent = [item for item in classified if item[1]["kind"] != "usenet"]
    out = usenet + torrent[:MAX_DELIVER - len(usenet)]

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

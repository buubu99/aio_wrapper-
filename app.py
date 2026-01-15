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
from difflib import SequenceMatcher
import threading
# Config from env (defaults updated)
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
PREFERRED_SOURCE = os.environ.get("PREFERRED_METADATA_SOURCE", "tmdb") # Default to TMDB for speed
VERIFY_STREAM = os.environ.get("VERIFY_STREAM", "true").lower() == "true"
TB_API_KEY = os.environ.get("TB_API_KEY", "")
TB_BASE = "https://api.torbox.app"
TB_BATCH_SIZE = 50 # Increased for Pro plan
REFORMAT_STREAMS = os.environ.get("REFORMAT_STREAMS", "true").lower() == "true"
MAX_DESC_CHARS = int(os.environ.get("MAX_DESC_CHARS", "180"))
WRAPPER_DEDUP = os.environ.get("WRAPPER_DEDUP", "true").lower() == "true"
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "30")) # Increased default
# App + logging
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
        record.rid = g.request_id if has_request_context() and hasattr(g, "request_id") else "GLOBAL"
        return True
LOG_LEVEL = _log_level(os.environ.get("LOG_LEVEL", "INFO"))
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(rid)s | %(message)s")
logging.getLogger().addFilter(RequestIdFilter())
@app.before_request
def _before_request() -> None:
    g.request_id = str(uuid.uuid4())[:8]
def _rid() -> str:
    return g.request_id if has_request_context() and hasattr(g, "request_id") else "GLOBAL"
# HTTP session
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.6, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET", "POST"])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)
# Data classes
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
    dropped_pollution: int = 0  # New
    deduped: int = 0
    verify_checked: int = 0
    verify_missing_hash: int = 0
    delivered: int = 0
    delivered_usenet: int = 0
    delivered_torrent: int = 0
# Blacklists / Fakes (examples - customize)
BLACKLISTS = [re.compile(r'\b(cam|hdcam|ts|telesync|fake)\b', re.I)] # Example regex
FAKES_DB = {"examplefakehash": True} # Example hashes
# Transliteration mapping
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
def transliterate_to_latin(text: str) -> str:
    return ''.join(CYRILLIC_MAPPING.get(char, char) for char in text)
# Metadata fetch (racing Trakt/TMDB)
TMDB_BASE = "https://api.themoviedb.org/3"
@lru_cache(maxsize=1000)
def get_expected_metadata(type_: str, id_: str) -> Dict[str, Any]:
    expected = {"trakt": {}, "tmdb": {}}
    lock = threading.Lock()
    def fetch_trakt():
        if not TRAKT_CLIENT_ID:
            return
        slug = id_.split(':')[0] if ':' in id_ else id_
        url = f"https://api.trakt.tv/{type_}s/{slug}?extended=full"
        headers = {"trakt-api-version": "2", "trakt-api-key": TRAKT_CLIENT_ID}
        try:
            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            with lock:
                expected["trakt"] = {"title": data.get("title", ""), "year": data.get("year"), "aliases": data.get("aliases", []), "language": data.get("language", None)}
        except Exception as e:
            logging.warning(f"Trakt fetch failed: {e}")
    def fetch_tmdb():
        if not TMDB_API_KEY:
            return
        slug = id_.split(':')[0] if ':' in id_ else id_
        tmdb_type = "movie" if type_ == "movie" else "tv"
        url = f"{TMDB_BASE}/find/{slug}?api_key={TMDB_API_KEY}&external_source=imdb_id"
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            results = data.get(f"{tmdb_type}_results", [])
            if results:
                detail_id = results[0]["id"]
                detail_url = f"{TMDB_BASE}/{tmdb_type}/{detail_id}?api_key={TMDB_API_KEY}"
                detail_resp = session.get(detail_url, timeout=REQUEST_TIMEOUT)
                detail_resp.raise_for_status()
                detail = detail_resp.json()
                title_key = "title" if type_ == "movie" else "name"
                year_key = "release_date" if type_ == "movie" else "first_air_date"
                year = int(detail[year_key][:4]) if detail.get(year_key) else None
                lang = detail.get("original_language", None)
                with lock:
                    expected["tmdb"] = {"title": detail.get(title_key, ""), "year": year, "aliases": [], "language": lang}
        except Exception as e:
            logging.warning(f"TMDB fetch failed: {e}")
    threads = []
    if PREFERRED_SOURCE == "trakt":
        t1 = threading.Thread(target=fetch_trakt)
        t2 = threading.Thread(target=fetch_tmdb)
    else:
        t1 = threading.Thread(target=fetch_tmdb)
        t2 = threading.Thread(target=fetch_trakt)
    t1.start()
    threads.append(t1)
    t2.start()
    threads.append(t2)
    for t in threads:
        t.join(timeout=REQUEST_TIMEOUT)
    meta = expected["trakt"] or expected["tmdb"] or {}
    # Force English if available (prefer TMDB/Trakt English title)
    if meta and 'title' in meta:
        meta['title'] = transliterate_to_latin(meta['title']) # Normalize to English chars
    return meta
# Helper functions
def classify(stream: Dict[str, Any]) -> Dict[str, Any]:
    orig_name = stream.get("name", "")
    orig_desc = stream.get("description", "")
    name = orig_name.lower()
    desc = orig_desc.lower()
    url = stream.get("url", "").lower()
    external_url = stream.get("externalUrl", "").lower()  # New: Check externalUrl
    bh = stream.get("behaviorHints", {})
    filename_norm = unicodedata.normalize('NFKD', bh.get('filename', '')).lower()
    # Updated provider: Check external, add AIO
    if "aiostreams" in url or "aiostreams" in external_url:
        provider = "AIO"
    elif "torbox" in url or "torbox" in external_url or "tb" in desc:
        provider = "TB"
    elif "realdebrid" in url or "realdebrid" in external_url or "rd" in desc:
        provider = "RD"
    elif "alldeb" in url or "alldeb" in external_url or "ad" in desc:
        provider = "AD"
    else:
        provider = "ND"
    kind = "usenet" if ".nzb" in bh.get("filename", "") or "usenet" in name else "torrent"
    # Updated: Search orig for res (case-sensitive match but upper result)
    res_match = re.search(r'(4[kK]|2160p|1080p|720p|480p)', orig_name + ' ' + orig_desc + ' ' + filename_norm, re.I)
    res = res_match.group(1).upper() if res_match else "SD"
    size = bh.get("videoSize", 0)
    if not size:  # Fallback parse from desc/title
        size_match = re.search(r'(\d+\.?\d*)\s*(GB|MB)', orig_desc + ' ' + orig_name + ' ' + filename_norm, re.I)
        if size_match:
            val = float(size_match.group(1))
            unit = size_match.group(2).upper()
            size = val * (1024 ** 3) if unit == 'GB' else val * (1024 ** 2) if unit == 'MB' else 0
    if isinstance(size, str):
        size = re.sub(r'[^\d.]', '', size)
        try:
            size = float(size) * (1024 ** 3) if size else 0
        except ValueError:
            size = 0
    seeders_match = re.search(r'seeders:(\d+)', name + desc)
    seeders = int(seeders_match.group(1)) if seeders_match else 0
    hash_match = re.search(r'([a-f0-9]{40})', url + external_url)
    infohash = hash_match.group(1) if hash_match else ""
    full_text = name + desc + filename_norm
    lang_match = re.search(r'\b(eng|english|kor|korean|multi|fr|es|spa|spanish|espanol|tam|tel|hin|vostfr)\b', full_text, re.I)
    language = lang_match.group(1).upper() if lang_match else None
    return {"provider": provider, "kind": kind, "res": res, "size": size, "seeders": seeders, "infohash": infohash, "language": language}
def trakt_validate(stream: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    if not TRAKT_VALIDATE_TITLES or not expected:
        return True
    stream_title = transliterate_to_latin(stream.get("name", "")).lower()
    stream_title = re.sub(r'[^a-z0-9 ]', '', unicodedata.normalize('NFKD', stream_title))
    exp_title = re.sub(r'[^a-z0-9 ]', '', expected.get("title", "").lower())
    ratio = SequenceMatcher(None, stream_title, exp_title).ratio()
    if ratio < TRAKT_TITLE_MIN_RATIO:
        return False
    year_match = re.search(r'\b(19|20)\d{2}\b', stream.get("name", "") + stream.get("description", ""))
    if year_match and expected.get("year") and TRAKT_STRICT_YEAR:
        return int(year_match.group(0)) == expected["year"]
    return True
def tb_get_cached(hashes: List[str]) -> Dict[str, bool]:
    if not TB_API_KEY or not hashes:
        return {h: False for h in hashes}
    cached = {}
    for batch_start in range(0, len(hashes), TB_BATCH_SIZE):
        batch = hashes[batch_start:batch_start + TB_BATCH_SIZE]
        params = {"hash": ",".join(batch)}
        headers = {"Authorization": f"Bearer {TB_API_KEY}"}
        try:
            resp = session.get(f"{TB_BASE}/v1/api/torrent/info", params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            for item in resp.json().get("data", []):
                cached[item["hash"].lower()] = item.get("is_cached", False)
        except Exception as e:
            logging.error(f"TB cache check failed: {e}")
    return cached
def dedup_key(stream: Dict[str, Any], meta: Dict[str, Any]) -> str:
    return f"{meta['infohash']}:{meta['size']}:{stream.get('url', '')}"
# New: Pollution check
def is_polluted(stream: Dict[str, Any], type_: str, season: Optional[int], episode: Optional[int]) -> bool:
    full = stream.get("name", "") + " " + stream.get("description", "")
    if type_ == "movie" and re.search(r'S\d+E\d+', full, re.I):
        return True
    if type_ == "series" and season is not None and episode is not None:
        ep_pattern = f'S{season:02}E{episode:02}'
        if not re.search(ep_pattern, full, re.I):
            return True
    return False
def format_stream_inplace(stream: Dict[str, Any], meta: Dict[str, Any], expected: Dict[str, Any], cached_hint: str, type_: str, season: Optional[int] = None, episode: Optional[int] = None) -> None:
    res = meta["res"] or "SD"
    size_gb = round(meta["size"] / (1024 ** 3), 2) if meta["size"] > 0 else 0.0
    size_str = f"{size_gb:.2f} GB" if size_gb > 0 else "? GB"
    provider = meta["provider"]
    lang_emoji = {
        'ENG': '🇺🇸', 'KOR': '🇰🇷', 'ES': '🇪🇸', 'FR': '🇫🇷', 'MULTI': '🌍', 'VOSTFR': '🇫🇷', None: '🌍'
    }.get(meta["language"] or expected.get("language"), '🌍')
    full_text = stream.get("description", "") + " " + stream.get("name", "") + " " + stream.get("behaviorHints", {}).get("filename", "")
    quality_match = re.search(r'(bluray|webrip|hdrip|dv|ddp|web-dl|h264|h.264|xvid|x264|x265|dd\+)', full_text, re.I)
    quality_str = quality_match.group(1).upper() if quality_match else "?"
    codec_match = re.search(r'(h264|h.264|xvid|x264|x265)', quality_str.lower() + full_text.lower(), re.I)
    codec = codec_match.group(1).upper() if codec_match else "?"
    quality_emoji = {
        'BLURAY': '📀', 'WEBRIP': '🌐', 'HDRIP': '🌐', 'DV': '📼', 'DDP': '🔊', 'WEB-DL': '🌐',
        'H264': '📼', 'H.264': '📼', 'XVID': '📼', 'X264': '📼', 'X265': '📼', 'DD+': '🔊'
    }.get(quality_str, '')
    # Name: Always 2 lines left with emojis
    stream["name"] = f"{provider} {cached_hint}\n{res} {size_str} 📺"
    # Desc: Always 3 lines right with emojis
    title_line = f"{expected.get('title', 'Unknown')} ({expected.get('year', '?')})"
    if type_ == "series" and season is not None and episode is not None:
        title_line += f" S{season:02}E{episode:02}"
    desc_lines = [title_line, lang_emoji, f"{quality_str} • {codec} {quality_emoji}"]
    stream["description"] = '\n'.join(desc_lines)
# Get streams from AIO
def get_streams(type_: str, id_: str) -> Tuple[List[Dict[str, Any]], int]:
    if not AIO_URL:
        logging.error("AIO_URL not set")
        return [], 0
    aio_base = AIO_URL.rsplit('/manifest.json', 1)[0] if '/manifest.json' in AIO_URL else AIO_URL
    url = f"{aio_base}/stream/{type_}/{id_}.json"
    headers = {}
    if AIOSTREAMS_AUTH:
        user, pw = AIOSTREAMS_AUTH.split(':', 1)
        headers["Authorization"] = "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()
    logging.info(f"AIO fetch URL: {url}")
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        logging.info(f"AIO status: {resp.status_code}, len: {len(resp.content)}")
        if resp.status_code != 200:
            return [], 0
        data = resp.json()
        logging.info(f"AIO keys: {list(data.keys())}")
        streams = data.get("streams", [])[:INPUT_CAP]
        logging.info(f"AIO streams len: {len(streams)}")
        return streams, len(streams)
    except json.JSONDecodeError as e:
        logging.error(f"AIO JSON error: {e}, text: {resp.text[:200]}")
        return [], 0
    except Exception as e:
        logging.error(f"AIO fetch error: {e}")
        return [], 0
# Filter pipeline
def filter_streams(type_: str, id_: str, streams: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], PipeStats]:
    stats = PipeStats()
    stats.aio_in = len(streams)
    stats.merged_in = len(streams)
    expected = get_expected_metadata(type_, id_)
    # Parse season/episode from id if series (e.g., "tmdb:123:1:3" → season=1, episode=3)
    season = episode = None
    if type_ == "series" and ':' in id_:
        parts = id_.split(':')
        if len(parts) >= 3:
            season = int(parts[-2])
            episode = int(parts[-1])
    classified = []
    for s in streams:
        # Coerce non-strings
        if not isinstance(s.get("name"), str):
            s["name"] = "Unknown Provider"
        if not isinstance(s.get("description"), str):
            s["description"] = "No Description"
        # Drop control/RTL chars
        control_re = re.compile(r'[\x00-\x1F\x7F-\x9F\u200E\u200F\u202A-\u202E]')
        s["name"] = control_re.sub('', s.get("name", ''))
        s["description"] = control_re.sub('', s.get("description", ''))
        # Drop if no URL
        if not s.get("url") and not s.get("externalUrl"):
            stats.dropped_error += 1
            continue
        classified.append((s, classify(s)))
    classified = [cm for cm in classified if cm[0].get("streamData", {}).get("type") != "error" and '[❌]' not in cm[0].get("name", "")]
    stats.dropped_error = stats.merged_in - len(classified)
    # New: Pollution drop
    new_classified = []
    for cm in classified:
        if is_polluted(cm[0], type_, season, episode):
            stats.dropped_pollution += 1
            continue
        new_classified.append(cm)
    classified = new_classified
    if USE_BLACKLISTS:
        classified = [cm for cm in classified if not any(bl.search(cm[0].get("name", "") + cm[0].get("description", "")) for bl in BLACKLISTS)]
        stats.dropped_blacklist = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_pollution
    if USE_FAKES_DB:
        classified = [cm for cm in classified if cm[1]["infohash"] not in FAKES_DB]
        stats.dropped_fake = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist - stats.dropped_pollution
    if USE_SIZE_MISMATCH:
        classified = [cm for cm in classified if cm[1]["size"] > 100 * 1024**2] # >100MB example
        stats.dropped_size = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist - stats.dropped_fake - stats.dropped_pollution
    if USE_AGE_HEURISTIC:
        classified = [cm for cm in classified if time.time() - cm[1].get("age", time.time()) < 30*24*3600] # <30 days example (add age parse if needed)
        stats.dropped_age = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist - stats.dropped_fake - stats.dropped_size - stats.dropped_pollution
    classified = [cm for cm in classified if trakt_validate(cm[0], expected)]
    stats.dropped_trakt = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist - stats.dropped_fake - stats.dropped_size - stats.dropped_age - stats.dropped_pollution
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
                new_classified.append((s, m))
                continue
            if cached_map.get(m["infohash"], False):
                new_classified.append((s, m))
            else:
                stats.dropped_verify += 1
        classified = new_classified
    min_seeders = int(os.environ.get("MIN_SEEDERS", "0"))
    classified = [cm for cm in classified if cm[1]["kind"] != "torrent" or cm[1]["seeders"] >= min_seeders]
    stats.dropped_seeders = stats.merged_in - len(classified) - stats.dropped_error - stats.dropped_blacklist - stats.dropped_fake - stats.dropped_trakt - stats.dropped_verify - stats.dropped_size - stats.dropped_age - stats.dropped_pollution
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
        res_val = {'4K': 4, '2160P': 4, '1080P': 3, '720P': 2, '480P': 1, 'SD': 0}.get(m["res"], 0)
        usenet_boost = 1 if m["kind"] == "usenet" and res_val >= 3 and m["size"] > 2*1024**3 else 0
        non_tb_penalty = -1 if m["provider"] in ["RD", "AD"] and any(im[1]["provider"] == "TB" for im in classified) else 0
        return (-cached - usenet_boost, -res_val, -m["size"], -m["seeders"], non_tb_penalty)
    classified.sort(key=sort_key)
    usenet = [item for item in classified if item[1]["kind"] == "usenet"][:MIN_USENET_KEEP]
    torrent = [item for item in classified if item[1]["kind"] != "usenet"]
    out = usenet + torrent[:MAX_DELIVER - len(usenet)]
    if REFORMAT_STREAMS:
        for s, m in out:
            cached_hint = "⚡" if m["provider"] == "TB" and cached_map.get(m["infohash"], False) else ""
            format_stream_inplace(s, m, expected, cached_hint, type_, season, episode)
    stats.delivered = len(out)
    stats.delivered_usenet = len(usenet)
    stats.delivered_torrent = stats.delivered - stats.delivered_usenet
    return [s for s, m in out], stats
# Endpoints
@app.get("/health")
def health():
    return jsonify({"ok": True, "ts": int(time.time())}), 200
@app.get("/manifest.json")
def manifest():
    return jsonify(
        {
            "id": "org.jason.wrapper",
            "version": "1.3.0",  # Bumped for emoji/format
            "name": "AIO Wrapper",
            "description": "Wraps AIOStreams to filter and format streams w usenet",
            "resources": ["stream"],
            "types": ["movie", "series"],
            "catalogs": [],
            "idPrefixes": ["tt", "tmdb"]
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
    except Exception as e:
        logging.exception(f"Stream error: {e}")
        return jsonify({"streams": []}), 200
    finally:
        logging.info(
            "WRAP_STATS rid=%s type=%s id=%s aio_in=%s merged_in=%s dropped_error=%s dropped_blacklist=%s dropped_fake=%s dropped_size=%s dropped_age=%s dropped_trakt=%s dropped_verify=%s dropped_seeders=%s dropped_pollution=%s deduped=%s verify_checked=%s verify_missing_hash=%s delivered=%s usenet=%s torrent=%s ms=%s",
            _rid(), type_, id_,
            stats.aio_in, stats.merged_in,
            stats.dropped_error, stats.dropped_blacklist, stats.dropped_fake,
            stats.dropped_size, stats.dropped_age, stats.dropped_trakt,
            stats.dropped_verify, stats.dropped_seeders, stats.dropped_pollution,
            stats.deduped,
            stats.verify_checked, stats.verify_missing_hash,
            stats.delivered, stats.delivered_usenet, stats.delivered_torrent,
            int((time.time() - t0) * 1000)
        )
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=LOG_LEVEL == logging.DEBUG, use_reloader=False)

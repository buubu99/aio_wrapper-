from __future__ import annotations
import base64
import hashlib
import json
import logging
import os
import re
import time
import unicodedata
import uuid
import difflib
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
import requests
from flask import Flask, jsonify, g, has_request_context, request, redirect, make_response, Response
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from werkzeug.exceptions import HTTPException

def _parse_bool(v: str, default: bool = False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1","true","yes","y","on"):
        return True
    if s in ("0","false","no","n","off"):
        return False
    return default

def _normalize_base(raw: str) -> str:
    raw = (raw or "").strip().rstrip("/")
    # Accept either a base addon URL or a full manifest URL.
    # Tolerate extra slashes, query params, and pasted .../manifest.json.
    if "?" in raw:
        raw = raw.split("?", 1)[0].rstrip("/")
    idx = raw.find("/manifest.json")
    if idx != -1:
        raw = raw[:idx]
    return raw.rstrip("/")

def _safe_int(v, default: int) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default

def _safe_float(v, default: float) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return default

def _safe_csv(v, default: str = "") -> list[str]:
    s = default if v is None else str(v)
    return [x.strip() for x in s.split(",") if x.strip()]


# ---------------------------
# Config (keep env names compatible with your existing Render setup)
# ---------------------------
AIO_URL = os.environ.get("AIO_URL", "")
# Robust: accepts either a base addon URL or a full manifest URL.
# Preserves token-in-path URLs while stripping any trailing /manifest.json.
AIO_BASE = _normalize_base(os.environ.get("AIO_BASE", "") or AIO_URL)
# can be base url or .../manifest.json
AIO_AUTH = os.environ.get("AIOSTREAMS_AUTH", "")  # "user:pass"
AIOSTREAMS_AUTH = AIO_AUTH  # backward-compat alias
# Optional second provider (another AIOStreams-compatible addon)
PROV2_URL = os.environ.get("PROV2_URL", "")
# Robust: accepts either a base addon URL or a full manifest URL.
# Preserves token-in-path URLs while stripping any trailing /manifest.json.
PROV2_BASE = _normalize_base(os.environ.get("PROV2_BASE", "") or PROV2_URL)
PROV2_AUTH = os.environ.get("PROV2_AUTH", "")  # 'user:pass' for Basic auth if needed
PROV2_TAG = os.environ.get("PROV2_TAG", "P2")
ANDROID_MAX_DELIVER = _safe_int(os.environ.get('ANDROID_MAX_DELIVER', '60'), 60)

INPUT_CAP = _safe_int(os.environ.get('INPUT_CAP', '4500'), 4500)
MAX_DELIVER = _safe_int(os.environ.get('MAX_DELIVER', '80'), 80)
# Formatting / UI constraints
REFORMAT_STREAMS = _parse_bool(os.environ.get("REFORMAT_STREAMS", "true"), True)
OUTPUT_NEW_OBJECT = _parse_bool(os.environ.get("OUTPUT_NEW_OBJECT", "true"), True)  # build brand-new stream objects (safe schema)
OUTPUT_LEFT_LINES = _safe_int(os.environ.get('OUTPUT_LEFT_LINES', '2'), 2)  # UI: 2 lines on left (quality + provider)
FORCE_ASCII_TITLE = _parse_bool(os.environ.get("FORCE_ASCII_TITLE", "true"), True)
MAX_TITLE_CHARS = _safe_int(os.environ.get('MAX_TITLE_CHARS', '110'), 110)
MAX_DESC_CHARS = _safe_int(os.environ.get('MAX_DESC_CHARS', '180'), 180)
# Optional: prettier names (emojis + single-line)
PRETTY_EMOJIS = _parse_bool(os.environ.get("PRETTY_EMOJIS", "true"), True)
NAME_SINGLE_LINE = _parse_bool(os.environ.get("NAME_SINGLE_LINE", "true"), True)
# Optional: title similarity drop (Trakt-like naming; works without Trakt)
TRAKT_VALIDATE_TITLES = _parse_bool(os.environ.get("TRAKT_VALIDATE_TITLES", "true"), True)
TRAKT_TITLE_MIN_RATIO = _safe_float(os.environ.get('TRAKT_TITLE_MIN_RATIO', '0.65'), 0.65)
TRAKT_STRICT_YEAR = _parse_bool(os.environ.get("TRAKT_STRICT_YEAR", "false"), False)

# Validation/testing toggles
VALIDATE_OFF = _parse_bool(os.environ.get("VALIDATE_OFF", "false"), False)  # pass-through for format testing
DROP_POLLUTED = _parse_bool(os.environ.get("DROP_POLLUTED", "true"), True)  # optional
# TorBox cache hint (optional; safe if unset)
TB_API_KEY = os.environ.get("TB_API_KEY", "")
TB_BASE = "https://api.torbox.app"
TB_BATCH_SIZE = _safe_int(os.environ.get('TB_BATCH_SIZE', '50'), 50)
TB_MAX_HASHES = _safe_int(os.environ.get('TB_MAX_HASHES', '60'), 60)  # limit hashes checked per request for speed
TB_API_MIN_HASHES = _safe_int(os.environ.get('TB_API_MIN_HASHES', '20'), 20)  # skip TorBox API calls if fewer hashes
TB_CACHE_HINTS = _parse_bool(os.environ.get("TB_CACHE_HINTS", "true"), True)  # enable TorBox cache hint lookups
TB_USENET_CHECK = _parse_bool(os.environ.get("TB_USENET_CHECK", "false"), False)  # optional usenet cache checks (requires identifiers)
REQUEST_TIMEOUT = _safe_float(os.environ.get('REQUEST_TIMEOUT', '30'), 30.0)
# Stream response cache TTL exposed to Stremio clients (seconds)
CACHE_TTL = _safe_int(os.environ.get('CACHE_TTL', '600'), 600)

# Client-side time budgets (seconds). These are upper bounds; we return as soon as we have results.
ANDROID_STREAM_TIMEOUT = _safe_float(os.environ.get('ANDROID_STREAM_TIMEOUT', '20'), 20.0)  # FIXED: increase default
DESKTOP_STREAM_TIMEOUT = _safe_float(os.environ.get('DESKTOP_STREAM_TIMEOUT', '30'), 30.0)  # FIXED: increase default

# Upstream fetch timeouts (seconds) used inside /stream.
# We keep P2 tighter because it can hang and trigger Gunicorn worker aborts if retries are enabled.
ANDROID_AIO_TIMEOUT = _safe_float(os.environ.get('ANDROID_AIO_TIMEOUT', '18'), 18.0)  # FIXED: increase default
ANDROID_P2_TIMEOUT = _safe_float(os.environ.get('ANDROID_P2_TIMEOUT', '12'), 12.0)  # FIXED: increase default
DESKTOP_AIO_TIMEOUT = _safe_float(os.environ.get('DESKTOP_AIO_TIMEOUT', '28'), 28.0)  # FIXED: increase default
DESKTOP_P2_TIMEOUT = _safe_float(os.environ.get('DESKTOP_P2_TIMEOUT', '15'), 15.0)  # FIXED: increase default

# TorBox API call timeout (seconds) used during cache checks.
TB_API_TIMEOUT = _safe_float(os.environ.get('TB_API_TIMEOUT', '8'), 8.0)
TMDB_TIMEOUT = _safe_float(os.environ.get('TMDB_TIMEOUT', '8'), 8.0)
# TMDB for metadata
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
BUILD_ID = os.environ.get("BUILD_ID", "1.0")
# Additional filters
MIN_SEEDERS = _safe_int(os.environ.get('MIN_SEEDERS', '1'), 1)
PREFERRED_LANG = os.environ.get("PREFERRED_LANG", "EN").upper()
# Premium priorities and verification
PREMIUM_PRIORITY = _safe_csv(os.environ.get('PREMIUM_PRIORITY', 'TB,RD,AD,ND'))
USENET_PRIORITY = _safe_csv(os.environ.get('USENET_PRIORITY', 'ND,EW,NG'))
VERIFY_PREMIUM = _parse_bool(os.environ.get("VERIFY_PREMIUM", "true"), True)
ASSUME_PREMIUM_ON_FAIL = _parse_bool(os.environ.get("ASSUME_PREMIUM_ON_FAIL", "false"), False)
POLL_ATTEMPTS = _safe_int(os.environ.get('POLL_ATTEMPTS', '2'), 2)

# TorBox WebDAV (optional fast existence checks; gated by USE_TB_WEBDAV)
USE_TB_WEBDAV = _parse_bool(os.environ.get('USE_TB_WEBDAV', 'true'), True)
TB_WEBDAV_URL = os.environ.get('TB_WEBDAV_URL', 'https://webdav.torbox.app')
TB_WEBDAV_USER = os.environ.get('TB_WEBDAV_USER', '')
TB_WEBDAV_PASS = os.environ.get('TB_WEBDAV_PASS', '')
TB_WEBDAV_TIMEOUT = _safe_float(os.environ.get('TB_WEBDAV_TIMEOUT', '1.0'), 1.0)
TB_WEBDAV_WORKERS = _safe_int(os.environ.get('TB_WEBDAV_WORKERS', '10'), 10)
TB_WEBDAV_TEMPLATES = [t.strip() for t in os.environ.get('TB_WEBDAV_TEMPLATES', 'downloads/{hash}/').split(',') if t.strip()]

# Optional: drop TorBox streams that WebDAV cannot confirm (fast-ish, but still extra requests)
TB_WEBDAV_STRICT = _parse_bool(os.environ.get('TB_WEBDAV_STRICT', 'true'), True)

# Optional: cached/instant validation for RD/AD (heuristics by default; strict workflow is opt-in)
VERIFY_CACHED_ONLY = _parse_bool(os.environ.get("VERIFY_CACHED_ONLY", "false"), False)
STRICT_PREMIUM_ONLY = _parse_bool(os.environ.get('STRICT_PREMIUM_ONLY', 'false'), False)  # loose default; strict drops uncached
MIN_CACHE_CONFIDENCE = _safe_float(os.environ.get('MIN_CACHE_CONFIDENCE', '0.8'), 0.8)
VALIDATE_CACHE_TIMEOUT = _safe_float(os.environ.get('VALIDATE_CACHE_TIMEOUT', '10'), 10.0)

# Cancelled RD/AD instant checks – removed functions, now heuristics only
# (No RD_STRICT_CACHE_CHECK, RD_API_KEY, AD_STRICT_CACHE_CHECK, AD_API_KEY)

# Limit strict cache checks per request to avoid excessive API churn
STRICT_CACHE_MAX = _safe_int(os.environ.get('STRICT_CACHE_MAX', '18'), 18)

# --- Add-ons / extensions (optional) ---
DEPRIORITIZE_RD = _parse_bool(os.environ.get("DEPRIORITIZE_RD", "false"), False)
DROP_RD = _parse_bool(os.environ.get("DROP_RD", "false"), False)
DROP_AD = _parse_bool(os.environ.get("DROP_AD", "false"), False)

MIN_RES = max(_safe_int(os.environ.get('MIN_RES', '1080'), 1080), 1080)  # hard floor: never below 1080
MAX_AGE_DAYS = _safe_int(os.environ.get('MAX_AGE_DAYS', '0'), 0)  # 0 = off
USE_AGE_HEURISTIC = _parse_bool(os.environ.get("USE_AGE_HEURISTIC", "true"), True)

ADD_CACHE_HINT = _parse_bool(os.environ.get("ADD_CACHE_HINT", "true"), True)

# TorBox strict cache filtering (different from TB_CACHE_HINTS which only adds a hint)
TORBOX_CACHE_CHECK = _parse_bool(os.environ.get("TORBOX_CACHE_CHECK", "true"), True)
VERIFY_TB_CACHE_OFF = _parse_bool(os.environ.get("VERIFY_TB_CACHE_OFF", "false"), False)

# Wrapper behavior toggles

# Use short opaque /r/<token> urls instead of base64-encoding the full upstream URL.
# Fixes Android/Google TV URL-length limits and keeps playback URLs private.
WRAP_URL_SHORT = _parse_bool(os.environ.get("WRAP_URL_SHORT", "true"), True)
WRAP_URL_TTL = _safe_int(os.environ.get('WRAP_URL_TTL', '3600'), 3600)  # seconds
WRAP_HEAD_MODE = (os.environ.get("WRAP_HEAD_MODE", "200_noloc") or "200_noloc").strip().lower()
WRAPPER_DEDUP = _parse_bool(os.environ.get("WRAPPER_DEDUP", "true"), True)

VERIFY_STREAM = _parse_bool(os.environ.get("VERIFY_STREAM", "true"), True)
VERIFY_STREAM_TIMEOUT = _safe_float(os.environ.get('VERIFY_STREAM_TIMEOUT', '4'), 4.0)

# Stronger playback verification (catches upstream /static/500.mp4 placeholders)
VERIFY_RANGE = _parse_bool(os.environ.get("VERIFY_RANGE", "true"), True)
VERIFY_DROP_STATIC_500 = _parse_bool(os.environ.get("VERIFY_DROP_STATIC_500", "true"), True)
VERIFY_MIN_TOTAL_BYTES = _safe_int(os.environ.get('VERIFY_MIN_TOTAL_BYTES', '5000000'), 5000000)  # 5MB floor
ANDROID_VERIFY_TOP_N = _safe_int(os.environ.get('ANDROID_VERIFY_TOP_N', '12'), 12)
ANDROID_VERIFY_TIMEOUT = _safe_float(os.environ.get('ANDROID_VERIFY_TIMEOUT', '2.2'), 2.2)


# Force a minimum share of usenet results (if they exist)
MIN_USENET_KEEP = _safe_int(os.environ.get('MIN_USENET_KEEP', '3'), 3)
MIN_USENET_DELIVER = _safe_int(os.environ.get('MIN_USENET_DELIVER', '3'), 3)

# Optional local/remote filtering sources
USE_BLACKLISTS = _parse_bool(os.environ.get("USE_BLACKLISTS", "true"), True)
BLACKLIST_TERMS = [t.strip().lower() for t in os.environ.get("BLACKLIST_TERMS", "").split(",") if t.strip()]
BLACKLIST_URL = os.environ.get("BLACKLIST_URL", "")
USE_FAKES_DB = _parse_bool(os.environ.get("USE_FAKES_DB", "true"), True)
FAKES_DB_URL = os.environ.get("FAKES_DB_URL", "")
USE_SIZE_MISMATCH = _parse_bool(os.environ.get("USE_SIZE_MISMATCH", "true"), True)

# Optional: simple rate-limit (e.g. "30/m", "5/s"). Blank disables it.
RATE_LIMIT = (os.environ.get("RATE_LIMIT", "") or "").strip()

# ---------------------------
# Helpers (used by the pipeline)
# ---------------------------
_blacklist_cache = {"ts": 0.0, "terms": set()}
_fakes_cache = {"ts": 0.0, "hashes": set()}
_BLACKLIST_LOCK = threading.Lock()
_FAKES_LOCK = threading.Lock()


def _truncate(s: str, max_chars: int) -> str:
    if s is None:
        return ""
    s = str(s)
    if max_chars <= 0:
        return s
    return s if len(s) <= max_chars else (s[: max_chars - 1] + "…")


def normalize_display_title(title: str) -> str:
    'Normalize a title for display (optionally force ASCII).'
    if title is None:
        return ""
    t = str(title)
    t = unicodedata.normalize('NFKC', t)
    # strip control chars
    t = ''.join(ch for ch in t if ch >= ' ')
    t = re.sub(r'\s+', ' ', t).strip()
    if FORCE_ASCII_TITLE:
        t = unicodedata.normalize('NFKD', t)
        t = t.encode('ascii', 'ignore').decode('ascii')
        t = re.sub(r'\s+', ' ', t).strip()
    return t

def normalize_label(label: str) -> str:
    """Normalize a noisy filename/bingeGroup/name into a stable, comparable label.

    Used only for dedup keys when infohash is missing.
    """
    if not label:
        return ""
    s = unicodedata.normalize('NFKC', str(label)).lower().strip()
    s = re.sub(r'\s+', ' ', s)
    # Remove bracketed tags that often create fake differences
    s = re.sub(r'[\[\(\{].*?[\]\)\}]', ' ', s)
    # Drop common file extensions
    s = re.sub(r'\.(mkv|mp4|avi|webm|ts|m2ts)$', '', s, flags=re.IGNORECASE)
    # Keep only simple chars for stability
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s



def _human_size_bytes(n: int) -> str:
    try:
        n = int(n or 0)
    except Exception:
        n = 0
    if n <= 0:
        return "0B"
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{f:.1f}{units[i]}".replace('.0', '')


def _extract_year(text: str) -> Optional[int]:
    """Extract a plausible year from text (e.g. 1999, 2024)."""
    if not text:
        return None
    m = re.search(r"\b(19|20)\d{2}\b", str(text))
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None

def _extract_infohash(text: str) -> str | None:
    '''Extract a 40-hex infohash from common patterns like IH:<hash>, infohash=<hash>, btih:<hash>.'''
    if not text:
        return None
    m = re.search(r"\b(?:ih|infohash|btih)\s*[:=]\s*([0-9a-fA-F]{40})\b", text, flags=re.I)
    if m:
        return m.group(1).lower()
    # Some providers embed the hash without a label; accept only if it appears as a standalone token.
    m = re.search(r"\b([0-9a-fA-F]{40})\b", text)
    if m:
        return m.group(1).lower()
    return None



def is_premium_plan(provider: str) -> bool:
    prov = (provider or '').upper().strip()
    if prov in (p.strip().upper() for p in PREMIUM_PRIORITY if p.strip()):
        return True
    if prov in (p.strip().upper() for p in USENET_PRIORITY if p.strip()):
        return True
    # Unknown providers are treated as non-premium
    return False


def sanitize_stream_inplace(s: Dict[str, Any]) -> bool:
    if not isinstance(s, dict):
        return False
    # Some add-ons return externalUrl instead of url
    if not s.get('url') and s.get('externalUrl'):
        s['url'] = s.get('externalUrl')
    url = s.get('url')
    if not url or not isinstance(url, str):
        return False
    # Ensure behaviorHints is a dict
    bh = s.get('behaviorHints')
    if bh is None or not isinstance(bh, dict):
        s['behaviorHints'] = {}
    return True


def _load_remote_lines(url: str, timeout: float = 4.0) -> List[str]:
    if not url:
        return []
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code != 200:
            return []
        lines = []
        for line in r.text.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            lines.append(line)
        return lines
    except Exception:
        return []


def _is_blacklisted(text: str) -> bool:
    if not text:
        return False
    t = str(text).lower()
    terms = set(BLACKLIST_TERMS)
    # Optional remote list (cache for 1h)
    if BLACKLIST_URL:
        now = time.time()
        with _BLACKLIST_LOCK:
            if now - _blacklist_cache['ts'] > 3600:
                remote = [x.lower() for x in _load_remote_lines(BLACKLIST_URL)]
                _blacklist_cache['terms'] = set(remote)
                _blacklist_cache['ts'] = now
            terms |= set(_blacklist_cache['terms'])
    for term in terms:
        if term and term in t:
            return True
    return False

def _load_fakes_db() -> set:
    # Cache for 6h
    if not FAKES_DB_URL:
        return set()
    now = time.time()
    with _FAKES_LOCK:
        if now - _fakes_cache['ts'] < 21600 and _fakes_cache['hashes']:
            return set(_fakes_cache['hashes'])
    hashes = set()
    for line in _load_remote_lines(FAKES_DB_URL, timeout=6.0):
        h = re.sub(r'[^0-9a-fA-F]', '', line).lower()
        if len(h) == 40:
            hashes.add(h)
    with _FAKES_LOCK:
        _fakes_cache['hashes'] = set(hashes)
        _fakes_cache['ts'] = now
    return hashes

def _parse_content_range_total(cr: Optional[str]) -> Optional[int]:
    # Example: "bytes 0-0/16440"
    if not cr or "/" not in cr:
        return None
    try:
        total = cr.split("/")[-1].strip()
        return int(total) if total.isdigit() else None
    except Exception:
        return None


def _looks_like_static_500(url: str) -> bool:
    u = (url or "").lower()
    return "500.mp4" in u and "/static/" in u


def _verify_stream_url(s: Dict[str, Any], timeout: Optional[float] = None, range_mode: Optional[bool] = None) -> bool:
    # Keep this cheap: allow magnets and stremio://
    url = (s.get('url') or '').strip()
    if not url:
        return False
    if url.startswith('magnet:') or url.startswith('stremio:'):
        return True
    if not (url.startswith('http://') or url.startswith('https://')):
        return True

    t = VERIFY_STREAM_TIMEOUT if timeout is None else timeout
    rm = VERIFY_RANGE if range_mode is None else range_mode

    try:
        if rm:
            r = fast_session.get(
                url,
                headers={'Range': 'bytes=0-0', 'User-Agent': ''},
                timeout=t,
                allow_redirects=True,
                stream=True,
            )
            final_url = getattr(r, 'url', '') or ''
            if VERIFY_DROP_STATIC_500 and _looks_like_static_500(final_url):
                return False
            total = _parse_content_range_total(r.headers.get('Content-Range'))
            if total is not None and total < VERIFY_MIN_TOTAL_BYTES:
                return False
            return 200 <= r.status_code < 400

        r = fast_session.head(url, timeout=t, allow_redirects=True, headers={'User-Agent': ''})
        if r.status_code in (405, 403, 401):
            r = fast_session.get(url, timeout=t, stream=True, allow_redirects=True, headers={'User-Agent': ''})
        return 200 <= r.status_code < 400
    except Exception:
        return False


def _provider_rank(provider: str) -> int:
    prov = (provider or '').upper().strip()
    if prov in PREMIUM_PRIORITY:
        return PREMIUM_PRIORITY.index(prov)
    if prov in USENET_PRIORITY:
        return len(PREMIUM_PRIORITY) + USENET_PRIORITY.index(prov)
    return 999


def tb_get_cached(hashes: List[str]) -> Dict[str, bool]:
    if not TB_API_KEY or not hashes:
        return {}
    out: Dict[str, bool] = {}
    headers = {
        'Authorization': f'Bearer {TB_API_KEY}',
        'X-API-Key': TB_API_KEY,
        'Accept': 'application/json',
    }
    # TorBox supports POST /v1/api/torrents/checkcached with a list of hashes.
    url = f'{TB_BASE}/v1/api/torrents/checkcached'
    for i in range(0, len(hashes), max(1, TB_BATCH_SIZE)):
        batch = hashes[i:i+TB_BATCH_SIZE]
        try:
            # TorBox `format` supports "object" or "list" ("list" is fastest). Using "hash" causes a 400.
            r = session.post(url, params={'format': 'list', 'list_files': '0'}, json={'hashes': batch}, headers=headers, timeout=TB_API_TIMEOUT)
            if r.status_code != 200:
                continue
            data = r.json()
            # Common response style: {success: bool, data: {hash: {...}}}
            d = data.get('data') if isinstance(data, dict) else None
            if isinstance(d, dict):
                cached = {str(k).lower().strip() for k in d.keys() if k}
            elif isinstance(d, list):
                cached = set()
                for x in d:
                    if isinstance(x, str):
                        cached.add(x)
                    elif isinstance(x, dict):
                        hx = x.get('hash') or x.get('info_hash')
                        if isinstance(hx, str):
                            cached.add(hx.lower().strip())
            else:
                cached = set()
            cached_norm = {str(x).lower().strip() for x in cached if x}
            for h in batch:
                hh = (h or '').lower().strip()
                out[hh] = hh in cached_norm
        except Exception:
            continue
    return out


def tb_get_usenet_cached(hashes: List[str]) -> Dict[str, bool]:
    """Optional TorBox Usenet cached availability check.

    TorBox exposes a separate usenet endpoint. Many Stremio streams do not carry a usable usenet identifier,
    so this is only used when we can collect identifiers (stored in meta['usenet_hash']).
    """
    if not TB_API_KEY or not hashes:
        return {}
    out: Dict[str, bool] = {}
    headers = {
        'Authorization': f'Bearer {TB_API_KEY}',
        'X-API-Key': TB_API_KEY,
        'Accept': 'application/json',
    }
    url = f'{TB_BASE}/v1/api/usenet/checkcached'
    for i in range(0, len(hashes), max(1, TB_BATCH_SIZE)):
        batch = hashes[i:i+TB_BATCH_SIZE]
        try:
            # Swagger/Postman show `hash` as the repeated query parameter; `format=list` returns the cached hashes.
            params = [('format', 'list')]
            params.extend([('hash', h) for h in batch])
            r = session.get(url, params=params, headers=headers, timeout=TB_API_TIMEOUT)
            if r.status_code != 200:
                continue
            data = r.json() if r.content else {}
            d = data.get('data') if isinstance(data, dict) else None
            if isinstance(d, dict):
                cached = {str(k).lower().strip() for k in d.keys() if k}
            elif isinstance(d, list):
                cached = set([x for x in d if isinstance(x, str)])
            else:
                cached = set()
            for h in batch:
                out[h] = h in cached
        except Exception:
            continue
    return out


class _WebDavUnauthorized(RuntimeError):
    """Raised when TorBox WebDAV returns 401 (bad/missing credentials)."""


def _tb_webdav_exists(url: str) -> bool:
    try:
        r = session.request(
            'PROPFIND',
            url,
            headers={'Depth': '0'},
            auth=(TB_WEBDAV_USER, TB_WEBDAV_PASS),
            timeout=TB_WEBDAV_TIMEOUT,
        )
        if r.status_code == 401:
            raise _WebDavUnauthorized('TorBox WebDAV unauthorized (401)')
        return r.status_code in (200, 207)
    except _WebDavUnauthorized:
        raise
    except Exception:
        return False


def tb_webdav_batch_check(hashes: List[str]) -> set:
    if not hashes or not TB_WEBDAV_URL:
        return set()
    base = TB_WEBDAV_URL.rstrip('/')

    # One-time credential probe: prevents spamming 401s on every hash when creds are missing/wrong.
    _ = _tb_webdav_exists(base + '/')

    ok = set()
    urls = []
    for h in hashes:
        for tmpl in TB_WEBDAV_TEMPLATES or ['downloads/{hash}/']:
            path = tmpl.format(hash=h).lstrip('/')
            urls.append((h, f'{base}/{path}'))
    def worker(item):
        h, u = item
        ok = _tb_webdav_exists(u)
        if ok:
            return h
        return None
    with ThreadPoolExecutor(max_workers=max(1, TB_WEBDAV_WORKERS)) as ex:
        for res in ex.map(worker, urls):
            if res:
                ok.add(res)
    return ok


def _is_valid_stream_id(type_: str, id_: str) -> bool:
    if type_ not in ('movie', 'series'):
        return False
    if not id_ or len(id_) > 220:
        return False
    # allow letters, digits, underscore, dash, dot, colon
    return re.fullmatch(r'[A-Za-z0-9_\-\.:]+', id_) is not None


# ---------------------------
# Lang normalization map (fix mismatch bug)
# ---------------------------
LANG_MAP = {
    "ENG": "EN",
    "ENGLISH": "EN",
    "SPANISH": "ES",
    "FRENCH": "FR",
    "GERMAN": "DE",
    "ITALIAN": "IT",
    "KOREAN": "KO",
    "JAPANESE": "JA",
    "CHINESE": "ZH",
    "MULTI": "MULTI",
    "VOSTFR": "FR",
    "SUBBED": "SUBBED",
}
# ---------------------------
# App + logging
# ---------------------------

# --- Playback URL HEAD workaround (some clients HEAD-check stream URLs) ---

WRAP_PLAYBACK_URLS = _parse_bool(os.environ.get("WRAP_PLAYBACK_URLS", "true"), True)
USENET_PSEUDO_INFOHASH = os.getenv("USENET_PSEUDO_INFOHASH", "1").strip() not in ("0", "false", "False")
WRAP_DEBUG = os.getenv("WRAP_DEBUG", "0").strip() in ("1", "true", "True")

def _pseudo_infohash_usenet(usenet_hash: str) -> str:
    """Create a deterministic 40-hex pseudo-infohash for Usenet items.

    Usenet has no BitTorrent infohash. We use SHA1("usenet:"+hash) to create
    a stable 40-hex identifier useful for dedup/sorting.
    """
    h = (usenet_hash or "").strip().lower()
    if not h:
        return ""
    if re.fullmatch(r"[0-9a-f]{40}", h):
        return h
    return hashlib.sha1(("usenet:" + h).encode("utf-8")).hexdigest()


def _public_base_url() -> str:
    """Best-effort public base URL behind proxies/CDN."""
    proto = request.headers.get("X-Forwarded-Proto") or request.scheme or "https"
    host = request.headers.get("X-Forwarded-Host") or request.host
    return f"{proto}://{host}/"

def _b64u_encode(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode("utf-8")).decode("ascii").rstrip("=")

def _b64u_decode(token: str) -> str:
    pad = '=' * (-len(token) % 4)
    return base64.urlsafe_b64decode((token + pad).encode("ascii")).decode("utf-8")


def _zurl_encode(url: str) -> str:
    """Compress+base64url encode a URL into a restart-safe token.

    This replaces the old in-memory short-token map (which breaks after redeploy).
    Token format: 'z' + base64url(zlib.compress(url_utf8))
    """
    import zlib
    raw = url.encode("utf-8")
    comp = zlib.compress(raw, level=9)
    return "z" + base64.urlsafe_b64encode(comp).decode("ascii").rstrip("=")

def _zurl_decode(token: str) -> str:
    """Decode a token produced by _zurl_encode."""
    import zlib
    if not token or not token.startswith("z"):
        raise ValueError("not ztoken")
    tok = token[1:]
    pad = "=" * (-len(tok) % 4)
    comp = base64.urlsafe_b64decode((tok + pad).encode("ascii"))
    raw = zlib.decompress(comp)
    return raw.decode("utf-8")

# In-memory short URL map for /r/<token> -> upstream URL
# Note: With WEB_CONCURRENCY>1, each worker has its own map; keep concurrency=1 for reliability.
_WRAP_URL_MAP = {}  # token -> (url, expires_epoch)
_WRAP_URL_LOCK = threading.Lock()

def _wrap_url_store(url: str) -> str:
    """Store a playback URL and return an opaque short token."""
    import os, time, base64
    # 9 random bytes -> 12 chars base64url (no padding)
    token = base64.urlsafe_b64encode(os.urandom(9)).decode('ascii').rstrip('=')
    exp = time.time() + max(60, int(WRAP_URL_TTL or 3600))
    with _WRAP_URL_LOCK:
        _WRAP_URL_MAP[token] = (url, exp)
    return token

def _wrap_url_load(token: str) -> Optional[str]:
    now = time.time()
    with _WRAP_URL_LOCK:
        val = _WRAP_URL_MAP.get(token)
        if not val:
            return None
        url, exp = val
        if exp and now > exp:
            _WRAP_URL_MAP.pop(token, None)
            return None
        return url

def wrap_playback_url(url: str) -> str:
    """Wrap outbound http(s) URLs behind our HEAD-friendly redirector.

    For Android/Google TV Stremio, keep the stream URL short and HEAD-safe.

    - If WRAP_URL_SHORT=true, we emit a restart-safe compressed token /r/z... (no in-memory dependency).
    - Otherwise, we fall back to base64-encoding the full URL (legacy).
    """
    if not url or not WRAP_PLAYBACK_URLS:
        return url
    u = str(url)
    # Avoid double-wrapping
    try:
        base = _public_base_url().rstrip('/')
        if u.startswith(base + '/r/'):
            return u
    except Exception:
        pass
    if u.startswith('http://') or u.startswith('https://'):
        if WRAP_URL_SHORT:
            # Restart-safe short token: compressed URL (no in-memory dependency)
            tok = _zurl_encode(u)
            wrapped = _public_base_url() + 'r/' + tok
        else:
            wrapped = _public_base_url() + 'r/' + _b64u_encode(u)
        if WRAP_DEBUG:
            logging.getLogger('aio-wrapper').info(f'WRAP_URL -> {wrapped}')
        return wrapped
    return u


app = Flask(__name__)


# --- fallback cache (serves last non-empty streams during upstream flakiness) ---
FALLBACK_CACHE_TTL = _safe_int(os.environ.get('FALLBACK_CACHE_TTL', '600'), 600)  # seconds
_LAST_GOOD_STREAMS = {}  # key -> list[dict]
_LAST_GOOD_TS = {}       # key -> float epoch
_CACHE_LOCK = threading.Lock()


def _cache_key(media_type: str, stremio_id: str) -> str:
    return f"{media_type}:{stremio_id}"


def cache_get(media_type: str, stremio_id: str = None):
    """Get last-good streams by either (media_type, stremio_id) or a single key 'type:id'."""
    if FALLBACK_CACHE_TTL <= 0:
        return None
    if stremio_id is None:
        k = str(media_type)
    else:
        k = _cache_key(media_type, stremio_id)
    now = time.time()
    with _CACHE_LOCK:
        ts = _LAST_GOOD_TS.get(k)
        if not ts or (now - ts) > FALLBACK_CACHE_TTL:
            return None
        return _LAST_GOOD_STREAMS.get(k)


def cache_set(media_type: str, stremio_id: str = None, streams=None):
    """Set last-good streams by either (media_type, stremio_id, streams) or (key, streams)."""
    if streams is None:
        # called as cache_set(key, streams)
        k = str(media_type)
        streams = stremio_id
    else:
        k = _cache_key(media_type, stremio_id)
    if FALLBACK_CACHE_TTL <= 0 or not streams:
        return
    with _CACHE_LOCK:
        _LAST_GOOD_STREAMS[k] = streams
        _LAST_GOOD_TS[k] = time.time()

app.config["JSON_AS_ASCII"] = False

# ---------------------------
# CORS (required for Stremio Web)
# ---------------------------
# Comma-separated list of allowed origins.
# IMPORTANT: If supports_credentials=True, the browser will reject "Access-Control-Allow-Origin: *".
# So we only enable credentials when origins are explicit.
ALLOWED_ORIGINS_ENV = os.getenv(
    "CORS_ORIGINS",
    "https://web.stremio.com,https://app.strem.io,https://staging.strem.io",
)
ALLOWED_ORIGINS = [o.strip() for o in ALLOWED_ORIGINS_ENV.split(",") if o.strip()]
WILDCARD_CORS = any(o == "*" for o in ALLOWED_ORIGINS)

if WILDCARD_CORS:
    CORS(
        app,
        resources={r"/*": {"origins": "*"}},
        supports_credentials=False,
        allow_headers=["Content-Type", "Authorization", "Origin", "Accept"],
        expose_headers=["Content-Length", "X-Content-Type-Options"],
    )
else:
    CORS(
        app,
        resources={r"/*": {"origins": ALLOWED_ORIGINS}},
        supports_credentials=True,
        allow_headers=["Content-Type", "Authorization", "Origin", "Accept"],
        expose_headers=["Content-Length", "X-Content-Type-Options"],
    )

# Add no-cache + Vary headers to all responses (prevents browser caching issues)
@app.after_request
def add_common_headers(response):
    # No caching for dynamic content like streams/manifest
    response.headers.setdefault("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
    response.headers.setdefault("Pragma", "no-cache")
    response.headers.setdefault("Expires", "0")

    # Vary: Origin is important when using specific origins (helps caching/CDN)
    response.headers["Vary"] = "Origin, Accept-Encoding"

    # Security headers
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")

    return response


@app.get("/")
def root():
    return "ok", 200



@app.route("/r/<path:token>", methods=["GET", "HEAD", "OPTIONS"])
def redirect_stream_url(token: str):
    """Redirector used to wrap playback URLs.

    Key goals for Google TV / Android Stremio:
    - Keep stream URLs SHORT (avoid URL-length limits) via WRAP_URL_SHORT + in-memory map.
    - Make HEAD validation succeed WITHOUT causing the client to HEAD the upstream.

    Behavior controlled by WRAP_HEAD_MODE:
      - 200_noloc (recommended): HEAD=200, no Location
      - 200_loc:               HEAD=200, include Location hint
      - 302:                   HEAD=302 redirect (not recommended)

    GET always returns 302 Location to the real upstream playback URL.
    """
    # Basic token validation (avoid abuse / pathological decode)
    if not token or len(token) > 4096 or re.fullmatch(r"[A-Za-z0-9_-]+=*", token) is None:
        return ("", 404)

    # Resolve token -> upstream URL
    url = None

    # 1) Backward-compat: old in-memory short tokens
    if WRAP_URL_SHORT:
        url = _wrap_url_load(token)

    # 2) Restart-safe compressed tokens (preferred)
    if not url and token.startswith("z"):
        try:
            url = _zurl_decode(token)
        except Exception:
            url = None

    # 3) Legacy: token may be base64 of the full URL
    if not url:
        try:
            url = _b64u_decode(token)
        except Exception:
            url = None
    if not url:
        return ("", 404)

    if request.method == "OPTIONS":
        resp = make_response("", 204)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET,HEAD,OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "*"
        resp.headers["Access-Control-Max-Age"] = "86400"
        return resp

    # Common headers
    def _base(resp: Response) -> Response:
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Content-Type"] = "application/octet-stream"
        resp.headers["Accept-Ranges"] = "bytes"
        resp.headers["Content-Length"] = "0"
        return resp

    if request.method == "HEAD":
        mode = (WRAP_HEAD_MODE or "200_noloc").lower()
        if mode == "302":
            resp = make_response("", 302)
            resp.headers["Location"] = url
        else:
            resp = make_response("", 200)
            # Do NOT set Location unless explicitly requested
            if mode == "200_loc":
                resp.headers["Location"] = url
            # Safe hint headers (do not trigger redirect logic)
            resp.headers["X-Head-Bypass"] = "1"
            resp.headers["X-Stream-Location"] = url
        resp = _base(resp)
    else:
        # GET: real playback
        resp = make_response("", 302)
        resp.headers["Location"] = url
        resp = _base(resp)

    # High-signal debug
    try:
        ua = request.headers.get("User-Agent", "")
        logger.info(
            "R_PROXY rid=%s method=%s ua_len=%d status=%s tok_len=%d short=%s",
            _rid(), request.method, len(ua or ""), resp.status_code, len(token or ""), bool(WRAP_URL_SHORT)
        )
    except Exception:
        pass
    return resp


def _log_level(v: str) -> int:
    return {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }.get((v or "INFO").upper(), logging.INFO)

class SafeFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'rid'):
            record.rid = '-'
        return super().format(record)

class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if has_request_context() and hasattr(g, "request_id"):
            record.rid = g.request_id
        else:
            record.rid = "GLOBAL"
        return True

LOG_LEVEL = _log_level(os.environ.get("LOG_LEVEL", "INFO"))
handler = logging.StreamHandler()
handler.setFormatter(SafeFormatter("%(asctime)s | %(levelname)s | %(rid)s | %(message)s"))
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(LOG_LEVEL)
logging.getLogger().addFilter(RequestIdFilter())
logger = logging.getLogger("aio-wrapper")

@app.before_request
def _before_request() -> None:
    g.request_id = str(uuid.uuid4())[:8]
    rl = _enforce_rate_limit()
    if rl:
        body, code = rl
        return jsonify(body), code

def _rid() -> str:
    return g.request_id if has_request_context() and hasattr(g, "request_id") else "GLOBAL"

def is_android_client() -> bool:
    """Best-effort detection of Android/Google TV Stremio clients.

    IMPORTANT: Some Android TV / Google TV Stremio builds send an empty User-Agent
    (it shows up as '-' in some access logs). Treat empty UA as Android/TV so
    we apply Android/TV-friendly time budgets and avoid client-side timeouts.
    """
    ua = request.headers.get("User-Agent", "") if has_request_context() else ""
    ua_l = (ua or "").strip().lower()

    # Key fix: Google TV clients may omit UA entirely.
    if not ua_l:
        return True

    return (
        ('android' in ua_l)
        or ('okhttp' in ua_l)
        or ('exoplayer' in ua_l)
        or ('stremio' in ua_l and 'tv' in ua_l)
    )


# ---------------------------
# HTTP session (retries)
# ---------------------------
session = requests.Session()

# In-process cache history for expensive cache checks (per infohash)
CACHED_HISTORY: dict[str, bool] = {}

CACHED_HISTORY_LOCK = threading.Lock()
retry = Retry(total=3, backoff_factor=0.6, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET", "POST"])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

# No-retry session: used for /stream fetches so a slow upstream doesn't balloon into 30s+
fast_session = requests.Session()
fast_retry = Retry(total=0)
fast_adapter = HTTPAdapter(max_retries=fast_retry)
fast_session.mount("http://", fast_adapter)
fast_session.mount("https://", fast_adapter)

# ---------------------------
# Simple in-process rate limiting (no extra deps)
# ---------------------------
_rate_buckets: dict[str, deque] = defaultdict(deque)
_RATE_LOCK = threading.Lock()


# ---------------------------
# Android/TV strict-client schema hardening
# ---------------------------
def _strip_control_chars(s: str) -> str:
    # Remove ASCII control characters that can break some Android JSON consumers.
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)


def android_sanitize_out_stream(stream: Any) -> Optional[Dict[str, Any]]:
    """Harden a single Stremio stream object for strict Android/TV clients.

    Returns a cleaned dict or None (drop).
    Rules:
      - Must be a dict with non-empty string 'url'
      - 'name' must be string if present; if absent, we synthesize a minimal name
      - 'description' must be string if present (or removed)
      - 'behaviorHints' must be dict if present; drop invalid
      - Remove any None values, strip control chars, cap string lengths.
    """
    if not isinstance(stream, dict):
        return None

    # If a stream object accidentally contains a nested 'streams' key, drop it (schema poison).
    if "streams" in stream:
        return None

    url = stream.get("url")
    if not isinstance(url, str) or not url.strip():
        return None
    url = url.strip()
    # Android parsers can choke on extremely long URLs; cap hard.
    if len(url) > 4096:
        return None

    out: Dict[str, Any] = {}

    # url is required
    out["url"] = url

    # name (optional but strongly recommended for UI)
    name = stream.get("name")
    if name is None:
        # best-effort fallback: keep very small; don't rely on other fields
        name = "Stream"
    if not isinstance(name, str):
        name = str(name)
    name = _strip_control_chars(name).strip()
    if not name:
        name = "Stream"
    if len(name) > MAX_TITLE_CHARS:
        name = name[:MAX_TITLE_CHARS].rstrip()
    out["name"] = name

    # description (optional)
    desc = stream.get("description")
    if desc is not None:
        if not isinstance(desc, str):
            desc = str(desc)
        desc = _strip_control_chars(desc).strip()
        if desc:
            if len(desc) > MAX_DESC_CHARS:
                desc = desc[:MAX_DESC_CHARS].rstrip()
            out["description"] = desc

    # behaviorHints (optional)
    bh = stream.get("behaviorHints")
    if bh is not None:
        if isinstance(bh, dict):
            # sanitize nested strings + remove None
            bh_out: Dict[str, Any] = {}
            for k, v in bh.items():
                if v is None:
                    continue
                if isinstance(v, str):
                    v2 = _strip_control_chars(v).strip()
                    if not v2:
                        continue
                    # filename can be large; cap.
                    if k == "filename" and len(v2) > 255:
                        v2 = v2[:255].rstrip()
                    bh_out[k] = v2
                else:
                    # allow booleans/ints/etc as-is
                    bh_out[k] = v
            if bh_out:
                out["behaviorHints"] = bh_out
        else:
            # invalid behaviorHints is poison; drop it rather than poisoning the whole stream
            pass

    # Copy through a small allowlist of common Stremio keys safely.
    # (Avoid copying arbitrary nested objects that can break strict parsers.)
    for k in ("title", "infoHash", "availability", "seeders"):
        v = stream.get(k)
        if v is None:
            continue
        if isinstance(v, str):
            v2 = _strip_control_chars(v).strip()
            if not v2:
                continue
            if len(v2) > 512:
                v2 = v2[:512].rstrip()
            out[k] = v2
        elif isinstance(v, (int, float, bool)):
            out[k] = v

    # Ensure no None values anywhere
    out = {k: v for k, v in out.items() if v is not None}
    return out


def android_sanitize_stream_list(streams: Any) -> List[Dict[str, Any]]:
    if not isinstance(streams, list):
        return []
    cleaned: List[Dict[str, Any]] = []
    for s in streams:
        cs = android_sanitize_out_stream(s)
        if isinstance(cs, dict) and cs.get("url"):
            cleaned.append(cs)
    return cleaned

def _parse_rate_limit(limit: str) -> tuple[int, int]:
    s = (limit or '').strip().lower()
    m = re.match(r'^(\d+)\s*/\s*(second|sec|minute|min|hour|hr)$', s)
    if not m:
        raise ValueError(s)
    n = int(m.group(1))
    unit = m.group(2)
    window = 1 if unit in ('second', 'sec') else 60 if unit in ('minute', 'min') else 3600
    return n, window

def _enforce_rate_limit() -> Optional[tuple[Dict[str, Any], int]]:
    if not RATE_LIMIT:
        return None
    try:
        max_req, window = _parse_rate_limit(RATE_LIMIT)
    except Exception:
        return None
    ip = (request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or request.remote_addr or 'unknown')
    now = time.time()
    with _RATE_LOCK:
        q = _rate_buckets[ip]
        while q and (now - q[0]) > window:
            q.popleft()
        if len(q) >= max_req:
            return ({'streams': []}, 429)
        q.append(now)
    return None



def _extract_age_days(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.lower()
    m = re.search(r'\b(\d{1,4})\s*d\b', t)
    if m:
        return int(m.group(1))
    m = re.search(r'\b(\d{1,4})\s*days?\b', t)
    if m:
        return int(m.group(1))
    return None

def _looks_instant(text: str) -> bool:
    text = text.lower()
    # High size = likely cached
    if re.search(r'size\s*(?:[4-9]\d|1[0-2]\d)\s*gb', text):
        return True
    # Premium quality tags
    if any(kw in text for kw in ['remux', 'bluray', 'uhd', 'truehd', 'atmos', 'ddp5.1', 'ddp7.1', 'hdr', 'dv']):
        return True
    # Good groups (expand as needed)
    if any(grp in text for grp in ['diyhdhome', 'aoc', 'tmt', 'surcode', 'bhysourbits']):
        return True
    return False

def _cache_confidence(s: Dict[str, Any], meta: Dict[str, Any]) -> float:
    try:
        text = f"{s.get('name','')} {s.get('description','')} {meta.get('title_raw','')}"
        size_gb = float(meta.get('size', 0)) / (1024 ** 3) if meta.get('size') else 0.0
        age = _extract_age_days(text)  # None if unknown
        instant = _looks_instant(text)
        score = 0.0
        if size_gb >= 2.0:
            score += 0.4
        if age is not None and age <= 30:
            score += 0.3
        if instant:
            score += 0.3
        return min(1.0, max(0.0, score))
    except Exception:
        return 0.0

def _heuristic_cached(s: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    return _cache_confidence(s, meta) >= MIN_CACHE_CONFIDENCE

# ---------------------------
# Pollution detection (optional; v20-style)
# ---------------------------
def is_polluted(s: Dict[str, Any], type_: str, season: Optional[int], episode: Optional[int]) -> bool:
    name = (s.get("name") or "").lower()
    desc = (s.get("description") or "").lower()
    filename = s.get("behaviorHints", {}).get("filename", "").lower()
    text = f"{name} {desc} {filename}"
    # Series pollution in movie
    if type_ == "movie" and re.search(r"\bs\d{1,2}e\d{1,2}\b|\bepisode\b|\bseason\b", text):
        return True
    # Movie pollution in series
    if type_ == "series":
        if season is not None and episode is not None:
            ep_pattern = rf"\bs0?{season}e0?{episode}\b"
            if not re.search(ep_pattern, text):
                return True
    return False

# ---------------------------
# Classification contract (meta)
# ---------------------------
_RES_RE = re.compile(r"\b(2160p|4k|1080p|720p|480p|sd)\b", re.I)
_SIZE_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(gb|mb)\b", re.I)
_SOURCE_TOKS = ["REMUX", "BLURAY", "BLU-RAY", "BD", "WEB-DL", "WEBDL", "WEBRIP", "HDTV", "DVDRIP", "CAM", "TS", "USENET", "NZB"]
_CODEC_TOKS = ["AV1", "X264", "H264", "X265", "H265", "HEVC"]
_AUDIO_TOKS = ["DDP", "DD+", "DD", "EAC3", "TRUEHD", "DTS-HD", "DTS", "AAC", "AC3", "FLAC", "ATMOS"]
_LANG_TOKS = ["ENG", "ENGLISH", "SPANISH", "FRENCH", "GERMAN", "ITALIAN", "KOREAN", "JAPANESE", "CHINESE", "MULTI", "VOSTFR", "SUBBED"]
_GROUP_RE = re.compile(r"[-.]([a-z0-9]+)$", re.I)
_HASH_RE = re.compile(r"btih:([0-9a-fA-F]{40})|([0-9a-fA-F]{40})", re.I)

def classify(s: Dict[str, Any]) -> Dict[str, Any]:
    name = s.get("name", "").lower()
    desc = s.get("description", "").lower()
    filename = s.get("behaviorHints", {}).get("filename", "").lower()
    text = f"{name} {desc} {filename}"
    # Provider (prefer formatter-injected shortName tokens; keep word boundaries to avoid HDR->RD)
    provider = "UNK"
    m_sn = re.search(r"\b(TB|TORBOX|RD|REAL[- ]?DEBRID|REALDEBRID|PM|PREMIUMIZE|AD|ALLDEBRID|DL|DEBRID[- ]?LINK|ND|NZBDAV|EW|EWEKA|NG|NZGEEK)\b", text, re.I)
    if m_sn:
        tok = m_sn.group(1).upper().replace(' ', '').replace('-', '')
        provider_map = {
            'TB': 'TB', 'TORBOX': 'TB',
            'RD': 'RD', 'REALDEBRID': 'RD',
            'PM': 'PM', 'PREMIUMIZE': 'PM',
            'AD': 'AD', 'ALLDEBRID': 'AD',
            'DL': 'DL', 'DEBRIDLINK': 'DL',
            'ND': 'ND', 'NZBDAV': 'ND',
            'EW': 'EW', 'EWEKA': 'EW',
            'NG': 'NG', 'NZGEEK': 'NG',
        }
        provider = provider_map.get(tok, provider)

    # Conservative fallback (keeps compatibility when formatter is missing)
    if provider == "UNK":
        if re.search(r"\breal[- ]?debrid\b", text):
            provider = "RD"
        elif re.search(r"\brd\b", text):
            provider = "RD"
        elif re.search(r"\btorbox\b", text):
            provider = "TB"
        elif re.search(r"\btb\b", text):
            provider = "TB"
        elif re.search(r"\ball[- ]?debrid\b", text):
            provider = "AD"
        elif re.search(r"\bad\b", text):
            provider = "AD"
        elif re.search(r"\bpremiumize\b", text):
            provider = "PM"
        elif re.search(r"\bpm\b", text):
            provider = "PM"
        elif re.search(r"\bnzbdav\b", text) or re.search(r"\bnzb\b", text):
            provider = "ND"
        elif re.search(r"\beweka\b", text):
            provider = "EW"
        elif re.search(r"\bnzgeek\b", text):
            provider = "NG"

    # Resolution
    m = _RES_RE.search(text)
    res = (m.group(1).upper() if m else "SD")
    if res == "4K":
        res = "2160P"
    # Source
    source = ""
    for tok in _SOURCE_TOKS:
        if tok.lower() in text:
            source = tok.upper()
            break
    # Codec
    codec = ""
    for tok in _CODEC_TOKS:
        if tok.lower() in text:
            codec = tok.upper()
            break
    # Audio
    audio = ""
    for tok in _AUDIO_TOKS:
        if tok.lower() in text:
            audio = tok.upper()
            break
    # Language
    language = "EN"
    for tok in _LANG_TOKS:
        if tok.lower() in text:
            language = tok.upper()
            break
    language = LANG_MAP.get(language, language)  # Normalize
    # Size
    size = int(s.get("behaviorHints", {}).get("videoSize", 0))
    if not size:
        m = _SIZE_RE.search(text)
        if m:
            val = float(m.group(1))
            unit = m.group(2).upper()
            size = int(val * (1024 ** 3 if unit == "GB" else 1024 ** 2))
    # Seeders
    m_seeds = re.search(r"(\\d+)\\s*(?:seeds?|seeders?)\\b", text, re.I)
    seeders = int(m_seeds.group(1)) if m_seeds else 0
    # Infohash
    url = s.get("url", "") or s.get("externalUrl", "")
    infohash = ""
    hash_src = ""

    ih_field = s.get("infoHash")
    if isinstance(ih_field, str) and re.fullmatch(r"[0-9a-fA-F]{40}", (ih_field or '').strip()):
        infohash = ih_field.strip().lower()
        hash_src = "field"

    if not infohash and url:
        hm = _HASH_RE.search(url)
        if hm:
            infohash = (hm.group(1) or hm.group(2)).lower()
            hash_src = "url"

    # Fallback: formatter-injected "IH:<hash>" (or similar labels) in text/description/name/filename
    if not infohash:
        cand = f"{text} {s.get('description','')} {s.get('name','')} {s.get('behaviorHints',{}).get('filename','')}"
        ih = _extract_infohash(cand)
        if ih:
            infohash = ih
            hash_src = "desc"

    # Optional Usenet identifier (only used if TB_USENET_CHECK=true).
    bh = s.get("behaviorHints", {}) or {}
    usenet_hash = ""
    for k in ("usenet_hash", "usenetHash", "nzb_hash", "nzbHash", "nzb_id", "guid"):
        v = bh.get(k)
        if isinstance(v, str) and v.strip():
            usenet_hash = v.strip().lower()
            break
    # Fallback: extract a Usenet identifier from the playback URL if present (e.g. TorBox hash/guid)
    if not usenet_hash and url:
        m2 = re.search(r'\"hash\"\s*:\s*\"([0-9a-fA-F]{8,64})\"', url)
        if not m2:
            m2 = re.search(r'/download/\d+/([0-9a-fA-F]{8,64})', url)
        if m2:
            usenet_hash = m2.group(1).lower()
    # Group
    gm = _GROUP_RE.search(filename)
    group = gm.group(1).upper() if gm else ""
    # Raw title for fallback
    title_raw = normalize_display_title(filename or desc or name)
    # Premium level
    premium_level = 1 if is_premium_plan(provider) else 0
    return {
        "provider": provider,
        "res": res,
        "source": source,
        "codec": codec,
        "audio": audio,
        "language": language,
        "size": size,
        "seeders": seeders,
        "infohash": infohash,
        "hash_src": hash_src,
        "usenet_hash": usenet_hash,
        "group": group,
        "title_raw": title_raw,
        "premium_level": premium_level,
    }

# Helper for provider keys (customize as per env)
def api_key_for_provider(provider: str) -> str:
    provider = (provider or "").upper()
    if provider == "TB":
        return os.environ.get("TB_API_KEY", "")
    # No RD/AD keys needed now
    return ""

# ---------------------------
# Expected metadata (real TMDB fetch)
# ---------------------------
@lru_cache(maxsize=2000)
def get_expected_metadata(type_: str, id_: str) -> Dict[str, Any]:
    if not TMDB_API_KEY:
        return {"title": "", "year": None, "type": type_}
    id_clean = id_.split(":")[0] if ":" in id_ else id_
    base = f"https://api.themoviedb.org/3/{'movie' if type_ == 'movie' else 'tv'}/{id_clean}"
    try:
        resp = session.get(f"{base}?api_key={TMDB_API_KEY}&language=en-US", timeout=TMDB_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        title = data.get("title") or data.get("name", "")
        release_date = data.get("release_date") or data.get("first_air_date", "")
        year = int(release_date[:4]) if release_date else None
        return {"title": title, "year": year, "type": type_}
    except Exception as e:
        logger.warning(f"TMDB fetch failed: {e}")
        return {"title": "", "year": None, "type": type_}

# ---------------------------
# Formatting: guaranteed 2-left + 3-right (title + 2 lines)
# ---------------------------
def format_stream_inplace(
    s: Dict[str, Any],
    m: Dict[str, Any],
    expected: Dict[str, Any],
    cached_hint: str = "",
    type_: str = "",
    season: Optional[int] = None,
    episode: Optional[int] = None,
) -> None:
    # Clean, modern formatting: sparse emojis + • separators + optional 2-line description.
    prov = (m.get('provider') or 'UNK').upper().strip()
    res = (m.get('res') or 'SD').upper().strip()
    codec = (m.get('codec') or '').upper().strip()
    source = (m.get('source') or '').upper().strip()
    group = (m.get('group') or '').strip()
    lang = (m.get('language') or 'EN').upper().strip()
    audio = (m.get('audio') or '').upper().strip()
    size_str = _human_size_bytes(m.get('size', 0))
    seeders = int(m.get('seeders') or 0)

    # Emoji mapping (kept intentionally sparse)
    if PRETTY_EMOJIS:
        prov_emoji = {
            'TB': '♻️',
            'RD': '🔴',
            'AD': '🔵',
            'ND': '📰',
            'EW': '⚡',
            'NG': '🔍',
        }.get(prov, '📺')
        cache_emoji = '✅' if (cached_hint and cached_hint.upper() == 'CACHED') else ('⭐' if (cached_hint and cached_hint.upper() == 'LIKELY') else '')
    else:
        prov_emoji = ''
        cache_emoji = ''

    # Name (left): provider + core tech summary
    tech_parts = [res]
    if 'HDR' in (m.get('flags') or '').upper() or 'hdr' in f"{s.get('name','')} {s.get('description','')}".lower():
        tech_parts.append('HDR')
    if audio:
        tech_parts.append(audio)
    # Keep name short and readable in Stremio list
    name_txt = f"{prov_emoji} {prov} • {' '.join([p for p in tech_parts if p])} {cache_emoji}".strip()
    s['name'] = _truncate(name_txt if NAME_SINGLE_LINE else name_txt.replace(' • ', '\n', 1), MAX_TITLE_CHARS)

    # Title: expected title from TMDB when available, else parsed/raw title
    base_title = expected.get('title') or m.get('title_raw') or 'Unknown'
    base_title = normalize_display_title(base_title)
    year = expected.get('year')
    ep_tag = f" S{season:02d}E{episode:02d}" if season and episode else ""
    s['title'] = _truncate(f"{base_title}{ep_tag}" + (f" ({year})" if year else ""), MAX_TITLE_CHARS)

    # Description: 2 clean lines (or single line if NAME_SINGLE_LINE)
    line1_bits = [p for p in [audio, lang, codec, source] if p]
    line1 = ' • '.join(line1_bits) if line1_bits else res
    seeds_str = f"{seeders} Seeds" if seeders > 0 else "Seeds Unknown"

    # Ensure machine-visible hints are present for downstream (Stremio UI + other tools)
    bh = s.setdefault('behaviorHints', {})
    ih = (m.get('infohash') or '').strip().lower()
    if ih and not s.get('infoHash'):
        s['infoHash'] = ih
    elif USENET_PSEUDO_INFOHASH and not s.get('infoHash'):
        uh = (m.get('usenet_hash') or '').strip().lower()
        pseudo = _pseudo_infohash_usenet(uh) if uh else ''
        if pseudo:
            s['infoHash'] = pseudo
            bh['usenetHash'] = uh


    bh['provider'] = prov
    # Cached: booleans are API-truth for torrents; 'LIKELY' is heuristic for hashless/usenet.
    if m.get('infohash'):
        if isinstance(m.get('cached'), bool):
            bh['cached'] = m.get('cached')
    else:
        if cached_hint == 'LIKELY':
            bh['cached'] = 'LIKELY'
    if m.get('source') and 'source' not in bh:
        bh['source'] = m.get('source')
    line2_bits = [p for p in [size_str, seeds_str, (f"Grp {group}" if group else '')] if p]
    line2 = ' • '.join(line2_bits)
    if cached_hint:
        line1 = f"{cached_hint} • {line1}".strip(' •')

    desc = _truncate(line1, MAX_DESC_CHARS) + "\n" + _truncate(line2, MAX_DESC_CHARS)
    if NAME_SINGLE_LINE:
        desc = desc.replace("\n", " • ")
    s['description'] = desc

# ---------------------------
# Fetch streams
# ---------------------------
def _quality_label(res: str) -> tuple[str, str]:
    r = (res or "").upper().strip()
    if r in ("4K", "2160", "2160P", "2160P+", "UHD"):
        return "🔥", "4K UHD"
    if r in ("1440P", "1440"):
        return "💎", "1440p"
    if r in ("1080P", "1080"):
        return "✨", "1080p"
    if r in ("720P", "720"):
        return "📺", "720p"
    if r in ("480P", "480"):
        return "📼", "480p"
    return "📼", "SD"

def _svc_label_and_dot(provider: str) -> tuple[str, str]:
    p = (provider or "UNK").upper().strip()
    # Service supplier label (what you pay for): TB/RD/AD; usenet collapses to NZB
    if p in ("TB", "TORBOX"):
        return "TB", "🟢"
    if p in ("RD", "REALDEBRID"):
        return "RD", "🔴"
    if p in ("AD", "ALLDEBRID"):
        return "AD", "🟡"
    if p in ("PM", "PREMIUMIZE"):
        return "PM", "🟠"
    if p in ("DL", "DEBRIDLINK"):
        return "DL", "🟤"
    if p in ("ND", "NZBDAV", "EW", "EWEKA", "NG", "NZGEEK"):
        return "NZB", "🟣"
    return p, "⚪"

def _extract_addon_label(raw_name: str, res: str = "") -> str:
    n = (raw_name or "").strip()
    if not n:
        return ""
    # Strip bracket tags like "[TB⚡]" and leading emoji noise
    n = re.sub(r"^\s*[^\w\[]*\s*", "", n)
    n = re.sub(r"\[[^\]]+\]\s*", "", n)
    # Drop trailing resolution token if present
    r = (res or "").upper().replace("P", "p")
    n = re.sub(r"\b(2160p|1080p|720p|480p|4k)\b", "", n, flags=re.I).strip()
    # Collapse extra spaces
    n = re.sub(r"\s{2,}", " ", n).strip()
    # Keep it short for left column
    if len(n) > 26:
        n = n[:25].rstrip() + "…"
    return n

def _extract_flag_emojis(text: str) -> list[str]:
    if not text:
        return []
    # Country flags are pairs of regional indicator symbols
    flags = re.findall(r"[\U0001F1E6-\U0001F1FF]{2}", text)
    out = []
    seen = set()
    for f in flags:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out

def _detect_tokens(text: str) -> Dict[str, Any]:
    t = (text or "").lower()
    # Video flags
    hdr10p = "hdr10+" if "hdr10+" in t else ""
    hdr10 = "hdr10" if ("hdr10" in t and "hdr10+" not in t) else ""
    dv = "DV" if ("dolby vision" in t or re.search(r"\bdv\b", t)) else ""
    hdr = "HDR" if ("hdr" in t) else ""
    if hdr10p:
        hdr = "HDR"  # keep HDR too
    bit10 = "10bit" if ("10bit" in t or "10-bit" in t) else ""
    remux = True if "remux" in t else False

    # Audio tokens
    audio = []
    for tok, nice in [
        ("atmos", "Atmos"),
        ("truehd", "TrueHD"),
        ("dts-hd", "DTS-HD"),
        ("dts hd", "DTS-HD"),
        ("dts", "DTS"),
        ("ddp", "DD+"),
        ("dd+", "DD+"),
        ("eac3", "DD+"),
        ("aac", "AAC"),
        ("ac3", "AC3"),
        ("flac", "FLAC"),
    ]:
        if tok in t and nice not in audio:
            audio.append(nice)

    ch = ""
    m = re.search(r"\b(7\.1|5\.1|2\.0)\b", t)
    if m:
        ch = m.group(1)

    # Languages: keep simple + pass through any flag emojis
    lang_words = []
    for w, nice in [
        ("multi", "Multi"),
        ("dual", "Dual Audio"),
        ("english", "English"),
        ("italian", "Italian"),
        ("french", "French"),
        ("spanish", "Spanish"),
        ("german", "German"),
        ("polish", "Polish"),
        ("russian", "Russian"),
        ("ukrain", "Ukrainian"),
        ("hindi", "Hindi"),
        ("japan", "Japanese"),
        ("korean", "Korean"),
        ("thai", "Thai"),
    ]:
        if w in t and nice not in lang_words:
            lang_words.append(nice)

    return {
        "hdr10p": hdr10p,
        "hdr10": hdr10,
        "dv": dv,
        "hdr": hdr,
        "bit10": bit10,
        "remux": remux,
        "audio": audio,
        "channels": ch,
        "lang_words": lang_words,
    }

def _fmt_gb(size_bytes: Any) -> str:
    try:
        b = int(size_bytes)
        if b <= 0:
            return ""
        gb = b / (1024 ** 3)
        if gb >= 1:
            return f"{gb:.2f} GB".rstrip('0').rstrip('.')
        mb = b / (1024 ** 2)
        return f"{mb:.0f} MB"
    except Exception:
        return ""

def build_stream_object_rich(
    raw_s: Dict[str, Any],
    m: Dict[str, Any],
    expected: Dict[str, Any],
    out_url: str,
    is_confirmed: bool,
    cached_hint: str,
    type_: str = "",
    season: Optional[int] = None,
    episode: Optional[int] = None,
) -> Dict[str, Any]:
    # Brand-new stream object for strict clients: only {name, description, url, behaviorHints}
    raw_name = raw_s.get("name", "") or ""
    raw_desc = raw_s.get("description", "") or ""
    raw_bh = raw_s.get("behaviorHints") or {}

    # Title (English) + year
    title = (expected or {}).get("title") or m.get("title_raw") or ""
    year = (expected or {}).get("year")
    if type_ == "series" and season and episode:
        se = f"S{int(season):02d}E{int(episode):02d}"
    else:
        se = ""
    title_line = title.strip()
    if year:
        title_line = f"{title_line} ({year})" if title_line else f"({year})"
    if se:
        title_line = f"{title_line} {se}".strip()

    # Left (2 lines): quality + supplier/provider
    q_emoji, q_label = _quality_label(m.get("res") or "")
    svc, dot = _svc_label_and_dot(m.get("provider") or "")
    addon = _extract_addon_label(raw_name, m.get("res") or "")

    cache_mark = "⚡" if (is_confirmed or (cached_hint or "").upper() == "LIKELY") else ""
    # Keep addon label on the left if present
    left2 = f"{dot}{svc}{cache_mark}" + (f" {addon}" if addon else "")
    left1 = f"{q_emoji}{q_label}"
    name = f"{left1}\n{left2}" if OUTPUT_LEFT_LINES >= 2 else left1

    # Right (3–4 lines): rich but schema-safe
    blob = f"{raw_name}\n{raw_desc}\n{raw_bh.get('filename','')}"
    tok = _detect_tokens(blob)
    flags = _extract_flag_emojis(blob)

    # Line 1: title
    parts = []
    if title_line:
        parts.append(f"🎬 {title_line}")
    # Line 2: source/codec/video flags
    src = (m.get("source") or "").upper().strip()
    if tok["remux"] and "REMUX" not in src:
        src = (src + " REMUX").strip()
    codec = (m.get("codec") or "").upper().strip()
    line2 = ""
    vflags = []
    if tok["hdr10p"]:
        vflags.append("HDR10+")
    if tok["hdr10"]:
        vflags.append("HDR10")
    if tok["dv"]:
        vflags.append("DV")
    if tok["hdr"]:
        # avoid duplicating HDR when only HDR10/HDR10+ present; keep as generic marker too
        if "HDR" not in vflags:
            vflags.append("HDR")
    if tok["bit10"]:
        vflags.append("10bit")
    vflag_txt = ("📺 " + " | ".join(vflags)) if vflags else ""
    if src or codec or vflag_txt:
        line2 = " ".join([x for x in [("🎥 " + src) if src else "", ("🎞️ " + codec) if codec else "", vflag_txt] if x]).strip()
        if line2:
            parts.append(line2)

    # Line 3: audio + langs
    audio_bits = []
    if tok["audio"]:
        audio_bits.append("🎧 " + " | ".join(tok["audio"]))
    if tok["channels"]:
        audio_bits.append("🔊 " + tok["channels"])
    lang_bits = []
    # Prefer explicit flags if present
    if flags:
        lang_bits.append(" / ".join(flags))
    elif tok["lang_words"]:
        lang_bits.append(" | ".join(tok["lang_words"]))
    if lang_bits:
        audio_bits.append("🗣️ " + " / ".join(lang_bits))
    if audio_bits:
        parts.append(" ".join(audio_bits).strip())

    # Line 4: size + seeders + group + readiness + addon/indexer
    size_txt = _fmt_gb(m.get("size"))
    seed = m.get("seeders") or 0
    group = (m.get("group") or "").strip()
    line4_parts = []
    if size_txt:
        line4_parts.append(f"📦 {size_txt}")
    if isinstance(seed, int) and seed > 0:
        line4_parts.append(f"👥 {seed}")
    if group:
        line4_parts.append(f"🏷️ {group}")

    ready_txt = ""
    if svc in ("TB", "RD", "AD", "PM", "DL", "NZB"):
        ready_txt = f"⚡Ready ({svc})" if (is_confirmed or (cached_hint or '').upper() == 'LIKELY') else f"⏳Unchecked ({svc})"
        line4_parts.append(ready_txt)

    prox = "⛊ Proxied" if WRAP_PLAYBACK_URLS else "🔓 Not Proxied"
    line4_parts.append(prox)

    # Try to surface the upstream addon/indexer name (Comet/Torrentio/StremThru/etc.)
    if addon:
        line4_parts.append(f"🔍 {addon}")

    if line4_parts:
        parts.append(" ".join(line4_parts).strip())

    # Keep 3–4 lines max (UI friendly)
    description = "\n".join(parts[:4]).strip()

    # behaviorHints: keep ONLY known-safe keys (strict clients)
    bh_out: Dict[str, Any] = {}
    def _copy_bh_key(k: str, types: tuple[type, ...]):
        v = raw_bh.get(k)
        if isinstance(v, types):
            bh_out[k] = v

    _copy_bh_key("filename", (str,))
    _copy_bh_key("videoSize", (int, float))
    _copy_bh_key("videoHash", (str,))
    _copy_bh_key("bingeGroup", (str,))
    # If subtitles is a list/dict, keep it (Stremio uses it sometimes)
    v_sub = raw_bh.get("subtitles")
    if isinstance(v_sub, (list, dict)):
        bh_out["subtitles"] = v_sub

    return {
        "name": name,
        "description": description,
        "url": out_url,
        "behaviorHints": bh_out,
    }
def _auth_headers(auth: str) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if not auth:
        return headers
    # Support Basic auth in the form user:pass
    if ':' in auth and not auth.lower().startswith('bearer '):
        user, pw = auth.split(':', 1)
        headers['Authorization'] = 'Basic ' + base64.b64encode(f'{user}:{pw}'.encode()).decode()
        return headers
    # Otherwise treat it as a ready-to-use Authorization value (e.g., 'Bearer ...')
    headers['Authorization'] = auth
    return headers

def _fetch_streams_from_base(base: str, auth: str, type_: str, id_: str, tag: str, timeout: float = REQUEST_TIMEOUT, no_retry: bool = False) -> List[Dict[str, Any]]:
    if not base:
        return []
    url = f"{base}/stream/{type_}/{id_}.json"
    headers = _auth_headers(auth)
    logger.info(f'{tag} fetch URL: {url}')
    try:
        sess = fast_session if no_retry else session
        resp = sess.get(url, headers=headers, timeout=timeout)
        logger.info(f'{tag} status={resp.status_code} bytes={len(resp.content)}')
        if resp.status_code != 200:
            return []
        data = resp.json() if resp.content else {}
        streams = (data.get('streams') or [])[:INPUT_CAP]
        # Lightweight tagging for debug (does not expose tokens)
        for s in streams:
            if isinstance(s, dict):
                s.setdefault('behaviorHints', {})
                s['behaviorHints'].setdefault('source', tag)
        return streams
    except json.JSONDecodeError as e:
        logger.error(f'{tag} JSON error: {e}; head={resp.text[:200] if "resp" in locals() else ""}')
        return []
    except Exception as e:
        logger.error(f'{tag} fetch error: {e}')
        return []


def get_streams_single(base: str, auth: str, type_: str, id_: str, tag: str, timeout: float, no_retry: bool = False) -> tuple[list[dict[str, Any]], int, int]:
    """Fetch a single provider and return (streams, count, ms)."""
    t0 = time.time()
    streams = _fetch_streams_from_base(base, auth, type_, id_, tag, timeout=timeout, no_retry=no_retry)
    ms = int((time.time() - t0) * 1000)
    return streams, len(streams), ms

def get_streams(
    type_: str,
    id_: str,
    *,
    is_android: bool = False,
    client_timeout_s: float | None = None,
) -> Tuple[List[Dict[str, Any]], int, int, int, int]:
    """Fetch provider streams in parallel (best-effort) and return merged results.

    Key behavior: after AIO returns, we only wait a *small grace period* for P2.
    This prevents slow P2 reads from causing client timeouts / Gunicorn aborts.
    """
    aio_streams: List[Dict[str, Any]] = []
    prov2_streams: List[Dict[str, Any]] = []
    aio_ms = 0
    p2_ms = 0

    total_timeout = float(
        client_timeout_s
        if client_timeout_s is not None
        else (ANDROID_STREAM_TIMEOUT if is_android else DESKTOP_STREAM_TIMEOUT)
    )
    aio_timeout = float(ANDROID_AIO_TIMEOUT if is_android else DESKTOP_AIO_TIMEOUT)
    p2_timeout = float(ANDROID_P2_TIMEOUT if is_android else DESKTOP_P2_TIMEOUT)
    logger.debug(
        "DEBUG_CONFIG rid=%s AIO_BASE=%s AIO_AUTH_set=%s PROV2_BASE=%s PROV2_AUTH_set=%s WRAP_URL_SHORT=%s",
        _rid(), AIO_BASE, bool(AIO_AUTH), PROV2_BASE, bool(PROV2_AUTH), bool(WRAP_URL_SHORT)
    )

    # After AIO completes, don't block long for P2.
    p2_grace = 1.5 if is_android else 4.0

    # Fire both fetches at once. Always use the no-retry session so read timeouts don't balloon.
    aio_in = 0
    p2_in = 0
    merged: List[Dict[str, Any]] = []

    deadline = time.monotonic() + max(0.1, total_timeout)

    with ThreadPoolExecutor(max_workers=2) as ex:
        t_aio0 = time.time()
        aio_fut = ex.submit(
            _fetch_streams_from_base,
            AIO_BASE,
            AIO_AUTH,
            type_,
            id_,
            "AIO",
            timeout=aio_timeout,
            no_retry=True,
        )

        p2_fut = None
        t_p20 = None
        if PROV2_BASE:
            t_p20 = time.time()
            p2_fut = ex.submit(
                _fetch_streams_from_base,
                PROV2_BASE,
                PROV2_AUTH,
                type_,
                id_,
                "P2",
                timeout=p2_timeout,
                no_retry=True,
            )

        # 1) Primary: AIO
        try:
            rem = max(0.05, deadline - time.monotonic())
            aio_streams = aio_fut.result(timeout=min(aio_timeout + 0.2, rem)) if aio_fut else []  # FIXED: get list only
            if not isinstance(aio_streams, list):
                aio_streams = []
            aio_in = len(aio_streams)  # FIXED: compute count from len
            aio_ms = int((time.time() - t_aio0) * 1000) if t_aio0 else 0
        except FuturesTimeoutError:
            aio_ms = int((time.time() - t_aio0) * 1000)
            logger.error("AIO fetch timeout")
        except Exception as e:
            aio_ms = int((time.time() - t_aio0) * 1000)
            logger.error(f"AIO fetch error: {e}")

        # 2) Secondary: P2 (best-effort)
        if p2_fut is not None and t_p20 is not None:
            try:
                rem = max(0.0, deadline - time.monotonic())
                if aio_streams:
                    # AIO gave us something; only wait a short grace period for P2.
                    wait_s = min(p2_grace, rem)
                else:
                    # If AIO failed/empty, give P2 more room (still bounded by deadline).
                    wait_s = min(p2_timeout + 0.2, rem)

                if wait_s > 0:
                    prov2_streams = p2_fut.result(timeout=wait_s) if p2_fut else []  # FIXED: get list only
                    if not isinstance(prov2_streams, list):
                        prov2_streams = []
                    p2_in = len(prov2_streams)  # FIXED: compute count from len
                    p2_ms = int((time.time() - t_p20) * 1000) if t_p20 else 0
            except FuturesTimeoutError:
                p2_ms = int((time.time() - t_p20) * 1000)
                logger.error("P2 fetch timeout")
            except Exception as e:
                p2_ms = int((time.time() - t_p20) * 1000)
                logger.error(f"P2 fetch error: {e}")

    aio_in = len(aio_streams)
    p2_in = len(prov2_streams)

    if aio_streams:
        merged.extend(aio_streams)
    if prov2_streams:
        merged.extend(prov2_streams)

    return merged, aio_in, p2_in, aio_ms, p2_ms



# ---------------------------
# Pipeline stats (required; used by /stream and filter_and_format)
# ---------------------------
@dataclass
class PipeStats:
    aio_in: int = 0
    prov2_in: int = 0
    merged_in: int = 0
    dropped_error: int = 0
    dropped_missing_url: int = 0
    dropped_pollution: int = 0
    dropped_low_seeders: int = 0
    dropped_lang: int = 0
    dropped_low_premium: int = 0
    dropped_rd: int = 0
    dropped_ad: int = 0
    dropped_low_res: int = 0
    dropped_old_age: int = 0
    dropped_blacklist: int = 0
    dropped_fakes_db: int = 0
    dropped_title_mismatch: int = 0
    dropped_dead_url: int = 0
    dropped_uncached: int = 0
    dropped_uncached_tb: int = 0
    dropped_android_magnets: int = 0
    deduped: int = 0
    delivered: int = 0

    # Timing/diagnostics (ms)
    ms_fetch_aio: int = 0
    ms_fetch_p2: int = 0
    ms_tmdb: int = 0
    ms_tb_api: int = 0
    ms_tb_webdav: int = 0
    ms_tb_usenet: int = 0

    # Hash counts (diagnostics)
    tb_api_hashes: int = 0
    tb_webdav_hashes: int = 0
    tb_usenet_hashes: int = 0


def hash_stats(pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]]):
    total = len(pairs)
    with_hash = 0
    uniq_hashes: set[str] = set()
    prov_total: Dict[str, int] = {}
    prov_with: Dict[str, int] = {}
    src_count: Dict[str, int] = {}

    for _s, m in pairs:
        prov = (m.get('provider') or 'UNK').upper()
        prov_total[prov] = prov_total.get(prov, 0) + 1

        h = (m.get('infohash') or '').lower().strip()
        if h:
            with_hash += 1
            uniq_hashes.add(h)
            prov_with[prov] = prov_with.get(prov, 0) + 1

        src = (m.get('hash_src') or '').strip().lower()
        if src:
            src_count[src] = src_count.get(src, 0) + 1

    return total, with_hash, len(uniq_hashes), prov_total, prov_with, src_count

def dedup_key(stream: Dict[str, Any], meta: Dict[str, Any]) -> str:
    """Stable dedup key.

    - Prefer infohash when present (universal across providers).
    - Fallback to normalized label when infohash is missing.
    """
    infohash = (meta.get('infohash') or '').lower().strip()
    res = (meta.get('res') or 'SD').upper()

    if infohash:
        return f"{infohash}:{res}"

    bh = stream.get('behaviorHints') or {}
    try:
        normalized_label = normalize_label(
            bh.get('filename') or bh.get('bingeGroup') or stream.get('name', '') or stream.get('description', '')
        )
    except Exception:
        normalized_label = ''

    return f"nohash:{normalized_label}:{res}"



def _drop_bad_top_n(
    candidates: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    top_n: int,
    timeout: float,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Drop obviously broken playback placeholders (e.g., redirects to /static/500.mp4) among top candidates."""
    if top_n <= 0 or not candidates:
        return candidates
    kept: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    i = 0
    while i < len(candidates):
        s, m = candidates[i]
        if len(kept) < top_n:
            try:
                ok = _verify_stream_url(s, timeout=timeout, range_mode=True)
                if not ok:
                    i += 1
                    continue
            except Exception:
                pass
        kept.append((s, m))
        i += 1
    return kept


def filter_and_format(type_: str, id_: str, streams: List[Dict[str, Any]], aio_in: int = 0, prov2_in: int = 0, is_android: bool = False, fast_mode: bool = False, deliver_cap: Optional[int] = None) -> Tuple[List[Dict[str, Any]], PipeStats]:
    stats = PipeStats()
    stats.aio_in = aio_in
    stats.prov2_in = prov2_in
    stats.merged_in = len(streams)

    deliver_cap_eff = int(deliver_cap or MAX_DELIVER or 60)

    cached_map: Dict[str, bool] = {}

    if fast_mode:
        expected = {}
        stats.ms_tmdb = 0
    else:
        t_tmdb0 = time.time()
        expected = get_expected_metadata(type_, id_)
        stats.ms_tmdb = int((time.time() - t_tmdb0) * 1000)

    # parse season/episode from id for series (tmdb:123:1:3 or tt..:1:3)
    season = episode = None
    if type_ == "series" and ":" in id_:
        parts = id_.split(":")
        if len(parts) >= 3:
            try:
                season = int(parts[-2])
                episode = int(parts[-1])
            except Exception:
                season = episode = None

    bad_hashes = _load_fakes_db() if USE_FAKES_DB else set()

    cleaned: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for s in streams:
        if not isinstance(s, dict):
            continue
        if not sanitize_stream_inplace(s):
            stats.dropped_missing_url += 1
            continue
        if (s.get("streamData") or {}).get("type") == "error":
            stats.dropped_error += 1
            continue

        m = classify(s)

        # Optional hard drops first
        if DROP_RD and m.get("provider") == "RD":
            stats.dropped_rd += 1
            continue
        if DROP_AD and m.get("provider") == "AD":
            stats.dropped_ad += 1
            continue

        if not VALIDATE_OFF:
            # Seeders
            if m.get('seeders', 0) < MIN_SEEDERS:
                stats.dropped_low_seeders += 1
                continue
            # Language
            if PREFERRED_LANG and m.get('language') != PREFERRED_LANG:
                stats.dropped_lang += 1
                continue
            # Pollution
            if DROP_POLLUTED and is_polluted(s, type_, season, episode):
                stats.dropped_pollution += 1
                continue
            # Premium plan (best-effort)
            if VERIFY_PREMIUM and m.get("premium_level", 0) == 0:
                stats.dropped_low_premium += 1
                continue

            # Resolution
            if MIN_RES > 0:
                if _res_to_int(m.get('res', '')) < MIN_RES:
                    stats.dropped_low_res += 1
                    continue

            # Age heuristic
            if USE_AGE_HEURISTIC and MAX_AGE_DAYS > 0:
                age = _extract_age_days((s.get('description') or '') + ' ' + (s.get('name') or ''))
                if age is not None and age > MAX_AGE_DAYS:
                    stats.dropped_old_age += 1
                    continue

            # Blacklists
            if USE_BLACKLISTS:
                text = f"{s.get('name','')} {s.get('description','')} {m.get('group','')}"
                if _is_blacklisted(text):
                    stats.dropped_blacklist += 1
                    continue

            # Fakes DB (infohash)
            if USE_FAKES_DB:
                h = (m.get('infohash') or '').lower()
                if h and h in bad_hashes:
                    stats.dropped_fakes_db += 1
                    continue

            # Optional URL verification (expensive)
            if (not fast_mode) and VERIFY_STREAM and not _verify_stream_url(s):
                stats.dropped_dead_url += 1
                continue

        cleaned.append((s, m))

    # Dedup
    out_pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    if WRAPPER_DEDUP:
        seen = set()
        for s, m in cleaned:
            k = dedup_key(s, m)
            if k in seen:
                stats.deduped += 1
                continue
            seen.add(k)
            out_pairs.append((s, m))
    else:
        out_pairs = cleaned[:]

    # Optional: title/year validation gate (local similarity; useful to drop obvious mismatches)
    # Uses parsed title from classify() when available (streams often have empty s['title']).
    if (not fast_mode) and (not VALIDATE_OFF) and (TRAKT_VALIDATE_TITLES or TRAKT_STRICT_YEAR):
        expected_title = (expected.get('title') or '').lower().strip()
        expected_year = expected.get('year')
        filtered_pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for s, m in out_pairs:
            # Prefer our parsed raw title; fall back to upstream 'title' then name/desc.
            cand_title = (m.get('title_raw') or s.get('title') or s.get('name') or '').lower().strip()

            if TRAKT_VALIDATE_TITLES and expected_title and cand_title:
                similarity = difflib.SequenceMatcher(None, cand_title, expected_title).ratio()
                if similarity < TRAKT_TITLE_MIN_RATIO:
                    stats.dropped_title_mismatch += 1
                    continue

            if TRAKT_STRICT_YEAR and expected_year:
                stream_year = _extract_year(s.get('name') or '') or _extract_year(s.get('description') or '')
                if stream_year and abs(int(stream_year) - int(expected_year)) > 1:
                    stats.dropped_title_mismatch += 1
                    continue

            filtered_pairs.append((s, m))

        out_pairs = filtered_pairs


    # Global hash visibility (per request)
    try:
        hs_total, hs_with, hs_uniq, hs_prov_total, hs_prov_with, hs_src = hash_stats(out_pairs)
        logger.info(
            "HASH_STATS rid=%s total=%d with_hash=%d uniq_hash=%d prov_total=%s prov_with_hash=%s hash_src=%s",
            _rid(), hs_total, hs_with, hs_uniq, hs_prov_total, hs_prov_with, hs_src
        )
    except Exception as _e:
        logger.debug(f"HASH_STATS_ERR rid={_rid()} err={_e}")

    # Sorting: enforce premium streams first, then resolution/seeders.
    # This makes the first MAX_DELIVER feel like a true "premium" view.

    def sort_key(pair: Tuple[Dict[str, Any], Dict[str, Any]]):
        s, m = pair
        prov = (m.get('provider', 'ZZ') or 'ZZ').upper()
        prov_idx = _provider_rank(prov)
        cached = m.get('cached')
        cached_val = 0 if cached is True else 1 if cached == 'LIKELY' else 2
        res = _res_to_int(m.get('res') or 'SD')
        seeders = int(m.get('seeders') or 0)
        return (prov_idx, cached_val, -res, -seeders)

    out_pairs.sort(key=sort_key)

    # Candidate window: a little bigger so we can satisfy usenet quotas.
    window = max(deliver_cap_eff, MIN_USENET_KEEP, MIN_USENET_DELIVER, 1)
    candidates = out_pairs[: min(len(out_pairs), window * 4, 500)]

    # Android/TV: remove streams that resolve to known error placeholders (e.g., /static/500.mp4)
    if is_android:
        try:
            candidates = _drop_bad_top_n(candidates, ANDROID_VERIFY_TOP_N, ANDROID_VERIFY_TIMEOUT)
        except Exception:
            pass

    # WebDAV strict (optional): drop TB items that WebDAV cannot confirm.
    # This can be used even without TB_API_KEY / TB_CACHE_HINTS.
    if (not fast_mode) and (not VALIDATE_OFF) and USE_TB_WEBDAV and TB_WEBDAV_USER and TB_WEBDAV_PASS and candidates and (TB_WEBDAV_STRICT or (not VERIFY_TB_CACHE_OFF)):
        tb_hashes: list[str] = []
        seen_tb: set[str] = set()
        for _s, _m in candidates:
            if (_m.get('provider') or '').upper() != 'TB':
                continue
            h = (_m.get('infohash') or '').lower()
            if not h or h in seen_tb:
                continue
            seen_tb.add(h)
            tb_hashes.append(h)
            if len(tb_hashes) >= TB_MAX_HASHES:
                break
        if tb_hashes:
            try:
                stats.tb_webdav_hashes = len(tb_hashes)
                t_wd0 = time.time()
                webdav_ok = tb_webdav_batch_check(tb_hashes)  # set of ok hashes
                stats.ms_tb_webdav = int((time.time() - t_wd0) * 1000)
            except _WebDavUnauthorized:
                # Credentials missing/wrong in environment. Do NOT drop TB results.
                webdav_ok = None
            except Exception:
                webdav_ok = None

            if webdav_ok is not None:
                before = len(candidates)
                def _keep_tb(p):
                    _s, _m = p
                    if (_m.get('provider') or '').upper() != 'TB':
                        return True
                    h = (_m.get('infohash') or '').lower()
                    return bool(h) and (h in webdav_ok)
                candidates = [p for p in candidates if _keep_tb(p)]
                stats.dropped_uncached_tb += before - len(candidates)


    # Optional: TorBox Usenet cache checks (only when we can extract a usenet identifier).
    usenet_cached_map: Dict[str, bool] = {}
    if (not fast_mode) and TB_USENET_CHECK and TB_API_KEY and candidates:
        uhashes: List[str] = []
        seen_u: set[str] = set()
        for _s, _m in candidates:
            uh = (_m.get('usenet_hash') or '').strip().lower()
            if uh and uh not in seen_u:
                seen_u.add(uh)
                uhashes.append(uh)
        stats.tb_usenet_hashes = len(uhashes)
        if uhashes:
            t_u0 = time.time()
            usenet_cached_map = tb_get_usenet_cached(uhashes)
            stats.ms_tb_usenet = int((time.time() - t_u0) * 1000)

    # Optional: cached/instant-only enforcement across providers.
    # - TB: requires TB_CACHE_HINTS + TB_API_KEY (otherwise we can't confirm)
    # - RD/AD: heuristic only (no API checks anymore)
    # Cache checks / premium validation
    cached_map: Dict[str, bool] = {}
    if (not fast_mode) and VERIFY_CACHED_ONLY and not VALIDATE_OFF:
        # TorBox API cached check (batched). Skip very small hash sets to avoid rate-limit/reset churn.
        if TB_CACHE_HINTS:
            hashes: List[str] = []
            seen_h: set = set()
            for _s, _m in candidates:
                if len(hashes) >= TB_MAX_HASHES:
                    break
                if (_m.get('provider') or '').upper() != 'TB':
                    continue
                h = (_m.get('infohash') or '').lower().strip()
                if h and h not in seen_h:
                    seen_h.add(h)
                    hashes.append(h)
            if len(hashes) >= TB_API_MIN_HASHES:
                t0 = time.time()
                cached_map = tb_get_cached(hashes)
                stats.ms_tb_api = int((time.time() - t0) * 1000)
                stats.tb_api_hashes = len(hashes)
            else:
                logger.info(f"TB_API_SKIP rid={_rid()} hashes={len(hashes)} <{TB_API_MIN_HASHES}")

        # Attach cached markers to meta; in loose mode we do NOT hard-drop.
        kept = []
        dropped_uncached = 0
        dropped_uncached_tb = 0
        for _s, _m in candidates:
            provider = (_m.get('provider') or '').upper()
            h = (_m.get('infohash') or '').lower().strip()
            bh = _s.get('behaviorHints') or {}
            text = (str(_s.get('name') or '') + ' ' + str(_s.get('description') or '') + ' ' + str(bh.get('filename') or '') + ' ' + str(bh.get('bingeGroup') or '')).lower()

            cached_marker = None
            if provider == 'TB':
                if h and cached_map:
                    cached_marker = bool(cached_map.get(h, False))
                elif h:
                    with CACHED_HISTORY_LOCK:
                        cached_marker = bool(CACHED_HISTORY.get(h, False))
                elif ASSUME_PREMIUM_ON_FAIL:
                    cached_marker = True
                else:
                    cached_marker = False
            elif provider in ('RD', 'AD'):
                cached_marker = bool(_heuristic_cached(_s, _m))
            elif provider in USENET_PRIORITY:
                cached_marker = 'LIKELY' if _looks_instant(text) else None
            else:
                cached_marker = None

            _m['cached'] = cached_marker
            if h and isinstance(cached_marker, bool):
                with CACHED_HISTORY_LOCK:
                    CACHED_HISTORY[h] = cached_marker

            if STRICT_PREMIUM_ONLY:
                if cached_marker is True or cached_marker == 'LIKELY':
                    kept.append((_s, _m))
                else:
                    dropped_uncached += 1
                    if provider == 'TB':
                        dropped_uncached_tb += 1
            else:
                kept.append((_s, _m))

        candidates = kept
        stats.dropped_uncached += dropped_uncached
        stats.dropped_uncached_tb += dropped_uncached_tb


    # Clarity log: tells you if TB checks actually ran and how many hashes were checked.
    logger.info(
        "TB_CHECKS rid=%s webdav_active=%s webdav_strict=%s api_active=%s api_hashes=%s webdav_hashes=%s",
        _rid(),
        bool(USE_TB_WEBDAV and TB_WEBDAV_USER),
        bool(TB_WEBDAV_STRICT),
        bool(TB_API_KEY and TB_CACHE_HINTS and (stats.tb_api_hashes or 0) > 0),
        int(stats.tb_api_hashes or 0),
        int(stats.tb_webdav_hashes or 0),
    )

    # Ensure we keep/deliver some usenet entries (if configured).
    def _is_usenet(pair):
        return pair[1].get('provider') in USENET_PRIORITY

    if MIN_USENET_KEEP or MIN_USENET_DELIVER:
        # KEEP: ensure at least MIN_USENET_KEEP in the pool
        if MIN_USENET_KEEP:
            have = sum(1 for p in candidates if _is_usenet(p))
            if have < MIN_USENET_KEEP:
                for p2 in out_pairs[len(candidates):]:
                    if _is_usenet(p2) and p2 not in candidates:
                        candidates.append(p2)
                        have += 1
                        if have >= MIN_USENET_KEEP:
                            break
        # DELIVER: ensure at least MIN_USENET_DELIVER within the first MAX_DELIVER
        if MIN_USENET_DELIVER:
            slice_ = candidates[:deliver_cap_eff]
            have = sum(1 for p in slice_ if _is_usenet(p))
            if have < MIN_USENET_DELIVER:
                extras = [p for p in candidates[deliver_cap_eff:] + out_pairs[len(candidates):] if _is_usenet(p) and p not in slice_]
                i = 0
                while have < MIN_USENET_DELIVER and i < len(extras):
                    for j in range(len(slice_) - 1, -1, -1):
                        if not _is_usenet(slice_[j]):
                            slice_[j] = extras[i]
                            i += 1
                            have += 1
                            break
                    else:
                        break
                candidates = slice_ + candidates[deliver_cap_eff:]
    # Android/Google TV clients can't handle magnet: links; drop them and backfill with direct URLs.
    if is_android:
        def _is_magnet(u: str) -> bool:
            return isinstance(u, str) and u.startswith("magnet:")
        magnets = sum(1 for s, _m in candidates if _is_magnet((s or {}).get("url", "")))
        if magnets:
            kept = []
            seen = set()
            for s, m in candidates:
                u = (s or {}).get("url", "")
                if _is_magnet(u):
                    continue
                key = (u, (s or {}).get("name") or "", (s or {}).get("title") or "")
                if key in seen:
                    continue
                seen.add(key)
                kept.append((s, m))
            if len(kept) < deliver_cap_eff:
                for s, m in out_pairs:
                    u = (s or {}).get("url", "")
                    if _is_magnet(u):
                        continue
                    key = (u, (s or {}).get("name") or "", (s or {}).get("title") or "")
                    if key in seen:
                        continue
                    seen.add(key)
                    kept.append((s, m))
                    if len(kept) >= deliver_cap_eff:
                        break
            stats.dropped_android_magnets += magnets
            logger.info(f"ANDROID_FILTER rid={_rid()} dropped_magnets={magnets}")
            candidates = kept

    # Format (last step)
    delivered: List[Dict[str, Any]] = []
    for s, m in candidates[:deliver_cap_eff]:
        h = (m.get("infohash") or "").lower().strip()
        cached_marker = m.get("cached")
        is_confirmed = (cached_marker is True) or (h and cached_map.get(h, False) is True)

        cached_hint = "CACHED" if is_confirmed else ""
        if ADD_CACHE_HINT and not cached_hint:
            if cached_marker == "LIKELY" or _looks_instant((s.get("name", "") or "") + " " + (s.get("description", "") or "")):
                cached_hint = "LIKELY"

        raw_url = s.get("url") or s.get("externalUrl") or ""
        out_url = raw_url
        if WRAP_PLAYBACK_URLS and isinstance(raw_url, str) and raw_url:
            out_url = wrap_playback_url(raw_url)

        if OUTPUT_NEW_OBJECT:
            delivered.append(
                build_stream_object_rich(
                    raw_s=s,
                    m=m,
                    expected=expected,
                    out_url=out_url,
                    is_confirmed=is_confirmed,
                    cached_hint=cached_hint,
                    type_=type_,
                    season=season,
                    episode=episode,
                )
            )
        else:
            # Legacy behavior (kept as a safety switch)
            bh = s.setdefault("behaviorHints", {})
            bh.setdefault("provider", (m.get("provider") or "UNK").upper())
            if m.get("infohash"):
                if isinstance(cached_marker, bool):
                    bh.setdefault("cached", cached_marker)
            elif cached_hint == "LIKELY":
                bh.setdefault("cached", "LIKELY")
            if REFORMAT_STREAMS:
                format_stream_inplace(s, m, expected, cached_hint, type_, season, episode)
            s["url"] = out_url
            delivered.append(s)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"DELIVERED_TOP5 rid={_rid()} count={len(delivered)}")
        for i, _s in enumerate(delivered[:5]):
            _bh = _s.get('behaviorHints') or {}
            logger.debug(f"  #{i} name='{_s.get('name')}' provider={_bh.get('provider','na')} cached={_bh.get('cached','na')} hash={'yes' if _s.get('infoHash') else 'no'} source={_bh.get('source','na')}")

    stats.delivered = len(delivered)
    return delivered, stats

# ---------------------------
# Endpoints
# ---------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "build": BUILD_ID, "ts": int(time.time())}), 200

@app.get("/manifest.json")
def manifest():
    return jsonify(
        {
            "id": "org.buubuu.aio.wrapper.merge",
            "version": "1.0.11",
            "name": "AIO Wrapper (Rich Output, 2 Lines Left) 9.0",
            "description": "Merges 2 providers and outputs a brand-new, strict-client-safe stream schema with rich AIOStreams-style emoji formatting (2-line left column).",
            "resources": ["stream"],
            "types": ["movie", "series"],
            "catalogs": [],
            "idPrefixes": ["tt", "tmdb"],
        }
    )

@app.get("/stream/<type_>/<id_>.json")
def stream(type_: str, id_: str):
    if not _is_valid_stream_id(type_, id_):
        return jsonify({"streams": []}), 400
    t0 = time.time()
    stats = PipeStats()
    is_android = is_android_client()

    try:
        streams, aio_in, prov2_in, ms_aio, ms_p2 = get_streams(
            type_,
            id_,
            is_android=is_android,
            client_timeout_s=(ANDROID_STREAM_TIMEOUT if is_android else DESKTOP_STREAM_TIMEOUT),
        )
        stats.aio_in = aio_in
        stats.prov2_in = prov2_in
        stats.ms_fetch_aio = ms_aio
        stats.ms_fetch_p2 = ms_p2
        out, stats = filter_and_format(type_, id_, streams, aio_in=aio_in, prov2_in=prov2_in, is_android=is_android)
        cache_key = f"{type_}:{id_}"
        if out:
            cache_set(cache_key, out)
        else:
            cached = cache_get(cache_key)
            if cached:
                out = cached
        tmp = [android_sanitize_out_stream(s) for s in out]
        tmp = [s for s in tmp if isinstance(s, dict) and s.get("url")]
        return jsonify({"streams": (tmp if is_android else out), "cacheMaxAge": int(CACHE_TTL)}), 200
    except Exception as e:
        logger.exception(f"Stream error: {e}")
        cached = cache_get(f"{type_}:{id_}")
        if cached:
            cached_out = (android_sanitize_stream_list(cached) if is_android else cached)
            return jsonify({"streams": cached_out, "cacheMaxAge": int(CACHE_TTL)}), 200
        return jsonify({"streams": [], "cacheMaxAge": int(CACHE_TTL)}), 200
    finally:
        logger.info(
            "WRAP_TIMING rid=%s type=%s id=%s aio_ms=%s p2_ms=%s tmdb_ms=%s tb_api_ms=%s tb_wd_ms=%s tb_api_hashes=%s tb_wd_hashes=%s",
            _rid(), type_, id_,
            stats.ms_fetch_aio, stats.ms_fetch_p2, stats.ms_tmdb, stats.ms_tb_api, stats.ms_tb_webdav,
            stats.tb_api_hashes, stats.tb_webdav_hashes,
        )
        logger.info(
            "WRAP_STATS rid=%s type=%s id=%s aio_in=%s prov2_in=%s merged_in=%s dropped_error=%s dropped_missing_url=%s dropped_pollution=%s dropped_low_seeders=%s dropped_lang=%s dropped_low_premium=%s dropped_rd=%s dropped_ad=%s dropped_low_res=%s dropped_old_age=%s dropped_blacklist=%s dropped_fakes_db=%s dropped_title_mismatch=%s dropped_dead_url=%s dropped_uncached=%s dropped_uncached_tb=%s deduped=%s delivered=%s ms=%s",
            _rid(), type_, id_,
            stats.aio_in, stats.prov2_in, stats.merged_in,
            stats.dropped_error, stats.dropped_missing_url, stats.dropped_pollution,
            stats.dropped_low_seeders, stats.dropped_lang,
            stats.dropped_low_premium,
            stats.dropped_rd, stats.dropped_ad, stats.dropped_low_res, stats.dropped_old_age,
            stats.dropped_blacklist, stats.dropped_fakes_db, stats.dropped_title_mismatch, stats.dropped_dead_url, stats.dropped_uncached, stats.dropped_uncached_tb,
            stats.deduped, stats.delivered,
            int((time.time() - t0) * 1000),
        )


@app.errorhandler(Exception)
def handle_unhandled_exception(e):
    # Pass through HTTP errors (404/405/etc) so they don't become fake 500s in logs.
    if isinstance(e, HTTPException):
        return e

    # Last-resort safety net so Stremio doesn't get HTML 500s (which break jq/tests).
    logger.exception("UNHANDLED %s %s: %s", request.method, request.path, e)
    if request.path.endswith('/manifest.json'):
        return jsonify(manifest()), 200
    if request.path.startswith('/stream/'):
        # Never fail hard for stream endpoints; empty list is better than a 500.
        cache_key = request.path.replace('/stream/','',1).replace('.json','',1).replace('/',':',1)
        cached = cache_get(cache_key)
        is_android = is_android_client()
        if cached:
            cached_out = (android_sanitize_stream_list(cached) if is_android else cached)
            return jsonify({'streams': cached_out, 'cacheMaxAge': int(CACHE_TTL)}), 200
        return jsonify({'streams': [], 'cacheMaxAge': int(CACHE_TTL)}), 200
    return ("Internal Server Error", 500)

if __name__ == "__main__":
    port = _safe_int(os.environ.get('PORT', '5000'), 5000)
    app.run(host="0.0.0.0", port=port, debug=LOG_LEVEL == logging.DEBUG, use_reloader=False)

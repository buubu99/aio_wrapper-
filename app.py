from __future__ import annotations
import base64
import json
import logging
import os
import re
import time
import unicodedata
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
import requests
from flask import Flask, jsonify, g, has_request_context, request
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
    raw = (raw or "").strip()
    # Accept either a base addon URL or a full manifest URL.
    # If the user pastes something like .../manifest.json/ (or with extra slashes), strip it.
    raw = raw.rstrip('/')
    idx = raw.find('/manifest.json')
    if idx != -1:
        raw = raw[:idx]
    return raw.rstrip('/')
# ---------------------------
# Config (keep env names compatible with your existing Render setup)
# ---------------------------
AIO_URL = os.environ.get("AIO_URL", "")
AIO_BASE = _normalize_base(os.environ.get("AIO_BASE", "") or AIO_URL)
# can be base url or .../manifest.json
AIOSTREAMS_AUTH = os.environ.get("AIOSTREAMS_AUTH", "") # "user:pass"

# Optional second provider (another AIOStreams-compatible addon)
PROV2_URL = os.environ.get("PROV2_URL", "")
PROV2_BASE = _normalize_base(os.environ.get("PROV2_BASE", "") or PROV2_URL)
PROV2_AUTH = os.environ.get("PROV2_AUTH", "")  # 'user:pass' for Basic auth if needed
PROV2_TAG = os.environ.get("PROV2_TAG", "P2")
INPUT_CAP = int(os.environ.get("INPUT_CAP", "4500"))
MAX_DELIVER = int(os.environ.get("MAX_DELIVER", "80"))
# Formatting / UI constraints
REFORMAT_STREAMS = _parse_bool(os.environ.get("REFORMAT_STREAMS", "true"), True)
FORCE_ASCII_TITLE = _parse_bool(os.environ.get("FORCE_ASCII_TITLE", "true"), True)
MAX_TITLE_CHARS = int(os.environ.get("MAX_TITLE_CHARS", "110"))
MAX_DESC_CHARS = int(os.environ.get("MAX_DESC_CHARS", "180"))
# Validation/testing toggles
VALIDATE_OFF = _parse_bool(os.environ.get("VALIDATE_OFF", "false"), False)  # pass-through for format testing
DROP_POLLUTED = _parse_bool(os.environ.get("DROP_POLLUTED", "false"), False)  # optional
# TorBox cache hint (optional; safe if unset)
TB_API_KEY = os.environ.get("TB_API_KEY", "")
TB_BASE = "https://api.torbox.app"
TB_BATCH_SIZE = int(os.environ.get("TB_BATCH_SIZE", "50"))
TB_MAX_HASHES = int(os.environ.get("TB_MAX_HASHES", "200"))  # limit hashes checked per request for speed
TB_CACHE_HINTS = _parse_bool(os.environ.get("TB_CACHE_HINTS", "false"), False)  # enable TorBox cache hint lookups
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "30"))
# TMDB for metadata
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
# Additional filters
MIN_SEEDERS = int(os.environ.get("MIN_SEEDERS", "0"))
PREFERRED_LANG = os.environ.get("PREFERRED_LANG", "EN").upper()
# Premium priorities and verification
PREMIUM_PRIORITY = os.environ.get("PREMIUM_PRIORITY", "RD,TB,AD").split(",")
USENET_PRIORITY = os.environ.get("USENET_PRIORITY", "ND,EW,NG").split(",")
VERIFY_PREMIUM = _parse_bool(os.environ.get("VERIFY_PREMIUM", "false"), False)
ASSUME_PREMIUM_ON_FAIL = _parse_bool(os.environ.get("ASSUME_PREMIUM_ON_FAIL", "false"), False)

# Optional: cached/instant validation for RD/AD (heuristics by default; strict workflow is opt-in)
VERIFY_CACHED_ONLY = _parse_bool(os.environ.get("VERIFY_CACHED_ONLY", "false"), False)
MIN_CACHE_CONFIDENCE = float(os.environ.get("MIN_CACHE_CONFIDENCE", "0.8"))
VALIDATE_CACHE_TIMEOUT = float(os.environ.get("VALIDATE_CACHE_TIMEOUT", "10"))

RD_STRICT_CACHE_CHECK = _parse_bool(os.environ.get("RD_STRICT_CACHE_CHECK", "false"), False)
RD_API_KEY = os.environ.get("RD_API_KEY", "")

AD_STRICT_CACHE_CHECK = _parse_bool(os.environ.get("AD_STRICT_CACHE_CHECK", "false"), False)
AD_API_KEY = os.environ.get("AD_API_KEY", "")

# Limit strict cache checks per request to avoid excessive API churn
STRICT_CACHE_MAX = int(os.environ.get("STRICT_CACHE_MAX", "18"))


# --- Add-ons / extensions (optional) ---
DEPRIORITIZE_RD = _parse_bool(os.environ.get("DEPRIORITIZE_RD", "false"), False)
DROP_RD = _parse_bool(os.environ.get("DROP_RD", "false"), False)

MIN_RES = int(os.environ.get("MIN_RES", "0"))  # e.g. 720, 1080, 2160
MAX_AGE_DAYS = int(os.environ.get("MAX_AGE_DAYS", "0"))  # 0 = off
USE_AGE_HEURISTIC = _parse_bool(os.environ.get("USE_AGE_HEURISTIC", "false"), False)

ADD_CACHE_HINT = _parse_bool(os.environ.get("ADD_CACHE_HINT", "false"), False)

# TorBox strict cache filtering (different from TB_CACHE_HINTS which only adds a hint)
TORBOX_CACHE_CHECK = _parse_bool(os.environ.get("TORBOX_CACHE_CHECK", "false"), False)
VERIFY_TB_CACHE_OFF = _parse_bool(os.environ.get("VERIFY_TB_CACHE_OFF", "false"), False)

# Wrapper behavior toggles
WRAPPER_DEDUP = _parse_bool(os.environ.get("WRAPPER_DEDUP", "true"), True)

VERIFY_STREAM = _parse_bool(os.environ.get("VERIFY_STREAM", "false"), False)
VERIFY_STREAM_TIMEOUT = float(os.environ.get("VERIFY_STREAM_TIMEOUT", "4"))

# Force a minimum share of usenet results (if they exist)
MIN_USENET_KEEP = int(os.environ.get("MIN_USENET_KEEP", "0"))
MIN_USENET_DELIVER = int(os.environ.get("MIN_USENET_DELIVER", "0"))

# Optional local/remote filtering sources
USE_BLACKLISTS = _parse_bool(os.environ.get("USE_BLACKLISTS", "false"), False)
BLACKLIST_URLS = [u.strip() for u in (os.environ.get("BLACKLIST_URLS", "") or "").split(",") if u.strip()]
BLACKLIST_TERMS = [t.strip().lower() for t in (os.environ.get("BLACKLIST_TERMS", "") or "").split(",") if t.strip()]
USE_FAKES_DB = _parse_bool(os.environ.get("USE_FAKES_DB", "false"), False)
FAKES_DB_URL = os.environ.get("FAKES_DB_URL", "")
USE_SIZE_MISMATCH = _parse_bool(os.environ.get("USE_SIZE_MISMATCH", "false"), False)

# Simple in-process rate limit for the wrapper itself (no extra deps). Example: RATE_LIMIT=120/minute
RATE_LIMIT = (os.environ.get("RATE_LIMIT", "") or "").strip()
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
# ---------------------------
# HTTP session (retries)
# ---------------------------
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.6, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET", "POST"])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


# ---------------------------
# Optional: simple in-process rate limiting (no extra deps)
# ---------------------------
_rate_buckets: dict[str, deque] = defaultdict(deque)


def _parse_rate_limit(limit: str) -> tuple[int, int]:
    """Parse strings like '100/minute', '20/second', '300/hour'."""
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
    q = _rate_buckets[ip]
    while q and (now - q[0]) > window:
        q.popleft()
    if len(q) >= max_req:
        return ({'streams': []}, 429)
    q.append(now)
    return None


# ---------------------------
# Parsing helpers (resolution / age)
# ---------------------------

def _res_to_int(res: str) -> int:
    r = (res or '').upper()
    if r in ('4K', '2160P'):
        return 2160
    m = re.search(r'(\d{3,4})P', r)
    if m:
        return int(m.group(1))
    return 0


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
    t = (text or '').lower()
    return any(tok in t for tok in ['cached', 'instant', '⚡', 'tb+', 'tb⚡', 'rd+', 'ad+', 'nd+'])


def _cache_confidence(s: Dict[str, Any], meta: Dict[str, Any]) -> float:
    """Heuristic confidence that a stream is instant/cached.

    This is intentionally conservative: if we can't infer much, we return a low score.
    """
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


def _rd_strict_is_cached(infohash: str) -> Optional[bool]:
    """Strict RD cache test (mutates account state).

    Uses addMagnet -> selectFiles(all) -> info -> delete.
    Returns True/False if it can determine, or None if it cannot.
    """
    if not RD_API_KEY or not infohash or not re.fullmatch(r"[0-9a-fA-F]{40}", infohash):
        return None
    magnet = f"magnet:?xt=urn:btih:{infohash.upper()}"
    torrent_id: Optional[str] = None
    try:
        add = session.post(
            "https://api.real-debrid.com/rest/1.0/torrents/addMagnet",
            params={"auth_token": RD_API_KEY},
            data={"magnet": magnet},
            timeout=VALIDATE_CACHE_TIMEOUT,
        )
        add.raise_for_status()
        add_data = add.json() or {}
        torrent_id = add_data.get("id")
        if not torrent_id:
            return None

        # select all files
        try:
            session.post(
                f"https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{torrent_id}",
                params={"auth_token": RD_API_KEY},
                data={"files": "all"},
                timeout=VALIDATE_CACHE_TIMEOUT,
            )
        except Exception:
            pass

        info = session.get(
            f"https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}",
            params={"auth_token": RD_API_KEY},
            timeout=VALIDATE_CACHE_TIMEOUT,
        )
        info.raise_for_status()
        info_data = info.json() or {}
        links = info_data.get("links") or []
        if isinstance(links, list) and len(links) > 0:
            return True
        # If no links, treat as not cached/instant.
        return False
    except Exception as e:
        logger.debug("RD strict cache check failed: %s", e)
        return None
    finally:
        if torrent_id:
            try:
                session.delete(
                    f"https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}",
                    params={"auth_token": RD_API_KEY},
                    timeout=min(VALIDATE_CACHE_TIMEOUT, 6),
                )
            except Exception:
                pass


def _verify_stream_url(s: Dict[str, Any]) -> bool:
    if not VERIFY_STREAM:
        return True
    url = s.get('url') or s.get('externalUrl')
    if not url or not isinstance(url, str):
        return False
    try:
        r = session.head(url, allow_redirects=True, timeout=VERIFY_STREAM_TIMEOUT)
        if r.status_code in (200, 206, 302, 303, 307, 308):
            return True
        if r.status_code in (403, 405, 400):
            r2 = session.get(url, headers={'Range': 'bytes=0-0'}, stream=True, allow_redirects=True, timeout=VERIFY_STREAM_TIMEOUT)
            ok = r2.status_code in (200, 206)
            try:
                r2.close()
            except Exception:
                pass
            return ok
        return False
    except Exception:
        return False


# ---------------------------
# Optional: external blacklists / fakes DB
# ---------------------------

@lru_cache(maxsize=8)
def _load_blacklist_patterns() -> list[re.Pattern]:
    pats: list[re.Pattern] = []
    for term in BLACKLIST_TERMS:
        try:
            pats.append(re.compile(re.escape(term), re.I))
        except Exception:
            continue
    for url in BLACKLIST_URLS:
        try:
            r = session.get(url, timeout=6)
            if r.status_code != 200:
                continue
            for line in (r.text or '').splitlines():
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                try:
                    pats.append(re.compile(line, re.I))
                except Exception:
                    try:
                        pats.append(re.compile(re.escape(line), re.I))
                    except Exception:
                        pass
        except Exception:
            continue
    return pats


@lru_cache(maxsize=4)
def _load_fakes_db() -> set[str]:
    bad: set[str] = set()
    if not USE_FAKES_DB or not FAKES_DB_URL:
        return bad
    try:
        r = session.get(FAKES_DB_URL, timeout=6)
        if r.status_code != 200:
            return bad
        ct = (r.headers.get('content-type') or '').lower()
        if 'json' in ct:
            data = r.json()
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get('hashes') or []
            else:
                items = []
            for h in items:
                if isinstance(h, str) and re.fullmatch(r'[0-9a-fA-F]{40}', h.strip()):
                    bad.add(h.strip().lower())
        else:
            for line in (r.text or '').splitlines():
                h = line.strip()
                if re.fullmatch(r'[0-9a-fA-F]{40}', h):
                    bad.add(h.lower())
    except Exception:
        return bad
    return bad


def _is_blacklisted(text: str) -> bool:
    if not USE_BLACKLISTS:
        return False
    for pat in _load_blacklist_patterns():
        try:
            if pat.search(text or ''):
                return True
        except Exception:
            continue
    return False


def _is_valid_stream_id(type_: str, id_: str) -> bool:
    if type_ not in ('movie', 'series'):
        return False
    if not id_ or not isinstance(id_, str):
        return False
    if type_ == 'movie':
        return bool(re.match(r'^(tt\d+|tmdb:\d+)$', id_, re.I))
    return bool(re.match(r'^(tt\d+|tmdb:\d+)(:\d{1,4}:\d{1,4})?$', id_, re.I))
# ---------------------------
# Helpers: text sanitation + title normalization (borrowed from v16 superior & v20 hardening)
# ---------------------------
_CONTROL_RTL_RE = re.compile(r"[\x00-\x1F\x7F-\x9F\u200E\u200F\u202A-\u202E]")
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
    return "".join(CYRILLIC_MAPPING.get(ch, ch) for ch in (text or ""))
def normalize_display_title(text: str) -> str:
    """Best-effort to turn title into 'English-looking' display text while preserving meaning."""
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", text)
    # make punctuation consistent
    t = t.replace("–", "-").replace("—", "-").replace("…", "...")
    t = t.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    t = _CONTROL_RTL_RE.sub("", t)
    try:
        from unidecode import unidecode
        t = unidecode(t)
    except ImportError:
        pass # Fallback to built-in if unidecode not available
    t = transliterate_to_latin(t)
    t = t.replace("_", " ").replace(".", " ").strip()
    t = re.sub(r"\s+", " ", t)
    return t
def _truncate(t: str, max_len: int) -> str:
    t = (t or "").strip()
    if max_len <= 0 or len(t) <= max_len:
        return t
    return t[: max(0, max_len - 3)].rstrip() + "…"
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
def sanitize_stream_inplace(s: Dict[str, Any]) -> bool:
    """
    Hard gate: ensure stream fields are well-typed and safe to parse.
    Returns False if stream should be dropped immediately.
    """
    if not isinstance(s.get("name"), str):
        s["name"] = ""
    if not isinstance(s.get("description"), str):
        s["description"] = ""
    s["name"] = _CONTROL_RTL_RE.sub("", s.get("name", ""))
    s["description"] = _CONTROL_RTL_RE.sub("", s.get("description", ""))
    bh = s.get("behaviorHints")
    if not isinstance(bh, dict):
        s["behaviorHints"] = {}
    # Drop non-playable
    if not s.get("url") and not s.get("externalUrl"):
        return False
    return True
# ---------------------------
# Premium check (with retries and backoff)
# ---------------------------
@lru_cache(maxsize=1)
def is_premium_plan(provider: str, api_key: str) -> bool:
    if not VERIFY_PREMIUM or not api_key:
        return True
    if provider == "TB":
        for attempt in range(3):
            try:
                r = session.get(f"{TB_BASE}/v1/api/account/info", headers={"Authorization": f"Bearer {api_key}"}, timeout=5)
                r.raise_for_status()
                data = r.json()
                return data.get("plan", "").lower() in ["pro", "standard"]
            except requests.exceptions.RequestException as e:
                logger.warning(f"TB premium check failed (attempt {attempt+1}/3): {e}")
                if attempt < 2:
                    time.sleep(0.5 * (2 ** attempt))
                else:
                    if ASSUME_PREMIUM_ON_FAIL:
                        logger.info("Assuming TB premium on validation fail")
                        return True
                    return False
    elif provider == "RD":
        for attempt in range(3):
            try:
                r = session.get("https://api.real-debrid.com/rest/1.0/user", params={"auth_token": api_key}, timeout=5)
                r.raise_for_status()
                data = r.json()
                return data.get("type", "") == "premium"
            except requests.exceptions.RequestException as e:
                logger.warning(f"RD premium check failed (attempt {attempt+1}/3): {e}")
                if attempt < 2:
                    time.sleep(0.5 * (2 ** attempt))
                else:
                    if ASSUME_PREMIUM_ON_FAIL:
                        logger.info("Assuming RD premium on validation fail")
                        return True
                    return False
    elif provider in ["ND", "EW", "NG"]:  # Usenet
        key = os.environ.get(f"{provider}_API_KEY", api_key)
        url = os.environ.get(f"{provider}_URL", "") or "default_usenet_api/health"  # e.g., for ND: http://nzbdav:3000/api/health
        for attempt in range(3):
            try:
                headers = {"X-Api-Key": key} if key else {}
                r = session.get(url, headers=headers, timeout=5)
                r.raise_for_status()
                return True  # Success = premium access
            except requests.exceptions.RequestException as e:
                logger.warning(f"{provider} premium check failed (attempt {attempt+1}/3): {e}")
                if attempt < 2:
                    time.sleep(0.5 * (2 ** attempt))
                else:
                    if ASSUME_PREMIUM_ON_FAIL:
                        logger.info(f"Assuming {provider} premium on validation fail")
                        return True
                    return False
    # Add for AD if needed
    return True
# ---------------------------
# TorBox cache check (optional)
# ---------------------------
def tb_get_cached(hashes: List[str]) -> Dict[str, bool]:
    """Return mapping of info-hash -> cached boolean.

    TorBox checkcached responses are typically shaped like:
      {"success": true, "data": {"<hash>": { ... torrent details ... }, ...}}

    Some edge cases may return an empty dict, or a bool in the "data" field.
    This wrapper tolerates all of those without throwing.
    """
    if (not hashes) or (not TB_API_KEY) or (not TB_CACHE_HINTS):
        return {}

    # De-dupe while preserving order, then limit to TB_MAX_HASHES
    uniq: list[str] = []
    seen: set[str] = set()
    for h in hashes:
        if not h:
            continue
        hl = str(h).lower()
        if hl in seen:
            continue
        seen.add(hl)
        uniq.append(hl)
        if len(uniq) >= TB_MAX_HASHES:
            break
    hashes = uniq

    cached: Dict[str, bool] = {}
    for i in range(0, len(hashes), TB_BATCH_SIZE):
        batch = hashes[i:i + TB_BATCH_SIZE]
        try:
            r = session.post(
                f"{TB_BASE}/v1/api/torrents/checkcached",
                json={"hashes": batch},
                headers={"Authorization": f"Bearer {TB_API_KEY}"},
                timeout=min(REQUEST_TIMEOUT / 2, 10),
            )
            r.raise_for_status()
            raw = r.json() or {}

            # New API shape: {success: bool, data: {...}}
            if isinstance(raw, dict) and ("success" in raw or "data" in raw):
                if not raw.get("success", True):
                    logger.warning("TB cache check API returned success=false: %s", raw.get("detail") or raw)
                    continue
                data = raw.get("data", {})
                if isinstance(data, bool):
                    for h in batch:
                        cached[h.lower()] = bool(data)
                    continue
                if isinstance(data, dict):
                    for h in batch:
                        info = data.get(h) or data.get(h.lower())
                        if isinstance(info, dict):
                            if "is_cached" in info:
                                cached[h.lower()] = bool(info.get("is_cached"))
                            elif "cached" in info:
                                cached[h.lower()] = bool(info.get("cached"))
                            else:
                                cached[h.lower()] = True
                        else:
                            cached[h.lower()] = bool(info)
                    continue

            # Legacy/alt shape: {"<hash>": {...}} or {"<hash>": true}
            if isinstance(raw, dict):
                for h, info in raw.items():
                    if isinstance(info, dict):
                        cached[h.lower()] = bool(info.get("is_cached") or info.get("cached") or True)
                    else:
                        cached[h.lower()] = bool(info)

        except Exception as e:
            logger.warning("TB cache check failed: %s", e)

    return cached
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
    # Provider
    provider = "UNK"
    if "real-debrid" in text or "rd" in text:
        provider = "RD"
    elif "torbox" in text or "tb" in text:
        provider = "TB"
    elif "alldebrid" in text or "ad" in text:
        provider = "AD"
    elif "aio" in text:
        provider = "AIOStreams" # Short badge for unknowns
    # Usenet providers
    elif "nzbdav" in text or "nzb" in text:
        provider = "ND"
    elif "eweka" in text:
        provider = "EW"
    elif "nzgeek" in text:
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
    seeders = int(re.search(r"(\d+) seeds?", text).group(1)) if re.search(r"(\d+) seeds?", text) else 0
    # Infohash
    url = s.get("url", "") or s.get("externalUrl", "")
    hm = _HASH_RE.search(url)
    infohash = (hm.group(1) or hm.group(2)).lower() if hm else ""
    # Group
    gm = _GROUP_RE.search(filename)
    group = gm.group(1).upper() if gm else ""
    # Raw title for fallback
    title_raw = normalize_display_title(filename or desc or name)
    # Premium level
    premium_level = 1 if is_premium_plan(provider, TB_API_KEY if provider == "TB" else api_key_for_provider(provider)) else 0  # Adjust api_key_for_provider as needed
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
        "group": group,
        "title_raw": title_raw,
        "premium_level": premium_level,
    }
# Helper for provider keys (customize as per env)
def api_key_for_provider(provider: str) -> str:
    return os.environ.get(f"{provider}_API_KEY", TB_API_KEY)  # Fallback to TB or add specifics
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
        resp = session.get(f"{base}?api_key={TMDB_API_KEY}&language=en-US", timeout=5)
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
    meta: Dict[str, Any],
    expected: Dict[str, Any],
    cached_hint: str = "",
    type_: str = "",
    season: Optional[int] = None,
    episode: Optional[int] = None,
) -> None:
    # Left column: exactly 2 lines
    provider = meta.get("provider", "UNK")
    res = meta.get("res", "SD")
    s["name"] = f"{provider}\n{res}".strip()
    # Right header: title (English from TMDB if available, else cleaned raw)
    base_title = expected.get("title", meta.get("title_raw", "Unknown Title"))
    if FORCE_ASCII_TITLE:
        base_title = normalize_display_title(base_title)
    base_title = _truncate(base_title, MAX_TITLE_CHARS)
    year = expected.get("year")
    ep_tag = f" S{season:02d}E{episode:02d}" if season and episode else ""
    title_str = f"{base_title}{ep_tag}" + (f" ({year})" if year else "")
    s["title"] = _truncate(title_str, MAX_TITLE_CHARS)
    # Right body: exactly 2 lines (always present)
    tech_parts = []
    if res:
        tech_parts.append(res)
    if meta.get("source"):
        tech_parts.append(meta["source"])
    if meta.get("codec"):
        tech_parts.append(meta["codec"])
    line1 = " / ".join(tech_parts) if tech_parts else "SD"
    size_str = _human_size_bytes(meta.get("size", 0))
    seeders = meta.get("seeders", 0)
    group = meta.get("group", "")
    lang = meta.get("language", "EN")
    audio = meta.get("audio", "")
    bits = [f"Size {size_str}", f"Seeds {seeders if seeders > 0 else '?'}"]
    if cached_hint:
        bits.insert(0, cached_hint)
    if group:
        bits.append(f"Grp {group}")
    if lang:
        bits.append(f"Lang {lang}")
    if audio:
        bits.append(f"Aud {audio}")
    line2 = " ".join(bits)
    s["description"] = _truncate(line1, MAX_DESC_CHARS) + "\n" + _truncate(line2, MAX_DESC_CHARS)
# ---------------------------
# Fetch streams
# ---------------------------
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

def _fetch_streams_from_base(base: str, auth: str, type_: str, id_: str, tag: str) -> List[Dict[str, Any]]:
    if not base:
        return []
    url = f"{base}/stream/{type_}/{id_}.json"
    headers = _auth_headers(auth)
    logger.info(f'{tag} fetch URL: {url}')
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
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

def get_streams(type_: str, id_: str) -> Tuple[List[Dict[str, Any]], int, int]:
    """Fetch both providers in parallel (best-effort) and return merged streams + counts."""
    s1: List[Dict[str, Any]] = []
    s2: List[Dict[str, Any]] = []

    tasks = []
    with ThreadPoolExecutor(max_workers=2) as ex:
        tasks.append(("AIO", ex.submit(_fetch_streams_from_base, AIO_BASE, AIOSTREAMS_AUTH, type_, id_, 'AIO')))
        if PROV2_BASE:
            tasks.append(("P2", ex.submit(_fetch_streams_from_base, PROV2_BASE, PROV2_AUTH, type_, id_, PROV2_TAG)))
        for tag, fut in tasks:
            try:
                res = fut.result(timeout=REQUEST_TIMEOUT + 2)
            except Exception as e:
                logger.error(f'{tag} fetch error (parallel): {e}')
                res = []
            if tag == 'AIO':
                s1 = res
            else:
                s2 = res

    merged = (s1 + s2)[:INPUT_CAP]
    return merged, len(s1), len(s2)

# ---------------------------
# Pipeline
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
    dropped_low_res: int = 0
    dropped_old_age: int = 0
    dropped_blacklist: int = 0
    dropped_fakes_db: int = 0
    dropped_dead_url: int = 0
    dropped_uncached: int = 0
    dropped_uncached_tb: int = 0
    deduped: int = 0
    delivered: int = 0
def dedup_key(stream: Dict[str, Any], meta: Dict[str, Any]) -> str:
    return f"{meta.get('infohash','')}:{meta.get('size',0)}:{stream.get('url','') or stream.get('externalUrl','')}:{meta.get('language','')}:{meta.get('audio','')}"
def filter_and_format(type_: str, id_: str, streams: List[Dict[str, Any]], aio_in: int = 0, prov2_in: int = 0) -> Tuple[List[Dict[str, Any]], PipeStats]:
    stats = PipeStats()
    stats.aio_in = aio_in
    stats.prov2_in = prov2_in
    stats.merged_in = len(streams)

    expected = get_expected_metadata(type_, id_)

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
                text = f"{s.get('name','')} {s.get('description','')} {m.get('release','')}"
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
            if VERIFY_STREAM and not _verify_stream_url(s):
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

    # Fast path sorting (cheap) to pick candidates first.
    priority_order = {p: len(PREMIUM_PRIORITY + USENET_PRIORITY) - i for i, p in enumerate(PREMIUM_PRIORITY + USENET_PRIORITY)}

    def _sort_key(pair: Tuple[Dict[str, Any], Dict[str, Any]]):
        _s, _m = pair
        provider = _m.get('provider', 'UNK')
        size = _m.get('size', 0)
        rd_penalty = -1 if (DEPRIORITIZE_RD and provider == 'RD') else 0
        return (
            _m.get('premium_level', 0),
            rd_penalty,
            size,
            _m.get('seeders', 0),
            priority_order.get(provider, 0),
        )

    out_pairs.sort(key=_sort_key, reverse=True)

    # Candidate window: a little bigger so we can satisfy usenet quotas.
    window = max(MAX_DELIVER, MIN_USENET_KEEP, MIN_USENET_DELIVER, 1)
    candidates = out_pairs[: window * 4]

    # TorBox cache hints / enforcement
    cached_map: Dict[str, bool] = {}
    if TB_API_KEY and TB_CACHE_HINTS and candidates:
        hashes: list[str] = []
        seen_h: set[str] = set()
        max_check = min(TB_MAX_HASHES, len(candidates))
        for _, meta in candidates:
            h = (meta.get('infohash') or '').lower()
            if not h or h in seen_h:
                continue
            seen_h.add(h)
            hashes.append(h)
            if len(hashes) >= max_check:
                break
        cached_map = tb_get_cached(hashes)

        # Re-sort candidates using cached hint as a tiebreaker.
        candidates.sort(
            key=lambda p: (
                p[1].get('premium_level', 0),
                cached_map.get((p[1].get('infohash') or '').lower(), False),
                -1 if (DEPRIORITIZE_RD and p[1].get('provider') == 'RD') else 0,
                p[1].get('size', 0),
                p[1].get('seeders', 0),
                priority_order.get(p[1].get('provider', 'UNK'), 0),
            ),
            reverse=True,
        )

        # Optional: enforce TorBox cached-only (drop TB streams that are not cached).
        if TORBOX_CACHE_CHECK and not VERIFY_TB_CACHE_OFF:
            before = len(candidates)
            def _is_tb_cached(pair):
                _s, _m = pair
                if _m.get('provider') != 'TB':
                    return True
                h = (_m.get('infohash') or '').lower()
                return bool(h) and cached_map.get(h, False)
            candidates = [p for p in candidates if _is_tb_cached(p)]
            stats.dropped_uncached_tb += before - len(candidates)

    # Optional: cached/instant-only enforcement across providers.
    # - TB: requires TB_CACHE_HINTS + TB_API_KEY (otherwise we can't confirm)
    # - RD: heuristic by default; strict workflow if RD_STRICT_CACHE_CHECK=true and RD_API_KEY is set
    # - AD: heuristic only (strict workflow not implemented here)
    if VERIFY_CACHED_ONLY and not VALIDATE_OFF:
        before = len(candidates)
        strict_used = 0

        def _is_cached(pair: Tuple[Dict[str, Any], Dict[str, Any]]) -> bool:
            nonlocal strict_used
            _s, _m = pair
            provider = _m.get('provider', 'UNK')
            h = (_m.get('infohash') or '').lower()

            # Usenet / unknown providers are treated as playable without cache confirmation.
            if provider in USENET_PRIORITY or provider not in ('RD', 'AD', 'TB'):
                _m['cached'] = True
                return True

            if provider == 'TB':
                if h and cached_map:
                    ok = bool(cached_map.get(h, False))
                    _m['cached'] = ok
                    return ok
                # If we can't check TB cache status, be conservative unless the user opted to assume.
                if ASSUME_PREMIUM_ON_FAIL:
                    _m['cached'] = True
                    return True
                return False

            if provider == 'RD':
                # Strict check is expensive and mutates RD state; cap it per request.
                if RD_STRICT_CACHE_CHECK and RD_API_KEY and h and strict_used < STRICT_CACHE_MAX:
                    strict_used += 1
                    res = _rd_strict_is_cached(h)
                    if res is not None:
                        _m['cached'] = bool(res)
                        return bool(res)
                # Fallback heuristic
                ok = _heuristic_cached(_s, _m)
                _m['cached'] = ok
                return ok

            if provider == 'AD':
                if AD_STRICT_CACHE_CHECK and AD_API_KEY:
                    # Not implemented: AllDebrid strict cache workflow differs per API.
                    # Fallback to heuristics.
                    pass
                ok = _heuristic_cached(_s, _m)
                _m['cached'] = ok
                return ok

            return True

        candidates = [p for p in candidates if _is_cached(p)]
        stats.dropped_uncached += before - len(candidates)

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
            slice_ = candidates[:MAX_DELIVER]
            have = sum(1 for p in slice_ if _is_usenet(p))
            if have < MIN_USENET_DELIVER:
                extras = [p for p in candidates[MAX_DELIVER:] + out_pairs[len(candidates):] if _is_usenet(p) and p not in slice_]
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
                candidates = slice_ + candidates[MAX_DELIVER:]

    # Format (last step)
    delivered: List[Dict[str, Any]] = []
    for s, m in candidates[:MAX_DELIVER]:
        h = (m.get('infohash') or '').lower()
        is_cached = bool(m.get('cached')) or (h and cached_map.get(h, False))
        cached_hint = "CACHED" if is_cached else ""
        if ADD_CACHE_HINT and not cached_hint:
            if _looks_instant((s.get('name','') or '') + ' ' + (s.get('description','') or '')):
                cached_hint = "LIKELY"
        if REFORMAT_STREAMS:
            format_stream_inplace(s, m, expected, cached_hint, type_, season, episode)
        delivered.append(s)

    stats.delivered = len(delivered)
    return delivered, stats

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
            "id": "org.buubuu.aio.wrapper.merge",
            "version": "1.0.0",
            "name": "AIO Wrapper (Unified 2 Providers)",
            "description": "Unified 2 providers into one stream list with hardened normalization + strict Stremio formatting. VALIDATE_OFF bypass supported.",
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
    try:
        streams, aio_in, prov2_in = get_streams(type_, id_)
        stats.aio_in = aio_in
        stats.prov2_in = prov2_in
        out, stats = filter_and_format(type_, id_, streams, aio_in=aio_in, prov2_in=prov2_in)
        return jsonify({"streams": out}), 200
    except Exception as e:
        logger.exception(f"Stream error: {e}")
        return jsonify({"streams": []}), 200
    finally:
        logger.info(
            "WRAP_STATS rid=%s type=%s id=%s aio_in=%s prov2_in=%s merged_in=%s dropped_error=%s dropped_missing_url=%s dropped_pollution=%s dropped_low_seeders=%s dropped_lang=%s dropped_low_premium=%s dropped_rd=%s dropped_low_res=%s dropped_old_age=%s dropped_blacklist=%s dropped_fakes_db=%s dropped_dead_url=%s dropped_uncached=%s dropped_uncached_tb=%s deduped=%s delivered=%s ms=%s",
            _rid(), type_, id_,
            stats.aio_in, stats.prov2_in, stats.merged_in,
            stats.dropped_error, stats.dropped_missing_url, stats.dropped_pollution,
            stats.dropped_low_seeders, stats.dropped_lang,
            stats.dropped_low_premium,
            stats.dropped_rd, stats.dropped_low_res, stats.dropped_old_age,
            stats.dropped_blacklist, stats.dropped_fakes_db, stats.dropped_dead_url, stats.dropped_uncached, stats.dropped_uncached_tb,
            stats.deduped, stats.delivered,
            int((time.time() - t0) * 1000),
        )
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=LOG_LEVEL == logging.DEBUG, use_reloader=False)

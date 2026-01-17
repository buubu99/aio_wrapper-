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
import requests
from flask import Flask, jsonify, g, has_request_context
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
    raw = (raw or "").strip().rstrip("/")
    if raw.endswith("/manifest.json"):
        raw = raw[: -len("/manifest.json")].rstrip("/")
    return raw
# ---------------------------
# Config (keep env names compatible with your existing Render setup)
# ---------------------------
AIO_URL = os.environ.get("AIO_URL", "")
AIO_BASE = _normalize_base(os.environ.get("AIO_BASE", "") or AIO_URL)
# can be base url or .../manifest.json
AIOSTREAMS_AUTH = os.environ.get("AIOSTREAMS_AUTH", "") # "user:pass"
INPUT_CAP = int(os.environ.get("INPUT_CAP", "4500"))
MAX_DELIVER = int(os.environ.get("MAX_DELIVER", "80"))
# Formatting / UI constraints
REFORMAT_STREAMS = _parse_bool(os.environ.get("REFORMAT_STREAMS", "true"), True)
FORCE_ASCII_TITLE = os.environ.get("FORCE_ASCII_TITLE", "true").lower() == "true"
MAX_TITLE_CHARS = int(os.environ.get("MAX_TITLE_CHARS", "110"))
MAX_DESC_CHARS = int(os.environ.get("MAX_DESC_CHARS", "180"))
# Validation/testing toggles
VALIDATE_OFF = _parse_bool(os.environ.get("VALIDATE_OFF", "false"), False)  # bypass strict filters when True
DROP_POLLUTED = _parse_bool(os.environ.get("DROP_POLLUTED", "false"), False)  # optional
# TorBox cache hint (optional; safe if unset)
TB_API_KEY = os.environ.get("TB_API_KEY", "")
TB_BASE = "https://api.torbox.app"
TB_BATCH_SIZE = int(os.environ.get("TB_BATCH_SIZE", "50"))
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
                    # Rare case: treat as all cached / none cached
                    for h in batch:
                        cached[h.lower()] = bool(data)
                    continue
                if isinstance(data, dict):
                    for h in batch:
                        info = data.get(h) or data.get(h.lower())
                        if isinstance(info, dict):
                            # Prefer explicit flags if present, else presence means cached
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
            logger.warning(f"TB cache check failed: {e}")

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
    s["name"] = f"{provider}\n⏳ {res}".strip()
    # Right header: title (English from TMDB if available, else cleaned raw)
    base_title = expected.get("title", meta.get("title_raw", "Unknown Title"))
    if FORCE_ASCII_TITLE:
        base_title = normalize_display_title(base_title)
    base_title = _truncate(base_title, MAX_TITLE_CHARS)
    year = expected.get("year")
    ep_tag = f" S{season:02d}E{episode:02d}" if season and episode else ""
    title_str = f"📄 {base_title}{ep_tag}" + (f" ({year})" if year else "")
    s["title"] = _truncate(title_str, MAX_TITLE_CHARS)
    # Right body: exactly 2 lines (always present)
    tech_parts = []
    if res:
        tech_parts.append(res)
    if meta.get("source"):
        tech_parts.append(meta["source"])
    if meta.get("codec"):
        tech_parts.append(meta["codec"])
    line1 = "💿 " + " • ".join(tech_parts) if tech_parts else "💿 SD"
    size_str = _human_size_bytes(meta.get("size", 0))
    seeders = meta.get("seeders", 0)
    group = meta.get("group", "")
    lang = meta.get("language", "EN")
    audio = meta.get("audio", "")
    bits = [f"📦 {size_str}", f"👥 {seeders if seeders > 0 else '?'}"]
    if cached_hint:
        bits.insert(0, cached_hint)
    if group:
        bits.append(f"⛓ {group}")
    if lang:
        bits.append(f"🌐 {lang}")
    if audio:
        bits.append(f"🔊 {audio}")
    line2 = " ".join(bits)
    s["description"] = _truncate(line1, MAX_DESC_CHARS) + "\n" + _truncate(line2, MAX_DESC_CHARS)
# ---------------------------
# Fetch streams
# ---------------------------
def get_streams(type_: str, id_: str) -> Tuple[List[Dict[str, Any]], int]:
    if not AIO_BASE:
        return [], 0
    url = f"{AIO_BASE}/stream/{type_}/{id_}.json"
    headers = {}
    if AIOSTREAMS_AUTH:
        user, pw = AIOSTREAMS_AUTH.split(":", 1)
        headers["Authorization"] = "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()
    logger.info(f"AIO fetch URL: {url}")
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        logger.info(f"AIO status={resp.status_code} bytes={len(resp.content)}")
        if resp.status_code != 200:
            return [], 0
        data = resp.json() if resp.content else {}
        streams = (data.get("streams") or [])[:INPUT_CAP]
        return streams, len(streams)
    except json.JSONDecodeError as e:
        logger.error(f"AIO JSON error: {e}; head={resp.text[:200] if 'resp' in locals() else ''}")
        return [], 0
    except Exception as e:
        logger.error(f"AIO fetch error: {e}")
        return [], 0
# ---------------------------
# Pipeline
# ---------------------------
@dataclass
class PipeStats:
    aio_in: int = 0
    merged_in: int = 0
    dropped_error: int = 0
    dropped_missing_url: int = 0
    dropped_pollution: int = 0
    dropped_low_seeders: int = 0
    dropped_lang: int = 0
    dropped_low_premium: int = 0
    deduped: int = 0
    delivered: int = 0
def dedup_key(stream: Dict[str, Any], meta: Dict[str, Any]) -> str:
    return f"{meta.get('infohash','')}:{meta.get('size',0)}:{stream.get('url','') or stream.get('externalUrl','')}:{meta.get('language','')}:{meta.get('audio','')}"
def filter_and_format(type_: str, id_: str, streams: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], PipeStats]:
    stats = PipeStats()
    stats.aio_in = len(streams)
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
    cleaned: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for s in streams:
        if not isinstance(s, dict):
            continue
        if not sanitize_stream_inplace(s):
            stats.dropped_missing_url += 1
            continue
        # Drop explicit error streams
        if (s.get("streamData") or {}).get("type") == "error":
            stats.dropped_error += 1
            continue
        m = classify(s)
        if m['seeders'] < MIN_SEEDERS:
            stats.dropped_low_seeders += 1
            continue
        if PREFERRED_LANG and m['language'] != PREFERRED_LANG:
            stats.dropped_lang += 1
            continue
        if DROP_POLLUTED and is_polluted(s, type_, season, episode):
            stats.dropped_pollution += 1
            continue
        if VERIFY_PREMIUM and m["premium_level"] == 0:
            stats.dropped_low_premium += 1
            continue
        cleaned.append((s, m))
    # Dedup
    out_pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    seen = set()
    for s, m in cleaned:
        k = dedup_key(s, m)
        if k in seen:
            stats.deduped += 1
            continue
        seen.add(k)
        out_pairs.append((s, m))
    # Cache hints (optional)
    cached_map: Dict[str, bool] = {}
    if TB_API_KEY:
        hashes = [m.get("infohash", "") for _, m in out_pairs if m.get("infohash")]
        cached_map = tb_get_cached(list({h for h in hashes if h}))
    # Sort by priority: premium_level > cached > seeders > provider
    priority_order = {p: len(PREMIUM_PRIORITY + USENET_PRIORITY) - i for i, p in enumerate(PREMIUM_PRIORITY + USENET_PRIORITY)}
    out_pairs.sort(key=lambda p: (p[1].get("premium_level", 0), cached_map.get(p[1].get('infohash', ''), False), p[1].get('seeders', 0), priority_order.get(p[1].get('provider', 'UNK'), 0)), reverse=True)
    # Format (last step)
    delivered: List[Dict[str, Any]] = []
    for s, m in out_pairs[:MAX_DELIVER]:
        cached_hint = "⚡ Cached" if cached_map.get(m.get("infohash", ""), False) else ""
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
            "name": "AIO Wrapper (Merged 16+20 - Stremio Strict)",
            "description": "Merged formatting (2-left, 3-right) + hardened normalization; validation pass-through for testing.",
            "resources": ["stream"],
            "types": ["movie", "series"],
            "catalogs": [],
            "idPrefixes": ["tt", "tmdb"],
        }
    )
@app.get("/stream/<type_>/<id_>.json")
def stream(type_: str, id_: str):
    if type_ not in ["movie", "series"] or not id_:
        return jsonify({"streams": []}), 400
    t0 = time.time()
    stats = PipeStats()
    try:
        streams, stats.aio_in = get_streams(type_, id_)
        out, stats = filter_and_format(type_, id_, streams)
        return jsonify({"streams": out}), 200
    except Exception as e:
        logger.exception(f"Stream error: {e}")
        return jsonify({"streams": []}), 200
    finally:
        logger.info(
            "WRAP_STATS rid=%s type=%s id=%s aio_in=%s merged_in=%s dropped_error=%s dropped_missing_url=%s dropped_pollution=%s dropped_low_seeders=%s dropped_lang=%s dropped_low_premium=%s deduped=%s delivered=%s ms=%s",
            _rid(), type_, id_,
            stats.aio_in, stats.merged_in,
            stats.dropped_error, stats.dropped_missing_url, stats.dropped_pollution,
            stats.dropped_low_seeders, stats.dropped_lang,
            stats.dropped_low_premium,
            stats.deduped, stats.delivered,
            int((time.time() - t0) * 1000),
        )
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=LOG_LEVEL == logging.DEBUG, use_reloader=False)

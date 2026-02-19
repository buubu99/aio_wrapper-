from __future__ import annotations
import base64
import hashlib
import html as _html
import json
import logging
import os
import re
import time
import unicodedata
import uuid
import difflib
import random
import subprocess

# ---------------------------
# Build / version metadata (logging)
# ---------------------------
def _get_git_commit() -> str:
    env = (os.environ.get("GIT_COMMIT") or os.environ.get("RENDER_GIT_COMMIT") or "").strip()
    if env:
        return env[:8]
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        if re.fullmatch(r"[0-9a-fA-F]{7,40}", out):
            return out[:8]
    except Exception:
        pass
    return "unknown"

GIT_COMMIT = _get_git_commit()
BUILD = os.environ.get("BUILD", os.environ.get("BUILD_ID", "v1.0"))  # Set BUILD (or legacy BUILD_ID) in Render for deploy labels

def _safe_ua(ua_str: str, max_len: int = 100) -> str:
    """Truncate/escape UA to keep logs sane."""
    try:
        s = str(ua_str or "")
        s = s[:max_len].replace('"', '\"').replace("\n", " ").replace("\r", " ")
        return s
    except Exception:
        return ""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET  # NZBGeek (Newznab) readiness checks
import threading
import resource  # For memory tracking (ru_maxrss)
from collections import defaultdict, deque
from dataclasses import dataclass, field

# ---------------------------
# Pipeline stats (required)
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
    skipped_title_mismatch: int = 0  # title/ep-title validation skipped (quality-only titles)
    dropped_dead_url: int = 0
    dropped_uncached: int = 0
    dropped_uncached_tb: int = 0
    dropped_android_magnets: int = 0
    dropped_iphone_magnets: int = 0
    dropped_low_size_iphone: int = 0  # iPhone size gate drops (min size)
    dropped_platform_specific: int = 0  # Total platform-specific drops (android+iphone)
    client_platform: str = ""
    platform: str = ""
    deduped: int = 0
    delivered: int = 0


    # Cache summary (delivered only)
    cache_hit: int = 0
    cache_miss: int = 0
    cache_rate: float = 0.0  # cache_hit/(cache_hit+cache_miss)
    # Timing/diagnostics (ms).
    # ms_fetch_* are local wrapper durations for each provider fetch (HTTP + decode + light tagging).
    # Use ms_fetch_wall to see total wall time spent in the parallel fetch phase for this request.
    ms_fetch_aio: int = 0
    ms_fetch_p2: int = 0
    # Provider-reported/internal fetch times (when provided by upstream JSON; extracted into meta['ms_provider']).
    ms_fetch_aio_remote: int = 0
    ms_fetch_p2_remote: int = 0
    ms_tmdb: int = 0
    ms_tb_api: int = 0
    ms_tb_webdav: int = 0
    ms_tb_usenet: int = 0
    # Local processing timings (measured inside Python; includes CPU work after fetches)
    ms_py_ff: int = 0              # total time inside filter_and_format
    ms_py_dedup: int = 0           # dedup + best-pick loop
    ms_py_wrap_emit: int = 0       # wrapping /r/<token> URLs + building final delivered list
    ms_join_aio: int = 0           # time spent waiting on AIO future join (executor wait)
    ms_join_p2: int = 0            # time spent waiting on P2 future join (executor wait)
    ms_py_clean: int = 0           # main filter/classify loop (pre title/TB)
    ms_py_sort: int = 0            # total time in sort(s)
    ms_py_mix: int = 0             # provider mixing/diversity reordering stage(s)
    ms_py_tb_prep: int = 0         # TorBox API hash prep (collect/prune)
    ms_py_tb_mark_only: int = 0    # time to mark TB cached flags on candidates (no dropping)
    ms_py_ff_overhead: int = 0     # unaccounted time inside filter_and_format (ms_py_ff - known subphases)
    ms_fetch_wall: int = 0         # wall-clock time around provider fetch phase
    ms_overhead: int = 0           # ms_total minus known phases (approx)
    ms_total: int = 0              # total request duration (monotonic)
    ms_usenet_ready_match: int = 0 # difflib.SequenceMatcher comparisons for usenet readiness
    ms_usenet_probe: int = 0      # direct usenet proxy byte-range probe (REAL vs STUB)

    ms_usenet_probe_fail_reasons: dict = field(default_factory=dict)  # e.g., {'STUB_LEN': 5}


    # Per-filter timings (ms)
    ms_title_mismatch: int = 0
    ms_uncached_check: int = 0

    memory_peak_kb: int = 0  # ru_maxrss delta during request (kb on Linux)

    # Hash counts (diagnostics)
    tb_api_hashes: int = 0
    tb_webdav_hashes: int = 0
    tb_usenet_hashes: int = 0

    # RD heuristic marker counters (per-request; proves heuristic ran)
    rd_heur_calls: int = 0
    rd_heur_true: int = 0
    rd_heur_false: int = 0
    rd_heur_conf_sum: float = 0.0

    # Debug/summary objects (kept small; used for logs and optional debug responses)
    fetch_aio: Dict[str, Any] = field(default_factory=dict)
    fetch_p2: Dict[str, Any] = field(default_factory=dict)
    counts_in: Dict[str, Any] = field(default_factory=dict)
    counts_out: Dict[str, Any] = field(default_factory=dict)

    # Captured issues for weekly review
    # Error breakdown (fetch/meta + exceptions)
    errors_timeout: int = 0
    errors_parse: int = 0
    errors_api: int = 0

    error_reasons: List[str] = field(default_factory=list)
    flag_issues: List[str] = field(default_factory=list)

def _set_stats_platform(stats: PipeStats, platform: str) -> None:
    """Keep platform fields in sync (client_platform + platform)."""
    stats.client_platform = platform
    stats.platform = platform


from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, wait
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
import requests


# Optional UA parsing (tiny pure-Python dep: `ua-parser`)
try:
    from ua_parser import parse as ua_parse  # pip install ua-parser
    UA_PARSER_AVAILABLE = True
except Exception:
    ua_parse = None
    UA_PARSER_AVAILABLE = False
from flask import Flask, jsonify, g, has_request_context, request, make_response, Response, send_from_directory, abort
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

def _is_true(v: str, default: bool = False) -> bool:
    """Compat helper: boolean env parsing.

    Earlier snippets referenced _is_true(); keep it as an alias to _parse_bool.
    """
    return _parse_bool(v, default)


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
# Add: validate provider base URLs (avoid silent empty streams on bad env)
if AIO_BASE and not str(AIO_BASE).startswith(("http://", "https://")):
    logger.warning("Invalid AIO_BASE: %s - disabling provider", AIO_BASE)
    AIO_BASE = ""
if PROV2_BASE and not str(PROV2_BASE).startswith(("http://", "https://")):
    logger.warning("Invalid PROV2_BASE: %s - disabling provider", PROV2_BASE)
    PROV2_BASE = ""

PROV2_AUTH = os.environ.get("PROV2_AUTH", "")  # 'user:pass' for Basic auth if needed
PROV2_TAG = os.environ.get("PROV2_TAG", "P2")
AIO_TAG = os.environ.get("AIO_TAG", "AIO")

USE_AIO_READY = _parse_bool(os.environ.get("USE_AIO_READY", "false"), False)  # Trust AIOStreams 2.23+ C:/P:/T: tags

# P1 Hybrid A+C (merge/sort only; no new network calls).
# - "ac" (default): expression-driven buckets + safe tie-breaks using 2.23 fields (RRXM/RXM/RSEM, VT/AT, SE★/NSE).
# - "legacy"/"off": keep legacy ordering (no P1 bucket/qscore in sort key).
P1_MODE_RAW = (os.environ.get("P1_MODE", "ac") or "ac").strip().lower()
if P1_MODE_RAW in ("0", "false", "no", "off", "legacy", "old"):
    P1_MODE = "legacy"
elif P1_MODE_RAW in ("1", "true", "yes", "ac", "hybrid", "p1"):
    P1_MODE = "ac"
else:
    P1_MODE = "ac"
P1_DEBUG = _parse_bool(os.environ.get("P1_DEBUG", "0"))

# Sanity demotion: prevent obviously mis-sized "4K BluRay/REMUX" entries from floating above real high-quality options.
# This does NOT drop streams; it only nudges suspicious entries lower in the sort.
SANITY_DEMOTE = _parse_bool(os.environ.get("SANITY_DEMOTE", "1"), True)
SANITY_4K_BLURAY_MIN_GB = _safe_float(os.environ.get("SANITY_4K_BLURAY_MIN_GB", "8"), 8.0)
SANITY_4K_REMUX_MIN_GB  = _safe_float(os.environ.get("SANITY_4K_REMUX_MIN_GB", "20"), 20.0)
# Apply sanity demotion only for movies (series can have smaller per-episode sizes and still be legit).
SANITY_MOVIES_ONLY = _parse_bool(os.environ.get("SANITY_MOVIES_ONLY", "1"), True)


WRAP_LOG_COUNTS = _parse_bool(os.environ.get("WRAP_LOG_COUNTS", "1"))
WRAP_EMBED_DEBUG = _parse_bool(os.environ.get("WRAP_EMBED_DEBUG", "0"))

# Weekly review flag thresholds (env-tunable)
FLAG_HIGH_DROP_PCT = _safe_float(os.environ.get("FLAG_HIGH_DROP_PCT", "50"), 50.0)
FLAG_SLOW_AIO_MS = _safe_int(os.environ.get("FLAG_SLOW_AIO_MS", "7000"), 7000)
FLAG_SLOW_P2_MS = _safe_int(os.environ.get("FLAG_SLOW_P2_MS", "7000"), 7000)
FLAG_SLOW_TB_API_MS = _safe_int(os.environ.get("FLAG_SLOW_TB_API_MS", "3000"), 3000)
FLAG_SLOW_TITLE_MS = _safe_int(os.environ.get("FLAG_SLOW_TITLE_MS", "800"), 800)
FLAG_SLOW_UNCACHED_MS = _safe_int(os.environ.get("FLAG_SLOW_UNCACHED_MS", "800"), 800)
ENABLE_STATS_ENDPOINT = _parse_bool(os.environ.get("ENABLE_STATS_ENDPOINT", "true"), True)

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

# Point 11 (Dedup tie-break tuning): weights are env-configurable so we can adjust without code changes.
# Readiness is derived from flags we already compute (cached True, NZBGeek ready True, cached=='LIKELY').
DEDUP_READINESS_TRUE = _safe_float(os.environ.get("DEDUP_READINESS_TRUE", "1.0"), 1.0)
DEDUP_READINESS_READY = _safe_float(os.environ.get("DEDUP_READINESS_READY", "0.8"), 0.8)  # default for NZBGeek ready
DEDUP_READINESS_LIKELY = _safe_float(os.environ.get("DEDUP_READINESS_LIKELY", "0.5"), 0.5)
DEDUP_TITLE_WEIGHT = _safe_float(os.environ.get("DEDUP_TITLE_WEIGHT", "1.0"), 1.0)  # multiplier for title match ratio
TRAKT_STRICT_YEAR = _parse_bool(os.environ.get("TRAKT_STRICT_YEAR", "false"), False)
TRAKT_CLIENT_ID = (os.environ.get('TRAKT_CLIENT_ID') or '').strip()

# Validation/testing toggles
VALIDATE_OFF = _parse_bool(os.environ.get("VALIDATE_OFF", "false"), False)  # pass-through for format testing
DROP_POLLUTED = _parse_bool(os.environ.get("DROP_POLLUTED", "true"), True)  # optional
# TorBox cache hint (optional; safe if unset)
TB_API_KEY = os.environ.get("TB_API_KEY", "")
TB_BASE = "https://api.torbox.app"
TB_BATCH_SIZE = _safe_int(os.environ.get('TB_BATCH_SIZE', '50'), 50)
TB_BATCH_CONCURRENCY = _safe_int(os.environ.get('TB_BATCH_CONCURRENCY', '1'), 1)  # 1=sequential; >1 parallelize TorBox batch requests
TB_MAX_HASHES = _safe_int(os.environ.get('TB_MAX_HASHES', '60'), 60)  # limit hashes checked per request for speed

# TorBox per-platform hash budgets (fallback to TB_MAX_HASHES if unset)
TB_MAX_HASHES_DESKTOP = _safe_int(os.environ.get('TB_MAX_HASHES_DESKTOP', str(TB_MAX_HASHES)), TB_MAX_HASHES)
TB_MAX_HASHES_ANDROID = _safe_int(os.environ.get('TB_MAX_HASHES_ANDROID', str(TB_MAX_HASHES)), TB_MAX_HASHES)
TB_MAX_HASHES_IPHONE = _safe_int(os.environ.get('TB_MAX_HASHES_IPHONE', str(TB_MAX_HASHES)), TB_MAX_HASHES)
TB_MAX_HASHES_ANDROIDTV = _safe_int(os.environ.get('TB_MAX_HASHES_ANDROIDTV', str(TB_MAX_HASHES)), TB_MAX_HASHES)

# Futures timeouts (seconds) to prevent slow/blocked futures from stalling /stream
TB_BATCH_FUTURE_TIMEOUT = _safe_float(os.environ.get('TB_BATCH_FUTURE_TIMEOUT', '8'), 8.0)
WEBDAV_FUTURE_TIMEOUT = _safe_float(os.environ.get('WEBDAV_FUTURE_TIMEOUT', '3'), 3.0)
VERIFY_FUTURE_TIMEOUT = _safe_float(os.environ.get('VERIFY_FUTURE_TIMEOUT', '4'), 4.0)

# RD heuristic tuning knobs
RD_HEUR_THR = _safe_float(os.environ.get('RD_HEUR_THR', '0.82'), 0.82)
RD_HEUR_MIN_SIZE_GB = _safe_float(os.environ.get('RD_HEUR_MIN_SIZE_GB', '1.0'), 1.0)

# TorBox known-cached memoization (TTL cache)
TB_KNOWN_CACHED_TTL = _safe_int(os.environ.get('TB_KNOWN_CACHED_TTL', '3600'), 3600)
TB_KNOWN_CACHED_MAX = _safe_int(os.environ.get('TB_KNOWN_CACHED_MAX', '20000'), 20000)
TB_API_MIN_HASHES = _safe_int(os.environ.get('TB_API_MIN_HASHES', '20'), 20)  # skip TorBox API calls if fewer hashes
TB_CACHE_HINTS = _parse_bool(os.environ.get("TB_CACHE_HINTS", "true"), True)  # enable TorBox cache hint lookups
TB_EARLY_EXIT = _parse_bool(os.environ.get("TB_EARLY_EXIT", "false"), False)  # skip TorBox checks when enough cached hints already present
TB_EARLY_EXIT_MULT = _safe_int(os.environ.get("TB_EARLY_EXIT_MULT", "2"), 2)  # lookahead multiplier for early-exit cached-hint scan
TB_USENET_CHECK = _parse_bool(os.environ.get("TB_USENET_CHECK", "false"), False)  # optional usenet cache checks (requires identifiers)
REQUEST_TIMEOUT = _safe_float(os.environ.get('REQUEST_TIMEOUT', '30'), 30.0)
# Stream response cache TTL exposed to Stremio clients (seconds)
CACHE_TTL = _safe_int(os.environ.get('CACHE_TTL', '600'), 600)

# Client-side time budgets (seconds). These are upper bounds; we return as soon as we have results.
ANDROID_STREAM_TIMEOUT = _safe_float(os.environ.get('ANDROID_STREAM_TIMEOUT', '20'), 20.0)  # FIXED: increase default
DESKTOP_STREAM_TIMEOUT = _safe_float(os.environ.get('DESKTOP_STREAM_TIMEOUT', '30'), 30.0)  # FIXED: increase default
EMPTY_UA_IS_ANDROID = _parse_bool(os.environ.get('EMPTY_UA_IS_ANDROID', 'false'), False)  # treat blank UA as Android

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
TMDB_FORCE_IMDB = _parse_bool(os.environ.get("TMDB_FORCE_IMDB", ""), False)
# NZBGeek readiness checks (Newznab API). Optional; set NZBGEEK_APIKEY in Render to enable.
NZBGEEK_APIKEY = os.environ.get("NZBGEEK_APIKEY", "")
NZBGEEK_BASE = os.environ.get("NZBGEEK_BASE", "https://api.nzbgeek.info/api")
NZBGEEK_TIMEOUT = _safe_float(os.environ.get("NZBGEEK_TIMEOUT", "5"), 5.0)
NZBGEEK_TITLE_MATCH_MIN_RATIO = _safe_float(os.environ.get("NZBGEEK_TITLE_MATCH_MIN_RATIO", "0.80"), 0.80)
NZBGEEK_TITLE_FALLBACK = _parse_bool(os.environ.get('NZBGEEK_TITLE_FALLBACK', 'false'))
# Gate NZBGeek readiness calls (can be disabled if you switch to direct playability probing).
USE_NZBGEEK_READY = _parse_bool(os.environ.get("USE_NZBGEEK_READY", "1"), True)

# Direct Usenet playability probe (replaces NZBGeek readiness as "source of truth" when enabled).
# Uses a tiny byte-range GET to distinguish REAL vs STUB proxy links.
USENET_PROBE_ENABLE = _parse_bool(os.environ.get("USENET_PROBE_ENABLE", "0"), False)
USENET_PROBE_TOP_N = _safe_int(os.environ.get("USENET_PROBE_TOP_N", os.environ.get("USENET_PROBE_TOP", "50")), 50)
# Stop early once we have this many REAL usenet links (keeps latency down).
USENET_PROBE_TARGET_REAL = _safe_int(os.environ.get("USENET_PROBE_TARGET_REAL", "12"), 12)
USENET_PROBE_TIMEOUT_S = _safe_float(os.environ.get("USENET_PROBE_TIMEOUT_S", "4.0"), 4.0)
USENET_PROBE_BUDGET_S = _safe_float(os.environ.get("USENET_PROBE_BUDGET_S", "8.5"), 8.5)  # hard wall for /stream latency
USENET_PROBE_DROP_FAILS = _parse_bool(os.environ.get("USENET_PROBE_DROP_FAILS", "1"), True)  # drop probed non-REAL links (STUB/ERR)
USENET_PROBE_REAL_TOP10_PCT = _safe_float(os.environ.get("USENET_PROBE_REAL_TOP10_PCT", "0.5"), 0.5)
USENET_PROBE_REAL_TOP20_N = _safe_int(os.environ.get("USENET_PROBE_REAL_TOP20_N", "20"), 20)
_tmp_verify_retries = _safe_int(os.environ.get("VERIFY_RETRIES", "0"), 0)
USENET_PROBE_RETRIES = _safe_int(os.environ.get("USENET_PROBE_RETRIES", str(_tmp_verify_retries)), _tmp_verify_retries)
USENET_PROBE_CONCURRENCY = _safe_int(os.environ.get("USENET_PROBE_CONCURRENCY", "20"), 20)
USENET_PROBE_RANGE_END = _safe_int(os.environ.get("USENET_PROBE_RANGE_END", "16440"), 16440)
# If we read exactly this many bytes back from the range probe, treat it as a "stub".
USENET_PROBE_STUB_LEN = _safe_int(os.environ.get("USENET_PROBE_STUB_LEN", "16440"), 16440)
# Demote (and optionally drop) stubs aggressively so REAL usenet links float to the top.
USENET_PROBE_DROP_STUBS = _parse_bool(os.environ.get("USENET_PROBE_DROP_STUBS", "1"), True)
USENET_PROBE_MARK_READY = _parse_bool(os.environ.get("USENET_PROBE_MARK_READY", "1"), True)



BUILD_ID = os.environ.get("BUILD_ID", "1.0")
# Additional filters
MIN_SEEDERS = _safe_int(os.environ.get('MIN_SEEDERS', '1'), 1)
PREFERRED_LANG = os.environ.get("PREFERRED_LANG", "EN").upper()
# Premium priorities and verification
PREMIUM_PRIORITY = _safe_csv(os.environ.get('PREMIUM_PRIORITY', 'TB,RD,AD,ND'))
USENET_PRIORITY = _safe_csv(os.environ.get('USENET_PRIORITY', 'ND,EW,NG'))
IPHONE_USENET_ONLY = _parse_bool(os.environ.get("IPHONE_USENET_ONLY", "false"), False)  # env-driven; default off
USENET_PROVIDERS = _safe_csv(os.environ.get("USENET_PROVIDERS", ",".join(USENET_PRIORITY) if USENET_PRIORITY else "ND,EW,NG"))
USENET_SEEDER_BOOST = _safe_int(os.environ.get('USENET_SEEDER_BOOST', '10'), 10)
INSTANT_BOOST_TOP_N = _safe_int(os.environ.get('INSTANT_BOOST_TOP_N', '0'), 0)  # 0=off; set in Render if wanted
DIVERSITY_TOP_M = _safe_int(os.environ.get('DIVERSITY_TOP_M', '0'), 0)  # 0=off; set in Render if wanted
DIVERSITY_POOL_MULT = _safe_int(os.environ.get('DIVERSITY_POOL_MULT', '10'), 10)  # pool = m * mult (lets diversity pull from deeper)
DIVERSITY_THRESHOLD = _safe_float(os.environ.get('DIVERSITY_THRESHOLD', '0.85'), 0.85)  # quality guard for diversity (0.0-1.0)
P2_SRC_BOOST = _safe_int(os.environ.get('P2_SRC_BOOST', '5'), 5)  # slight preference for P2 when diversifying
INPUT_CAP_PER_SOURCE = _safe_int(os.environ.get('INPUT_CAP_PER_SOURCE', '0'), 0)  # 0=off; per-supplier cap if set
DL_ASSOC_PARSE = _parse_bool(os.environ.get('DL_ASSOC_PARSE', 'true'), True)  # default true; set false in Render to disable
VERIFY_PREMIUM = _parse_bool(os.environ.get("VERIFY_PREMIUM", "true"), True)
ASSUME_PREMIUM_ON_FAIL = _parse_bool(os.environ.get("ASSUME_PREMIUM_ON_FAIL", "false"), False)

# TorBox WebDAV — INACTIVE  //✅
# Kept as stubs for future experimentation, but FORCED OFF so it never affects runtime or logic.
WEBDAV_INACTIVE = True  # INACTIVE  //✅
USE_TB_WEBDAV = False   # INACTIVE  //✅ (ignore env)
TB_WEBDAV_URL = os.environ.get('TB_WEBDAV_URL', 'https://webdav.torbox.app')  # INACTIVE  //✅
TB_WEBDAV_USER = os.environ.get('TB_WEBDAV_USER', '')  # INACTIVE  //✅
TB_WEBDAV_PASS = os.environ.get('TB_WEBDAV_PASS', '')  # INACTIVE  //✅
TB_WEBDAV_TIMEOUT = _safe_float(os.environ.get('TB_WEBDAV_TIMEOUT', '1.0'), 1.0)  # INACTIVE  //✅
TB_WEBDAV_WORKERS = _safe_int(os.environ.get('TB_WEBDAV_WORKERS', '10'), 10)  # INACTIVE  //✅
TB_WEBDAV_TEMPLATES = [t.strip() for t in os.environ.get('TB_WEBDAV_TEMPLATES', 'downloads/{hash}/').split(',') if t.strip()]  # INACTIVE  //✅
TB_WEBDAV_STRICT = False  # INACTIVE  //✅ (ignore env)

VERIFY_CACHED_ONLY = _parse_bool(os.environ.get("VERIFY_CACHED_ONLY", "false"), False)
STRICT_PREMIUM_ONLY = _parse_bool(os.environ.get('STRICT_PREMIUM_ONLY', 'false'), False)  # loose default; strict drops uncached
MIN_CACHE_CONFIDENCE = _safe_float(os.environ.get('MIN_CACHE_CONFIDENCE', '0.8'), 0.8)

# Cancelled RD/AD instant checks – removed functions, now heuristics only
# (No RD_STRICT_CACHE_CHECK, RD_API_KEY, AD_STRICT_CACHE_CHECK, AD_API_KEY)

# Limit strict cache checks per request to avoid excessive API churn

# --- Add-ons / extensions (optional) ---
DROP_RD = _parse_bool(os.environ.get("DROP_RD", "false"), False)
DROP_AD = _parse_bool(os.environ.get("DROP_AD", "false"), False)

MIN_RES = max(_safe_int(os.environ.get('MIN_RES', '1080'), 1080), 1080)  # hard floor: never below 1080
MAX_AGE_DAYS = _safe_int(os.environ.get('MAX_AGE_DAYS', '0'), 0)  # 0 = off
USE_AGE_HEURISTIC = _parse_bool(os.environ.get("USE_AGE_HEURISTIC", "true"), True)

ADD_CACHE_HINT = _parse_bool(os.environ.get("ADD_CACHE_HINT", "true"), True)

# TorBox strict cache filtering (different from TB_CACHE_HINTS which only adds a hint)
VERIFY_TB_CACHE_OFF = _parse_bool(os.environ.get("VERIFY_TB_CACHE_OFF", "false"), False)

# Wrapper behavior toggles

# Use short opaque /r/<token> urls instead of base64-encoding the full upstream URL.
# Fixes Android/Google TV URL-length limits and keeps playback URLs private.
WRAP_URL_SHORT = _parse_bool(os.environ.get("WRAP_URL_SHORT", "true"), True)
WRAP_URL_TTL = _safe_int(os.environ.get('WRAP_URL_TTL', '3600'), 3600)  # seconds
WRAP_HEAD_MODE = (os.environ.get("WRAP_HEAD_MODE", "200_noloc") or "200_noloc").strip().lower()
RANGE_PROBE_GUARD = _parse_bool(os.environ.get("RANGE_PROBE_GUARD", "true"), True)
RANGE_PROBE_MAX_BYTES = _safe_int(os.environ.get("RANGE_PROBE_MAX_BYTES", "1024"), 1024)
WRAPPER_DEDUP = _parse_bool(os.environ.get("WRAPPER_DEDUP", "true"), True)

VERIFY_STREAM = _parse_bool(os.environ.get("VERIFY_STREAM", "false"), False)  # env-driven; default off
VERIFY_STREAM_TIMEOUT = _safe_float(os.environ.get('VERIFY_STREAM_TIMEOUT', '4'), 4.0)

# Probe/verify tuning knobs (shared)
VERIFY_RETRIES = _safe_int(os.environ.get("VERIFY_RETRIES", "0"), 0)  # 0 for fast/no-retry
VERIFY_MAX_WORKERS = _safe_int(os.environ.get("VERIFY_MAX_WORKERS", "32"), 32)
VERIFY_SNIFF_BYTES = _safe_int(os.environ.get("VERIFY_SNIFF_BYTES", "64"), 64)

# Extra verify controls (optional; used for placeholder/stub protection)
VERIFY_DROP_STUBS = _parse_bool(os.environ.get("VERIFY_DROP_STUBS", "0"), False)
VERIFY_STUB_MAX_BYTES_RAW = (os.environ.get("VERIFY_STUB_MAX_BYTES", "") or "").strip()
VERIFY_STUB_MAX_BYTES = _safe_int(VERIFY_STUB_MAX_BYTES_RAW or "16384", 16384)

# Stronger playback verification (catches upstream /static/500.mp4 placeholders)
VERIFY_RANGE = _parse_bool(os.environ.get("VERIFY_RANGE", "true"), True)
ANDROID_VERIFY_TOP_N = _safe_int(os.environ.get('ANDROID_VERIFY_TOP_N', '6'), 6)
VERIFY_DESKTOP_TOP_N = _safe_int(os.environ.get('VERIFY_DESKTOP_TOP_N', '20'), 20)
ANDROID_VERIFY_TIMEOUT = _safe_float(os.environ.get('ANDROID_VERIFY_TIMEOUT', '3.0'), 3.0)
ANDROID_VERIFY_OFF = _parse_bool(os.environ.get("ANDROID_VERIFY_OFF", "false"), False)


# Force a minimum share of usenet results (if they exist)
MIN_USENET_KEEP = _safe_int(os.environ.get('MIN_USENET_KEEP', '0'), 0)
MIN_USENET_DELIVER = _safe_int(os.environ.get('MIN_USENET_DELIVER', '0'), 0)
MIN_TB_DELIVER = _safe_int(os.environ.get('MIN_TB_DELIVER', '0'), 0)
MIN_RD_DELIVER = _safe_int(os.environ.get('MIN_RD_DELIVER', '0'), 0)

# Optional local/remote filtering sources
USE_BLACKLISTS = _parse_bool(os.environ.get("USE_BLACKLISTS", "true"), True)
BLACKLIST_TERMS = [t.strip().lower() for t in os.environ.get("BLACKLIST_TERMS", "").split(",") if t.strip()]
BLACKLIST_URL = os.environ.get("BLACKLIST_URL", "")
USE_FAKES_DB = _parse_bool(os.environ.get("USE_FAKES_DB", "true"), True)
FAKES_DB_URL = os.environ.get("FAKES_DB_URL", "")

# Optional: simple rate-limit (e.g. "30/m", "5/s"). Blank disables it.
RATE_LIMIT = (os.environ.get("RATE_LIMIT", "") or "").strip()
# Validate critical env (makes missing config obvious in Render logs)
if not AIO_BASE:
    logger.error("Missing AIO_BASE - app will have no streams")
if not TB_API_KEY:
    logger.warning("Missing TB_API_KEY - TB features disabled")
if VERIFY_DROP_STUBS and not VERIFY_STUB_MAX_BYTES_RAW:
    logger.warning(
        "VERIFY_DROP_STUBS enabled but no VERIFY_STUB_MAX_BYTES - using default %s",
        VERIFY_STUB_MAX_BYTES,
    )



# ---------- FETCH EXECUTOR + AIO CACHE ----------
WRAP_FETCH_WORKERS = int(os.getenv("WRAP_FETCH_WORKERS") or os.getenv("FETCH_WORKERS") or "8")
FETCH_EXECUTOR = ThreadPoolExecutor(max_workers=WRAP_FETCH_WORKERS)

AIO_CACHE_TTL_S = int(os.getenv("AIO_CACHE_TTL_S", "600") or 600)   # 0 disables cache
AIO_CACHE_MAX = int(os.getenv("AIO_CACHE_MAX", "200") or 200)
AIO_CACHE_MODE = (os.getenv("AIO_CACHE_MODE", "off") or "off").lower()  # off|swr|soft
AIO_SOFT_TIMEOUT_S = float(os.getenv("AIO_SOFT_TIMEOUT_S", "0") or 0)   # only used in 'soft' mode


_AIO_CACHE = {}  # key -> (ts_monotonic, streams, count, ms)
_AIO_CACHE_LOCK = threading.Lock()

def _aio_cache_get(key: str):
    if AIO_CACHE_TTL_S <= 0:
        return None
    now = time.monotonic()
    with _AIO_CACHE_LOCK:
        v = _AIO_CACHE.get(key)
        if not v:
            return None
        # Backward compatible with older 3-tuple cache entries
        try:
            ts, streams, count, ms = v
        except Exception:
            try:
                ts, streams, count = v
                ms = 0
            except Exception:
                return None
        if (now - ts) > AIO_CACHE_TTL_S:
            _AIO_CACHE.pop(key, None)
            return None
        return streams, count, int(ms or 0)

def _aio_cache_set(key: str, streams: list, count: int, ms: int = 0):
    if AIO_CACHE_TTL_S <= 0:
        return
    now = time.monotonic()
    with _AIO_CACHE_LOCK:
        _AIO_CACHE[key] = (now, streams, count, int(ms or 0))
        # evict oldest entries if over cap
        while len(_AIO_CACHE) > AIO_CACHE_MAX:
            oldest = next(iter(_AIO_CACHE))
            _AIO_CACHE.pop(oldest, None)


def _make_aio_cache_update_cb(aio_key: str):
    """Return a Future callback that updates the AIO cache when the fetch completes."""
    def _cb(fut):
        try:
            s, cnt, _ms, _meta, _local_ms = fut.result()
            if s:
                _aio_cache_set(aio_key, s, cnt, int(_ms or 0))
        except Exception:
            pass
    return _cb

def _aio_cache_key(type_: str, id_: str, extras) -> str:
    # extras can be dict or None; keep stable key
    if not extras:
        return f"{type_}:{id_}"
    try:
        return f"{type_}:{id_}:{json.dumps(extras, sort_keys=True, separators=(',',':'))}"
    except Exception:
        return f"{type_}:{id_}:{str(extras)}"


# ---------------------------
# Helpers (used by the pipeline)
# ---------------------------
_blacklist_cache = {"ts": 0.0, "terms": set()}
_fakes_cache = {"ts": 0.0, "hashes": set()}
_BLACKLIST_LOCK = threading.Lock()
_FAKES_LOCK = threading.Lock()


# --- infohash normalization ---
_INFOHASH_HEX_RE = re.compile(r"(?i)\b[0-9a-f]{40}\b")
_INFOHASH_B32_RE = re.compile(r"(?i)\b[a-z2-7]{32}\b")

def norm_infohash(raw: Any) -> str:
    """Normalize many infohash/id forms into lowercase 40-hex when possible."""
    if raw is None:
        return ""
    s = str(raw).strip().lower()
    if not s:
        return ""
    # Common prefixes / encodings (we only need the underlying hash).
    for p in ("urn:btih:", "btih:", "ih:", "infohash:", "hash:"):
        if s.startswith(p):
            s = s[len(p):]
            break
    # URL-encoded variants (best-effort).
    s = s.replace("urn%3abtih%3a", "").replace("btih%3a", "").replace("ih%3a", "")

    m = _INFOHASH_HEX_RE.search(s)
    if m:
        return m.group(0).lower()

    # Base32 infohash (32 chars) -> hex (40 chars).
    m2 = _INFOHASH_B32_RE.fullmatch(s)
    if m2:
        try:
            return base64.b32decode(s.upper()).hex().lower()
        except Exception:
            return s

    return s
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




# --- Title normalization for mismatch checks (used by TRAKT/TMDB validation) ---
# Keeps compare strings stable by stripping common release noise (resolution, codec, source, audio, groups, etc.)
_TITLE_NOISE_RE = re.compile(
    r"""(?ix)
    \b(
        s\d{1,2}e\d{1,2} | \d{1,2}x\d{1,2} | episode\s*\d{1,3} |
        480p|576p|720p|1080p|1440p|2160p|4320p|4k|8k|
        hdr10\+?|hdr|dv|dovi|dolby\s*vision|sdr|
        bluray|blu[-\s]?ray|bdrip|bdremux|remux|web[-\s]?dl|webrip|hdtv|dvdrip|
        x264|x265|h\.264|h\.265|hevc|avc|vp9|av1|
        aac|ac3|eac3|ddp|dts(?:-hd)?|truehd|atmos|opus|flac|mp3|
        2\.0|5\.1|7\.1|stereo|
        multi|dual\s*audio|dub(?:bed)?|sub(?:bed)?|eng|english|
        proper|repack|internal|extended|unrated|limited|imax|
        (?:19|20)\d{2} |
        mkv|mp4|avi
    )\b
    """
)


def clean_title_for_compare(title: str) -> str:
    """Normalize a title string for fuzzy matching (difflib)."""
    if not title:
        return ""
    t = str(title)
    t = t.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    t = t.lower()
    # Drop bracketed bits: [..] (..)
    t = re.sub(r"\[[^\]]*\]", " ", t)
    t = re.sub(r"\([^\)]*\)", " ", t)
    # Convert punctuation/separators to spaces
    t = re.sub(r"[\._\-/]+", " ", t)
    # Remove known release noise tokens
    t = _TITLE_NOISE_RE.sub(" ", t)
    # Keep only letters/digits/spaces
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    return t


# Words that are common in "quality-only" strings and shouldn't be treated as real titles.
_QUALITY_ONLY_WORDS = {
    # containers / sources
    "web", "dl", "webrip", "webdl", "hdrip", "bdrip", "dvdrip",
    "bluray", "blu", "ray", "remux",
    # codecs / formats
    "x264", "x265", "h264", "h265", "hevc", "avc", "aac", "ac3", "eac3", "dts", "truehd",
    "hdr", "sdr", "dv", "dolby", "vision", "atmos",
    # misc release words
    "proper", "repack", "rerip", "internal", "limited", "uncut", "extended",
    "dub", "dubs", "sub", "subs", "multisub", "multi", "dual", "audio",
    # generic
    "rip", "encode", "reencode",
}


# --- Patch 2 (A/B/C): improved title normalization + scoring for title-mismatch filter ---
# Step A: normalize titles hard (strip bracket noise, years, separators, release junk)
# Step B: score using token containment + token Jaccard + fuzzy fallback
# Step C: compare against TMDB aliases (title + original title/name) when available

_JUNK_TOKENS = {
    # resolution / general
    "2160p", "1080p", "720p", "480p", "576p", "1440p", "4320p", "4k", "8k",
    # hdr / formats
    "hdr10", "hdr10+", "hdr", "sdr", "dv", "dovi", "dolby", "vision",
    # sources / containers
    "web", "dl", "webrip", "webdl", "web-dl", "bluray", "brrip", "bdrip", "remux", "hdrip",
    # codecs
    "x264", "x265", "h264", "h265", "hevc", "avc", "vp9", "av1",
    # audio
    "aac", "ac3", "eac3", "ddp", "dd", "atmos", "dts", "truehd", "opus", "flac",
    # subs / language / services
    "multi", "dual", "subs", "sub", "esub", "dub", "dublado", "ita", "eng", "kor",
    "nf", "netflix", "amzn", "amazon", "hulu", "disney", "max",
    # misc release words
    "proper", "repack", "internal", "limited", "extended", "unrated", "imax",
    # file extensions (also removed earlier but keep here too)
    "mkv", "mp4", "avi", "m4v",
}

def _strip_brackets(s: str) -> str:
    # remove 【...】 and (...) and [...]
    s = re.sub(r"【[^】]*】", " ", s or "")
    s = re.sub(r"\[[^\]]*\]", " ", s)
    s = re.sub(r"\([^)]*\)", " ", s)
    return s

def norm_title(s: str) -> str:
    """Normalize a candidate/expected title for mismatch scoring."""
    if not s:
        return ""
    s = str(s).lower()
    s = _strip_brackets(s)
    s = re.sub(r"\.(mkv|mp4|avi|m4v)$", "", s)      # file ext at end
    s = re.sub(r"[\._\-:/]+", " ", s)              # separators & colon -> space
    s = re.sub(r"\b(19|20)\d{2}\b", " ", s)       # years
    s = re.sub(r"[^a-z0-9 ]+", " ", s)               # drop non-alnum
    toks = [t for t in s.split() if t and t not in _JUNK_TOKENS]
    return " ".join(toks)

def title_score(cand: str, expected: str) -> float:
    """Score in [0,1] for title-mismatch matching (robust to extra junk words)."""
    try:
        c = norm_title(cand)
        e = norm_title(expected)
        if not c or not e:
            return 0.0

        # If expected appears as a whole-word token sequence, accept strongly.
        if f" {e} " in f" {c} ":
            return 1.0

        cset = set(c.split())
        eset = set(e.split())
        if not cset or not eset:
            return 0.0

        # Token Jaccard (robust to extra junk words)
        jacc = len(cset & eset) / len(cset | eset)

        # Fuzzy fallback on normalized strings
        seq = difflib.SequenceMatcher(None, c, e).ratio()

        return float(max(jacc, seq))
    except Exception:
        return 0.0



def _similarity_ratio(a, b):
    """Similarity ratio in [0,1] for already-normalized compare strings.

    This helper MUST be exception-safe: title-mismatch logic should never crash the stream pipeline.
    """
    try:
        if not a or not b:
            return 0.0
        a2 = " ".join(str(a).split())
        b2 = " ".join(str(b).split())
        return difflib.SequenceMatcher(None, a2, b2).ratio()
    except Exception:
        return 0.0

def _is_quality_only_title(t: str) -> bool:
    """True if string looks like only quality/tech metadata (no real title words)."""
    if not t:
        return True
    tl = t.lower()

    # If there are non-latin letters and no latin letters, treat as NOT quality-only
    # (we'll run mismatch logic, usually dropping it).
    if re.search(r"[a-z]", tl) is None and re.search(r"[^\W\d_]", tl, flags=re.UNICODE):
        return False

    words = re.findall(r"[a-z]{3,}", tl)
    meaningful = [w for w in words if w not in _QUALITY_ONLY_WORDS]
    return len(meaningful) == 0
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
    '''Extract an infohash/id token (32–40 hex) from common patterns like IH:<hash>, infohash=<hash>, btih:<hash>.'''
    if not text:
        return None
    m = re.search(r"(?:ih|infohash|btih)\s*[:=]\s*([0-9a-fA-F]{32,40})", text, flags=re.I)
    if m:
        return m.group(1).lower()
    # Some providers embed the hash without a label; accept only if it appears as a standalone token.
    m = re.search(r"\b([0-9a-fA-F]{32,40})\b", text)
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

def _parse_http_range(rng: str):
    """
    Parse a simple single-range header like 'bytes=0-0' or 'bytes=0-1023'.
    Returns (start, end_or_None). If unparsable, returns (None, None).
    """
    if not rng:
        return (None, None)
    m = re.match(r"^bytes=(\d+)-(\d*)$", (rng or "").strip())
    if not m:
        return (None, None)
    start = int(m.group(1))
    end_s = m.group(2)
    end = int(end_s) if end_s != "" else None
    return (start, end)



def _verify_stream_url(
    url: str,
    session: Optional["requests.Session"] = None,
    *,
    timeout_s: Optional[float] = None,
    range_mode: Optional[bool] = None,
    req_headers: Optional[Dict[str, str]] = None,
) -> Tuple[bool, int, str]:
    """
    Verify an upstream stream URL is plausibly playable and safe to probe.

    Returns: (keep, penalty, classification)
      - keep False => drop (unsafe / not playable / text error / probe unsafe)
      - keep True  => keep, possibly with penalty (risky / stub / weak signals)

    Implements the "7 rules" sanity layer with conservative, low-byte probing.
    """
    # Defaults / knobs
    sniff_bytes = max(256, _safe_int(os.environ.get("VERIFY_SNIFF_BYTES", "2048"), 2048))
    leak_limit = max(1024, _safe_int(os.environ.get("LEAK_GUARD_BYTES", "8192"), 8192))
    max_redirects = max(0, _safe_int(os.environ.get("VERIFY_MAX_REDIRECTS", "4"), 4))
    pen_risky = _safe_int(os.environ.get("VERIFY_PEN_RISKY", "20"), 20)
    pen_stub = _safe_int(os.environ.get("VERIFY_PEN_STUB", "120"), 120)
    pen_atoms_bad = _safe_int(os.environ.get("VERIFY_PEN_MP4_ATOMS_BAD", "60"), 60)

    # STUB threshold (bytes). Small default keeps verification fast.
    stub_max = VERIFY_STUB_MAX_BYTES
    drop_stubs = VERIFY_DROP_STUBS

    risky_servers = [
        t.strip().lower()
        for t in str(os.environ.get("VERIFY_RISKY_SERVERS", "lity,nexus")).split(",")
        if t.strip()
    ]
    risky_cts = {"application/octet-stream", "application/force-download"}

    # Resolve short /r token if needed
    if not url:
        return (False, 0, "EMPTY_URL")
    if "/r/" in url:
        u2 = _unwrap_short_url(url)
        if u2:
            url = u2

    # Skip non-http(s)
    if not (url.startswith("http://") or url.startswith("https://")):
        return (True, 0, "SKIP_NONHTTP")

    # Host cache (avoid repeated probes)
    try:
        from urllib.parse import urlparse
        h0 = (urlparse(url).netloc or "").split("@")[-1]
        h0 = h0.split(":")[0].lower().strip()
    except Exception:
        h0 = ""
    cached = _verify_host_cache_get(h0) if h0 else None
    if cached:
        level, reason = cached
        if level == "unsafe":
            return (False, 0, "HOST_CACHED_UNSAFE")
        if level == "risky":
            return (True, pen_risky, "HOST_CACHED_RISKY")

    prefer_range = True if range_mode is None else bool(range_mode)
    if timeout_s is None:
        timeout_s = float(os.environ.get("VERIFY_TIMEOUT_S", "6.0") or 6.0)
    timeout = (min(3.0, timeout_s), timeout_s)  # connect, read

    sess = session or requests.Session()

    # Base headers
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Connection": "close",
    }
    if req_headers:
        headers.update({k: v for k, v in req_headers.items() if k and v})

    def _parse_int(hv: Optional[str]) -> Optional[int]:
        try:
            if hv is None:
                return None
            hv = hv.strip()
            if not hv:
                return None
            return int(hv)
        except Exception:
            return None

    def _parse_content_range(cr: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        # "bytes 0-0/12345" or "bytes 0-2047/12345"
        try:
            cr = (cr or "").strip().lower()
            if not cr.startswith("bytes"):
                return (None, None, None)
            _, rest = cr.split(" ", 1)
            span, total = rest.split("/", 1)
            if "-" in span:
                a, b = span.split("-", 1)
                start = int(a)
                end = int(b)
            else:
                return (None, None, None)
            total_i = None if total == "*" else int(total)
            return (start, end, total_i)
        except Exception:
            return (None, None, None)

    def _looks_texty(ct: str) -> bool:
        ct = (ct or "").split(";", 1)[0].strip().lower()
        if ct.startswith("text/"):
            return True
        if ct in {"application/json", "application/xml"}:
            return True
        if ct in {"text/html", "application/xhtml+xml"}:
            return True
        return False

    def _sig_classify(buf: bytes) -> str:
        """Return best-effort signature label for first bytes."""
        if not buf:
            return ""
        b0 = buf[:16].lstrip()
        if b0.startswith(b"{") or b0.startswith(b"["):
            return "JSON"
        if b0.startswith(b"<") or b"<html" in buf[:256].lower() or b"<!doctype" in buf[:256].lower():
            return "HTML"
        # MKV EBML magic
        if buf[:4] == b"\x1a\x45\xdf\xa3":
            return "MKV"
        # MP4-ish atoms
        if b"ftyp" in buf[:1024] or b"moov" in buf[:2048] or b"moof" in buf[:2048]:
            return "MP4"
        return ""

    def _mp4_atoms_ok(buf: bytes) -> Tuple[bool, bool, bool]:
        """Return (mp4ish, has_ftyp, has_moov_or_moof)."""
        if not buf:
            return (False, False, False)
        low = buf[:4096]
        has_ftyp = b"ftyp" in low[:1024]
        has_moov = b"moov" in low
        has_moof = b"moof" in low
        mp4ish = has_ftyp or has_moov or has_moof
        return (mp4ish, has_ftyp, (has_moov or has_moof))

    # --- Probe step 1: Range 0-0 (safest) ---
    cur = url
    visited = set()
    last_headers: Dict[str, str] = {}
    last_status: Optional[int] = None
    final_url = None
    got0 = b""
    total_size: Optional[int] = None
    server_hdr = ""
    ct = ""

    def _do_get_range(cur_url: str, range_header: str) -> "requests.Response":
        hh = dict(headers)
        if prefer_range:
            hh["Range"] = range_header
        return sess.get(cur_url, headers=hh, timeout=timeout, stream=True, allow_redirects=False)

    for hop in range(max_redirects + 1):
        if cur in visited:
            return (False, 0, "REDIRECT_LOOP")
        visited.add(cur)
        try:
            r = _do_get_range(cur, "bytes=0-0")
        except Exception as e:
            return (False, 0, "VERIFY_GET_ERR")
        last_status = r.status_code
        last_headers = {k: v for k, v in (r.headers or {}).items()}

        # Redirect handling
        if last_status in (301, 302, 303, 307, 308):
            loc = (r.headers or {}).get("Location")
            if not loc:
                return (False, 0, "REDIRECT_NO_LOCATION")
            try:
                from urllib.parse import urljoin
                cur = urljoin(cur, loc)
                continue
            except Exception:
                cur = loc
                continue

        # Final response
        final_url = cur
        ct = (r.headers or {}).get("Content-Type", "") or ""
        server_hdr = (r.headers or {}).get("Server", "") or ""
        cr = (r.headers or {}).get("Content-Range", "") or ""
        cl = _parse_int((r.headers or {}).get("Content-Length"))
        s0, e0, t0 = _parse_content_range(cr)
        if t0 is not None:
            total_size = t0
        elif cl is not None:
            total_size = cl

        # PROBE_UNSAFE (2b): 206 + 0-0 but content-length not 1
        if last_status == 206 and s0 == 0 and e0 == 0 and cl is not None and cl not in (0, 1):
            if h0:
                _verify_host_cache_set(h0, "unsafe", "PROBE_UNSAFE_CL_MISMATCH")
            return (False, 0, "PROBE_UNSAFE_CL_MISMATCH")

        # Read tiny amount and detect leak
        downloaded = 0
        try:
            for chunk in r.iter_content(chunk_size=1024):
                if not chunk:
                    continue
                got0 += chunk
                downloaded += len(chunk)
                if downloaded > leak_limit:
                    if h0:
                        _verify_host_cache_set(h0, "unsafe", "PROBE_UNSAFE_LEAK")
                    return (False, 0, "PROBE_UNSAFE_LEAK")
                if len(got0) >= 8:  # enough to spot obvious text/html/json markers
                    break
        except Exception:
            # Ignore read error; continue with what we got
            pass

        # If server ignored Range (status 200 on range request), treat unsafe if it looks big or leaky.
        if prefer_range and last_status == 200:
            # If it already leaked beyond a few KB, mark unsafe.
            if len(got0) > 1024 or (cl is not None and cl > sniff_bytes):
                if h0:
                    _verify_host_cache_set(h0, "unsafe", "PROBE_UNSAFE_RANGE_IGNORED")
                return (False, 0, "PROBE_UNSAFE_RANGE_IGNORED")

        break

    if final_url is None or last_status is None:
        return (False, 0, "VERIFY_NO_RESPONSE")

    if last_status not in (200, 206):
        if h0:
            _verify_host_cache_set(h0, "unsafe", "FINAL_NOT_PLAYABLE")
        return (False, 0, "FINAL_NOT_PLAYABLE")

    # Quick reject: obvious upstream error text types or signatures.
    sig0 = _sig_classify(got0)
    if _looks_texty(ct) or sig0 in {"HTML", "JSON"}:
        if h0:
            _verify_host_cache_set(h0, "unsafe", "UPSTREAM_ERROR_TEXT")
        return (False, 0, "UPSTREAM_ERROR_TEXT")

    # --- Probe step 2: read first N bytes for signatures / MP4 atoms (still guarded) ---
    got = got0
    if prefer_range and len(got) < min(256, sniff_bytes):
        try:
            # Re-probe with a slightly wider range for signature checks.
            r2 = _do_get_range(final_url, f"bytes=0-{sniff_bytes-1}")
            sc2 = r2.status_code
            cr2 = (r2.headers or {}).get("Content-Range", "") or ""
            cl2 = _parse_int((r2.headers or {}).get("Content-Length"))
            s2, e2, t2 = _parse_content_range(cr2)
            if t2 is not None:
                total_size = t2

            # PROBE_UNSAFE (2b): 206 + 0-0/total but Content-Length huge (some servers lie)
            if sc2 == 206 and s2 == 0 and e2 == 0 and cl2 is not None and cl2 not in (0, 1):
                if h0:
                    _verify_host_cache_set(h0, "unsafe", "PROBE_UNSAFE_CL_MISMATCH")
                return (False, 0, "PROBE_UNSAFE_CL_MISMATCH")

            # Read up to sniff_bytes but hard stop on leak_limit
            downloaded = 0
            got = b""
            for chunk in r2.iter_content(chunk_size=4096):
                if not chunk:
                    continue
                got += chunk
                downloaded += len(chunk)
                if downloaded > leak_limit:
                    if h0:
                        _verify_host_cache_set(h0, "unsafe", "PROBE_UNSAFE_LEAK")
                    return (False, 0, "PROBE_UNSAFE_LEAK")
                if len(got) >= sniff_bytes:
                    break

            # If range ignored, treat unsafe.
            if prefer_range and sc2 == 200:
                if downloaded > 1024 or (cl2 is not None and cl2 > sniff_bytes):
                    if h0:
                        _verify_host_cache_set(h0, "unsafe", "PROBE_UNSAFE_RANGE_IGNORED")
                    return (False, 0, "PROBE_UNSAFE_RANGE_IGNORED")

            # Header mismatch: Content-Length much bigger than Content-Range span
            if sc2 == 206 and s2 is not None and e2 is not None and cl2 is not None:
                span = (e2 - s2 + 1)
                if cl2 > (span + 1024):
                    if h0:
                        _verify_host_cache_set(h0, "unsafe", "PROBE_UNSAFE_CL_MISMATCH")
                    return (False, 0, "PROBE_UNSAFE_CL_MISMATCH")

            # Re-check text signatures using richer bytes.
            sig = _sig_classify(got)
            if _looks_texty(ct) or sig in {"HTML", "JSON"}:
                if h0:
                    _verify_host_cache_set(h0, "unsafe", "UPSTREAM_ERROR_TEXT")
                return (False, 0, "UPSTREAM_ERROR_TEXT")
        except Exception:
            # If sniff probe fails, don't drop; fall back to weak confidence.
            got = got0

    # Determine kind from headers/signatures
    ct_low = (ct or "").split(";", 1)[0].strip().lower()
    sig = _sig_classify(got)
    kind = ""
    if "video/mp4" in ct_low:
        kind = "MP4"
    elif "matroska" in ct_low or "x-matroska" in ct_low or "video/webm" in ct_low:
        kind = "MKV"
    elif sig in {"MP4", "MKV"}:
        kind = sig

    # Content signature mismatch (7a)
    if kind and sig and kind != sig and sig in {"MP4", "MKV", "HTML", "JSON"}:
        if h0:
            _verify_host_cache_set(h0, "unsafe", "CT_SIGNATURE_MISMATCH")
        return (False, 0, "CT_SIGNATURE_MISMATCH")

    # MP4 atoms sanity (7b)
    mp4ish, has_ftyp, has_moov_or_moof = _mp4_atoms_ok(got)
    if kind == "MP4" or mp4ish:
        if not has_ftyp or not has_moov_or_moof:
            # Penalize (do not drop) – some mp4s have moov late, but placeholders often fail this.
            return (True, pen_atoms_bad, "MP4_ATOMS_BAD")

    # STUBS (tiny mp4 total) (1)
    if stub_max and total_size is not None and total_size <= stub_max:
        if kind == "MP4" or mp4ish:
            if drop_stubs:
                return (False, 0, "STUB_MP4_TINY_TOTAL")
            return (True, pen_stub, "STUB_MP4_TINY_TOTAL")

    # RISKY CT / SERVER (4)
    srv_low = (server_hdr or "").lower()
    is_server_risky = any(tok in srv_low for tok in risky_servers) if risky_servers else False
    is_ct_risky = ct_low in risky_cts
    if (is_server_risky or is_ct_risky) and (total_size is None or total_size > stub_max):
        # Slight penalty; keep visible but don't let it beat clean sources.
        if h0:
            _verify_host_cache_set(h0, "risky", "RISKY_CT_OR_SERVER", ttl_s=_safe_int(os.environ.get("VERIFY_HOST_CACHE_TTL_RISKY", "300"), 300))
        return (True, pen_risky, "RISKY_CT_OR_SERVER")

    return (True, 0, "OK")


# ---------------------------
# Usenet playability probe (proxy-range heuristic)
# ---------------------------

_VERIFY_TLS = threading.local()

def _tls_requests_session() -> requests.Session:
    """Thread-local requests.Session (safe for ThreadPoolExecutor)."""
    s = getattr(_VERIFY_TLS, "sess", None)
    if s is None:
        s = requests.Session()
        setattr(_VERIFY_TLS, "sess", s)
    return s

def _basic_auth_header(userpass: str) -> Dict[str, str]:
    """Build a Basic Authorization header from 'user:pass'."""
    try:
        up = (userpass or "").strip()
        if not up or ":" not in up:
            return {}
        tok = base64.b64encode(up.encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {tok}"}
    except Exception:
        return {}

def _probe_auth_headers_for_url(u: str) -> Dict[str, str]:
    """Attach Basic auth for AIOStreams proxy hosts when needed (AIO_BASE / PROV2_BASE)."""
    try:
        from urllib.parse import urlparse
        netloc = (urlparse(u).netloc or "").lower().split("@")[-1]
        if not netloc:
            return {}
        aio_host = (urlparse(AIO_BASE).netloc or "").lower().split("@")[-1]
        p2_host = (urlparse(PROV2_BASE).netloc or "").lower().split("@")[-1]
        if aio_host and netloc == aio_host and AIO_AUTH:
            return _basic_auth_header(AIO_AUTH)
        if p2_host and netloc == p2_host and PROV2_AUTH:
            return _basic_auth_header(PROV2_AUTH)
    except Exception:
        pass
    return {}


def shannon_entropy(data: bytes) -> float:
    """Shannon entropy in bits/byte (0..8) for small byte buffers.

    Very low entropy (roughly <~3.0) often indicates structured stubs (HTML/JSON error pages, proxy placeholders),
    while higher entropy tends to look more like real media bytes.
    """
    try:
        if not data:
            return 0.0
        buf = data[:65536]
        counts = [0] * 256
        for b in buf:
            counts[b] += 1
        n = float(len(buf))
        import math
        ent = 0.0
        for cnt in counts:
            if cnt:
                p = float(cnt) / n
                ent -= p * math.log(p, 2)
        return float(ent)
    except Exception:
        return 0.0

def sniff_magic(data: bytes) -> str:
    """Very cheap content sniffing for proxy stubs (HTML/JSON/XML-ish vs unknown)."""
    try:
        if not data:
            return "unknown"
        head = data[:512].lstrip()
        lo = head.lower()
        if lo.startswith(b"<!doctype html") or lo.startswith(b"<html") or b"<html" in lo[:128]:
            return "html"
        if lo.startswith(b"{") or lo.startswith(b"["):
            return "json"
        if lo.startswith(b"<?xml"):
            return "xml/htmlish"
        if lo.startswith(b"<"):
            # Common for HTML error pages / nginx / cloudflare etc.
            return "xml/htmlish"
        return "unknown"
    except Exception:
        return "unknown"

def _usenet_range_probe_is_real(url: str, *, timeout_s: float, range_end: int, stub_len: int, retries: int, deadline_ts: Optional[float] = None) -> Tuple[bool, str, int]:
    """Return (is_real, reason, bytes_read).

    Heuristic: request Range bytes=0-range_end and read up to (range_end+1) bytes.
    If we read exactly `stub_len` bytes, treat as STUB. If we read exactly (range_end+1), treat as REAL.
    Anything else is treated as not-real (short read, range ignored, auth, etc.).
    """
    u = url or ""
    try:
        # If caller passed a short /r/<token> URL, unwrap to upstream.
        if "/r/" in u:
            long_u = _unwrap_short_url(u)
            if long_u:
                u = long_u
    except Exception:
        pass

    expected = int(range_end) + 1
    ua = globals().get("VERIFY_UA", "Mozilla/5.0")
    base_headers = {"User-Agent": ua, "Range": f"bytes=0-{int(range_end)}", "Accept": "*/*"}
    auth_h = _probe_auth_headers_for_url(u)
    if auth_h:
        base_headers.update(auth_h)

    last_err = ""
    last_bytes = 0

    # No retry loop for fast (retries=0 via env)
    # Enforce an overall deadline so the probe can't blow past the /stream latency budget.
    if deadline_ts is not None:
        rem = float(deadline_ts) - float(time.monotonic())
        if rem <= 0.15:
            return (False, "BUDGET", int(last_bytes or 0))
        # Cap per-request timeout by remaining time (and keep a small minimum so connect doesn't instantly fail).
        timeout_eff = max(0.35, min(float(timeout_s), rem))
    else:
        timeout_eff = float(timeout_s)

    try:
        sess = _tls_requests_session()
        r = sess.get(u, headers=base_headers, timeout=float(timeout_eff), allow_redirects=True, stream=True)
        try:
            st = int(getattr(r, "status_code", 0) or 0)
        except Exception:
            st = 0
        if st in (401, 403):
            try:
                r.close()
            except Exception:
                pass
            return (False, "AUTH", 0)
        if st < 200 or st >= 400:
            last_err = f"HTTP_{st}"
            try:
                r.close()
            except Exception:
                pass
            return (False, last_err, 0)

        ct = (r.headers.get("Content-Type") or "").lower()
        if ct.startswith("text/") or ("json" in ct) or ("xml" in ct) or ("html" in ct):
            try:
                r.close()
            except Exception:
                pass
            return (False, "CT_TEXT", 0)

        # Read up to expected bytes (and stop). Guard against servers ignoring Range.
        total = 0
        max_allow = expected + 2  # tiny slack
        body = b""
        for chunk in r.iter_content(chunk_size=4096):
            if not chunk:
                continue
            body += chunk
            total += len(chunk)
            if total >= expected:
                break
            if total > max_allow:
                break

        try:
            r.close()
        except Exception:
            pass

        last_bytes = int(total)

        # Fast probe checks (entropy/magic for stubs)
        magic = sniff_magic(body)
        ent = shannon_entropy(body)
        if total <= stub_len:  # Tiny/stub size
            return (False, "STUB_LEN", int(total))
        if magic in ("html", "json", "xml/htmlish"):  # Bad content
            return (False, f"BAD_MAGIC_{magic}", int(total))
        if ent < 3.0:  # Low entropy stub
            return (False, f"LOW_ENT_{ent:.2f}", int(total))
        if total == expected and (magic != "unknown" or ent >= 4.5):
            return (True, "REAL", int(total))
        if total > expected:
            return (False, "RANGE_IGNORED", int(total))
        return (False, f"SHORT_{int(total)}", int(total))

    except requests.Timeout:
        last_err = "TIMEOUT"
    except Exception as e:
        last_err = f"ERR:{type(e).__name__}"

    return (False, last_err or "ERR", int(last_bytes or 0))

def _apply_usenet_playability_probe(
    pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    *,
    top_n: int,
    target_real: int,
    timeout_s: float,
    budget_s: float,
    range_end: int,
    stub_len: int,
    retries: int,
    concurrency: int,
    drop_stubs: bool,
    drop_fails: bool,
    mark_ready: bool,
    stats: Optional["PipeStats"] = None,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Probe top-N usenet candidates and mark/drop unplayable proxy links.

    Goals:
      - Keep /stream latency bounded via an overall budget (default ~8.5s).
      - Identify REAL links (read exactly range_end+1 bytes).
      - Mark REAL with meta["usenet_probe"]="REAL" and (optionally) meta["ready"]=True.
      - Mark non-REAL with meta["usenet_probe"]="STUB"/"ERR" (unless we ran out of budget).
      - Drop probed STUBs (and optionally drop probed ERR) from output so they don't occupy top spots.
      - Stop early once `target_real` REAL links are found *or* we hit the budget.

    IMPORTANT: Anything we *didn't* get to probe within the budget is treated as "not probed"
    and stays in the normal sorting pack with debrid.
    """
    if not pairs:
        return pairs

    tn = max(0, int(top_n or 0))
    if tn <= 0:
        return pairs

    # Identify usenet-like providers
    usenet_provs = {str(p).upper() for p in (USENET_PROVIDERS or USENET_PRIORITY or [])}
    usenet_provs.add("ND")

    def _is_usenet_pair(pair: Tuple[Dict[str, Any], Dict[str, Any]]) -> bool:
        _s, _m = pair
        prov = str((_m or {}).get("provider") or "").upper().strip()
        if prov in usenet_provs:
            return True
        aio = (_m or {}).get("aio")
        try:
            if USE_AIO_READY and isinstance(aio, dict) and (aio.get("type") == "usenet"):
                return True
        except Exception:
            pass
        return False

    # Candidate indices: first N usenet items by current order (not first N overall).
    # We scan the full list until we collect tn unique usenet URLs. This ensures usenet
    # candidates that would otherwise sit below debrid in the global sort still get probed.
    cand: List[int] = []
    seen_url: set[str] = set()
    for i, (s, m) in enumerate(pairs):
        if not _is_usenet_pair((s, m)):
            continue
        u = ""
        try:
            u = (s.get("url") or "") if isinstance(s, dict) else ""
        except Exception:
            u = ""
        if not u:
            continue
        if u in seen_url:
            continue
        seen_url.add(u)
        cand.append(i)
        if len(cand) >= tn:
            break


    if not cand:
        return pairs

    max_workers = max(1, min(int(concurrency or 1), len(cand), int(VERIFY_MAX_WORKERS or 1)))
    target = max(0, int(target_real or 0))
    budget = max(0.5, float(budget_s or 0.0))
    deadline = float(time.monotonic()) + float(budget)

    t0 = time.monotonic()
    real_idx: List[int] = []
    stub_idx: List[int] = []
    err_idx: List[int] = []
    budget_idx: List[int] = []

    # Use a single executor; avoid batch-level waits that can exceed the budget.
    from collections import deque
    q = deque(cand)

    ex = ThreadPoolExecutor(max_workers=max_workers)
    in_flight: Dict[Any, int] = {}
    try:

        def _submit(i: int) -> None:
            s, _m = pairs[i]
            try:
                u = s.get("url") if isinstance(s, dict) else ""
            except Exception:
                u = ""
            # Pass the overall deadline down so the probe can't outlive the budget.
            fut = ex.submit(
                _usenet_range_probe_is_real,
                u,
                timeout_s=float(timeout_s),
                range_end=int(range_end),
                stub_len=int(stub_len),
                retries=int(retries),
                deadline_ts=float(deadline),
            )
            in_flight[fut] = i

        # Prime the pool
        while q and len(in_flight) < max_workers and time.monotonic() < deadline:
            _submit(q.popleft())

        def _handle_done(fut) -> None:
            i = in_flight.pop(fut, None)
            if i is None:
                return
            try:
                ok, reason, nbytes = fut.result(timeout=0)
            except Exception:
                ok, reason, nbytes = (False, "ERR", 0)

            s, m = pairs[i]
            if not isinstance(m, dict):
                return

            # Budget sentinel: treat as "not probed" (do not drop).
            if str(reason) == "BUDGET":
                budget_idx.append(i)
                return

            if ok:
                real_idx.append(i)
                m["usenet_probe"] = "REAL"
                m["usenet_probe_bytes"] = int(nbytes or 0)
                if mark_ready:
                    m["ready"] = True
            else:
                if reason == "STUB_LEN" or str(reason).startswith("SHORT_"):
                    stub_idx.append(i)
                    m["usenet_probe"] = "STUB"
                else:
                    err_idx.append(i)
                    m["usenet_probe"] = "ERR"
                m["usenet_probe_reason"] = str(reason)
                # Track why probes failed (counts by reason) for debugging/metrics.
                if stats is not None:
                    try:
                        _d = getattr(stats, "ms_usenet_probe_fail_reasons", None)
                        if isinstance(_d, dict):
                            _r = str(reason)
                            _d[_r] = int(_d.get(_r, 0)) + 1
                    except Exception:
                        pass
                m["usenet_probe_bytes"] = int(nbytes or 0)
                # Demote hard so these don't occupy top spots.
                try:
                    m["verify_rank"] = max(int(m.get("verify_rank") or 0), 9999)
                except Exception:
                    m["verify_rank"] = 9999

        # Main completion loop
        from concurrent.futures import FIRST_COMPLETED
        while in_flight and time.monotonic() < deadline and (target == 0 or len(real_idx) < target):
            rem = max(0.01, float(deadline) - float(time.monotonic()))
            done, _not_done = wait(list(in_flight.keys()), timeout=rem, return_when=FIRST_COMPLETED)
            if not done:
                break
            for fut in list(done):
                _handle_done(fut)
                if time.monotonic() >= deadline:
                    break
                if target and len(real_idx) >= target:
                    break
                if q and len(in_flight) < max_workers and time.monotonic() < deadline:
                    _submit(q.popleft())

        # Small grace drain (do not extend budget): collect any completions already done right now.
        try:
            done, _ = wait(list(in_flight.keys()), timeout=0.01, return_when=FIRST_COMPLETED)
            for fut in list(done):
                _handle_done(fut)
        except Exception:
            pass

        # Cancel anything still running; do not wait for full shutdown (avoid blowing /stream latency).
        try:
            for fut in list(in_flight.keys()):
                try:
                    fut.cancel()
                except Exception:
                    pass
            ex.shutdown(wait=False, cancel_futures=True)
        except Exception:
            try:
                ex.shutdown(wait=False)
            except Exception:
                pass

        # Drop probed STUBs (and optionally ERR) from output entirely (keeps list reliable).
        drop_idx: set[int] = set()
        if drop_stubs and stub_idx:
            drop_idx.update(stub_idx)
        if drop_fails and err_idx:
            drop_idx.update(err_idx)
        if drop_idx:
            pairs = [p for j, p in enumerate(pairs) if j not in drop_idx]

        # Lightweight summary log (no URLs).
        try:
            ms = int((time.monotonic() - t0) * 1000)
            logger.info(
                "USENET_PROBE rid=%s scanned=%s real=%s stub=%s err=%s budget=%s ms=%s",
                _rid(), int(len(cand)), int(len(real_idx)), int(len(stub_idx)), int(len(err_idx)), int(len(budget_idx)), int(ms),
            )
            if stats is not None:
                try:
                    stats.counts_out = stats.counts_out or {}
                    stats.counts_out["usenet_probe_real"] = int(len(real_idx))
                    stats.counts_out["usenet_probe_stub"] = int(len(stub_idx))
                    stats.counts_out["usenet_probe_err"] = int(len(err_idx))
                    stats.counts_out["usenet_probe_budget"] = int(len(budget_idx))
                except Exception:
                    pass
        except Exception:
            pass

        return pairs
    finally:
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            # Python < 3.9: cancel_futures not supported
            ex.shutdown(wait=False)
        except Exception:
            pass



def _apply_usenet_real_priority_mix(
    pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    *,
    deliver_cap: int,
    top10_pct: float,
    top20_n: int,
    stats: Optional["PipeStats"] = None,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Re-order *delivered slice* so probed REAL usenet links are prioritized deterministically.

    Policy (defaults):
      - Cap REAL usenet inside top-10 to ~50% (so debrid stays visible).
      - Promote remaining REAL usenet into the rest of top-20.
      - Everything else stays in original order (debrid sort order preserved).
    """
    if not pairs:
        return pairs

    try:
        dc = int(deliver_cap or 0)
    except Exception:
        dc = 0
    if dc <= 0:
        return pairs

    deliver_n = min(len(pairs), dc)
    if deliver_n <= 0:
        return pairs

    # Identify usenet-like providers
    usenet_provs = {str(p).upper() for p in (USENET_PROVIDERS or USENET_PRIORITY or [])}
    usenet_provs.add("ND")

    def _is_usenet_pair(pair: Tuple[Dict[str, Any], Dict[str, Any]]) -> bool:
        _s, _m = pair
        prov = str((_m or {}).get("provider") or "").upper().strip()
        if prov in usenet_provs:
            return True
        aio = (_m or {}).get("aio")
        try:
            if USE_AIO_READY and isinstance(aio, dict) and (aio.get("type") == "usenet"):
                return True
        except Exception:
            pass
        return False

    def _key_of(pair: Tuple[Dict[str, Any], Dict[str, Any]]) -> Tuple[str, str, str]:
        s, m = pair
        try:
            u = (s.get("url") or s.get("externalUrl") or "").strip()
        except Exception:
            u = ""
        try:
            ih = (m.get("infoHash") or m.get("infohash") or s.get("infoHash") or s.get("infohash") or "").strip().lower()
        except Exception:
            ih = ""
        try:
            n = (s.get("name") or "").strip()
        except Exception:
            n = ""
        return (u, ih, n)

    # REAL usenet list (preserve current order)
    real_usenet: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for p in pairs[:]:
        s, m = p
        if not isinstance(m, dict):
            continue
        if not _is_usenet_pair(p):
            continue
        if str(m.get("usenet_probe") or "") != "REAL":
            continue
        real_usenet.append(p)

    if not real_usenet:
        return pairs

    top10_n = min(10, deliver_n)
    top20_n_eff = min(max(0, int(top20_n or 20)), deliver_n)
    try:
        pct = float(top10_pct or 0.5)
    except Exception:
        pct = 0.5
    pct = max(0.0, min(1.0, pct))
    cap10 = int(float(top10_n) * pct)
    cap10 = max(0, min(cap10, top10_n, len(real_usenet)))
    cap20 = max(0, min(top20_n_eff, len(real_usenet)))

    # Build head with a strict cap inside top10.
    used = set()

    head: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    # 1) Put first cap10 REAL usenet into the head.
    for p in real_usenet[:cap10]:
        k = _key_of(p)
        if k in used:
            continue
        head.append(p)
        used.add(k)

    # 2) Fill remaining top10 slots with best non-REAL items (preserve current order),
    #    and avoid accidentally adding more REAL usenet in top10 (strict cap).
    for p in pairs:
        if len(head) >= top10_n:
            break
        k = _key_of(p)
        if k in used:
            continue
        # Strict cap: keep additional REAL usenet out of top10.
        _m = p[1]
        if isinstance(_m, dict) and str(_m.get("usenet_probe") or "") == "REAL" and _is_usenet_pair(p):
            continue
        head.append(p)
        used.add(k)

    # 3) Promote remaining REAL usenet into the rest of top20.
    for p in real_usenet[cap10:cap20]:
        if len(head) >= top20_n_eff:
            break
        k = _key_of(p)
        if k in used:
            continue
        head.append(p)
        used.add(k)

    # 4) Fill to top20 with remaining items (preserve order).
    for p in pairs:
        if len(head) >= top20_n_eff:
            break
        k = _key_of(p)
        if k in used:
            continue
        head.append(p)
        used.add(k)

    # 5) Append the remainder in original order (including extra REAL usenet beyond top20 if any).
    tail: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for p in pairs:
        k = _key_of(p)
        if k in used:
            continue
        tail.append(p)

    out = head + tail

    try:
        # Stats/log helper (no URLs)
        in_top10 = 0
        in_top20 = 0
        for i, (_s0, _m0) in enumerate(out[:max(top20_n_eff, top10_n)], start=1):
            if not isinstance(_m0, dict):
                continue
            if str(_m0.get("usenet_probe") or "") == "REAL":
                if i <= top10_n:
                    in_top10 += 1
                if i <= top20_n_eff:
                    in_top20 += 1
        logger.info(
            "USENET_REAL_MIX rid=%s real_total=%s real_top10=%s real_top20=%s cap10=%s top20=%s",
            _rid(), int(len(real_usenet)), int(in_top10), int(in_top20), int(cap10), int(top20_n_eff),
        )
        if stats is not None:
            try:
                stats.counts_out = stats.counts_out or {}
                stats.counts_out["usenet_real_total"] = int(len(real_usenet))
                stats.counts_out["usenet_real_top10"] = int(in_top10)
                stats.counts_out["usenet_real_top20"] = int(in_top20)
            except Exception:
                pass
    except Exception:
        pass

    return out



def _provider_rank(prov: str) -> int:
    """Provider ordering for global sort.
    Supports 'DL-<BASE>' (e.g., DL-TB) by ranking as BASE.
    """
    prov_u = (prov or "").upper().strip()
    # DL association (e.g., DL-TB ranks as TB)
    if prov_u.startswith("DL-"):
        base = prov_u.split("-", 1)[1].strip()
        return PREMIUM_PRIORITY.index(base) if base in PREMIUM_PRIORITY else (len(PREMIUM_PRIORITY) + len(USENET_PRIORITY) + 1)

    # Treat generic Debrid-Link as mid-tier unless explicitly in PREMIUM_PRIORITY
    if prov_u == "DEBRIDLINK":
        prov_u = "DL"

    if prov_u in PREMIUM_PRIORITY:
        return PREMIUM_PRIORITY.index(prov_u)
    if prov_u in USENET_PRIORITY:
        return len(PREMIUM_PRIORITY) + USENET_PRIORITY.index(prov_u)

    # Mid-tier for wrapper supplier markers if they ever appear here
    if prov_u in {"AIO", "P2", "PROV2", "DL"}:
        return len(PREMIUM_PRIORITY) + len(USENET_PRIORITY) + 1

    return 999


# TorBox known-cached memoization (TTL cache).
# This avoids re-checking hashes we've recently confirmed as cached,
# reducing TorBox API churn and improving /stream latency.
_TB_KNOWN_CACHED_LOCK = threading.Lock()
_TB_KNOWN_CACHED: Dict[str, float] = {}  # hash -> expires_epoch

def _tb_known_cached_prune(now_epoch: float, ttl_s: int, max_items: int) -> None:
    """Prune expired entries and cap total size."""
    if ttl_s <= 0 or max_items <= 0:
        with _TB_KNOWN_CACHED_LOCK:
            _TB_KNOWN_CACHED.clear()
        return
    with _TB_KNOWN_CACHED_LOCK:
        # Remove expired
        for h, exp in list(_TB_KNOWN_CACHED.items()):
            if exp <= now_epoch:
                _TB_KNOWN_CACHED.pop(h, None)
        # Cap size (evict arbitrary oldest-in-dict entries)
        while len(_TB_KNOWN_CACHED) > max_items:
            _TB_KNOWN_CACHED.pop(next(iter(_TB_KNOWN_CACHED)), None)

def _tb_known_cached_is_live(h: str, now_epoch: Optional[float] = None) -> bool:
    if not h:
        return False
    hh = (h or '').strip().lower()
    now = float(now_epoch) if now_epoch is not None else time.time()
    with _TB_KNOWN_CACHED_LOCK:
        exp = _TB_KNOWN_CACHED.get(hh)
    return bool(exp and exp > now)

def _tb_known_cached_refresh(h: str, now_epoch: float, ttl_s: int) -> None:
    if not h or ttl_s <= 0:
        return
    hh = (h or '').strip().lower()
    with _TB_KNOWN_CACHED_LOCK:
        _TB_KNOWN_CACHED[hh] = float(now_epoch) + float(ttl_s)

def _tb_known_cached_premark(hashes: List[str], cached_map: Dict[str, Any], now_epoch: float) -> set:
    """Pre-mark cached_map for hashes known cached and return the known set."""
    known = set()
    for h in hashes or []:
        hh = (h or '').strip().lower()
        if not hh:
            continue
        if _tb_known_cached_is_live(hh, now_epoch=now_epoch):
            cached_map[hh] = True
            known.add(hh)
    return known

def tb_get_cached(hashes: List[str]) -> Dict[str, bool]:
    """TorBox torrent cached availability check.

    Uses TorBox batch endpoint. If TB_BATCH_CONCURRENCY > 1, batches are checked in parallel.
    """
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

    bs = max(1, int(TB_BATCH_SIZE or 50))
    batches = [hashes[i:i + bs] for i in range(0, len(hashes), bs)]

    def _do_batch(batch: List[str]) -> Dict[str, bool]:
        out_b: Dict[str, bool] = {}
        if not batch:
            return out_b
        try:
            # TorBox `format` supports "object" or "list" ("list" is fastest). Using "hash" causes a 400.
            r = requests.post(
                url,
                params={'format': 'list', 'list': 'true'},
                json={'hashes': batch},
                headers=headers,
                timeout=TB_API_TIMEOUT,
            )
            if r.status_code != 200:
                return out_b
            data = r.json() if r.content else {}
            # Common response style: {success: bool, data: {hash: {...}}} OR {data: [hashes]}
            d = data.get('data') if isinstance(data, dict) else None
            if isinstance(d, dict):
                cached = {str(k).lower().strip() for k in d.keys() if k}
            elif isinstance(d, list):
                cached = set()
                for x in d:
                    if isinstance(x, str):
                        nx = norm_infohash(x)
                        if nx:
                            cached.add(nx)
                    elif isinstance(x, dict):
                        hx = x.get('hash') or x.get('info_hash')
                        if isinstance(hx, str):
                            nx = norm_infohash(hx)
                            if nx:
                                cached.add(nx)
            else:
                cached = set()
            cached_norm = {norm_infohash(x) for x in cached if x}
            for h in batch:
                hh = norm_infohash(h)
                if hh:
                    out_b[hh] = hh in cached_norm
        except Exception:
            return out_b
        return out_b

    bc = max(1, int(TB_BATCH_CONCURRENCY or 1))
    if bc > 1 and len(batches) > 1:
        ex = ThreadPoolExecutor(max_workers=min(bc, len(batches)))
        futs = [ex.submit(_do_batch, b) for b in batches]
        try:
            done, not_done = wait(futs, timeout=float(TB_BATCH_FUTURE_TIMEOUT or 8.0))
            for fut in done:
                try:
                    out.update(fut.result() or {})
                except Exception:
                    continue
            # Cancel any remaining work; don't stall request on slow batches.
            for fut in not_done:
                try:
                    fut.cancel()
                except Exception:
                    pass
        finally:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                ex.shutdown(wait=False)
    else:
        for b in batches:
            out.update(_do_batch(b))

    return out

def tb_get_usenet_cached(hashes: List[str]) -> Dict[str, bool]:
    """Optional TorBox Usenet cached availability check.

    TorBox exposes a separate usenet endpoint. Many Stremio streams do not carry a usable usenet identifier,
    so this is only used when we can collect identifiers (stored in meta['usenet_hash']).

    If TB_BATCH_CONCURRENCY > 1, batches are checked in parallel.
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

    bs = max(1, int(TB_BATCH_SIZE or 50))
    batches = [hashes[i:i + bs] for i in range(0, len(hashes), bs)]

    def _do_batch(batch: List[str]) -> Dict[str, bool]:
        out_b: Dict[str, bool] = {}
        if not batch:
            return out_b
        try:
            # Swagger/Postman show `hash` as the repeated query parameter; `format=list` returns the cached hashes.
            params = [('format', 'list')]
            params.extend([('hash', h) for h in batch])
            r = requests.get(url, params=params, headers=headers, timeout=TB_API_TIMEOUT)
            if r.status_code != 200:
                return out_b
            data = r.json() if r.content else {}
            d = data.get('data') if isinstance(data, dict) else None
            if isinstance(d, dict):
                cached = {str(k).lower().strip() for k in d.keys() if k}
            elif isinstance(d, list):
                cached = set()
                for x in d:
                    if isinstance(x, str):
                        cached.add(x.lower().strip())
                    elif isinstance(x, dict):
                        hx = x.get('hash') or x.get('usenet_hash')
                        if isinstance(hx, str):
                            cached.add(hx.lower().strip())
            else:
                cached = set()
            cached_norm = {str(x).lower().strip() for x in cached if x}
            for h in batch:
                hh = str(h).lower().strip()
                if hh:
                    out_b[hh] = hh in cached_norm
        except Exception:
            return out_b
        return out_b

    bc = max(1, int(TB_BATCH_CONCURRENCY or 1))
    if bc > 1 and len(batches) > 1:
        ex = ThreadPoolExecutor(max_workers=min(bc, len(batches)))
        futs = [ex.submit(_do_batch, b) for b in batches]
        try:
            done, not_done = wait(futs, timeout=float(TB_BATCH_FUTURE_TIMEOUT or 8.0))
            for fut in done:
                try:
                    out.update(fut.result() or {})
                except Exception:
                    continue
            for fut in not_done:
                try:
                    fut.cancel()
                except Exception:
                    pass
        finally:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                ex.shutdown(wait=False)
    else:
        for b in batches:
            out.update(_do_batch(b))

    return out

class _WebDavUnauthorized(RuntimeError):
    """Raised when TorBox WebDAV returns 401 (bad/missing credentials)."""


def _tb_webdav_exists(url: str) -> bool:
    if WEBDAV_INACTIVE:
        return False  # INACTIVE  //✅

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


def tb_webdav_batch_check(hashes: List[str], stats: Optional[PipeStats] = None) -> set:
    if WEBDAV_INACTIVE:
        return set()  # INACTIVE  //✅
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
    ex = ThreadPoolExecutor(max_workers=max(1, TB_WEBDAV_WORKERS))
    futures = [ex.submit(worker, item) for item in urls]
    try:
        done, not_done = wait(futures, timeout=float(WEBDAV_FUTURE_TIMEOUT or 3.0))
        for fut in done:
            try:
                res = fut.result()
                if res:
                    ok.add(res)
            except Exception:
                continue
        for fut in not_done:
            try:
                fut.cancel()
            except Exception:
                pass
    finally:
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            ex.shutdown(wait=False)

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

DEBUG_LOG_FULL_STREAMS = _parse_bool(os.getenv("DEBUG_LOG_FULL_STREAMS", "false"), False)

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
_WRAP_URL_HASH_TO_TOKEN = {}  # sha256(url) -> token (best-effort dedup)
_WRAP_URL_LOCK = threading.Lock()

# --- Verify host cache (short-lived) ---
# Used to avoid repeatedly probing hosts that have already proven unsafe/risky.
# host -> (expires_ts, level, reason) where level in {"unsafe","risky"}
_VERIFY_HOST_CACHE_LOCK = threading.Lock()
_VERIFY_HOST_CACHE: Dict[str, Tuple[float, str, str]] = {}

def _verify_host_cache_get(host: str) -> Optional[Tuple[str, str]]:
    """Return (level, reason) if host cache entry is valid."""
    try:
        h = (host or "").strip().lower()
        if not h:
            return None
        now = time.time()
        with _VERIFY_HOST_CACHE_LOCK:
            ent = _VERIFY_HOST_CACHE.get(h)
            if not ent:
                return None
            exp, level, reason = ent
            if exp <= now:
                _VERIFY_HOST_CACHE.pop(h, None)
                return None
            return (level, reason)
    except Exception:
        return None

def _verify_host_cache_set(host: str, level: str, reason: str, ttl_s: Optional[int] = None) -> None:
    """Cache host classification for a short time."""
    try:
        h = (host or "").strip().lower()
        if not h:
            return
        if ttl_s is None:
            ttl_s = _safe_int(os.environ.get("VERIFY_HOST_CACHE_TTL", "300"), 300)
        ttl_s = max(30, int(ttl_s))
        exp = time.time() + ttl_s
        with _VERIFY_HOST_CACHE_LOCK:
            _VERIFY_HOST_CACHE[h] = (exp, str(level or ""), str(reason or ""))
    except Exception:
        return
_WRAP_URL_META = {}  # token -> (meta_dict, expires_epoch)

def _wrap_url_store(url: str, meta: Optional[Dict[str, Any]] = None) -> str:
    """Store a playback URL and return an opaque short token.

    This is an in-memory map per-process. If you rely on these tokens, keep GUNICORN_CONCURRENCY=1.
    """
    import time, base64, hashlib, uuid

    exp = time.time() + max(60, int(WRAP_URL_TTL or 3600))
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()

    with _WRAP_URL_LOCK:
        if WRAPPER_DEDUP:
            tok = _WRAP_URL_HASH_TO_TOKEN.get(h)
            if tok:
                val = _WRAP_URL_MAP.get(tok)
                if val:
                    old_url, old_exp = val
                    if old_url == url and (not old_exp or time.time() <= old_exp):
                        _WRAP_URL_MAP[tok] = (url, exp)  # refresh TTL
                        if meta is not None:
                            try:
                                from urllib.parse import urlparse
                                if isinstance(meta, dict) and "up_host" not in meta:
                                    meta["up_host"] = (urlparse(url).netloc or "").lower()
                            except Exception:
                                pass
                            _WRAP_URL_META[tok] = (meta, exp)
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug("REUSED_TOKEN len=%d", len(tok))
                        return tok
                _WRAP_URL_HASH_TO_TOKEN.pop(h, None)

        # 16 hex chars; stays well under Android URL limits
        token = uuid.uuid4().hex[:16]  # 16 hex chars (Android-safe)
        _WRAP_URL_MAP[token] = (url, exp)
        if meta is not None:
            try:
                from urllib.parse import urlparse
                if isinstance(meta, dict) and "up_host" not in meta:
                    meta["up_host"] = (urlparse(url).netloc or "").lower()
            except Exception:
                pass
            _WRAP_URL_META[token] = (meta, exp)
        if WRAPPER_DEDUP:
            _WRAP_URL_HASH_TO_TOKEN[h] = token
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("NEW_TOKEN len=%d", len(token))
    return token


def _wrap_url_load(token: str) -> Optional[str]:
    import hashlib
    now = time.time()
    with _WRAP_URL_LOCK:
        val = _WRAP_URL_MAP.get(token)
        if not val:
            return None
        url, exp = val
        if exp and now > exp:
            _WRAP_URL_MAP.pop(token, None)
            _WRAP_URL_META.pop(token, None)
            if WRAPPER_DEDUP:
                try:
                    h = hashlib.sha256(url.encode("utf-8")).hexdigest()
                    if _WRAP_URL_HASH_TO_TOKEN.get(h) == token:
                        _WRAP_URL_HASH_TO_TOKEN.pop(h, None)
                except Exception:
                    pass
            return None
        return url



def _unwrap_short_url(u: str) -> Optional[str]:
    """Best-effort: convert our short /r/<token> URLs back to the stored upstream URL.

    Only meaningful when WRAP_URL_SHORT is enabled; otherwise returns None.
    Handles either a relative '/r/<token>' or an absolute URL whose path contains '/r/<token>'.
    """
    try:
        if not u:
            return None
        # If short URLs are disabled, nothing to unwrap.
        if not globals().get("WRAP_URL_SHORT", True):
            return None

        token = ""

        # Relative form
        if u.startswith("/r/"):
            token = u.split("/r/", 1)[1]
        else:
            # Absolute URLs that contain /r/<token> in their path
            from urllib.parse import urlparse
            p = urlparse(u)
            path = p.path or ""
            if "/r/" in path:
                token = path.split("/r/", 1)[1]

        if not token:
            return None

        # Drop query/hash and any additional path segments after the token
        token = token.split("?", 1)[0].split("#", 1)[0].split("/", 1)[0].strip()
        if not token:
            return None

        # Token may be URL-encoded in some clients
        try:
            from urllib.parse import unquote
            token = unquote(token)
        except Exception:
            pass

        if not token:
            return None

        return _wrap_url_load(token)
    except Exception:
        return None

def _wrap_url_meta_load(token: str) -> Optional[Dict[str, Any]]:
    """Load stored metadata for a short token (best-effort)."""
    try:
        now = time.time()
        with _WRAP_URL_LOCK:
            v = _WRAP_URL_META.get(token)
            if not v:
                return None
            meta, exp = v
            if exp and now > exp:
                _WRAP_URL_META.pop(token, None)
                return None
            return meta if isinstance(meta, dict) else None
    except Exception:
        return None


def _wrap_url_meta_update(token: str, meta: Dict[str, Any]) -> None:
    """Update metadata for an existing short token (best-effort)."""
    if not (token and isinstance(meta, dict)):
        return
    try:
        now = time.time()
        with _WRAP_URL_LOCK:
            v = _WRAP_URL_MAP.get(token)
            if not v:
                return
            _url, exp = v
            if exp and now > exp:
                return
            _WRAP_URL_META[token] = (meta, exp)
    except Exception:
        return


def _safe_url_host(u: str) -> str:
    try:
        from urllib.parse import urlparse
        p = urlparse(u)
        host = (p.netloc or "").lower()
        scheme = (p.scheme or "").lower()
        return f"{scheme}://{host}" if host else scheme
    except Exception:
        return ""


def wrap_playback_url(url: str, _base: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> str:
    """Wrap outbound http(s) URLs behind our HEAD-friendly redirector.

    For Android/Google TV Stremio, keep WRAP_URL_SHORT=true so emitted URLs stay short.
    `_base` may be supplied to avoid repeated header lookups (compute once per request).
    """
    if not url or not WRAP_PLAYBACK_URLS:
        return url
    u = str(url)

    # Compute base once (or accept a caller-provided base)
    try:
        base = (_base or _public_base_url()).rstrip('/')
    except Exception:
        base = (_base or "").rstrip('/')

    # Avoid double-wrapping (if stream already points at our redirector)
    try:
        if base and u.startswith(base + '/r/'):
            return u
    except Exception:
        pass

    if u.startswith('http://') or u.startswith('https://'):
        if WRAP_URL_SHORT:
            # Strict clients (Android/TV, some web/iphone builds) need short URLs.
            # Always emit a short token; we can still *accept* long tokens in the redirector.
            tok = _wrap_url_store(u, meta=meta)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("WRAP_EMIT short=True tok_len=%d", len(tok or ""))
        else:
            restart_safe = _is_true(os.environ.get('WRAP_URL_RESTART_SAFE', 'false'))
            tok = _zurl_encode(u) if restart_safe else _b64u_encode(u)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("WRAP_EMIT short=False restart_safe=%s tok_len=%d", restart_safe, len(tok or ""))

        if not base:
            base = _public_base_url().rstrip('/')
        wrapped = base + '/r/' + tok
        try:
            if _is_true(os.environ.get("WRAP_LOG_TOKEN_EMIT", "false")):
                _tok_disp = tok if _is_true(os.environ.get("WRAP_LOG_TOKEN_FULL", "false")) else (tok[:4] + "…" + tok[-4:] if len(tok) > 10 else tok)
                _m = meta if isinstance(meta, dict) else {}
                logger.info(
                    "TOKEN_EMIT rid=%s tok=%s host=%s prov=%s tag=%s res=%s seeders=%s cached=%s ready=%s",
                    _rid(),
                    _tok_disp,
                    _safe_url_host(u),
                    (_m.get("provider") or ""),
                    (_m.get("tag") or ""),
                    (_m.get("res") or ""),
                    (_m.get("seeders") or ""),
                    (_m.get("cached") or ""),
                    (_m.get("ready") or ""),
                )
        except Exception:
            pass

        if WRAP_DEBUG:
            logging.getLogger('aio-wrapper').info(f'WRAP_URL -> {wrapped}')
        return wrapped

    return u


app = Flask(__name__)

# --- optional rate limiting ---
# RATE_LIMIT is a config string like "60/minute". If Flask-Limiter isn't installed, we just log and continue.
limiter = None
if RATE_LIMIT:
    try:
        from flask_limiter import Limiter
        from flask_limiter.util import get_remote_address
        limiter = Limiter(
            get_remote_address,
            app=app,
            default_limits=[RATE_LIMIT],
            storage_uri="memory://",
        )
    except Exception as _e:
        logger.warning("RATE_LIMIT enabled but flask-limiter unavailable/failed: %s", _e)


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

    # Best-effort: attach stored token metadata for correlation (especially iPhone "not ready" cases)
    _tok_meta = None
    try:
        if WRAP_URL_SHORT and token and (not token.startswith("z")):
            _tok_meta = _wrap_url_meta_load(token)
    except Exception:
        _tok_meta = None

    try:
        if _is_true(os.environ.get("WRAP_LOG_TOKEN_HIT", "false")):
            # Sampling (percent)
            try:
                pct = float(os.environ.get("WRAP_LOG_TOKEN_SAMPLE_PCT", "100") or "100")
            except Exception:
                pct = 100.0
            if pct >= 100.0 or (random.random() * 100.0) <= pct:
                _tok_disp = token if _is_true(os.environ.get("WRAP_LOG_TOKEN_FULL", "false")) else (token[:4] + "…" + token[-4:] if len(token) > 10 else token)
                _rng = request.headers.get("Range", "")
                logger.info(
                    "TOKEN_HIT rid=%s tok=%s method=%s platform=%s ua=%s range=%s host=%s emit_rid=%s prov=%s tag=%s res=%s seeders=%s cached=%s ready=%s",
                    _rid(),
                    _tok_disp,
                    request.method,
                    client_platform(is_android=is_android_client(), is_iphone=is_iphone_client()),
                    (request.headers.get("User-Agent", "") or "")[:80],
                    _rng[:64],
                    _safe_url_host(url),
                    (_tok_meta or {}).get("emit_rid", ""),
                    (_tok_meta or {}).get("provider", ""),
                    (_tok_meta or {}).get("tag", ""),
                    (_tok_meta or {}).get("res", ""),
                    (_tok_meta or {}).get("seeders", ""),
                    (_tok_meta or {}).get("cached", ""),
                    (_tok_meta or {}).get("ready", ""),
                )
    except Exception:
        pass


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
        # iOS/AVPlayer clients may treat a bypassed HEAD (Content-Length: 0) as "empty" and stall.
        # Prefer redirect-style HEAD on iOS, configurable via WRAP_HEAD_MODE_IOS.
        if is_iphone_client():
            mode = (os.environ.get("WRAP_HEAD_MODE_IOS", "302") or "302").lower()
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
        # Convenience: expose a human-friendly copy page for the underlying URL
        try:
            if _is_true(os.environ.get("COPY_PAGE_ENABLED", "false")):
                resp.headers["X-Copy-Page"] = _public_base_url().rstrip("/") + "/copy/" + str(token)
        except Exception:
            pass

    else:
        # GET: real playback. Normally we redirect to the upstream proxy URL.
        #
        # Some upstream proxies ignore tiny Range probes (e.g. "bytes=0-0") and start streaming a lot of data
        # while still reporting a 206 response. iPhone / Android TV Stremio clients can interpret that as
        # "not ready" (or stall). For probe-sized ranges, we answer locally with a tiny 206 so the client
        # can move on to the real playback request.
        rng = request.headers.get("Range", "")
        platform = "iphone" if is_iphone_client() else ("android_tv" if is_android_tv_client() else "")
        start_b, end_b = _parse_http_range(rng) if rng else (None, None)
        is_probe = (
            RANGE_PROBE_GUARD
            and platform
            and start_b == 0
            and end_b is not None
            and end_b >= 0
            and (end_b - start_b + 1) <= max(1, RANGE_PROBE_MAX_BYTES)
        )
        if is_probe:
            total = int((_tok_meta or {}).get("size") or 0)
            if total <= 0:
                total = int(end_b) + 1
            n = int(end_b - start_b + 1)
            body = b"\0" * n
            resp = make_response(body, 206)
            resp = _base(resp)
            resp.headers["Content-Type"] = "application/octet-stream"
            resp.headers["Accept-Ranges"] = "bytes"
            resp.headers["Content-Range"] = f"bytes {start_b}-{end_b}/{total}"
            resp.headers["Content-Length"] = str(n)
            resp.headers["X-Range-Guard"] = "1"
            # Convenience: expose a human-friendly copy page for the underlying URL
            try:
                if _is_true(os.environ.get("COPY_PAGE_ENABLED", "false")):
                    resp.headers["X-Copy-Page"] = _public_base_url().rstrip("/") + "/copy/" + str(token)
            except Exception:
                pass
            try:
                logger.info("RANGE_GUARD rid=%s tok=%s plat=%s range=%s total=%s", _rid(), str(token)[:8], platform, rng, total)
            except Exception:
                pass
        else:
            resp = make_response("", 302)
            resp.headers["Location"] = url
            resp = _base(resp)
            # Convenience: expose a human-friendly copy page for the underlying URL
            try:
                if _is_true(os.environ.get("COPY_PAGE_ENABLED", "false")):
                    resp.headers["X-Copy-Page"] = _public_base_url().rstrip("/") + "/copy/" + str(token)
            except Exception:
                pass


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


@app.route("/copy/<path:token>", methods=["GET"])
def copy_page(token: str):
    """Human-friendly page to copy the underlying upstream URL for a /r/<token>.
    Disabled by default; enable with COPY_PAGE_ENABLED=true.
    """
    if not _is_true(os.environ.get("COPY_PAGE_ENABLED", "false")):
        return ("", 404)

    # Resolve token the same way redirect_stream_url does
    url = None
    if WRAP_URL_SHORT:
        url = _wrap_url_load(token)
    if not url and token.startswith("z"):
        try:
            url = _zurl_decode(token)
        except Exception:
            url = None
    if not url:
        try:
            url = _b64u_decode(token)
        except Exception:
            url = None
    if not url:
        return ("", 404)

    meta = None
    try:
        if WRAP_URL_SHORT and token and not token.startswith("z"):
            meta = _wrap_url_meta_load(token)
    except Exception:
        meta = None

    from html import escape
    url_esc = escape(url)
    meta_txt = ""
    try:
        if isinstance(meta, dict) and meta:
            keys = ["emit_rid", "provider", "tag", "res", "seeders", "cached", "ready", "has_hash", "name"]
            parts = []
            for k in keys:
                if k in meta:
                    parts.append(f"{k}={escape(str(meta.get(k)))}")
            if parts:
                meta_txt = "<div style='margin-top:10px;color:#555;font-size:13px;word-break:break-word;'>" + " | ".join(parts) + "</div>"
    except Exception:
        meta_txt = ""

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Copy Stream URL</title>
  <style>
    body {{ font-family: -apple-system, system-ui, Segoe UI, Roboto, Arial, sans-serif; padding: 18px; }}
    .box {{ padding: 12px; border: 1px solid #ddd; border-radius: 10px; background: #fafafa; }}
    button {{ padding: 10px 14px; border-radius: 10px; border: 0; cursor: pointer; }}
    textarea {{ width: 100%; height: 110px; margin-top: 10px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
    .small {{ margin-top: 10px; color: #666; font-size: 13px; }}
  </style>
</head>
<body>
  <h2>Copy playback URL</h2>
  <div class="box">
    <button id="copyBtn">Copy</button>
    <textarea id="u" readonly>{url_esc}</textarea>
    {meta_txt}
    <div class="small">If iPhone says “try again later”, paste this URL into a browser/curl and check the upstream response.</div>
  </div>
<script>
  const btn = document.getElementById('copyBtn');
  const ta = document.getElementById('u');
  btn.addEventListener('click', async () => {{
    ta.select();
    ta.setSelectionRange(0, 999999);
    try {{
      await navigator.clipboard.writeText(ta.value);
      btn.textContent = "Copied!";
      setTimeout(() => btn.textContent = "Copy", 1200);
    }} catch(e) {{
      document.execCommand('copy');
      btn.textContent = "Copied!";
      setTimeout(() => btn.textContent = "Copy", 1200);
    }}
  }});
</script>
</body>
</html>"""
    try:
        logger.info("COPY_PAGE rid=%s tok_len=%d host=%s", _rid(), len(token or ""), _safe_url_host(url))
    except Exception:
        pass
    resp = make_response(html, 200)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
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
logger.info(f"CONFIG tmdb_force_imdb={TMDB_FORCE_IMDB} tb_api_min_hashes={TB_API_MIN_HASHES} nzbgeek_title_match_min_ratio={NZBGEEK_TITLE_MATCH_MIN_RATIO} nzbgeek_timeout={NZBGEEK_TIMEOUT} nzbgeek_title_fallback={NZBGEEK_TITLE_FALLBACK} use_nzbgeek_ready={USE_NZBGEEK_READY} usenet_probe_enable={USENET_PROBE_ENABLE} usenet_probe_top_n={USENET_PROBE_TOP_N} usenet_probe_target_real={USENET_PROBE_TARGET_REAL} usenet_probe_timeout_s={USENET_PROBE_TIMEOUT_S} usenet_probe_budget_s={USENET_PROBE_BUDGET_S} usenet_probe_drop_fails={USENET_PROBE_DROP_FAILS} usenet_probe_real_top10_pct={USENET_PROBE_REAL_TOP10_PCT} usenet_probe_real_top20_n={USENET_PROBE_REAL_TOP20_N}")


def _debug_log_full_streams(type_: str, id_: str, platform: str, out_for_client: Optional[List[Dict[str, Any]]]) -> None:
    """DEBUG-only: log the full, final stream list (URLs + cached + prov + name).

    This intentionally logs short /r/<token> URLs so you can copy/paste and validate instant-play behavior.
    It is gated by logger DEBUG level to avoid noise/leakage in normal INFO deployments.
    """
    if not DEBUG_LOG_FULL_STREAMS:
        return
    if not logger.isEnabledFor(logging.DEBUG):
        return

    streams: List[Dict[str, Any]] = []
    try:
        streams = [s for s in (out_for_client or []) if isinstance(s, dict)]
    except Exception:
        streams = []

    rid = _rid()
    logger.debug("FULL_STREAMS rid=%s type=%s id=%s platform=%s count=%d", rid, type_, id_, platform, len(streams))

    for i, s in enumerate(streams):
        try:
            bh = s.get("behaviorHints") or {}
            logger.debug(
                "STREAM_%d rid=%s url=%s cached=%s prov=%s name=%s",
                i,
                rid,
                s.get("url"),
                bh.get("cached"),
                (s.get("prov") or s.get("provider") or bh.get("prov") or bh.get("provider")),
                (s.get("name") or s.get("title")),
            )
        except Exception:
            logger.debug("STREAM_%d rid=%s <log-failed>", i, rid, exc_info=True)

@app.before_request
def _before_request() -> None:
    g.request_id = str(uuid.uuid4())[:8]
    logger.info("REQ_IN %s %s rid=%s mark=%s ua=%s", request.method, request.path, _rid(), _mark(), _safe_ua(request.headers.get("User-Agent")) )
    rl = _enforce_rate_limit()
    if rl:
        body, code = rl
        return jsonify(body), code

def _rid() -> str:
    return g.request_id if has_request_context() and hasattr(g, "request_id") else "GLOBAL"

def _mark() -> str:
    """Request correlation marker from client header (X-REQ-MARK)."""
    try:
        if not has_request_context():
            return ""
        return (request.headers.get("X-REQ-MARK") or "")[:80]
    except Exception:
        return ""


# Thread-local pointer to current request stats for lightweight heuristic markers.
_TLS = threading.local()

def is_android_client() -> bool:
    """True for Android Mobile + Android TV."""
    p = _platform_cached()
    return p in ("android", "androidtv")


def is_android_tv_client(ua: str | None = None) -> bool:
    """Heuristic for Android TV / Google TV / Chromecast-type devices."""
    if ua is None:
        ua = (request.headers.get("User-Agent") or "").lower()
    else:
        ua = (ua or "").lower()

    if not ua:
        return False

    # Stremio explicit UA token (most reliable)
    if "stremioandroidtv" in ua:
        return True

    # General Android-TV / Google-TV hints
    tv_hints = (
        "android tv", "androidtv",
        "googletv", "google tv", "google_tv",
        "smart-tv", "smarttv",
        "hbbtv",
        "bravia", "shield", "nexus player",
        "aftt", "aftmm",  # Fire TV models
        "crkey", "chromecast",
    )
    return ("android" in ua) and any(h in ua for h in tv_hints)


def get_ua() -> str:
    return (request.headers.get("User-Agent") or "").strip()


def _ua_token(ua_lower: str) -> str:
    for tok in ("stremioshell", "stremioandroidtv", "stremioios", "stremio", "web.stremio.com"):
        if tok in ua_lower:
            return tok
    if any(k in ua_lower for k in ("iphone", "ipad", "ipod")):
        return "apple"
    if "android" in ua_lower:
        return "android"
    if any(k in ua_lower for k in ("macintosh", "windows", "linux")):
        return "desktop"
    return "other"

def _ua_family(ua: str) -> str:
    """More specific UA classifier (logging/debug only).

    Keeps `_ua_token` stable for existing logic while letting logs
    distinguish WebStremio Safari vs Chrome, etc.
    """
    u = (ua or "").lower()

    # Stremio native apps first
    if "stremioios" in u:
        return "stremio_ios"
    if "stremioandroidtv" in u:
        return "stremio_androidtv"
    if "stremioshell" in u:
        return "stremio_shell"
    if "stremio" in u and "android" in u:
        return "stremio_android"

    # Browser families (WebStremio)
    if "edg/" in u or "edge/" in u:
        return "web_edge"
    if "firefox/" in u:
        return "web_firefox"
    if "crios/" in u or "chrome/" in u:
        return "web_chrome"
    # Safari: has Safari but not Chrome/CriOS/Android
    if "safari/" in u and "chrome/" not in u and "crios/" not in u and "android" not in u:
        return "web_safari"

    # Fallback (broad buckets)
    return _ua_token(u)



def parse_platform(ua: str | None = None) -> str:
    """
    Returns: 'android', 'androidtv', 'ios', 'desktop', 'unknown'

    Key points:
    - iPhone + iPad are unified as 'ios'
    - iPad Safari may advertise a macOS UA; detect via 'Macintosh' + 'Mobile/'
    - Uses `ua-parser` if installed, with robust string fallback
    """
    if ua is None:
        ua = get_ua()

    if not ua:
        if EMPTY_UA_IS_ANDROID:
            return "android"
        logger.debug("EMPTY_UA rid=%s", _rid())
        return "unknown"

    ua_lower = ua.lower()

    # Stremio-specific overrides (avoid fragile substring tricks)
    if "stremioshell" in ua_lower:
        return "desktop"
    if "stremioandroidtv" in ua_lower:
        return "androidtv"
    if "stremioios" in ua_lower:
        return "ios"

    # UA-parser path (optional)
    if UA_PARSER_AVAILABLE and ua_parse is not None:
        try:
            parsed = ua_parse(ua)
            os_family = (getattr(parsed.os, "family", "") or "").lower()
            dev_family = (getattr(parsed.device, "family", "") or "").lower()

            if "android" in os_family:
                if ("tv" in dev_family) or is_android_tv_client(ua_lower):
                    return "androidtv"
                return "android"

            if os_family == "ios" or any(k in dev_family for k in ("iphone", "ipad", "ipod")):
                return "ios"

            # iPadOS masquerading as macOS (Safari on iPad can do this)
            if (os_family in ("mac os x", "macos")) and ("macintosh" in ua_lower) and ("mobile/" in ua_lower):
                return "ios"

            if os_family in ("mac os x", "macos", "windows", "linux", "ubuntu", "chrome os"):
                return "desktop"
        except Exception:
            pass  # fall through

    # String fallback (avoid naive 'ios' substring to prevent 'stremio[s]hell' false positives)
    if "android" in ua_lower:
        return "androidtv" if is_android_tv_client(ua_lower) else "android"

    if any(k in ua_lower for k in ("iphone", "ipad", "ipod", "cpu iphone os", "cpu os", "stremioios")):
        return "ios"

    if ("macintosh" in ua_lower) and ("mobile/" in ua_lower):
        return "ios"

    if any(k in ua_lower for k in ("windows", "macintosh", "mac os x", "linux", "x11")):
        return "desktop"

    return "unknown"


def _platform_cached() -> str:
    p = getattr(g, "_cached_platform", None)
    if p:
        return p
    p = parse_platform()
    g._cached_platform = p
    ua = get_ua() or ""
    g._cached_ua_tok = _ua_token(ua.lower())
    g._cached_ua_family = _ua_family(ua)
    return p


def is_iphone_client() -> bool:
    """Back-compat name: now means iOS (iPhone + iPad)."""
    return _platform_cached() == "ios"


def client_platform(is_android: bool, is_iphone: bool) -> str:
    """Return 'android', 'androidtv', 'ios', or 'desktop'."""
    p = _platform_cached()
    if p == "unknown":
        if is_android:
            return "android"
        if is_iphone:
            return "ios"
        return "desktop"
    return p
def _choose_tb_max_hashes(platform: str) -> int:
    """Choose the TorBox hash budget based on client platform (fallback to TB_MAX_HASHES)."""
    p = (platform or '').strip().lower()
    if p == 'desktop':
        return int(TB_MAX_HASHES_DESKTOP or TB_MAX_HASHES)
    if p == 'androidtv':
        return int(TB_MAX_HASHES_ANDROIDTV or TB_MAX_HASHES)
    if p == 'android':
        return int(TB_MAX_HASHES_ANDROID or TB_MAX_HASHES)
    if p in ('iphone','ios'):
        return int(TB_MAX_HASHES_IPHONE or TB_MAX_HASHES)
    return int(TB_MAX_HASHES)


# ---------------------------
# HTTP session (retries)
# ---------------------------
session = requests.Session()

# In-process cache history for expensive cache checks (per infohash)
CACHED_HISTORY: dict[str, bool] = {}

CACHED_HISTORY_LOCK = threading.Lock()
retry = Retry(total=5, connect=5, read=3, redirect=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST"])
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

    # Add: Strip hints for iPhone hash skip (add instead of delete)
    if is_iphone_client():
        try:
            if "infoHash" in stream:
                del stream["infoHash"]  # Conditional remove trigger (safe—no full delete)
        except Exception:
            pass
        try:
            _u = stream.get("url")
            if isinstance(_u, str) and _u.startswith("magnet:"):
                return None  # Drop magnets only (keeps HTTP paths)
        except Exception:
            pass

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

    # NEW: Optional player hints (match frame rate / resolution) to reduce buffering on strict clients.
    MATCH_FRAME_RES = _parse_bool(os.environ.get("MATCH_FRAME_RES", "0"))
    if MATCH_FRAME_RES:
        try:
            bh2 = out.get("behaviorHints")
            if not isinstance(bh2, dict):
                bh2 = {}
            nm = str(out.get("name") or stream.get("name") or "").lower()

            # Resolution hints
            if ("2160p" in nm) or ("4k" in nm):
                bh2.setdefault("resolution", "2160p")
            elif "1080p" in nm:
                bh2.setdefault("resolution", "1080p")
            elif "720p" in nm:
                bh2.setdefault("resolution", "720p")

            # Frame rate hints
            if ("60fps" in nm) or ("60 fps" in nm):
                bh2.setdefault("frameRate", 60)

            out["behaviorHints"] = bh2
        except Exception:
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

def _heuristic_cached(s: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    """Heuristic guess for whether a premium/debrid stream is effectively cached/ready.

    This should be *cheap* and must never throw; it is used as a fallback when a provider
    doesn't explicitly mark cached readiness.
    """
    try:
        info_hash = (meta.get("infohash") or s.get("infoHash") or "").strip().lower()
    except Exception:
        info_hash = ""

    # Require a valid 40-hex infohash for debrid-style heuristics
    if not (isinstance(info_hash, str) and re.fullmatch(r"[0-9a-f]{40}", info_hash or "")):
        return False

    # Grab request-local stats if available (set in filter_and_format/get_streams)
    stats = getattr(_TLS, "stats", None)

    try:
        provider = str(meta.get("provider") or "").upper().strip()
        seeders = int(meta.get("seeders") or 0)
        size_b = int(meta.get("size") or 0)
        size_gb = float(size_b) / float(1024 ** 3) if size_b > 0 else 0.0
        title = str(meta.get("title_raw") or s.get("name") or "")
        desc = str(s.get("description") or "")
        text = f"{title} {desc}"
    except Exception:
        provider = str(meta.get("provider") or "").upper().strip()
        seeders, size_gb, title, text = 0, 0.0, "", ""

    # --- confidence scoring (keeps prior intent, adds episode/completion/tiny-size signals) ---
    conf = 0.0

    # Original-ish rules (kept conceptually; weights tuned to stay in 0..1 range)
    try:
        # Seeders tiers
        if seeders >= 50:
            conf += 0.30
        elif seeders >= 20:
            conf += 0.20

        # Size tiers
        if size_gb >= 2.0:
            conf += 0.20
        elif size_gb >= 1.0:
            conf += 0.10

        # Freshness proxy (age parsed from name/desc when present)
        age_days = _extract_age_days(text)
        if age_days is not None and age_days <= 30:
            conf += 0.20

        # Instant keywords ("instant", "cached", etc.)
        if _looks_instant(text):
            conf += 0.20

        # Resolution keywords
        tl = title.lower()
        if ("2160p" in tl) or ("4k" in tl):
            conf += 0.15
        elif "1080p" in tl:
            conf += 0.10

        # Episode-specific proxy (helps series links)
        if re.search(r"\bS\d{1,2}E\d{1,2}\b", text, re.I) or re.search(r"\bepisode\b", text, re.I):
            conf += 0.10

        # Completion proxy (avoid tiny/partial results)
        if size_gb > 0.50:
            conf += 0.15
        if size_gb > 0 and size_gb < 0.10:
            conf -= 0.20

        # Provider-specific tiny tweaks (keep prior behavior without changing thresholds)
        if provider == "TB" and size_gb >= 10.0:
            conf += 0.05

    except Exception:
        pass

    # Clamp to sane range
    if conf < 0.0:
        conf = 0.0
    if conf > 1.0:
        conf = 1.0

    # Provider-specific thresholds (unchanged)
    ok = False
    try:
        if provider == "RD":
            thr = float(RD_HEUR_THR or 0.70)
            min_gb = float(RD_HEUR_MIN_SIZE_GB or 0.0)
            if size_gb is not None and float(size_gb) < min_gb:
                ok = False
            else:
                ok = conf >= thr
        elif provider == "AD":
            ok = conf >= 0.65
        else:
            ok = conf >= float(MIN_CACHE_CONFIDENCE or 0.60)
    except Exception:
        ok = False

    # Stats bookkeeping + occasional structured logging
    try:
        if stats is not None:
            stats.rd_heur_calls += 1
            if ok:
                stats.rd_heur_true += 1
            else:
                stats.rd_heur_false += 1
            stats.rd_heur_conf_sum += float(conf)

            # Log occasionally to avoid spam; keep it tuneable.
            try:
                log_every = int(os.environ.get("RD_HEUR_LOG_EVERY", "25") or 25)
            except Exception:
                log_every = 25
            if log_every <= 0:
                log_every = 25

            if stats.rd_heur_calls == 1 or (stats.rd_heur_calls % log_every == 0):
                logger.info(
                    "RD_HEUR rid=%s provider=%s calls=%s true=%s false=%s conf_sum=%.2f",
                    _rid(), provider, stats.rd_heur_calls, stats.rd_heur_true, stats.rd_heur_false, float(stats.rd_heur_conf_sum),
                )
    except Exception:
        pass

    return ok

def is_polluted(s: Dict[str, Any], type_: str, season: Optional[int], episode: Optional[int]) -> bool:
    name = (s.get("name") or "").lower()
    desc = (s.get("description") or "").lower()
    filename = s.get("behaviorHints", {}).get("filename", "").lower()
    text = f"{name} {desc} {filename}"
    # Container hint from filename/text
    m_container = re.search(r'\.(MKV|MP4|AVI|M2TS|TS)\b', text, re.I)
    container = (m_container.group(1).upper() if m_container else "UNK")
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
_CACHED_TAG_RE = re.compile(r'\b(?:cached|c)\s*:\s*true\b', re.I)  # Matches e.g. 'CACHED:true' or 'C:true'

# --- AIOStreams 2.23+ tag parsing (no-throw, fast) ---
# We intentionally keep this tiny and defensive. These tags are produced by your AIOStreams formatter
# (examples; some fields may be missing per stream):
#   IH:<hash> TB|RD|ND
#   RG:<group>
#   SE:<num>  NSE:<0-100>  SE★:<stars>  RSEM:<comma tags>
#   RX:<num>  NRX:<0-100>  RX★:<stars>  RXM:<comma tags>  RRXM:<comma tags>
#   BR:<Mbps>  SZ:<GB>
#   VT:<visual tags>  AT:<audio tags>
#   UL:<lang codes>  ULE:<emojis>
#   T:debrid|usenet  C:true|false  P:true|false
#
# Step 0 goal: parse into meta["aio"] with *no behavior change* by itself.
_AIO_T_RE  = re.compile(r"(?:^|\s)T\s*:\s*(debrid|usenet)(?=\s|$)", re.I)
_AIO_C_RE  = re.compile(r"(?:^|\s)C\s*:\s*(true|false)(?=\s|$)", re.I)
_AIO_P_RE  = re.compile(r"(?:^|\s)P\s*:\s*(true|false)(?=\s|$)", re.I)
_AIO_IH_RE = re.compile(r"(?:^|\s)IH\s*:\s*([0-9a-fA-F]{32,40})(?:\s+([A-Za-z|]+))?", re.I)

_AIO_RG_RE      = re.compile(r"(?:^|\s)RG\s*:\s*([^\s]+)", re.I)
_AIO_SE_RE      = re.compile(r"(?:^|\s)SE\s*:\s*(\d+(?:\.\d+)?)", re.I)
_AIO_NSE_RE     = re.compile(r"(?:^|\s)NSE\s*:\s*(\d+(?:\.\d+)?)", re.I)
_AIO_SE_STAR_RE = re.compile(r"(?:^|\s)SE(?:★|\*)\s*:\s*(\d+)", re.I)
_AIO_RSEM_RE    = re.compile(r"(?:^|\s)RSEM\s*:\s*([^\r\n]+)", re.I)

_AIO_RX_RE      = re.compile(r"(?:^|\s)RX\s*:\s*([-+]?\d+(?:\.\d+)?)", re.I)
_AIO_NRX_RE     = re.compile(r"(?:^|\s)NRX\s*:\s*([-+]?\d+(?:\.\d+)?)", re.I)
_AIO_RX_STAR_RE = re.compile(r"(?:^|\s)RX(?:★|\*)\s*:\s*(\d+)", re.I)
_AIO_RXM_RE     = re.compile(r"(?:^|\s)RXM\s*:\s*([^\r\n]+)", re.I)
_AIO_RRXM_RE    = re.compile(r"(?:^|\s)RRXM\s*:\s*([^\r\n]+)", re.I)

_AIO_MT_RE     = re.compile(r"(?:^|\s)MT\s*:\s*([^\r\n]+)", re.I)
_AIO_MY_RE     = re.compile(r"(?:^|\s)MY\s*:\s*(\d{4})", re.I)

_AIO_BR_RE  = re.compile(r"(?:^|\s)BR\s*:\s*(\d+(?:\.\d+)?)", re.I)
_AIO_SZ_RE  = re.compile(r"(?:^|\s)SZ\s*:\s*(\d+(?:\.\d+)?)", re.I)
_AIO_VT_RE  = re.compile(r"(?:^|\s)VT\s*:\s*([^\r\n]+)", re.I)
_AIO_AT_RE  = re.compile(r"(?:^|\s)AT\s*:\s*([^\r\n]+)", re.I)
_AIO_UL_RE  = re.compile(r"(?:^|\s)UL\s*:\s*([^\r\n]+)", re.I)
_AIO_ULE_RE = re.compile(r"(?:^|\s)ULE\s*:\s*([^\r\n]+)", re.I)

_AIO_TAG_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]*\+?")  # handles HDR10+ / DD+ / TrueHD etc.
_AIO_LANG_TOKEN_RE = re.compile(r"[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})?")

def _aio_split_csv(raw: str) -> List[str]:
    try:
        return [x.strip() for x in str(raw or "").split(",") if x.strip()]
    except Exception:
        return []


def _aio_trim_value(val: str, stop_keys: Optional[List[str]] = None) -> str:
    """Trim an extracted inline tag value so it doesn't swallow the next TAG: on the same line."""
    try:
        s = str(val or "").strip()
        if not s:
            return ""
        keys = stop_keys or []
        low = s.lower()
        cut = None
        for k in keys:
            needle = f" {k.lower()}:"
            pos = low.find(needle)
            if pos != -1:
                cut = pos if cut is None else min(cut, pos)
        if cut is not None:
            return s[:cut].strip()
        return s
    except Exception:
        return str(val or "").strip()

def _aio_split_tags(raw: str, upper: bool = True) -> List[str]:
    """Split VT/AT style compact strings into tokens; keeps tokens like HDR10+ and DD+ intact."""
    try:
        toks = [t for t in _AIO_TAG_TOKEN_RE.findall(str(raw or "")) if t]
        if upper:
            toks = [t.upper() for t in toks]
        # de-dup preserve order
        seen = set()
        out = []
        for t in toks:
            if t in seen:
                continue
            seen.add(t)
            out.append(t)
        return out
    except Exception:
        return []

def _aio_split_langs(raw: str) -> List[str]:
    try:
        toks = [t.upper() for t in _AIO_LANG_TOKEN_RE.findall(str(raw or "")) if t]
        seen = set()
        out = []
        for t in toks:
            if t in seen:
                continue
            seen.add(t)
            out.append(t)
        return out
    except Exception:
        return []

def _parse_aio_223_tags(desc: Any) -> Dict[str, Any]:
    """Parse AIOStreams 2.23+ machine-readable tags from stream.description.

    Returns dict with optional keys:
      - ih (str)            : hash from IH:<hash> if present
      - ih_provs (list)     : provider codes after IH, split by '|'
      - type (str)          : 'debrid' or 'usenet' (T:)
      - cached (bool)       : from C:true/false
      - proxied (bool)      : from P:true/false
      - rg (str)            : release group (RG:)
      - se (float)          : SE score (SE:)
      - nse (float)         : normalized SE score 0-100 (NSE:)
      - se_star (int)       : SE★ stars (SE★:)
      - rsem (list[str])    : stream-expression match tags (RSEM:)
      - rx (float)          : regex score (RX:)
      - nrx (float)         : normalized regex score 0-100 (NRX:)
      - rx_star (int)       : RX★ stars (RX★:)
      - rxm (list[str])     : regex match tags (RXM:)
      - rrxm (list[str])    : reverse regex match tags (RRXM:)
      - br_mbps (float)     : bitrate Mbps (BR:)
      - sz_gb (float)       : size GB (SZ:)
      - vt (list[str])      : visual tags (VT:)
      - at (list[str])      : audio tags (AT:)
      - ul (list[str])      : language codes (UL:)
      - ule (str)           : language emojis string (ULE:)

    Never raises.
    """
    out: Dict[str, Any] = {}
    try:
        s = str(desc or "")
    except Exception:
        return out
    if not s.strip():
        return out

    # Single pass over lines keeps it fast and avoids accidental matches in free-form prose.
    try:
        for ln in s.splitlines():
            if not ln:
                continue
            line = ln.strip()
            if not line or ":" not in line:
                continue

            # IH:<hash> <provs>
            if "ih" not in out:
                try:
                    m = _AIO_IH_RE.search(line)
                    if m:
                        ih = (m.group(1) or "").strip()
                        if ih:
                            out["ih"] = ih.lower()
                        provs = (m.group(2) or "").strip()
                        if provs:
                            out["ih_provs"] = [p for p in provs.split("|") if p]
                except Exception:
                    pass

            # Combined line: T:/C:/P: may share a single line.
            if "type" not in out:
                try:
                    mt = _AIO_T_RE.search(line)
                    if mt:
                        out["type"] = (mt.group(1) or "").strip().lower()
                except Exception:
                    pass
            if "cached" not in out:
                try:
                    mc = _AIO_C_RE.search(line)
                    if mc:
                        out["cached"] = ((mc.group(1) or "").strip().lower() == "true")
                except Exception:
                    pass
            if "proxied" not in out:
                try:
                    mp = _AIO_P_RE.search(line)
                    if mp:
                        out["proxied"] = ((mp.group(1) or "").strip().lower() == "true")
                except Exception:
                    pass

            # The remaining tags are typically one per line, but we still search defensively.
            if "rg" not in out:
                try:
                    mrg = _AIO_RG_RE.search(line)
                    if mrg:
                        out["rg"] = (mrg.group(1) or "").strip()
                except Exception:
                    pass

            # Scores (SE/RX) and match reasons
            try:
                if "se" not in out:
                    mse = _AIO_SE_RE.search(line)
                    if mse:
                        out["se"] = _safe_float(mse.group(1), 0.0)
                if "nse" not in out:
                    mnse = _AIO_NSE_RE.search(line)
                    if mnse:
                        out["nse"] = _safe_float(mnse.group(1), 0.0)
                if "se_star" not in out:
                    mses = _AIO_SE_STAR_RE.search(line)
                    if mses:
                        out["se_star"] = _safe_int(mses.group(1), 0)
                if "rsem" not in out:
                    mrsem = _AIO_RSEM_RE.search(line)
                    if mrsem:
                        out["rsem"] = _aio_split_csv(mrsem.group(1))
            except Exception:
                pass

            try:
                if "rx" not in out:
                    mrx = _AIO_RX_RE.search(line)
                    if mrx:
                        out["rx"] = _safe_float(mrx.group(1), 0.0)
                if "nrx" not in out:
                    mnrx = _AIO_NRX_RE.search(line)
                    if mnrx:
                        out["nrx"] = _safe_float(mnrx.group(1), 0.0)
                if "rx_star" not in out:
                    mrxs = _AIO_RX_STAR_RE.search(line)
                    if mrxs:
                        out["rx_star"] = _safe_int(mrxs.group(1), 0)
                if "rxm" not in out:
                    mrxm = _AIO_RXM_RE.search(line)
                    if mrxm:
                        out["rxm"] = _aio_split_csv(_aio_trim_value(mrxm.group(1), stop_keys=["RRXM","RSEM","RX","NRX","VT","AT","UL","ULE","T","C","P","BR","SZ","RG","SE","NSE","MT","MY"]))
                if "rrxm" not in out:
                    mrrxm = _AIO_RRXM_RE.search(line)
                    if mrrxm:
                        out["rrxm"] = _aio_split_csv(_aio_trim_value(mrrxm.group(1), stop_keys=["RXM","RSEM","RX","NRX","VT","AT","UL","ULE","T","C","P","BR","SZ","RG","SE","NSE","MT","MY"]))
                        if "mt" not in out:
                            mmt = _AIO_MT_RE.search(line)
                            if mmt:
                                out["mt"] = _aio_trim_value(mmt.group(1), stop_keys=["MY","VT","AT","UL","ULE","T","C","P","BR","SZ","RG","SE","NSE","RSEM","RX","NRX","RXM","RRXM"]).strip()
                        if "my" not in out:
                            mmy = _AIO_MY_RE.search(line)
                            if mmy:
                                out["my"] = _safe_int(mmy.group(1), 0)
            except Exception:
                pass

            # Bitrate / size
            try:
                if "br_mbps" not in out:
                    mbr = _AIO_BR_RE.search(line)
                    if mbr:
                        out["br_mbps"] = _safe_float(mbr.group(1), 0.0)
                if "sz_gb" not in out:
                    msz = _AIO_SZ_RE.search(line)
                    if msz:
                        out["sz_gb"] = _safe_float(msz.group(1), 0.0)
            except Exception:
                pass

            # Visual/audio tags
            try:
                if "vt" not in out:
                    mvt = _AIO_VT_RE.search(line)
                    if mvt:
                        out["vt"] = _aio_split_tags(_aio_trim_value(mvt.group(1), stop_keys=["AT","UL","ULE","T","C","P","BR","SZ","RG","SE","NSE","RSEM","RX","NRX","RXM","RRXM","MT","MY"]), upper=True)
                if "at" not in out:
                    mat = _AIO_AT_RE.search(line)
                    if mat:
                        out["at"] = _aio_split_tags(_aio_trim_value(mat.group(1), stop_keys=["VT","UL","ULE","T","C","P","BR","SZ","RG","SE","NSE","RSEM","RX","NRX","RXM","RRXM","MT","MY"]), upper=True)
            except Exception:
                pass

            # Language tags
            try:
                if "ul" not in out:
                    mul = _AIO_UL_RE.search(line)
                    if mul:
                        out["ul"] = _aio_split_langs(_aio_trim_value(mul.group(1), stop_keys=["ULE","VT","AT","T","C","P","BR","SZ","RG","SE","NSE","RSEM","RX","NRX","RXM","RRXM","MT","MY"]))
                if "ule" not in out:
                    mule = _AIO_ULE_RE.search(line)
                    if mule:
                        out["ule"] = (mule.group(1) or "").strip()
            except Exception:
                pass

    except Exception:
        # Fully defensive: never throw from parsing.
        return out

    return out

    try:
        m = _AIO_IH_RE.search(s)
        if m:
            ih = (m.group(1) or "").strip()
            if ih:
                out["ih"] = ih.lower()
            provs = (m.group(2) or "").strip()
            if provs:
                out["ih_provs"] = [p for p in provs.split("|") if p]
    except Exception:
        pass

    try:
        mt = _AIO_T_RE.search(s)
        if mt:
            out["type"] = (mt.group(1) or "").strip().lower()
    except Exception:
        pass

    try:
        mc = _AIO_C_RE.search(s)
        if mc:
            out["cached"] = ((mc.group(1) or "").strip().lower() == "true")
    except Exception:
        pass

    try:
        mp = _AIO_P_RE.search(s)
        if mp:
            out["proxied"] = ((mp.group(1) or "").strip().lower() == "true")
    except Exception:
        pass

    return out

def _is_controlled_playback_url(url: Any) -> bool:
    """True if the URL is an AIOStreams-controlled playback endpoint (safe even if P:false)."""
    try:
        u = str(url or "")
        if not u:
            return False
        return bool(re.search(r"/api/v1/(?:debrid|usenet)/playback/", u))
    except Exception:
        return False


def _aio_tagged_instant(aio: Any, url: Any = None) -> Optional[bool]:
    """Return True/False if AIO tags indicate 'instant/ready', else None.

    Option C:
      instant if cached==true AND (proxied==true OR url is a controlled playback endpoint).
    """
    try:
        if not isinstance(aio, dict):
            return None
        if ("cached" in aio) and ("proxied" in aio):
            cached = aio.get("cached", None)
            proxied = aio.get("proxied", None)
            if cached is None or proxied is None:
                return None
            if bool(cached) is False:
                return False
            if bool(proxied) is True:
                return True
            return _is_controlled_playback_url(url)
    except Exception:
        return None
    return None


# ---------------------------
# P1 Hybrid A+C scoring helpers (AIOStreams v2.23+ fields)
# ---------------------------

_P1_BUCKET_OTHER = 6
_P1_BUCKET_LOWQ = 99

# Recognized "class" names from AIOStreams configs (RRXM/RXM/RSEM carry these).
# Lower bucket value is better.
_P1_CLASS_MAP = [
    ("REMUX T1", 0, "REMUX_T1"),
    ("REMUX T2", 1, "REMUX_T2"),
    ("BLURAY T1", 2, "BLURAY_T1"),
    ("BLURAY T2", 3, "BLURAY_T2"),
    ("WEB T1", 4, "WEB_T1"),
    ("WEB SCENE", 4, "WEB_SCENE"),
    ("WEB T2", 5, "WEB_T2"),
]

# Low-quality tokens (avoid false positives like WEB-DL).
_P1_LOWQ_PATTERNS = [
    r"\bCAM\b",
    r"\bHDCAM\b",
    r"\bTS\b",
    r"\bHDTS\b",
    r"\bTELESYNC\b",
    r"\bT[ -]?SYNC\b",
    r"\bTC\b",
    r"\bTELECINE\b",
    r"\bSCREENER\b",
]

def _p1_ac_labels(aio: Any) -> List[str]:
    """Collect rule-hit labels from AIO 2.23 fields; returns uppercased list (dedup, order preserved)."""
    out: List[str] = []
    if not isinstance(aio, dict):
        return out
    try:
        raw_lists = []
        for k in ("rrxm", "rxm", "rsem"):
            v = aio.get(k)
            if isinstance(v, list):
                raw_lists.append(v)
            elif isinstance(v, str) and v.strip():
                raw_lists.append([v])
        seen = set()
        for lst in raw_lists:
            for it in lst:
                t = str(it or "").strip()
                if not t:
                    continue
                u = t.upper()
                if u in seen:
                    continue
                seen.add(u)
                out.append(u)
    except Exception:
        return out
    return out

def _p1_ac_bucket(aio: Any, text_upper: str) -> Tuple[int, str]:
    """Return (bucket_rank, bucket_name).

    NOTE: In real-world AIOStreams output, many debrid streams (including BluRay REMUX) may not carry RXM/RRXM labels,
    and instead only expose RSEM concept tags (cached,4K,DV,hdr,hevc...). If we rely on rule-hit labels only, REMUX
    can be misclassified as OTHER/BLURAY. So we add a conservative text-based override for REMUX and a light fallback
    for BLURAY/WEB when labels are absent.
    """
    # Low-quality guard first.
    try:
        for pat in _P1_LOWQ_PATTERNS:
            if re.search(pat, text_upper):
                return (_P1_BUCKET_LOWQ, "LOWQ")
    except Exception:
        pass

    labels = _p1_ac_labels(aio)

    # Strong override: if the stream text explicitly says REMUX, treat it as REMUX even if upstream labels are missing.
    # This fixes common cases like "2160p BluRay REMUX ..." where RRXM/RXM are blank.
    try:
        if "REMUX" in text_upper and not any("REMUX" in lab for lab in labels):
            return (0, "REMUX_TEXT")
    except Exception:
        pass

    # Primary: rule-hit label mapping (if present)
    for key, b, name in _P1_CLASS_MAP:
        try:
            if any(key in lab for lab in labels):
                return (b, name)
        except Exception:
            continue

    # Fallback: text-based bucket when no rule labels exist at all
    try:
        t = text_upper
        if "REMUX" in t:
            return (0, "REMUX_TEXT")
        if ("WEB-DL" in t) or ("WEBRIP" in t) or ("WEBDL" in t) or ("WEB RIP" in t):
            return (4, "WEB_TEXT")
        if ("BLURAY" in t) or ("BLU-RAY" in t) or re.search(r"\bBD\b", t):
            return (2, "BLURAY_TEXT")
    except Exception:
        pass

    return (_P1_BUCKET_OTHER, "OTHER")

def _p1_ac_qscore(aio: Any, bucket: int, text_upper: str) -> float:
    """Higher is better. Designed to be robust across sources (caps numeric contributions)."""
    q = 0.0
    if not isinstance(aio, dict):
        return q

    # Visual tags (VT) and audio tags (AT) are uppercased lists by parser.
    vt = aio.get("vt") if isinstance(aio.get("vt"), list) else []
    at = aio.get("at") if isinstance(aio.get("at"), list) else []

    try:
        vt_set = set([str(t).upper() for t in vt if str(t).strip()])
        at_set = set([str(t).upper() for t in at if str(t).strip()])
    except Exception:
        vt_set, at_set = set(), set()

    # Visual bonuses
    if "DV" in vt_set or "DOVI" in vt_set or "DOLBYVISION" in vt_set:
        q += 12.0
    if any(("HDR10+" in t) or ("HDR10++" in t) for t in vt_set):
        q += 8.0
    elif "HDR" in vt_set or "HDR10" in vt_set or "HDR10PLUS" in vt_set:
        q += 5.0
    if "10BIT" in vt_set or "10-BIT" in vt_set:
        q += 2.0
    if "AV1" in vt_set:
        q += 3.0
    if "HEVC" in vt_set or "X265" in vt_set or "H265" in vt_set:
        q += 2.0

    # Audio bonuses
    if "ATMOS" in at_set:
        q += 5.0
    if "TRUEHD" in at_set:
        q += 4.0
    if ("DTSX" in at_set) or ("DTS:X" in at_set):
        q += 4.0
    if any(t.startswith("DTS-HD") for t in at_set):
        q += 3.0
    if "DTS" in at_set:
        q += 1.5
    if ("EAC3" in at_set) or ("DD+" in at_set) or ("DDP" in at_set):
        q += 2.0
    if ("AC3" in at_set) or ("DD" in at_set):
        q += 1.0

    # Stars (prefer SE★ when present)
    try:
        stars = int(aio.get("se_star") or 0)
        if stars <= 0:
            stars = int(aio.get("rx_star") or 0)
        stars = max(0, min(5, stars))
        q += float(stars) * 1.0
    except Exception:
        pass

    # Normalized SE score is safe-ish within a source; cap contribution tightly.
    try:
        nse = float(aio.get("nse") or 0.0)
        nse = max(0.0, min(100.0, nse))
        q += nse * 0.02  # up to +2.0
    except Exception:
        pass

    # Bitrate/size: tiny nudge only (avoid dominating across configs).
    try:
        br = float(aio.get("br_mbps") or 0.0)
        br = max(0.0, min(120.0, br))
        q += br * 0.02  # up to +2.4
    except Exception:
        pass
    try:
        sz = float(aio.get("sz_gb") or 0.0)
        sz = max(0.0, min(200.0, sz))
        q += sz * 0.01  # up to +2.0
    except Exception:
        pass

    # Bucket-aware penalty for LOWQ (belt and suspenders).
    if bucket >= _P1_BUCKET_LOWQ:
        q -= 50.0

    # Clamp (stability)
    if q > 100.0:
        q = 100.0
    if q < -100.0:
        q = -100.0
    return q

def _p1_ac_features(aio: Any, text_upper: str) -> Tuple[int, str, float]:
    b, name = _p1_ac_bucket(aio, text_upper)
    q = _p1_ac_qscore(aio, b, text_upper)
    return b, name, q

def classify(s: Dict[str, Any]) -> Dict[str, Any]:
    bh = s.get("behaviorHints", {}) or {}
    name = s.get("name", "").lower()
    desc_raw = s.get("description", "") or ""
    desc = str(desc_raw).lower()
    filename = (bh.get("filename", "") or "").lower()
    text = f"{name} {desc} {filename}"

    # Step 0: parse AIOStreams 2.23+ machine tags (prefer persisted bh["wrap_aio"] if available)
    aio = (bh.get("wrap_aio") if isinstance(bh.get("wrap_aio"), dict) else _parse_aio_223_tags(desc_raw))
    aio_cached = None
    try:
        if isinstance(aio, dict):
            aio_cached = aio.get("cached", None)
    except Exception:
        aio_cached = None

    # +++ Usenet Mod 1: cached tag parsing (matches e.g. 'CACHED:true')
    cached = True if _CACHED_TAG_RE.search(text) else None
    # Step 1: if AIO 2.23+ C: tag is present (true/false) and USE_AIO_READY is enabled, trust it.
    try:
        if USE_AIO_READY and isinstance(aio, dict) and (aio.get("cached", None) is not None):
            cached = bool(aio.get("cached"))
    except Exception:
        pass
    # Container (from filename extension)
    m_container = re.search(r'\.(MKV|MP4|AVI|M2TS|TS)\b', text, re.I)
    container = (m_container.group(1).upper() if m_container else 'UNK')

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
            'DEBRIDLINK': 'DL',
            'ND': 'ND', 'NZBDAV': 'ND',
            'EW': 'EW', 'EWEKA': 'EW',
            'NG': 'NG', 'NZGEEK': 'NG',
        }
        provider = provider_map.get(tok, provider)

    # DL association / clarity:
    # - Only treat Debrid-Link as provider when explicitly mentioned (DEBRIDLINK / DEBRID-LINK / 🟢DL),
    #   NOT from WEB-DL release tags.
    # - If Debrid-Link and a base provider token is also present, label as DL-<BASE> (e.g., DL-TB) for logging clarity.
    up = text.upper()
    has_debridlink = bool(re.search(r"\bDEBRID[- ]?LINK\b", text, re.I) or ("🟢DL" in up) or ("DL⚡" in up))
    has_webdl = bool(re.search(r"\bWEB-?DL\b", text, re.I))
    # Detect associated base provider independently
    m_assoc = re.search(r"\b(TB|TORBOX|RD|REAL[- ]?DEBRID|REALDEBRID|AD|ALLDEBRID|ALL[- ]?DEBRID|PM|PREMIUMIZE|ND|NZBDAV|EW|EWEKA|NG|NZGEEK)\b", text, re.I)
    assoc = None
    if m_assoc:
        tok2 = m_assoc.group(1).upper().replace(' ', '').replace('-', '')
        assoc_map = {
            'TB': 'TB', 'TORBOX': 'TB',
            'RD': 'RD', 'REALDEBRID': 'RD',
            'PM': 'PM', 'PREMIUMIZE': 'PM',
            'AD': 'AD', 'ALLDEBRID': 'AD', 'ALLDEBRID': 'AD',
            'ND': 'ND', 'NZBDAV': 'ND',
            'EW': 'EW', 'EWEKA': 'EW',
            'NG': 'NG', 'NZGEEK': 'NG',
        }
        assoc = assoc_map.get(tok2, tok2)

    if has_debridlink and DL_ASSOC_PARSE:
        # Normalize to DL or DL-<BASE>
        if assoc:
            provider = f"DL-{assoc}"
        else:
            provider = "DL"
    else:
        # If provider accidentally became DL from stray "DL" tokens elsewhere,
        # and we only see WEB-DL (no Debrid-Link), restore associated base provider if present.
        if has_webdl and provider == "DL" and assoc:
            provider = assoc

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

        # Extra heuristic for emoji-tagged formatter names like "🟢TB⚡ ..."
    if provider == "UNK":
        up = text.upper()
        if "🟢TB" in up or " TB⚡" in up or re.search(r"(?<![A-Z0-9])TB(?![A-Z0-9])", up):
            provider = "TB"
        elif "🟢RD" in up or re.search(r"(?<![A-Z0-9])RD(?![A-Z0-9])", up):
            provider = "RD"
        elif "🟢AD" in up or re.search(r"(?<![A-Z0-9])AD(?![A-Z0-9])", up):
            provider = "AD"
        elif "🟢DL" in up or re.search(r"(?<![A-Z0-9])DL(?![A-Z0-9])", up):
            provider = "DL"
        elif "USENET" in up or "NZB" in up or "NZBDAV" in up:
            provider = "ND"

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
    m_seeds = re.search(r"(\d+)\s*(?:seeds?|seeders?)\b", text, re.I)
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
        m2 = re.search(r'"hash"\s*:\s*"([0-9a-fA-F]{8,64})"', url)
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

    # P1 Hybrid A+C: compute bucket + qscore from AIOStreams 2.23 fields (merge/sort only).
    p1_bucket = _P1_BUCKET_OTHER
    p1_class = "OTHER"
    p1_q = 0.0
    try:
        if P1_MODE == "ac":
            p1_bucket, p1_class, p1_q = _p1_ac_features(aio, text.upper())
    except Exception:
        pass

    return {
        "provider": provider,
        "cached": cached,
        "aio": aio,
        "p1_bucket": p1_bucket,
        "p1_class": p1_class,
        "p1_q": p1_q,
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
        "container": container,
    }


# ---------------------------
# Expected metadata (real TMDB fetch)
# ---------------------------
@lru_cache(maxsize=2000)
def get_expected_metadata(type_: str, id_: str) -> dict:
    """Fetch expected metadata for title/year validation and nicer series labels.

    Supports Stremio ids like:
      - movie:  tt0111161
      - series: tt0944947:1:1
      - tmdb:   tmdb:1399:1:1 (optional)
      - numeric tmdb: 1399 (optional)

    Prefers Trakt (if TRAKT_CLIENT_ID set) and falls back to TMDB (/find for IMDb ids).

    Returns:
      title, year, tmdb_id, episode_title, season, episode, source, imdb_id
    """
    if not (TRAKT_CLIENT_ID or TMDB_API_KEY):
        return {
            "title": "",
            "year": None,
            "tmdb_id": None,
            "episode_title": "",
            "season": None,
            "episode": None,
            "source": "",
            "imdb_id": "",
        }

    parts = (id_ or "").split(":")
    base = parts[0] if parts else ""
    season = episode = None
    tmdb_id = None
    imdb_id = None

    # Parse stremio id variants
    try:
        if base == "tmdb" and len(parts) >= 2 and str(parts[1]).isdigit():
            tmdb_id = int(parts[1])
            if len(parts) >= 4 and str(parts[2]).isdigit() and str(parts[3]).isdigit():
                season, episode = int(parts[2]), int(parts[3])
        elif str(base).isdigit():
            tmdb_id = int(base)
        elif str(base).startswith("tt"):
            imdb_id = str(base)
            if len(parts) >= 3 and str(parts[1]).isdigit() and str(parts[2]).isdigit():
                season, episode = int(parts[1]), int(parts[2])
    except Exception:
        pass

    # --- Prefer Trakt (VIP-friendly, no TMDB key required) ---
    if imdb_id and TRAKT_CLIENT_ID and not TMDB_FORCE_IMDB:
        try:
            kind = "movie" if type_ == "movie" else "show"
            headers = {
                "trakt-api-version": "2",
                "trakt-api-key": TRAKT_CLIENT_ID,
                "Content-Type": "application/json",
            }
            search_url = f"https://api.trakt.tv/search/imdb/{imdb_id}?type={kind}"
            r = session.get(search_url, headers=headers, timeout=TMDB_TIMEOUT)
            data = r.json() if getattr(r, "ok", False) else []

            if isinstance(data, list) and data:
                obj = data[0].get(kind) if isinstance(data[0], dict) else None
                if isinstance(obj, dict):
                    title = (obj.get("title") or "").strip()
                    year = obj.get("year")
                    ids = obj.get("ids") or {}
                    trakt_id = ids.get("trakt") or ids.get("slug")
                    ep_title = ""

                    # Pull TMDB id from Trakt ids if present (useful for fallback/telemetry)
                    tmdb_from_trakt = ids.get("tmdb")
                    if tmdb_id is None and tmdb_from_trakt and str(tmdb_from_trakt).isdigit():
                        tmdb_id = int(tmdb_from_trakt)

                    # Canonical EN episode title (series only)
                    if type_ == "series" and season is not None and episode is not None and trakt_id:
                        ep_url = f"https://api.trakt.tv/shows/{trakt_id}/seasons/{season}/episodes/{episode}"
                        r2 = session.get(ep_url, headers=headers, timeout=TMDB_TIMEOUT)
                        ep = r2.json() if getattr(r2, "ok", False) else {}
                        if isinstance(ep, dict):
                            ep_title = (ep.get("title") or "").strip()


                            title_original = ""
                            try:
                                if TMDB_API_KEY and tmdb_id:
                                    kind2 = "movie" if type_ == "movie" else "tv"
                                    meta_url2 = f"https://api.themoviedb.org/3/{kind2}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
                                    r0 = session.get(meta_url2, timeout=TMDB_TIMEOUT)
                                    j0 = r0.json() if getattr(r0, "ok", False) else {}
                                    title_original = (j0.get("original_title") if kind2 == "movie" else j0.get("original_name")) or ""
                            except Exception:
                                title_original = ""

                    # Ensure IMDb id (prefer Trakt ids, else TMDB external_ids)
                    imdb_from_trakt = (ids.get("imdb") or "").strip()
                    if (not imdb_from_trakt) and TMDB_API_KEY:
                        tmdb_id_fallback = tmdb_from_trakt or tmdb_id
                        if tmdb_id_fallback:
                            imdb_from_trakt = _tmdb_external_imdb_id(type_, str(tmdb_id_fallback)) or ""

                    return {
                        "title": title,
                        "year": int(year) if str(year).isdigit() else year,
                        
                        "title_original": str(title_original).strip(),"tmdb_id": tmdb_id,
                        "episode_title": ep_title,
                        "season": season,
                        "episode": episode,
                        "source": "trakt",
                        "imdb_id": imdb_from_trakt or "",
                    }
        except Exception as e:
            logger.warning(f"TRAKT_EXPECTED_META_FAIL type={type_} id={id_}: {e}")

    # --- TMDB fallback (handles tmdb ids and IMDb via /find) ---
    if not TMDB_API_KEY:
        return {
            "title": "",
            "year": None,
            "tmdb_id": tmdb_id,
            "episode_title": "",
            "season": season,
            "episode": episode,
            "source": "",
            "imdb_id": imdb_id or "",
        }

    try:
        # IMDb -> TMDB via /find
        if imdb_id and tmdb_id is None:
            find_url = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={TMDB_API_KEY}&external_source=imdb_id"
            r = session.get(find_url, timeout=TMDB_TIMEOUT)
            j = r.json() if getattr(r, "ok", False) else {}
            if type_ == "movie":
                tmdb_id = (j.get("movie_results") or [{}])[0].get("id")
            else:
                tmdb_id = (j.get("tv_results") or [{}])[0].get("id")
            if tmdb_id is not None and str(tmdb_id).isdigit():
                tmdb_id = int(tmdb_id)
            else:
                tmdb_id = None

        if tmdb_id is None:
            logger.debug(f"EXPECTED_META_BLANK type={type_} id={id_}: no tmdb id")
            return {
                "title": "",
                "year": None,
                "tmdb_id": None,
                "episode_title": "",
                "season": season,
                "episode": episode,
                "source": "tmdb",
                "imdb_id": imdb_id or "",
            }

        kind = "movie" if type_ == "movie" else "tv"
        meta_url = f"https://api.themoviedb.org/3/{kind}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
        r = session.get(meta_url, timeout=TMDB_TIMEOUT)
        j = r.json() if getattr(r, "ok", False) else {}

        title = (j.get("title") if kind == "movie" else j.get("name")) or ""
        title_original = (j.get("original_title") if kind == "movie" else j.get("original_name")) or ""
        date_str = (j.get("release_date") if kind == "movie" else j.get("first_air_date")) or ""
        year = int(date_str[:4]) if (len(date_str) >= 4 and date_str[:4].isdigit()) else None

        ep_title = ""
        if type_ == "series" and season is not None and episode is not None:
            ep_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season}/episode/{episode}?api_key={TMDB_API_KEY}&language=en-US"
            r2 = session.get(ep_url, timeout=TMDB_TIMEOUT)
            j2 = r2.json() if getattr(r2, "ok", False) else {}
            ep_title = (j2.get("name") or "").strip()

        # Ensure IMDb from TMDB external_ids
        imdb_from_tmdb = _tmdb_external_imdb_id(type_, str(tmdb_id)) or ""

        return {
            "title": str(title).strip(),
            "year": year,
            "tmdb_id": tmdb_id,
            "episode_title": ep_title,
            "season": season,
            "episode": episode,
            "source": "tmdb",
            "imdb_id": imdb_from_tmdb,
        }
    except Exception as e:
        logger.warning(f"TMDB_EXPECTED_META_FAIL type={type_} id={id_}: {e}")
        return {
            "title": "",
            "year": None,
            "tmdb_id": tmdb_id,
            "episode_title": "",
            "season": season,
            "episode": episode,
            "source": "tmdb",
            "imdb_id": imdb_id or "",
        }

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
    container = (m.get('container') or '').upper().strip()

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
    ep_title = (expected.get('episode_title') or '').strip()
    if season and episode and ep_title:
        # Series: Show SxxEyy — Canonical EN episode title (from Trakt/TMDB)
        s['title'] = _truncate(f"{base_title}{ep_tag} — {ep_title}" + (f" ({year})" if year else ""), MAX_TITLE_CHARS)
    else:
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
    con_disp = container if container and str(container).upper() not in ('UNK','UNKNOWN') else ''
    con_disp = container if container and str(container).upper() not in ('UNK','UNKNOWN') else ''
    line2_bits = [p for p in [size_str, con_disp, seeds_str, (f"Grp {group}" if group else '')] if p]
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

    # Helpful hints for debugging/stats (safe under behaviorHints).
    # These are ignored by strict clients, but make logs and troubleshooting much easier.
    prov_hint = str((m or {}).get("provider") or raw_bh.get("provider") or "").upper().strip()
    if prov_hint:
        bh_out["provider"] = prov_hint
    src_hint = str((m or {}).get("source") or raw_bh.get("source") or "").strip()
    if src_hint:
        bh_out["source"] = src_hint
    if is_confirmed:
        bh_out["cached"] = True
    elif isinstance(raw_bh.get("cached"), (bool, str)):
        bh_out["cached"] = raw_bh.get("cached")
    elif isinstance((m or {}).get("cached"), (bool, str)):
        bh_out["cached"] = (m or {}).get("cached")
    elif cached_hint == "LIKELY":
        bh_out["cached"] = "LIKELY"

    # Preserve wrapper supplier tag (AIO/P2) on delivered streams so WRAP_COUNTS out.by_supplier stays correct.
    # NOTE: behaviorHints.source may be WEB/BLURAY/REMUX/etc; supplier is tracked separately in wrap_src/source_tag.
    default_sup = str(AIO_TAG or "AIO").upper()
    try:
        supplier = (raw_bh.get("wrap_src") or raw_bh.get("source_tag") or "").strip()
        supplier_u = str(supplier).upper() if supplier else ""
        allowed = {str(AIO_TAG).upper(), str(PROV2_TAG).upper()}
        if supplier_u in allowed:
            bh_out["wrap_src"] = supplier_u
            bh_out["source_tag"] = supplier_u
        else:
            bh_out["wrap_src"] = default_sup
            bh_out["source_tag"] = default_sup
    except Exception:
        bh_out["wrap_src"] = default_sup
        bh_out["source_tag"] = default_sup

    out = {
        "name": name,
        "description": description,
        "url": out_url,
        "behaviorHints": bh_out,
    }

    # Add infoHash if available (helps Stremio Android / debrid torrent playback)
    h = (
        (m.get("infohash") if isinstance(m, dict) else None)
        or (m.get("infoHash") if isinstance(m, dict) else None)
        or (raw_s.get("infoHash") if isinstance(raw_s, dict) else None)
        or (raw_s.get("infohash") if isinstance(raw_s, dict) else None)
        or ""
    )
    h = (h or "").lower().strip()
    if h:
        out["infoHash"] = h

    return out
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


def _fetch_streams_from_base_with_meta(base: str, auth: str, type_: str, id_: str, tag: str, timeout: float = REQUEST_TIMEOUT, no_retry: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch provider streams and return (streams, meta).

    meta keys (safe for logs/debug):
      - tag, ok, status, bytes, count, ms
      - http_ms, read_ms, json_ms, post_ms
      - ms_provider (upstream/provider internal timing if present in JSON)
      - wait_ms (time spent waiting on the future join; filled by caller)
      - err
    """
    meta: dict[str, Any] = {
        "tag": str(tag),
        "ok": False,
        "status": 0,
        "bytes": 0,
        "count": 0,
        "ms": 0,
        "http_ms": 0,
        "read_ms": 0,
        "json_ms": 0,
        "post_ms": 0,
        "ms_provider": 0,
        "wait_ms": 0,
        "err": "",
    }
    t0 = time.monotonic()
    if not base:
        meta["err"] = "no_base"
        return [], meta
    url = f"{base}/stream/{type_}/{id_}.json"
    headers = _auth_headers(auth)
    try:
        sess = fast_session if no_retry else session

        # HTTP timing: stream=True so we can separate response header time (http_ms)
        # from full body read time (read_ms).
        t_http0 = time.monotonic()
        with sess.get(url, headers=headers, timeout=timeout, stream=True) as resp:
            meta["http_ms"] = int((time.monotonic() - t_http0) * 1000)
            meta["status"] = int(getattr(resp, "status_code", 0) or 0)

            # Read/download timing (body)
            t_read0 = time.monotonic()
            raw = getattr(resp, "content", b"") or b""
            meta["read_ms"] = int((time.monotonic() - t_read0) * 1000)
            meta["bytes"] = int(len(raw))

            if meta["status"] != 200:
                meta["err"] = f"http_{meta['status']}"
                return [], meta

            # JSON decode timing
            t_json0 = time.monotonic()
            data = json.loads(raw) if raw else {}
            meta["json_ms"] = int((time.monotonic() - t_json0) * 1000)

            # Upstream/provider timing (if provided by JSON)
            provider_ms = 0
            try:
                if isinstance(data, dict):
                    provider_ms = int(data.get("ms") or (data.get("meta") or {}).get("ms") or 0)
            except Exception:
                provider_ms = 0
            meta["ms_provider"] = provider_ms

            streams = []
            if isinstance(data, dict):
                streams = (data.get("streams") or [])[:INPUT_CAP]

            # Lightweight post-load tagging timing (does not expose tokens)
            t_post0 = time.monotonic()
            for s in streams:
                if not isinstance(s, dict):
                    continue
                bh = s.get("behaviorHints")
                if not isinstance(bh, dict):
                    bh = {}
                    s["behaviorHints"] = bh
                # Always tag supplier (AIO/P2) for downstream counters/debug.
                bh["wrap_src"] = tag
                bh["source_tag"] = tag
                # Parse and persist AIOStreams 2.23+ machine tags EARLY (before we rewrite description for UI).
                # This lets later stages (sorting/counts/logs) use C:/P:/T:/UL:/VT:/NSE etc even if description gets overwritten.
                try:
                    desc_raw = s.get("description", "") or ""
                    aio_tags = _parse_aio_223_tags(desc_raw)
                    if isinstance(aio_tags, dict) and aio_tags:
                        bh["wrap_aio"] = aio_tags  # full tag dict (debuggable; not shown to user UI)
                        # Step 1 truth signals: trust C:/P:/T: when present (gated by USE_AIO_READY).
                        if USE_AIO_READY:
                            if aio_tags.get("cached", None) is not None:
                                bh["cached"] = bool(aio_tags.get("cached"))
                            if aio_tags.get("proxied", None) is not None:
                                bh["wrap_proxied"] = bool(aio_tags.get("proxied"))
                            if aio_tags.get("type"):
                                bh["wrap_type"] = str(aio_tags.get("type"))
                except Exception:
                    pass
            meta["post_ms"] = int((time.monotonic() - t_post0) * 1000)

            meta["count"] = int(len(streams))
            meta["ok"] = True
            return streams, meta

    except requests.Timeout:
        meta["err"] = "timeout"
        return [], meta
    except json.JSONDecodeError:
        meta["err"] = "json"
        return [], meta
    except Exception as e:
        meta["err"] = type(e).__name__
        return [], meta
    finally:
        meta["ms"] = int((time.monotonic() - t0) * 1000)



def get_streams_single(base: str, auth: str, type_: str, id_: str, tag: str, timeout: float = REQUEST_TIMEOUT, no_retry: bool = False) -> tuple[list[dict[str, Any]], int, int, dict[str, Any], int]:
    """Fetch a single provider and return (streams, count, ms_remote, meta, local_ms)."""
    t0 = time.monotonic()
    streams, meta = _fetch_streams_from_base_with_meta(base, auth, type_, id_, tag, timeout=timeout, no_retry=no_retry)
    local_ms = int((time.monotonic() - t0) * 1000)
    ms_remote = int(meta.get("ms") or 0)
    return streams, int(len(streams)), ms_remote, meta, local_ms

def get_streams(type_: str, id_: str, *, is_android: bool = False, is_iphone: bool = False, client_timeout_s: float | None = None):
    """
    Fetch streams from:
      - AIO_BASE (debrid instance)
      - PROV2_BASE (usenet instance)

    Patch 3 adds optional AIO cache / soft-timeout:
      - AIO_CACHE_MODE=off  : previous behavior (wait for both)
      - AIO_CACHE_MODE=swr  : use cached AIO immediately (stale-while-revalidate) and refresh in background
      - AIO_CACHE_MODE=soft : wait up to AIO_SOFT_TIMEOUT_S for AIO; if not ready, fall back to cached AIO (if any)

    Notes:
      - This function must NOT use a "with ThreadPoolExecutor(...)" context manager,
        because that would block on exit and defeat early-return/caching.
    """
    if client_timeout_s is None:
        client_timeout_s = ANDROID_STREAM_TIMEOUT if is_android else DESKTOP_STREAM_TIMEOUT

    t0 = time.monotonic()
    deadline = t0 + float(client_timeout_s)

    # In this wrapper, extras are not used (movie/series id already encodes what upstream needs).
    # Kept here for future series extras support.
    extras = None
    upstream_id = _canonical_id_for_upstream(id_)
    aio_url = f"{AIO_BASE}/stream/{type_}/{upstream_id}.json" if AIO_BASE else ""
    prov2_url = f"{PROV2_BASE}/stream/{type_}/{upstream_id}.json" if PROV2_BASE else ""


    aio_streams: List[Dict[str, Any]] = []
    p2_streams: List[Dict[str, Any]] = []
    aio_in = 0
    prov2_in = 0
    aio_ms_remote = 0
    p2_ms_remote = 0
    aio_ms_local = 0
    p2_ms_local = 0

    aio_meta: dict[str, Any] = {}
    p2_meta: dict[str, Any] = {}

    aio_key = _aio_cache_key(type_, id_, extras)
    cached = _aio_cache_get(aio_key)

    iphone_usenet_mode = bool(is_iphone and IPHONE_USENET_ONLY)
    if iphone_usenet_mode:
        # iOS/iphone usenet-only: do NOT use AIO cache and do NOT fetch AIO at all
        cached = None
        aio_meta = {'tag': AIO_TAG, 'ok': False, 'err': 'skipped_iphone_usenet_only'}

    aio_fut = None
    p2_fut = None

    if AIO_BASE and not iphone_usenet_mode:
        aio_fut = FETCH_EXECUTOR.submit(get_streams_single, AIO_BASE, AIO_AUTH, type_, upstream_id, AIO_TAG, (ANDROID_AIO_TIMEOUT if is_android else DESKTOP_AIO_TIMEOUT))
    elif iphone_usenet_mode:
        logger.info("AIO skipped rid=%s reason=iphone_usenet_only", _rid())
    else:
        aio_meta = {'tag': AIO_TAG, 'ok': False, 'err': 'no_base'}
        logger.warning("AIO disabled rid=%s reason=no_base", _rid())
    if PROV2_BASE:
        p2_fut = FETCH_EXECUTOR.submit(get_streams_single, PROV2_BASE, PROV2_AUTH, type_, upstream_id, PROV2_TAG, (ANDROID_P2_TIMEOUT if is_android else DESKTOP_P2_TIMEOUT))
    def _harvest_p2():
        nonlocal p2_streams, prov2_in, p2_meta, p2_ms_remote, p2_ms_local
        if not p2_fut:
            return
        remaining = max(0.05, deadline - time.monotonic())
        t_wait0 = time.monotonic()
        try:
            p2_streams, prov2_in, p2_ms_remote, p2_meta, p2_ms_local = p2_fut.result(timeout=remaining)
        except FuturesTimeoutError:
            p2_streams, prov2_in, p2_ms_remote, p2_meta, p2_ms_local = [], 0, 0, {'tag': PROV2_TAG, 'ok': False, 'err': 'timeout'}, 0
        except Exception as e:
            p2_streams, prov2_in, p2_ms_remote, p2_meta, p2_ms_local = [], 0, 0, {'tag': PROV2_TAG, 'ok': False, 'err': f'error:{type(e).__name__}'}, 0
        try:
            if isinstance(p2_meta, dict):
                p2_meta["wait_ms"] = int((time.monotonic() - t_wait0) * 1000)
        except Exception:
            pass

    mode = AIO_CACHE_MODE

    if mode == "swr" and cached is not None:
        # Use cached AIO instantly; refresh in background
        aio_streams, aio_in, cached_ms = cached
        aio_ms_remote = int(cached_ms or 0)
        aio_ms_local = 0
        aio_meta = {'tag': AIO_TAG, 'ok': True, 'status': 200, 'err': 'cache_hit', 'count': int(aio_in or 0), 'ms': int(aio_ms_remote or 0), 'wait_ms': 0}

        if aio_fut:
            aio_fut.add_done_callback(_make_aio_cache_update_cb(aio_key))

        _harvest_p2()

    elif mode == "soft" and aio_fut is not None:
        soft = float(AIO_SOFT_TIMEOUT_S or 0)
        if soft <= 0:
            # behave like "off"
            soft = max(0.05, deadline - time.monotonic())

        t_wait0 = time.monotonic()
        try:
            aio_streams, aio_in, aio_ms_remote, aio_meta, aio_ms_local = aio_fut.result(timeout=min(soft, max(0.05, deadline - time.monotonic())))
        except FuturesTimeoutError:
            if cached is not None:
                aio_streams, aio_in, cached_ms = cached
                aio_ms_remote = int(cached_ms or 0)
                aio_ms_local = 0
                aio_meta = {'tag': AIO_TAG, 'ok': True, 'err': 'soft_timeout_cache' if float(soft or 0) > 0 else 'cache_hit', 'count': int(aio_in or 0), 'ms': int(aio_ms_remote or 0), 'soft_timeout_ms': int(float(soft or 0) * 1000)}
            else:
                aio_streams, aio_in = [], 0
                aio_ms_remote = 0
                aio_ms_local = 0
                aio_meta = {'tag': AIO_TAG, 'ok': False, 'err': 'timeout', 'count': 0, 'ms': 0, 'soft_timeout_ms': int(float(soft or 0) * 1000)}

            # refresh cache when AIO finishes
            aio_fut.add_done_callback(_make_aio_cache_update_cb(aio_key))


        try:
            if isinstance(aio_meta, dict):
                aio_meta["wait_ms"] = int((time.monotonic() - t_wait0) * 1000)
        except Exception:
            pass

        _harvest_p2()

    else:
        # mode == "off" OR no cache: wait for AIO and P2 within the deadline
        if aio_fut:
            remaining = max(0.05, deadline - time.monotonic())
            t_wait0 = time.monotonic()
            try:
                aio_streams, aio_in, aio_ms_remote, aio_meta, aio_ms_local = aio_fut.result(timeout=remaining)
            except FuturesTimeoutError:
                aio_streams, aio_in, aio_ms_remote, aio_meta, aio_ms_local = [], 0, 0, {'tag': AIO_TAG, 'ok': False, 'err': 'timeout'}, 0

            try:
                if isinstance(aio_meta, dict):
                    aio_meta["wait_ms"] = int((time.monotonic() - t_wait0) * 1000)
            except Exception:
                pass


        _harvest_p2()

        if aio_streams:
            _aio_cache_set(aio_key, aio_streams, aio_in, int(aio_ms_remote or 0))

    merged = aio_streams + p2_streams


    # NEW: Optional debrid redundancy re-ordering (keeps a bit of each debrid early for uptime).
    # This only reorders; it does not drop streams.
    DEBRID_REDUNDANCY = _parse_bool(os.environ.get("DEBRID_REDUNDANCY", "0"))
    if DEBRID_REDUNDANCY and merged and not (is_iphone and IPHONE_USENET_ONLY):
        try:
            from collections import defaultdict as _dd

            scored: List[Tuple[int, Dict[str, Any], Dict[str, Any]]] = []
            for i, st in enumerate(merged):
                if not isinstance(st, dict):
                    continue
                try:
                    mt = classify(st)
                except Exception:
                    mt = {}
                scored.append((i, st, mt))

            debrid_tags = {"RD", "TB", "AD", "PM"}
            groups = _dd(list)
            for i, st, mt in scored:
                tag = str(mt.get("provider") or "").upper().strip()
                if tag in debrid_tags:
                    groups[tag].append((i, st, mt))

            take_idxs: List[int] = []
            taken: set[int] = set()
            for tag, arr in groups.items():
                arr = sorted(arr, key=lambda t: (-int(t[2].get("seeders") or 0), -int(t[2].get("size") or 0)))
                for i, _, _ in arr[:2]:
                    if i not in taken:
                        take_idxs.append(i)
                        taken.add(i)

            if take_idxs:
                merged = [merged[i] for i in take_idxs] + [st for j, st in enumerate(merged) if j not in taken]
        except Exception as e:
            try:
                logger.debug("DEBRID_REDUNDANCY_ERR rid=%s err=%s", _rid(), e)
            except Exception:
                pass
    # +++ New: iPhone/iPad usenet-only branch (prov2-only; AIO is skipped above)
    if is_iphone and IPHONE_USENET_ONLY:
        merged = p2_streams
        try:
            logger.info('IPHONE_USENET_ONLY rid=%s p2_in=%d kept=%d', _rid(), len(p2_streams), len(merged))
        except Exception:
            pass

    return merged, aio_in, prov2_in, aio_ms_local, p2_ms_local, False, None, {'aio': aio_meta, 'p2': p2_meta}

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

    - Torrent/debrid: prefer infohash (strong, global) + resolution (preserve distinct encodes).
    - Usenet: upstream "infohash" may be a placeholder shared by many results, which can collapse
      the entire set down to a couple of items. For usenet providers, prefer URL/label+size bucketing.
    """
    # Provider detection (best-effort)
    try:
        bh = (stream.get('behaviorHints') or {}) if isinstance(stream, dict) else {}
        prov = (
            (meta.get('provider') if isinstance(meta, dict) else None)
            or (bh.get('provider') if isinstance(bh, dict) else None)
            or (stream.get('prov') if isinstance(stream, dict) else None)
            or (stream.get('provider') if isinstance(stream, dict) else None)
            or ''
        )
        prov_u = str(prov).upper().strip()
    except Exception:
        prov_u = ''

    # Common usenet set
    usenet_provs = {str(p).upper() for p in (USENET_PROVIDERS or USENET_PRIORITY or []) if p}
    usenet_provs.add('ND')  # Treat ND as usenet-like

    # Shared fields
    res = ((meta.get('res') if isinstance(meta, dict) else None) or 'SD').upper()
    raw_url = (stream.get('url') or stream.get('externalUrl') or '') if isinstance(stream, dict) else ''
    raw_url = (raw_url or '').strip()
    size = (meta.get('size') if isinstance(meta, dict) else None) or (meta.get('bytes') if isinstance(meta, dict) else None) or (meta.get('videoSize') if isinstance(meta, dict) else None) or 0
    try:
        size_i = int(size or 0)
    except Exception:
        size_i = 0

    # USENET: prefer URL-based key even if a (possibly-placeholder) infohash exists.
    if prov_u in usenet_provs:
        if raw_url:
            uhash = hashlib.sha1(raw_url.encode('utf-8')).hexdigest()[:16]
            size_bucket = int(size_i / (500 * 1024 * 1024)) if size_i else -1
            return f"usenet:{prov_u}:u:{uhash}:{size_bucket}:{res}"

        bh = (stream.get('behaviorHints') or {}) if isinstance(stream, dict) else {}
        try:
            normalized_label = normalize_label(
                (bh.get('filename') if isinstance(bh, dict) else None)
                or (bh.get('bingeGroup') if isinstance(bh, dict) else None)
                or (stream.get('name') if isinstance(stream, dict) else None)
                or (stream.get('description') if isinstance(stream, dict) else None)
                or ''
            )
        except Exception:
            normalized_label = ''

        size_bucket = int(size_i / (500 * 1024 * 1024)) if size_i else -1
        normalized_label = (normalized_label or '')[:80]
        return f"usenet:{prov_u}:nohash:{normalized_label}:{size_bucket}:{res}"

    # Non-usenet: prefer infohash
    infohash = (
        (meta.get('infohash') if isinstance(meta, dict) else None)
        or (meta.get('infoHash') if isinstance(meta, dict) else None)
        or (stream.get('infoHash') if isinstance(stream, dict) else None)
        or (stream.get('infohash') if isinstance(stream, dict) else None)
        or ''
    )
    infohash = (infohash or '').lower().strip()
    if infohash:
        return f"h:{infohash}:{res}"

    # Fallback: URL-hash + size
    if raw_url:
        uhash = hashlib.sha1(raw_url.encode('utf-8')).hexdigest()[:16]
        return f"u:{uhash}:{size_i}:{res}"

    # Last resort: normalized label (+ size bucket)
    bh = (stream.get('behaviorHints') or {}) if isinstance(stream, dict) else {}
    try:
        normalized_label = normalize_label(
            (bh.get('filename') if isinstance(bh, dict) else None)
            or (bh.get('bingeGroup') if isinstance(bh, dict) else None)
            or (stream.get('name') if isinstance(stream, dict) else None)
            or (stream.get('description') if isinstance(stream, dict) else None)
            or ''
        )
    except Exception:
        normalized_label = ''

    size_bucket = int(size_i / (500 * 1024 * 1024)) if size_i else -1
    normalized_label = (normalized_label or '')[:80]
    return f"nohash:{normalized_label}:{size_bucket}:{res}"



# Point 11: Finalize dedup tie-breaks with insta score + title match ratio (from point 10)
def _tie_break_score(m: Dict[str, Any], mismatch_ratio: float = 0.0) -> float:
    """Higher is better.

    Components:
      - insta readiness: TB cached True (1.0) > NZB ready (0.8) > RD heuristic LIKELY (0.5)
      - title match ratio: similarity ratio from title validation (0.0..1.0)
    """
    insta = 0.0
    try:
        cached = m.get("cached")
    except Exception:
        cached = None
    try:
        ready = bool(m.get("ready", False))
    except Exception:
        ready = False

    if cached is True:
        insta = DEDUP_READINESS_TRUE
    elif ready:
        insta = DEDUP_READINESS_READY
    elif cached == "LIKELY":
        insta = DEDUP_READINESS_LIKELY

    try:
        r = float(mismatch_ratio or 0.0)
    except Exception:
        r = 0.0
    return insta + (r * DEDUP_TITLE_WEIGHT)



def _drop_bad_top_n(
    pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    *,
    top_n: int,
    timeout_s: Optional[float] = None,
    range_mode: Optional[bool] = None,
    deliver_cap: Optional[int] = None,
    sort_buffer: Optional[int] = None,
    base_sort_key: Optional[Callable[[Tuple[Dict[str, Any], Dict[str, Any]]], Any]] = None,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Verify the top-N (and optionally refill) streams in sorted order, then:
      - hard-drop unsafe/non-playable items
      - host-propagate unsafe classifications across the whole candidate buffer
      - rank-down risky sources (server/content-type, tiny stub mp4, etc.)
    """
    if not pairs or top_n <= 0:
        return pairs

    n_total = len(pairs)
    deliver_cap_eff = max(0, int(deliver_cap or 0))
    sort_buffer_eff = max(0, int(sort_buffer or 0))

    # How deep do we "look" for candidates to verify
    scan_n = min(n_total, max(sort_buffer_eff, top_n))
    # Refill logic: if drops happen, verify deeper until we have enough "kept" verified items.
    refill = _safe_int(os.environ.get("VERIFY_REFILL_BUFFER", "0"), 0)
    target_kept = top_n
    if deliver_cap_eff > 0:
        target_kept = max(top_n, min(scan_n, min(deliver_cap_eff, top_n + max(0, refill))))
    max_total = _safe_int(os.environ.get("VERIFY_MAX_TOTAL", str(min(scan_n, max(target_kept + 20, top_n)))), min(scan_n, max(target_kept + 20, top_n)))
    max_total = max(top_n, min(scan_n, max_total))

    # Build ordered list of unique-URL indices within scan window
    idxs: List[int] = []
    seen_urls: set = set()
    for i in range(scan_n):
        s, _m = pairs[i]
        u = s.get("url") or s.get("externalUrl") or ""
        if not u:
            continue
        if u in seen_urls:
            continue
        seen_urls.add(u)
        idxs.append(i)

    if not idxs:
        return pairs

    unsafe_hosts: set = set()
    risky_host_pen: Dict[str, int] = {}
    to_drop_idx: set = set()
    results: Dict[int, Tuple[bool, int, str]] = {}

    def _host_key(s: Dict[str, Any], m: Dict[str, Any]) -> str:
        # Prefer upstream host captured at wrap-time (pre /r token).
        try:
            h = (m.get("up_host") or "").strip().lower()
            if h:
                return h
        except Exception:
            pass
        u = (s.get("url") or s.get("externalUrl") or "")
        if not u:
            return ""
        try:
            from urllib.parse import urlparse
            net = (urlparse(u).netloc or "").lower()
            net = net.split("@")[-1]
            return net.split(":")[0]
        except Exception:
            return ""

    def _is_host_unsafe(vcls: str) -> bool:
        if not vcls:
            return False
        if vcls.startswith("PROBE_UNSAFE_"):
            return True
        return vcls in {
            "FINAL_NOT_PLAYABLE",
            "UPSTREAM_ERROR_TEXT",
            "CT_SIGNATURE_MISMATCH",
            "REDIRECT_LOOP",
            "REDIRECT_NO_LOCATION",
            "PROBE_UNSAFE_LEAK",
            "PROBE_UNSAFE_CL_MISMATCH",
            "PROBE_UNSAFE_RANGE_IGNORED",
            "HOST_CACHED_UNSAFE",
        }

    def _is_host_risky(vcls: str) -> bool:
        if not vcls:
            return False
        if vcls.startswith("RISKY_"):
            return True
        return vcls in {"RISKY_SERVER_HEADER", "RISKY_CT_OR_SERVER", "RISKY_CT", "STUB_MP4_TINY_TOTAL"}

    # Parallel worker
    max_workers = min(_safe_int(os.environ.get("VERIFY_MAX_WORKERS", "12"), 12), max(1, len(idxs)))
    sess = requests.Session()
    if timeout_s is None:
        timeout_s = float(os.environ.get("VERIFY_TIMEOUT_S", "6.0") or 6.0)

    def _worker(idx: int) -> Tuple[int, Tuple[bool, int, str, str]]:
        s, m = pairs[idx]
        hk = _host_key(s, m)
        if hk in unsafe_hosts:
            return (idx, (False, 0, "HOST_ALREADY_UNSAFE", hk))
        u = s.get("url") or s.get("externalUrl") or ""
        keep, pen, cls = _verify_stream_url(u, sess, timeout_s=timeout_s, range_mode=range_mode)
        return (idx, (keep, pen, cls, hk))

    # Progressive verify: batches until we have enough kept verified items
    kept_ok = 0
    total_done = 0
    ptr = 0
    batch_size = max(4, _safe_int(os.environ.get("VERIFY_BATCH_SIZE", str(min(10, top_n))), min(10, top_n)))
    while ptr < len(idxs) and total_done < max_total and kept_ok < target_kept:
        batch = idxs[ptr : ptr + batch_size]
        ptr += len(batch)

        try:
            ex = ThreadPoolExecutor(max_workers=max_workers)
            futs = [ex.submit(_worker, i) for i in batch]
            try:
                done, not_done = wait(futs, timeout=float(VERIFY_FUTURE_TIMEOUT or timeout_s or 4.0))
                for fut in done:
                    idx, (keep, pen, cls, hk) = fut.result()
                    results[idx] = (keep, pen, cls)
                    total_done += 1

                    # Host-level side effects
                    if hk:
                        if _is_host_unsafe(cls):
                            unsafe_hosts.add(hk)
                            _verify_host_cache_set(hk, "unsafe", cls)
                        elif _is_host_risky(cls):
                            # Keep the *largest* penalty for that host
                            prev = risky_host_pen.get(hk, 0)
                            if pen > prev:
                                risky_host_pen[hk] = pen
                            _verify_host_cache_set(hk, "risky", cls, ttl_s=_safe_int(os.environ.get("VERIFY_HOST_CACHE_TTL_RISKY", "300"), 300))

                    # Stream-level drops
                    if not keep:
                        to_drop_idx.add(idx)
                    else:
                        kept_ok += 1
                for fut in not_done:
                    try:
                        fut.cancel()
                    except Exception:
                        pass
            finally:
                try:
                    ex.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    ex.shutdown(wait=False)
        except Exception:
            # If verification machinery fails, fall back to no-op (keep original ordering).
            return pairs

    # Apply results to metadata for logging/ranking
    for i, (s, m) in enumerate(pairs[:scan_n]):
        if i in results:
            keep, pen, cls = results[i]
            m["verify_cls"] = cls
            if pen:
                m["verify_rank"] = pen

    # Propagate host risk penalties across the full candidate set
    if risky_host_pen:
        for s, m in pairs:
            hk = _host_key(s, m)
            if hk and hk in risky_host_pen:
                m["verify_cls"] = m.get("verify_cls") or "HOST_RISKY_PROPAGATED"
                m["verify_rank"] = max(int(m.get("verify_rank") or 0), int(risky_host_pen[hk]))

    # Drop anything flagged unsafe explicitly, plus anything from unsafe hosts
    filtered: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for i, (s, m) in enumerate(pairs):
        hk = _host_key(s, m)
        vcls = (m.get("verify_cls") or "")
        if i in to_drop_idx:
            continue
        if hk and hk in unsafe_hosts:
            continue
        # If the stream itself has unsafe class, drop it too
        if _is_host_unsafe(vcls):
            continue
        filtered.append((s, m))

    return filtered


def _res_to_int(res: str) -> int:
    """Normalize resolution strings into an integer height.
    Handles common variants + some non-Latin lookalikes (e.g., Cyrillic 'К').
    """
    r = (res or "").strip()
    ru = r.upper()
    # normalize a couple of common lookalikes
    ru = ru.replace("К", "K")  # Cyrillic Ka -> Latin K
    ru = ru.replace("Р", "P")  # Cyrillic Er -> Latin P (rare)
    res_map = {
        "SD": 480, "480P": 480,
        "HD": 720, "720P": 720,
        "FHD": 1080, "FULLHD": 1080, "1080P": 1080,
        "2K": 1440, "1440P": 1440,
        "4K": 2160, "UHD": 2160, "2160": 2160, "2160P": 2160,
        "8K": 4320, "4320": 4320, "4320P": 4320,
    }
    # direct map
    if ru in res_map:
        return res_map[ru]
    # substring fallbacks
    s = ru
    if "4320" in s or "8K" in s:
        return 4320
    if "2160" in s or "4K" in s or "UHD" in s:
        return 2160
    if "1440" in s or "2K" in s:
        return 1440
    if "1080" in s or "FHD" in s:
        return 1080
    if "720" in s or "HD" in s:
        return 720
    if "480" in s or "SD" in s:
        return 480
    return 0  # Default low


# ---------------------------
# Counts / summary helpers (Patch 6)
# ---------------------------
_DEBRID_PROVIDERS = {"TB", "RD", "AD", "PM", "DL"}
_USENET_PROVIDERS = {"ND", "NZB", "EW", "NG", "USENET"}

_RES_ORDER = ["2160P", "1440P", "1080P", "720P", "480P", "SD"]

def _stack_for_provider(p: str) -> str:
    p = (p or "UNK").upper()
    if p.startswith("DL-"):
        return "debrid"
    if p in _DEBRID_PROVIDERS or p == "DEBRIDLINK":
        return "debrid"
    if p in _USENET_PROVIDERS or p in USENET_PRIORITY:
        return "usenet"
    return "unk"


def _res_bucket(res: str) -> str:
    r = (res or "").upper()
    if r in ("4K", "2160", "2160P"):
        return "2160P"
    if r in ("1440", "1440P"):
        return "1440P"
    if r in ("1080", "1080P"):
        return "1080P"
    if r in ("720", "720P"):
        return "720P"
    if r in ("480", "480P"):
        return "480P"
    if r in ("SD", ""):
        return "SD"
    # last resort: keep short
    return r[:8]

def _size_bucket(size_bytes: int) -> str:
    try:
        b = int(size_bytes or 0)
    except Exception:
        b = 0
    if b <= 0:
        return "unk"
    gb = b / (1024 ** 3)
    if gb < 0.5:
        return "<0.5GB"
    if gb < 1:
        return "0.5-1GB"
    if gb < 2:
        return "1-2GB"
    if gb < 4:
        return "2-4GB"
    if gb < 8:
        return "4-8GB"
    return "8GB+"

def _bump(d: Dict[str, int], k: str, n: int = 1):
    d[k] = int(d.get(k, 0)) + int(n)

def _summarize_streams_for_counts(streams: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compact summary for logs/debug: counts by supplier/provider/stack/res/hash/cached/size."""
    out: Dict[str, Any] = {
        "total": 0,
        "by_supplier": {},
        "by_provider": {},
        "by_stack": {},
        "by_res": {},
        "by_size": {},
        "hash": {"yes": 0, "no": 0},
        "cached": {"true": 0, "likely": 0, "false": 0, "unk": 0},
    }
    if not streams:
        return out
    for s in streams:
        if not isinstance(s, dict):
            continue
        out["total"] += 1
        bh = s.get("behaviorHints")
        if not isinstance(bh, dict):
            bh = {}
        supplier = (bh.get("wrap_src") or "")
        if not supplier:
            # Only trust source_tag when it matches our wrapper supplier tags (avoid WEB/BLURAY/REMUX/etc pollution)
            st = (bh.get("source_tag") or "").strip()
            st_u = str(st).upper() if st else ""
            allowed = {str(AIO_TAG).upper(), str(PROV2_TAG).upper()}
            supplier = st_u if st_u in allowed else str(AIO_TAG or "AIO").upper()
        supplier = str(supplier).upper()

        try:
            m = classify(s)
        except Exception:
            m = {}
        prov = str(m.get("provider") or "UNK").upper()
        stack = _stack_for_provider(prov)
        res = _res_bucket(m.get("res") or "")
        size_b = int(m.get("size") or 0)
        size_k = _size_bucket(size_b)
        infohash = (m.get("infohash") or "").strip()
        cached = bh.get("cached", None)

        _bump(out["by_supplier"], supplier)
        _bump(out["by_provider"], prov)
        _bump(out["by_stack"], stack)
        _bump(out["by_res"], res)
        _bump(out["by_size"], size_k)

        if infohash:
            out["hash"]["yes"] += 1
        else:
            out["hash"]["no"] += 1

        if cached is True:
            out["cached"]["true"] += 1
        elif cached is False:
            out["cached"]["false"] += 1
        elif isinstance(cached, str) and cached.upper() == "LIKELY":
            out["cached"]["likely"] += 1
        else:
            out["cached"]["unk"] += 1

    # Stable ordering for res keys (purely for readability in debug/JSON)
    try:
        out["by_res"] = {k: out["by_res"].get(k, 0) for k in _RES_ORDER if k in out["by_res"]} | {k: v for k, v in out["by_res"].items() if k not in _RES_ORDER}
    except Exception:
        pass
    return out

def _compact_fetch_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(meta, dict):
        return {}
    # Only keep the safe/compact keys
    keep = ("tag", "ok", "status", "bytes", "count", "ms", "ms_provider", "wait_ms", "http_ms", "read_ms", "json_ms", "post_ms", "soft_timeout_ms", "err")
    return {k: meta.get(k) for k in keep if k in meta and meta.get(k) not in (None, "", {})}

# Diversify top M while preserving quality: pick from a larger pool, bucketed by resolution, then mix providers/suppliers.
# - Does NOT force lower resolutions above higher ones; it fills 4K first, then 1080p, etc.
# - Uses a size-based threshold so "diversity" can't pull tiny encodes ahead of huge REMUXes.
def _diversify_by_quality_bucket(
    out_pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    m: int,
    sort_key,
    threshold: float,
    p2_src_boost: int,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    if not out_pairs or m <= 0:
        return out_pairs

    # Pull from a wider pool so we can swap in "nearby" candidates that were just below the cut.
    pool_n = min(len(out_pairs), max(m * DIVERSITY_POOL_MULT, 200))
    pool = out_pairs[:pool_n]
    tail = out_pairs[pool_n:]

    from collections import defaultdict

    def _supplier_of(pair):
        s, _m = pair
        bh = (s.get("behaviorHints") or {}) if isinstance(s, dict) else {}
        if not isinstance(bh, dict):
            bh = {}
        return str((bh.get("wrap_src") or bh.get("source_tag") or _m.get("supplier") or (AIO_TAG or "AIO"))).upper()

    def _size_gb(pair) -> float:
        _s, _m = pair
        try:
            return float(_m.get("size") or 0) / (1024.0 ** 3)
        except Exception:
            return 0.0

    # Group pool by resolution (numeric) so higher res never gets displaced by lower res.
    res_groups = defaultdict(list)
    for p in pool:
        res_v = _res_to_int((p[1].get("res") or "SD"))
        res_groups[res_v].append(p)

    # Highest resolution first (e.g., 2160, 1080, 720...)
    res_levels = sorted(res_groups.keys(), reverse=True)

    selected: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    prov_ct = defaultdict(int)
    sup_ct = defaultdict(int)

    # Clamp threshold sane range; if user sets weird value, just fall back.
    try:
        threshold = float(threshold)
    except Exception:
        threshold = 0.85
    if threshold <= 0.0 or threshold > 1.0:
        threshold = 0.85

    # Convert P2 boost into a small penalty reduction in cached_val-space.
    p2_bonus = max(0.0, float(p2_src_boost) * 0.05)

    for res_v in res_levels:
        if len(selected) >= m:
            break

        bucket = res_groups[res_v]
        if not bucket:
            continue

        # Within a resolution bucket, group by (provider, supplier) so we can alternate across both.
        groups = defaultdict(list)
        for pair in bucket:
            prov = str(pair[1].get("provider") or "UNK").upper()
            sup = _supplier_of(pair)
            groups[(prov, sup)].append(pair)

        # Greedy selection within this res bucket.
        bucket_selected: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []

        while len(selected) < m:
            best_pair = None
            best_group = None
            best_k = None

            # Size floor for this res bucket (only after we have at least one selected in this bucket).
            min_size = min((_size_gb(p) for p in bucket_selected), default=0.0)

            for (prov, sup), lst in groups.items():
                if not lst:
                    continue
                cand = lst[0]
                size = _size_gb(cand)

                # Quality guard: don't pick something far smaller than what we've already accepted in this bucket.
                if min_size > 0.0 and size > 0.0 and size < (threshold * min_size):
                    continue

                base = sort_key(cand)
                penalty = (prov_ct[prov] * 0.12) + (sup_ct[sup] * 0.18)
                if sup == "P2":
                    penalty = penalty - p2_bonus

                k = (penalty,) + tuple(base)
                if best_k is None or k < best_k:
                    best_k = k
                    best_pair = cand
                    best_group = (prov, sup)

            if best_pair is None:
                # If threshold blocks everything (rare), fall back to "best available" without the size guard.
                for (prov, sup), lst in groups.items():
                    if not lst:
                        continue
                    cand = lst[0]
                    base = sort_key(cand)
                    penalty = (prov_ct[prov] * 0.12) + (sup_ct[sup] * 0.18)
                    if sup == "P2":
                        penalty = penalty - p2_bonus
                    k = (penalty,) + tuple(base)
                    if best_k is None or k < best_k:
                        best_k = k
                        best_pair = cand
                        best_group = (prov, sup)

            if best_pair is None:
                break  # nothing left in this bucket

            # Select it
            groups[best_group].pop(0)
            selected.append(best_pair)
            bucket_selected.append(best_pair)
            prov_ct[best_group[0]] += 1
            sup_ct[best_group[1]] += 1

    # Rebuild list: diversified top M from pool, then the remaining pool items in original order, then tail.
    sel_ids = {id(p) for p in selected[:m]}
    remaining_pool = [p for p in pool if id(p) not in sel_ids]
    return selected[:m] + remaining_pool + tail

# ---------------------------
# NZBGeek readiness (Newznab)
# ---------------------------

_NZB_NEWZNAB_NS = "http://www.newznab.com/DTD/2010/feeds/attributes/"

def _extract_imdbid_for_nzbgeek(id_: str, type_: str = "") -> str:
    """Return imdb id like 'tt1234567' from 'imdb:tt...' or raw 'tt...' ids.
    Also supports Stremio/tmdb ids and will resolve via TMDB external_ids when possible.
    """
    try:
        if not id_:
            return ""
        sid = str(id_).strip()
        if not sid:
            logger.debug(f"IMDB_BLANK id={id_}: empty")
            return ""

        # Allow "imdb:tt0123456"
        if sid.startswith("imdb:"):
            sid = sid.split(":", 1)[1].strip()

        # For series ids like "tt0944947:1:1" keep base
        base = sid.split(":", 1)[0].strip()

        # Direct IMDb id
        if re.match(r"^tt\d{5,10}$", base):
            return base

        # Try TMDB forms: "tmdb:1399:1:1", "tmdb:1399", or plain "1399"
        tmdb_id = _extract_tmdbid_for_lookup(sid) or _extract_tmdbid_for_lookup(base)
        if tmdb_id:
            # If type_ is unknown, try tv first (common for series) then movie
            try_order = [type_] if type_ in ("movie", "series") else ["series", "movie"]
            for t in try_order:
                imdb_from_tmdb = _tmdb_external_imdb_id(t, tmdb_id)
                if imdb_from_tmdb:
                    return imdb_from_tmdb
            logger.warning(f"TMDB_IMDB_RESOLVE_FAIL id={sid} tmdb_id={tmdb_id}: no IMDb found")
            return ""

        logger.debug(f"IMDB_BLANK id={id_}: no match/resolution")
        return ""
    except Exception as e:
        logger.warning(f"IMDB_EXTRACT_FAIL id={id_}: {e}")
        return ""

def _canonical_id_for_upstream(id_: str) -> str:
    """Canonicalize incoming Stremio id for upstream add-ons.

    Goals:
    - Keep debrid behavior working even if clients send ids like 'imdb:tt...'
    - Preserve series season/episode suffixes (':S:E') if present.
    - Do NOT attempt to guess/convert TMDB ids; pass through unchanged.
    """
    try:
        if not id_:
            return ""
        sid = str(id_).strip()
        # Stremio iOS / some clients may prefix IMDb ids with 'imdb:'
        if sid.startswith("imdb:"):
            sid = sid.split(":", 1)[1].strip()
        return sid
    except Exception:
        return str(id_ or "").strip()


def _extract_tmdbid_for_lookup(id_: str) -> str:
    """Extract TMDB numeric id from 'tmdb:<id>' (optionally with ':S:E')."""
    try:
        if not id_:
            return ""
        sid = str(id_).strip()
        if sid.startswith("tmdb:"):
            parts = sid.split(":")
            if len(parts) >= 2 and parts[1].isdigit():
                return parts[1]
        # some callers might pass plain numeric TMDB ids
        if sid.isdigit():
            return sid
    except Exception:
        return ""
    return ""


@lru_cache(maxsize=2048)
def _tmdb_external_imdb_id(type_: str, tmdb_id: str) -> str:
    """Resolve TMDB numeric id to IMDb tt-id using TMDB external_ids.

    Returns '' if unavailable or on error. Cached to avoid repeated calls.
    """
    try:
        if not TMDB_API_KEY:
            return ""
        if not tmdb_id or not str(tmdb_id).isdigit():
            return ""
        endpoint = "movie" if type_ == "movie" else "tv"
        url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}"
        # Keep this tight; this is an optional enhancement, never a blocker.
        resp = session.get(url, timeout=min(2.0, float(TMDB_TIMEOUT or 2.0)))
        resp.raise_for_status()
        data = resp.json() if resp else {}
        imdb_id = (data.get("imdb_id") or "").strip()
        if re.match(r"^tt\d{5,10}$", imdb_id):
            return imdb_id
    except Exception:
        return ""
    return ""

def check_nzbgeek_readiness(imdbid: str) -> List[str]:
    """Query NZBGeek (Newznab) and return a list of *normalized* titles we consider 'ready'."""
    ready_titles: List[str] = []
    if not NZBGEEK_APIKEY:
        return ready_titles

    try:
        if not imdbid:
            logger.debug("NZBGEEK_SKIP_NO_IMDB")
            return ready_titles

        # IMPORTANT: do not log params (contains apikey)
        params = {
            "t": "search",
            "imdbid": imdbid,
            "apikey": NZBGEEK_APIKEY,
            "extended": "1",
        }
        r = requests.get(NZBGEEK_BASE, params=params, timeout=NZBGEEK_TIMEOUT)
        if r.status_code != 200:
            return ready_titles

        root = ET.fromstring(r.content)
        ns = {"newznab": "http://www.newznab.com/DTD/2010/feeds/attributes/"}
        now_utc = datetime.now(timezone.utc)

        for item in root.findall("./channel/item"):
            title = (item.findtext("title") or "").strip()
            if not title:
                continue

            # Fast category guards (avoid accidental adult results)
            cat_text = (item.findtext("category") or "")
            if "XXX" in cat_text.upper():
                continue

            # Collect all newznab attrs (some names appear multiple times, e.g., category)
            attrs = defaultdict(list)
            for a in item.findall("newznab:attr", ns):
                n = (a.get("name") or "").strip()
                v = (a.get("value") or "").strip()
                if n:
                    attrs[n].append(v)

            cat_ids = attrs.get("category") or []
            # NZBGeek uses 6000+ for XXX categories (e.g., 6000/6040)
            if any(v.startswith("6") for v in cat_ids if v):
                continue

            # Pull the fields we actually have in the real feed
            try:
                grabs = int((attrs.get("grabs") or ["0"])[0] or 0)
            except Exception:
                grabs = 0
            try:
                size_bytes = int((attrs.get("size") or ["0"])[0] or 0)
            except Exception:
                size_bytes = 0
            password = ((attrs.get("password") or ["0"])[0] or "0").strip()

            # Age: compute from usenetdate/pubDate.
            date_str = ((attrs.get("usenetdate") or [""])[0] or "").strip() or (item.findtext("pubDate") or "").strip()
            age_days: Optional[int] = None
            if date_str:
                try:
                    dt = parsedate_to_datetime(date_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    dt = dt.astimezone(timezone.utc)
                    age_days = int((now_utc - dt).total_seconds() // 86400)
                except Exception:
                    age_days = None

            # Readiness heuristic (conservative):
            is_ready = (
                grabs >= 20 and
                password != "1" and
                size_bytes > 1_000_000_000
            )
            if age_days is not None:
                is_ready = is_ready and (age_days <= 180)

            if is_ready:
                ready_titles.append(normalize_label(title))

            # keep list bounded (we only use it for matching)
            if len(ready_titles) >= 200:
                break

    except Exception as e:
        logger.warning(f"NZBGeek readiness check failed: {e}")

    return ready_titles



def check_nzbgeek_readiness_title(title_query: str) -> List[str]:
    """Optional fallback: query NZBGeek by title (when no IMDb id is available)."""
    ready_titles: List[str] = []
    if not NZBGEEK_APIKEY:
        return ready_titles
    tq = (title_query or "").strip()
    if not tq:
        return ready_titles

    try:
        params = {
            "t": "search",
            "q": tq,
            "apikey": NZBGEEK_APIKEY,
            "extended": "1",
        }
        r = requests.get(NZBGEEK_BASE, params=params, timeout=NZBGEEK_TIMEOUT)
        if r.status_code != 200:
            return ready_titles

        root = ET.fromstring(r.content)
        ns = {"newznab": "http://www.newznab.com/DTD/2010/feeds/attributes/"}
        now_utc = datetime.now(timezone.utc)

        for item in root.findall("./channel/item"):
            title = (item.findtext("title") or "").strip()
            if not title:
                continue

            cat_text = (item.findtext("category") or "")
            if "XXX" in cat_text.upper():
                continue

            attrs = defaultdict(list)
            for a in item.findall("newznab:attr", ns):
                n = (a.get("name") or "").strip()
                v = (a.get("value") or "").strip()
                if n:
                    attrs[n].append(v)

            cat_ids = attrs.get("category") or []
            if any(v.startswith("6") for v in cat_ids if v):
                continue

            try:
                grabs = int((attrs.get("grabs") or ["0"])[0] or 0)
            except Exception:
                grabs = 0
            try:
                size_bytes = int((attrs.get("size") or ["0"])[0] or 0)
            except Exception:
                size_bytes = 0
            password = ((attrs.get("password") or ["0"])[0] or "0").strip()

            date_str = ((attrs.get("usenetdate") or [""])[0] or "").strip() or (item.findtext("pubDate") or "").strip()
            age_days: Optional[int] = None
            if date_str:
                try:
                    dt = parsedate_to_datetime(date_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    dt = dt.astimezone(timezone.utc)
                    age_days = int((now_utc - dt).total_seconds() // 86400)
                except Exception:
                    age_days = None

            is_ready = (
                grabs >= 20 and
                password != "1" and
                size_bytes > 1_000_000_000
            )
            if age_days is not None:
                is_ready = is_ready and (age_days <= 180)

            if is_ready:
                ready_titles.append(normalize_label(title))

            if len(ready_titles) >= 200:
                break
    except Exception as e:
        logger.warning(f"NZBGeek title readiness check failed: {e}")

    return ready_titles

def filter_and_format(type_: str, id_: str, streams: List[Dict[str, Any]], aio_in: int = 0, prov2_in: int = 0, is_android: bool = False, is_iphone: bool = False, fast_mode: bool = False, deliver_cap: Optional[int] = None) -> Tuple[List[Dict[str, Any]], PipeStats]:
    stats = PipeStats()
    rid = _rid()
    t_ff0 = time.monotonic()
    # Expose per-request stats to heuristic helpers (thread-local)
    try:
        _TLS.stats = stats
    except Exception:
        pass
    stats.aio_in = aio_in
    stats.prov2_in = prov2_in
    stats.merged_in = len(streams)
    # Client platform + per-request TorBox hash budget
    platform = stats.client_platform or client_platform(is_android=is_android, is_iphone=is_iphone)
    _set_stats_platform(stats, platform)
    tb_max_hashes = _choose_tb_max_hashes(platform)

    iphone_usenet_mode = bool(is_iphone and IPHONE_USENET_ONLY)
    usenet_priority_set = {str(p).upper() for p in (USENET_PRIORITY or []) if p}
    if iphone_usenet_mode and usenet_priority_set:
        _before = len(streams)

        def _prov_guess(_s: Dict[str, Any]) -> str:
            try:
                if not isinstance(_s, dict):
                    return "UNK"
                bh = _s.get("behaviorHints") or {}
                p = None
                if isinstance(bh, dict):
                    p = bh.get("provider") or bh.get("prov")
                p = p or _s.get("prov") or _s.get("provider")
                if p:
                    return str(p).upper().strip()

                txt = f"{_s.get('name','')} {_s.get('description','')}"
                if isinstance(bh, dict):
                    txt += f" {bh.get('filename','')}"
                txt_u = txt.upper()

                # common alias normalization
                if re.search(r"(?<![A-Z0-9])EWEKA(?![A-Z0-9])", txt_u):
                    return "EW"
                if re.search(r"(?<![A-Z0-9])NZGEEK(?![A-Z0-9])", txt_u):
                    return "NG"

                for ap in usenet_priority_set:
                    if re.search(rf"(?<![A-Z0-9]){re.escape(ap)}(?![A-Z0-9])", txt_u):
                        return ap
                return "UNK"
            except Exception:
                return "UNK"

        _filtered = [s for s in streams if _prov_guess(s) in usenet_priority_set]
        if _filtered:
            streams = _filtered

        logger.debug("IPHONE_USENET_ONLY filtered to %d usenet streams (from %d)", len(streams), _before)
        stats.merged_in = len(streams)

    deliver_cap_eff = int(deliver_cap or MAX_DELIVER or 60)

    # Expected metadata (TMDB); independent of TorBox checks.
    if fast_mode:
        expected = {}
        stats.ms_tmdb = 0
    else:
        t_tmdb0 = time.monotonic()
        expected = get_expected_metadata(type_, id_)
        stats.ms_tmdb = int((time.monotonic() - t_tmdb0) * 1000)


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

    # Early cap & cheap pre-dedup: large merged inputs can dominate CPU time (drops/dedup/sort).
    EARLY_CAP = _safe_int(os.environ.get("EARLY_CAP", "250"), 250)
    MAX_CANDIDATES = _safe_int(os.environ.get("MAX_CANDIDATES", "250"), 250)

    # Headroom: verification can hard-drop some top-N entries. Ensure we keep enough candidates to still fill deliver_cap_eff.
    VERIFY_REFILL_BUFFER = _safe_int(os.environ.get("VERIFY_REFILL_BUFFER", "40"), 40)
    VERIFY_SORT_BUFFER = _safe_int(os.environ.get("VERIFY_SORT_BUFFER", "140"), 140)
    try:
        MAX_CANDIDATES = max(int(MAX_CANDIDATES or 0), int(deliver_cap_eff or 0) + max(10, int(VERIFY_REFILL_BUFFER or 0)), int(VERIFY_SORT_BUFFER or 0))
    except Exception:
        pass
    try:
        if EARLY_CAP and EARLY_CAP > 0:
            EARLY_CAP = max(int(EARLY_CAP), int(MAX_CANDIDATES or 0))
    except Exception:
        pass

    def _quick_provider(_s: Dict[str, Any]) -> str:
        bh = (_s.get("behaviorHints") or {})
        txt = f"{_s.get('name','')} {_s.get('description','')} {bh.get('filename','')} {bh.get('source','')} {bh.get('provider','')}".upper()
        # Prefer explicit markers first
        if re.search(r"(?<![A-Z0-9])TORBOX(?![A-Z0-9])", txt) or re.search(r"(?<![A-Z0-9])TB(?![A-Z0-9])", txt):
            return "TB"
        if re.search(r"(?<![A-Z0-9])REAL[- ]?DEBRID(?![A-Z0-9])", txt) or re.search(r"(?<![A-Z0-9])RD(?![A-Z0-9])", txt):
            return "RD"
        if re.search(r"(?<![A-Z0-9])ALL[- ]?DEBRID(?![A-Z0-9])", txt) or re.search(r"(?<![A-Z0-9])AD(?![A-Z0-9])", txt):
            return "AD"
        # Debrid-Link (DL) provider clarity: do NOT treat WEB-DL or ".DL." release tokens as provider.
        # Only classify as DL when Debrid-Link is explicitly mentioned or a deliberate marker is present.
        if re.search(r"(?<![A-Z0-9])DEBRID[- ]?LINK(?![A-Z0-9])", txt) or ("🟢DL" in txt) or ("DL⚡" in txt):
            return "DL"
        # Usenet-ish
        if "USENET" in txt or "NZB" in txt or "NZBDAV" in txt or re.search(r"(?<![A-Z0-9])ND(?![A-Z0-9])", txt):
            return "ND"
        return "UNK"

    def _quick_res_int(_s: Dict[str, Any]) -> int:
        txt = f"{_s.get('name','')} {(_s.get('behaviorHints') or {}).get('filename','')}".upper()
        if "2160" in txt or "4K" in txt:
            return 2160
        if "1080" in txt:
            return 1080
        if "720" in txt:
            return 720
        if "480" in txt:
            return 480
        return 0

    def _quick_seeders(_s: Dict[str, Any]) -> int:
        bh = (_s.get("behaviorHints") or {})
        v = bh.get("seeders") if isinstance(bh, dict) else None
        if v is None:
            v = _s.get("seeders")
        try:
            return int(v or 0)
        except Exception:
            return 0

    if EARLY_CAP > 0 and len(streams) > EARLY_CAP:
        orig_n = len(streams)

        # Cheap pre-dedup by infoHash/url before we do any heavy parsing.
        seen_pre: set[str] = set()
        pre: List[Dict[str, Any]] = []
        for _s in streams:
            if not isinstance(_s, dict):
                continue
            bh = (_s.get("behaviorHints") or {})
            key = (
                _s.get("infoHash")
                or _s.get("infohash")
                or (bh.get("infoHash") if isinstance(bh, dict) else None)
                or _s.get("url")
                or _s.get("externalUrl")
            )
            if not key:
                # fallback key by name+url
                key = f"{(_s.get('name') or '')}|{(_s.get('url') or _s.get('externalUrl') or '')}"
            key = str(key)
            if key in seen_pre:
                continue
            seen_pre.add(key)
            pre.append(_s)

        if len(pre) != orig_n:
            logger.info("PRE_DEDUP rid=%s before=%d after=%d", rid, orig_n, len(pre))

        if len(pre) > EARLY_CAP:
            # IMPORTANT: keep diversity across premium providers at cap-time.
            # Otherwise, if PREMIUM_PRIORITY is [RD,TB] (or [TB,RD]) and EARLY_CAP=200,
            # we can starve the secondary provider entirely and later stages can never "mix".
            def _quick_provider(_s: Dict[str, Any]) -> str:
                bh = _s.get("behaviorHints") or {}
                prov = ""
                if isinstance(bh, dict):
                    prov = (bh.get("provider") or "")
                if not prov:
                    n = (_s.get("name") or "") + " " + (_s.get("description") or "")
                    # Include filename too (many formatters put provider tokens there)
                    try:
                        _bh2 = _s.get("behaviorHints") or {}
                        if isinstance(_bh2, dict):
                            n += " " + str(_bh2.get("filename") or "")
                    except Exception:
                        pass
                    nu = n.upper()
                    # prefer formatter-injected shortName tokens; keep word boundaries to avoid HDR->RD.
                    m_sn = re.search(r"\b(TB|TORBOX|RD|REAL[- ]?DEBRID|AD|ALLDEBRID|DL|DEBRIDLINK|ND|NZB|USENET)\b", nu)
                    if m_sn:
                        tok = m_sn.group(1)
                        if tok in ("TORBOX", "TB"):
                            prov = "TB"
                        elif tok.startswith("REAL") or tok == "RD":
                            prov = "RD"
                        elif tok in ("ALLDEBRID", "AD"):
                            prov = "AD"
                        elif tok in ("DEBRIDLINK", "DL"):
                            prov = "DL"
                        elif tok in ("ND", "NZB", "USENET"):
                            prov = "ND"
                return (prov or "").upper() or "UNK"

            def _quick_res_int(_s: Dict[str, Any]) -> int:
                n = (_s.get("name") or "") + " " + (_s.get("description") or "")
                nu = n.upper()
                if "2160" in nu or "4K" in nu:
                    return 2160
                if "1080" in nu:
                    return 1080
                if "720" in nu:
                    return 720
                if "480" in nu:
                    return 480
                return 0

            def _quick_seeders(_s: Dict[str, Any]) -> int:
                n = (_s.get("name") or "") + " " + (_s.get("description") or "")
                m2 = re.search(r"(\d{1,6})\s*(SEEDS?|SEEDERS?)\b", n, re.I)
                if m2:
                    try:
                        return int(m2.group(1))
                    except Exception:
                        return 0
                return 0

            groups: Dict[str, List[Dict[str, Any]]] = {}
            for _s in pre:
                p = _quick_provider(_s)
                groups.setdefault(p, []).append(_s)

            # Sort each group by cheap quality (res, seeders).
            for _p, _arr in groups.items():
                _arr.sort(key=lambda _s: (_quick_res_int(_s), _quick_seeders(_s)), reverse=True)

            # iPhone *usenet-only mode* (IPHONE_USENET_ONLY=true): don't waste EARLY_CAP slots on debrid providers.
            if iphone_usenet_mode and "ND" in groups and len(groups["ND"]) >= EARLY_CAP:
                streams = groups["ND"][:EARLY_CAP]
                logger.info(
                    "EARLY_CAP_IPHONE rid=%s capped=%d original=%d nd=%d",
                    rid, len(streams), orig_n, len(groups.get("ND", []))
                )
            else:
                # Priority order: premium providers first (in configured order), then everything else.
                priority_order: List[str] = [p for p in PREMIUM_PRIORITY if p in groups]
                for p in sorted(groups.keys()):
                    if p not in priority_order:
                        priority_order.append(p)

                present = [p for p in priority_order if groups.get(p)]
                if len(present) <= 1:
                    # Fallback to old behavior
                    pre.sort(key=lambda _s: (
                        (999 - _provider_rank(_quick_provider(_s))),
                        _quick_res_int(_s),
                        _quick_seeders(_s),
                    ), reverse=True)
                    streams = pre[:EARLY_CAP]
                    logger.info("EARLY_CAP rid=%s capped=%d original=%d", rid, len(streams), orig_n)
                else:
                    cap = EARLY_CAP
                    nprov = len(present)

                    # Small floor per provider (scaled to cap) so TB/RD both survive the cap.
                    floor = min(25, max(5, cap // max(1, nprov * 4)))
                    quotas: Dict[str, int] = {p: min(floor, len(groups[p])) for p in present}
                    used = sum(quotas.values())
                    remaining = cap - used

                    # Distribute remainder round-robin in priority order.
                    while remaining > 0:
                        progressed = False
                        for p in present:
                            if remaining <= 0:
                                break
                            if quotas[p] < len(groups[p]):
                                quotas[p] += 1
                                remaining -= 1
                                progressed = True
                        if not progressed:
                            break

                    capped: List[Dict[str, Any]] = []
                    for p in present:
                        capped.extend(groups[p][:quotas[p]])

                    # If some providers ran out early, backfill from leftovers (still cheap-sorted).
                    if len(capped) < cap:
                        leftovers: List[Dict[str, Any]] = []
                        for p in present:
                            leftovers.extend(groups[p][quotas[p]:])
                        for p, arr in groups.items():
                            if p not in present:
                                leftovers.extend(arr)
                        leftovers.sort(key=lambda _s: (
                            (999 - _provider_rank(_quick_provider(_s))),
                            _quick_res_int(_s),
                            _quick_seeders(_s),
                        ), reverse=True)
                        capped.extend(leftovers[:cap - len(capped)])

                    streams = capped[:cap]
                    try:
                        gsz = {p: len(groups[p]) for p in present}
                    except Exception:
                        gsz = {}
                    logger.info(
                        "EARLY_CAP_STRAT rid=%s capped=%d original=%d groups=%s quotas=%s",
                        rid, len(streams), orig_n, gsz, quotas
                    )

                # NEW: For iPhone (even when not in usenet-only mode), keep at least one top ND stream if available.
                if is_iphone and (not iphone_usenet_mode):
                    try:
                        nd_top = None
                        try:
                            nd_arr = groups.get("ND", [])
                            if isinstance(nd_arr, list) and nd_arr:
                                nd_top = nd_arr[0]
                        except Exception:
                            nd_top = None
                        if nd_top and (nd_top not in streams):
                            streams = [nd_top] + [x for x in streams if x is not nd_top]
                            streams = streams[:EARLY_CAP]
                            logger.info("IPHONE_ND_DIVERSITY rid=%s included_nd=1 capped=%d", rid, len(streams))
                    except Exception:
                        pass

        else:
            streams = pre
            logger.info("NO_EARLY_CAP rid=%s original=%d cap=%d", rid, orig_n, EARLY_CAP)


    cleaned: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    t_clean0 = time.monotonic()
    for s in streams:
        if not isinstance(s, dict):
            continue
        if not sanitize_stream_inplace(s):
            stats.dropped_missing_url += 1
            continue
        if (s.get("streamData") or {}).get("type") == "error":
            stats.dropped_error += 1
            continue

        try:
            m = classify(s)
        except Exception as e:
            stats.dropped_error += 1
            if len(stats.error_reasons) < 8:
                stats.error_reasons.append(f"classify:{type(e).__name__}")
            logger.warning(
                "EXEC_ISSUE rid=%s stage=classify type=%s id=%s err=%s",
                rid, type_, id_, (str(e)[:200] if e else "unknown"),
            )
            continue

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
            # Optional URL verification moved to batched/parallel top-N pass (see below)

            # Add: iPhone min size for hash fix (even big files)
            try:
                size_b = int(m.get("size") or 0)
            except Exception:
                size_b = 0
            min_size = 500 * 1024 * 1024 if is_iphone else 0  # 500MB min for iPhone
            if min_size and size_b < min_size:
                # Dedicated counter for accuracy + per-drop log (mobile debugging)
                stats.dropped_low_size_iphone += 1
                logger.info("DROP_IPHONE_SIZE rid=%s dropped_low_size_iphone=%s", rid, stats.dropped_low_size_iphone)
                continue



        cleaned.append((s, m))

    try:
        stats.ms_py_clean = int((time.monotonic() - t_clean0) * 1000)
    except Exception:
        pass

    # Candidates before validation/scoring/dedup (dedup runs later with Point 11 tie-breaks)
    out_pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = cleaned[:]

    # Optional: title/year validation gate (local similarity; useful to drop obvious mismatches)
    # Uses parsed title from classify() when available (streams often have empty s['title']).
    if (not fast_mode) and (not VALIDATE_OFF) and (TRAKT_VALIDATE_TITLES or TRAKT_STRICT_YEAR):
        t_title0 = time.monotonic()
        expected_title = (expected.get('title') or '').lower().strip()
        expected_ep_title = (expected.get('episode_title') or '').lower().strip()
        expected_year = expected.get('year')
        meta_source = (expected.get('source') or '').strip()
        
        # Flag empty metadata from the chosen source (helps interpret drops).
        if (TRAKT_VALIDATE_TITLES or TRAKT_STRICT_YEAR) and (not expected_title) and (not expected_ep_title) and (expected_year is None):
            if meta_source == 'tmdb' and TMDB_API_KEY:
                if 'tmdb_fail' not in stats.flag_issues:
                    stats.flag_issues.append('tmdb_fail')
            if meta_source == 'trakt' and TRAKT_CLIENT_ID:
                if 'trakt_fail' not in stats.flag_issues:
                    stats.flag_issues.append('trakt_fail')
        filtered_pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for s, m in out_pairs:
            # Prefer our parsed raw title; fall back to upstream 'title' then name/desc.
            cand_title = (m.get('title_raw') or s.get('title') or s.get('name') or s.get('description') or '').lower().strip()

            if TRAKT_VALIDATE_TITLES and (expected_title or expected_ep_title) and cand_title:
                is_quality_only = _is_quality_only_title(cand_title)
                if is_quality_only:
                    # Providers sometimes emit names like '2160p WEB-DL 23.3 GB' with no real title words.
                    # Treat as 'unknown title' and skip mismatch dropping to avoid under-delivery.
                    stats.skipped_title_mismatch += 1
                else:
                    # Patch 2 (A/B/C): token containment + Jaccard + fuzzy score; plus TMDB alias titles
                    expected_titles = []
                    if expected_ep_title:
                        expected_titles.append(expected_ep_title)
                    if expected_title:
                        expected_titles.append(expected_title)
                    try:
                        orig = (expected.get("title_original") or expected.get("original_title") or expected.get("original_name") or "")
                    except Exception:
                        orig = ""
                    if orig:
                        expected_titles.append(str(orig).strip().lower())
                    # Dedup expected candidates (preserve order)
                    seen = set()
                    exp_list = []
                    for texp in expected_titles:
                        texp = (texp or "").strip()
                        if not texp:
                            continue
                        if texp in seen:
                            continue
                        seen.add(texp)
                        exp_list.append(texp)
                    best = 1.0
                    if exp_list:
                        best = 0.0
                        for texp in exp_list:
                            sc = title_score(cand_title, texp)
                            if sc > best:
                                best = sc
                    m['_mismatch_ratio'] = best
                    thr = TRAKT_TITLE_MIN_RATIO
                    try:
                        nt = norm_title(expected_title)
                        if nt and len(nt) <= 6:
                            thr = min(thr, 0.50)
                    except Exception:
                        pass
                    if best < thr:
                        stats.dropped_title_mismatch += 1
                        logger.info(
                            'DROP_TITLE_MISMATCH rid=%s platform=%s type=%s id=%s cand=%r expected_title=%r expected_ep=%r ratio=%.3f thr=%.2f #%d',
                            rid, platform, type_, id_, cand_title, expected_title, expected_ep_title, best, thr, stats.dropped_title_mismatch
                        )
                        continue
            if TRAKT_STRICT_YEAR and expected_year:
                stream_year = _extract_year(s.get('name') or '') or _extract_year(s.get('description') or '')
                if stream_year and abs(int(stream_year) - int(expected_year)) > 1:
                    stats.dropped_title_mismatch += 1
                    try:
                        logger.info("DROP_TITLE_MISMATCH rid=%s name=%s expected_title=%s dropped_title_mismatch=%s", rid, s.get("name"), expected_title, stats.dropped_title_mismatch)
                    except Exception:
                        pass
                    continue

            filtered_pairs.append((s, m))

        out_pairs = filtered_pairs

        try:
            stats.ms_title_mismatch += int((time.monotonic() - t_title0) * 1000)
        except Exception:
            pass


    # Global hash visibility (per request)
    try:
        hs_total, hs_with, hs_uniq, hs_prov_total, hs_prov_with, hs_src = hash_stats(out_pairs)
        logger.info(
            "HASH_STATS rid=%s total=%d with_hash=%d uniq_hash=%d prov_total=%s prov_with_hash=%s hash_src=%s",
            _rid(), hs_total, hs_with, hs_uniq, hs_prov_total, hs_prov_with, hs_src
        )
    except Exception as _e:
        logger.debug(f"HASH_STATS_ERR rid={_rid()} err={_e}")

    # +++ NZBGeek Readiness Check for Usenet (iPhone exclusive + general mix)
    # This is a hint only: it never drops streams, it only sets meta['ready']=True for better ordering.
    try:
        usenet_provs_set = {str(p).upper() for p in (USENET_PROVIDERS or USENET_PRIORITY or []) if p}
        usenet_provs_set.add("ND")  # Treat ND as usenet-like
        has_usenet = any(str(meta.get("provider") or "").upper().strip() in usenet_provs_set for _, meta in out_pairs)

        ready_titles: List[str] = []
        imdbid = ""

        # Only call NZBGeek when we actually have Usenet streams and the API key is configured.
        if has_usenet and NZBGEEK_APIKEY and USE_NZBGEEK_READY and (not USENET_PROBE_ENABLE):
            imdbid = _extract_imdbid_for_nzbgeek(id_, type_=type_)

            # Maintain: Usenet NZBGeek API (not affected by RD heuristics—keep readiness)
            try:
                logger.debug("NZBGEEK_MAINTAIN rid=%s imdb=%s", _rid(), imdbid)
            except Exception:
                pass

            # If request id is TMDB-based (tmdb:123), resolve to IMDb via TMDB external_ids.
            if (not imdbid) and TMDB_API_KEY:
                tmdb_id = _extract_tmdbid_for_lookup(id_)
                if tmdb_id:
                    imdbid = _tmdb_external_imdb_id(type_, tmdb_id)

            t0_ready = time.monotonic()
            mode = "skip"
            if imdbid:
                ready_titles = check_nzbgeek_readiness(imdbid)
                mode = "imdb"
            else:
                ready_titles = []
                if NZBGEEK_TITLE_FALLBACK:
                    # Title fallback for cases where we can't confidently derive an IMDb id.
                    try:
                        expected_meta = get_expected_metadata(type_, id_)
                    except Exception:
                        expected_meta = {}
                    imdbid2 = (expected_meta.get("imdb_id") or "").strip() if isinstance(expected_meta, dict) else ""
                    if imdbid2:
                        imdbid = imdbid2
                        ready_titles = check_nzbgeek_readiness(imdbid)
                        mode = "imdb_expected"
                    else:
                        title_q = ""
                        if isinstance(expected_meta, dict):
                            title_q = ((expected_meta.get("title") or "").strip() + " " + (expected_meta.get("episode_title") or "").strip()).strip()
                        if title_q:
                            try:
                                logger.debug("NZBGEEK_TITLE_FALLBACK rid=%s q=%s", _rid(), title_q)
                            except Exception:
                                pass
                            ready_titles = check_nzbgeek_readiness_title(title_q)
                            mode = "title"
                        else:
                            try:
                                logger.warning("NZBGEEK_SKIP rid=%s: no imdbid and no title for fallback", _rid())
                            except Exception:
                                pass
                else:
                    try:
                        logger.debug("NZBGEEK_SKIP rid=%s: no imdbid (fallback disabled)", _rid())
                    except Exception:
                        pass

            stats.ms_tb_usenet += int((time.monotonic() - t0_ready) * 1000)
            try:
                logger.info(
                    "NZBGEEK_DONE rid=%s mode=%s imdb=%s ready_titles=%s ms_tb_usenet=%s",
                    _rid(), mode, imdbid, len(ready_titles or []), stats.ms_tb_usenet
                )
            except Exception:
                pass

        # NOTE: NZBGeek readiness matching is applied later (after dedup) for speed.
    except Exception as _e:
        logger.debug("NZBGEEK_READY_ERR rid=%s err=%s", rid, _e)

    
    # Dedup (Point 11): choose best candidate per stable dedup_key using insta readiness + title match ratio.
    # - Keeps ordering stable by preserving the first-seen index for each key.
    # - Replaces the stored entry when a later duplicate has a higher tie-break score.
    ties_resolved = 0
    if WRAPPER_DEDUP and out_pairs:
        t_dedup0 = time.monotonic()
        deduped: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        best_idx: Dict[str, int] = {}
        best_score: Dict[str, float] = {}
        ties_resolved = 0

        for s, m in out_pairs:
            try:
                k = dedup_key(s, m)
            except Exception:
                k = ""

            try:
                ratio = float(m.get("_mismatch_ratio") or 0.0)
            except Exception:
                ratio = 0.0

            sc = _tie_break_score(m, ratio)

            if k and k in best_idx:
                stats.deduped += 1
                i = best_idx[k]
                _ps, _pm = deduped[i]
                prev_sc = float(best_score.get(k, 0.0) or 0.0)

                replace = False
                if sc > prev_sc + 1e-9:
                    replace = True
                elif abs(sc - prev_sc) <= 1e-9:
                    # Secondary ties: prefer larger size, then more seeders (keep stable order otherwise).
                    try:
                        size_b = int(m.get("size") or 0)
                    except Exception:
                        size_b = 0
                    try:
                        prev_size_b = int(_pm.get("size") or 0)
                    except Exception:
                        prev_size_b = 0

                    if size_b > prev_size_b:
                        replace = True
                    elif size_b == prev_size_b:
                        try:
                            seeders = int(m.get("seeders") or 0)
                        except Exception:
                            seeders = 0
                        try:
                            prev_seeders = int(_pm.get("seeders") or 0)
                        except Exception:
                            prev_seeders = 0
                        if seeders > prev_seeders:
                            replace = True

                if replace:
                    ties_resolved += 1
                    deduped[i] = (s, m)
                    best_score[k] = sc
                continue

            # First seen (or missing key): preserve original ordering index.
            if k:
                best_idx[k] = len(deduped)
                best_score[k] = sc
            deduped.append((s, m))

        out_pairs = deduped
        stats.ms_py_dedup = int((time.monotonic() - t_dedup0) * 1000)

    # Apply NZBGeek readiness matching AFTER dedup (smaller candidate set) to reduce CPU.
    if ready_titles and out_pairs:
        try:
            t_usmatch0 = time.monotonic()
            flagged = 0
            ratio_thr = float(NZBGEEK_TITLE_MATCH_MIN_RATIO or 0.80)

            # ready_titles are already normalized by normalize_label(); build fast lookup structures.
            ready_norms = [str(rt).lower().strip() for rt in (ready_titles or []) if rt]
            ready_set = set(ready_norms)
            tok_index = defaultdict(list)
            for rt in ready_norms:
                for tok in rt.split():
                    if len(tok) >= 3:
                        tok_index[tok].append(rt)

            # Cache readiness by normalized stream title to avoid repeated work within the request.
            ready_cache = {}

            for s, meta in out_pairs:
                prov_u = str(meta.get('provider') or '').upper().strip()
                if prov_u not in usenet_provs_set:
                    continue

                st = normalize_label(meta.get('title_raw') or s.get('title') or s.get('name') or '')
                st = st.lower().strip()
                if not st:
                    continue

                if st not in ready_cache:
                    is_ready = False
                    if st in ready_set:
                        is_ready = True
                    else:
                        toks = [t for t in st.split() if len(t) >= 3]
                        cand_counts = {}
                        for t in toks:
                            for rt in tok_index.get(t, ()):
                                cand_counts[rt] = cand_counts.get(rt, 0) + 1

                        if cand_counts:
                            # Prefer candidates sharing >=2 meaningful tokens; else fall back to a small top list.
                            cands = [rt for rt, c in sorted(cand_counts.items(), key=lambda kv: (-kv[1], kv[0])) if c >= 2]
                            if not cands:
                                cands = [rt for rt, _ in sorted(cand_counts.items(), key=lambda kv: (-kv[1], kv[0]))][:20]

                            # Fast substring check before any fuzzy ratio work.
                            for rt in cands:
                                if rt and (rt in st or st in rt):
                                    is_ready = True
                                    break

                            # Fuzzy match only against a small, token-filtered candidate set.
                            if (not is_ready) and cands:
                                for rt in cands[:50]:
                                    if not rt:
                                        continue
                                    if abs(len(st) - len(rt)) > max(8, int(0.35 * max(len(st), len(rt)))):
                                        continue
                                    if difflib.SequenceMatcher(None, st, rt).ratio() >= ratio_thr:
                                        is_ready = True
                                        break

                    ready_cache[st] = is_ready

                if ready_cache.get(st) and not meta.get('ready'):
                    meta['ready'] = True
                    flagged += 1

            stats.ms_usenet_ready_match = int((time.monotonic() - t_usmatch0) * 1000)
            if flagged:
                logger.info("NZBGEEK_READY rid=%s imdb=%s ready_titles=%s flagged=%s ms_tb_usenet=%s ms_match=%s", rid, imdbid, len(ready_titles or []), flagged, stats.ms_tb_usenet, stats.ms_usenet_ready_match)
        except Exception as _e:
            stats.ms_usenet_ready_match = 0
            logger.debug("NZBGEEK_READY_MATCH_ERR rid=%s err=%s", rid, _e)
    else:
        stats.ms_usenet_ready_match = 0

# MARKERS: show how many streams were validated by each "instant" mechanism.
    # NOTE: TB instant is NOT WebDAV here; it is usually signaled by upstream tags (CACHED:TRUE + PROXIED:TRUE)
    # that were computed from hashes upstream (or by our TorBox API hash check when VERIFY_CACHED_ONLY=true).
    try:
        from collections import Counter
        c_tot = Counter(); c_tagged = Counter(); c_cachedtag = Counter(); c_proxiedtag = Counter(); c_hash = Counter(); c_ready = Counter()
        if ties_resolved > 0:
            logger.info(f"DEDUP_TIES rid={_rid()} resolved={ties_resolved} policy=readiness_title")

        for _s, _m in out_pairs:
            prov = str(_m.get('provider') or 'UNK').upper().strip()
            c_tot[prov] += 1
            desc_u = ''
            try:
                if isinstance(_s, dict):
                    desc_u = str(_s.get('description') or '').upper()
            except Exception:
                desc_u = ''
            aio = _m.get('aio') if isinstance(_m, dict) else None
            aio_cached = None
            aio_proxied = None
            try:
                if isinstance(aio, dict):
                    aio_cached = aio.get('cached', None)
                    aio_proxied = aio.get('proxied', None)
            except Exception:
                aio_cached = None
                aio_proxied = None

            has_cached_tag = ('CACHED:TRUE' in desc_u) or (USE_AIO_READY and (aio_cached is True))
            has_proxied_tag = ('PROXIED:TRUE' in desc_u) or (USE_AIO_READY and (aio_proxied is True))

            if has_cached_tag:
                c_cachedtag[prov] += 1
            if has_proxied_tag:
                c_proxiedtag[prov] += 1

            # "Tagged instant" means cached+proxied are both True (truth tags if present, else legacy TRUE tokens).
            if ('CACHED:TRUE' in desc_u and 'PROXIED:TRUE' in desc_u) or (USE_AIO_READY and (aio_cached is True and aio_proxied is True)):
                c_tagged[prov] += 1
            if _m.get('infohash'):
                c_hash[prov] += 1
            if _m.get('ready'):
                c_ready[prov] += 1

        logger.info(
            'INSTA_MARKERS rid=%s totals=%s tagged_instant=%s cached_tag=%s proxied_tag=%s has_hash=%s ready=%s',
            _rid(), dict(c_tot), dict(c_tagged), dict(c_cachedtag), dict(c_proxiedtag), dict(c_hash), dict(c_ready)
        )
    except Exception as _e:
        logger.debug('INSTA_MARKERS_ERR rid=%s err=%s', _rid(), _e)

    # Sorting: quality-first GLOBAL sort AFTER merge/dedup.
    # Order: (iPhone usenet) ready > instant > cached > res > size > seeders > provider.
    # (general) instant > cached > ready > res > size > seeders > provider.
    # This prevents provider-append "burial" and surfaces best quality first.
    def sort_key(pair: Tuple[Dict[str, Any], Dict[str, Any]]):
        s, m = pair
        res = _res_to_int(m.get('res') or 'SD')
        size_b = int(m.get('size') or 0)
        seeders = int(m.get('seeders') or 0)
        prov = str(m.get('provider') or 'UNK').upper().strip()

        # Usenet fallback: many usenet items have 0 seeders, so give a small tiebreak boost.
        usenet_provs = {str(p).upper() for p in (USENET_PROVIDERS or USENET_PRIORITY or [])}
        usenet_provs.add('ND')  # Treat ND as usenet-like

        aio = m.get('aio') if isinstance(m, dict) else None
        aio_type = None
        try:
            if isinstance(aio, dict):
                aio_type = aio.get('type', None)
        except Exception:
            aio_type = None
        is_usenet = (prov in usenet_provs) or (USE_AIO_READY and (aio_type == 'usenet'))

        if seeders == 0 and is_usenet:
            seeders = USENET_SEEDER_BOOST

        cached = m.get('cached')
        if cached is True:
            cached_val = 0
        elif cached == 'LIKELY':
            cached_val = 0.5
        else:
            cached_val = 1.0 if is_usenet else 2.0

        # Optional (usenet-only mode): prefer usenet slightly to avoid iOS torrent edge cases
        if iphone_usenet_mode and is_usenet:
            cached_val = min(cached_val, 0.1)

        # "Instant / ready-to-play" signal (preferred over provider priority).
        # Primary signal: formatter tags (CACHED:true + PROXIED:true). Secondary: heuristic on text.
        desc = ""
        try:
            if isinstance(s, dict):
                desc = s.get("description") or ""
        except Exception:
            desc = ""
        desc_u = str(desc).upper()
        # Step 1: Prefer truth tags (C:/P:) when available (gated by USE_AIO_READY). Fall back to legacy tokens + heuristic.
        aio_ti = _aio_tagged_instant(aio, (s.get('url') if isinstance(s, dict) else None)) if USE_AIO_READY else None
        if aio_ti is not None:
            is_instant = bool(aio_ti)
        else:
            is_instant = ((("CACHED:TRUE" in desc_u and "PROXIED:TRUE" in desc_u) or ("C:TRUE" in desc_u and ("P:TRUE" in desc_u or _is_controlled_playback_url(s.get("url") if isinstance(s, dict) else None)))) or ("C:TRUE" in desc_u and ("P:TRUE" in desc_u or _is_controlled_playback_url(s.get("url") if isinstance(s, dict) else None)))) or _looks_instant(str(desc))
        instant_val = 0 if is_instant else 1

        ready_val = 0 if m.get("ready") else 1
        verify_rank = int(m.get("verify_rank") or 0)
        prov_idx = _provider_rank(prov)
        score = _tie_break_score(m, float(m.get('_mismatch_ratio') or 0.0))
        # P1 Hybrid A+C ordering (bucket + qscore). Kept after instant/ready/cached/verify to preserve playability tiering.
        p1_bucket = int(m.get("p1_bucket") or _P1_BUCKET_OTHER)
        try:
            p1_q = float(m.get("p1_q") or 0.0)
        except Exception:
            p1_q = 0.0
        if P1_MODE != "ac":
            p1_bucket = _P1_BUCKET_OTHER
            p1_q = 0.0

        # Sanity demotion: keep obviously mis-sized "4K BluRay/REMUX" from rising above real high-quality options.
        # This primarily targets junk like "4K BluRay 1 GB" without dropping it outright.
        sanity_val = 0
        sanity_reason = ""
        if SANITY_DEMOTE and (not SANITY_MOVIES_ONLY or type_ == "movie") and res >= 2160 and size_b:
            try:
                bh = s.get("behaviorHints") or {}
                t_u = (f"{s.get('name', '')} {s.get('description', '')} {bh.get('filename', '')}").upper()
            except Exception:
                t_u = ""
            src_u = str(m.get("source") or "").upper()
            size_gb = float(size_b) / float(1024 ** 3) if size_b else 0.0
            try:
                if (("REMUX" in t_u) or ("REMUX" in src_u)) and size_gb > 0 and size_gb < SANITY_4K_REMUX_MIN_GB:
                    sanity_val = 1
                    sanity_reason = f"4k_remux_small<{SANITY_4K_REMUX_MIN_GB:g}gb"
                elif (("BLURAY" in t_u) or ("BLU-RAY" in t_u) or ("UHD" in t_u) or re.search(r"\bBD\b", t_u) or ("BLURAY" in src_u)) and size_gb > 0 and size_gb < SANITY_4K_BLURAY_MIN_GB:
                    sanity_val = 1
                    sanity_reason = f"4k_bluray_small<{SANITY_4K_BLURAY_MIN_GB:g}gb"
            except Exception:
                sanity_val = 0
                sanity_reason = ""
        # Store for later debug logs (meta-only; not shown to clients)
        try:
            m["_sanity"] = sanity_val
            m["_sanity_reason"] = sanity_reason
        except Exception:
            pass


        
        # Store core sort signals for debug logs (meta-only; not shown to clients)
        try:
            m["_instant_val"] = int(instant_val)
            m["_ready_val"] = int(ready_val)
            m["_cached_val"] = float(cached_val)
            m["_verify_rank0"] = int(verify_rank)
            m["_res_i"] = int(res)
            m["_size_b"] = int(size_b)
            m["_prov_u"] = str(prov)
        except Exception:
            pass

# Sort order: instant -> cached -> resolution -> size -> seeders -> provider rank
        # Sort order: instant -> cached -> resolution -> size -> seeders -> provider rank
        if iphone_usenet_mode and usenet_priority_set:
            usenet_rank = 0 if prov in usenet_priority_set else 1
            return (usenet_rank, ready_val, instant_val, cached_val, verify_rank, -res, sanity_val, p1_bucket, -p1_q, -size_b, -score, -seeders, prov_idx)
        return (instant_val, ready_val, cached_val, verify_rank, -res, sanity_val, p1_bucket, -p1_q, -size_b, -score, -seeders, prov_idx)  # Add: Swap for stronger ready (Usenet beats non-instant cached)

    did_verify = False
    _t_sort0 = time.monotonic()
    out_pairs.sort(key=sort_key)
    try:
        stats.ms_py_sort += int((time.monotonic() - _t_sort0) * 1000)
    except Exception:
        pass
    # Direct Usenet playability probe (REAL vs STUB) - replaces NZBGeek readiness when enabled.
    # This runs AFTER global sort/dedup and marks/demotes stubs so playable links float upward.
    if (not fast_mode) and has_usenet and USENET_PROBE_ENABLE:
        try:
            t_up0 = time.monotonic()
            out_pairs = _apply_usenet_playability_probe(
                out_pairs,
                top_n=int(USENET_PROBE_TOP_N or 0),
                target_real=int(USENET_PROBE_TARGET_REAL or 0),
                timeout_s=float(USENET_PROBE_TIMEOUT_S or 4.0),
                budget_s=float(USENET_PROBE_BUDGET_S or 8.5),
                range_end=int(USENET_PROBE_RANGE_END or 16440),
                stub_len=int(USENET_PROBE_STUB_LEN or 16440),
                retries=int(USENET_PROBE_RETRIES or 0),
                concurrency=int(USENET_PROBE_CONCURRENCY or 8),
                drop_stubs=bool(USENET_PROBE_DROP_STUBS),
                drop_fails=bool(USENET_PROBE_DROP_FAILS),
                mark_ready=bool(USENET_PROBE_MARK_READY),
                stats=stats,
            )
            stats.ms_usenet_probe = int((time.monotonic() - t_up0) * 1000)
            _t_sort0 = time.monotonic()
            out_pairs.sort(key=sort_key)
            try:
                stats.ms_py_sort += int((time.monotonic() - _t_sort0) * 1000)
            except Exception:
                pass
        except Exception as _e:
            logger.debug("USENET_PROBE_ERR rid=%s err=%s", rid, _e)

    # Parallel verification (batched for speed; only verify top-N after global sort)
    if (not fast_mode) and VERIFY_STREAM and (not (is_android and ANDROID_VERIFY_OFF)):
        try:
            top_n = ANDROID_VERIFY_TOP_N if (is_android or is_iphone) else VERIFY_DESKTOP_TOP_N
            timeout = ANDROID_VERIFY_TIMEOUT if (is_android or is_iphone) else VERIFY_STREAM_TIMEOUT
            out_pairs = _drop_bad_top_n(out_pairs, top_n=int(top_n or 0), timeout_s=float(timeout or VERIFY_STREAM_TIMEOUT), range_mode=VERIFY_RANGE, deliver_cap=deliver_cap_eff, sort_buffer=VERIFY_SORT_BUFFER)
            _t_sort0 = time.monotonic()
            out_pairs.sort(key=sort_key)
            try:
                stats.ms_py_sort += int((time.monotonic() - _t_sort0) * 1000)
            except Exception:
                pass
            did_verify = True
        except Exception as e:
            logger.debug("VERIFY_PARALLEL_SKIPPED rid=%s err=%s", rid, e)


    # Proof log: top N after global sort (provider/supplier/res + sort signals)
    try:
        proof_n = _safe_int(os.environ.get("SORT_PROOF_TOP_N", "8"), 8)
        proof_n = max(1, min(25, int(proof_n or 8)))
        topn = []
        for rank, (s, m) in enumerate(out_pairs[:proof_n], start=1):
            bh = (s.get("behaviorHints") or {}) if isinstance(s, dict) else {}
            if not isinstance(bh, dict):
                bh = {}
            supplier = (bh.get("wrap_src") or bh.get("source_tag") or (AIO_TAG or "AIO"))
            desc = ""
            try:
                if isinstance(s, dict):
                    desc = s.get("description") or ""
            except Exception:
                desc = ""
            desc_u = str(desc).upper()
            aio = m.get("aio") if isinstance(m, dict) else None
            aio_cached = None
            aio_proxied = None
            try:
                if isinstance(aio, dict):
                    aio_cached = aio.get("cached", None)
                    aio_proxied = aio.get("proxied", None)
            except Exception:
                aio_cached = None
                aio_proxied = None
            aio_ti = (aio_cached is True and aio_proxied is True) if (USE_AIO_READY and (aio_cached is not None and aio_proxied is not None)) else None
            tagged_instant = bool(aio_ti) if (aio_ti is not None) else (("CACHED:TRUE" in desc_u and "PROXIED:TRUE" in desc_u) or ("C:TRUE" in desc_u and ("P:TRUE" in desc_u or _is_controlled_playback_url(s.get("url") if isinstance(s, dict) else None))))
            # Prefer cached signal from the outgoing stream's behaviorHints (what the client sees).
            bh = {}
            try:
                if isinstance(s, dict):
                    bh = s.get("behaviorHints") or {}
            except Exception:
                bh = {}
            cached_bh = bh.get("cached") if isinstance(bh, dict) else None
            cached_m = m.get("cached", None)
            cached_disp = cached_bh if cached_bh is not None else cached_m
            topn.append({
                "rank": int(rank),
                "supplier": str(supplier),
                "prov": str(m.get("provider", "UNK")),
                "res": str(m.get("res", "SD")),
                "size_gb": round(float(int(m.get("size") or 0)) / (1024 ** 3), 2),
                "seeders": int(m.get("seeders") or 0),
                "cached": cached_disp,
                "cached_bh": cached_bh,
                "cached_m": cached_m,
                "premium_level": m.get("premium_level", None),
                "ready": bool(m.get("ready", False)),
                "p1_bucket": int(m.get("p1_bucket") or -1),
                "p1_class": str(m.get("p1_class") or ""),
                "p1_q": round(float(m.get("p1_q") or 0.0), 3),

                "tagged_instant": bool(tagged_instant),
                "instant_val": int(m.get("_instant_val") or 0),
                "ready_val": int(m.get("_ready_val") or 0),
                "cached_val": float(m.get("_cached_val") or 0.0),
                "verify_rank": int(m.get("_verify_rank0") or 0),
                "sanity": int(m.get("_sanity") or 0),
                "sanity_reason": str(m.get("_sanity_reason") or ""),
                "sort_key": sort_key((s, m)),
            })
        # Helpful positioning signal: where does the first REMUX land after primary sort?
        try:
            remux_pos = next((i + 1 for i, (_s0, _m0) in enumerate(out_pairs) if int(_m0.get("p1_bucket") or 99) == 0), None)
            if remux_pos is not None:
                logger.info("P1_REMUX_POS rid=%s pos=%s total=%s", _rid(), remux_pos, len(out_pairs))
        except Exception:
            pass

        logger.info("POST_SORT_TOP rid=%s mark=%s topN=%s", _rid(), _mark(), proof_n)
        for x in topn:
            sk = x.get("sort_key") or ()
            sk0 = sk[0] if len(sk) > 0 else None
            sk1 = sk[1] if len(sk) > 1 else None
            sk2 = sk[2] if len(sk) > 2 else None
            sk3 = sk[3] if len(sk) > 3 else None
            logger.info(
                "POST_SORT_ITEM rid=%s mark=%s r=%s prov=%s stack=%s res=%s size_gb=%s "
                "b=%s p1=%s inst=%s ready=%s cached=%s tc=%s tp=%s cbh=%s pbh=%s cm=%s pm=%s "
                "sk0=%s sk1=%s sk2=%s sk3=%s",
                _rid(), _mark(), x.get("rank"), x.get("prov"), x.get("stack"), x.get("res"), x.get("size_gb"),
                x.get("p1_bucket"), x.get("p1_class"), x.get("instant"), x.get("ready"), x.get("cached"),
                x.get("tagged_cached"), x.get("tagged_proxied"), x.get("cached_bh"), x.get("proxied_bh"),
                x.get("cached_m"), x.get("proxied_m"), sk0, sk1, sk2, sk3,
            )
    except Exception as _e:
        logger.debug("POST_SORT_TOP_ERR rid=%s err=%s", _rid(), _e)

    # OPTIONAL: Instant boost in top N (OFF by default; set INSTANT_BOOST_TOP_N in Render to enable).
    instant_boost_top_n = INSTANT_BOOST_TOP_N
    if instant_boost_top_n and instant_boost_top_n > 0:
        top_n = min(int(instant_boost_top_n), len(out_pairs))
        top_pairs = out_pairs[:top_n]

        def instant_key(p):
            s, m = p
            k = sort_key(p)

            # Extra "super-instant" bump (optional): prefer formatter-tagged ready-to-play streams.
            desc = ""
            try:
                if isinstance(s, dict):
                    desc = s.get("description") or ""
            except Exception:
                desc = ""
            desc_u = str(desc).upper()
            aio = m.get("aio") if isinstance(m, dict) else None
            aio_cached = None
            aio_proxied = None
            try:
                if isinstance(aio, dict):
                    aio_cached = aio.get("cached", None)
                    aio_proxied = aio.get("proxied", None)
            except Exception:
                aio_cached = None
                aio_proxied = None
            ready_flag = bool((m.get('ready') if isinstance(m, dict) else False) or (aio.get('ready') if isinstance(aio, dict) else False))
            if USE_AIO_READY and (aio_cached is not None) and (aio_proxied is not None):
                super_instant = 0 if ((aio_cached is True and aio_proxied is True) or ready_flag) else 1
            else:
                super_instant = 0 if (("CACHED:TRUE" in desc_u and "PROXIED:TRUE" in desc_u) or ("C:TRUE" in desc_u and ("P:TRUE" in desc_u or _is_controlled_playback_url(s.get("url") if isinstance(s, dict) else None)))) else 1
            return (super_instant,) + k

        top_pairs.sort(key=instant_key)
        out_pairs = top_pairs + out_pairs[top_n:]
        try:
            logger.debug("POST_INSTANT_TOP rid=%s cached_top5=%s", _rid(), [p[1].get("cached", None) for p in out_pairs[:5]])
        except Exception:
            pass

    # OPTIONAL: Diversity nudge in top M (OFF by default; set DIVERSITY_TOP_M in Render to enable).
    # Deterministic greedy selection: lightly penalize repeats of supplier and provider in the *top slice* only.
    diversity_top_m = DIVERSITY_TOP_M
    if diversity_top_m > 0:
        out_pairs = _diversify_by_quality_bucket(
            out_pairs,
            m=min(diversity_top_m, len(out_pairs)),
            sort_key=sort_key,
            threshold=DIVERSITY_THRESHOLD,
            p2_src_boost=P2_SRC_BOOST,
        )
        try:
            sup_top10 = []
            for _s, _m in out_pairs[:10]:
                bh = (_s.get('behaviorHints') or {}) if isinstance(_s, dict) else {}
                if not isinstance(bh, dict):
                    bh = {}
                sup_top10.append(str((bh.get('wrap_src') or bh.get('source_tag') or (AIO_TAG or 'AIO'))).upper())
            logger.debug("POST_DIVERSITY_BUCKET rid=%s sup_top10=%s", rid, sup_top10)
        except Exception:
            pass

    
    # --- Premium mix (Android/Desktop): prevent a single debrid provider from dominating the top list
    # when multiple premium providers are present (e.g., TB + RD). This helps avoid "all green" / "all red"
    # swings when upstream ordering/caps change.
    if (not iphone_usenet_mode) and deliver_cap_eff >= 20 and len(PREMIUM_PRIORITY) >= 2:
        # Quality-protect: keep the very top of the globally sorted list untouched.
        # This preserves "best overall wins" (e.g., REMUX over BLURAY) while still allowing provider variety right after.
        try:
            _default_protect = "5" if P1_MODE == "ac" else "0"
            protect_top = int(os.environ.get("PREMIUM_MIX_PROTECT_TOP", _default_protect) or _default_protect)
        except Exception:
            protect_top = 0
        protect_top = max(0, min(20, int(protect_top or 0)))
        protected_head = out_pairs[:protect_top] if protect_top else []
        mix_base = out_pairs[protect_top:] if protect_top else out_pairs

        present_provs: set[str] = set()
        for _s, _m in mix_base:
            pp = (_m.get("provider") or "").upper()
            if pp:
                present_provs.add(pp)

        active: List[str] = [p for p in PREMIUM_PRIORITY if p in present_provs]
        if len(active) >= 2:
            min_each = max(5, min(12, deliver_cap_eff // 6))  # 60 -> 10
            byp: Dict[str, List[Tuple[Dict[str, Any], Dict[str, Any]]]] = {p: [] for p in active}
            for pair in mix_base:
                pp = (pair[1].get("provider") or "").upper()
                if pp in byp:
                    byp[pp].append(pair)

            active2 = [p for p in active if byp.get(p)]
            if len(active2) >= 2:
                min_each_eff = min_each
                for p in active2:
                    if len(byp[p]) < min_each_eff:
                        min_each_eff = max(1, len(byp[p]))

                picked_stream_ids: set[int] = set()
                head: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
                for i in range(min_each_eff):
                    for p in active2:
                        if i < len(byp[p]):
                            pair = byp[p][i]
                            head.append(pair)
                            picked_stream_ids.add(id(pair[0]))

                mixed: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
                mixed.extend(head)
                for pair in mix_base:
                    if id(pair[0]) in picked_stream_ids:
                        continue
                    mixed.append(pair)

                out_pairs = (protected_head + mixed) if protected_head else mixed

                try:
                    top_by: Dict[str, int] = {}
                    for _s, _m in out_pairs[:deliver_cap_eff]:
                        pp = str(_m.get("provider") or "").upper().strip()
                        if not pp:
                            continue
                        top_by[pp] = top_by.get(pp, 0) + 1
                    logger.info(
                        "PREMIUM_MIX rid=%s top=%d min_each=%d providers=%s top_by_provider=%s",
                        rid, deliver_cap_eff, min_each_eff, active2, top_by
                    )
                except Exception:
                    logger.info(
                        "PREMIUM_MIX rid=%s top=%d min_each=%d providers=%s",
                        rid, deliver_cap_eff, min_each_eff, active2
                    )

    # --- Streak mix (Android/Desktop): break up long same-provider runs (esp. Usenet) without destroying quality order.
    # This prevents situations where, after an initial "mix head", a long NZB/ND block pushes high-quality RD/TB items
    # to the very end of the delivered slice.
    t_mix0 = time.monotonic()
    if (not iphone_usenet_mode) and out_pairs and deliver_cap_eff >= 20:
        try:
            from collections import deque

            def _pair_provider(_pair):
                """Provider bucket used for mixing. Mirrors what clients see."""
                try:
                    s, m = _pair
                    bh = (s.get("behaviorHints") or {}) if isinstance(s, dict) else {}
                    prov = (
                        (m.get("provider") if isinstance(m, dict) else None)
                        or (m.get("prov") if isinstance(m, dict) else None)
                        or (bh.get("provider") if isinstance(bh, dict) else None)
                        or (s.get("prov") if isinstance(s, dict) else None)
                        or (s.get("provider") if isinstance(s, dict) else None)
                        or ""
                    )
                    prov_u = str(prov).upper().strip()
                except Exception:
                    prov_u = ""

                # Collapse all Usenet variants into ND so streak/mix matches displayed provider.
                if prov_u in {"ND", "NZB", "NZBDAV", "EW", "EWEKA", "NG", "NZGEEK"}:
                    return "ND"
                if prov_u in {"TORBOX"}:
                    return "TB"
                if prov_u in {"REALDEBRID", "REAL-DEBRID"}:
                    return "RD"
                return prov_u or "UNK"

            # Only re-order the delivered slice; keep the tail (undelivered) as-is.
            _work = out_pairs[:deliver_cap_eff]
            _tail = out_pairs[deliver_cap_eff:]

            # If we only have one provider in the delivered slice, nothing to do.
            _top_provs = [_pair_provider(p) for p in _work]
            if len(set(_top_provs)) >= 2:
                _premium_set = set(p.strip().upper() for p in (PREMIUM_PRIORITY or []) if str(p).strip())

                # Bucketize while preserving current (quality-sorted) order inside each provider.
                _buckets = {}
                _counts = {}
                for _i, _pair in enumerate(_work):
                    _p = _pair_provider(_pair)
                    _buckets.setdefault(_p, deque()).append((_i, _pair))
                    _counts[_p] = _counts.get(_p, 0) + 1

                _providers = list(_counts.keys())

                # Deterministic provider order: premium-first, then others, Usenet last.
                def _prov_rank(p):
                    if p in _premium_set:
                        return (0, p)
                    if p == "ND":
                        return (2, p)
                    return (1, p)

                _providers.sort(key=_prov_rank)

                _total = len(_work)
                _used = {p: 0 for p in _providers}
                _expected = {p: (_counts[p] / float(_total)) for p in _providers}

                _out = []
                _last = None
                _streak = 0

                # Stronger mixing: keep providers interleaved across the whole delivered slice.
                _MAX_USENET_STREAK = 1
                _MAX_OTHER_STREAK = 1

                for _pos in range(_total):
                    _choices = [p for p in _providers if _buckets.get(p) and len(_buckets[p]) > 0]
                    if not _choices:
                        break

                    def _mx(p):
                        return _MAX_USENET_STREAK if p == "ND" else _MAX_OTHER_STREAK

                    _filtered = []
                    for p in _choices:
                        if p == _last and _streak >= _mx(p):
                            continue
                        _filtered.append(p)
                    if not _filtered:
                        _filtered = _choices

                    # Fair scheduling: pick provider most "behind" its expected share,
                    # tie-break by earlier original index to preserve quality within-provider ordering.
                    _best_p = None
                    _best_key = None
                    for p in _filtered:
                        _idx0, _ = _buckets[p][0]
                        deficit = (_expected[p] * (_pos + 1)) - _used[p]
                        premium_bonus = 0.25 if (p in _premium_set) else 0.0
                        # Minimize key => maximize deficit+bonus, then prefer earlier index.
                        _key = (-(deficit + premium_bonus), _idx0, p)
                        if _best_key is None or _key < _best_key:
                            _best_key = _key
                            _best_p = p

                    _idx0, _pair = _buckets[_best_p].popleft()
                    _out.append(_pair)
                    _used[_best_p] += 1

                    if _best_p == _last:
                        _streak += 1
                    else:
                        _last = _best_p
                        _streak = 1

                out_pairs = _out + _tail
            else:
                # Only one provider present; leave as-is.
                pass
                try:
                    _top_by = {}
                    for _s, _m in out_pairs[:deliver_cap_eff]:
                        _pp = str(_m.get("provider") or "").upper().strip()
                        if not _pp:
                            continue
                        _top_by[_pp] = _top_by.get(_pp, 0) + 1
                    logger.info(
                        "STREAK_MIX rid=%s top=%d max_usenet=%d top_by_provider=%s",
                        rid, deliver_cap_eff, _MAX_USENET_STREAK, _top_by
                    )
                except Exception:
                    logger.info("STREAK_MIX rid=%s top=%d max_usenet=%d", rid, deliver_cap_eff, _MAX_USENET_STREAK)
        except Exception:
            # Never fail the request due to ordering tweaks.
            pass

    try:
        stats.ms_py_mix = int((time.monotonic() - t_mix0) * 1000)
    except Exception:
        pass


# Candidate pool (post-sort/post-diversity). Everything below operates on `candidates`.
    # NOTE: Patch3 fix — patch2 accidentally referenced `candidates` before it was initialized.
    if MAX_CANDIDATES and MAX_CANDIDATES > 0:
        candidates = out_pairs[:MAX_CANDIDATES]
    else:
        candidates = list(out_pairs)

    # Candidate window (diversity pool visibility)
    # This is *not* time-based; it's a "top-K slice" view so we can see if Usenet/P2 gets squeezed
    # out before delivery due to sorting/caps.
    try:
        k = int(os.getenv("CAND_WINDOW_K", "200") or "200")
        k = max(0, min(k, len(out_pairs)))
        win_pairs = out_pairs[:k]

        def _pair_provider(p):
            _s, _m = p
            return str(_m.get("provider") or (_s.get("behaviorHints") or {}).get("provider") or "").upper()

        def _pair_supplier(p):
            _s, _m = p
            bh = (_s.get("behaviorHints") or {}) if isinstance(_s, dict) else {}
            return str(bh.get("wrap_src") or bh.get("source_tag") or _m.get("supplier") or "").upper()

        def _is_usenet_pair(p):
            prov = _pair_provider(p)
            return ("USENET" in prov) or (prov in set(USENET_PRIORITY)) or (prov == "ND")

        def _is_p2_pair(p):
            return _pair_supplier(p) == "P2"

        in_usenet = sum(1 for p in out_pairs if _is_usenet_pair(p))
        usenet_in_k = sum(1 for p in win_pairs if _is_usenet_pair(p))
        p2_in_k = sum(1 for p in win_pairs if _is_p2_pair(p))

        logger.info(
            "CAND_WINDOW rid=%s id=%s k=%s total_pairs=%s in_usenet=%s usenet_in_k=%s p2_in_k=%s",
            rid, id_, k, len(out_pairs), in_usenet, usenet_in_k, p2_in_k
        )
    except Exception as e:
        logger.warning("CAND_WINDOW_FAIL rid=%s id=%s err=%s", rid, id_, e)

    # Android/TV: remove streams that resolve to known error placeholders (e.g., /static/500.mp4)
    if is_android and not ANDROID_VERIFY_OFF:
        if VERIFY_STREAM:
            if did_verify:
                logger.debug("ANDROID_VERIFY_SKIP rid=%s reason=already_verified", rid)
            else:
                try:
                    candidates = _drop_bad_top_n(candidates, top_n=int(ANDROID_VERIFY_TOP_N or 0), timeout_s=float(ANDROID_VERIFY_TIMEOUT or 0.0) or None, range_mode=VERIFY_RANGE, deliver_cap=deliver_cap_eff, sort_buffer=VERIFY_SORT_BUFFER)
                except Exception as e:
                    logger.debug("ANDROID_VERIFY_SKIPPED rid=%s err=%s", rid, e)
        else:
            logger.info("VERIFY_SKIP rid=%s reason=VERIFY_STREAM=false", rid)

    # WebDAV strict (optional): drop TB items that WebDAV cannot confirm.
    # This can be used even without TB_API_KEY / TB_CACHE_HINTS.
    if (not fast_mode) and (not VALIDATE_OFF) and (not WEBDAV_INACTIVE) and USE_TB_WEBDAV and TB_WEBDAV_USER and TB_WEBDAV_PASS and candidates and (TB_WEBDAV_STRICT or (not VERIFY_TB_CACHE_OFF)):
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
            if len(tb_hashes) >= tb_max_hashes:
                break
        if tb_hashes:
            try:
                stats.tb_webdav_hashes = len(tb_hashes)
                t_wd0 = time.monotonic()
                webdav_ok = tb_webdav_batch_check(tb_hashes, stats)  # set of ok hashes
                stats.ms_tb_webdav = int((time.monotonic() - t_wd0) * 1000)
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
    tb_usenet_should_run = False
    tb_usenet_hashes_list: List[str] = []
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
            # Defer the API call so we can run it concurrently with TB torrent cached checks when both are needed.
            tb_usenet_should_run = True
            tb_usenet_hashes_list = uhashes

    # Optional: cached/instant-only enforcement across providers.
    # - TB: requires TB_CACHE_HINTS + TB_API_KEY (otherwise we can't confirm)
    # - RD/AD: heuristic only (no API checks anymore)
    # Cache checks / premium validation
    cached_map: Dict[str, bool] = {}

    # TorBox hash-insta API cached check — runs even when VERIFY_CACHED_ONLY=false (mark-only).
    tb_api_ran = False
    tb_api_reason = ""
    tb_hashes: List[str] = []
    tb_skip_checks = False
    tb_skip_reason = ""
    # Optional early-exit: if enough cached hints already exist in the top slice, skip TorBox API calls.
    # This never skips URL wrapping (/r/<token>) — it only skips expensive TorBox checks.
    if TB_EARLY_EXIT and (not VERIFY_CACHED_ONLY) and candidates:
        try:
            lookahead = min(
                len(candidates),
                max(int(tb_max_hashes or 0) * int(TB_EARLY_EXIT_MULT or 2), int(deliver_cap_eff or MAX_DELIVER or 60)),
            )
            insta_count = 0
            for _s, _m in candidates[:lookahead]:
                bh = (_s.get('behaviorHints') or {})
                c_m = _m.get('cached')
                c_bh = bh.get('cached')
                if c_m is True or c_bh is True:
                    insta_count += 1
                    continue
                if isinstance(c_m, str) and c_m.upper() == "LIKELY":
                    insta_count += 1
                    continue
                if isinstance(c_bh, str) and c_bh.upper() == "LIKELY":
                    insta_count += 1
                    continue
            if insta_count >= int(deliver_cap_eff or MAX_DELIVER or 60):
                tb_skip_checks = True
                tb_skip_reason = f"early_exit_cached_hints={insta_count}"
                # If we're skipping TorBox checks, also skip deferred usenet cached calls.
                tb_usenet_should_run = False
        except Exception:
            pass
    if fast_mode:
        tb_api_reason = "fast_mode"
    elif VALIDATE_OFF:
        tb_api_reason = "validate_off"
    elif not TB_CACHE_HINTS:
        tb_api_reason = "cache_hints=false"
    elif not TB_API_KEY:
        tb_api_reason = "disabled_or_no_key"
    elif not candidates:
        tb_api_reason = "no_candidates"
    else:
        t_tb_prep0 = time.monotonic()
        seen_h: set[str] = set()
        for _s, _m in candidates:
            if len(tb_hashes) >= tb_max_hashes:
                break
            if (_m.get('provider') or '').upper() != 'TB':
                continue
            h = norm_infohash(_m.get('infohash'))
            if h and re.fullmatch(r"[0-9a-f]{40}", h) and h not in seen_h:
                seen_h.add(h)
                tb_hashes.append(h)

        # TorBox known-cached memoization: premark and avoid redundant API checks.
        now_epoch = time.time()
        try:
            _tb_known_cached_prune(now_epoch, int(TB_KNOWN_CACHED_TTL or 0), int(TB_KNOWN_CACHED_MAX or 0))
        except Exception:
            pass
        known_cached = _tb_known_cached_premark(tb_hashes, cached_map, now_epoch)
        tb_hashes_api = [h for h in tb_hashes if h not in known_cached]
        if len(tb_hashes_api) < int(TB_API_MIN_HASHES or 0):
            tb_api_reason = "min_hashes"
        else:
            try:
                try:
                    stats.ms_py_tb_prep = int((time.monotonic() - t_tb_prep0) * 1000)
                except Exception:
                    pass
                t0 = time.monotonic()
                # If both TorBox torrent and TorBox usenet checks are queued, run them concurrently.
                if tb_usenet_should_run and tb_usenet_hashes_list:
                    t_u0 = time.monotonic()
                    try:
                        _ex = ThreadPoolExecutor(max_workers=2)
                        _f_t = _ex.submit(tb_get_cached, tb_hashes_api)
                        _f_u = _ex.submit(tb_get_usenet_cached, tb_usenet_hashes_list)
                        try:
                            try:
                                _timeout = float(os.environ.get('TB_PARALLEL_FUTURE_TIMEOUT', str(TB_BATCH_FUTURE_TIMEOUT or 8.0)))
                            except Exception:
                                _timeout = float(TB_BATCH_FUTURE_TIMEOUT or 8.0)
                            done, not_done = wait([_f_t, _f_u], timeout=_timeout)
                            if _f_t in done:
                                try:
                                    cached_map_raw = _f_t.result()
                                except Exception:
                                    cached_map_raw = {}
                            else:
                                cached_map_raw = {}
                                try:
                                    _f_t.cancel()
                                except Exception:
                                    pass
                            if _f_u in done:
                                try:
                                    usenet_cached_map = _f_u.result() or {}
                                except Exception:
                                    usenet_cached_map = {}
                            else:
                                usenet_cached_map = {}
                                try:
                                    _f_u.cancel()
                                except Exception:
                                    pass
                        finally:
                            try:
                                _ex.shutdown(wait=False, cancel_futures=True)
                            except Exception:
                                pass
                        stats.ms_tb_usenet = int((time.monotonic() - t_u0) * 1000)
                        tb_usenet_should_run = False
                    except Exception:
                        cached_map_raw = tb_get_cached(tb_hashes_api)
                        tb_usenet_should_run = False  # don't retry later; keep request bounded
                else:
                    cached_map_raw = tb_get_cached(tb_hashes_api)
                # cached_map already contains known-cached premasks; merge API results into it.
                for _k, _v in (cached_map_raw or {}).items():
                    _nk = norm_infohash(_k)
                    if _nk:
                        v = bool(_v)
                        cached_map[_nk] = v
                        if v:
                            _tb_known_cached_refresh(_nk, now_epoch, int(TB_KNOWN_CACHED_TTL or 0))
                stats.ms_tb_api = int((time.monotonic() - t0) * 1000)
                stats.tb_api_hashes = len(tb_hashes_api)
                tb_api_ran = True
                tb_api_reason = "ok"
                try:
                    _t = sum(1 for _v in (cached_map or {}).values() if _v)
                    _f = sum(1 for _v in (cached_map or {}).values() if (_v is False))
                    logger.info(
                        "TB_API_DONE rid=%s hashes=%d true=%d false=%d ms_tb_api=%d",
                        _rid(), int(len(tb_hashes)), int(_t), int(_f), int(stats.ms_tb_api or 0)
                    )
                except Exception as _e:
                    logger.debug("TB_API_DONE_ERR rid=%s err=%s", _rid(), _e)
            except Exception as _e:
                tb_api_reason = "api_error"
                try:
                    logger.warning("TB_API_ERR rid=%s err=%s", _rid(), _e)
                except Exception:
                    pass

    try:
        logger.info(
            "TB_API_CHECK rid=%s ran=%s reason=%s hashes=%d min=%d cache_hints=%s api_key=%s fast=%s validate_off=%s verify_cached_only=%s",
            _rid(), bool(tb_api_ran), str(tb_api_reason), int(len(tb_hashes)), int(TB_API_MIN_HASHES or 0),
            bool(TB_CACHE_HINTS), bool(TB_API_KEY), bool(fast_mode), bool(VALIDATE_OFF), bool(VERIFY_CACHED_ONLY),
        )
    except Exception:
        pass


    # If TorBox usenet cached check was deferred and not executed alongside TB torrent cached checks, run it now.
    if tb_usenet_should_run and tb_usenet_hashes_list:
        try:
            t_u0 = time.monotonic()
            usenet_cached_map = tb_get_usenet_cached(tb_usenet_hashes_list)
            stats.ms_tb_usenet = int((time.monotonic() - t_u0) * 1000)
        except Exception:
            pass
        tb_usenet_should_run = False

    # Point 3 clarity: always log whether "uncached" enforcement actually ran,
    # and whether it was only marking (KEEP) vs hard dropping (DROP).
    uncached_policy = "SKIP"   # SKIP | KEEP | DROP
    uncached_reason = ""
    uncached_ran = False
    dropped_uncached = 0
    dropped_uncached_tb = 0

    if fast_mode:
        uncached_reason = "fast_mode"
    elif VALIDATE_OFF:
        uncached_reason = "VALIDATE_OFF"
    elif not VERIFY_CACHED_ONLY:
        uncached_reason = "VERIFY_CACHED_ONLY=false"
    elif STRICT_PREMIUM_ONLY:
        uncached_policy = "DROP"
        uncached_reason = "STRICT_PREMIUM_ONLY=true"
    else:
        uncached_policy = "KEEP"
        uncached_reason = "mark_only"


    # Mark-only: update TB cached flags from the API even when VERIFY_CACHED_ONLY=false (no dropping).
    if (not fast_mode) and (not VALIDATE_OFF) and (not VERIFY_CACHED_ONLY) and tb_api_ran:
        tb_total = 0
        tb_mark_true = 0
        tb_mark_false = 0
        tb_flip = 0
        t_mark0 = time.monotonic()
        for _s, _m in candidates:
            if (_m.get('provider') or '').upper() != 'TB':
                continue
            tb_total += 1
            h = norm_infohash(_m.get('infohash'))
            orig_tb_cached = (_m.get('cached') is True)
            if h and h in cached_map:
                _m['cached'] = bool(cached_map.get(h, False))
                if _m['cached'] is True:
                    tb_mark_true += 1
                else:
                    tb_mark_false += 1
                if _m['cached'] is True and not orig_tb_cached:
                    tb_flip += 1
                with CACHED_HISTORY_LOCK:
                    CACHED_HISTORY[h] = bool(_m['cached'])
        try:
            stats.ms_py_tb_mark_only = int((time.monotonic() - t_mark0) * 1000)
        except Exception:
            pass

        if tb_total:
            try:
                logger.info("TB_FLIPS rid=%s flipped=%s/%s tb_api_hashes=%s", _rid(), tb_flip, tb_total, int(stats.tb_api_hashes or 0))
                logger.info(
                    "TB_MARK_SUMMARY rid=%s mode=mark_only tb_total=%d mark_true=%d mark_false=%d src_api=%d src_hist=%d src_assume=%d src_nohash=%d",
                    _rid(), int(tb_total), int(tb_mark_true), int(tb_mark_false),
                    int(tb_total), 0, 0, 0,
                )
            except Exception:
                pass

    if (not fast_mode) and VERIFY_CACHED_ONLY and not VALIDATE_OFF:
        uncached_ran = True
        # TorBox API cached check is performed above (runs even when VERIFY_CACHED_ONLY=false).
        # Here we only attach cached markers / enforce policy based on `cached_map`.
        # Attach cached markers to meta; in loose mode we do NOT hard-drop.
        t_unc0 = time.monotonic()
        kept = []
        dropped_uncached = 0
        dropped_uncached_tb = 0
        tb_flip = 0
        tb_total = 0
        tb_mark_true = 0
        tb_mark_false = 0
        tb_src_api = 0
        tb_src_hist = 0
        tb_src_assume = 0
        tb_src_nohash = 0
        usenet_provs = {str(p).upper() for p in (USENET_PROVIDERS or USENET_PRIORITY or [])}
        usenet_provs.add('ND')  # Treat ND as usenet-like
        for _s, _m in candidates:
            provider = (_m.get('provider') or '').upper()
            h = norm_infohash(_m.get('infohash'))
            orig_tb_cached = (_m.get('cached') is True)
            bh = _s.get('behaviorHints') or {}
            text = (str(_s.get('name') or '') + ' ' + str(_s.get('description') or '') + ' ' + str(bh.get('filename') or '') + ' ' + str(bh.get('bingeGroup') or '')).lower()

            cached_marker = None
            if provider == 'TB':
                if h and cached_map:
                    cached_marker = bool(cached_map.get(h, False))
                    tb_src_api += 1
                elif h:
                    with CACHED_HISTORY_LOCK:
                        cached_marker = bool(CACHED_HISTORY.get(h, False))
                    tb_src_hist += 1
                else:
                    # No hash: prefer truth tag (C:true/false) if present (gated).
                    aio0 = _m.get('aio') if isinstance(_m, dict) else None
                    aio_cached0 = None
                    try:
                        if isinstance(aio0, dict):
                            aio_cached0 = aio0.get('cached', None)
                    except Exception:
                        aio_cached0 = None

                    if USE_AIO_READY and isinstance(aio_cached0, bool):
                        cached_marker = bool(aio_cached0)
                    elif ASSUME_PREMIUM_ON_FAIL:
                        cached_marker = True
                        tb_src_assume += 1
                    else:
                        cached_marker = False
                        tb_src_nohash += 1
            elif provider in ('RD', 'AD'):
                aio0 = _m.get('aio') if isinstance(_m, dict) else None
                aio_cached0 = None
                try:
                    if isinstance(aio0, dict):
                        aio_cached0 = aio0.get('cached', None)
                except Exception:
                    aio_cached0 = None

                if USE_AIO_READY and isinstance(aio_cached0, bool):
                    cached_marker = bool(aio_cached0)
                else:
                    cached_marker = 'LIKELY' if _heuristic_cached(_s, _m) else False
            elif provider in usenet_provs:
                existing = _m.get('cached', None)

                aio0 = _m.get('aio') if isinstance(_m, dict) else None
                aio_cached0 = None
                try:
                    if isinstance(aio0, dict):
                        aio_cached0 = aio0.get('cached', None)
                except Exception:
                    aio_cached0 = None

                if USE_AIO_READY and isinstance(aio_cached0, bool):
                    cached_marker = bool(aio_cached0)
                elif existing is True:
                    cached_marker = True
                elif existing == 'LIKELY':
                    cached_marker = 'LIKELY'
                else:
                    cached_marker = 'LIKELY' if _looks_instant(text) else None
            else:
                cached_marker = None

            _m['cached'] = cached_marker
            if provider == 'TB':
                tb_total += 1
                if cached_marker is True:
                    tb_mark_true += 1
                else:
                    tb_mark_false += 1
                if h and cached_marker is True and not orig_tb_cached:
                    tb_flip += 1
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

        if tb_total:
            logger.info("TB_FLIPS rid=%s flipped=%s/%s tb_api_hashes=%s", _rid(), tb_flip, tb_total, int(stats.tb_api_hashes or 0))
            logger.info(
                "TB_MARK_SUMMARY rid=%s tb_total=%d mark_true=%d mark_false=%d src_api=%d src_hist=%d src_assume=%d src_nohash=%d",
                _rid(), int(tb_total), int(tb_mark_true), int(tb_mark_false),
                int(tb_src_api), int(tb_src_hist), int(tb_src_assume), int(tb_src_nohash)
            )
        candidates = kept
        stats.dropped_uncached += dropped_uncached
        stats.dropped_uncached_tb += dropped_uncached_tb

        try:
            stats.ms_uncached_check += int((time.monotonic() - t_unc0) * 1000)
        except Exception:
            pass


    logger.info(
        "UNCACHED_POLICY rid=%s policy=%s ran=%s reason=%s dropped_uncached=%s dropped_uncached_tb=%s",
        _rid(),
        str(uncached_policy),
        bool(uncached_ran),
        str(uncached_reason),
        int(dropped_uncached or 0),
        int(dropped_uncached_tb or 0),
    )

    # Clarity log: tells you if TB checks actually ran and how many hashes were checked.
    logger.info(
        "TB_CHECKS rid=%s webdav_active=%s webdav_reason=%s api_ran=%s api_reason=%s api_hashes=%s tb_hashes=%s min=%s cache_hints=%s api_key=%s fast=%s validate_off=%s verify_cached_only=%s",
        _rid(),
        False,
        "INACTIVE",
        bool(tb_api_ran),
        str(tb_api_reason),
        int(stats.tb_api_hashes or 0),
        int(len(tb_hashes)),
        int(TB_API_MIN_HASHES or 0),
        bool(TB_CACHE_HINTS),
        bool(TB_API_KEY),
        bool(fast_mode),
        bool(VALIDATE_OFF),
        bool(VERIFY_CACHED_ONLY),
    )

    # Ensure we keep/deliver some usenet entries (if configured).
    if MIN_USENET_KEEP or MIN_USENET_DELIVER:
        # KEEP: ensure at least MIN_USENET_KEEP in the pool
        if MIN_USENET_KEEP:
            have = sum(1 for p in candidates if _is_usenet_pair(p))
            if have < MIN_USENET_KEEP:
                for p2 in out_pairs[len(candidates):]:
                    if _is_usenet_pair(p2) and p2 not in candidates:
                        candidates.append(p2)
                        have += 1
                        if have >= MIN_USENET_KEEP:
                            break
        # DELIVER: ensure at least MIN_USENET_DELIVER within the first MAX_DELIVER
        if MIN_USENET_DELIVER:
            slice_ = candidates[:deliver_cap_eff]
            have = sum(1 for p in slice_ if _is_usenet_pair(p))
            if have < MIN_USENET_DELIVER:
                # Extras from beyond the deliver slice (and from the tail of out_pairs) that are usenet-like.
                # Interleave them into the first window WITHOUT re-sorting the full slice (preserves stability).
                pool = candidates[deliver_cap_eff:] + out_pairs[len(candidates):]
                extras: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
                seen: set = set()

                for p in pool:
                    if not _is_usenet_pair(p):
                        continue
                    if p in slice_:
                        continue
                    try:
                        k2 = dedup_key(p[0], p[1])
                    except Exception:
                        k2 = ""
                    if k2 and k2 in seen:
                        continue
                    if k2:
                        seen.add(k2)
                    extras.append(p)

                extras.sort(key=sort_key)

                target_extras = min(MIN_USENET_DELIVER - have, len(extras))
                if target_extras > 0:
                    new_slice: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
                    debrid_idx = 0
                    extra_idx = 0
                    while len(new_slice) < deliver_cap_eff and (debrid_idx < len(slice_) or extra_idx < target_extras):
                        if extra_idx < target_extras:
                            new_slice.append(extras[extra_idx])
                            extra_idx += 1
                        if debrid_idx < len(slice_):
                            new_slice.append(slice_[debrid_idx])
                            debrid_idx += 1
                    # Fill remaining slots with original slice (no re-sort)
                    while len(new_slice) < deliver_cap_eff and debrid_idx < len(slice_):
                        new_slice.append(slice_[debrid_idx])
                        debrid_idx += 1
                    slice_ = new_slice[:deliver_cap_eff]
            candidates = slice_ + candidates[deliver_cap_eff:]    # Android/Google TV clients can't handle magnet: links; drop them and backfill with direct URLs.
    # Premium mix: optionally ensure TB/RD representation in the first deliver_cap_eff
    if (MIN_TB_DELIVER or MIN_RD_DELIVER) and (not iphone_usenet_mode):
        def _prov_of(pair):
            return (pair[1].get("provider") or "").upper()

        def _key_of(pair):
            s, m = pair
            u = (s.get("url") or s.get("externalUrl") or "").strip()
            ih = (m.get("infoHash") or s.get("infoHash") or "").strip().lower()
            n = (s.get("name") or "").strip()
            return (u, ih, n)

        def _inject_min(slice_, pool, prov, need_n):
            if need_n <= 0:
                return slice_
            have = sum(1 for p in slice_ if _prov_of(p) == prov)
            if have >= need_n:
                return slice_
            need = need_n - have
            used = set(_key_of(p) for p in slice_)
            extras = []
            for p in pool:
                if _prov_of(p) != prov:
                    continue
                k = _key_of(p)
                if k in used:
                    continue
                extras.append(p)
                used.add(k)
                if len(extras) >= need:
                    break
            if not extras:
                return slice_
            # interleave: put an extra, then 2 originals, repeat
            out = []
            ei = 0
            oi = 0
            while len(out) < len(slice_) and (ei < len(extras) or oi < len(slice_)):
                if ei < len(extras):
                    out.append(extras[ei]); ei += 1
                    if len(out) >= deliver_cap_eff:
                        break
                # keep a couple originals for stability
                for _ in range(2):
                    if oi < len(slice_) and len(out) < deliver_cap_eff:
                        out.append(slice_[oi]); oi += 1
            # fill remainder
            while oi < len(slice_) and len(out) < deliver_cap_eff:
                out.append(slice_[oi]); oi += 1
            return out[:deliver_cap_eff]

        slice_ = candidates[:deliver_cap_eff]
        pool = candidates[deliver_cap_eff:] + out_pairs  # out_pairs is already sorted; safe as a pool
        slice_ = _inject_min(slice_, pool, "TB", int(MIN_TB_DELIVER or 0))
        slice_ = _inject_min(slice_, pool, "RD", int(MIN_RD_DELIVER or 0))
        # rebuild candidates with enforced slice
        used = set(_key_of(p) for p in slice_)
        tail = [p for p in candidates if _key_of(p) not in used]
        candidates = slice_ + tail

    if is_iphone:
        def _is_magnet(u: str) -> bool:
            return isinstance(u, str) and u.startswith("magnet:")
        magnets = sum(1 for s, _m in candidates if _is_magnet((s or {}).get("url", "")))
        if magnets:
            long_urls = 0
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
                    if is_iphone and isinstance(u, str) and len(u) > 2000:
                        long_urls += 1
                        continue
                    key = (u, (s or {}).get("name") or "", (s or {}).get("title") or "")
                    if key in seen:
                        continue
                    seen.add(key)
                    kept.append((s, m))
                    if len(kept) >= deliver_cap_eff:
                        break
            if is_iphone:
                stats.dropped_iphone_magnets += (magnets + (long_urls if 'long_urls' in locals() else 0))
                platform = "iphone"
            else:
                stats.dropped_android_magnets += magnets
                platform = "android"
            # Total platform-specific drops (android+iphone magnets)
            stats.dropped_platform_specific += magnets
            logger.info("MOBILE_FILTER rid=%s platform=%s dropped_magnets=%s", _rid(), platform, magnets)
            candidates = kept


    # --- Usenet REAL priority mix (final-order shaping)
    # If we have probed REAL usenet links, ensure they are visible early without letting usenet dominate top-10.
    # Policy: ~50% of top-10 from REAL usenet, and promote remaining REAL usenet into top-20.
    if USENET_PROBE_ENABLE and candidates and deliver_cap_eff > 0:
        try:
            candidates = _apply_usenet_real_priority_mix(
                candidates,
                deliver_cap=int(deliver_cap_eff or 0),
                top10_pct=float(USENET_PROBE_REAL_TOP10_PCT or 0.5),
                top20_n=int(USENET_PROBE_REAL_TOP20_N or 20),
                stats=stats,
            )
        except Exception:
            pass

    # Format (last step)
    delivered: List[Dict[str, Any]] = []
    delivered_dbg: List[Dict[str, Any]] = []  # debug-only (not returned)
    _wrap_base = None
    _wrapped_url_map: Dict[str, str] = {}
    if WRAP_PLAYBACK_URLS:
        try:
            _wrap_base = _public_base_url().rstrip('/')
        except Exception:
            _wrap_base = None
        try:
            _seen_u: set[str] = set()
            for _s, _m in candidates[:deliver_cap_eff]:
                _u = _s.get("url") or _s.get("externalUrl") or ""
                if isinstance(_u, str) and _u and (_u not in _seen_u):
                    _seen_u.add(_u)
                    _wrapped_url_map[_u] = wrap_playback_url(_u, _base=_wrap_base, meta={
    "emit_rid": _rid(),
    "provider": (_m.get("provider") or "UNK"),
    "tag": (_m.get("tag") or ""),
    "res": (_m.get("res") or "SD"),
    "seeders": int(_m.get("seeders") or 0),
    "size": int(_m.get("size") or 0),
    "cached": (_m.get("cached") if _m.get("cached") is not None else ""),
    "ready": bool(_m.get("ready") or False),
    "has_hash": bool((_m.get("infohash") or "").strip() or _s.get("infoHash") or _s.get("infohash")),
    "name": (_s.get("name") or ""),
    "platform": client_platform(is_android=is_android, is_iphone=is_iphone),
})
        except Exception:
            _wrapped_url_map = {}
    t_wrap0 = time.monotonic()
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
            out_url = _wrapped_url_map.get(raw_url)
            if out_url:
                # Best-effort: enrich token metadata for debugging
                try:
                    if WRAP_URL_SHORT and _wrap_base and isinstance(out_url, str) and out_url.startswith(_wrap_base + "/r/"):
                        _tok = out_url.split("/r/", 1)[1]
                        _wrap_url_meta_update(_tok, {
                            "emit_rid": _rid(),
                            "provider": (m.get("provider") or "UNK"),
                            "tag": (m.get("tag") or ""),
                            "res": (m.get("res") or "SD"),
                            "seeders": int(m.get("seeders") or 0),
                            "size": int(m.get("size") or 0),
                            "name": (s.get("name") or ""),
                            "cached": (cached_marker if cached_marker is not None else (cached_hint or "")),
                            "ready": bool(m.get("ready") or False),
                            "has_hash": bool(h or s.get("infoHash") or s.get("infohash")),
                            "platform": client_platform(is_android=is_android, is_iphone=is_iphone),
                        })
                except Exception:
                    pass
            else:
                out_url = wrap_playback_url(raw_url, _base=_wrap_base, meta={
                    "emit_rid": _rid(),
                    "provider": (m.get("provider") or "UNK"),
                    "tag": (m.get("tag") or ""),
                    "res": (m.get("res") or "SD"),
                    "seeders": int(m.get("seeders") or 0),
                    "size": int(m.get("size") or 0),
                    "name": (s.get("name") or ""),
                    "cached": (cached_marker if cached_marker is not None else (cached_hint or "")),
                    "ready": bool(m.get("ready") or False),
                    "has_hash": bool(h or s.get("infoHash") or s.get("infohash")),
                    "platform": client_platform(is_android=is_android, is_iphone=is_iphone),
                })

        if logger.isEnabledFor(logging.DEBUG):
            delivered_dbg.append({
                "name": s.get("name", ""),
                "provider": (m.get("provider") or "UNK"),
                "res": (m.get("res") or "SD"),
                "seeders": int(m.get("seeders") or 0),
                "cached": cached_marker if cached_marker is not None else (cached_hint or ""),
                "has_hash": bool(h or s.get("infoHash") or s.get("infohash")),
                "url_len": len(out_url or ""),
            })

        if OUTPUT_NEW_OBJECT:
            _out_s = build_stream_object_rich(
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
            # Add: Hint for Stremio hash skip on iPhone
            if is_iphone:
                _bh = _out_s.get("behaviorHints", {})
                if isinstance(_bh, dict):
                    _bh["noHash"] = True  # Add flag for direct play (bypasses popup)
                    _out_s["behaviorHints"] = _bh

            # Expose usenet probe result/type in behaviorHints for debugging/tests.
            # (Stremio typically ignores unknown behaviorHints keys.)
            try:
                _bh2 = _out_s.get("behaviorHints", {})
                if isinstance(_bh2, dict):
                    _up = m.get("usenet_probe")
                    if _up:
                        _bh2["usenetProbe"] = str(_up)
                    _aio2 = m.get("aio")
                    if isinstance(_aio2, dict) and _aio2.get("type"):
                        _bh2["aioType"] = str(_aio2.get("type"))
                    _out_s["behaviorHints"] = _bh2
            except Exception:
                pass
            delivered.append(_out_s)

        else:
            # Legacy behavior (kept as a safety switch)
            bh = s.get("behaviorHints")
            if not isinstance(bh, dict):
                bh = {}
                s["behaviorHints"] = bh
            try:
                _sup = str((bh.get("wrap_src") or bh.get("source_tag") or m.get("supplier") or (AIO_TAG or "AIO"))).upper()
                bh["wrap_src"] = _sup
                bh["source_tag"] = _sup
            except Exception:
                pass
            # Always write provider/source for debugging & consistent stats
            bh["provider"] = str(m.get("provider") or bh.get("provider") or "UNK").upper()
            bh["source"] = str(m.get("source") or bh.get("source") or "UNK")
            # Cache marker should reflect our best-known truth on delivered streams
            if is_confirmed:
                bh["cached"] = True
            elif isinstance(cached_marker, bool):
                bh["cached"] = cached_marker
            elif cached_hint == "LIKELY":
                bh.setdefault("cached", "LIKELY")
            if REFORMAT_STREAMS:
                format_stream_inplace(s, m, expected, cached_hint, type_, season, episode)
            s["url"] = out_url
            delivered.append(s)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("DELIVERED_TOP5 rid=%s count=%d", _rid(), len(delivered))
        for i, d in enumerate(delivered_dbg[:5]):
            logger.debug(
                "  #%d res=%s seeders=%s prov=%s cached=%s hash=%s url_len=%s platform_note=%s name=%r",
                i,
                d.get("res"),
                d.get("seeders"),
                d.get("provider"),
                d.get("cached"),
                "yes" if d.get("has_hash") else "no",
                d.get("url_len"),
                ("android" if is_android else ("iphone" if is_iphone else "")),
                d.get("name"),
            )

    stats.ms_py_wrap_emit = int((time.monotonic() - t_wrap0) * 1000)
    stats.delivered = len(delivered)

    # Cache summary (delivered streams only): keep WRAP_STATS aligned with WRAP_COUNTS out.cached
    try:
        hit = 0
        miss = 0
        likely = 0
        for _s in delivered:
            if not isinstance(_s, dict):
                continue
            _bh = _s.get("behaviorHints") or {}
            _c = _bh.get("cached", None)
            if _c is True:
                hit += 1
            elif _c is False:
                miss += 1
            elif _c == "LIKELY":
                likely += 1
        stats.cache_hit = int(hit)
        stats.cache_miss = int(miss)
        denom = hit + miss + likely
        stats.cache_rate = (float(hit) + (0.5 * float(likely))) / float(denom) if denom > 0 else 0.0
    except Exception:
        pass



    # Flag potential issues (per-request; visible in logs and ?debug=1)
    try:
        if int(stats.merged_in or 0) > 0:
            total_drops = max(0, int(stats.merged_in) - int(stats.delivered or 0))
            drop_pct = (total_drops / float(stats.merged_in)) * 100.0 if float(stats.merged_in or 0) > 0 else 0.0
            if drop_pct >= float(FLAG_HIGH_DROP_PCT):
                stats.flag_issues.append(f"high_drops:{drop_pct:.1f}%")
    except Exception:
        pass
    try:
        if int(stats.ms_title_mismatch or 0) >= int(FLAG_SLOW_TITLE_MS):
            stats.flag_issues.append(f"slow_title:{int(stats.ms_title_mismatch)}ms")
        if int(stats.ms_uncached_check or 0) >= int(FLAG_SLOW_UNCACHED_MS):
            stats.flag_issues.append(f"slow_uncached:{int(stats.ms_uncached_check)}ms")
    except Exception:
        pass


    # RD heuristic marker (parity with NZBGeek markers): proves RD heuristic actually ran.
    try:
        rd_out_true = rd_out_likely = rd_out_false = rd_out_unk = 0
        for _s in delivered:
            if not isinstance(_s, dict):
                continue
            _bh = _s.get("behaviorHints") or {}
            _prov = str((_s.get("provider") or _bh.get("provider") or "")).upper().strip()
            if _prov.startswith("DL-"):
                _prov = _prov[3:]
            if _prov not in ("RD", "REALDEBRID"):
                continue
            _c = _bh.get("cached", None)
            if _c is True:
                rd_out_true += 1
            elif _c is False:
                rd_out_false += 1
            elif _c == "LIKELY":
                rd_out_likely += 1
            else:
                rd_out_unk += 1

        if int(getattr(stats, "rd_heur_calls", 0) or 0) > 0 or (rd_out_true + rd_out_likely + rd_out_false + rd_out_unk) > 0:
            avg_conf = (float(getattr(stats, "rd_heur_conf_sum", 0.0) or 0.0) / float(stats.rd_heur_calls)) if int(stats.rd_heur_calls or 0) > 0 else 0.0
            logger.info(
                "RD_HEUR_MAINTAIN rid=%s mode=heuristic thr=%.2f calls=%d ok=%d miss=%d avg_conf=%.2f out_cached_true=%d out_cached_likely=%d out_cached_false=%d out_cached_unk=%d",
                _rid(),
                float(RD_HEUR_THR or 0.70),
                int(getattr(stats, "rd_heur_calls", 0) or 0),
                int(getattr(stats, "rd_heur_true", 0) or 0),
                int(getattr(stats, "rd_heur_false", 0) or 0),
                float(avg_conf),
                int(rd_out_true),
                int(rd_out_likely),
                int(rd_out_false),
                int(rd_out_unk),
            )
    except Exception:
        pass

    stats.ms_py_ff = int((time.monotonic() - t_ff0) * 1000)
    # Unaccounted time inside filter_and_format (helps explain “where did the seconds go?”).
    # NOTE: These are *our-side* timers only; upstream (AIOStreams) wall times must be compared via logs externally.
    try:
        _known = 0
        _known += int(stats.ms_tmdb or 0)
        _known += int(stats.ms_py_clean or 0)
        _known += int(stats.ms_title_mismatch or 0)
        _known += int(stats.ms_py_sort or 0)
        _known += int(stats.ms_py_mix or 0)
        _known += int(stats.ms_py_dedup or 0)
        _known += int(stats.ms_tb_webdav or 0)
        _known += int(stats.ms_py_tb_prep or 0)
        _known += int(stats.ms_tb_api or 0)
        _known += int(stats.ms_py_tb_mark_only or 0)
        _known += int(stats.ms_tb_usenet or 0)
        _known += int(stats.ms_usenet_ready_match or 0)
        _known += int(stats.ms_usenet_probe or 0)
        _known += int(stats.ms_py_wrap_emit or 0)
        stats.ms_py_ff_overhead = max(0, int(stats.ms_py_ff or 0) - _known)
    except Exception:
        pass

    # Clear TLS stats pointer to avoid leaking across requests
    try:
        if hasattr(_TLS, "stats"):
            delattr(_TLS, "stats")
    except Exception:
        pass

    return delivered, stats

# ---------------------------
# Endpoints
# ---------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "build": BUILD_ID, "ts": int(time.time())}), 200


# ---------------------------
# Global rolling stats (/stats)
# ---------------------------
_GLOBAL_STATS_LOCK = threading.Lock()
_GLOBAL_STATS: Dict[str, Any] = {
    "since_ts": int(time.time()),
    "requests": 0,
    "errors": 0,
    "served_cache": 0,
    "served_empty": 0,
    "by_platform": {},
    "delivered_sum": 0,
    "ms_sum": 0,
}
_RECENT_REQUESTS = deque(maxlen=75)

def _gs_bump(d: Dict[str, Any], k: str, n: int = 1) -> None:
    try:
        d[k] = int(d.get(k, 0)) + int(n)
    except Exception:
        d[k] = int(n)

def _update_global_stats(*, platform: str, delivered: int, ms_total: int, served_cache: bool, is_error: bool, flags: List[str]) -> None:
    try:
        with _GLOBAL_STATS_LOCK:
            _GLOBAL_STATS["requests"] = int(_GLOBAL_STATS.get("requests", 0)) + 1
            if is_error:
                _GLOBAL_STATS["errors"] = int(_GLOBAL_STATS.get("errors", 0)) + 1
            if served_cache:
                _GLOBAL_STATS["served_cache"] = int(_GLOBAL_STATS.get("served_cache", 0)) + 1
            if int(delivered or 0) <= 0:
                _GLOBAL_STATS["served_empty"] = int(_GLOBAL_STATS.get("served_empty", 0)) + 1
            _gs_bump(_GLOBAL_STATS.setdefault("by_platform", {}), (platform or "unknown"))
            _GLOBAL_STATS["delivered_sum"] = int(_GLOBAL_STATS.get("delivered_sum", 0)) + int(delivered or 0)
            _GLOBAL_STATS["ms_sum"] = int(_GLOBAL_STATS.get("ms_sum", 0)) + int(ms_total or 0)

            _RECENT_REQUESTS.appendleft({
                "ts": int(time.time()),
                "rid": _rid(),
                "platform": platform or "unknown",
                "delivered": int(delivered or 0),
                "ms": int(ms_total or 0),
                "cache": bool(served_cache),
                "flags": (list(flags)[:6] if isinstance(flags, list) else []),
            })
    except Exception:
        pass

@app.get("/stats")
def stats_endpoint():
    if not ENABLE_STATS_ENDPOINT:
        return jsonify({"ok": False, "disabled": True}), 404
    with _GLOBAL_STATS_LOCK:
        snap = dict(_GLOBAL_STATS)
        snap["recent"] = list(_RECENT_REQUESTS)
        # convenience derived fields
        req = int(snap.get("requests", 0) or 0)
        snap["avg_delivered"] = (float(snap.get("delivered_sum", 0)) / req) if req else 0.0
        snap["avg_ms"] = (float(snap.get("ms_sum", 0)) / req) if req else 0.0
    return jsonify(snap), 200

@lru_cache(maxsize=1)
def _manifest_base() -> dict:
    """Load an optional manifest template from disk (default: ./manifest.json).

    This lets you keep stable fields (id, idPrefixes, description, etc.) in the repo,
    while still overriding name/version via env without editing Python.
    """
    path = (os.environ.get("ADDON_MANIFEST_FILE") or "manifest.json").strip()
    if not path:
        return {}
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = path if os.path.isabs(path) else os.path.join(base_dir, path)
        with open(full_path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


@app.get("/logo.png")
def logo_png():
    """
    Serve a local logo.png from the repo root.
    This avoids Stremio/web clients blocking external image hosts.
    """
    try:
        here = os.path.dirname(__file__)
        path = os.path.join(here, "logo.png")
        if not os.path.exists(path):
            return ("", 404)
        return send_from_directory(here, "logo.png", mimetype="image/png")
    except Exception:
        return ("", 404)

@app.get("/manifest.json")
def manifest():
    base = dict(_manifest_base() or {})

    # Defaults (can be overridden by manifest.json and/or env vars).
    DEFAULT_ADDON_NAME = "Buubuu Wrapper"
    DEFAULT_ADDON_DESC = "The best mix of AIOStreams, usenet & debrid for premium, filtered streams — fast & reliable!"
    # Prefer local /logo.png if present (more reliable for Stremio). Fallback to a public icon.
    local_logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
    if os.path.exists(local_logo_path):
        DEFAULT_ADDON_LOGO = request.url_root.rstrip("/") + "/logo.png"
    else:
        DEFAULT_ADDON_LOGO = (
            "https://uxwing.com/wp-content/themes/uxwing/download/"
            "video-photography-multimedia/live-streaming-icon.png"
        )
    addon_name = (os.environ.get("ADDON_NAME") or DEFAULT_ADDON_NAME).strip() or DEFAULT_ADDON_NAME
    addon_desc = (os.environ.get("ADDON_DESCRIPTION") or DEFAULT_ADDON_DESC).strip() or DEFAULT_ADDON_DESC
    # Stremio already shows "Movies & Series" from types; avoid repeating it in description.
    _d = addon_desc.strip()
    if _d.lower().startswith("movies & series"):
        for _sep in ("|", "—", "-", ":"):
            if _sep in _d:
                _left, _right = _d.split(_sep, 1)
                if _left.strip().lower().startswith("movies & series"):
                    addon_desc = _right.strip()
                    break
    addon_logo = (os.environ.get("ADDON_LOGO") or base.get("logo") or DEFAULT_ADDON_LOGO).strip() or DEFAULT_ADDON_LOGO
    addon_display_ver = (os.environ.get("ADDON_DISPLAY_VERSION") or "11.7").strip() or "11.7"
    addon_manifest_ver = (os.environ.get("ADDON_MANIFEST_VERSION") or "11.7.0").strip() or "11.7.0"

    # Keep stable fields from manifest.json (if present) but ensure required keys exist.
    base.setdefault("id", "org.buubuu.aio.wrapper.merge")
    base.setdefault("resources", ["stream"])
    base.setdefault("types", ["movie", "series"])
    base.setdefault("catalogs", [])
    base.setdefault("idPrefixes", ["tt", "tmdb"])
    base.setdefault("behaviorHints", {"configurable": True, "configurationRequired": False})

    # Override name/version/branding via env for easy changes without code edits.
    base["version"] = addon_manifest_ver
    base["name"] = f"{addon_name} v{addon_display_ver}"
    base["description"] = addon_desc
    base["logo"] = addon_logo

    return jsonify(base)
@app.get("/stream/<type_>/<id_>.json")
def stream(type_: str, id_: str):
    if not _is_valid_stream_id(type_, id_):
        return jsonify({"streams": []}), 400

    mem_start = 0
    try:
        mem_start = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss or 0)
    except Exception:
        mem_start = 0

    t0 = time.monotonic()
    fetch_wall_ms = 0
    stats = PipeStats()

    is_android = is_android_client()
    is_iphone = is_iphone_client()
    platform = client_platform(is_android, is_iphone)
    _set_stats_platform(stats, platform)
    cache_key = f"{type_}:{id_}"
    served_from_cache = False
    is_error = False

    # debug toggle
    dbg_q = request.args.get("debug") or request.args.get("dbg") or ""
    want_dbg = WRAP_EMBED_DEBUG or (isinstance(dbg_q, str) and dbg_q.strip() not in ("", "0", "false", "False"))

    try:
        t_fetch_wall0 = time.monotonic()
        streams, aio_in, prov2_in, ms_aio_local, ms_p2_local, prefiltered, pre_stats, fetch_meta = get_streams(
            type_,
            id_,
            is_android=is_android,
            is_iphone=is_iphone,
            client_timeout_s=(ANDROID_STREAM_TIMEOUT if (is_android or is_iphone) else DESKTOP_STREAM_TIMEOUT),
        )

        fetch_wall_ms = int((time.monotonic() - t_fetch_wall0) * 1000)

        if prefiltered:
            out = streams
            stats = pre_stats if isinstance(pre_stats, PipeStats) else PipeStats()
        else:
            out, stats = filter_and_format(
                type_,
                id_,
                streams,
                aio_in=aio_in,
                prov2_in=prov2_in,
                is_android=is_android,
                is_iphone=is_iphone,
            )

        # Ensure platform info survives prefiltered stats
        _set_stats_platform(stats, platform)
        # Memory tracking (ru_maxrss delta; kb on Linux)
        try:
            mem_end = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss or 0)
            stats.memory_peak_kb = max(0, int(mem_end) - int(mem_start or 0))
        except Exception:
            stats.memory_peak_kb = 0

        # Attach fetch timings
        stats.aio_in = int(aio_in or 0)
        stats.prov2_in = int(prov2_in or 0)
        stats.ms_fetch_aio = int(ms_aio_local or 0)
        stats.ms_fetch_p2 = int(ms_p2_local or 0)

        # Patch 6: counts + fetch meta (safe; used for logs/debug)
        try:
            stats.fetch_aio = _compact_fetch_meta((fetch_meta or {}).get("aio") or {})
            stats.fetch_p2 = _compact_fetch_meta((fetch_meta or {}).get("p2") or {})
            try:
                stats.ms_join_aio = int(((fetch_meta or {}).get("aio") or {}).get("wait_ms") or 0)
                stats.ms_join_p2  = int(((fetch_meta or {}).get("p2") or {}).get("wait_ms") or 0)
            except Exception:
                pass
            try:
                ms_aio_remote = int((stats.fetch_aio or {}).get('ms_provider') or 0)
                ms_p2_remote  = int((stats.fetch_p2 or {}).get('ms_provider') or 0)
                stats.ms_fetch_aio_remote = ms_aio_remote
                stats.ms_fetch_p2_remote  = ms_p2_remote
            except Exception:
                stats.ms_fetch_aio_remote = 0
                stats.ms_fetch_p2_remote = 0
            # Error breakdown from fetch meta (timeout/json/other)
            try:
                for _fm in (stats.fetch_aio, stats.fetch_p2):
                    _err = str((_fm or {}).get("err") or "").lower().strip()
                    if not _err:
                        continue
                    if _err == "timeout":
                        stats.errors_timeout += 1
                    elif _err == "json":
                        stats.errors_parse += 1
                    elif _err not in ("", "no_base", "cache_hit", "cache_miss"):
                        stats.errors_api += 1
            except Exception:
                pass
        except Exception:
            stats.fetch_aio, stats.fetch_p2 = {}, {}
        try:
            stats.counts_in = _summarize_streams_for_counts(streams)
            stats.counts_out = _summarize_streams_for_counts(out)
        except Exception:
            stats.counts_in, stats.counts_out = {}, {}

        # Provider anomalies (useful when env changes)
        try:
            if AIO_BASE and int(stats.aio_in or 0) == 0:
                if is_iphone and IPHONE_USENET_ONLY:
                    stats.flag_issues.append("aio_skipped_iphone")
                else:
                    stats.flag_issues.append("aio_empty")
            if not AIO_BASE:
                stats.flag_issues.append("aio_no_base")
            if PROV2_BASE and int(stats.prov2_in or 0) == 0:
                stats.flag_issues.append("p2_empty")
        except Exception:
            pass

        # Fallback cache: only used when upstream returns empty (or errors later)
        if out:
            cache_set(cache_key, out)
        else:
            cached = cache_get(cache_key)
            if cached:
                out = cached
                served_from_cache = True
                stats.flag_issues.append("served_cache")
            else:
                stats.flag_issues.append("empty_out")

        # Mobile output sanitization
        if is_android or is_iphone:
            tmp = [android_sanitize_out_stream(s) for s in out]
            tmp = [s for s in tmp if isinstance(s, dict) and s.get("url")]
            out_for_client = tmp
        else:
            out_for_client = out

        payload: Dict[str, Any] = {"streams": out_for_client, "cacheMaxAge": int(CACHE_TTL)}

        if want_dbg:
            # Prefer remote timings, but fall back to wait timings when remote is unavailable.
            aio_local_ms = int(stats.ms_fetch_aio or 0)
            p2_local_ms  = int(stats.ms_fetch_p2 or 0)
            aio_join_ms  = int(stats.ms_join_aio or 0)
            p2_join_ms   = int(stats.ms_join_p2 or 0)
            aio_provider_ms = int(stats.ms_fetch_aio_remote or 0)
            p2_provider_ms  = int(stats.ms_fetch_p2_remote or 0)
            aio_http_ms = int((stats.fetch_aio or {}).get("http_ms") or 0)
            aio_read_ms = int((stats.fetch_aio or {}).get("read_ms") or 0)
            aio_json_ms = int((stats.fetch_aio or {}).get("json_ms") or 0)
            aio_post_ms = int((stats.fetch_aio or {}).get("post_ms") or 0)
            p2_http_ms  = int((stats.fetch_p2 or {}).get("http_ms") or 0)
            p2_read_ms  = int((stats.fetch_p2 or {}).get("read_ms") or 0)
            p2_json_ms  = int((stats.fetch_p2 or {}).get("json_ms") or 0)
            p2_post_ms  = int((stats.fetch_p2 or {}).get("post_ms") or 0)

            # What we "report" as timing_ms.* (used by smoke tests): provider internal when we have it, otherwise our local fetch time.
            aio_ms = aio_provider_ms if aio_provider_ms > 0 else aio_local_ms
            p2_ms  = p2_provider_ms  if p2_provider_ms  > 0 else p2_local_ms

            tmdb_ms = int(stats.ms_tmdb or 0)
            tb_api_ms = int(stats.ms_tb_api or 0)
            tb_wd_ms = int(stats.ms_tb_webdav or 0)
            tb_usenet_ms = int(stats.ms_tb_usenet or 0)
            title_mismatch_ms = int(stats.ms_title_mismatch or 0)
            uncached_check_ms = int(stats.ms_uncached_check or 0)

            payload["debug"] = {
                "rid": _rid(),
                "platform": platform,
                "ua_class": (getattr(stats, "client_class", "") or platform),
                "ua_tok": getattr(g, "_cached_ua_tok", ""),
                "ua_family": getattr(g, "_cached_ua_family", ""),
                "cache": ("hit" if served_from_cache else "miss"),
                "served_cache": bool(served_from_cache),
                "fetch": {"aio": stats.fetch_aio, "p2": stats.fetch_p2},
                "in": stats.counts_in,
                "out": stats.counts_out,
                                "timing_ms": {
                                    "aio": aio_ms,
                                    "p2": p2_ms,
                                    "tb_api": tb_api_ms,
                                    "tmdb": tmdb_ms,
                                    "aio_remote": aio_provider_ms,
                                    "p2_remote": p2_provider_ms,
                                    "aio_wait": aio_join_ms,
                                    "p2_wait": p2_join_ms,
                                    "aio_local": aio_local_ms,
                                    "p2_local": p2_local_ms,
                                    "aio_provider": aio_provider_ms,
                                    "p2_provider": p2_provider_ms,
                                    "aio_join": aio_join_ms,
                                    "p2_join": p2_join_ms,
                                    "aio_http": aio_http_ms,
                                    "aio_read": aio_read_ms,
                                    "aio_json": aio_json_ms,
                                    "aio_post": aio_post_ms,
                                    "p2_http": p2_http_ms,
                                    "p2_read": p2_read_ms,
                                    "p2_json": p2_json_ms,
                                    "p2_post": p2_post_ms,
                                    "tb_wd": tb_wd_ms,
                                    "tb_usenet": tb_usenet_ms,
                                    "title_mismatch": title_mismatch_ms,
                                    "uncached_check": uncached_check_ms,
                                    "py_ff": int(getattr(stats, "ms_py_ff", 0) or 0),
                                    "py_dedup": int(getattr(stats, "ms_py_dedup", 0) or 0),
                                    "py_wrap_emit": int(getattr(stats, "ms_py_wrap_emit", 0) or 0),
                                    "py_clean": int(getattr(stats, "ms_py_clean", 0) or 0),
                                    "py_sort": int(getattr(stats, "ms_py_sort", 0) or 0),
                                    "py_mix": int(getattr(stats, "ms_py_mix", 0) or 0),
                                    "py_tb_prep": int(getattr(stats, "ms_py_tb_prep", 0) or 0),
                                    "py_tb_mark_only": int(getattr(stats, "ms_py_tb_mark_only", 0) or 0),
                                    "py_ff_overhead": int(getattr(stats, "ms_py_ff_overhead", 0) or 0),
                                    "usenet_ready_match": int(getattr(stats, "ms_usenet_ready_match", 0) or 0),
                                    "usenet_probe": int(getattr(stats, "ms_usenet_probe", 0) or 0),
                                    "fetch_wall": int(getattr(stats, "ms_fetch_wall", 0) or 0),
                                    "py_pre_wrap": max(int(getattr(stats, "ms_py_ff", 0) or 0) - int(getattr(stats, "ms_py_wrap_emit", 0) or 0), 0),
                                    "overhead": int(getattr(stats, "ms_overhead", 0) or 0),
                                },
                                "remote_ms": {"aio": aio_provider_ms, "p2": p2_provider_ms},
                                "wait_ms": {"aio": aio_join_ms, "p2": p2_join_ms},
                                "local_ms": {"aio": aio_local_ms, "p2": p2_local_ms},
                                "fetch_breakdown_ms": {
                                    "aio": {"http": aio_http_ms, "read": aio_read_ms, "json": aio_json_ms, "post": aio_post_ms},
                                    "p2":  {"http": p2_http_ms,  "read": p2_read_ms,  "json": p2_json_ms,  "post": p2_post_ms},
                                },
                                "delivered": int(stats.delivered or 0),
                "drops": {
                    "error": int(stats.dropped_error or 0),
                    "missing_url": int(stats.dropped_missing_url or 0),
                    "pollution": int(stats.dropped_pollution or 0),
                    "title_mismatch": int(stats.dropped_title_mismatch or 0),
                    "dead_url": int(stats.dropped_dead_url or 0),
                    "uncached": int(stats.dropped_uncached or 0),
                    "uncached_tb": int(stats.dropped_uncached_tb or 0),
                    "android_magnets": int(stats.dropped_android_magnets or 0),
                    "iphone_magnets": int(stats.dropped_iphone_magnets or 0),
                },
                "errors": list(stats.error_reasons)[:8],
                "flags": list(stats.flag_issues)[:12],
            }

        payload.update(payload.get("debug") or {})  # flatten debug keys to top-level for dbg=1
        _debug_log_full_streams(type_, id_, platform, out_for_client)
        return jsonify(payload), 200

    except Exception as e:
        is_error = True
        logger.exception("STREAM_EXCEPTION rid=%s type=%s id=%s err=%s", _rid(), type_, id_, e)
        try:
            if len(stats.error_reasons) < 8:
                stats.error_reasons.append(f"stream:{type(e).__name__}")
        except Exception:
            pass

        cached = cache_get(cache_key)
        if cached:
            out = cached
            served_from_cache = True
        else:
            out = []

        if is_android or is_iphone:
            tmp = [android_sanitize_out_stream(s) for s in out]
            tmp = [s for s in tmp if isinstance(s, dict) and s.get("url")]
            out_for_client = tmp
        else:
            out_for_client = out

        payload: Dict[str, Any] = {"streams": out_for_client, "cacheMaxAge": int(CACHE_TTL)}
        if want_dbg:
            # Best-effort debug even when the main stream handler threw.
            try:
                if (not stats.fetch_aio) or (not stats.fetch_p2):
                    fm = locals().get("fetch_meta") or {}
                    stats.fetch_aio = stats.fetch_aio or _compact_fetch_meta((fm.get("aio") if isinstance(fm, dict) else {}) or {})
                    stats.fetch_p2 = stats.fetch_p2 or _compact_fetch_meta((fm.get("p2") if isinstance(fm, dict) else {}) or {})
            except Exception:
                pass
            try:
                if not stats.counts_in:
                    stats.counts_in = _summarize_streams_for_counts(locals().get("streams") or [])
            except Exception:
                pass
            try:
                out_sum = _summarize_streams_for_counts(out_for_client)
            except Exception:
                out_sum = {}
            payload["debug"] = {
                "rid": _rid(),
                "platform": platform,
                "ua_class": platform,
                "ua_tok": getattr(g, "_cached_ua_tok", ""),
                "ua_family": getattr(g, "_cached_ua_family", ""),
                "cache": ("hit" if served_from_cache else "miss"),
                "served_cache": bool(served_from_cache),
                "fetch": {"aio": stats.fetch_aio, "p2": stats.fetch_p2},
                "in": (stats.counts_in or {}),
                "out": out_sum,
                "timing_ms": {
                    "aio": int(stats.ms_fetch_aio or 0),
                    "p2": int(stats.ms_fetch_p2 or 0),
                    "tmdb": int(stats.ms_tmdb or 0),
                    "tb_api": int(stats.ms_tb_api or 0),
                    "tb_webdav": int(stats.ms_tb_webdav or 0),
                    "title": int(stats.ms_title_mismatch or 0),
                    "uncached": int(stats.ms_uncached_check or 0),
                },
                "delivered": int(len(out_for_client) if out_for_client is not None else 0),
                "drops": {
                    "error": int(stats.dropped_error or 0),
                    "missing_url": int(stats.dropped_missing_url or 0),
                    "pollution": int(stats.dropped_pollution or 0),
                    "title_mismatch": int(stats.dropped_title_mismatch or 0),
                    "dead_url": int(stats.dropped_dead_url or 0),
                    "uncached": int(stats.dropped_uncached or 0),
                    "uncached_tb": int(stats.dropped_uncached_tb or 0),
                    "android_magnets": int(stats.dropped_android_magnets or 0),
                    "iphone_magnets": int(stats.dropped_iphone_magnets or 0),
                },
                "errors": list(stats.error_reasons)[:8],
                "flags": list(stats.flag_issues)[:12],
            }
        _debug_log_full_streams(type_, id_, platform, out_for_client)
        return jsonify(payload), 200

    finally:
        try:
            stats.ms_fetch_wall = int(fetch_wall_ms or 0)
        except Exception:
            pass

        # Slow-phase flags (add late so ms_fetch_* is filled)
        try:
            if int(stats.ms_fetch_aio or 0) >= int(FLAG_SLOW_AIO_MS):
                stats.flag_issues.append(f"slow_aio:{int(stats.ms_fetch_aio)}ms")
            if int(stats.ms_fetch_p2 or 0) >= int(FLAG_SLOW_P2_MS):
                stats.flag_issues.append(f"slow_p2:{int(stats.ms_fetch_p2)}ms")
            if int(stats.ms_tb_api or 0) >= int(FLAG_SLOW_TB_API_MS):
                stats.flag_issues.append(f"slow_tb_api:{int(stats.ms_tb_api)}ms")
        except Exception:
            pass

        # De-dupe flags/errors (keep them short)
        try:
            stats.flag_issues = list(dict.fromkeys([str(x) for x in (stats.flag_issues or [])]))[:16]
        except Exception:
            pass
        try:
            stats.error_reasons = list(dict.fromkeys([str(x) for x in (stats.error_reasons or [])]))[:16]
        except Exception:
            pass

        ms_total = int((time.monotonic() - t0) * 1000)

        try:
            stats.ms_total = int(ms_total or 0)
        except Exception:
            pass
        try:
            _sum = 0
            _sum += int(getattr(stats, "ms_fetch_wall", 0) or 0)
            _sum += int(getattr(stats, "ms_py_ff", 0) or 0)
            # ms_overhead is “everything we didn't explicitly time”, outside fetch_wall + filter_and_format.
            stats.ms_overhead = max(0, int(ms_total) - int(_sum))
        except Exception:
            pass


        # Global stats update
        _update_global_stats(
            platform=stats.client_platform or platform,
            delivered=int(stats.delivered or 0),
            ms_total=ms_total,
            served_cache=bool(served_from_cache),
            is_error=bool(is_error),
            flags=list(stats.flag_issues) if isinstance(stats.flag_issues, list) else [],
        )

        # Flag platform-specific drops (magnets removed on mobile)
        try:
            if int(stats.dropped_platform_specific or 0) > 0 and isinstance(stats.flag_issues, list):
                _bd = f"android:{int(stats.dropped_android_magnets or 0)},iphone:{int(stats.dropped_iphone_magnets or 0)}"
                stats.flag_issues.append(f"platform_drops:{_bd}")
        except Exception:
            pass


        # Derived: py_pre_wrap_ms = py_ff_ms - py_wrap_emit_ms (kept for continuity with prior logs)
        py_pre_wrap_ms = 0
        try:
            py_pre_wrap_ms = max(0, int(getattr(stats, "ms_py_ff", 0) or 0) - int(getattr(stats, "ms_py_wrap_emit", 0) or 0))
        except Exception:
            py_pre_wrap_ms = 0

        logger.info(
                "WRAP_TIMING rid=%s total_ms=%s fetch_wall_ms=%s "
                "aio_local_ms=%s aio_join_ms=%s aio_http_ms=%s aio_read_ms=%s aio_json_ms=%s aio_post_ms=%s aio_prov_ms=%s "
                "p2_local_ms=%s p2_join_ms=%s p2_http_ms=%s p2_read_ms=%s p2_json_ms=%s p2_post_ms=%s p2_prov_ms=%s "
                "parallel_slack_ms=%s "
                "tmdb_ms=%s py_ff_ms=%s py_clean_ms=%s title_ms=%s py_sort_ms=%s py_mix_ms=%s "
                "py_tb_prep_ms=%s tb_api_ms=%s py_tb_mark_ms=%s "
                "tb_webdav_ms=%s tb_usenet_ms=%s usenet_ready_ms=%s usenet_probe_ms=%s "
                "py_dedup_ms=%s py_wrap_emit_ms=%s py_ff_overhead_ms=%s py_pre_wrap_ms=%s overhead_ms=%s",
                _rid(),
                int(ms_total),
                int(getattr(stats, "ms_fetch_wall", 0) or 0),
                int(getattr(stats, "ms_fetch_aio", 0) or 0),
                int(getattr(stats, "ms_join_aio", 0) or 0),
                int(((getattr(stats, "fetch_aio", {}) or {}).get("http_ms")) or 0),
                int(((getattr(stats, "fetch_aio", {}) or {}).get("read_ms")) or 0),
                int(((getattr(stats, "fetch_aio", {}) or {}).get("json_ms")) or 0),
                int(((getattr(stats, "fetch_aio", {}) or {}).get("post_ms")) or 0),
                int(getattr(stats, "ms_fetch_aio_remote", 0) or 0),
                int(getattr(stats, "ms_fetch_p2", 0) or 0),
                int(getattr(stats, "ms_join_p2", 0) or 0),
                int(((getattr(stats, "fetch_p2", {}) or {}).get("http_ms")) or 0),
                int(((getattr(stats, "fetch_p2", {}) or {}).get("read_ms")) or 0),
                int(((getattr(stats, "fetch_p2", {}) or {}).get("json_ms")) or 0),
                int(((getattr(stats, "fetch_p2", {}) or {}).get("post_ms")) or 0),
                int(getattr(stats, "ms_fetch_p2_remote", 0) or 0),
                max(0, int(getattr(stats, "ms_fetch_wall", 0) or 0) - max(int(getattr(stats, "ms_fetch_aio", 0) or 0), int(getattr(stats, "ms_fetch_p2", 0) or 0))),
                int(getattr(stats, "ms_tmdb", 0) or 0),
                int(getattr(stats, "ms_py_ff", 0) or 0),
                int(getattr(stats, "ms_py_clean", 0) or 0),
                int(getattr(stats, "ms_title_mismatch", 0) or 0),
                int(getattr(stats, "ms_py_sort", 0) or 0),
                int(getattr(stats, "ms_py_mix", 0) or 0),
                int(getattr(stats, "ms_py_tb_prep", 0) or 0),
                int(getattr(stats, "ms_tb_api", 0) or 0),
                int(getattr(stats, "ms_py_tb_mark_only", 0) or 0),
                int(getattr(stats, "ms_tb_webdav", 0) or 0),
                int(getattr(stats, "ms_tb_usenet", 0) or 0),
                int(getattr(stats, "ms_usenet_ready_match", 0) or 0),
                int(getattr(stats, "ms_usenet_probe", 0) or 0),
                int(getattr(stats, "ms_py_dedup", 0) or 0),
                int(getattr(stats, "ms_py_wrap_emit", 0) or 0),
                int(getattr(stats, "ms_py_ff_overhead", 0) or 0),
                int(py_pre_wrap_ms),
                int(getattr(stats, "ms_overhead", 0) or 0),
                )
        # New: Approximate output size (bytes) for debugging response bloat
        total_streams = 0
        out_size = 0
        try:
            total_streams = len(out_for_client) if out_for_client else 0
            out_size = len(json.dumps(out_for_client, separators=(",", ":"))) if out_for_client else 0
        except Exception:
            total_streams = 0
            out_size = 0

        logger.info(
            "WRAP_STATS rid=%s mark=%s build=%s git=%s path=%s client_platform=%s ua_tok=%s ua_family=%s type=%s id=%s aio_in=%s prov2_in=%s merged_in=%s dropped_error=%s dropped_missing_url=%s dropped_pollution=%s dropped_placeholder=%s dropped_low_seeders=%s dropped_lang=%s dropped_low_premium=%s dropped_rd=%s dropped_ad=%s dropped_low_res=%s dropped_old_age=%s dropped_blacklist=%s dropped_fakes_db=%s dropped_title_mismatch=%s dropped_dead_url=%s dropped_uncached=%s dropped_uncached_tb=%s dropped_android_magnets=%s dropped_iphone_magnets=%s dropped_platform_specific=%s dropped_magnet=%s deduped=%s dedup=%s delivered=%s out_bytes=%s cache_hit=%s cache_miss=%s cache_rate=%s platform=%s flags=%s errors=%s errors_timeout=%s errors_parse=%s errors_api=%s ms=%s total_streams=%s",
            _rid(), _mark(), BUILD, GIT_COMMIT, request.path, str(stats.client_platform or "unknown"),
            getattr(g, "_cached_ua_tok", ""),
            getattr(g, "_cached_ua_family", ""),
            type_, id_,
            int(stats.aio_in or 0), int(stats.prov2_in or 0), int(stats.merged_in or 0),
            int(stats.dropped_error or 0), int(stats.dropped_missing_url or 0),
            int(stats.dropped_pollution or 0),  # legacy name
            int(stats.dropped_pollution or 0),  # alias: dropped_placeholder
            int(stats.dropped_low_seeders or 0), int(stats.dropped_lang or 0), int(stats.dropped_low_premium or 0),
            int(stats.dropped_rd or 0), int(stats.dropped_ad or 0),
            int(stats.dropped_low_res or 0), int(stats.dropped_old_age or 0),
            int(stats.dropped_blacklist or 0), int(stats.dropped_fakes_db or 0),
            int(stats.dropped_title_mismatch or 0), int(stats.dropped_dead_url or 0), int(stats.dropped_uncached or 0),
            int(stats.dropped_uncached_tb or 0),
            int(stats.dropped_android_magnets or 0), int(stats.dropped_iphone_magnets or 0),
            int(stats.dropped_platform_specific or 0),  # legacy name
            int(stats.dropped_platform_specific or 0),  # alias: dropped_magnet
            int(stats.deduped or 0),  # legacy name
            int(stats.deduped or 0),  # alias: dedup
            int(stats.delivered or 0), int(out_size or 0),
            int(stats.cache_hit or 0), int(stats.cache_miss or 0), float(stats.cache_rate or 0.0),
            str(stats.client_platform or ""),
            ",".join(list(stats.flag_issues)[:8]) if isinstance(stats.flag_issues, list) else "",
            ",".join(list(stats.error_reasons)[:6]) if isinstance(stats.error_reasons, list) else "",
            int(stats.errors_timeout or 0), int(stats.errors_parse or 0), int(stats.errors_api or 0),
            ms_total,
            int(total_streams or 0),
        )
# NEW: Flag slow fetches with high seeders (diagnose buffering despite peers)
        try:
            ms_fetch_aio = int(getattr(stats, "ms_fetch_aio", 0) or 0)
            if ms_fetch_aio > 5000 and any(int(s.get("seeders", 0) or 0) > 50 for s in (out_for_client or [])):
                if isinstance(stats.flag_issues, list):
                    stats.flag_issues.append("slow_high_seeders")
                logger.info(
                    "FLAG_SLOW_HIGH_SEEDERS rid=%s ms_fetch_aio=%s high_seeders_delivered=%s",
                    _rid(),
                    ms_fetch_aio,
                    sum(1 for s in (out_for_client or []) if int(s.get("seeders", 0) or 0) > 50),
                )
        except Exception:
            pass
        if WRAP_LOG_COUNTS:
            try:
                logger.info(
                    "WRAP_COUNTS rid=%s mark=%s build=%s git=%s path=%s type=%s id=%s fetch_aio=%s fetch_p2=%s in=%s out=%s",
                    _rid(), _mark(), BUILD, GIT_COMMIT, request.path,
                    type_, id_,
                    json.dumps(stats.fetch_aio, separators=(",", ":"), sort_keys=True),
                    json.dumps(stats.fetch_p2, separators=(",", ":"), sort_keys=True),
                    json.dumps(stats.counts_in, separators=(",", ":"), sort_keys=True),
                    json.dumps(stats.counts_out, separators=(",", ":"), sort_keys=True),
                )

            except Exception:
                pass


@app.errorhandler(Exception)
def handle_unhandled_exception(e):
    # Pass through HTTP errors (404/405/etc) so they don't become fake 500s in logs.
    if isinstance(e, HTTPException):
        return e

    # Last-resort safety net so Stremio doesn't get HTML 500s (which break jq/tests).
    logger.exception("UNHANDLED %s %s: %s", request.method, request.path, e)
    try:
        if hasattr(g, 'stats') and isinstance(g.stats, PipeStats):
            logger.info(
                "UNHANDLED_STATS rid=%s errors=%s flags=%s",
                _rid(),
                ",".join(list(g.stats.error_reasons)[:3]) if isinstance(g.stats.error_reasons, list) else "",
                ",".join(list(g.stats.flag_issues)[:3]) if isinstance(g.stats.flag_issues, list) else "",
            )
    except Exception:
        pass

    try:
        if "timeout" in str(e).lower():
            logger.warning("EXEC_ISSUE rid=%s: timeout in %s", _rid(), request.path)
    except Exception:
        pass

    if request.path.endswith("/manifest.json"):
        return manifest()

    if request.path.startswith("/stream/"):
        # Never fail hard for stream endpoints; empty list is better than a 500.
        try:
            parts = request.path.split("/")
            type_ = parts[2] if len(parts) > 2 else ""
            id_part = parts[3] if len(parts) > 3 else ""
            stremio_id = id_part[:-5] if id_part.endswith(".json") else id_part
            cache_key = f"{type_}:{stremio_id}" if type_ and stremio_id else ""
        except Exception:
            type_ = ""
            stremio_id = ""
            cache_key = ""

        cached = cache_get(cache_key) if cache_key else None

        is_android = is_android_client()
        is_iphone = is_iphone_client()
        platform = client_platform(is_android, is_iphone)

        out = cached or []
        if is_android or is_iphone:
            tmp = [android_sanitize_out_stream(s) for s in out]
            tmp = [s for s in tmp if isinstance(s, dict) and s.get("url")]
            out_for_client = tmp
        else:
            out_for_client = out

        dbg_q = request.args.get("debug") or request.args.get("dbg") or ""
        want_dbg = WRAP_EMBED_DEBUG or (isinstance(dbg_q, str) and dbg_q.strip() not in ("", "0", "false", "False"))

        payload: Dict[str, Any] = {"streams": out_for_client, "cacheMaxAge": int(CACHE_TTL)}
        if want_dbg:
            try:
                csum = _summarize_streams_for_counts(out_for_client)
            except Exception:
                csum = {}
            payload["debug"] = {
                "rid": _rid(),
                "platform": platform,
                "cache": ("hit" if bool(cached) else "miss"),
                "served_cache": bool(cached),
                "fetch": {
                    "aio": {"ok": False, "err": "unhandled"},
                    "p2": {"ok": False, "err": "unhandled"},
                },
                "in": csum,
                "out": csum,
                "timing_ms": {},
                "delivered": int(len(out_for_client) if out_for_client is not None else 0),
                "errors": [f"unhandled:{type(e).__name__}"],
                "flags": ["unhandled_exception"],
            }
        _debug_log_full_streams(type_ or "", stremio_id or "", platform, out_for_client)
        return jsonify(payload), 200

    return ("Internal Server Error", 500)

if __name__ == "__main__":
    port = _safe_int(os.environ.get('PORT', '5000'), 5000)
    app.run(host="0.0.0.0", port=port, debug=LOG_LEVEL == logging.DEBUG, use_reloader=False)

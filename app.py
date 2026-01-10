from flask import Flask, request, jsonify, g, has_request_context
import requests
import os
import logging
import re
import json
import unicodedata
import time
import uuid
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from logging.handlers import RotatingFileHandler

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, resources={r"/*": {"origins": "*"}})

@app.get("/health")
def health():
    return jsonify(ok=True, ts=int(time.time())), 200


# ---------------------------
# Logging helpers
# ---------------------------
def _coerce_log_level(value: str) -> int:
    """
    Accepts: debug/DEBUG/info/INFO/warn/WARNING/error/ERROR, etc. or numeric strings.
    Returns python logging level int. Never throws.
    """
    if value is None:
        return logging.DEBUG

    v = str(value).strip()
    if not v:
        return logging.DEBUG

    # numeric?
    if v.isdigit():
        try:
            return int(v)
        except Exception:
            return logging.DEBUG

    v = v.upper()
    if v == "WARN":
        v = "WARNING"
    if v == "FATAL":
        v = "CRITICAL"

    return logging._nameToLevel.get(v, logging.DEBUG)


class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Avoid crashing when no request context (startup logs, background threads, etc.)
        if has_request_context():
            rid = getattr(g, "rid", "-")
        else:
            rid = "-"
        record.rid = rid
        return True


LOG_LEVEL_ENV = os.environ.get("LOG_LEVEL", "INFO")
LOG_LEVEL = _coerce_log_level(LOG_LEVEL_ENV)

# Console logging (Render captures stdout)
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(rid)s | %(message)s"
)

root_logger = logging.getLogger()
root_logger.addFilter(RequestIdFilter())

# Optional local rotating file (Render ephemeral, but harmless)
file_handler = RotatingFileHandler("app.log", maxBytes=200000, backupCount=3)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(rid)s | %(message)s"))
file_handler.addFilter(RequestIdFilter())
root_logger.addHandler(file_handler)


# ---------------------------
# HTTP session w/ retry
# ---------------------------
session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


# ---------------------------
# Config / ENV (env-first)
# ---------------------------
AIO_BASE = os.environ.get(
    "AIO_URL",
    "https://buubuu99-aiostreams.elfhosted.cc/stremio/acc199cb-6b12-4fa9-be4e-a8ff4c13fa50/REPLACE_ME"
)

STORE_BASE = os.environ.get(
    "STORE_URL",
    "https://buubuu99-stremthru.elfhosted.cc/stremio/store/REPLACE_ME"
)

USE_STORE = os.environ.get("USE_STORE", "false").lower() == "true"

MIN_SEEDERS = int(os.environ.get("MIN_SEEDERS", 10))
MIN_SIZE_BYTES = int(os.environ.get("MIN_SIZE_BYTES", 2000000000))  # ~2GB min
MAX_SIZE_BYTES = int(os.environ.get("MAX_SIZE_BYTES", 100000000000))
REQUEST_TIMEOUT = int(os.environ.get("TIMEOUT", 60000)) / 1000

MAX_UNCACHED_KEEP = int(os.environ.get("MAX_UNCACHED_KEEP", 5))

PING_TIMEOUT = 2
API_POLL_TIMEOUT = 10

# API keys (env-first)
RD_API_KEY = os.environ.get("RD_API_KEY", "")
TB_API_KEY = os.environ.get("TB_API_KEY", "")
AD_API_KEY = os.environ.get("AD_API_KEY", "")

LANGUAGE_FLAGS = {
    "eng": "🇬🇧", "en": "🇬🇧", "jpn": "🇯🇵", "jp": "🇯🇵", "ita": "🇮🇹", "it": "🇮🇹",
    "fra": "🇫🇷", "fr": "🇫🇷", "kor": "🇰🇷", "kr": "🇰🇷", "chn": "🇨🇳", "cn": "🇨🇳",
    "uk": "🇬🇧", "ger": "🇩🇪", "de": "🇩🇪", "hun": "🇭🇺", "yes": "📝", "ko": "🇰🇷",
    "rus": "🇷🇺", "hin": "🇮🇳", "multi": "🌍"
}
LANGUAGE_TEXT_FALLBACK = {
    "eng": "[GB]", "en": "[GB]", "jpn": "[JP]", "jp": "[JP]", "ita": "[IT]", "it": "[IT]",
    "fra": "[FR]", "fr": "[FR]", "kor": "[KR]", "kr": "[KR]", "chn": "[CN]", "cn": "[CN]",
    "uk": "[GB]", "ger": "[DE]", "de": "[DE]", "hun": "[HU]", "yes": "[SUB]", "ko": "[KR]",
    "rus": "[RU]", "hin": "[IN]", "multi": "[MULTI]"
}
SERVICE_COLORS = {
    "rd": "[red]", "realdebrid": "[red]",
    "tb": "[blue]", "torbox": "[blue]",
    "ad": "[green]", "alldebrid": "[green]",
    "store": "[purple]", "stremthru": "[purple]"
}


# ---------------------------
# Request logging (RID + latency)
# ---------------------------
@app.before_request
def log_request():
    g.rid = uuid.uuid4().hex[:10]
    g.t0 = time.time()
    logging.info(f"Incoming request: {request.method} {request.path} from {request.remote_addr}")

@app.after_request
def log_response(resp):
    try:
        dt = (time.time() - getattr(g, "t0", time.time())) * 1000
        logging.info(f"Response: {resp.status_code} {request.path} {dt:.2f}ms")
    except Exception:
        pass
    return resp


# ---------------------------
# Debrid cache check
# ---------------------------
def debrid_check_cache(url, service="rd"):
    hash_match = re.search(r"(?:magnet:\?xt=urn:btih:)?([a-fA-F0-9]{40})", url, re.I)
    if not hash_match:
        logging.debug(f"No torrent hash found in URL: {url}")
        return False

    torrent_hash = hash_match.group(1).lower()

    if service == "rd":
        api_url = f"https://api.real-debrid.com/rest/1.0/torrents/instantAvailability/{torrent_hash}"
        headers = {"Authorization": f"Bearer {RD_API_KEY}"} if RD_API_KEY else {}
    elif service == "tb":
        api_url = f"https://api.torbox.app/v1/api/torrents/checkcached?hash={torrent_hash}&list_files=true"
        headers = {"Authorization": f"Bearer {TB_API_KEY}"} if TB_API_KEY else {}
    elif service == "ad":
        api_url = (
            "https://api.alldebrid.com/v4/magnet/instant"
            f"?agent=StremioWrapper&version=1.0&magnet={requests.utils.quote(url)}"
        )
        params = {"apikey": AD_API_KEY} if AD_API_KEY else {}
        try:
            response = session.get(api_url, params=params, timeout=REQUEST_TIMEOUT)
            logging.debug(f"AD API response: status={response.status_code}, content={response.text[:200]}...")
            response.raise_for_status()
            data = response.json()
            return data.get("data", {}).get("magnets", {}).get("instant", False)
        except Exception as e:
            logging.exception(f"AD cache check failed for URL {url}: {e}")
            return False
    else:
        return False

    try:
        response = session.get(api_url, headers=headers, timeout=REQUEST_TIMEOUT)
        logging.debug(f"{service.upper()} API response: status={response.status_code}, content={response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        if service == "rd":
            return bool(data.get(torrent_hash, {}).get("rd"))
        elif service == "tb":
            return data.get("data", {}).get("is_cached", False)
    except Exception as e:
        logging.exception(f"{service.upper()} cache check failed for URL {url}: {e}")

    return False


# ---------------------------
# Fetch streams (returns streams + per-backend counts)
# ---------------------------
def get_streams(type_, id_):
    aio_url = f"{AIO_BASE}/stream/{type_}/{id_}.json"
    store_url = f"{STORE_BASE}/stream/{type_}/{id_}.json" if USE_STORE else None

    streams = []
    aio_count = 0
    store_count = 0

    start = time.time()
    try:
        response = session.get(aio_url, timeout=REQUEST_TIMEOUT)
        logging.debug(f"AIO response: status={response.status_code}, content={response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        aio_streams = data.get("streams", [])
        aio_count = len(aio_streams)
        streams.extend(aio_streams)
        logging.info(f"Fetched {aio_count} streams from AIO in {time.time() - start:.2f}s")
        if aio_count == 0:
            logging.warning("No streams fetched from AIO - check URL/network/AIO service status")
    except Exception as e:
        logging.exception(f"AIO fetch failed: {e}")

    if USE_STORE and store_url:
        start_store = time.time()
        try:
            response_store = session.get(store_url, timeout=REQUEST_TIMEOUT)
            logging.debug(f"Store response: status={response_store.status_code}, content={response_store.text[:200]}...")
            response_store.raise_for_status()
            data_store = response_store.json()
            store_streams = data_store.get("streams", [])
            store_count = len(store_streams)
            streams.extend(store_streams)
            logging.info(f"Fetched {store_count} streams from Store in {time.time() - start_store:.2f}s")
            if store_count == 0:
                logging.warning("No streams fetched from Store - check URL/network/Store status")
        except Exception as e:
            logging.exception(f"Store fetch failed: {e}")

    logging.info(f"Raw streams count from all backends: {len(streams)}")
    return streams, aio_count, store_count


@app.route("/manifest.json")
def manifest():
    manifest_data = {
        "id": "org.grok.wrapper",
        "version": "1.0.46",
        "name": "AIO Wrapper",
        "description": "Wraps AIOStreams to filter and format streams (Store optional) - Enhanced Logging Edition",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "catalogs": [],
        "idPrefixes": ["tt"]
    }
    return jsonify(manifest_data)


# ---------------------------
# Stream endpoint w/ FINAL STATS
# ---------------------------
@app.route("/stream/<type_>/<id_>.json")
def stream(type_, id_):
    logging.info(f"Stream request: {type_}/{id_}")

    # Final summary counters (what you want to compare)
    stats = {
        "aio_in": 0,       # AIOStreams returned count (already after AIO dedupe from your perspective)
        "raw_in": 0,       # total from all backends (aio + store if enabled)
        "post_filter": 0,  # after wrapper filters
        "delivered": 0     # returned to Stremio (after top cap)
    }

    streams, aio_count, store_count = get_streams(type_, id_)
    stats["aio_in"] = aio_count
    stats["raw_in"] = len(streams)

    filtered = []
    uncached_count = 0

    for i, s in enumerate(streams):
        try:
            parse_string = unicodedata.normalize(
                "NFKD",
                (s.get("name", "") + " " + s.get("title", "") + " " + s.get("infoHash", ""))
            ).lower()

            size_match = re.search(r"(\d+(?:\.\d+)?)\s*(gb|mb)", parse_string, re.I)
            if size_match:
                size_gb = float(size_match.group(1)) if size_match.group(2).lower() == "gb" else float(size_match.group(1)) / 1024
                size_bytes = size_gb * 1073741824
            else:
                size_bytes = 0

            if size_bytes and (size_bytes < MIN_SIZE_BYTES or size_bytes > MAX_SIZE_BYTES):
                continue

            seeder_match = re.search(r"(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)", parse_string, re.I | re.U)
            seeders = int(seeder_match.group(1) or seeder_match.group(2)) if seeder_match else 0
            if seeders < MIN_SEEDERS:
                continue

            hints = {}
            service = next((k for k in SERVICE_COLORS if k in parse_string), None)
            if service:
                hints["service"] = service
                url = s.get("url", "") or s.get("behaviorHints", {}).get("proxyUrl", "")
                if url:
                    is_cached = debrid_check_cache(url, service)
                    hints["isCached"] = is_cached
                    if not is_cached:
                        uncached_count += 1
                        if uncached_count > MAX_UNCACHED_KEEP:
                            continue

            s["hints"] = hints
            filtered.append(s)

        except Exception:
            logging.exception(f"Exception while processing stream[{i}] (keeping it OUT)")
            continue

    stats["post_filter"] = len(filtered)
    logging.info(f"Post-filter count: {stats['post_filter']} (from {stats['raw_in']} raw)")

    # Sorting
    res_order = {"4k": 5, "2160p": 5, "1440p": 4, "1080p": 3, "720p": 2, "480p": 1, "sd": 0}

    def sort_key(s):
        parse = unicodedata.normalize("NFKD", (s.get("name", "") + " " + s.get("title", ""))).lower()
        res_match = re.search(r"(4k|2160p|1440p|1080p|720p|480p|sd)", parse, re.I)
        res_score = res_order.get(res_match.group(1).lower() if res_match else "sd", 0)

        size_match = re.search(r"(\d+(?:\.\d+)?)\s*(gb|mb)", parse, re.I)
        size_gb = float(size_match.group(1)) if size_match and size_match.group(2).lower() == "gb" else (float(size_match.group(1)) / 1024 if size_match else 0)

        seeder_match = re.search(r"(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)", parse, re.I | re.U)
        seeders = int(seeder_match.group(1) or seeder_match.group(2)) if seeder_match else 0

        is_cached = s.get("hints", {}).get("isCached", False)
        return (-int(is_cached), -seeders, -size_gb, -res_score)

    filtered.sort(key=sort_key)

    # Keep formatting (unchanged behavior)
    for s in filtered:
        parse_string = unicodedata.normalize("NFKD", (s.get("name", "") + " " + s.get("title", "") + " " + s.get("infoHash", ""))).lower()
        hints = s.get("hints", {})
        service = hints.get("service", "")
        color = SERVICE_COLORS.get(service, "")
        name = f"{color} " if color else ""

        res_match = re.search(r"(4k|2160p|1440p|1080p|720p|480p|sd)", parse_string, re.I)
        if res_match:
            name += f"📺 {res_match.group(1).upper()}"

        size_match = re.search(r"(\d+(?:\.\d+)?)\s*(gb|mb)", parse_string, re.I)
        if size_match:
            name += f" 💿 {size_match.group(1)} {size_match.group(2).upper()}"

        seeder_match = re.search(r"(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)", parse_string, re.I | re.U)
        if seeder_match:
            seeders = seeder_match.group(1) or seeder_match.group(2)
            name += f" 👥 {seeders}"

        lang_match = re.search(r"([a-z]{2,3}(?:[ ·,·-]*[a-z]{2,3})*)", parse_string, re.I | re.U)
        if lang_match:
            langs = re.findall(r"[a-z]{2,3}", lang_match.group(1), re.I)
            flags_added = [LANGUAGE_FLAGS[lang] for lang in langs if lang in LANGUAGE_FLAGS]
            if flags_added:
                name += " " + " ".join(sorted(set(flags_added)))

        audio_match = re.search(r"(dd\+|dd|dts-hd|dts|opus|aac|atmos|ma|5\.1|7\.1|2\.0|h\.26[4-5]|hev|dv|hdr)", parse_string, re.I | re.U)
        channel_match = re.search(r"(\d\.\d)", parse_string, re.I | re.U)
        if audio_match:
            audio = audio_match.group(1).upper()
            channels = channel_match.group(1) if channel_match else ""
            name += f" ♬ {audio} {channels}".strip()

        if "store" in parse_string or "4k" in parse_string or "stremthru" in parse_string:
            name = f"★ {name}"

        if "⏳" in name or not hints.get("isCached", False):
            s["name"] = f"[dim]{name} (Unverified ⏳)[/dim]"
        else:
            s["name"] = name

    final_streams = filtered[:60]
    stats["delivered"] = len(final_streams)

    # ONE clean line per request for comparison/grep
    logging.info("WRAP_FINAL_STATS " + json.dumps(stats, ensure_ascii=False, separators=(",", ":")))

    return jsonify({"streams": final_streams})


if __name__ == "__main__":
    try:
        port = int(os.environ.get("PORT", 5000))
        app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
    except Exception as e:
        logging.exception(f"App startup error: {e}")

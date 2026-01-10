from flask import Flask, request, jsonify, has_request_context
import requests
import os
import logging
import re
import json
import unicodedata
import time
import uuid
from collections import Counter
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from logging.handlers import RotatingFileHandler

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, resources={r"/*": {"origins": "*"}})

@app.get("/health")
def health():
    # Tiny, fast response for uptime checks
    return jsonify(ok=True, ts=int(time.time())), 200


# ==========================================================
# IMPROVED LOGGING ONLY (NO BEHAVIOR CUTS)
# - Adds RID per request so you can trace one stream call
# - Adds errors.log (ERROR-only) so you never miss entry errors
# - Adds drop-reason summaries in /stream endpoint
# - Adds try/except around per-stream processing + formatting,
#   logging payload snippets on errors
# ==========================================================

class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if has_request_context():
                record.rid = request.environ.get("RID", "-")
            else:
                record.rid = "-"
        except Exception:
            record.rid = "-"
        return True

# Attach filter BEFORE handlers so %(rid)s is always present
root_logger = logging.getLogger()
root_logger.addFilter(RequestIdFilter())

# Enhanced logging: Console + rotating file (+ errors.log)
# NOTE: we keep your style, just add RID and bigger log size
logging.basicConfig(
    level=os.environ.get('LOG_LEVEL', 'DEBUG'),
    format='%(asctime)s | %(levelname)s | %(rid)s | %(message)s'
)

# Keep app.log but make it actually usable (was 10KB, too small)
file_handler = RotatingFileHandler('app.log', maxBytes=5_000_000, backupCount=5)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(rid)s | %(message)s'))
root_logger.addHandler(file_handler)

# NEW: errors.log (ERROR only, with payload snippets)
error_handler = RotatingFileHandler('errors.log', maxBytes=5_000_000, backupCount=10)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(rid)s | %(message)s'))
root_logger.addHandler(error_handler)


session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

AIO_BASE = os.environ.get('AIO_URL', 'https://buubuu99-aiostreams.elfhosted.cc/stremio/acc199cb-6b12-4fa9-be4e-a8ff4c13fa50/eyJpIjoiRTJES0N1ZFBaWm8wb25aS05tNEFsUT09IiwiZSI6InhrVVFtdTFEWm5lcGVrcEh5VUZaejZlcEJLMEMrcXdLakY4UU9zUDJoOFE9IiwidCI6ImEifQ')
STORE_BASE = os.environ.get('STORE_URL', 'https://buubuu99-stremthru.elfhosted.cc/stremio/store/eyJzdG9yZV9uYW1lIjoiIiwic3RvcmVfdG9rZW4iOiJZblYxWW5WMU9UazZUV0Z5YVhOellUazVRREV4Tnc9PSIsImhpZGVfY2F0YWxvZyI6dHJ1ZSwid2ViZGwiOnRydWV9')
USE_STORE = os.environ.get('USE_STORE', 'false').lower() == 'true'

MIN_SEEDERS = int(os.environ.get('MIN_SEEDERS', 10))  # Updated to 10 for stricter filtering
MIN_SIZE_BYTES = int(os.environ.get('MIN_SIZE_BYTES', 2000000000))  # ~2GB min
MAX_SIZE_BYTES = int(os.environ.get('MAX_SIZE_BYTES', 100000000000))
REQUEST_TIMEOUT = int(os.environ.get('TIMEOUT', 60000)) / 1000
MAX_UNCACHED_KEEP = 5
PING_TIMEOUT = 2
API_POLL_TIMEOUT = 10
RD_API_KEY = 'Z4C3UT777IK2U6EUZ5HLVVMO7BYQPDNEOJUGAFLUBXMGTSX2Z6RA'
TB_API_KEY = '1046c773-9d0d-4d53-95ab-8976a559a5f6'
AD_API_KEY = 'ZXzHvUmLsuuRmgyHg5zjFsk8'

LANGUAGE_FLAGS = {
    'eng': '🇬🇧', 'en': '🇬🇧', 'jpn': '🇯🇵', 'jp': '🇯🇵', 'ita': '🇮🇹', 'it': '🇮🇹',
    'fra': '🇫🇷', 'fr': '🇫🇷', 'kor': '🇰🇷', 'kr': '🇰🇷', 'chn': '🇨🇳', 'cn': '🇨🇳',
    'uk': '🇬🇧', 'ger': '🇩🇪', 'de': '🇩🇪', 'hun': '🇭🇺', 'yes': '📝', 'ko': '🇰🇷',
    'rus': '🇷🇺', 'hin': '🇮🇳', 'multi': '🌍'  # Added for common langs in screenshots
}
LANGUAGE_TEXT_FALLBACK = {
    'eng': '[GB]', 'en': '[GB]', 'jpn': '[JP]', 'jp': '[JP]', 'ita': '[IT]', 'it': '[IT]',
    'fra': '[FR]', 'fr': '[FR]', 'kor': '[KR]', 'kr': '[KR]', 'chn': '[CN]', 'cn': '[CN]',
    'uk': '[GB]', 'ger': '[DE]', 'de': '[DE]', 'hun': '[HU]', 'yes': '[SUB]', 'ko': '[KR]',
    'rus': '[RU]', 'hin': '[IN]', 'multi': '[MULTI]'
}
SERVICE_COLORS = {
    'rd': '[red]', 'realdebrid': '[red]',
    'tb': '[blue]', 'torbox': '[blue]',
    'ad': '[green]', 'alldebrid': '[green]',
    'store': '[purple]', 'stremthru': '[purple]'
}

@app.before_request
def log_request():
    # NEW: RID per request (short)
    if "RID" not in request.environ:
        request.environ["RID"] = str(uuid.uuid4())[:8]
    logging.info(f"Incoming request: {request.method} {request.url} from {request.remote_addr}")

def debrid_check_cache(url, service='rd'):
    hash_match = re.search(r'(?:magnet:\?xt=urn:btih:)?([a-fA-F0-9]{40})', url, re.I)
    if not hash_match:
        logging.debug(f"No torrent hash found in URL: {url}")
        return False
    torrent_hash = hash_match.group(1).lower()
    if service == 'rd':
        api_url = f"https://api.real-debrid.com/rest/1.0/torrents/instantAvailability/{torrent_hash}"
        headers = {'Authorization': f'Bearer {RD_API_KEY}'}
    elif service == 'tb':
        api_url = f"https://api.torbox.app/v1/api/torrents/checkcached?hash={torrent_hash}&list_files=true"
        headers = {'Authorization': f'Bearer {TB_API_KEY}'}
    elif service == 'ad':
        api_url = f"https://api.alldebrid.com/v4/magnet/instant?agent=StremioWrapper&version=1.0&magnet={requests.utils.quote(url)}"
        params = {'apikey': AD_API_KEY}
        try:
            response = session.get(api_url, params=params, timeout=REQUEST_TIMEOUT)
            logging.debug(f"AD API response: status={response.status_code}, content={response.text[:200]}...")
            response.raise_for_status()
            data = response.json()
            return data.get('data', {}).get('magnets', {}).get('instant', False)
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
        if service == 'rd':
            return bool(data.get(torrent_hash, {}).get('rd'))
        elif service == 'tb':
            return data.get('data', {}).get('is_cached', False)
    except Exception as e:
        logging.exception(f"{service.upper()} cache check failed for URL {url}: {e}")
        return False

def get_streams(type_, id_):
    aio_url = f"{AIO_BASE}/stream/{type_}/{id_}.json"
    store_url = f"{STORE_BASE}/stream/{type_}/{id_}.json" if USE_STORE else None
    streams = []
    start = time.time()
    try:
        response = session.get(aio_url, timeout=REQUEST_TIMEOUT)
        logging.debug(f"AIO response: status={response.status_code}, content={response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        aio_streams = data.get('streams', [])
        streams.extend(aio_streams)
        logging.info(f"Fetched {len(aio_streams)} streams from AIO in {time.time() - start:.2f}s")
        if len(aio_streams) == 0:
            logging.warning("No streams fetched from AIO - check URL, network, or AIO service status")
    except Exception as e:
        logging.exception(f"AIO fetch failed: {e}")
    if USE_STORE:
        start_store = time.time()
        try:
            response_store = session.get(store_url, timeout=REQUEST_TIMEOUT)
            logging.debug(f"Store response: status={response_store.status_code}, content={response_store.text[:200]}...")
            response_store.raise_for_status()
            data_store = response_store.json()
            store_streams = data_store.get('streams', [])
            streams.extend(store_streams)
            logging.info(f"Fetched {len(store_streams)} streams from Store in {time.time() - start_store:.2f}s")
            if len(store_streams) == 0:
                logging.warning("No streams fetched from Store - check URL, network, or Store service status")
        except Exception as e:
            logging.exception(f"Store fetch failed: {e}")
    total_raw = len(streams)
    logging.info(f"Raw streams count from all backends: {total_raw}")
    if total_raw > 0:
        logging.debug(f"Sample raw streams (first 3): {json.dumps(streams[:3], indent=2)}")
    elif total_raw == 0:
        logging.warning("Zero raw streams fetched overall - potential issue with backends or query")
    return streams

@app.route('/manifest.json')
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

@app.route('/stream/<type_>/<id_>.json')
def stream(type_, id_):
    logging.info(f"Stream request: {type_}/{id_}")

    streams = get_streams(type_, id_)
    raw_count = len(streams)
    logging.debug(f"Raw streams fetched: {raw_count}")

    filtered = []
    uncached_count = 0

    # NEW: per-request counters (no behavior change)
    drop_counts = Counter()
    stream_errors = 0

    for i, s in enumerate(streams):
        try:
            parse_string = unicodedata.normalize('NFKD', s.get('name', '') + ' ' + s.get('title', '') + ' ' + s.get('infoHash', '')).lower()
            logging.debug(f"Parsing stream {i}: {parse_string[:100]}...")

            size_match = re.search(r'(\d+(?:\.\d+)?)\s*(gb|mb)', parse_string, re.I)
            if size_match:
                size_gb = float(size_match.group(1)) if size_match.group(2).lower() == 'gb' else float(size_match.group(1)) / 1024
                size_bytes = size_gb * 1073741824
                logging.debug(f"Size match for stream {i}: {size_gb} GB ({size_bytes} bytes)")
            else:
                size_bytes = 0
                drop_counts["missing_size_match"] += 1
                logging.debug(f"No size match for stream {i}: pattern=r'(\\d+(?:\\.\\d+)?)\\s*(gb|mb)', parse_string={parse_string[:100]}...")

            if size_bytes and (size_bytes < MIN_SIZE_BYTES or size_bytes > MAX_SIZE_BYTES):
                drop_counts["size_out_of_range"] += 1
                logging.debug(f"Filtered out stream {i}: size {size_bytes} bytes out of range [{MIN_SIZE_BYTES}, {MAX_SIZE_BYTES}]")
                continue

            seeder_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', parse_string, re.I | re.U)
            seeders = int(seeder_match.group(1) or seeder_match.group(2)) if seeder_match else 0
            if not seeder_match:
                drop_counts["missing_seeders_match"] += 1

            if seeders < MIN_SEEDERS:
                drop_counts["seeders_below_min"] += 1
                logging.debug(f"Filtered out stream {i}: seeders {seeders} < {MIN_SEEDERS}")
                continue

            hints = {}
            service = next((k for k in SERVICE_COLORS if k in parse_string), None)
            if not service:
                drop_counts["no_service_detected"] += 1

            if service:
                hints['service'] = service
                url = s.get('url', '') or s.get('behaviorHints', {}).get('proxyUrl', '')
                if not url:
                    drop_counts["missing_url_for_cache"] += 1
                if url:
                    is_cached = debrid_check_cache(url, service)
                    hints['isCached'] = is_cached
                    if not is_cached:
                        drop_counts["uncached"] += 1
                        uncached_count += 1
                        if uncached_count > MAX_UNCACHED_KEEP:
                            drop_counts["uncached_limit_exceeded"] += 1
                            logging.debug(f"Filtered out uncached stream {i}: exceeded max uncached keep {MAX_UNCACHED_KEEP}")
                            continue

            filtered.append(s)
            s['hints'] = hints
            logging.debug(f"Kept stream {i}: seeders={seeders}, size={size_bytes}, cached={hints.get('isCached', 'N/A')}")

        except Exception as e:
            stream_errors += 1
            drop_counts["stream_processing_error"] += 1
            logging.exception(
                f"STREAM_PROCESSING_ERROR idx={i} err={e} "
                f"payload_snip={json.dumps(s, ensure_ascii=False)[:2000]}"
            )
            continue

    filtered_count = len(filtered)
    logging.info(f"Post-filter count: {filtered_count} (from {raw_count} raw)")
    logging.info(f"FILTER_DROP_SUMMARY top15={drop_counts.most_common(15)} stream_errors={stream_errors}")

    if filtered_count == 0 and raw_count > 0:
        logging.warning(f"Zero streams after filtering - check filters (MIN_SEEDERS={MIN_SEEDERS}, MIN_SIZE={MIN_SIZE_BYTES}) or stream quality")

    res_order = {'4k': 5, '2160p': 5, '1440p': 4, '1080p': 3, '720p': 2, '480p': 1, 'sd': 0}

    def sort_key(s):
        parse = unicodedata.normalize('NFKD', s.get('name', '') + ' ' + s.get('title', '')).lower()
        res_match = re.search(r'(4k|2160p|1440p|1080p|720p|480p|sd)', parse, re.I)
        res_score = res_order.get(res_match.group(1).lower() if res_match else 'sd', 0)
        size_match = re.search(r'(\d+(?:\.\d+)?)\s*(gb|mb)', parse, re.I)
        size_gb = float(size_match.group(1)) if size_match and size_match.group(2).lower() == 'gb' else (float(size_match.group(1)) / 1024 if size_match else 0)
        seeder_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', parse, re.I | re.U)
        seeders = int(seeder_match.group(1) or seeder_match.group(2)) if seeder_match else 0
        is_cached = s.get('hints', {}).get('isCached', False)
        return (-int(is_cached), -seeders, -size_gb, -res_score)

    filtered.sort(key=sort_key)
    sorted_count = len(filtered)
    logging.info(f"Post-sort count: {sorted_count} (cached first, then seeders/size/res descending)")
    if sorted_count != filtered_count:
        logging.warning(f"Count mismatch after sort: pre-sort {filtered_count}, post-sort {sorted_count} - potential bug")

    # NEW: protect formatting from crashing the request; log stream payload on errors
    format_errors = 0

    for i, s in enumerate(filtered):
        try:
            parse_string = unicodedata.normalize('NFKD', s.get('name', '') + ' ' + s.get('title', '') + ' ' + s.get('infoHash', '')).lower()
            hints = s.get('hints', {})
            service = hints.get('service', '')
            color = SERVICE_COLORS.get(service, '')
            name = f"{color} " if color else ''

            res_match = re.search(r'(4k|2160p|1440p|1080p|720p|480p|sd)', parse_string, re.I)
            if res_match:
                res = res_match.group(1).upper()
                name += f"📺 {res}"
                logging.debug(f"Added res {res} for stream {i} from match: {res_match.group(0)}")
            else:
                logging.debug(f"No res match for stream {i}: pattern=r'(4k|2160p|1440p|1080p|720p|480p|sd)', parse_string={parse_string[:100]}...")

            size_match = re.search(r'(\d+(?:\.\d+)?)\s*(gb|mb)', parse_string, re.I)
            if size_match:
                size = f"{size_match.group(1)} {size_match.group(2).upper()}"
                name += f" 💿 {size}"
                logging.debug(f"Added size {size} for stream {i} from match: {size_match.group(0)}")
            else:
                logging.debug(f"No size match for stream {i}: pattern=r'(\\d+(?:\\.\\d+)?)\\s*(gb|mb)', parse_string={parse_string[:100]}...")

            seeder_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', parse_string, re.I | re.U)
            if seeder_match:
                seeders = seeder_match.group(1) or seeder_match.group(2)
                name += f" 👥 {seeders}"
                logging.debug(f"Added seeders {seeders} for stream {i} from match: {seeder_match.group(0)}")
            else:
                logging.debug(f"Seeders match failed for stream {i}: pattern=r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\\d+)(?:𖧧)?|(\\d+)\\s*(?:seed|𖧧)', parse_string={parse_string[:100]}...")

            lang_match = re.search(r'([a-z]{2,3}(?:[ ·,·-]*[a-z]{2,3})*)', parse_string, re.I | re.U)
            if lang_match:
                langs = re.findall(r'[a-z]{2,3}', lang_match.group(1), re.I)
                flags_added = set(LANGUAGE_FLAGS.get(lang, '') for lang in langs if lang in LANGUAGE_FLAGS)
                if flags_added:
                    name += ' ' + ' '.join(flags_added)
                    logging.debug(f"Added flags {flags_added} for stream {i} from match: {lang_match.group(0)}")
                else:
                    logging.debug(f"Lang match but no known flags for stream {i}: {lang_match.group(0)}")
            else:
                logging.debug(f"No lang pattern match for stream {i}: pattern=r'([a-z]{2,3}(?:[ ·,·-]*[a-z]{2,3})*)', parse_string={parse_string[:100]}...")

            audio_match = re.search(r'(dd\+|dd|dts-hd|dts|opus|aac|atmos|ma|5\.1|7\.1|2\.0|h\.26[4-5]|hev|dv|hdr)', parse_string, re.I | re.U)
            channel_match = re.search(r'(\d\.\d)', parse_string, re.I | re.U)
            if audio_match:
                audio = audio_match.group(1).upper()
                channels = channel_match.group(1) if channel_match else ''
                name += f" ♬ {audio} {channels}".strip()
                logging.debug(f"Added audio attribute {audio} {channels} for stream {i} from match: {audio_match.group(0)}")
            else:
                logging.debug(f"No audio match for stream {i}: pattern=r'(dd\\+|dd|dts-hd|dts|opus|aac|atmos|ma|5\\.1|7\\.1|2\\.0|h\\.26[4-5]|hev|dv|hdr)', parse_string={parse_string[:100]}...")

            if 'store' in parse_string or '4k' in parse_string or 'stremthru' in parse_string:
                name = f"★ {name}"
            if '⏳' in name or not hints.get('isCached', False):
                s['name'] = f"[dim]{name} (Unverified ⏳)[/dim]"
            else:
                s['name'] = name

            logging.debug(f"Formatted stream {i}: final name='{s['name']}'")

        except Exception as e:
            format_errors += 1
            logging.exception(
                f"STREAM_FORMAT_ERROR idx={i} err={e} "
                f"payload_snip={json.dumps(s, ensure_ascii=False)[:2000]}"
            )
            continue

    final_count = len(filtered)
    logging.info(f"Final delivered count: {final_count}")
    if final_count == 0:
        logging.warning(f"Zero final streams - full pipeline stats: raw={raw_count}, filtered={filtered_count}, sorted={sorted_count}")

    final_streams = filtered[:60]
    try:
        if final_count > 0:
            logging.debug(f"Final streams JSON sample (first 5): {json.dumps({'streams': final_streams[:5]}, indent=2)} ... (total {final_count})")
            logging.info(f"Final delivered sample: Name '{final_streams[0].get('name', 'NO NAME')}'")
    except Exception as e:
        logging.exception(f"Final JSON log failed: {e} - Streams count {final_count}; possible large data issue")

    # NEW: end-of-request summary line you can grep by RID
    logging.info(f"REQUEST_SUMMARY raw={raw_count} delivered={len(final_streams)} stream_errors={stream_errors} format_errors={format_errors} drops_top10={drop_counts.most_common(10)}")

    return jsonify({'streams': final_streams})

if __name__ == '__main__':
    try:
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)
    except Exception as e:
        logging.exception(f"App startup error: {e}")

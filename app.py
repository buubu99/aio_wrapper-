from flask import Flask, request, jsonify
import requests
import os
import logging
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))

# Session with retry for reliable fetches
session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

AIO_BASE = os.environ.get('AIO_URL', 'https://buubuu99-aiostreams.elfhosted.cc/stremio/acc199cb-6b12-4fa9-be4e-a8ff4c13fa50/eyJpIjoiRTJES0N1ZFBaWm8wb25aS05tNEFsUT09IiwiZSI6InhrVVFtdTFEWm5lcGVrcEh5VUZaejZlcEJLMEMrcXdLakY4UU9zUDJoOFE9IiwidCI6ImEifQ')

STORE_BASE = os.environ.get('STORE_URL', 'https://buubuu99-stremthru.elfhosted.cc/stremio/store/eyJzdG9yZV9uYW1lIjoiIiwic3RvcmVfdG9rZW4iOiJZblYxWW5WMU9UazZUV0Z5YVhOellUazVRREV4Tnc9PSIsImhpZGVfY2F0YWxvZyI6dHJ1ZSwid2ViZGwiOnRydWV9')

USE_STORE = os.environ.get('USE_STORE', 'true').lower() == 'true'  # Optional Store

MIN_SEEDERS = int(os.environ.get('MIN_SEEDERS', 0))  # Relax

MIN_SIZE_BYTES = int(os.environ.get('MIN_SIZE_BYTES', 500000000))

MAX_SIZE_BYTES = int(os.environ.get('MAX_SIZE_BYTES', 100000000000))

REQUEST_TIMEOUT = int(os.environ.get('TIMEOUT', 60000)) / 1000  # Default 60s

CONCURRENCY_LIMIT = int(os.environ.get('CONCURRENCY_LIMIT', 20))

# Flag mapping for languages
LANGUAGE_FLAGS = {
    'eng': '🇬🇧',
    'jpn': '🇯🇵',
    'ita': '🇮🇹',
    'fra': '🇫🇷',
    'kor': '🇰🇷',
    'chn': '🇨🇳',
    'uk': '🇬🇧'  # UK same as Eng
    # Add more as needed, e.g., 'spa': '🇪🇸', 'ger': '🇩🇪'
}

@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.0",
        "name": "Grok AIO Wrapper",
        "description": "Wraps AIOStreams to filter and format streams (Store optional)",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "catalogs": [],
        "idPrefixes": ["tt"]
    })

@app.route('/health')
def health():
    return "OK", 200  # For pinger, small output

@app.route('/stream/<media_type>/<media_id>.json')
def streams(media_type, media_id):
    all_streams = []
    try:
        aio_url = f"{AIO_BASE}/stream/{media_type}/{media_id}.json"
        aio_response = session.get(aio_url, timeout=REQUEST_TIMEOUT)
        aio_response.raise_for_status()
        aio_streams = aio_response.json().get('streams', [])
        all_streams += aio_streams
        logging.info(f"AIO fetch success: {len(aio_streams)} streams")
    except Exception as e:
        logging.error(f"AIO fetch error: {e}")
    if USE_STORE:
        try:
            store_url = f"{STORE_BASE}/stream/{media_type}/{media_id}.json"
            store_response = session.get(store_url, timeout=REQUEST_TIMEOUT)
            store_response.raise_for_status()
            store_streams = store_response.json().get('streams', [])
            all_streams += store_streams
            logging.info(f"Store fetch success: {len(store_streams)} streams")
        except Exception as e:
            logging.error(f"Store fetch error: {e}")
    # Filter: Relax - skip only true ⏳ uncached; include if parse fails
    filtered = []
    for s in all_streams:
        hints = s.get('behaviorHints', {})
        title = s.get('title', '').lower()
        is_cached = hints.get('isCached', False)
        logging.debug(f"Stream: {title}, isCached: {is_cached}")
        if '⏳' in title and not is_cached:
            continue
        parts = title.split()
        seeders = 0
        for i, part in enumerate(parts):
            if 'seed' in part.lower() and i > 0 and parts[i-1].isdigit():
                seeders = int(parts[i-1])
                break
        size_str = next((part for part in parts if 'gb' in part.lower() or 'mb' in part.lower()), '0 gb')
        try:
            size_num = float(size_str.rstrip('gmb'))
        except ValueError:
            size_num = 0
        size = size_num * (10**9 if 'gb' in size_str.lower() else 10**6)
        if seeders >= MIN_SEEDERS and MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES:
            filtered.append(s)
    # Sort: Quality/res first, then provider, cache
    def sort_key(s):
        title = s.get('title', '').lower()
        hints = s.get('behaviorHints', {})
        res_priority = {'4k': 0, '2160p': 0, '1080p': 1, '720p': 2}.get(next((r for r in ['4k', '2160p', '1080p', '720p'] if r in title), ''), 3)
        source_priority = 0 if 'store' in title or 'stremthru' in title else (1 if 'rd' in title or 'realdebrid' in title else (2 if 'tb' in title or 'torbox' in title else (3 if 'ad' in title or 'alldebrid' in title else 4)))
        cache_priority = 0 if hints.get('isCached', False) else 1
        return (res_priority, source_priority, cache_priority)
    filtered.sort(key=sort_key)
    # Reformat: ★ for Store/4K/StremThru, dim uncached/⏳, add flags for languages
    for s in filtered:
        title = s.get('title', '')
        hints = s.get('behaviorHints', {})
        # Add flags
        for code, flag in LANGUAGE_FLAGS.items():
            if code in title.lower():
                title += f" {flag}"
        # Update title
        if 'store' in title.lower() or '4k' in title.lower() or 'stremthru' in title.lower():
            s['title'] = f"★ {title}"
        if '⏳' in title or not hints.get('isCached', False):
            s['title'] = f"[dim]{title} (Unverified)[/dim]"
        else:
            s['title'] = title  # Update with flags
    logging.info(f"Final filtered: {len(filtered)}")
    return jsonify({'streams': filtered[:60]})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

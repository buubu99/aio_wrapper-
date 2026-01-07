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
USE_STORE = os.environ.get('USE_STORE', 'true').lower() == 'true' # Optional Store
MIN_SEEDERS = int(os.environ.get('MIN_SEEDERS', 0)) # Relax
MIN_SIZE_BYTES = int(os.environ.get('MIN_SIZE_BYTES', 500000000))
MAX_SIZE_BYTES = int(os.environ.get('MAX_SIZE_BYTES', 100000000000))
REQUEST_TIMEOUT = int(os.environ.get('TIMEOUT', 60000)) / 1000 # Default 60s
CONCURRENCY_LIMIT = int(os.environ.get('CONCURRENCY_LIMIT', 20))
# Flag mapping for languages (focus on short codes to avoid false positives)
LANGUAGE_FLAGS = {
    'eng': '🇬🇧',
    'en': '🇬🇧',
    'jpn': '🇯🇵',
    'jp': '🇯🇵',
    'ita': '🇮🇹',
    'it': '🇮🇹',
    'fra': '🇫🇷',
    'fr': '🇫🇷',
    'kor': '🇰🇷',
    'kr': '🇰🇷',
    'chn': '🇨🇳',
    'cn': '🇨🇳',
    'uk': '🇬🇧'
    # Add more as needed, remove full words like 'english' since we're parsing exactly
}
@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.2",  # Bump for sorting/flags fix
        "name": "Grok AIO Wrapper",
        "description": "Wraps AIOStreams to filter and format streams (Store optional)",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "catalogs": [],
        "idPrefixes": ["tt"]
    })
@app.route('/health')
def health():
    return "OK", 200 # For pinger, small output
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
    # Filter: Stricter - skip if ⏳ or uncached
    filtered = []
    for s in all_streams:
        hints = s.get('behaviorHints', {})
        title_lower = s.get('title', '').lower()
        is_cached = hints.get('isCached', False)
        logging.debug(f"Stream: {title_lower}, isCached: {is_cached}")
        if '⏳' in title_lower or not is_cached:
            continue
        parts = title_lower.split()
        seeders = 0
        for i, part in enumerate(parts):
            if 'seed' in part and i > 0 and parts[i-1].isdigit():
                seeders = int(parts[i-1])
                break
        size_str = next((part for part in parts if 'gb' in part or 'mb' in part), '0 gb')
        try:
            size_num = float(''.join(c for c in size_str if c.isdigit() or c == '.'))
        except ValueError:
            size_num = 0
        size = size_num * (10**9 if 'gb' in size_str else 10**6)
        if seeders >= MIN_SEEDERS and MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES:
            filtered.append(s)
    # Sort: Res → Quality → Provider → Size desc (Tam-Taro approx: best first, mix providers, size within)
    def sort_key(s):
        title_lower = s.get('title', '').lower()
        hints = s.get('behaviorHints', {})
        # Res priority (lower better: 4k=0)
        res_priority = {'4k': 0, '2160p': 0, '1080p': 1, '720p': 2}.get(next((r for r in ['4k', '2160p', '1080p', '720p'] if r in title_lower), ''), 3)
        # Quality priority (lower better: remux=0, bluray=1, etc.)
        quality_priority = {'remux': 0, 'bluray': 1, 'web-dl': 2, 'webrip': 3}.get(next((q for q in ['remux', 'bluray', 'web-dl', 'webrip'] if q in title_lower), ''), 4)
        # Provider priority (lower better: store=0, rd=1, tb=2, ad=3)
        source_priority = 0 if 'store' in title_lower or 'stremthru' in title_lower else (1 if 'rd' in title_lower or 'realdebrid' in title_lower else (2 if 'tb' in title_lower or 'torbox' in title_lower else (3 if 'ad' in title_lower or 'alldebrid' in title_lower else 4)))
        # Size desc (-size_num for higher better in asc sort)
        parts = title_lower.split()
        size_str = next((part for part in parts if 'gb' in part or 'mb' in part), '0 gb')
        try:
            size_num = float(''.join(c for c in size_str if c.isdigit() or c == '.'))
        except ValueError:
            size_num = 0
        logging.debug(f"Parsed size for '{title_lower}': {size_num} from '{size_str}'")
        return (res_priority, quality_priority, source_priority, -size_num)
    filtered.sort(key=sort_key)
    # Reformat: ★ for Store/4K/StremThru, dim uncached/⏳, parse/replace languages with unique flags
    for s in filtered:
        title = s.get('title', '')
        hints = s.get('behaviorHints', {})
        orig_title = title  # For debug
        # Parse and replace languages (assume last part if comma-separated short codes)
        parts = title.split()
        flags_added = set()
        if len(parts) > 1 and ',' in parts[-1]:
            lang_str = parts[-1]
            langs = [l.strip().lower() for l in lang_str.split(',') if l.strip()]
            for lang in langs:
                if lang in LANGUAGE_FLAGS:
                    flags_added.add(LANGUAGE_FLAGS[lang])
        if flags_added:
            # Replace lang text with flags (matches Stremthru style)
            title = ' '.join(parts[:-1]) + ' ' + ' '.join(flags_added)
            logging.debug(f"Added flags to '{orig_title}': {title}")
        else:
            logging.debug(f"No lang match in '{orig_title}'")
        # Update title
        if 'store' in title.lower() or '4k' in title.lower() or 'stremthru' in title.lower():
            title = f"★ {title}"  # Prepend after flags
        if '⏳' in title or not hints.get('isCached', False):
            s['title'] = f"[dim]{title} (Unverified)[/dim]"
        else:
            s['title'] = title
    logging.info(f"Final filtered: {len(filtered)}")
    return jsonify({'streams': filtered[:60]})
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

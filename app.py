from flask import Flask, request, jsonify
import requests
import os
import logging
import re
import json  # For logging raw stream dict
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # Ensure Unicode/emojis not escaped
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
# Flag mapping (emojis + fallback text)
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
    'uk': '🇬🇧',
    'ger': '🇩🇪',
    'de': '🇩🇪',
    'yes': '📝'  # For subtitle indicators
}
LANGUAGE_TEXT_FALLBACK = {
    'eng': '[GB]',
    'en': '[GB]',
    'jpn': '[JP]',
    'jp': '[JP]',
    'ita': '[IT]',
    'it': '[IT]',
    'fra': '[FR]',
    'fr': '[FR]',
    'kor': '[KR]',
    'kr': '[KR]',
    'chn': '[CN]',
    'cn': '[CN]',
    'uk': '[GB]',
    'ger': '[DE]',
    'de': '[DE]',
    'yes': '[SUB]'
}
@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.8",  # Bump for logging focus
        "name": "Grok AIO Wrapper",
        "description": "Wraps AIOStreams to filter and format streams (Store optional)",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "catalogs": [],
        "idPrefixes": ["tt"]
    })
@app.route('/health')
def health():
    return "OK", 200
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
        logging.debug(f"Raw AIO response keys: {list(aio_response.json().keys())}")
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
            logging.debug(f"Raw Store response keys: {list(store_response.json().keys())}")
        except Exception as e:
            logging.error(f"Store fetch error: {e}")
    # Filter: Stricter - skip if ⏳ or uncached
    filtered = []
    for i, s in enumerate(all_streams):
        hints = s.get('behaviorHints', {})
        title = s.get('title', '').replace('\n', ' ')  # Normalize multi-line
        if '\n' in s.get('title', ''):  # Flag multi-line issue
            logging.debug(f"Multi-line title detected in stream {i}: {s.get('title', 'NO TITLE')}")
        title_lower = title.lower()
        is_cached = hints.get('isCached', False)
        if not title:  # Log full raw stream if no title
            logging.debug(f"Stream {i} received with NO TITLE: raw dict = {json.dumps(s)}")
        logging.debug(f"Stream {i} title: {title if title else 'NO TITLE'}, isCached: {is_cached}")
        if '⏳' in title_lower or not is_cached:
            logging.debug(f"Skipped stream {i}: {title if title else 'NO TITLE'} (uncached or ⏳)")
            continue
        # Parse seeders
        seed_match = re.search(r'👥 (\d+)|(\d+)\s*seed', title_lower, re.I)
        seeders = int(seed_match.group(1) or seed_match.group(2)) if seed_match else 0
        if not seed_match:
            logging.debug(f"Seeders pattern match failed for stream {i}: {title}")
        # Parse size (handles no space)
        size_match = re.search(r'(\d+\.?\d*)(gb|mb)', title_lower, re.I)
        size = 0
        if size_match:
            size_num = float(size_match.group(1))
            unit = size_match.group(2).lower()
            size = size_num * (10**9 if unit == 'gb' else 10**6)
        else:
            logging.debug(f"Size pattern match failed for stream {i}: {title}")
        logging.debug(f"Parsed seeders/size for stream {i} '{title}': seeders={seeders}, size={size}")
        if seeders >= MIN_SEEDERS and MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES:
            filtered.append(s)
            logging.debug(f"Kept stream {i}: {title} (cached, meets criteria)")
        else:
            logging.debug(f"Skipped stream {i}: {title} (low seeders/size)")
    # Sort: Res > Quality > Provider > Size desc > Seeders desc
    def sort_key(s):
        title = s.get('title', '').replace('\n', ' ')
        title_lower = title.lower()
        hints = s.get('behaviorHints', {})
        res_priority = {'4k': 0, '2160p': 0, '1080p': 1, '720p': 2}.get(next((r for r in ['4k', '2160p', '1080p', '720p'] if r in title_lower), ''), 3)
        quality_priority = {'remux': 0, 'bluray': 1, 'web-dl': 2, 'webrip': 3}.get(next((q for q in ['remux', 'bluray', 'web-dl', 'webrip'] if q in title_lower), ''), 4)
        source_priority = 0 if 'store' in title_lower or 'stremthru' in title_lower else (1 if 'rd' in title_lower or 'realdebrid' in title_lower else (2 if 'tb' in title_lower or 'torbox' in title_lower else (3 if 'ad' in title_lower or 'alldebrid' in title_lower else 4)))
        size_match = re.search(r'(\d+\.?\d*)(gb|mb)', title_lower, re.I)
        size_num = float(size_match.group(1)) if size_match else 0
        seed_match = re.search(r'👥 (\d+)|(\d+)\s*seed', title_lower, re.I)
        seeders = int(seed_match.group(1) or seed_match.group(2)) if seed_match else 0
        key = (res_priority, quality_priority, source_priority, -size_num, -seeders)
        logging.debug(f"Sort key for '{title}': {key} (res={res_priority}, qual={quality_priority}, src={source_priority}, size={size_num}, seeds={seeders})")
        return key
    filtered.sort(key=sort_key)
    logging.info(f"Sorted filtered streams (first 5 titles/keys): {[(f.get('title', 'NO TITLE'), sort_key(f)) for f in filtered[:5]]}")
    # Reformat: ★ for Store/4K/StremThru, dim uncached/⏳, parse/replace languages with unique flags
    use_emoji_flags = True  # Set to False for text fallback [GB]
    for i, s in enumerate(filtered):
        title = s.get('title', '').replace('\n', ' ')
        hints = s.get('behaviorHints', {})
        orig_title = title
        # Parse langs at end (handles spaces, e.g., "ita, eng, yes")
        lang_match = re.search(r'([a-z]{2,3}(?:,\s*[a-z]{2,3})*)$', title.lower())
        if lang_match:
            lang_str = lang_match.group(1)
            langs = [l.strip() for l in lang_str.split(',')]
            if use_emoji_flags:
                flags_added = set(LANGUAGE_FLAGS.get(lang, '') for lang in langs if lang in LANGUAGE_FLAGS)
            else:
                flags_added = set(LANGUAGE_TEXT_FALLBACK.get(lang, '') for lang in langs if lang in LANGUAGE_TEXT_FALLBACK)
            if flags_added:
                title = re.sub(r'([a-z]{2,3}(?:,\s*[a-z]{2,3})*)$', ' ' + ' '.join(flags_added), title, flags=re.I)
                logging.debug(f"Stream {i} added flags to '{orig_title}': {title} (matched pattern: {lang_match.group(0)})")
            else:
                logging.debug(f"Stream {i} flag pattern matched but no known langs: '{orig_title}' (pattern: {lang_match.group(0)})")
        else:
            logging.debug(f"Stream {i} no lang pattern match in '{orig_title}' (expected trailing langs like 'ita, eng')")
        title += ' 🇬🇧'  # Temporary test flag
        logging.debug(f"Stream {i} test flag added: {title}")
        # Update title
        if 'store' in title.lower() or '4k' in title.lower() or 'stremthru' in title.lower():
            title = f"★ {title}"
        if '⏳' in title or not hints.get('isCached', False):
            s['title'] = f"[dim]{title} (Unverified)[/dim]"
        else:
            s['title'] = title
    logging.info(f"Final filtered: {len(filtered)}")
    return jsonify({'streams': filtered[:60]})
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

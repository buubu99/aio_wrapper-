from flask import Flask, request, jsonify
import requests
import os
import logging
import re
import json
import unicodedata
import time
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fuzzywuzzy import fuzz
import Levenshtein  # For speed, but optional

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, resources={r"/*": {"origins": "*"}})

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'DEBUG'), format='%(asctime)s | %(levelname)s | %(message)s')

session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

AIO_BASE = os.environ.get('AIO_URL', 'https://buubuu99-aiostreams.elfhosted.cc/stremio/acc199cb-6b12-4fa9-be4e-a8ff4c13fa50/eyJpIjoiRTJES0N1ZFBaWm8wb25aS05tNEFsUT09IiwiZSI6InhrVVFtdTFEWm5lcGVrcEh5VUZaejZlcEJLMEMrcXdLakY4UU9zUDJoOFE9IiwidCI6ImEifQ')
STORE_BASE = os.environ.get('STORE_URL', 'https://buubuu99-stremthru.elfhosted.cc/stremio/store/eyJzdG9yZV9uYW1lIjoiIiwic3RvcmVfdG9rZW4iOiJZblYxWW5WMU9UazZUV0Z5YVhOellUazVRREV4Tnc9PSIsImhpZGVfY2F0YWxvZyI6dHJ1ZSwid2ViZGwiOnRydWV9')
USE_STORE = os.environ.get('USE_STORE', 'true').lower() == 'true'
MIN_SEEDERS = int(os.environ.get('MIN_SEEDERS', 1))  # Tightened
MIN_SIZE_BYTES = int(os.environ.get('MIN_SIZE_BYTES', 500000000))
MAX_SIZE_BYTES = int(os.environ.get('MAX_SIZE_BYTES', 100000000000))
REQUEST_TIMEOUT = int(os.environ.get('TIMEOUT', 60000)) / 1000
MAX_UNCACHED_KEEP = 20  # Increased
PING_TIMEOUT = 2
API_POLL_TIMEOUT = 10
POLL_INTERVAL = 1  # Base for backoff
MAX_POLLS = 15  # Increased

RD_API_KEY = 'Z4C3UT777IK2U6EUZ5HLVVMO7BYQPDNEOJUGAFLUBXMGTSX2Z6RA'
TB_API_KEY = '1046c773-9d0d-4d53-95ab-8976a559a5f6'
AD_API_KEY = 'Z2HvUmLsuuRmyHg5Zf5K8'

LANGUAGE_FLAGS = {
    'eng': '🇬🇧', 'en': '🇬🇧', 'jpn': '🇯🇵', 'jp': '🇯🇵', 'ita': '🇮🇹', 'it': '🇮🇹',
    'fra': '🇫🇷', 'fr': '🇫🇷', 'kor': '🇰🇷', 'kr': '🇰🇷', 'chn': '🇨🇳', 'cn': '🇨🇳',
    'uk': '🇬🇧', 'ger': '🇩🇪', 'de': '🇩🇪', 'hun': '🇭🇺', 'yes': '📝', 'ko': '🇰🇷'
}
LANGUAGE_TEXT_FALLBACK = {
    'eng': '[GB]', 'en': '[GB]', 'jpn': '[JP]', 'jp': '[JP]', 'ita': '[IT]', 'it': '[IT]',
    'fra': '[FR]', 'fr': '[FR]', 'kor': '[KR]', 'kr': '[KR]', 'chn': '[CN]', 'cn': '[CN]',
    'uk': '[GB]', 'ger': '[DE]', 'de': '[DE]', 'hun': '[HU]', 'yes': '[SUB]', 'ko': '[KR]'
}
SERVICE_COLORS = {
    'rd': '[red]', 'realdebrid': '[red]',
    'tb': '[blue]', 'torbox': '[blue]',
    'ad': '[green]', 'alldebrid': '[green]',
    'store': '[purple]', 'stremthru': '[purple]'
}
PREFERRED_TITLE = 'wailing goksung'  # For fuzz relevance

# Provider order for grouping
PROVIDER_ORDER = ['store', 'rd', 'tb', 'ad']

# Log env vars at startup
logging.info("APP START | ENV VARS DUMP: %s", json.dumps(dict(os.environ), indent=2))

def debrid_check_cache(url, service='rd'):
    start_time = time.time()
    hash_match = re.search(r'(?:magnet:\?xt=urn:btih:)?([a-fA-F0-9]{40})', url, re.I)
    if not hash_match:
        logging.warning("DEBRID_CHECK | NO_HASH | URL: %s | SERVICE: %s | DURATION: %.3fs", url, service, time.time() - start_time)
        return False
    torrent_hash = hash_match.group(1).upper()
    logging.debug("DEBRID_CHECK | START | HASH: %s | SERVICE: %s | URL: %s", torrent_hash, service, url)
    
    if service == 'rd':
        headers = {'Authorization': f'Bearer {RD_API_KEY}'}
        check_url = f'https://api.real-debrid.com/rest/1.0/torrents/instantAvailability/{torrent_hash}'
        logging.debug("DEBRID_CHECK | RD_INSTANT | URL: %s | HEADERS: %s", check_url, json.dumps(dict(headers)))
        try:
            response = session.get(check_url, headers=headers, timeout=API_POLL_TIMEOUT)
            logging.debug("DEBRID_CHECK | RD_RESPONSE | STATUS: %d | TEXT: %s", response.status_code, response.text[:500])
            if response.status_code == 200:
                data = response.json()
                logging.debug("DEBRID_CHECK | RD_DATA_DUMP: %s", json.dumps(data, indent=2))
                if data.get(torrent_hash.lower(), {}).get('rd'):  # If has hoster files
                    logging.info("DEBRID_CHECK | RD_CACHED_TRUE | HASH: %s | DURATION: %.3fs", torrent_hash, time.time() - start_time)
                    return True
        except Exception as e:
            logging.error("DEBRID_CHECK | RD_ERROR: %s | DURATION: %.3fs", str(e), time.time() - start_time)
        # Fallback to add/poll if instant fails
        add_url = 'https://api.real-debrid.com/rest/1.0/torrents/addMagnet'
        data = {'magnet': f'magnet:?xt=urn:btih:{torrent_hash}'}
        logging.debug("DEBRID_CHECK | RD_FALLBACK_ADD | URL: %s | DATA: %s | HEADERS: %s", add_url, data, json.dumps(dict(headers)))
        response = session.post(add_url, headers=headers, data=data)
        logging.debug("DEBRID_CHECK | RD_ADD_RESPONSE | STATUS: %d | TEXT: %s", response.status_code, response.text[:500])
        if response.status_code != 200:
            logging.warning("DEBRID_CHECK | RD_ADD_FAIL | HASH: %s | DURATION: %.3fs", torrent_hash, time.time() - start_time)
            return False
        torrent_id = response.json().get('id')
        if not torrent_id:
            return False
        poll_start = time.time()
        for poll in range(1, MAX_POLLS + 1):
            info_url = f'https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}'
            logging.debug("DEBRID_CHECK | RD_POLL | POLL_NUM: %d | URL: %s | HEADERS: %s", poll, info_url, json.dumps(dict(headers)))
            info_resp = session.get(info_url, headers=headers)
            logging.debug("DEBRID_CHECK | RD_POLL_RESPONSE | STATUS: %d | TEXT: %s", info_resp.status_code, info_resp.text[:500])
            if info_resp.status_code == 200:
                info = info_resp.json()
                logging.debug("DEBRID_CHECK | RD_POLL_DATA: %s", json.dumps(info, indent=2))
                if info.get('progress') == 100 or info.get('status') in ['downloaded', 'queued']:
                    delete_url = f'https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}'
                    session.delete(delete_url, headers=headers)
                    logging.info("DEBRID_CHECK | RD_FALLBACK_CACHED_TRUE | HASH: %s | POLLS: %d | DURATION: %.3fs", torrent_hash, poll, time.time() - start_time)
                    return True
            time.sleep(POLL_INTERVAL * (1.5 ** poll))
        delete_url = f'https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}'
        session.delete(delete_url, headers=headers)
        logging.warning("DEBRID_CHECK | RD_FALLBACK_TIMEOUT | HASH: %s | DURATION: %.3fs", torrent_hash, time.time() - start_time)
        return False
    
    elif service == 'tb':
        headers = {'Authorization': f'Bearer {TB_API_KEY}'}
        check_url = f'https://api.torbox.app/v1/api/torrents/checkcache?hash={torrent_hash}'
        logging.debug("DEBRID_CHECK | TB_CHECKCACHE | URL: %s | HEADERS: %s", check_url, json.dumps(dict(headers)))
        try:
            response = session.get(check_url, headers=headers, timeout=API_POLL_TIMEOUT)
            logging.debug("DEBRID_CHECK | TB_RESPONSE | STATUS: %d | TEXT: %s", response.status_code, response.text[:500])
            if response.status_code == 200 and response.json().get('data', {}).get('cached', False):
                logging.info("DEBRID_CHECK | TB_CACHED_TRUE | HASH: %s | DURATION: %.3fs", torrent_hash, time.time() - start_time)
                return True
        except Exception as e:
            logging.error("DEBRID_CHECK | TB_ERROR: %s | DURATION: %.3fs", str(e), time.time() - start_time)
        return False
    
    elif service == 'ad':
        headers = {'Authorization': f'Bearer {AD_API_KEY}'}
        add_url = 'https://api.alldebrid.com/v4/magnet/upload'
        params = {'magnets[]': f'magnet:?xt=urn:btih:{torrent_hash}'}
        logging.debug("DEBRID_CHECK | AD_ADD | URL: %s | PARAMS: %s | HEADERS: %s", add_url, params, json.dumps(dict(headers)))
        response = session.get(add_url, headers=headers, params=params)  # Fixed to GET
        logging.debug("DEBRID_CHECK | AD_RESPONSE | STATUS: %d | TEXT: %s", response.status_code, response.text[:500])
        if response.status_code != 200:
            logging.warning("DEBRID_CHECK | AD_ADD_FAIL | HASH: %s | DURATION: %.3fs", torrent_hash, time.time() - start_time)
            return False
        magnets = response.json().get('data', {}).get('magnets', [])
        if not magnets:
            return False
        ready = magnets[0].get('ready', False)
        torrent_id = magnets[0].get('id')
        if ready:
            logging.info("DEBRID_CHECK | AD_CACHED_TRUE | HASH: %s | DURATION: %.3fs", torrent_hash, time.time() - start_time)
        if torrent_id:
            delete_url = 'https://api.alldebrid.com/v4/magnet/delete'
            del_params = {'id': torrent_id}
            session.get(delete_url, headers=headers, params=del_params)
            logging.debug("DEBRID_CHECK | AD_DELETE | ID: %s | DURATION: %.3fs", torrent_id, time.time() - start_time)
        return ready
    logging.warning("DEBRID_CHECK | UNKNOWN_SERVICE | SERVICE: %s | DURATION: %.3fs", service, time.time() - start_time)
    return False

@app.route('/manifest.json')
def manifest():
    logging.debug("MANIFEST | REQUEST")
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.35",
        "name": "Grok AIO Wrapper",
        "description": "Wraps AIOStreams to filter and format streams (Store optional)",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "catalogs": [],
        "idPrefixes": ["tt"]
    })

@app.route('/health')
def health():
    logging.debug("HEALTH | REQUEST")
    return "OK", 200

@app.route('/stream/<media_type>/<media_id>.json')
def streams(media_type, media_id):
    start_time = time.time()
    logging.info("STREAM_REQ | START | TYPE: %s | ID: %s | DURATION_START: %.3fs", media_type, media_id, time.time() - start_time)
    all_streams = []
    template_count = 0
    uncached_count = 0
    lang_counter = defaultdict(int)
    qual_counter = defaultdict(int)
    res_counter = defaultdict(int)
    try:
        aio_start = time.time()
        aio_url = f"{AIO_BASE}/stream/{media_type}/{media_id}.json"
        logging.debug("STREAM_REQ | AIO_FETCH | URL: %s | TIMEOUT: %s", aio_url, REQUEST_TIMEOUT)
        aio_response = session.get(aio_url, timeout=REQUEST_TIMEOUT)
        aio_response.raise_for_status()
        aio_data = aio_response.json()
        logging.debug("STREAM_REQ | AIO_RAW_DUMP: %s", json.dumps(aio_data, indent=2))
        aio_streams = aio_data.get('streams', [])
        all_streams += aio_streams
        logging.info("STREAM_REQ | AIO_SUCCESS | STREAMS: %d | DURATION: %.3fs", len(aio_streams), time.time() - aio_start)
    except Exception as e:
        logging.error("STREAM_REQ | AIO_ERROR: %s | DURATION: %.3fs", str(e), time.time() - aio_start)
    
    if USE_STORE:
        store_start = time.time()
        try:
            store_url = f"{STORE_BASE}/stream/{media_type}/{media_id}.json"
            logging.debug("STREAM_REQ | STORE_FETCH | URL: %s | TIMEOUT: %s", store_url, REQUEST_TIMEOUT)
            store_response = session.get(store_url, timeout=REQUEST_TIMEOUT)
            store_response.raise_for_status()
            store_data = store_response.json()
            logging.debug("STREAM_REQ | STORE_RAW_DUMP: %s", json.dumps(store_data, indent=2))
            store_streams = store_data.get('streams', [])
            all_streams += store_streams
            logging.info("STREAM_REQ | STORE_SUCCESS | STREAMS: %d | DURATION: %.3fs", len(store_streams), time.time() - store_start)
        except Exception as e:
            logging.error("STREAM_REQ | STORE_ERROR: %s | DURATION: %.3fs", str(e), time.time() - store_start)
    
    logging.info("STREAM_REQ | TOTAL_RAW_STREAMS: %d | DURATION_SO_FAR: %.3fs", len(all_streams), time.time() - start_time)
    logging.debug("STREAM_REQ | ALL_RAW_DUMP: %s", json.dumps(all_streams, indent=2))
    
    process_start = time.time()
    for i, s in enumerate(all_streams):
        stream_start = time.time()
        name = s.get('name', '')
        description = s.get('description', '')
        hints = s.get('behaviorHints', {})
        source = 'AIO' if i < len(aio_streams) else 'Store'
        logging.debug("STREAM_ID: %d | PROCESS_RAW | SOURCE: %s | DICT_DUMP: %s | DURATION_START: %.3fs", i, source, json.dumps(s, indent=2), time.time() - stream_start)
        
        full_text = (name + ' ' + description + ' ' + hints.get('filename', '')).lower()
        if fuzz.partial_ratio(PREFERRED_TITLE, full_text) < 70:
            logging.warning("STREAM_ID: %d | RELEVANCE_FAIL | SCORE: %d | TEXT: %s | SKIPPED", i, fuzz.partial_ratio(PREFERRED_TITLE, full_text), full_text[:200])
            continue
        
        if re.search(r'\{stream\..*::', name) or not name.strip():
            template_count += 1
            filename = hints.get('filename', '')
            url = s.get('url', '')
            logging.warning("STREAM_ID: %d | TEMPLATE_DETECT | NAME: %s | FILENAME: %s | URL: %s | DESC: %s", i, name, filename, url, description)
            qual_match = re.search(r'(Web-dl|Webrip|Bluray|Bdremux|Remux|Hdrip|Tc|Ts|Cam|Dvdrip|Hdtv)', filename + ' ' + name + ' ' + description, re.I)
            res_match = re.search(r'(4k|2160p|1440p|1080p|720p)', filename + ' ' + name + ' ' + description, re.I)
            quality = qual_match.group(1).title() if qual_match else 'Unknown'
            qual_counter[quality] += 1
            res = res_match.group(1).upper() if res_match else '⍰'
            res_counter[res] += 1
            fallback_name = f"{res} {quality}"
            if filename:
                fallback_name = f"{fallback_name} from {filename.replace('.', ' ').title()}"
            s['name'] = fallback_name
            logging.debug("STREAM_ID: %d | TEMPLATE_FALLBACK | NEW_NAME: %s | QUAL_MATCH: %s | RES_MATCH: %s", i, fallback_name, qual_match.group(0) if qual_match else 'NONE', res_match.group(0) if res_match else 'NONE')
        
        if '⏳' in name or not hints.get('isCached', False):
            uncached_count += 1
            logging.debug("STREAM_ID: %d | UNCACHED_DETECT | NAME: %s", i, name)
        
        lang_match = re.search(r'([a-z]{2,3}(?:[ ·,·-]*[a-z]{2,3})*)', full_text, re.I)
        if lang_match:
            langs = re.findall(r'[a-z]{2,3}', lang_match.group(1), re.I)
            for l in langs:
                lang_counter[l] += 1
            logging.debug("STREAM_ID: %d | LANG_DETECT | LANGS: %s | MATCH: %s", i, langs, lang_match.group(0))
    
    logging.info("STREAM_REQ | PRE_FILTER_SUMMARY | TEMPLATES: %d | UNCACHED: %d | LANGS: %s | QUALS: %s | RES: %s | DURATION: %.3fs", template_count, uncached_count, dict(lang_counter), dict(qual_counter), dict(res_counter), time.time() - process_start)
    
    filter_start = time.time()
    filtered = []
    uncached_filtered = []
    logging.debug("STREAM_REQ | FILTER_START | TOTAL_INPUT: %d", len(all_streams))
    for i, s in enumerate(all_streams):
        stream_filter_start = time.time()
        hints = s.get('behaviorHints', {})
        name = s.get('name', '').replace('\n', ' ')
        description = s.get('description', '').replace('\n', ' ')
        parse_string = (name + ' ' + description).lower()
        normalized_parse = unicodedata.normalize('NFKD', parse_string).encode('ascii', 'ignore').decode('utf-8')
        if normalized_parse != parse_string:
            logging.debug("STREAM_ID: %d | UNICODE_NORM | ORIGINAL: %s... | NORMALIZED: %s... | DIFF_LEN: %d", i, parse_string[:100], normalized_parse[:100], len(parse_string) - len(normalized_parse))
        parse_string = normalized_parse
        is_cached = hints.get('isCached', False)
        seed_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\\d+)(?:𖧧)?|(\\d+)\\s*(?:seed|𖧧)', parse_string, re.I | re.U)  # Escaped \\d
        seeders = int(seed_match.group(1) or seed_match.group(2) or 0) if seed_match else (1 if not is_cached else 0)
        if not seed_match:
            logging.debug("STREAM_ID: %d | SEED_FAIL | PATTERN: r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\\d+)(?:𖧧)?|(\\d+)\\s*(?:seed|𖧧)' | PARSE: %s...", i, parse_string[:100])
        size_match = re.search(r'(\\d+\\.?\\d*)\\s*(gb|mb)', parse_string, re.I | re.U)
        size = 0
        if size_match:
            size_num = float(size_match.group(1))
            unit = size_match.group(2).lower()
            size = int(size_num * (10**9 if unit == 'gb' else 10**6))
            logging.debug("STREAM_ID: %d | SIZE_MATCH | NUM: %.2f | UNIT: %s | BYTES: %d", i, size_num, unit, size)
        if size == 0:
            size = hints.get('videoSize', 0)
        if is_cached and size == 0:
            logging.warning("STREAM_ID: %d | MISFLAG_CACHED | ISCACHED: True | SIZE: 0 | NAME: %s", i, name)
        keep = seeders >= MIN_SEEDERS and MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES
        logging.debug("STREAM_ID: %d | FILTER_CRITERIA | KEEP: %s | ISCACHED: %s | SEEDERS: %d (MIN: %d) | SIZE: %d (MIN: %d, MAX: %d) | DURATION: %.3fs", i, keep, is_cached, seeders, MIN_SEEDERS, size, MIN_SIZE_BYTES, MAX_SIZE_BYTES, time.time() - stream_filter_start)
        if keep:
            if is_cached:
                filtered.append(s)
            else:
                uncached_filtered.append(s)
    
    logging.info("STREAM_REQ | FILTER_SUMMARY | CACHED_KEPT: %d | UNCACHED_TO_VERIFY: %d | DURATION: %.3fs", len(filtered), len(uncached_filtered), time.time() - filter_start)
    
    verify_start = time.time()
    uncached_filtered.sort(key=sort_key)
    verified_uncached = []
    for j, us in enumerate(uncached_filtered[:30]):  # Check more but cap keep
        us_start = time.time()
        url = us.get('url', '')
        logging.debug("STREAM_ID: UNCACHED_%d | VERIFY_START | URL: %s", j, url)
        if debrid_check_cache(url, 'rd') or debrid_check_cache(url, 'tb') or debrid_check_cache(url, 'ad'):
            verified_uncached.append(us)
            logging.info("STREAM_ID: UNCACHED_%d | VERIFY_SUCCESS | ADDED_TO_KEPT | DURATION: %.3fs", j, time.time() - us_start)
        else:
            if 'http' in url.lower():
                try:
                    head_start = time.time()
                    head = session.head(url, timeout=PING_TIMEOUT)
                    logging.debug("STREAM_ID: UNCACHED_%d | PING_RESPONSE | STATUS: %d | DURATION: %.3fs", j, head.status_code, time.time() - head_start)
                    if head.status_code == 200:
                        verified_uncached.append(us)
                        logging.info("STREAM_ID: UNCACHED_%d | PING_SUCCESS | ADDED_TO_KEPT | DURATION: %.3fs", j, time.time() - us_start)
                except Exception as e:
                    logging.debug("STREAM_ID: UNCACHED_%d | PING_ERROR: %s | DURATION: %.3fs", j, str(e), time.time() - us_start)
    
    filtered += verified_uncached[:MAX_UNCACHED_KEEP]
    logging.info("STREAM_REQ | VERIFY_SUMMARY | VERIFIED_UNCACHED_KEPT: %d | TOTAL_FILTERED: %d | DURATION: %.3fs", len(verified_uncached), len(filtered), time.time() - verify_start)
    
    def sort_key(s):
        key_start = time.time()
        name = s.get('name', '').replace('\n', ' ')
        description = s.get('description', '').replace('\n', ' ')
        name_lower = (name + ' ' + description).lower()
        normalized_lower = unicodedata.normalize('NFKD', name_lower).encode('ascii', 'ignore').decode('utf-8')
        if normalized_lower != name_lower:
            logging.debug("SORT_KEY | UNICODE_NORM | NAME: %s | ORIGINAL: %s... | NORMALIZED: %s...", name, name_lower[:100], normalized_lower[:100])
        name_lower = normalized_lower
        hints = s.get('behaviorHints', {})
        is_cached = hints.get('isCached', False)
        cached_priority = 0 if is_cached else 1
        complete_priority = 0 if 'complete' in name_lower or '100%' in name_lower else 1
        res_priority = {'4k': 0, '2160p': 0, '1440p': 1, '1080p': 2, '720p': 3, 'bdremux': 0}.get(next((r.lower() for r in ['4k', '2160p', '1440p', '1080p', '720p', 'bdremux'] if r.lower() in name_lower), ''), 4)
        if res_priority == 4:
            logging.debug("SORT_KEY | RES_FAIL | PATTERN: (4k|2160p|1440p|1080p|720p) | NAME_LOWER: %s...", name_lower[:100])
        quality_priority = {'remux': 0, 'bdremux': 0, 'bluray': 1, 'web-dl': 2, 'webrip': 3, 'web': 3, 'hdtv': 4, 'ts': 5}.get(next((q.lower() for q in ['remux', 'bdremux', 'bluray', 'web-dl', 'webrip', 'web', 'hdtv', 'ts'] if q.lower() in name_lower), ''), 4)
        if quality_priority == 4:
            logging.debug("SORT_KEY | QUAL_FAIL | PATTERN: (remux|bdremux|bluray|web-dl|webrip|web|hdtv|ts) | NAME_LOWER: %s...", name_lower[:100])
        seed_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\\d+)(?:𖧧)?|(\\d+)\\s*(?:seed|𖧧)', name_lower, re.I | re.U)
        seeders = int(seed_match.group(1) or seed_match.group(2) or 0) if seed_match else 0
        seadex_priority = 0 if 'seadex' in name_lower or 'ʙᴇsᴛ ʀᴇʟᴇᴀsᴇ' in name_lower else 1
        key = (cached_priority, complete_priority, seadex_priority, res_priority, quality_priority, -seeders)  # Removed size from sort key
        logging.debug("SORT_KEY | NAME: %s | KEY: %s | CACHED_PRI: %d | COMPLETE_PRI: %d | RES: %d | QUAL: %d | SEEDS: %d | NAME_LOWER: %s... | DURATION: %.3fs", name, key, cached_priority, complete_priority, res_priority, quality_priority, seeders, name_lower[:100], time.time() - key_start)
        return key
    
    sort_start = time.time()
    # Group by provider
    grouped = defaultdict(list)
    for s in filtered:
        name_lower = (s.get('name', '') + ' ' + s.get('description', '')).lower()
        provider = next((p for p in PROVIDER_ORDER if p in name_lower), 'other')
        grouped[provider].append(s)
    
    # Sort each group by quality (using sort_key, which now focuses on qual without size)
    for provider in grouped:
        grouped[provider].sort(key=sort_key)
        logging.debug("GROUP_SORT | PROVIDER: %s | SORTED_NAMES: %s", provider, [f.get('name', 'NO NAME') for f in grouped[provider]])
    
    # Concatenate in provider order
    filtered = []
    for provider in PROVIDER_ORDER + ['other']:
        if provider in grouped:
            filtered.extend(grouped[provider])
    
    logging.info("STREAM_REQ | SORT_SUMMARY | GROUPED_COUNTS: %s | FIRST_5_OVERALL: %s | DURATION: %.3fs", {p: len(grouped[p]) for p in grouped}, [(f.get('name', 'NO NAME'), sort_key(f)) for f in filtered[:5]], time.time() - sort_start)
    logging.debug("STREAM_REQ | ALL_SORTED_NAMES: %s", json.dumps([f.get('name', 'NO NAME') for f in filtered], indent=2))
    
    format_start = time.time()
    for i, s in enumerate(filtered):
        format_stream_start = time.time()
        name = s.get('name', '').replace('\n', ' ')
        description = s.get('description', '').replace('\n', ' ')
        parse_string = (name + ' ' + description).lower()
        normalized_parse = unicodedata.normalize('NFKD', parse_string).encode('ascii', 'ignore').decode('utf-8')
        if normalized_parse != parse_string:
            logging.debug("STREAM_ID: %d | FORMAT_UNICODE_NORM | ORIGINAL: %s... | NORMALIZED: %s...", i, parse_string[:100], normalized_parse[:100])
        parse_string = normalized_parse
        hints = s.get('behaviorHints', {})
        service = next((k for k in SERVICE_COLORS if k in parse_string), '')
        if service:
            name = f"{SERVICE_COLORS[service]}{name}[/]"
            logging.debug("STREAM_ID: %d | FORMAT_COLOR | SERVICE: %s | NEW_NAME: %s", i, service, name)
        else:
            logging.debug("STREAM_ID: %d | FORMAT_NO_COLOR | PATTERN: %s | PARSE: %s...", i, list(SERVICE_COLORS.keys()), parse_string[:100])
        if re.search(r'[\uac00-\ud7a3]', parse_string, re.U):
            name += ' 🇰🇷'
            logging.debug("STREAM_ID: %d | FORMAT_HANGUL_FLAG", i)
        elif re.search(r'[\u3040-\u30ff\u4e00-\u9faf]', parse_string, re.U):
            name += ' 🇯🇵'
            logging.debug("STREAM_ID: %d | FORMAT_KANJI_FLAG", i)
        lang_match = re.search(r'([a-z]{2,3}(?:[ ·,·-]*[a-z]{2,3})*)', parse_string, re.I | re.U)
        if lang_match:
            langs = re.findall(r'[a-z]{2,3}', lang_match.group(1), re.I)
            flags_added = set(LANGUAGE_FLAGS.get(lang.lower(), LANGUAGE_TEXT_FALLBACK.get(lang.lower(), '')) for lang in langs)
            if flags_added:
                name += ' ' + ' '.join(flags_added)
                logging.debug("STREAM_ID: %d | FORMAT_FLAGS | ADDED: %s | MATCH: %s", i, flags_added, lang_match.group(0))
            else:
                logging.debug("STREAM_ID: %d | FORMAT_NO_FLAGS | MATCH: %s", i, lang_match.group(0))
        else:
            logging.debug("STREAM_ID: %d | FORMAT_NO_LANG | PATTERN: r'([a-z]{2,3}(?:[ ·,·-]*[a-z]{2,3})*)' | PARSE: %s...", i, parse_string[:100])
        audio_match = re.search(r'(dd\+|dd|dts-hd|dts|opus|aac|atmos|ma|5\.1|7\.1|2\.0)', parse_string, re.I | re.U)
        channel_match = re.search(r'(\\d\.\\d)', parse_string, re.I | re.U)
        if audio_match:
            audio = audio_match.group(1).upper()
            channels = channel_match.group(1) if channel_match else ''
            name += f" ♬ {audio} {channels}".strip()
            logging.debug("STREAM_ID: %d | FORMAT_AUDIO | ATTR: %s %s | MATCH: %s", i, audio, channels, audio_match.group(0))
        else:
            logging.debug("STREAM_ID: %d | FORMAT_NO_AUDIO | PATTERN: r'(dd\+|dd|dts-hd|dts|opus|aac|atmos|ma|5\.1|7\.1|2\.0)' | PARSE: %s...", i, parse_string[:100])
        if 'store' in parse_string or '4k' in parse_string or 'stremthru' in parse_string:
            name = f"★ {name}"
            logging.debug("STREAM_ID: %d | FORMAT_STAR", i)
        if '⏳' in name or not hints.get('isCached', False):
            s['name'] = f"[dim]{name} (Unverified ⏳)[/dim]"
        else:
            s['name'] = name
        logging.debug("STREAM_ID: %d | FORMAT_FINAL | NAME: %s | DURATION: %.3fs", i, s['name'], time.time() - format_stream_start)
    
    logging.info("STREAM_REQ | FORMAT_SUMMARY | TOTAL_FORMATTED: %d | DURATION: %.3fs", len(filtered), time.time() - format_start)
    logging.debug("STREAM_REQ | FINAL_JSON_DUMP: %s", json.dumps({'streams': filtered[:60]}, indent=2))
    
    total_duration = time.time() - start_time
    logging.info("STREAM_REQ | END | TOTAL_STREAMS_RETURNED: 60 (capped) | TOTAL_DURATION: %.3fs", total_duration)
    return jsonify({'streams': filtered[:60]})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logging.info("APP_RUN | START | HOST: 0.0.0.0 | PORT: %d", port)
    app.run(host='0.0.0.0', port=port)

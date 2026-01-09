```python
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

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, resources={r"/*": {"origins": "*"}})
logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'DEBUG'))

session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

AIO_BASE = os.environ.get('AIO_URL', 'https://buubuu99-aiostreams.elfhosted.cc/stremio/acc199cb-6b12-4fa9-be4e-a8ff4c13fa50/eyJpIjoiRTJES0N1ZFBaWm8wb25aS05tNEFsUT09IiwiZSI6InhrVVFtdTFEWm5lcGVrcEh5VUZaejZlcEJLMEMrcXdLakY4UU9zUDJoOFE9IiwidCI6ImEifQ')
STORE_BASE = os.environ.get('STORE_URL', 'https://buubuu99-stremthru.elfhosted.cc/stremio/store/eyJzdG9yZV9uYW1lIjoiIiwic3RvcmVfdG9rZW4iOiJZblYxWW5WMU9UazZUV0Z5YVhOellUazVRREV4Tnc9PSIsImhpZGVfY2F0YWxvZyI6dHJ1ZSwid2ViZGwiOnRydWV9')
USE_STORE = os.environ.get('USE_STORE', 'false').lower() == 'true'

MIN_SEEDERS = int(os.environ.get('MIN_SEEDERS', 0))
MIN_SIZE_BYTES = int(os.environ.get('MIN_SIZE_BYTES', 500000000))
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
            response.raise_for_status()
            data = response.json()
            return data.get('data', {}).get('magnets', {}).get('instant', False)
        except Exception as e:
            logging.error(f"AD cache check failed: {e}")
            return False
    else:
        return False
    try:
        response = session.get(api_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if service == 'rd':
            return bool(data.get(torrent_hash, {}).get('rd'))
        elif service == 'tb':
            return data.get('data', {}).get('is_cached', False)
    except Exception as e:
        logging.error(f"{service.upper()} cache check failed: {e}")
        return False

def get_streams(type_, id_):
    aio_url = f"{AIO_BASE}/stream/{type_}/{id_}.json"
    store_url = f"{STORE_BASE}/stream/{type_}/{id_}.json" if USE_STORE else None
    streams = []
    start = time.time()
    try:
        response = session.get(aio_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        streams.extend(data.get('streams', []))
        logging.info(f"Fetched {len(streams)} from AIO in {time.time() - start:.2f}s")
    except Exception as e:
        logging.error(f"AIO fetch failed: {e}")
    if USE_STORE:
        start_store = time.time()
        try:
            response_store = session.get(store_url, timeout=REQUEST_TIMEOUT)
            response_store.raise_for_status()
            data_store = response_store.json()
            streams.extend(data_store.get('streams', []))
            logging.info(f"Fetched {len(data_store.get('streams', []))} from Store in {time.time() - start_store:.2f}s")
        except Exception as e:
            logging.error(f"Store fetch failed: {e}")
    return streams

@app.route('/manifest.json')
def manifest():
    manifest_data = {
        "id": "org.grok.wrapper",
        "version": "1.0.45",
        "name": "AIO Wrapper",
        "description": "Wraps AIOStreams to filter and format streams (Store optional)",
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
    logging.debug(f"Raw streams fetched: {len(streams)}")
    filtered = []
    uncached_count = 0
    for i, s in enumerate(streams):
        parse_string = unicodedata.normalize('NFKD', s.get('name', '') + ' ' + s.get('title', '') + ' ' + s.get('infoHash', '')).lower()
        logging.debug(f"Parsing stream {i}: {parse_string[:100]}...")
        size_match = re.search(r'(\d+(?:\.\d+)?)\s*(gb|mb)', parse_string, re.I)
        if size_match:
            size_gb = float(size_match.group(1)) if size_match.group(2).lower() == 'gb' else float(size_match.group(1)) / 1024
            size_bytes = size_gb * 1073741824
            logging.debug(f"Size match for stream {i}: {size_gb} GB ({size_bytes} bytes)")
        else:
            size_bytes = 0
            logging.debug(f"No size match for stream {i}: pattern=r'(\\d+(?:\\.\\d+)?)\\s*(gb|mb)', parse_string={parse_string[:100]}...")
        if size_bytes and (size_bytes < MIN_SIZE_BYTES or size_bytes > MAX_SIZE_BYTES):
            logging.debug(f"Filtered out stream {i}: size {size_bytes} bytes out of range [{MIN_SIZE_BYTES}, {MAX_SIZE_BYTES}]")
            continue
        seeder_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', parse_string, re.I | re.U)
        seeders = int(seeder_match.group(1) or seeder_match.group(2)) if seeder_match else 0
        if seeders < MIN_SEEDERS:
            logging.debug(f"Filtered out stream {i}: seeders {seeders} < {MIN_SEEDERS}")
            continue
        hints = {}
        service = next((k for k in SERVICE_COLORS if k in parse_string), None)
        if service:
            hints['service'] = service
            url = s.get('url', '') or s.get('behaviorHints', {}).get('proxyUrl', '')
            if url:
                is_cached = debrid_check_cache(url, service)
                hints['isCached'] = is_cached
                if not is_cached:
                    uncached_count += 1
                    if uncached_count > MAX_UNCACHED_KEEP:
                        logging.debug(f"Filtered out uncached stream {i}: exceeded max uncached keep {MAX_UNCACHED_KEEP}")
                        continue
        filtered.append(s)
        s['hints'] = hints
        logging.debug(f"Kept stream {i}: seeders={seeders}, size={size_bytes}, cached={hints.get('isCached', 'N/A')}")
    logging.info(f"Filtered: {len(filtered)} (from {len(streams)} raw)")
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
    logging.info(f"Sorted {len(filtered)} streams: cached first, then seeders/size/res descending")
    for i, s in enumerate(filtered):
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
        audio_match = re.search(r'(dd\+|dd|dts-hd|dts|opus|aac|atmos|ma|5\.1|7\.1|2\.0)', parse_string, re.I | re.U)
        channel_match = re.search(r'(\d\.\d)', parse_string, re.I | re.U)
        if audio_match:
            audio = audio_match.group(1).upper()
            channels = channel_match.group(1) if channel_match else ''
            name += f" ♬ {audio} {channels}".strip()
            logging.debug(f"Added audio attribute {audio} {channels} for stream {i} from match: {audio_match.group(0)}")
        else:
            logging.debug(f"No audio match for stream {i}: pattern=r'(dd\\+|dd|aac|atmos|5\\.1|2\\.0)', parse_string={parse_string[:100]}...")
        if 'store' in parse_string or '4k' in parse_string or 'stremthru' in parse_string:
            name = f"★ {name}"
        if '⏳' in name or not hints.get('isCached', False):
            s['name'] = f"[dim]{name} (Unverified ⏳)[/dim]"
        else:
            s['name'] = name
        logging.debug(f"Formatted stream {i}: final name='{s['name']}'")
    
    logging.info(f"Final filtered: {len(filtered)}")
    
    final_streams = filtered[:60]
    try:
        logging.debug(f"Final streams JSON (sample if large): {json.dumps({'streams': final_streams[:5]}, indent=2)} ... (total {len(final_streams)})")
        if final_streams:
            logging.info(f"Final delivered sample: Name '{final_streams[0].get('name', 'NO NAME')}'")
    except Exception as e:
        logging.error(f"Final JSON log failed: {e} - Streams count {len(final_streams)}; possible large data issue - Mismatch with Stremio if >0")
    
    return jsonify({'streams': final_streams})

if __name__ == '__main__':
    try:
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)
    except Exception as e:
        logging.error(f"App startup error: {e}")
```

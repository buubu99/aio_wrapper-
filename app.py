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
from collections import defaultdict

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, resources={r"/*": {"origins": "*"}})

logging.basicConfig(level='DEBUG', format='%(asctime)s | %(levelname)s | %(message)s')

session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

AIO_BASE = os.environ.get('AIO_URL', 'https://buubuu99-aiostreams.elfhosted.cc/stremio/acc199cb-6b12-4fa9-be4e-a8ff4c13fa50/eyJpIjoiRTJES0N1ZFBaWm8wb25aS05tNEFsUT09IiwiZSI6InhrVVFtdTFEWm5lcGVrcEh5VUZaejZlcEJLMEMrcXdLakY4UU9zUDJoOFE9IiwidCI6ImEifQ')
MIN_SEEDERS = 1
MIN_SIZE_BYTES = 500000000
REQUEST_TIMEOUT = 15
API_POLL_TIMEOUT = 15
POLL_INTERVAL = 2
MAX_POLLS = 20

DEBRID_KEYS = {
    'tb': '1046c773-9d0d-4d53-95ab-8976a559a5f6',  # First (best)
    'rd': 'Z4C3UT777IK2U6EUZ5HLVVMO7BYQPDNEOJUGAFLUBXMGTSX2Z6RA',
    'ad': 'ZXzHvUmLsuuRmgyHg5zjFsk8'  # New key
}

LANGUAGE_FLAGS = {'en': '🇺🇸', 'eng': '🇺🇸', 'ko': '🇰🇷', 'kor': '🇰🇷', 'korean': '🇰🇷'}

QUALITY_PATTERNS = [  # Simple tiers
    {"name": "Remux", "pattern": r"remux", "flags": re.I},
    {"name": "Bluray", "pattern": r"blu[-_]?ray|brrip", "flags": re.I},
    {"name": "Web", "pattern": r"web[-_.]?(dl|rip)", "flags": re.I},
    {"name": "Bad", "pattern": r"yify|psa|pahe|afg|cmrg|ion10|megusta|tgx", "flags": re.I}
]

MOJIBAKE_FIX = {'â°': '⍰', 'â³': '⏳', 'ã': '〈', 'ã': '〉', 'â¤': '▤', 'â§': '◧'}

def clean_string(text):
    for bad, good in MOJIBAKE_FIX.items():
        text = text.replace(bad, good)
    text = re.sub(r'\{.*?\}', '', text, flags=re.I)
    text = text.encode('utf-8', errors='ignore').decode('unicode_escape', errors='ignore')
    return text.lower()

def get_quality_tier(parse_string):
    for pat in QUALITY_PATTERNS:
        if re.search(pat['pattern'], parse_string, pat.get('flags', 0)):
            return pat['name']
    return 'Unknown'

def verify_cached(hashes, service='tb'):  # TB first
    cached = {}
    api_key = DEBRID_KEYS.get(service)
    headers = {'Authorization': f'Bearer {api_key}'}
    base_url = 'https://api.torbox.app/v1/api' if service == 'tb' else 'https://api.real-debrid.com/rest/1.0' if service == 'rd' else 'https://api.alldebrid.com/v4'
    
    try:
        for h in hashes:
            magnet = f'magnet:?xt=urn:btih:{h}'
            add_url = f'{base_url}/torrents/add?magnet={magnet}' if service == 'tb' else f'{base_url}/torrents/addMagnet' if service == 'rd' else f'{base_url}/magnet/upload?magnets[]={magnet}'
            response = session.post(add_url, headers=headers, timeout=API_POLL_TIMEOUT)
            if response.status_code != 200:
                continue
            
            data = response.json()
            torrent_id = data.get('data', {}).get('id') if service == 'tb' else data.get('id') if service == 'rd' else data.get('data', {}).get('magnets', [{}])[0].get('id')
            if not torrent_id:
                continue
            
            for poll in range(1, MAX_POLLS + 1):
                info_url = f'{base_url}/torrents/info/{torrent_id}' if service in ['tb', 'rd'] else f'{base_url}/magnet/status?id={torrent_id}'
                info_resp = session.get(info_url, headers=headers, timeout=API_POLL_TIMEOUT)
                if info_resp.status_code != 200:
                    break
                
                status = info_resp.json().get('status')
                if status in ['ready', 'downloaded']:
                    cached[h] = True
                    break
                time.sleep(POLL_INTERVAL * (1.5 ** poll))
            
            delete_url = f'{base_url}/torrents/delete/{torrent_id}'
            session.delete(delete_url, headers=headers)
        
    except Exception as e:
        logging.error("VERIFY_ERROR: %s | SERVICE: %s", str(e), service)
    
    return cached

@app.route('/stream/<media_type>/<media_id>.json')
def streams(media_type, media_id):
    all_streams = []
    try:
        aio_url = f"{AIO_BASE}/stream/{media_type}/{media_id}.json"
        response = session.get(aio_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        all_streams = response.json().get('streams', [])
    except Exception as e:
        logging.error("AIO_ERROR: %s", str(e))
        return jsonify({'streams': []})

    total_raw = len(all_streams)
    logging.info("RAW_COUNT: %d", total_raw)

    filtered = []
    uncached_hashes = []
    for i, s in enumerate(all_streams):
        parse_string = clean_string(s.get('name', '') + ' ' + s.get('description', ''))
        hints = s.get('behaviorHints', {})
        
        quality = get_quality_tier(parse_string)
        if quality == 'Bad':
            logging.debug("DISCARD_BAD | ID: %d | QUALITY: %s", i, quality)
            continue
        
        seeders = int(re.search(r'seeders? ?(\d+)', parse_string, re.I).group(1) or hints.get('seeders', 0) or 0)
        size = hints.get('videoSize', 0) or (float(re.search(r'(\d+\.?\d*) ?(gb|mb)', parse_string, re.I).group(1) or 0) * (1024**3 if 'gb' else 1024**2))
        res = re.search(r'(4k|2160p|1080p|720p)', parse_string, re.I).group(0).upper() if re.search else hints.get('resolution', 'Unknown').upper() if hints.get('resolution') else 'Unknown'
        
        if size < MIN_SIZE_BYTES or (res in ['SD', 'UNKNOWN'] and any(r in ['4K', '1080P', '720P'] for r in [st.get('resolution', '') for st in all_streams])):
            logging.debug("DISCARD_LOW_SIZE_RES | ID: %d | SIZE: %d | RES: %s", i, size, res)
            continue
        if seeders < MIN_SEEDERS:
            logging.debug("DISCARD_LOW_SEED | ID: %d | SEED: %d", i, seeders)
            continue
        
        is_cached = hints.get('isCached', '⚡' in parse_string)
        if not is_cached:
            hash_match = re.search(r'[a-fA-F0-9]{40}', s.get('url', ''), re.I)
            if hash_match:
                uncached_hashes.append(hash_match.group(0).upper())
        
        filtered.append(s)
        s['quality'] = quality

    # Verify uncached (TB first)
    verified = verify_cached(uncached_hashes, 'tb')
    for s in all_streams:
        url = s.get('url', '')
        h_match = re.search(r'[a-fA-F0-9]{40}', url, re.I)
        if h_match and h_match.group(0).upper() in verified and s not in filtered:
            filtered.append(s)
            logging.debug("ADD_VERIFIED | HASH: %s", h_match.group(0))

    kept = len(filtered)
    logging.info("FILTERED_COUNT: %d", kept)

    # Sorting: Provider first, quality high
    def sort_key(s):
        parse_string = (s.get('name', '') + ' ' + s.get('description', '')).lower()
        provider_p = 0 if 'tb' in parse_string or 'torbox' in parse_string else 1 if 'rd' in parse_string or 'realdebrid' in parse_string else 2 if 'ad' in parse_string or 'alldebrid' in parse_string else 3
        quality_p = 0 if s.get('quality') == 'Remux' else 1 if 'Bluray' else 2 if 'Web' else 3
        return (provider_p, quality_p)

    filtered.sort(key=sort_key)

    # Formatting: Pass AIO's, add flags
    for s in filtered:
        desc = s.get('description', '').lower()
        lang_match = re.search(r'(ko|kor|korean)', desc, re.I)
        if lang_match and '🇰🇷' not in s.get('name', ''):
            s['name'] = s.get('name', '') + ' 🇰🇷'
            logging.debug("ADD_FLAG | LANG: %s", lang_match.group(0))

    delivered = len(filtered)
    logging.info("DELIVERED_COUNT: %d | FINAL", delivered)

    return jsonify({'streams': filtered})

@app.route('/health')
def health():
    logging.info("HEALTH_CHECK: OK")
    return "OK", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

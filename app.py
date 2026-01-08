import requests
from datetime import datetime, timedelta, timezone
import os
import logging
import re
import json
import unicodedata
import time
from flask import Flask, request, jsonify
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import Counter, defaultdict
from fuzzywuzzy import fuzz

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
USE_STORE = os.environ.get('USE_STORE', 'true').lower() == 'true'
MIN_SEEDERS = int(os.environ.get('MIN_SEEDERS', 1))  # Tighten
MIN_SIZE_BYTES = int(os.environ.get('MIN_SIZE_BYTES', 500000000))
MAX_SIZE_BYTES = int(os.environ.get('MAX_SIZE_BYTES', 10000000000))  # Uncached cap
REQUEST_TIMEOUT = int(os.environ.get('TIMEOUT', 60000)) / 1000
MAX_UNCACHED_KEEP = 20  # Increased
API_TIMEOUT = 10
POLL_INTERVAL = 3  # Increased
MAX_POLLS = 15  # Increased
USE_PING_FALLBACK = 'true'
PING_TIMEOUT = 5

RD_API_KEY = 'Z4C3UT777IK2U6EUZ5HLVVMO7BYQPDNEOJUGAFLUBXMGTSX2Z6RA'
TB_API_KEY = '1046c773-9d0d-4d53-95ab-8976a559a5f6'
AD_API_KEY = 'Z2HvUmLsuuRmyHg5Zf5K8'

LANGUAGE_FLAGS = { ... }  # Extended as before
LANGUAGE_TEXT_FALLBACK = { ... }
SERVICE_COLORS = { ... }
PREFERRED_LANGS = ['en', 'eng', 'ko', 'kor', 'kr']  # Strict

def debrid_check_cache(hashes, service='rd'):
    results = {}
    hashes = list(set(hashes))  # Dedup
    if service == 'rd':
        headers = {'Authorization': f'Bearer {RD_API_KEY}'}
        magnets = [f'magnet:?xt=urn:btih:{h.upper()}' for h in hashes]
        add_url = 'https://api.real-debrid.com/rest/1.0/torrents/addMagnet'
        try:
            add_resp = session.post(add_url, headers=headers, data={'magnet': '\n'.join(magnets)}, timeout=API_TIMEOUT)
            if add_resp.status_code not in [200, 201]:
                return results
            added = add_resp.json().get('files', []) if isinstance(add_resp.json().get('files'), list) else []
            ids = [a.get('id') for a in added if a.get('id')]
            for poll in range(1, MAX_POLLS + 1):
                if not ids:
                    break
                info_url = 'https://api.real-debrid.com/rest/1.0/torrents/info'
                info_data = {'ids': ','.join(map(str, ids))}
                info_resp = session.post(info_url, headers=headers, data=info_data, timeout=API_TIMEOUT)
                if info_resp.status_code == 200:
                    infos = info_resp.json() if isinstance(info_resp.json(), list) else [info_resp.json()]
                    for info in infos:
                        h = info.get('hash', '').upper()
                        if h and (info.get('progress', 0) == 100 or info.get('status', '') in ['downloaded', 'queued']):
                            results[h] = True
                        elif h:
                            results[h] = False
                time.sleep(POLL_INTERVAL * (1.5 ** poll))
            for tid in ids:
                del_url = f'https://api.real-debrid.com/rest/1.0/torrents/delete/{tid}'
                session.delete(del_url, headers=headers)
        except Exception as e:
            logging.error(f"RD error: {e}")
    # TB single loop
    if service == 'tb':
        headers = {'Authorization': f'Bearer {TB_API_KEY}'}
        for h in hashes:
            check_url = f'https://api.torbox.app/v1/api/torrents/checkcache?hash={h.upper()}'
            try:
                resp = session.get(check_url, headers=headers, timeout=API_TIMEOUT)
                if resp.status_code == 200 and resp.json().get('data', {}).get('cached', False):
                    results[h.upper()] = True
                else:
                    results[h.upper()] = False
            except:
                results[h.upper()] = False
    # AD batch
    if service == 'ad':
        headers = {'Authorization': f'Bearer {AD_API_KEY}'}
        add_url = 'https://api.alldebrid.com/v4/magnet/upload'
        params = {'magnets[]': [f'magnet:?xt=urn:btih:{h.upper()}' for h in hashes]}
        try:
            add_resp = session.get(add_url, headers=headers, params=params, timeout=API_TIMEOUT)
            if add_resp.status_code == 200:
                magnets = add_resp.json().get('data', {}).get('magnets', [])
                ids = [m.get('id') for m in magnets if m.get('id')]
                for poll in range(1, MAX_POLLS + 1):
                    status_url = 'https://api.alldebrid.com/v4/magnet/status'
                    for tid in ids:
                        s_params = {'id': tid}
                        s_resp = session.get(status_url, headers=headers, params=s_params, timeout=API_TIMEOUT)
                        if s_resp.status_code == 200:
                            info = s_resp.json().get('data', {}).get('magnets', {})
                            h = info.get('hash', '').upper()
                            if h and info.get('statusCode') in [3, 4]:
                                results[h] = True
                            elif h:
                                results[h] = False
                    time.sleep(POLL_INTERVAL * (1.5 ** poll))
                for tid in ids:
                    del_url = 'https://api.alldebrid.com/v4/magnet/delete'
                    session.get(del_url, headers=headers, params={'id': tid})
        except Exception as e:
            logging.error(f"AD error: {e}")
    return results

@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.33",
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
    logging.debug(f"Starting for {media_type}/{media_id}")
    all_streams = []
    title_query = 'wailing goksung'  # For fuzz, include variants
    try:
        aio_url = f"{AIO_BASE}/stream/{media_type}/{media_id}.json"
        aio_resp = session.get(aio_url, timeout=REQUEST_TIMEOUT)
        aio_resp.raise_for_status()
        all_streams += aio_resp.json().get('streams', [])
    except Exception as e:
        logging.error(f"AIO error: {e}")

    global USE_STORE  # Allow disable
    if USE_STORE:
        try:
            store_url = f"{STORE_BASE}/stream/{media_type}/{media_id}.json"
            store_resp = session.get(store_url, timeout=REQUEST_TIMEOUT)
            if store_resp.status_code == 401:
                logging.warning("Store 401 - Disabling Store")
                USE_STORE = False
            else:
                store_resp.raise_for_status()
                all_streams += store_resp.json().get('streams', [])
        except Exception as e:
            logging.error(f"Store error: {e}")

    # Dedup by hash(URL + size + ext.lower())
    seen = set()
    deduped = []
    for s in all_streams:
        url = s.get('url', '')
        size = s.get('behaviorHints', {}).get('videoSize', 0)
        fn = s.get('behaviorHints', {}).get('filename', '')
        ext = os.path.splitext(fn)[1].lower() if fn else ''
        h = hash((url, size, ext))
        if h not in seen:
            seen.add(h)
            deduped.append(s)
    all_streams = deduped

    # Full unicode denormalize
    for s in all_streams:
        for key in ['name', 'description']:
            if key in s:
                s[key] = unicodedata.normalize('NFKD', s[key]).encode('ascii', 'ignore').decode('utf-8').replace('\\u', '')

    # Filter relevance/lang
    filtered = []
    for s in all_streams:
        full_text = (s.get('name', '') + ' ' + s.get('description', '') + ' ' + s.get('behaviorHints', {}).get('filename', '')).lower()
        if fuzz.partial_ratio(title_query, full_text) < 70:
            continue  # Mismatch
        langs = re.findall(r'[a-z]{2,3}', full_text, re.I)
        if any(l.lower() in PREFERRED_LANGS for l in langs):
            filtered.append(s)
        # Exclude others unless multi-lang

    # Batch uncached verification
    uncached = [s for s in filtered if not s.get('behaviorHints', {}).get('isCached', False)]
    hashes = list(set(re.search(r'([a-fA-F0-9]{40})', s.get('url', ''), re.I).group(1) for s in uncached if re.search(r'([a-fA-F0-9]{40})', s.get('url', ''), re.I)))
    cache_results = {}
    for serv in ['rd', 'tb', 'ad']:
        cache_results.update(debrid_check_cache(hashes, serv))
    verified_uncached = []
    for s in uncached:
        h_match = re.search(r'([a-fA-F0-9]{40})', s.get('url', ''), re.I)
        if h_match and cache_results.get(h_match.group(1).upper(), False):
            s['behaviorHints']['isCached'] = True
            s['name'] = s['name'].replace('⏳', '').strip()
            if s.get('behaviorHints', {}).get('videoSize', 0) <= MAX_SIZE_BYTES:
                verified_uncached.append(s)
        else:
            if USE_PING_FALLBACK:
                try:
                    head = session.head(s.get('url', ''), timeout=PING_TIMEOUT)
                    if head.status_code == 200:
                        s['behaviorHints']['isCached'] = True
                        s['name'] = s['name'].replace('⏳', '').strip()
                        verified_uncached.append(s)
                except:
                    pass
    filtered = [s for s in filtered if s.get('behaviorHints', {}).get('isCached', False)] + verified_uncached[:MAX_UNCACHED_KEEP]

    # Improved template fallback from desc
    for s in filtered:
        name = s.get('name', '')
        desc = s.get('description', '')
        if re.search(r'\{stream\..*::', name):
            qual_match = re.search(r'(Web-dl|Webrip|Bluray|Bdremux|Remux|Hdrip|Tc|Ts|Cam|Dvdrip|Hdtv)', desc + name, re.I)
            res_match = re.search(r'(4k|2160p|1440p|1080p|720p)', desc + name, re.I)
            s['name'] = f"{res_match.group(1).upper() if res_match else '⍰'} {qual_match.group(1).title() if qual_match else 'Unknown'}"

    # Formatting (add flags/audio/colors)
    for s in filtered:
        full_text = (s.get('name', '') + ' ' + s.get('description', '') + ' ' + s.get('behaviorHints', {}).get('filename', '')).lower()
        service = next((k for k in SERVICE_COLORS if k in full_text), '')
        if service:
            s['name'] = f"{SERVICE_COLORS[service]}{s['name']}[/{service}]"
        langs = re.findall(r'[a-z]{2,3}', full_text, re.I)
        flags = ' '.join(set(LANGUAGE_FLAGS.get(l.lower(), LANGUAGE_TEXT_FALLBACK.get(l.lower(), '')) for l in langs))
        if flags:
            s['name'] += f" {flags}"
        audio_match = re.search(r'(dd\+|dd|dts-hd|dts|opus|aac|atmos|ma|5\.1|7\.1|2\.0)', full_text, re.I)
        if audio_match:
            s['name'] += f" ♬ {audio_match.group(0).upper()}"

    filtered.sort(key=sort_key)
    return jsonify({'streams': filtered[:60]})

def sort_key(s):
    full_text = (s.get('name', '') + ' ' + s.get('description', '') + ' ' + s.get('behaviorHints', {}).get('filename', '')).lower()
    hints = s.get('behaviorHints', {})
    is_cached = hints.get('isCached', False)
    cached_pri = 0 if is_cached else 1
    completion_pri = 0 if is_cached or 'complete' in full_text or '100%' in full_text else 1
    res_pri = {'4k': 0, '2160p': 0, '1440p': 1, '1080p': 2, '720p': 3}.get(next((r.lower() for r in ['4k', '2160p', '1440p', '1080p', '720p'] if r.lower() in full_text), ''), 4)
    qual_pri = {'bdremux': 0, 'remux': 0, 'bluray': 1, 'web-dl': 2, 'webrip': 3, 'web': 3, 'hdtv': 4, 'ts': 5}.get(next((q.lower() for q in ['bdremux', 'remux', 'bluray', 'web-dl', 'webrip', 'web', 'hdtv', 'ts'] if q.lower() in full_text), ''), 4)
    lang_pri = 0 if any(l in full_text for l in PREFERRED_LANGS) else 1
    size_num = hints.get('videoSize', 0) / 10**9
    seed_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', full_text, re.I | re.U)
    seeders = int(seed_match.group(1) or seed_match.group(2) or 0) if seed_match else 0
    seadex_pri = 0 if 'seadex' in full_text or 'best release' in full_text else 1
    source_pri = 0 if 'store' in full_text or 'stremthru' in full_text else (1 if 'rd' in full_text else (2 if 'tb' in full_text else 3))
    return (cached_pri, completion_pri, res_pri, qual_pri, seadex_pri, source_pri, lang_pri, -size_num, -seeders)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

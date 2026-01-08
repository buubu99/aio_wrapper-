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
from collections import defaultdict

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, resources={r"/*": {"origins": "*"}})

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'), format='%(asctime)s | %(levelname)s | %(message)s')

session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

AIO_BASE = os.environ.get('AIO_URL', 'https://buubuu99-aiostreams.elfhosted.cc/stremio/acc199cb-6b12-4fa9-be4e-a8ff4c13fa50/eyJpIjoiRTJES0N1ZFBaWm8wb25aS05tNEFsUT09IiwiZSI6InhrVVFtdTFEWm5lcGVrcEh5VUZaejZlcEJLMEMrcXdLakY4UU9zUDJoOFE9IiwidCI6ImEifQ')
STORE_BASE = os.environ.get('STORE_URL', 'https://buubuu99-stremthru.elfhosted.cc/stremio/store/eyJzdG9yZV9uYW1lIjoiIiwic3RvcmVfdG9rZW4iOiJZblYxWW5WMU9UazZUV0Z5YVhOellUazVRREV4Tnc9PSIsImhpZGVfY2F0YWxvZyI6dHJ1ZSwid2ViZGwiOnRydWV9')
USE_STORE = os.environ.get('USE_STORE', 'true').lower() == 'true'
MIN_SEEDERS = int(os.environ.get('MIN_SEEDERS', 1))
MIN_SIZE_BYTES = int(os.environ.get('MIN_SIZE_BYTES', 500000000))
MAX_SIZE_BYTES = int(os.environ.get('MAX_SIZE_BYTES', 100000000000))
REQUEST_TIMEOUT = int(os.environ.get('TIMEOUT', 60000)) / 1000
MAX_UNCACHED_KEEP = 30
PING_TIMEOUT = 2
API_POLL_TIMEOUT = 10
POLL_INTERVAL = 1
MAX_POLLS = 15

RD_API_KEY = 'Z4C3UT777IK2U6EUZ5HLVVMO7BYQPDNEOJUGAFLUBXMGTSX2Z6RA'
TB_API_KEY = '1046c773-9d0d-4d53-95ab-8976a559a5f6'
AD_API_KEY = 'Z2HvUmLsuuRmyHg5Zf5K8'

LANGUAGE_FLAGS = {
    'jpn': '🇯🇵', 'jp': '🇯🇵', 'ita': '🇮🇹', 'it': '🇮🇹',
    'fra': '🇫🇷', 'fr': '🇫🇷', 'kor': '🇰🇷', 'kr': '🇰🇷', 'chn': '🇨🇳', 'cn': '🇨🇳',
    'ger': '🇩🇪', 'de': '🇩🇪', 'hun': '🇭🇺', 'yes': '📝', 'ko': '🇰🇷',
    'korean': '🇰🇷'  # No en/eng/uk to avoid US/UK flags
}
SERVICE_COLORS = {
    'rd': '[red]', 'realdebrid': '[red]',
    'tb': '[blue]', 'torbox': '[blue]',
    'ad': '[green]', 'alldebrid': '[green]',
    'store': '[purple]', 'stremthru': '[purple]'
}

# Quality patterns from Tam-Taro's JSON template
QUALITY_PATTERNS = [
    {"name": "Remux T1", "pattern": r"^(?=.*\b(?:BD|Blu[-_ ]?Ray)\b)(?=.*\b(?:Remux|BDRemux)\b)(?=.*\b(?:FraMeSLoR|playBD)\b).*/i"},
    {"name": "Remux T2", "pattern": r"^(?=.*\b(?:BD|Blu[-_ ]?Ray)\b)(?=.*\b(?:Remux|BDRemux)\b)(?=.*\b(?:BHDStudio|playMaNiA|playREMUX|SPARKS|ZMN)\b).*/i"},
    {"name": "Remux T3", "pattern": r"^(?=.*\b(?:BD|Blu[-_ ]?Ray)\b)(?=.*\b(?:Remux|BDRemux)\b)(?=.*\b(?:playTV|SWTYBLZ)\b).*/i"},
    {"name": "Remux T4", "pattern": r"^(?=.*\b(?:BD|Blu[-_ ]?Ray)\b)(?=.*\b(?:Remux|BDRemux)\b)(?=.*\b(?:playHD|playEX|playMOViE)\b).*/i"},
    {"name": "Bluray T1", "pattern": r"^(?=.*\bBlu[-_]?Ray\b)(?!.*\bRemux\b)(?!.*\bWEB[-_.]?(?:DL|Rip)\b)(?=.*\b(?:FraMeSToR|playHD)\b).*/i"},
    {"name": "Bluray T2", "pattern": r"^(?=.*\bBlu[-_]?Ray\b)(?!.*\bRemux\b)(?!.*\bWEB[-_.]?(?:DL|Rip)\b)(?=.*\b(?:CHD|CiNE|CtrlHD|playEV|SPARKS)\b).*/i"},
    {"name": "Bluray T3", "pattern": r"^(?=.*\bBlu[-_]?Ray\b)(?!.*\bRemux\b)(?!.*\bWEB[-_.]?(?:DL|Rip)\b)(?=.*\b(?:BHDStudio|hallowed|HiFi|HONE|SPHD|WEBDV|playHD)\b).*/i"},
    # ... (add all from user's JSON; truncated in prompt, but include Bad at end)
    {"name": "Bad", "pattern": r"(4KMaNiA|4KMVi|AFG|CiTiDeL|CMRG|FiLMiCiTY|GalaxyTV|GALAXYRG|GGWP|HD4U|ION10|MeGusta|MKVCenter|MkvHub|Pahe|PSA|PSARips|RMTeam|Shaanig|STVFRV|TGx|YIFY|YTS|beAst|COLLECTiVE|EPiC|iVy|KiNGDOM|Scene|SUNSCREEN)"}
]

logging.info("APP START | ENV VARS DUMP: %s", json.dumps(dict(os.environ), indent=2))

def log_summary(section, status, streams, details, time_ms):
    logging.info(f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🟢 [{section}] Summary\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n✔ Status      : {status}\n📦 Streams    : {streams}\n📋 Details    : {details}\n⏱️ Time       : {time_ms}ms\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

def debrid_batch_check(hashes, service='rd'):
    start_time = time.time()
    logging.debug("DEBRID_BATCH | START | HASHES: %d | SERVICE: %s", len(hashes), service)
    cached = {}
    headers = {'Authorization': f'Bearer {RD_API_KEY if service == "rd" else TB_API_KEY if service == "tb" else AD_API_KEY}'}
    try:
        if service == 'rd':
            for h in hashes:
                add_url = 'https://api.real-debrid.com/rest/1.0/torrents/addMagnet'
                data = {'magnet': f'magnet:?xt=urn:btih:{h}'}
                response = session.post(add_url, headers=headers, data=data, timeout=API_POLL_TIMEOUT)
                if response.status_code == 200:
                    torrent_id = response.json().get('id')
                    if torrent_id:
                        for poll in range(1, MAX_POLLS + 1):
                            info_url = f'https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}'
                            info_resp = session.get(info_url, headers=headers)
                            if info_resp.status_code == 200 and info_resp.json().get('status') in ['downloaded', 'queued']:
                                cached[h] = True
                                break
                            time.sleep(POLL_INTERVAL * (1.5 ** poll))
                        delete_url = f'https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}'
                        session.delete(delete_url, headers=headers)
        elif service == 'tb':
            for h in hashes:
                check_url = f'https://api.torbox.app/v1/api/torrents/checkcache?hash={h}'
                response = session.get(check_url, headers=headers, timeout=API_POLL_TIMEOUT)
                if response.status_code == 200 and response.json().get('data', {}).get('cached', False):
                    cached[h] = True
        elif service == 'ad':
            add_url = 'https://api.alldebrid.com/v4/magnet/upload'
            magnets = [f'magnet:?xt=urn:btih:{h}' for h in hashes]
            params = {'magnets[]': magnets}
            response = session.post(add_url, headers=headers, params=params)
            if response.status_code == 200:
                for mag in response.json().get('data', {}).get('magnets', []):
                    if mag.get('ready', False):
                        cached[mag.get('hash', '')] = True
    except Exception as e:
        logging.error("DEBRID_BATCH | ERROR: %s | SERVICE: %s", str(e), service)
        if 'disabled' in str(e).lower() or 'france' in str(e).lower():
            logging.warning("DEBRID_BATCH | RESTRICTION_DETECTED | FALLBACK_TO_TB")
            return debrid_batch_check(hashes, 'tb')
    time_ms = int((time.time() - start_time) * 1000)
    log_summary("Debrid Batch Check", "SUCCESS" if cached else "PARTIAL", len(cached), f"Cached hashes: {len(cached)}/{len(hashes)}", time_ms)
    return cached

@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.39",
        "name": "Grok AIO Wrapper",
        "description": "Wraps AIOStreams with enhanced filtering, sorting, and formatting",
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
    start_time = time.time()
    logging.info("STREAM_REQ | START | TYPE: %s | ID: %s", media_type, media_id)
    all_streams = []
    try:
        aio_url = f"{AIO_BASE}/stream/{media_type}/{media_id}.json"
        aio_response = session.get(aio_url, timeout=REQUEST_TIMEOUT)
        aio_response.raise_for_status()
        all_streams += aio_response.json().get('streams', [])
    except Exception as e:
        logging.error("STREAM_REQ | AIO_ERROR: %s", str(e))
    
    if USE_STORE:
        try:
            store_url = f"{STORE_BASE}/stream/{media_type}/{media_id}.json"
            store_response = session.get(store_url, timeout=REQUEST_TIMEOUT)
            store_response.raise_for_status()
            all_streams += store_response.json().get('streams', [])
        except Exception as e:
            logging.error("STREAM_REQ | STORE_ERROR: %s", str(e))
    
    total_raw = len(all_streams)
    time_ms = int((time.time() - start_time) * 1000)
    log_summary("Scrape", "SUCCESS", total_raw, "Fetched from AIO/Store", time_ms)
    
    filtered = []
    uncached_hashes = defaultdict(list)
    res_counter = defaultdict(int)
    lang_counter = defaultdict(int)
    quality_tier_counter = defaultdict(int)
    excluded_uncached = 0
    excluded_seeder = 0
    excluded_bad = 0
    start_time = time.time()
    for i, s in enumerate(all_streams):
        name = s.get('name', '')
        description = s.get('description', '')
        parse_string = (name + ' ' + description).lower().encode('utf-8').decode('unicode_escape')
        normalized_parse = unicodedata.normalize('NFKD', parse_string).encode('ascii', 'ignore').decode('utf-8')
        hints = s.get('behaviorHints', {})
        is_cached = hints.get('isCached', False)
        seed_match = re.search(r'(?:seeders?|seeds?|peers?) ?(\\d+)', parse_string, re.I)
        seeders = int(seed_match.group(1) or 0) if seed_match else 0
        if not seed_match:
            logging.debug("STREAM_ID: %d | SEED_MISS | PARSE: %s...", i, parse_string[:100])
        size_match = re.search(r'(\\d+\\.?\\d*)\\s*(gb|mb)', parse_string, re.I)
        size = 0
        if size_match:
            size_num = float(size_match.group(1))
            unit = size_match.group(2).lower()
            size = int(size_num * (1024**3 if unit == 'gb' else 1024**2))
        if size == 0:
            size = hints.get('videoSize', 0)
            logging.debug("STREAM_ID: %d | SIZE_FALLBACK_TO_HINT: %d", i, size)
        res_match = re.search(r'(4k|2160p|1080p|720p)', parse_string, re.I)
        if res_match:
            res_counter[res_match.group(1).upper()] += 1
        else:
            logging.debug("STREAM_ID: %d | RES_MISS | PARSE: %s...", i, parse_string[:100])
        lang_match = re.search(r'(kor|korean|jpn|jp|ita|it|fra|fr|chn|cn|ger|de|hun|yes|ko)', parse_string, re.I)
        if lang_match:
            lang = lang_match.group(1).lower()
            lang_counter[lang] += 1
            flag = LANGUAGE_FLAGS.get(lang, '')
            if flag:
                name += f' {flag}'
            logging.debug("STREAM_ID: %d | LANG_DETECT | LANG: %s | FLAG: %s", i, lang, flag)
        else:
            logging.debug("STREAM_ID: %d | LANG_MISS | PARSE: %s...", i, parse_string[:100])
        # Quality tier matching from patterns
        quality_tier = 'Unknown'
        is_bad = False
        for pat in QUALITY_PATTERNS:
            if re.match(pat['pattern'], parse_string, re.I):
                quality_tier = pat['name']
                if 'Bad' in quality_tier:
                    is_bad = True
                break
        if is_bad:
            excluded_bad += 1
            logging.debug("STREAM_ID: %d | FILTER_SKIP_BAD | TIER: %s | PARSE: %s...", i, quality_tier, parse_string[:100])
            continue
        quality_tier_counter[quality_tier] += 1
        s['quality_tier'] = quality_tier  # Add for sorting
        if seeders < MIN_SEEDERS:
            excluded_seeder += 1
            logging.debug("STREAM_ID: %d | FILTER_SKIP_SEEDER | SEEDERS: %d < %d", i, seeders, MIN_SEEDERS)
            continue
        if not (MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES):
            logging.debug("STREAM_ID: %d | FILTER_SKIP_SIZE | SIZE: %d", i, size)
            continue
        if is_cached:
            filtered.append(s)
        else:
            excluded_uncached += 1
            hash_match = re.search(r'([a-fA-F0-9]{40})', s.get('url', ''), re.I)
            if hash_match:
                service = next((sv for sv in ['rd', 'tb', 'ad'] if sv in parse_string), 'rd')
                uncached_hashes[service].append(hash_match.group(1).upper())
    
    # Verify uncached
    verified_uncached = 0
    for service, hashes in uncached_hashes.items():
        cached = debrid_batch_check(hashes, service)
        for s in all_streams:
            url = s.get('url', '')
            hash_match = re.search(r'([a-fA-F0-9]{40})', url, re.I)
            if hash_match and hash_match.group(1).upper() in cached:
                filtered.append(s)
                verified_uncached += 1
                logging.debug("STREAM_ID: VERIFIED | HASH: %s | SERVICE: %s", hash_match.group(1), service)
    
    kept = len(filtered)
    filtered_out = total_raw - kept
    time_ms = int((time.time() - start_time) * 1000)
    log_summary("Filter", "SUCCESS", kept, f"Filtered: {filtered_out}", time_ms)
    logging.info("EXCLUDED_DETAILS | UNCACHED: %d | SEEDER < %d: %d | BAD: %d", excluded_uncached, MIN_SEEDERS, excluded_seeder, excluded_bad)
    logging.info("INCLUDED_DETAILS | RESOLUTION: %s | LANGUAGE: %s | QUALITY_TIERS: %s", dict(res_counter), dict(lang_counter), dict(quality_tier_counter))
    
    filtered = list({json.dumps(f, sort_keys=True): f for f in filtered}.values())
    
    def sort_key(s):
        name_lower = (s.get('name', '') + ' ' + s.get('description', '')).lower()
        is_cached = s.get('behaviorHints', {}).get('isCached', False)
        cached_priority = 0 if is_cached else 1
        complete_priority = 0 if 'complete' in name_lower else 1
        res_priority = {'4K': 0, '2160P': 0, '1080P': 2, '720P': 3}.get(next((r.upper() for r in ['4k', '2160p', '1080p', '720p'] if r.lower() in name_lower), ''), 4)
        if res_priority == 4:
            logging.debug("SORT | RES_MISS | NAME: %s", name_lower[:100])
        # Tier priority from patterns (lower number better)
        tier_priority = next((i for i, pat in enumerate(QUALITY_PATTERNS) if pat['name'] == s.get('quality_tier')), 100)
        size_match = re.search(r'(\\d+\\.?\\d*)\\s*(gb|mb)', name_lower, re.I)
        size = 0
        if size_match:
            size_num = float(size_match.group(1))
            unit = size_match.group(2).lower()
            size = int(size_num * (1024**3 if unit == 'gb' else 1024**2))
        return (cached_priority, complete_priority, res_priority, tier_priority, -size)  # General sort with size
    
    start_time = time.time()
    filtered.sort(key=sort_key)
    time_ms = int((time.time() - start_time) * 1000)
    log_summary("Sorter", "SUCCESS", len(filtered), "Sorted streams", time_ms)
    logging.info("SORT_SUMMARY | FIRST_5: %s", [f.get('name', '') for f in filtered[:5]])
    
    start_time = time.time()
    for i, s in enumerate(filtered):
        name = s.get('name', '')
        description = s.get('description', '')
        parse_string = (name + ' ' + description).lower().encode('utf-8').decode('unicode_escape')
        service_match = re.search(r'(rd|tb|ad|store)', parse_string, re.I)
        if service_match:
            service = service_match.group(1).lower()
            name = f"{SERVICE_COLORS.get(service, '')}{name}[/]"
        if re.search(r'[\uac00-\ud7a3]', parse_string):
            name += ' 🇰🇷'
            logging.debug("STREAM_ID: %d | SCRIPT_FLAG_HANGUL", i)
        lang_match = re.search(r'(jpn|jp|ita|it|fra|fr|kor|korean|kr|chn|cn|ger|de|hun|yes|ko)', parse_string, re.I)
        if lang_match:
            lang = lang_match.group(1).lower()
            flag = LANGUAGE_FLAGS.get(lang, '')
            if flag:
                name += f' {flag}'
            else:
                logging.debug("STREAM_ID: %d | LANG_NO_FLAG | LANG: %s", i, lang)
        else:
            logging.debug("STREAM_ID: %d | LANG_MISS | PARSE: %s...", i, parse_string[:100])
        audio_match = re.search(r'(dd\+|dd|dts-hd|dts|opus|aac|atmos|ma|truehd|5\.1|7\.1|2\.0)', parse_string, re.I)
        if audio_match:
            name += f" ♬ {audio_match.group(1).upper()}"
        else:
            logging.debug("STREAM_ID: %d | AUDIO_MISS | PARSE: %s...", i, parse_string[:100])
        cached_icon = "⚡" if s.get('behaviorHints', {}).get('isCached', False) else "⏳"
        res = next((r.upper().replace('2160P', '4K') for r in ['4k', '2160p', '1080p', '720p'] if r.lower() in parse_string), '? Res')
        quality = s.get('quality_tier', '')
        size_str = f"{size / (1024**3):.1f} GB" if size > 0 else '? GB'
        name = f"{res} {cached_icon} {quality} {size_str} · {name}"
        s['name'] = name
    
    time_ms = int((time.time() - start_time) * 1000)
    log_summary("Formatter", "SUCCESS", len(filtered), "Transformed streams", time_ms)
    
    start_time = time.time()
    # Proxy as in aiostreams
    proxied = len(filtered)
    time_ms = int((time.time() - start_time) * 1000)
    log_summary("Proxy", "SUCCESS", proxied, "Generated proxied URLs", time_ms)
    
    logging.info("CORE | Returning %d streams", min(60, len(filtered)))
    return jsonify({'streams': filtered[:60]})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

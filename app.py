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
USE_STORE = os.environ.get('USE_STORE', 'true').lower() == 'true'

MIN_SEEDERS = int(os.environ.get('MIN_SEEDERS', 0))
MIN_SIZE_BYTES = int(os.environ.get('MIN_SIZE_BYTES', 500000000))
MAX_SIZE_BYTES = int(os.environ.get('MAX_SIZE_BYTES', 100000000000))
REQUEST_TIMEOUT = int(os.environ.get('TIMEOUT', 60000)) / 1000
MAX_UNCACHED_KEEP = 5
PING_TIMEOUT = 2
API_POLL_TIMEOUT = 10
RD_API_KEY = 'Z4C3UT777IK2U6EUZ5HLVVMO7BYQPDNEOJUGAFLUBXMGTSX2Z6RA'
TB_API_KEY = '1046c773-9d0d-4d53-95ab-8976a559a5f6'
AD_API_KEY = 'ZXzHvUmLsuuRmgyHg5zjFsk8'  # Updated to v2's key

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
    torrent_hash = hash_match.group(1).upper()
    if service == 'rd':
        headers = {'Authorization': f'Bearer {RD_API_KEY}'}
        add_url = 'https://api.real-debrid.com/rest/1.0/torrents/addMagnet'
        data = {'magnet': f'magnet:?xt=urn:btih:{torrent_hash}'}
        response = session.post(add_url, headers=headers, data=data)
        if response.status_code != 200:
            logging.debug(f"RD add magnet failed: {response.text}")
            return False
        torrent_id = response.json().get('id')
        if not torrent_id:
            return False
        start_time = time.time()
        while time.time() - start_time < API_POLL_TIMEOUT:
            info_url = f'https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}'
            info_resp = session.get(info_url, headers=headers)
            if info_resp.status_code == 200:
                info = info_resp.json()
                if info.get('progress') == 100 or 'filename' in info:
                    logging.debug(f"RD API confirmed cached for hash {torrent_hash}")
                    delete_url = f'https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}'
                    session.delete(delete_url, headers=headers)
                    return True
            time.sleep(1)
        delete_url = f'https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}'
        session.delete(delete_url, headers=headers)
        logging.debug(f"RD API timeout/not cached for hash {torrent_hash}")
        return False
    elif service == 'tb':
        headers = {'Authorization': f'Bearer {TB_API_KEY}'}
        add_url = 'https://api.torbox.app/v1/api/torrents/async_create_torrent'
        data = {'magnet': f'magnet:?xt=urn:btih:{torrent_hash}'}
        response = session.post(add_url, headers=headers, json=data)
        if response.status_code != 200:
            logging.debug(f"TB add failed: {response.text}")
            return False
        torrent_id = response.json().get('id')
        if not torrent_id:
            return False
        start_time = time.time()
        while time.time() - start_time < API_POLL_TIMEOUT:
            info_url = f'https://api.torbox.app/v1/api/torrents/{torrent_id}'
            info_resp = session.get(info_url, headers=headers)
            if info_resp.status_code == 200:
                info = info_resp.json()
                if info.get('status') == 'finished':
                    logging.debug(f"TB API confirmed cached for hash {torrent_hash}")
                    delete_url = f'https://api.torbox.app/v1/api/torrents/{torrent_id}'
                    session.delete(delete_url, headers=headers)
                    return True
            time.sleep(1)
        delete_url = f'https://api.torbox.app/v1/api/torrents/{torrent_id}'
        session.delete(delete_url, headers=headers)
        logging.debug(f"TB API timeout/not cached for hash {torrent_hash}")
        return False
    elif service == 'ad':
        headers = {'Authorization': f'Bearer {AD_API_KEY}'}
        add_url = 'https://api.alldebrid.com/v4/magnet/upload'
        params = {'magnets[]': f'magnet:?xt=urn:btih:{torrent_hash}'}
        response = session.post(add_url, headers=headers, params=params)
        if response.status_code != 200:
            logging.debug(f"AD add magnet failed: {response.text}")
            return False
        magnets = response.json().get('data', {}).get('magnets', [])
        if not magnets:
            return False
        torrent_id = magnets[0].get('id')
        if not torrent_id:
            return False
        start_time = time.time()
        while time.time() - start_time < API_POLL_TIMEOUT:
            info_url = 'https://api.alldebrid.com/v4.1/magnet/status'  # Updated to v4.1
            params = {'id': torrent_id}
            info_resp = session.post(info_url, headers=headers, params=params)
            if info_resp.status_code == 200:
                info = info_resp.json().get('data', {}).get('magnets', [{}])[0]
                if info.get('statusCode') == 4:  # 4 = ready/completed
                    logging.debug(f"AD API confirmed cached for hash {torrent_hash}")
                    delete_url = 'https://api.alldebrid.com/v4/magnet/delete'
                    data = {'id': torrent_id}  # Changed to data for POST body
                    session.post(delete_url, headers=headers, data=data)
                    return True
            time.sleep(1)
        delete_url = 'https://api.alldebrid.com/v4/magnet/delete'
        data = {'id': torrent_id}  # Changed to data for POST body
        session.post(delete_url, headers=headers, data=data)
        logging.debug(f"AD API timeout/not cached for hash {torrent_hash}")
        return False
    return False

@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.46",  # Updated to 1.0.46
        "name": "AIO Wrapper",
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
    logging.info("Runtime check: Debug level active? %s", logging.getLogger().isEnabledFor(logging.DEBUG))
    logging.debug(f"Starting stream processing for {media_type}/{media_id}")
    all_streams = []
    template_count = 0
    uncached_count = 0
    seed_miss_count = 0  # Initialized
    res_miss_count = 0  # Initialized
    try:
        aio_url = f"{AIO_BASE}/stream/{media_type}/{media_id}.json"
        logging.debug(f"Fetching AIO: {aio_url}")
        aio_response = session.get(aio_url, timeout=REQUEST_TIMEOUT)
        aio_response.raise_for_status()
        aio_data = aio_response.json()
        aio_streams = aio_data.get('streams', [])
        logging.debug(f"Full raw AIO response: {json.dumps(aio_data, indent=2)}")
        all_streams += aio_streams
        logging.info(f"AIO fetch success: {len(aio_streams)} streams")
        
        # New: Data quality check
        template_ratio = sum(1 for s in aio_streams if re.search(r'\{stream\..*::', s.get('name', ''))) / len(aio_streams) if aio_streams else 0
        unicode_issues = sum(1 for s in aio_streams if unicodedata.normalize('NFKD', (s.get('name', '') + s.get('description', '')).lower()).encode('ascii', 'ignore').decode('utf-8') != (s.get('name', '') + s.get('description', '')).lower())
        logging.info(f"AIO data quality: Templates {template_ratio*100:.1f}% | Unicode issues {unicode_issues}/{len(aio_streams)} - if high, expect filter drops/mismatches")
    except Exception as e:
        logging.error(f"AIO fetch error: {e}")
    
    if USE_STORE:
        try:
            store_url = f"{STORE_BASE}/stream/{media_type}/{media_id}.json"
            logging.debug(f"Fetching Store: {store_url}")
            store_response = session.get(store_url, timeout=REQUEST_TIMEOUT)
            store_response.raise_for_status()
            store_data = store_response.json()
            store_streams = store_data.get('streams', [])
            logging.debug(f"Full raw Store response: {json.dumps(store_data, indent=2)}")
            all_streams += store_streams
            logging.info(f"Store fetch success: {len(store_streams)} streams")
        except Exception as e:
            logging.error(f"Store fetch error: {e}")
    
    logging.info(f"Total received streams: {len(all_streams)}")
    logging.debug(f"All raw streams before processing: {json.dumps(all_streams, indent=2)}")
    
    for i, s in enumerate(all_streams):
        name = s.get('name', '')
        description = s.get('description', '')
        hints = s.get('behaviorHints', {})
        source = 'AIO' if i < len(aio_streams) else 'Store'
        logging.debug(f"Processing stream {i} from {source}: raw dict = {json.dumps(s, indent=2)}")
        if re.search(r'\{stream\..*::', name) or not name.strip():
            template_count += 1
            filename = hints.get('filename', '')
            url = s.get('url', '')
            logging.warning(f"Unrendered template detected in stream {i} ({source}): name='{name}'")
            logging.debug(f" Filename: {filename}")
            logging.debug(f" URL: {url}")
            logging.debug(f" Description: {description}")
            qual_match = re.search(r'(Web-dl|Webrip|Bluray|Bdremux|Remux|Hdrip|Tc|Ts|Cam|Dvdrip|Hdtv)', filename + ' ' + name + ' ' + description, re.I | re.U)
            res_match = re.search(r'(4k|2160p|1440p|1080p|720p)', filename + ' ' + name + ' ' + description, re.I | re.U)
            quality = qual_match.group(1).title() if qual_match else 'Unknown'
            res = res_match.group(1).upper() if res_match else '⍰'
            fallback_name = f"{res} {quality}"
            if filename:
                fallback_name = f"{fallback_name} from {filename.replace('.', ' ').title()}"
            s['name'] = fallback_name
            logging.debug(f" Fallback title applied (filename prioritized): {fallback_name}")
        if '⏳' in name or not hints.get('isCached', False):
            uncached_count += 1
            logging.debug(f"Uncached (⏳) detected in stream {i} ({source}): {name}")
    
    logging.info(f"Template streams: {template_count}/{len(all_streams)} | Uncached (⏳) streams: {uncached_count}/{len(all_streams)}")
    
    filtered = []
    uncached_filtered = []
    logging.debug("Starting filtering process")
    
    for i, s in enumerate(all_streams):
        hints = s.get('behaviorHints', {})
        name = s.get('name', '').replace('\n', ' ')
        description = s.get('description', '').replace('\n', ' ')
        parse_string = (name + ' ' + description).lower()
        normalized_parse = unicodedata.normalize('NFKD', parse_string).encode('ascii', 'ignore').decode('utf-8')
        if normalized_parse != parse_string:
            logging.debug(f"Unicode normalized for stream {i}: original='{parse_string[:100]}...' -> normalized='{normalized_parse[:100]}...'")
        parse_string = normalized_parse
        is_cached = hints.get('isCached', False)
        seed_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', parse_string, re.I | re.U)
        seeders = int(seed_match.group(1) or seed_match.group(2) or 0) if seed_match else (1 if not is_cached else 0)
        if not seed_match:
            logging.debug(f"Seeders match failed for stream {i}: pattern=r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', parse_string={parse_string[:100]}...")
            seed_miss_count += 1
        size_match = re.search(r'(\d+\.?\d*)\s*(gb|mb)', parse_string, re.I | re.U)
        size = 0
        if size_match:
            size_num = float(size_match.group(1))
            unit = size_match.group(2).lower()
            size = int(size_num * (10**9 if unit == 'gb' else 10**6))
        if size == 0:
            size = hints.get('videoSize', 0)
        if is_cached and size == 0:
            logging.warning(f"Potential misflagged cached stream {i}: isCached=True but size=0 - '{name}'")
        if seeders >= MIN_SEEDERS and MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES:
            if is_cached:
                filtered.append(s)
            else:
                uncached_filtered.append(s)
        logging.debug(f"Stream {i} after criteria check: kept={seeders >= MIN_SEEDERS and MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES}, is_cached={is_cached}, seeders={seeders}, size={size}")
    
    # New: Aggregate misses log
    logging.info(f"Aggregate misses: Seed {seed_miss_count}/{len(all_streams)}, Res {res_miss_count}/{len(all_streams)} - possible bad data if high")
    
    logging.debug(f"Filtered cached: {len(filtered)}, Uncached to verify: {len(uncached_filtered)}")
    
    # New: Drop reasons (fixed: re-compute per s)
    drop_reasons = {'low_seed': 0, 'size_out': 0, 'other': 0}
    for s in all_streams:
        # Re-compute seeders/size for this s
        parse_string = (s.get('name', '') + ' ' + s.get('description', '')).lower().replace('\n', ' ')
        seed_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', parse_string, re.I | re.U)
        seeders = int(seed_match.group(1) or seed_match.group(2) or 0) if seed_match else 0
        size_match = re.search(r'(\d+\.?\d*)\s*(gb|mb)', parse_string, re.I | re.U)
        size = 0
        if size_match:
            size_num = float(size_match.group(1))
            unit = size_match.group(2).lower()
            size = int(size_num * (10**9 if unit == 'gb' else 10**6))
        if size == 0:
            size = s.get('behaviorHints', {}).get('videoSize', 0)
        if seeders < MIN_SEEDERS: drop_reasons['low_seed'] += 1
        elif not MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES: drop_reasons['size_out'] += 1
        else: drop_reasons['other'] += 1
    logging.info(f"Drop stats: Low seed {drop_reasons['low_seed']}, Size out {drop_reasons['size_out']}, Other {drop_reasons['other']} - Total dropped {sum(drop_reasons.values())}")
    
    uncached_filtered.sort(key=sort_key)
    verified_uncached = []
    for us in uncached_filtered[:10]:
        url = us.get('url', '')
        if debrid_check_cache(url, 'rd') or debrid_check_cache(url, 'tb') or debrid_check_cache(url, 'ad'):
            verified_uncached.append(us)
        else:
            try:
                head = session.head(url, timeout=PING_TIMEOUT)
                if head.status_code == 200:
                    verified_uncached.append(us)
                    logging.debug(f"Fallback ping success for uncached stream {url}")
                else:
                    logging.debug(f"Fallback ping failed: status {head.status_code}")
            except Exception as e:
                logging.debug(f"Fallback ping error: {e}")
    filtered += verified_uncached[:MAX_UNCACHED_KEEP]
    logging.debug(f"After adding verified uncached: total filtered {len(filtered)}")
    
    def sort_key(s):
        global res_miss_count  # Declare global
        # Existing sort_key code...
        name = s.get('name', '').replace('\n', ' ')
        description = s.get('description', '').replace('\n', ' ')
        name_lower = (name + ' ' + description).lower()
        normalized_lower = unicodedata.normalize('NFKD', name_lower).encode('ascii', 'ignore').decode('utf-8')
        if normalized_lower != name_lower:
            logging.debug(f"Unicode normalized for sort key of '{name}': original='{name_lower[:100]}...' -> normalized='{normalized_lower[:100]}...'")
        name_lower = normalized_lower
        hints = s.get('behaviorHints', {})
        is_cached = hints.get('isCached', False)
        cached_priority = 0 if is_cached else 1
        res_priority = {'4k': 0, '2160p': 0, '1440p': 1, '1080p': 2, '720p': 3, 'bdremux': 0}.get(next((r for r in ['4k', '2160p', '1440p', '1080p', '720p', 'bdremux'] if r in name_lower), ''), 4)
        if res_priority == 4:
            logging.debug(f"Res match failed for stream {s.get('url', 'unknown')}: pattern=r'(4k|2160p|1440p|1080p|720p)', name_lower={name_lower[:100]}... - using default 4")
            res_miss_count += 1
        quality_priority = {'remux': 0, 'bdremux': 0, 'bluray': 1, 'web-dl': 2, 'webrip': 3, 'web': 3, 'hdtv': 4, 'ts': 5}.get(next((q for q in ['remux', 'bdremux', 'bluray', 'web-dl', 'webrip', 'web', 'hdtv', 'ts'] if q in name_lower), ''), 4)
        if quality_priority == 4:
            logging.debug(f"Quality match failed for stream {s.get('url', 'unknown')}: pattern=r'(remux|bluray|web-dl|webrip)', name_lower={name_lower[:100]}... - using default 4")
        source_priority = 0 if 'store' in name_lower or 'stremthru' in name_lower else (1 if 'rd' in name_lower or 'realdebrid' in name_lower else (2 if 'tb' in name_lower or 'torbox' in name_lower else (3 if 'ad' in name_lower or 'alldebrid' in name_lower else 4)))
        size_match = re.search(r'(\d+\.?\d*)\s*(gb|mb)', name_lower, re.I | re.U)
        size_num = float(size_match.group(1)) if size_match else (hints.get('videoSize', 0) / 10**9)
        seed_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', name_lower, re.I | re.U)
        seeders = int(seed_match.group(1) or seed_match.group(2) or 0) if seed_match else 0
        seadex_priority = 0 if 'seadex' in name_lower or 'ʙᴇsᴛ ʀᴇʟᴇᴀsᴇ' in name_lower else 1
        key = (cached_priority, seadex_priority, res_priority, quality_priority, source_priority, -size_num, -seeders)
        logging.debug(f"Sort key for '{name}': {key} (cached_pri={cached_priority}, res={res_priority}, qual={quality_priority}, src={source_priority}, size={size_num}, seeds={seeders}) - from name_lower: {name_lower[:100]}...")
        return key
    
    filtered.sort(key=sort_key)
    logging.info(f"Sorted filtered streams (first 5): {[(f.get('name', 'NO NAME'), sort_key(f)) for f in filtered[:5]]}")
    logging.debug(f"All sorted streams: {json.dumps([f.get('name', 'NO NAME') for f in filtered], indent=2)}")
    
    # New: Post-filter stats
    if filtered:
        kept_cached = sum(1 for f in filtered if f.get('behaviorHints', {}).get('isCached', False))
        kept_res = {res: sum(1 for f in filtered if res in f.get('name', '').lower()) for res in ['4k', '2160p', '1440p', '1080p', '720p']}
        kept_qual = {qual: sum(1 for f in filtered if qual in f.get('name', '').lower()) for qual in ['remux', 'bdremux', 'bluray', 'web-dl', 'webrip', 'web', 'hdtv', 'ts']}
        logging.info(f"Post-filter stats: Kept {len(filtered)}, Cached {kept_cached}, Resolutions {kept_res}, Qualities {kept_qual} - Mismatch alert if 0 but Stremio shows streams")
    else:
        logging.warning("0 kept - Mismatch with Stremio? Check AIO data or filters")
    
    use_emoji_flags = True
    for i, s in enumerate(filtered):
        name = s.get('name', '').replace('\n', ' ')
        description = s.get('description', '').replace('\n', ' ')
        parse_string = (name + ' ' + description).lower()
        normalized_parse = unicodedata.normalize('NFKD', parse_string).encode('ascii', 'ignore').decode('utf-8')
        if normalized_parse != parse_string:
            logging.debug(f"Unicode normalized for formatting stream {i}: original='{parse_string[:100]}...' -> normalized='{normalized_parse[:100]}...'")
        parse_string = normalized_parse
        hints = s.get('behaviorHints', {})
        service = next((k for k in SERVICE_COLORS if k in parse_string), '')
        if service:
            name = f"{SERVICE_COLORS[service]}{name}[/]"
            logging.debug(f"Applied color for service {service} in stream {i}")
        else:
            logging.debug(f"No service match for coloring in stream {i}: pattern=SERVICE_COLORS keys, parse_string={parse_string[:100]}...")
        if re.search(r'[\uac00-\ud7a3]', parse_string, re.U):
            name += ' 🇰🇷'
            logging.debug(f"Added KR flag for stream {i} via Hangul detection")
        elif re.search(r'[\u3040-\u30ff\u4e00-\u9faf]', parse_string, re.U):
            name += ' 🇯🇵'
            logging.debug(f"Added JP flag for stream {i} via Kanji detection")
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
            logging.debug(f"No audio match for stream {i}: pattern=r'(dd\+|dd|aac|atmos|5\.1|2\.0)', parse_string={parse_string[:100]}...")
        if 'store' in parse_string or '4k' in parse_string or 'stremthru' in parse_string:
            name = f"★ {name}"
        if '⏳' in name or not hints.get('isCached', False):
            s['name'] = f"[dim]{name} (Unverified ⏳)[/dim]"
        else:
            s['name'] = name
        logging.debug(f"Formatted stream {i}: final name='{s['name']}'")
    
    logging.info(f"Final filtered: {len(filtered)}")
    
    # New: Safe final JSON log
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
        app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)  # Force debug for full logs
    except Exception as e:
        logging.error(f"App startup error: {e}")

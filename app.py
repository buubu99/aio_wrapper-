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
API_TIMEOUT = 10  # General API timeout in seconds
POLL_INTERVAL = 2  # Seconds between polls in add-magnet method
MAX_POLLS = 5  # Max polls before timeout (total ~10s)
USE_PING_FALLBACK = os.environ.get('USE_PING_FALLBACK', 'false').lower() == 'true'  # Default off
PING_TIMEOUT = 2

# API Keys (unchanged)
RD_API_KEY = 'Z4C3UT777IK2U6EUZ5HLVVMO7BYQPDNEOJUGAFLUBXMGTSX2Z6RA'
TB_API_KEY = '1046c773-9d0d-4d53-95ab-8976a559a5f6'
AD_API_KEY = 'Z2HvUmLsuuRmyHg5Zf5K8'  # New key from screenshot

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
    logging.debug(f"Extracted hash: {torrent_hash} from URL: {url} for service: {service}")
    
    if service == 'rd':
        headers = {'Authorization': f'Bearer {RD_API_KEY}'}
        # First, try instant availability (may be restricted)
        check_url = f'https://api.real-debrid.com/rest/1.0/torrents/instantAvailability/{torrent_hash}'
        try:
            response = session.get(check_url, headers=headers, timeout=API_TIMEOUT)
            logging.debug(f"RD instant check response status: {response.status_code}, full response: {response.text}")
            if response.status_code == 200:
                data = response.json()
                rd_variants = data.get(torrent_hash.lower(), {}).get('rd', [])
                if rd_variants and any(variant for variant in rd_variants):
                    logging.debug(f"RD instant confirmed cached (variants: {len(rd_variants)})")
                    return True
                else:
                    logging.debug(f"RD instant: not cached or no variants")
            else:
                logging.warning(f"RD instant failed: {response.status_code} - {response.text}. Falling back to add-magnet method.")
        except Exception as e:
            logging.error(f"RD instant error: {e}. Falling back to add-magnet method.")
        
        # Fallback to add-magnet-poll-delete
        add_url = 'https://api.real-debrid.com/rest/1.0/torrents/addMagnet'
        data = {'magnet': f'magnet:?xt=urn:btih:{torrent_hash}'}
        try:
            add_response = session.post(add_url, headers=headers, data=data, timeout=API_TIMEOUT)
            logging.debug(f"RD add-magnet response: {add_response.status_code} - {add_response.text}")
            if add_response.status_code != 200:
                return False
            torrent_id = add_response.json().get('id')
            if not torrent_id:
                return False
            
            for _ in range(MAX_POLLS):
                info_url = f'https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}'
                info_resp = session.get(info_url, headers=headers, timeout=API_TIMEOUT)
                logging.debug(f"RD poll info response: {info_resp.status_code} - {info_resp.text}")
                if info_resp.status_code == 200:
                    info = info_resp.json()
                    progress = info.get('progress', 0)
                    status = info.get('status', '')
                    if progress == 100 or status in ['downloaded', 'magnet_conversion']:
                        logging.debug(f"RD fallback confirmed cached (progress: {progress}, status: {status})")
                        delete_url = f'https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}'
                        del_resp = session.delete(delete_url, headers=headers)
                        logging.debug(f"RD delete response: {del_resp.status_code} - {del_resp.text}")
                        return True
                time.sleep(POLL_INTERVAL)
            
            logging.debug(f"RD fallback timeout after {MAX_POLLS} polls")
            delete_url = f'https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}'
            del_resp = session.delete(delete_url, headers=headers)
            logging.debug(f"RD delete on timeout: {del_resp.status_code} - {del_resp.text}")
            return False
        except Exception as e:
            logging.error(f"RD fallback error: {e}")
            return False
    
    elif service == 'tb':
        headers = {'Authorization': f'Bearer {TB_API_KEY}'}
        check_url = f'https://api.torbox.app/v1/api/torrents/checkcache?hash={torrent_hash}'
        try:
            response = session.get(check_url, headers=headers, timeout=API_TIMEOUT)
            logging.debug(f"TB checkcache response: {response.status_code} - {response.text}")
            if response.status_code == 200:
                data = response.json()
                if data.get('data', {}).get('cached', False):
                    logging.debug(f"TB confirmed cached")
                    return True
                else:
                    logging.debug(f"TB: not cached")
                    return False
            else:
                logging.warning(f"TB check failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logging.error(f"TB check error: {e}")
            return False
    
    elif service == 'ad':
        headers = {'Authorization': f'Bearer {AD_API_KEY}'}
        # Try instant (may be removed)
        check_url = 'https://api.alldebrid.com/v4/magnet/instant'
        params = {'magnets[]': f'magnet:?xt=urn:btih:{torrent_hash}'}
        try:
            response = session.get(check_url, headers=headers, params=params, timeout=API_TIMEOUT)
            logging.debug(f"AD instant response: {response.status_code} - {response.text}")
            if response.status_code == 200:
                data = response.json()
                magnets = data.get('data', {}).get('magnets', [])
                if magnets and magnets[0].get('instant', False):
                    logging.debug(f"AD instant confirmed cached")
                    return True
                else:
                    logging.debug(f"AD instant: not cached")
            else:
                logging.warning(f"AD instant failed: {response.status_code} - {response.text}. Falling back to add-magnet method.")
        except Exception as e:
            logging.error(f"AD instant error: {e}. Falling back to add-magnet method.")
        
        # Fallback to add-magnet-poll-delete for AD
        add_url = 'https://api.alldebrid.com/v4/magnet/upload'
        params = {'magnets[]': f'magnet:?xt=urn:btih:{torrent_hash}'}
        try:
            add_response = session.get(add_url, headers=headers, params=params, timeout=API_TIMEOUT)
            logging.debug(f"AD add-magnet response: {add_response.status_code} - {add_response.text}")
            if add_response.status_code != 200:
                return False
            magnets = add_response.json().get('data', {}).get('magnets', [])
            if not magnets:
                return False
            torrent_id = magnets[0].get('id')
            if not torrent_id:
                return False
            
            for _ in range(MAX_POLLS):
                status_url = 'https://api.alldebrid.com/v4/magnet/status'
                params = {'id': torrent_id}
                status_resp = session.get(status_url, headers=headers, params=params, timeout=API_TIMEOUT)
                logging.debug(f"AD poll status response: {status_resp.status_code} - {status_resp.text}")
                if status_resp.status_code == 200:
                    info = status_resp.json().get('data', {}).get('magnets', {})
                    status_code = info.get('statusCode')
                    if status_code == 4:  # 4 = ready
                        logging.debug(f"AD fallback confirmed cached (statusCode: {status_code})")
                        delete_url = 'https://api.alldebrid.com/v4/magnet/delete'
                        del_params = {'id': torrent_id}
                        del_resp = session.get(delete_url, headers=headers, params=del_params)
                        logging.debug(f"AD delete response: {del_resp.status_code} - {del_resp.text}")
                        return True
                time.sleep(POLL_INTERVAL)
            
            logging.debug(f"AD fallback timeout after {MAX_POLLS} polls")
            delete_url = 'https://api.alldebrid.com/v4/magnet/delete'
            del_params = {'id': torrent_id}
            del_resp = session.get(delete_url, headers=headers, params=del_params)
            logging.debug(f"AD delete on timeout: {del_resp.status_code} - {del_resp.text}")
            return False
        except Exception as e:
            logging.error(f"AD fallback error: {e}")
            return False
    
    return False

@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.27",  # Updated version
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
    logging.debug(f"Starting stream processing for {media_type}/{media_id}")
    all_streams = []
    template_count = 0
    uncached_count = 0
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
        source = 'AIO' if i < len(aio_streams) else 'Store' if USE_STORE else 'AIO'
        logging.debug(f"Processing stream {i} from {source}: raw dict = {json.dumps(s, indent=2)}")
        
        if re.search(r'\{stream\..*::', name) or not name.strip():
            template_count += 1
            filename = hints.get('filename', '')
            url = s.get('url', '')
            logging.warning(f"Unrendered template detected in stream {i} ({source}): name='{name}'")
            logging.debug(f"  Filename: {filename}")
            logging.debug(f"  URL: {url}")
            logging.debug(f"  Description: {description}")
            qual_match = re.search(r'(Web-dl|Webrip|Bluray|Bdremux|Remux|Hdrip|Tc|Ts|Cam|Dvdrip|Hdtv)', filename + ' ' + name + ' ' + description, re.I | re.U)
            res_match = re.search(r'(4k|2160p|1440p|1080p|720p)', filename + ' ' + name + ' ' + description, re.I | re.U)
            quality = qual_match.group(1).title() if qual_match else 'Unknown'
            res = res_match.group(1).upper() if res_match else '⍰'
            fallback_name = f"{res} {quality}"
            if filename:
                fallback_name = f"{fallback_name} from {filename.replace('.', ' ').title()}"
            s['name'] = fallback_name
            logging.debug(f"  Fallback title applied (filename prioritized): {fallback_name}")
        
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
            logging.debug(f"Seeders match failed for stream {i}: pattern=r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', full_parse_string='{parse_string}'")
        
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
    
    logging.debug(f"Filtered cached: {len(filtered)}, Uncached to verify: {len(uncached_filtered)}")
    
    uncached_filtered.sort(key=lambda s: sort_key(s))  # Sort uncached before verifying
    verified_uncached = []
    for us in uncached_filtered[:50]:
        url = us.get('url', '')
        # Cycle services: RD (with fallback), TB, AD (with fallback)
        for serv in ['rd', 'tb', 'ad']:
            if debrid_check_cache(url, serv):
                verified_uncached.append(us)
                logging.debug(f"API verified cached via {serv} (overriding upstream flag) for stream: {us.get('name', 'NO NAME')} - URL: {url}")
                break
        else:
            if USE_PING_FALLBACK:
                try:
                    head = session.head(url, timeout=PING_TIMEOUT)
                    logging.debug(f"Ping response: {head.status_code} - Headers: {head.headers}")
                    if head.status_code == 200:
                        verified_uncached.append(us)
                        logging.debug(f"Fallback ping success for {url}")
                    else:
                        logging.debug(f"Fallback ping failed: status {head.status_code} for {url}")
                except Exception as e:
                    logging.debug(f"Fallback ping error: {e} for {url}")
            else:
                logging.debug(f"Skipped uncached stream (no API cache, ping disabled): {us.get('name', 'NO NAME')} - URL: {url}")
    
    filtered += verified_uncached[:MAX_UNCACHED_KEEP]
    logging.debug(f"After adding verified uncached: total filtered {len(filtered)}")
    
    def sort_key(s):
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
        
        full_text = name_lower + ' ' + hints.get('filename', '').lower()
        res_priority = {'4k': 0, '2160p': 0, '1440p': 1, '1080p': 2, '720p': 3}.get(next((r.lower() for r in ['4k', '2160p', '1440p', '1080p', '720p'] if r.lower() in full_text), ''), 4)
        if res_priority == 4:
            logging.debug(f"Res match failed for stream {s.get('url', 'unknown')}: pattern=r'(4k|2160p|1440p|1080p|720p)', full_text={full_text}")
        
        quality_priority = {'bdremux': 0, 'remux': 0, 'bluray': 1, 'web-dl': 2, 'webrip': 3, 'web': 3, 'hdtv': 4, 'ts': 5}.get(next((q.lower() for q in ['bdremux', 'remux', 'bluray', 'web-dl', 'webrip', 'web', 'hdtv', 'ts'] if q.lower() in full_text), ''), 4)
        if quality_priority == 4:
            logging.debug(f"Quality match failed for stream {s.get('url', 'unknown')}: pattern=r'(bdremux|remux|bluray|web-dl|webrip|web|hdtv|ts)', full_text={full_text}")
        
        source_priority = 0 if 'store' in name_lower or 'stremthru' in name_lower else (1 if 'rd' in name_lower or 'realdebrid' in name_lower else (2 if 'tb' in name_lower or 'torbox' in name_lower else (3 if 'ad' in name_lower or 'alldebrid' in name_lower else 4)))
        
        size_match = re.search(r'(\d+\.?\d*)\s*(gb|mb)', name_lower, re.I | re.U)
        size_num = float(size_match.group(1)) if size_match else (hints.get('videoSize', 0) / 10**9)
        
        seed_match = re.search(r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', name_lower, re.I | re.U)
        seeders = int(seed_match.group(1) or seed_match.group(2) or 0) if seed_match else 0
        
        seadex_priority = 0 if 'seadex' in name_lower or 'ʙᴇsᴛ ʀᴇʟᴇᴀsᴇ' in name_lower else 1
        
        lang_priority = 0 if any(l in full_text for l in ['eng', 'en', 'kor', 'kr', 'ko']) else 1
        
        completion_priority = 0 if is_cached or 'complete' in name_lower or '100%' in name_lower else 1
        
        key = (cached_priority, completion_priority, seadex_priority, res_priority, quality_priority, source_priority, lang_priority, -size_num, -seeders)
        logging.debug(f"Sort key for '{name}': {key} (cached_pri={cached_priority}, complete_pri={completion_priority}, res={res_priority}, qual={quality_priority}, src={source_priority}, lang={lang_priority}, size={size_num}, seeds={seeders}) - from full_text: {full_text}")
        return key
    
    filtered.sort(key=sort_key)
    logging.info(f"Sorted filtered streams (first 5): {[(f.get('name', 'NO NAME'), sort_key(f)) for f in filtered[:5]]}")
    logging.debug(f"All sorted streams: {json.dumps([f.get('name', 'NO NAME') for f in filtered], indent=2)}")
    
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
            logging.debug(f"No service match for coloring in stream {i}: pattern=SERVICE_COLORS keys, full_parse_string={parse_string}")
        
        if re.search(r'[\uac00-\ud7a3]', parse_string, re.U):
            name += ' 🇰🇷'
            logging.debug(f"Added KR flag for stream {i} via Hangul detection")
        elif re.search(r'[\u3040-\u30ff\u4e00-\u9faf]', parse_string, re.U):
            name += ' 🇯🇵'
            logging.debug(f"Added JP flag for stream {i} via Kanji detection")
        
        lang_match = re.search(r'([a-z]{2,3}(?:[ ·,·-]*[a-z]{2,3})*)', parse_string, re.I | re.U)
        if lang_match:
            langs = re.findall(r'[a-z]{2,3}', lang_match.group(1), re.I)
            flags_added = set(LANGUAGE_FLAGS.get(lang.lower(), '') for lang in langs if lang.lower() in LANGUAGE_FLAGS)
            if flags_added:
                name += ' ' + ' '.join(flags_added)
                logging.debug(f"Added flags {flags_added} for stream {i} from match: {lang_match.group(0)}")
            else:
                logging.debug(f"Lang match but no known flags for stream {i}: {lang_match.group(0)}")
        else:
            logging.debug(f"No lang pattern match for stream {i}: pattern=r'([a-z]{2,3}(?:[ ·,·-]*[a-z]{2,3})*)', full_parse_string={parse_string}")
        
        audio_match = re.search(r'(dd\+|dd|dts-hd|dts|opus|aac|atmos|ma|5\.1|7\.1|2\.0)', parse_string, re.I | re.U)
        channel_match = re.search(r'(\d\.\d)', parse_string, re.I | re.U)
        if audio_match:
            audio = audio_match.group(1).upper()
            channels = channel_match.group(1) if channel_match else ''
            name += f" ♬ {audio} {channels}".strip()
            logging.debug(f"Added audio attribute {audio} {channels} for stream {i} from match: {audio_match.group(0)}")
        else:
            logging.debug(f"No audio match for stream {i}: pattern=r'(dd\+|dd|aac|atmos|5\.1|2\.0)', full_parse_string={parse_string}")
        
        if 'store' in parse_string or '4k' in parse_string or 'stremthru' in parse_string:
            name = f"★ {name}"
        
        if '⏳' in name or not hints.get('isCached', False):
            s['name'] = f"[dim]{name} (Unverified ⏳)[/dim]"
        else:
            s['name'] = name
        logging.debug(f"Formatted stream {i}: final name='{s['name']}'")
    
    logging.info(f"Final filtered: {len(filtered)}")
    logging.debug(f"Final streams JSON: {json.dumps({'streams': filtered[:60]}, indent=2)}")
    return jsonify({'streams': filtered[:60]})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

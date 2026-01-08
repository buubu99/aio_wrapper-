from flask import Flask, request, jsonify
import requests
import os
import logging
import re
import json
import unicodedata
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, resources={r"/*": {"origins": "*"}})
logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'DEBUG'))  # Set to DEBUG for heavier logging
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
@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.23",
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
        source = 'AIO' if i < len(aio_streams) else 'Store'
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
            logging.debug(f"Seeders match failed for stream {i}: pattern=r'(?:👥|seeders?|seeds?|⇋|peers?) ?(\d+)(?:𖧧)?|(\d+)\s*(?:seed|𖧧)', parse_string={parse_string[:100]}...")
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
    uncached_filtered.sort(key=sort_key)
    verified_uncached = []
    for us in uncached_filtered[:10]:
        url = us.get('url', '')
        if url:
            try:
                head = session.head(url, timeout=PING_TIMEOUT)
                if head.status_code == 200:
                    verified_uncached.append(us)
                    logging.debug(f"Smart ping success for uncached stream {url} - adding as verified")
                else:
                    logging.debug(f"Smart ping failed for uncached stream {url}: status {head.status_code}")
            except Exception as e:
                logging.debug(f"Smart ping error for {url}: {e}")
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
        res_priority = {'4k': 0, '2160p': 0, '1440p': 1, '1080p': 2, '720p': 3, 'bdremux': 0}.get(next((r for r in ['4k', '2160p', '1440p', '1080p', '720p', 'bdremux'] if r in name_lower), ''), 4)
        if res_priority == 4:
            logging.debug(f"Res match failed for stream {s.get('url', 'unknown')}: pattern=r'(4k|2160p|1440p|1080p|720p)', name_lower={name_lower[:100]}... - using default 4")
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
    logging.debug(f"Final streams JSON: {json.dumps({'streams': filtered[:60]}, indent=2)}")
    return jsonify({'streams': filtered[:60]})
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

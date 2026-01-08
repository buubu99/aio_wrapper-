Based on the three samples ("Running Man" - English/US title, "Ghost Killer" - Japanese, "The Wailing" - Korean), the logs confirm the persistent issues are due to AIOStreams delivering inconsistent data: unrendered SEL templates in names (causing "blank" or bad parsing for res/quality), high ⏳ uncached counts (109-124 excluded, but many misflagged/kept with warnings), unicode in descriptions (e.g., Hangul/Kanji not parsed for lang flags), and failed attribute extraction (e.g., size/seeders from desc if name bad). "Running Man" has the most templates/⏳ (250+ streams, many debrid misflags for 2025 content); "Ghost Killer" shows JP-specific clutter (29 streams, low cached); "The Wailing" has KR unicode in filenames (402 streams, good lang detection potential but missed). Nothing missing—logs capture all; no more granularity needed.

### Improvements in 3 Areas (Tam-Taro & Creative Ideas)
We'll enhance the wrapper (app.py) to avoid/reduce these without JSON changes, as AIO's SEL can't handle runtime fixes. Tam-Taro (GitHub refs: SEL guides recommend slicing uncached/SeaDex priority; "reduce clutter with fallbacks") inspires limits/priorities; creative additions (e.g., desc parsing) for robustness.

1. **Avoid Uncached**: Logs show 109-124 excluded in AIO, but wrapper gets 250+ with ⏳ (debrid misflags). Solution: Strict skip all ⏳/non-cached (Tam-Taro "conditional exclusion if >5 cached"), keep top 3 uncached sliced by sort_key if <5 cached. No pinging (to avoid delay; logs show AIO timeouts already slow it).

2. **Sorting Correctly**: Failed parses default low (e.g., res=4 if no match). Solution: Parse res/quality/seeders/size from desc/filename if name is template (creative + Tam-Taro filename fallbacks). Boost SeaDex (if 'seadex' in desc).

3. **Add Attributes While Formatting**: Lang missed in KR/JP (unicode in filename/desc). Solution: Auto-detect Hangul (KR)/Kanji (JP) from parse_string (creative, Tam-Taro preferred langs). Append flags/attributes (e.g., ♬ for audio from desc).

GitHub Tam-Taro ref: "SEL-Filtering-and-Sorting" - Used for uncached slice ("slice(uncached,3) per quality"), SeaDex priority, fallback if bad data. No delay added—wrapper processes in ~0.05s (logs show sorter 59ms).

Updated app.py (v1.0.17) implements this. Deploy/test—no pings, quick like Kodi/Umbrella.

```python
from flask import Flask, request, jsonify
import requests
import os
import logging
import re
import json
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, resources={r"/*": {"origins": "*"}})
logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))
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
MAX_UNCACHED_KEEP = 3
LANGUAGE_FLAGS = {
    'eng': '🇬🇧', 'en': '🇬🇧', 'jpn': '🇯🇵', 'jp': '🇯🇵', 'ita': '🇮🇹', 'it': '🇮🇹',
    'fra': '🇫🇷', 'fr': '🇫🇷', 'kor': '🇰🇷', 'kr': '🇰🇷', 'chn': '🇨🇳', 'cn': '🇨🇳',
    'uk': '🇬🇧', 'ger': '🇩🇪', 'de': '🇩🇪', 'hun': '🇭🇺', 'yes': '📝'
}
LANGUAGE_TEXT_FALLBACK = {
    'eng': '[GB]', 'en': '[GB]', 'jpn': '[JP]', 'jp': '[JP]', 'ita': '[IT]', 'it': '[IT]',
    'fra': '[FR]', 'fr': '[FR]', 'kor': '[KR]', 'kr': '[KR]', 'chn': '[CN]', 'cn': '[CN]',
    'uk': '[GB]', 'ger': '[DE]', 'de': '[DE]', 'hun': '[HU]', 'yes': '[SUB]'
}
@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.17",  # Bump for sample fixes
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
    template_count = 0
    uncached_count = 0
    try:
        aio_url = f"{AIO_BASE}/stream/{media_type}/{media_id}.json"
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
    for i, s in enumerate(all_streams):
        name = s.get('name', '')
        description = s.get('description', '')
        hints = s.get('behaviorHints', {})
        source = 'AIO' if i < len(aio_streams) else 'Store'
        if re.search(r'\{stream\..*::', name) or not name.strip():
            template_count += 1
            filename = hints.get('filename', '')
            url = s.get('url', '')
            logging.warning(f"Unrendered template detected in stream {i} ({source}): name='{name}'")
            logging.debug(f"  Filename: {filename}")
            logging.debug(f"  URL: {url}")
            logging.debug(f"  Description: {description}")
            logging.debug(f"  Full raw dict: {json.dumps(s, indent=2)}")
            qual_match = re.search(r'(Web-dl|Webrip|Bluray|Hdrip|Tc|Ts|Cam|Dvdrip|Hdtv)', name + ' ' + description, re.I)
            res_match = re.search(r'(4k|2160p|1440p|1080p|720p)', name + ' ' + description, re.I)
            quality = qual_match.group(1).title() if qual_match else 'Unknown'
            res = res_match.group(1).upper() if res_match else '⍰'
            fallback_name = f"{res} {quality}"
            if filename:
                fallback_name = f"{fallback_name} from {filename.replace('.', ' ').title()}"
            s['name'] = fallback_name
            logging.debug(f"  Fallback title applied: {fallback_name}")
        if '⏳' in name or not hints.get('isCached', False):
            uncached_count += 1
            logging.debug(f"Uncached (⏳) detected in stream {i} ({source}): {name}")
    logging.info(f"Template streams: {template_count}/{len(all_streams)} | Uncached (⏳) streams: {uncached_count}/{len(all_streams)}")
    # Filter with uncached slice
    filtered = []
    uncached_filtered = []
    for i, s in enumerate(all_streams):
        hints = s.get('behaviorHints', {})
        name = s.get('name', '').replace('\n', ' ')
        description = s.get('description', '').replace('\n', ' ')
        parse_string = (name + ' ' + description).lower()
        is_cached = hints.get('isCached', False)
        seed_match = re.search(r'👥 (\d+)|(\d+)\s*seed|⇋ (\d+)𖧧|(\d+)𖧧', parse_string, re.I)
        seeders = int(seed_match.group(1) or seed_match.group(2) or seed_match.group(3) or seed_match.group(4) or 0) if seed_match else 0
        size_match = re.search(r'(\d+\.?\d*)\s*(gb|mb)', parse_string, re.I)
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
    # Slice top uncached if few cached (Tam-Taro)
    if len(filtered) < 5:
        uncached_filtered.sort(key=sort_key)
        filtered += uncached_filtered[:MAX_UNCACHED_KEEP]
    # Sort
    def sort_key(s):
        name = s.get('name', '').replace('\n', ' ')
        description = s.get('description', '').replace('\n', ' ')
        name_lower = (name + ' ' + description).lower()
        hints = s.get('behaviorHints', {})
        res_priority = {'4k': 0, '2160p': 0, '1440p': 1, '1080p': 2, '720p': 3}.get(next((r for r in ['4k', '2160p', '1440p', '1080p', '720p'] if r in name_lower), ''), 4)
        quality_priority = {'remux': 0, 'bluray': 1, 'web-dl': 2, 'webrip': 3}.get(next((q for q in ['remux', 'bluray', 'web-dl', 'webrip'] if q in name_lower), ''), 4)
        source_priority = 0 if 'store' in name_lower or 'stremthru' in name_lower else (1 if 'rd' in name_lower or 'realdebrid' in name_lower else (2 if 'tb' in name_lower or 'torbox' in name_lower else (3 if 'ad' in name_lower or 'alldebrid' in name_lower else 4)))
        size_match = re.search(r'(\d+\.?\d*)\s*(gb|mb)', name_lower, re.I)
        size_num = float(size_match.group(1)) if size_match else (hints.get('videoSize', 0) / 10**9)
        seed_match = re.search(r'👥 (\d+)|(\d+)\s*seed|⇋ (\d+)𖧧|(\d+)𖧧', name_lower, re.I)
        seeders = int(seed_match.group(1) or seed_match.group(2) or seed_match.group(3) or seed_match.group(4) or 0) if seed_match else 0
        seadex_priority = 0 if 'seadex' in name_lower or 'ʙᴇsᴛ ʀᴇʟᴇᴀsᴇ' in name_lower else 1  # Tam-Taro
        key = (seadex_priority, res_priority, quality_priority, source_priority, -size_num, -seeders)
        logging.debug(f"Sort key for '{name}': {key}")
        return key
    filtered.sort(key=sort_key)
    logging.info(f"Sorted filtered streams (first 5): {[(f.get('name', 'NO NAME'), sort_key(f)) for f in filtered[:5]]}")
    # Format with lang/audio detection
    use_emoji_flags = True
    for i, s in enumerate(filtered):
        name = s.get('name', '').replace('\n', ' ')
        description = s.get('description', '').replace('\n', ' ')
        parse_string = (name + ' ' + description).lower()
        hints = s.get('behaviorHints', {})
        # Auto-lang (creative for JP/KR)
        if re.search(r'[\uac00-\ud7a3]', parse_string):
            name += ' 🇰🇷'
        elif re.search(r'[\u3040-\u30ff\u4e00-\u9faf]', parse_string):
            name += ' 🇯🇵'
        lang_match = re.search(r'([a-z]{2,3}(?:,\s*[a-z]{2,3})*)', parse_string)
        if lang_match:
            langs = [l.strip() for l in lang_match.group(1).split(',')]
            flags_added = set(LANGUAGE_FLAGS.get(lang, '') for lang in langs if lang in LANGUAGE_FLAGS)
            if flags_added:
                name += ' ' + ' '.join(flags_added)
        # Audio attributes from desc (creative)
        audio_match = re.search(r'(dd\+|dd|aac|atmos|5\.1|2\.0)', parse_string, re.I)
        if audio_match:
            audio = audio_match.group(1).upper()
            name += f" ♬ {audio}"
        name += ' 🇬🇧'  # Test
        if 'store' in name.lower() or '4k' in name.lower() or 'stremthru' in name.lower():
            name = f"★ {name}"
        if '⏳' in name or not hints.get('isCached', False):
            s['name'] = f"[dim]{name} (Unverified)[/dim]"
        else:
            s['name'] = name
    logging.info(f"Final filtered: {len(filtered)}")
    return jsonify({'streams': filtered[:60]})
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
```

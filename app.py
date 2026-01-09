from flask import Flask, request, jsonify
import requests
import os
import logging
import re
import json
import time
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import defaultdict, Counter
from fuzzywuzzy import fuzz
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
DEBRID_KEYS = {
    'tb': '1046c773-9d0d-4d53-95ab-8976a559a5f6',
    'rd': 'Z4C3UT777IK2U6EUZ5HLVVMO7BYQPDNEOJUGAFLUBXMGTSX2Z6RA',
    'ad': 'ZXzHvUmLsuuRmgyHg5zjFsk8'
}
QUALITY_PATTERNS = [
    {"name": "Remux", "pattern": r"remux", "flags": re.I},
    {"name": "Bluray", "pattern": r"blu[-_]?ray|brrip", "flags": re.I},
    {"name": "Web", "pattern": r"web[-_.]?(dl|rip)", "flags": re.I},
    {"name": "Bad", "pattern": r"yify|psa|pahe|afg|cmrg|ion10|megusta|tgx", "flags": re.I}
]
played_feedback = Counter()  # Idea 4; mock for demo
def update_patterns():  # Idea 3
    try:
        resp = requests.get('https://raw.githubusercontent.com/Vidhin05/Releases-Regex/main/patterns.json')
        new_patterns = resp.json()
        QUALITY_PATTERNS.extend(new_patterns)
        logging.info("UPDATED_PATTERNS | ADDED: %d", len(new_patterns))
    except:
        logging.warning("PATTERNS_UPDATE_FAIL")
update_patterns()
def clean_string(text):
    text = re.sub(r'\{.*?\}', '', text, flags=re.I)
    return text.lower()
def get_quality_tier(parse_string):
    for pat in QUALITY_PATTERNS:
        if re.search(pat['pattern'], parse_string, pat.get('flags', 0)):
            return pat['name']
    return 'Unknown'
HEALTH_TAGS = {'✅': 2, '🧝': 1, '⚠️': -1, '🚫': -2}  # Creative Usenet scores
def get_health_score(desc):
    score = 0
    for tag, val in HEALTH_TAGS.items():
        if tag in desc:
            score += val
    return score
def verify_cached(hashes):  # Idea 2
    votes = defaultdict(int)
    hashes_upper = [h.upper() for h in hashes]
    # RD batch
    if DEBRID_KEYS['rd']:
        rd_url = f"https://api.real-debrid.com/rest/1.0/torrents/instantAvailability/{'/'.join(hashes_upper[:40])}"
        try:
            resp = session.get(rd_url, headers={'Authorization': f"Bearer {DEBRID_KEYS['rd']}"}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                for h in hashes_upper:
                    if data.get(h.lower(), {}).get('rd'):
                        votes[h] += 1
        except Exception as e:
            logging.error(f"RD_VERIFY_ERROR: {e}")
    # AD batch
    if DEBRID_KEYS['ad']:
        ad_url = "https://api.alldebrid.com/v4/magnet/instant"
        params = {'agent': 'wrapper', 'apikey': DEBRID_KEYS['ad']}
        for i in range(0, len(hashes_upper), 10):
            params['magnets[]'] = hashes_upper[i:i+10]
            try:
                resp = session.get(ad_url, params=params, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()['data']['magnets']
                    for item in data:
                        if item['instant']:
                            votes[item['hash'].upper()] += 1
            except Exception as e:
                logging.error(f"AD_VERIFY_ERROR: {e}")
    # TB batch (Pro Usenet-enabled)
    if DEBRID_KEYS['tb']:
        tb_url = "https://api.torbox.app/v1/api/torrent/check"
        try:
            resp = session.post(tb_url, json={'apikey': DEBRID_KEYS['tb'], 'hashes': hashes_upper}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                for h, status in data.items():
                    if status.get('cached') or status.get('available'):
                        votes[h.upper()] += 1
        except Exception as e:
            logging.error(f"TB_VERIFY_ERROR: {e}")
    return [h for h in hashes if votes.get(h.upper(), 0) >= 2]
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
    title_ref = "the wailing"  # Idea 1 (adjust if needed)
    stale_count = 0
    for i, s in enumerate(all_streams):
        parse_string = clean_string(s.get('name', '') + ' ' + s.get('description', ''))
        hints = s.get('behaviorHints', {})
        match_score = fuzz.ratio(parse_string, title_ref)
        if match_score < 60:
            logging.debug("DISCARD_FAKE | ID: %d | SCORE: %d", i, match_score)
            continue
        quality = get_quality_tier(parse_string)
        if quality == 'Bad':
            logging.debug("DISCARD_BAD | ID: %d | QUALITY: %s", i, quality)
            continue
        desc = s.get('description', '')
        health_score = get_health_score(desc)
        if health_score <= -1:
            stale_count += 1
            logging.debug("DISCARD_STALE_USENET | ID: %d | SCORE: %d", i, health_score)
            if 'tb' in parse_string or 'torbox' in parse_string:
                logging.info("TORBOX_USENET_EDGE: Filtered 1 stale NZB")
            continue
        s['health_score'] = health_score
        seed_match = re.search(r'seeders? ?(\d+)', parse_string, re.I)
        seeders = int(seed_match.group(1)) if seed_match else hints.get('seeders', 0) or 0
        size_match = re.search(r'(\d+\.?\d*) ?(gb|mb)', parse_string, re.I)
        size_num = float(size_match.group(1)) if size_match else 0
        unit = size_match.group(2).lower() if size_match else ''
        size = size_num * (1024**3 if 'gb' in unit else 1024**2 if 'mb' in unit else 0) or hints.get('videoSize', 0)
        res_match = re.search(r'(4k|2160p|1440p|1080p|720p|576p|540p|480p|360p|240p|144p)', parse_string, re.I)
        res = res_match.group(0).upper() if res_match else hints.get('resolution', 'Unknown').upper()
        if size < MIN_SIZE_BYTES or (res in ['SD', 'UNKNOWN', '360P', '480P', '240P', '144P'] and any(r in ['4K', '2160P', '1080P', '720P'] for st in all_streams for r in [st.get('resolution', '').upper()])):
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
        s['res'] = res
        played_feedback[quality] += 1  # Mock; add real feedback if needed
    verified = verify_cached(uncached_hashes)
    for s in all_streams:
        url = s.get('url', '')
        h_match = re.search(r'[a-fA-F0-9]{40}', url, re.I)
        if h_match and h_match.group(0).upper() in verified and s not in filtered:
            filtered.append(s)
            logging.debug("ADD_VERIFIED | HASH: %s", h_match.group(0))
    kept = len(filtered)
    logging.info("FILTERED_COUNT: %d | STALE_FILTERED: %d", kept, stale_count)
    res_groups = defaultdict(list)
    for s in filtered:
        res_groups[s.get('res', 'Unknown')].append(s)
    filtered = []
    for group in res_groups.values():
        group.sort(key=lambda s: played_feedback[s['quality']], reverse=True)
        filtered.extend(group[:10])
    logging.info("SAMPLED | PER_RES: %s", {r: len(g) for r, g in res_groups.items()})
    def sort_key(s):
        parse_string = (s.get('name', '') + ' ' + s.get('description', '')).lower()
        service = hints.get('service', '') or ('tb' if 'tb' in parse_string or 'torbox' in parse_string else 'rd' if 'rd' in parse_string or 'realdebrid' in parse_string else 'ad' if 'ad' in parse_string or 'alldebrid' in parse_string else 'unknown')
        if service == 'unknown':
            logging.warning("PROVIDER_MISS | STREAM: %s", s.get('name', ''))
        provider_p = 0 if service == 'tb' else 1 if service == 'rd' else 2 if service == 'ad' else 3
        quality_p = 0 if s.get('quality') == 'Remux' else 1 if s.get('quality') == 'Bluray' else 2 if s.get('quality') == 'Web' else 3
        health_boost = -s.get('health_score', 0) if ('usenet' in parse_string or 'nzb' in parse_string) else 0
        return (provider_p, quality_p, health_boost)
    filtered.sort(key=sort_key)
    delivered = len(filtered)
    logging.info("DELIVERED_COUNT: %d | FINAL", delivered)
    return jsonify({'streams': filtered})
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

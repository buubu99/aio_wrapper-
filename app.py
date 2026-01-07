from flask import Flask, request, jsonify
import requests
import os
import logging

app = Flask(__name__)

# Set logging level from EV (default INFO)
logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))

# Your ElfHosted AIOStreams base URL (set in Render env if different)
AIO_BASE = os.environ.get('AIO_URL', 'https://buubuu99-aiostreams.elfhosted.cc/stremio/acc199cb-6b12-4fa9-be4e-a8ff4c13fa50/eyJpIjoiRTJES0N1ZFBaWm8wb25aS05tNEFsUT09IiwiZSI6InhrVVFtdTFEWm5lcGVrcEh5VUZaejZlcEJLMEMrcXdLakY4UU9zUDJoOFE9IiwidCI6ImEifQ')

# Your ElfHosted Store Stremthru base URL (set in Render env if different)
STORE_BASE = os.environ.get('STORE_URL', 'https://buubuu99-stremthru.elfhosted.cc/stremio/store/eyJzdG9yZV9uYW1lIjoiIiwic3RvcmVfdG9rZW4iOiJZblYxWW5WMU9UazZUV0Z5YVhOellUazVRREV4Tnc9PSIsImhpZGVfY2F0YWxvZyI6dHJ1ZSwid2ViZGwiOnRydWV9')

# Min thresholds to filter low-viability streams (avoid ⏳) - from EVs
MIN_SEEDERS = int(os.environ.get('MIN_SEEDERS', 10))
MIN_SIZE_BYTES = int(os.environ.get('MIN_SIZE_BYTES', 500000000))  # 500MB
MAX_SIZE_BYTES = int(os.environ.get('MAX_SIZE_BYTES', 100000000000))  # 100GB

# Timeout in milliseconds from EV (default 30000 ms = 30s)
REQUEST_TIMEOUT = int(os.environ.get('TIMEOUT', 30000)) / 1000  # Convert to seconds for requests

# Concurrency limit (not used yet, but future-proof)
CONCURRENCY_LIMIT = int(os.environ.get('CONCURRENCY_LIMIT', 20))

@app.route('/manifest.json')
def manifest():
    return jsonify({
        "id": "org.grok.wrapper",
        "version": "1.0.0",
        "name": "Grok AIO Wrapper",
        "description": "Wraps AIOStreams and Store Stremthru to filter and format streams",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "catalogs": [],
        "idPrefixes": ["tt"]
    })

@app.route('/stream/<media_type>/<media_id>.json')
def streams(media_type, media_id):
    try:
        # Fetch from AIOStreams
        aio_url = f"{AIO_BASE}/stream/{media_type}/{media_id}.json"
        aio_response = requests.get(aio_url, timeout=REQUEST_TIMEOUT)
        aio_response.raise_for_status()
        aio_data = aio_response.json()
        all_streams = aio_data.get('streams', [])

        # Fetch from Store Stremthru
        store_url = f"{STORE_BASE}/stream/{media_type}/{media_id}.json"
        store_response = requests.get(store_url, timeout=REQUEST_TIMEOUT)
        store_response.raise_for_status()
        store_data = store_response.json()
        all_streams += store_data.get('streams', [])

        # Filter: Only cached (⚡), min seeders/size, exclude ⏳
        filtered = []
        for s in all_streams:
            hints = s.get('behaviorHints', {})
            title = s.get('title', '').lower()
            if '⏳' in title or not hints.get('isCached', False):
                continue
            # Parse seeders/size from title (assume AIO includes; adjust if needed)
            parts = title.split()
            seeders = 0
            for i, part in enumerate(parts):
                if 'seed' in part and i > 0 and parts[i-1].isdigit():
                    seeders = int(parts[i-1])
                    break
            size_str = next((part for part in parts if 'gb' in part.lower() or 'mb' in part.lower()), '0 gb')
            try:
                size_num = float(size_str.rstrip('gmb'))
            except ValueError:
                size_num = 0
            size = size_num * (10**9 if 'gb' in size_str.lower() else 10**6)
            if seeders >= MIN_SEEDERS and MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES:
                filtered.append(s)

        # Sort: Store/StremThru/4K first, then debrid priority (RD > TB > AD), high-res descending
        def sort_key(s):
            title = s.get('title', '').lower()
            res_priority = {'4k': 0, '2160p': 0, '1080p': 1, '720p': 2}.get(next((r for r in ['4k', '2160p', '1080p', '720p'] if r in title), ''), 3)
            source_priority = 0 if 'store' in title or 'stremthru' in title else (1 if 'rd' in title or 'realdebrid' in title else (2 if 'tb' in title or 'torbox' in title else (3 if 'ad' in title or 'alldebrid' in title else 4)))
            return (source_priority, res_priority)

        filtered.sort(key=sort_key)

        # Reformat: Add ★ for Store/4K/StremThru, dim any lingering ⏳ (should be none)
        for s in filtered:
            title = s.get('title', '')
            if 'store' in title.lower() or '4k' in title.lower() or 'stremthru' in title.lower():
                s['title'] = f"★ {title}"
            if '⏳' in title:
                s['title'] = f"[dim]{title} (Unverified)[/dim]"

        return jsonify({'streams': filtered[:60]})  # Limit to 60 to avoid clutter
    except Exception as e:
        logging.error(f"Error fetching streams: {e}")
        return jsonify({'streams': []}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

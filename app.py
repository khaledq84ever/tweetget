from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
import os, uuid, json, re, glob, threading, time, shutil, subprocess
import requests as req_lib
from collections import defaultdict

try:
    import static_ffmpeg
    static_ffmpeg.add_paths()
except Exception:
    pass

app = Flask(__name__)
CORS(app)

DOWNLOAD_DIR = '/tmp/tw_cache'
FILE_TTL     = 1800
RATE_LIMIT   = 10

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

jobs        = {}
jobs_lock   = threading.Lock()
_rate_store = defaultdict(list)
_rate_lock  = threading.Lock()

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}


# ── Job persistence ───────────────────────────────────────────────────────────

def _job_path(job_id):
    return os.path.join(DOWNLOAD_DIR, f'job_{job_id}.json')

def _save_job(job_id, job):
    try:
        with open(_job_path(job_id), 'w') as f:
            json.dump(job, f)
    except Exception:
        pass

def _load_job_from_disk(job_id):
    try:
        with open(_job_path(job_id)) as f:
            return json.load(f)
    except Exception:
        return None

def _load_all_jobs():
    for p in glob.glob(os.path.join(DOWNLOAD_DIR, 'job_*.json')):
        try:
            with open(p) as f:
                job = json.load(f)
            job_id = os.path.basename(p)[4:-5]
            if job.get('status') in ('pending', 'processing'):
                job['status'] = 'error'
                job['error']  = 'Server restarted. Please try again.'
                _save_job(job_id, job)
            if job.get('status') == 'done' and not os.path.exists(job.get('file', '')):
                os.remove(p); continue
            jobs[job_id] = job
        except Exception:
            pass

_load_all_jobs()


# ── URL helpers ───────────────────────────────────────────────────────────────

_TW_RE = re.compile(
    r'(?:twitter\.com|x\.com)/\w+/status/(\d+)',
    re.IGNORECASE)

def is_valid_url(url):
    return bool(_TW_RE.search(url))

def extract_tweet_id(url):
    m = _TW_RE.search(url)
    return m.group(1) if m else None

def normalize_url(url):
    url = url.strip()
    if not url.startswith('http'):
        url = 'https://' + url
    return url

def make_filename(title, ext='mp4'):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f#@]', '', title or 'tweet').strip()
    name = re.sub(r'\s+', ' ', name)
    return (name[:80] or 'tweet') + '.' + ext

def _find_ffmpeg():
    p = shutil.which('ffmpeg')
    if p: return p
    for d in ['/nix/var/nix/profiles/default/bin', '/usr/bin', '/usr/local/bin']:
        fp = os.path.join(d, 'ffmpeg')
        if os.path.isfile(fp): return fp
    nix = glob.glob('/nix/store/*/bin/ffmpeg')
    return nix[0] if nix else None

def _find_ytdlp():
    p = shutil.which('yt-dlp')
    if p: return p
    for d in ['/nix/var/nix/profiles/default/bin', '/usr/bin', '/usr/local/bin',
              os.path.expanduser('~/.local/bin')]:
        fp = os.path.join(d, 'yt-dlp')
        if os.path.isfile(fp): return fp
    return None


# ── Twitter scraper ───────────────────────────────────────────────────────────

def tw_scrape(tweet_id, url):
    ytdlp = _find_ytdlp()
    if not ytdlp:
        return None, 'yt-dlp not found on server.'

    try:
        result = subprocess.run(
            [ytdlp, '--dump-json', '--no-playlist', url],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            if 'No video could be found' in (result.stderr or ''):
                return None, 'This tweet has no video — for images, use the TweetGet extension or save them directly.'
            return None, 'Could not fetch tweet. Make sure it is a public tweet.'

        # Multi-video tweets make yt-dlp emit one JSON object per video
        # (newline-separated). The tweet text is the same on all of them,
        # so parse just the first line.
        first = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ''
        data = json.loads(first)
        title = data.get('description') or data.get('title') or f'tweet_{tweet_id}'
        uploader = data.get('uploader') or data.get('uploader_id') or ''
        thumb = data.get('thumbnail') or ''
        is_video = data.get('ext') not in ('jpg', 'jpeg', 'png', 'webp', None) or bool(data.get('formats'))
        duration = data.get('duration') or 0

        return {
            'title':    title[:200],
            'uploader': uploader,
            'thumb_url': thumb,
            'is_video': is_video,
            'duration': duration,
            'url':      url,
        }, None

    except subprocess.TimeoutExpired:
        return None, 'Request timed out. Please try again.'
    except Exception as e:
        return None, 'Could not fetch tweet.'


# ── Worker ────────────────────────────────────────────────────────────────────

def _set_job(job_id, updates):
    with jobs_lock:
        jobs[job_id].update(updates)
        _save_job(job_id, jobs[job_id])

def schedule_cleanup(job_id, path):
    def _cleanup():
        time.sleep(FILE_TTL)
        try:
            if os.path.isfile(path):  os.remove(path)
            elif os.path.isdir(path): shutil.rmtree(path, ignore_errors=True)
        except Exception: pass
        try: os.remove(_job_path(job_id))
        except Exception: pass
        with jobs_lock: jobs.pop(job_id, None)
    threading.Thread(target=_cleanup, daemon=True).start()

def do_download(job_id, url, title, fmt):
    _set_job(job_id, {'status': 'processing', 'progress': 5})
    try:
        ytdlp = _find_ytdlp()
        if not ytdlp:
            _set_job(job_id, {'status': 'error', 'error': 'yt-dlp not available.'}); return

        # No title from the client (e.g. /info failed earlier) → fetch the
        # tweet text ourselves so the file isn't named tweet_<uuid>.
        if not title:
            tid = extract_tweet_id(url)
            post, _err = tw_scrape(tid, url) if tid else (None, None)
            if post:
                title = post.get('title') or ''

        file_id  = str(uuid.uuid4())
        out_tmpl = os.path.join(DOWNLOAD_DIR, f'{file_id}.%(ext)s')

        _set_job(job_id, {'progress': 15})

        # --playlist-items 1: multi-video tweets are exposed as a playlist; all
        # entries would write to the same template, so grab the first one only
        # (deterministic, no wasted bandwidth).
        if fmt == 'mp3':
            cmd = [ytdlp, '-x', '--audio-format', 'mp3', '-o', out_tmpl,
                   '--no-playlist', '--playlist-items', '1', url]
        else:
            cmd = [ytdlp, '-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                   '-o', out_tmpl, '--no-playlist', '--playlist-items', '1', url]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        _set_job(job_id, {'progress': 85})

        if result.returncode != 0:
            msg = ('This tweet has no video — for images, use the TweetGet extension or save them directly.'
                   if 'No video could be found' in (result.stderr or '')
                   else 'Download failed. Tweet may be private or contain no media.')
            _set_job(job_id, {'status': 'error', 'error': msg}); return

        ext       = 'mp3' if fmt == 'mp3' else 'mp4'
        out_path  = os.path.join(DOWNLOAD_DIR, f'{file_id}.{ext}')
        if not os.path.exists(out_path):
            found = glob.glob(os.path.join(DOWNLOAD_DIR, f'{file_id}.*'))
            out_path = found[0] if found else None

        if not out_path or not os.path.exists(out_path):
            _set_job(job_id, {'status': 'error', 'error': 'Output file not found.'}); return

        real_ext = os.path.splitext(out_path)[1].lstrip('.')
        t        = title or f'tweet_{file_id}'
        filename = make_filename(t, real_ext)

        _set_job(job_id, {'status': 'done', 'file': out_path,
                           'filename': filename, 'progress': 100})
        schedule_cleanup(job_id, out_path)

    except subprocess.TimeoutExpired:
        _set_job(job_id, {'status': 'error', 'error': 'Download timed out. Please try again.'})
    except Exception:
        _set_job(job_id, {'status': 'error', 'error': 'Download failed. Please try again.'})


# ── Rate limiter ──────────────────────────────────────────────────────────────

def _check_rate(ip):
    now = time.time()
    with _rate_lock:
        _rate_store[ip] = [t for t in _rate_store[ip] if now - t < 60]
        if len(_rate_store[ip]) >= RATE_LIMIT: return False
        _rate_store[ip].append(now)
        return True

def _client_ip():
    return (request.headers.get('X-Forwarded-For', '').split(',')[0].strip()
            or request.remote_addr or 'unknown')


# ── Security headers ──────────────────────────────────────────────────────────

@app.after_request
def add_security_headers(resp):
    resp.headers['X-Frame-Options']        = 'SAMEORIGIN'
    resp.headers['X-Content-Type-Options'] = 'nosniff'
    resp.headers['Referrer-Policy']        = 'strict-origin-when-cross-origin'
    return resp


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/manifest.json')
def manifest():
    return jsonify({"name":"TweetGet","short_name":"TweetGet",
                    "description":"Download Twitter/X videos and GIFs",
                    "start_url":"/","display":"standalone",
                    "background_color":"#0a0a0a","theme_color":"#1d9bf0","icons":[]})

@app.route('/robots.txt')
def robots():
    body = ('User-agent: *\n'
            'Allow: /\n\n'
            'Sitemap: https://tweetget-production.up.railway.app/sitemap.xml\n')
    return body, 200, {'Content-Type': 'text/plain'}

@app.route('/sitemap.xml')
def sitemap():
    today = time.strftime('%Y-%m-%d')
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        '  <url>\n'
        '    <loc>https://tweetget-production.up.railway.app/</loc>\n'
        f'    <lastmod>{today}</lastmod>\n'
        '    <changefreq>weekly</changefreq>\n'
        '    <priority>1.0</priority>\n'
        '  </url>\n'
        '</urlset>\n'
    )
    return xml, 200, {'Content-Type': 'application/xml'}

@app.route('/info', methods=['POST'])
def get_info():
    if not _check_rate(_client_ip()):
        return jsonify({'error': 'Too many requests. Please wait a moment.'}), 429
    data = request.get_json() or {}
    url  = normalize_url(data.get('url', '').strip())
    if not url or not is_valid_url(url):
        return jsonify({'error': 'Invalid Twitter/X URL — paste a tweet link.'}), 400
    tweet_id = extract_tweet_id(url)
    if not tweet_id:
        return jsonify({'error': 'Could not parse tweet URL.'}), 400
    post, err = tw_scrape(tweet_id, url)
    if err or not post:
        return jsonify({'error': err or 'Could not fetch tweet.'}), 400

    dur = post['duration']
    mins, secs = divmod(int(dur), 60) if dur else (0, 0)
    dur_str = f'{mins}:{secs:02d}' if dur else '—'

    return jsonify({
        'title':        post['title'],
        'thumbnail':    post['thumb_url'],
        'uploader':     post['uploader'],
        'is_video':     post['is_video'],
        'duration':     dur_str,
        'duration_sec': dur,
        'url':          url,
    })

@app.route('/start', methods=['POST'])
def start_convert():
    if not _check_rate(_client_ip()):
        return jsonify({'error': 'Too many requests. Please wait a moment.'}), 429
    data  = request.get_json() or {}
    url   = normalize_url(data.get('url', '').strip())
    title = data.get('title', '').strip()
    fmt   = data.get('format', 'mp4')
    if fmt not in ('mp4', 'mp3'): fmt = 'mp4'
    if not is_valid_url(url):
        return jsonify({'error': 'Invalid Twitter/X URL'}), 400

    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {'status': 'pending', 'file': None, 'filename': None,
                         'error': None, 'progress': 0}
        _save_job(job_id, jobs[job_id])

    threading.Thread(target=do_download,
                     args=(job_id, url, title or None, fmt), daemon=True).start()
    return jsonify({'job_id': job_id})

@app.route('/status/<job_id>')
def get_status(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        job = _load_job_from_disk(job_id)
        if job:
            with jobs_lock: jobs[job_id] = job
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify({k: job.get(k) for k in ('status', 'error', 'filename', 'progress')})

@app.route('/download/<job_id>')
@app.route('/download/<job_id>/<path:_fname>')
def download_file(job_id, _fname=None):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        job = _load_job_from_disk(job_id)
        if job:
            with jobs_lock: jobs[job_id] = job
    if not job or job['status'] != 'done':
        return jsonify({'error': 'File not ready — please try again.'}), 404
    path, filename = job['file'], job['filename']
    if not os.path.exists(path):
        return jsonify({'error': 'File expired. Please download again.'}), 410
    safe = re.sub(r'[^\w\s\-\.\(\)]', '', filename).strip() or 'tweet.mp4'
    mime = 'audio/mpeg' if safe.endswith('.mp3') else 'video/mp4'
    return send_file(path, as_attachment=True, download_name=safe, mimetype=mime)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

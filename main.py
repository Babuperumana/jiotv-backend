import asyncio
import asyncio.selector_events
import base64
import json
import os
import random
import re
import time
from contextlib import asynccontextmanager
from uuid import uuid4
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, quote

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse, RedirectResponse, PlainTextResponse, HTMLResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Python 3.13 asyncio bug workaround (Windows only)
# _SelectorSocketTransport._write_send() asserts buffer is non-empty,
# but Starlette/uvicorn can schedule writes after buffer is already drained.
# Monkey-patch to silently return instead of crashing.
# ---------------------------------------------------------------------------
if hasattr(asyncio, "selector_events") and hasattr(asyncio.selector_events, "_SelectorSocketTransport"):
    _orig_write_send = asyncio.selector_events._SelectorSocketTransport._write_send

    def _patched_write_send(self):
        if not self._buffer:
            return
        _orig_write_send(self)

    asyncio.selector_events._SelectorSocketTransport._write_send = _patched_write_send

# ---------------------------------------------------------------------------
# Shared HTTP client (connection pooling — eliminates per-request TCP/TLS churn)
# ---------------------------------------------------------------------------
_http_client: httpx.AsyncClient | None = None
_token_lock = asyncio.Lock()

# Token refresh interval: re-fetch CDN token every 90 seconds proactively
TOKEN_MAX_AGE = 90


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client
    _http_client = httpx.AsyncClient(
        verify=False,
        follow_redirects=True,
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=30),
    )
    yield
    await _http_client.aclose()
    _http_client = None


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Session persistence (single-user POC)
# ---------------------------------------------------------------------------
DATA_DIR = os.environ.get("DATA_DIR", ".")
SESSION_FILE = os.path.join(DATA_DIR, "session.json")
STREAM_CACHE_FILE = os.path.join(DATA_DIR, "stream_cache.json")
SESSION: dict = {}


def _save_session():
    with open(SESSION_FILE, "w") as f:
        json.dump(SESSION, f)


def _load_session():
    global SESSION
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r") as f:
                SESSION = json.load(f)
        except (json.JSONDecodeError, IOError):
            SESSION = {}


# Load on startup
_load_session()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
OTP_HEADERS = {
    "user-agent": "okhttp/4.2.2",
    "os": "android",
    "host": "jiotvapi.media.jio.com",
    "devicetype": "phone",
    "appname": "RJIL_JioTV",
    "Content-Type": "application/json",
}

PLAYBACK_UA = "plaYtv/7.1.5 (Linux;Android 9) ExoPlayerLib/2.11.7"

LANGUAGES = {
    1: "Hindi", 2: "Marathi", 3: "Punjabi", 4: "Urdu", 5: "Bengali",
    6: "English", 7: "Malayalam", 8: "Tamil", 9: "Gujarati", 10: "Odia",
    11: "Telugu", 12: "Bhojpuri", 13: "Kannada", 14: "Assamese",
    15: "Nepali", 16: "French", 21: "Other",
}

CATEGORIES = {
    5: "Entertainment", 6: "Movies", 7: "Kids", 8: "Sports", 9: "Lifestyle",
    10: "Infotainment", 12: "News", 13: "Music", 15: "Devotional",
    16: "Business", 17: "Educational", 18: "Shopping",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _device_info() -> dict:
    return {
        "consumptionDeviceName": "unknown sdk_google_atv_x86",
        "info": {
            "type": "android",
            "platform": {"name": "generic_x86"},
            "androidId": str(uuid4()),
        },
    }


def _is_session_expired() -> bool:
    """Check if the ssotoken JWT is expired."""
    token = SESSION.get("ssotoken", "")
    if not token:
        return True
    try:
        payload = token.split(".")[1]
        # Fix base64 padding
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.b64decode(payload))
        exp = data.get("exp")
        if exp and time.time() > exp:
            return True
    except Exception:
        pass
    return False


def _build_playback_headers() -> dict:
    """Build headers needed for geturl / stream requests."""
    if not SESSION or not SESSION.get("ssotoken"):
        raise HTTPException(status_code=401, detail="Not logged in")
    if _is_session_expired():
        raise HTTPException(status_code=401, detail="Session expired, please re-login")
    return {
        "appName": "RJIL_JioTV",
        "deviceId": SESSION.get("deviceid", ""),
        "devicetype": "phone",
        "os": "android",
        "osversion": "9",
        "partner": "jiotvvod",
        "user-agent": PLAYBACK_UA,
        "usergroup": "tvYR7NSNn7rymo3F",
        "versioncode": "396",
        "platform": "ANDROID_PHONE",
        "dm": "ZUK ZUK Z1",
        "authtoken": SESSION.get("authtoken", ""),
        "ssotoken": SESSION.get("ssotoken", ""),
        "userid": SESSION.get("userid", ""),
        "uniqueid": SESSION.get("uniqueid", ""),
        "crmid": SESSION.get("crmid", ""),
        "subscriberid": SESSION.get("subscriberid", ""),
    }


def _sony_headers() -> dict:
    """Sony-channel specific headers (SAB TV etc.)."""
    base = _build_playback_headers()
    base.update({
        "Host": "jiotvapi.media.jio.com",
        "Appkey": "NzNiMDhlYzQyNjJm",
        "Osversion": "11",
        "Dm": "Google Pixel 5",
        "Uniqueid": SESSION.get("deviceid", ""),
        "Languageid": "6",
        "Sid": "892898ba-f9de-4572-b6c2-e717b0ad",
        "Isott": "false",
        "Lbcookie": "1",
        "Accesstoken": SESSION.get("authtoken", ""),
        "Subscriberid": SESSION.get("subscriberid", ""),
        "analyticsId": SESSION.get("deviceid", ""),
    })
    return base


def _extract_cookie(stream_url: str) -> str:
    """Extract __hdnea__ cookie from stream URL."""
    if "__hdnea__" in stream_url:
        return "__hdnea__" + stream_url.split("__hdnea__")[-1]
    return ""


def _fix_key_url(url: str, base_url: str) -> str:
    """Rewrite key URLs from tv.media.jio.com/fallback/... to the CDN host.

    The key server at tv.media.jio.com returns 403 for direct requests.
    The same key is accessible on the CDN under the same path (minus /fallback/)
    when the __hdnea__ cookie is provided.
    """
    if "tv.media.jio.com/fallback/" in url:
        # Extract the CDN host from the base m3u8 URL
        parsed = urlparse(base_url)
        cdn_host = parsed.scheme + "://" + parsed.netloc
        # Strip the /fallback prefix to get the CDN path
        key_path = url.split("tv.media.jio.com/fallback", 1)[1]
        return cdn_host + key_path
    return url


def _resolve_url(relative: str, base_url: str) -> str:
    """Resolve a possibly-relative URL against a base URL."""
    if relative.startswith("http"):
        return _fix_key_url(relative, base_url)
    # Strip query params before resolving — they contain '/' in __hdnea__ values
    base_no_query = base_url.split("?")[0]
    base_dir = base_no_query.rsplit("/", 1)[0]
    return base_dir + "/" + relative


def _proxy_url_for(url: str, request_base: str) -> str:
    """Build proxy URL for a given upstream URL."""
    if ".m3u8" in url.split("?")[0] or ".m3u8?" in url:
        return request_base + "/api/proxy/m3u8?url=" + quote(url, safe="")
    return request_base + "/api/proxy/segment?url=" + quote(url, safe="")


def _rewrite_m3u8(body: str, base_url: str, cookie: str, request_base: str) -> str:
    """Rewrite URLs inside an m3u8 manifest to go through our proxy."""
    lines = body.splitlines()
    out = []
    for line in lines:
        stripped = line.strip()

        if stripped and not stripped.startswith("#"):
            # Bare URL line (segment or sub-playlist)
            url = _resolve_url(stripped, base_url)
            out.append(_proxy_url_for(url, request_base))

        elif stripped.startswith("#"):
            # Rewrite URI="..." attributes inside # tags
            # e.g. #EXT-X-MAP:URI="init.mp4", #EXT-X-KEY:...URI="https://..."
            def replace_uri(match):
                raw = match.group(1)
                url = _resolve_url(raw, base_url)
                proxied = _proxy_url_for(url, request_base)
                return f'URI="{proxied}"'

            rewritten = re.sub(r'URI="([^"]+)"', replace_uri, stripped)
            out.append(rewritten)
        else:
            out.append(line)

    return "\n".join(out)


def _get_request_base(request: Request) -> str:
    """Get the base URL of this server from the incoming request."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.headers.get("host", "localhost:8888"))
    return f"{scheme}://{host}"


def _update_url_token(url: str, cookie: str) -> str:
    """Replace or append the __hdnea__ token in a URL."""
    if not cookie:
        return url
    if "__hdnea__" in url:
        base = url.split("__hdnea__")[0].rstrip("?&")
    else:
        base = url
    separator = "?" if "?" not in base else "&"
    return base + separator + cookie


async def _refresh_stream_token() -> str:
    """Re-call JioTV geturl API to get a fresh CDN token.

    Uses asyncio.Lock to prevent thundering-herd when many concurrent
    requests all discover the token is expired at the same time.
    """
    async with _token_lock:
        # Double-check: another coroutine may have refreshed while we waited
        token_ts = SESSION.get("_token_ts", 0)
        if time.time() - token_ts < 30:
            return SESSION.get("_cookie", "")

        channel_id = SESSION.get("_channel_id")
        if not channel_id:
            return SESSION.get("_cookie", "")

        try:
            headers = _sony_headers()
            resp = await _http_client.post(
                "https://jiotvapi.media.jio.com/playback/apis/v1/geturl?langId=6",
                data={"stream_type": "Live", "channel_id": channel_id},
                headers=headers,
            )
            data = resp.json()
            stream_url = data.get("result", "")
            if stream_url and stream_url.startswith("http"):
                cookie = _extract_cookie(stream_url)
                SESSION["_cookie"] = cookie
                SESSION["_raw_stream_url"] = stream_url
                SESSION["_token_ts"] = time.time()
                _save_session()
                print(f"[token] refreshed for channel {channel_id}")
                return cookie
        except Exception as e:
            print(f"[token] refresh failed: {e}")

        return SESSION.get("_cookie", "")


async def _ensure_fresh_token() -> str:
    """Return the current cookie, refreshing proactively if it's too old."""
    token_ts = SESSION.get("_token_ts", 0)
    if time.time() - token_ts > TOKEN_MAX_AGE:
        return await _refresh_stream_token()
    return SESSION.get("_cookie", "")


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class SendOtpReq(BaseModel):
    mobile: str

class VerifyOtpReq(BaseModel):
    mobile: str
    otp: str

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/api/session")
async def check_session():
    """Check if a valid session exists."""
    if SESSION and SESSION.get("ssotoken"):
        return {"status": "ok", "logged_in": True}
    return {"status": "ok", "logged_in": False}


@app.post("/api/send-otp")
async def send_otp(body: SendOtpReq):
    mobile = "+91" + body.mobile
    encoded = base64.b64encode(mobile.encode("ascii")).decode("ascii")
    resp = await _http_client.post(
        "https://jiotvapi.media.jio.com/userservice/apis/v1/loginotp/send",
        json={"number": encoded},
        headers=OTP_HEADERS,
    )
    if resp.status_code == 204 or resp.status_code == 200:
        return {"status": "ok", "message": "OTP sent"}
    return {"status": "error", "message": resp.text, "code": resp.status_code}


@app.post("/api/verify-otp")
async def verify_otp(body: VerifyOtpReq):
    global SESSION
    mobile = "+91" + body.mobile
    encoded = base64.b64encode(mobile.encode("ascii")).decode("ascii")
    payload = {
        "number": encoded,
        "otp": body.otp,
        "deviceInfo": _device_info(),
    }
    resp = await _http_client.post(
        "https://jiotvapi.media.jio.com/userservice/apis/v1/loginotp/verify",
        json=payload,
        headers=OTP_HEADERS,
    )
    data = resp.json()
    if not data.get("ssoToken"):
        raise HTTPException(status_code=401, detail=data)

    SESSION = {
        "ssotoken": data.get("ssoToken", ""),
        "userid": data.get("sessionAttributes", {}).get("user", {}).get("uid", ""),
        "uniqueid": data.get("sessionAttributes", {}).get("user", {}).get("unique", ""),
        "crmid": data.get("sessionAttributes", {}).get("user", {}).get("subscriberId", ""),
        "subscriberid": data.get("sessionAttributes", {}).get("user", {}).get("subscriberId", ""),
        "authtoken": data.get("authToken", ""),
        "jtoken": data.get("jToken", ""),
        "deviceid": data.get("deviceId", ""),
    }
    _save_session()
    return {"status": "ok", "message": "Login successful"}


@app.post("/api/logout")
async def logout():
    """Clear session and delete session.json"""
    global SESSION
    SESSION = {}
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)
    return {"status": "ok", "message": "Logged out successfully"}


@app.get("/login", response_class=HTMLResponse)
async def login_page():
    """Serve a simple HTML login page."""
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>JioTV Login</title>
        <style>
            body { font-family: Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background-color: #f0f2f5; }
            .container { background: white; padding: 2rem; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); width: 100%; max-width: 400px; text-align: center; }
            input { width: calc(100% - 20px); padding: 10px; margin: 10px 0; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; }
            button { width: 100%; padding: 10px; background-color: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; margin-top: 10px; }
            button:hover { background-color: #0056b3; }
            #message { margin-top: 15px; color: #d9534f; }
            #otp-section { display: none; }
        </style>
    </head>
    <body>
        <div class="container">
            <h2>JioTV Login</h2>
            <div id="phone-section">
                <input type="text" id="mobile" placeholder="Mobile Number (without +91)" required>
                <button onclick="sendOtp()">Send OTP</button>
            </div>
            <div id="otp-section">
                <input type="text" id="otp" placeholder="Enter OTP" required>
                <button onclick="verifyOtp()">Verify OTP</button>
            </div>
            <p id="message"></p>
        </div>
        <script>
            async function sendOtp() {
                const mobile = document.getElementById('mobile').value;
                document.getElementById('message').innerText = "Sending...";
                const res = await fetch('/api/send-otp', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({mobile})
                });
                const data = await res.json();
                if(data.status === 'ok') {
                    document.getElementById('phone-section').style.display = 'none';
                    document.getElementById('otp-section').style.display = 'block';
                    document.getElementById('message').innerText = "OTP sent to your number.";
                    document.getElementById('message').style.color = "green";
                } else {
                    document.getElementById('message').innerText = data.message || "Failed to send OTP.";
                }
            }
            async function verifyOtp() {
                const mobile = document.getElementById('mobile').value;
                const otp = document.getElementById('otp').value;
                document.getElementById('message').innerText = "Verifying...";
                const res = await fetch('/api/verify-otp', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({mobile, otp})
                });
                if(res.ok) {
                    document.getElementById('message').innerText = "Login successful! You can now use playlist.m3u";
                    document.getElementById('message').style.color = "green";
                } else {
                    const data = await res.json();
                    document.getElementById('message').innerText = data.detail || "Login failed.";
                    document.getElementById('message').style.color = "red";
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/api/filters")
async def get_filters():
    """Return available language and category options."""
    return {
        "languages": [{"id": k, "name": v} for k, v in sorted(LANGUAGES.items(), key=lambda x: x[1])],
        "categories": [{"id": k, "name": v} for k, v in sorted(CATEGORIES.items(), key=lambda x: x[1])],
    }


@app.get("/api/channels")
async def get_channels(lang: str = "", cat: str = ""):
    url = (
        "https://jiotvapi.cdn.jio.com/apis/v3.0/getMobileChannelList/get/"
        "?langId=6&devicetype=phone&os=android&usertype=JIO&version=396"
    )
    resp = await _http_client.get(url)
    data = resp.json()
    channels = data.get("result", [])

    # Always filter to Hindi(1), Punjabi(3)
    ALLOWED_LANGS = {1, 6, 7, 8}
    channels = [ch for ch in channels if ch.get("channelLanguageId") in ALLOWED_LANGS]

    cat_ids = {int(x) for x in cat.split(",") if x.strip()} if cat else set()
    if cat_ids:
        channels = [ch for ch in channels if ch.get("channelCategoryId") in cat_ids]

    # Enrich with names
    for ch in channels:
        ch["languageName"] = LANGUAGES.get(ch.get("channelLanguageId", 0), "")
        ch["categoryName"] = CATEGORIES.get(ch.get("channelCategoryId", 0), "")

    return {"result": channels}


@app.get("/playlist.m3u")
async def get_playlist(request: Request):
    """Generate M3U playlist for all available channels."""
    channels_resp = await get_channels()
    channels = channels_resp.get("result", [])
    
    request_base = _get_request_base(request)
    
    channels.sort(key=lambda x: (x.get("languageName", "Unknown"), x.get("categoryName", "Unknown"), x.get("channel_name", "")))

    lines = ["#EXTM3U"]
    for ch in channels:
        cid = ch.get("channel_id")
        name = ch.get("channel_name", "Unknown")
        logo = ch.get("logoUrl", "")
        if logo:
            logo = f"http://jiotv.catchup.cdn.jio.com/dare_images/images/{logo}"
        group = ch.get("languageName", "Unknown")
        
        # Build EXTINF line
        extinf = f'#EXTINF:-1 tvg-id="{cid}" tvg-name="{name}" tvg-logo="{logo}" group-title="{group}",{name}'
        lines.append(extinf)
        
        # Add Widevine DRM properties for likely encrypted channels (Star, Sony, Viacom18, etc.)
        # This tells TiviMate/Kodi to initialize the DRM decryptor.
        is_premium = ch.get("is_premium")
        broadcaster = ch.get("broadcasterId")
        if is_premium or broadcaster in (6, 230, 55):
            lines.append('#KODIPROP:inputstream.adaptive.license_type=clearkey')
            lines.append(f'#KODIPROP:inputstream.adaptive.license_key=https://keys2.cply.dpdns.org/key?id={cid}')
            lines.append('#EXTVLCOPT:http-user-agent=plaYtv/7.1.3 (Linux;Android 13) - @CloudPlay - ExoPlayerLib/824.0')
        
        # Stream URL
        stream_url = f"{request_base}/api/stream/{cid}.m3u8"
        lines.append(stream_url)
        
    # Append external VOD playlist if available
    try:
        ext_playlist_url = "https://raw.githubusercontent.com/Babuperumana/movies_m3u/refs/heads/main/playlist.m3u"
        resp = await _http_client.get(ext_playlist_url, timeout=5.0)
        if resp.status_code == 200:
            vod_lines = resp.text.splitlines()
            # Skip the first #EXTM3U line if it exists
            if vod_lines and vod_lines[0].strip().startswith("#EXTM3U"):
                vod_lines = vod_lines[1:]
            
            lines.append("\n# --- VOD MOVIES (External) ---")
            lines.extend(vod_lines)
    except Exception as e:
        print(f"Failed to fetch external VOD playlist: {e}")

    return PlainTextResponse("\n".join(lines))


@app.get("/api/stream/{channel_id}.m3u8")
async def stream_channel_direct(channel_id: str, request: Request):
    """Direct stream URL for a channel. Redirects to the proxied m3u8."""
    # Play channel function already does the hard work (fetching, caching, generating proxy URL)
    data = await play_channel(channel_id, request)
    proxy_url = data.get("url")
    if not proxy_url:
        raise HTTPException(status_code=404, detail="Stream not found")
    
    return RedirectResponse(url=proxy_url)


@app.get("/api/play/{channel_id}")
async def play_channel(channel_id: str, request: Request):
    base = _get_request_base(request)

    # Check cache first for instant response
    cached = _get_cached_stream(channel_id)
    if cached:
        cookie = cached["cookie"]
        SESSION["_cookie"] = cookie
        SESSION["_channel_id"] = channel_id
        SESSION["_raw_stream_url"] = cached["raw_url"]
        SESSION["_token_ts"] = cached["ts"]
        _save_session()
        return {"url": cached["url"], "cookie": cookie, "raw_url": cached["raw_url"]}

    headers = _sony_headers()
    resp = await _http_client.post(
        "https://jiotvapi.media.jio.com/playback/apis/v1/geturl?langId=6",
        data={"stream_type": "Live", "channel_id": channel_id},
        headers=headers,
    )
    data = resp.json()
    stream_url = data.get("result", "")
    
    is_mpd = False
    key_url = ""
    hls_url = data.get("result", "")
    
    # Check if HLS URL is valid (returns 200). If it returns 404, fallback to MPD (DRM channel)
    hls_valid = False
    if hls_url and hls_url.startswith("http"):
        try:
            h_resp = await _http_client.head(hls_url, headers={"user-agent": PLAYBACK_UA}, follow_redirects=True, timeout=3.0)
            if h_resp.status_code == 200:
                hls_valid = True
                stream_url = hls_url
        except:
            pass
            
    if not hls_valid:
        mpd_data = data.get("mpd")
        if mpd_data and mpd_data.get("result"):
            stream_url = mpd_data.get("result")
            is_mpd = True
            key_url = mpd_data.get("key", "")
        else:
            stream_url = hls_url # fallback to whatever was originally there

    if not stream_url or not stream_url.startswith("http"):
        # Check if this looks like an auth failure
        error_code = data.get("code") or data.get("errorCode") or ""
        error_msg = data.get("message") or data.get("errorMessage") or ""
        if _is_session_expired() or str(error_code) in ("401", "403", "110", "419"):
            raise HTTPException(status_code=401, detail={
                "message": "Session expired, please re-login",
                "api_response": data,
            })
        raise HTTPException(status_code=502, detail={
            "message": "JioTV returned no stream URL",
            "api_response": data,
        })

    cookie = _extract_cookie(stream_url)

    # Store cookie + metadata for proxy & auto-refresh
    SESSION["_cookie"] = cookie
    SESSION["_channel_id"] = channel_id
    SESSION["_raw_stream_url"] = stream_url
    SESSION["_token_ts"] = time.time()
    _save_session()

    # Cache this stream URL
    if is_mpd:
        # We don't proxy DASH, just return it directly to the CDN!
        # ExoPlayer will receive the 24-hour Set-Cookie from Jio CDN and use it for all segments.
        proxy_url = stream_url
        _stream_cache[channel_id] = {
            "url": proxy_url, "cookie": cookie, "raw_url": stream_url, "ts": time.time(), "key_url": key_url, "is_mpd": True
        }
    else:
        proxy_url = base + "/api/proxy/m3u8?url=" + quote(stream_url, safe="")
        _stream_cache[channel_id] = {
            "url": proxy_url, "cookie": cookie, "raw_url": stream_url, "ts": time.time(), "is_mpd": False
        }

    return {"url": proxy_url, "cookie": cookie, "raw_url": stream_url}


# ---------------------------------------------------------------------------
# Stream URL cache for instant channel switching
# ---------------------------------------------------------------------------
# { channel_id: { "url": proxy_url, "cookie": cookie, "ts": timestamp } }
_stream_cache: dict = {}
STREAM_CACHE_TTL = 60  # seconds


def _get_cached_stream(channel_id: str) -> dict | None:
    entry = _stream_cache.get(channel_id)
    if entry and time.time() - entry["ts"] < STREAM_CACHE_TTL:
        return entry
    return None


async def _fetch_and_cache_stream(channel_id: str, request_base: str) -> dict:
    """Fetch stream URL from JioTV and cache it."""
    cached = _get_cached_stream(channel_id)
    if cached:
        return cached

    try:
        headers = _sony_headers()
        resp = await _http_client.post(
            "https://jiotvapi.media.jio.com/playback/apis/v1/geturl?langId=6",
            data={"stream_type": "Live", "channel_id": channel_id},
            headers=headers,
        )
        data = resp.json()
        stream_url = data.get("result", "")
        
        is_mpd = False
        key_url = ""
        hls_url = data.get("result", "")
        
        hls_valid = False
        if hls_url and hls_url.startswith("http"):
            try:
                h_resp = await _http_client.head(hls_url, headers={"user-agent": PLAYBACK_UA}, follow_redirects=True, timeout=3.0)
                if h_resp.status_code == 200:
                    hls_valid = True
                    stream_url = hls_url
            except:
                pass
                
        if not hls_valid:
            mpd_data = data.get("mpd")
            if mpd_data and mpd_data.get("result"):
                stream_url = mpd_data.get("result")
                is_mpd = True
                key_url = mpd_data.get("key", "")
            else:
                stream_url = hls_url

        if stream_url and stream_url.startswith("http"):
            cookie = _extract_cookie(stream_url)
            if is_mpd:
                proxy_url = stream_url
                entry = {"url": proxy_url, "cookie": cookie, "raw_url": stream_url, "ts": time.time(), "key_url": key_url, "is_mpd": True}
            else:
                proxy_url = request_base + "/api/proxy/m3u8?url=" + quote(stream_url, safe="")
                entry = {"url": proxy_url, "cookie": cookie, "raw_url": stream_url, "ts": time.time(), "is_mpd": False}
            _stream_cache[channel_id] = entry
            return entry
    except Exception as e:
        print(f"[prewarm] failed for {channel_id}: {e}")
    return {}


@app.post("/api/prewarm")
async def prewarm_channels(request: Request):
    """Pre-fetch stream URLs for given channel IDs (fire-and-forget from app)."""
    body = await request.json()
    channel_ids = body.get("channel_ids", [])
    if not channel_ids:
        return {"status": "ok", "prewarmed": 0}

    request_base = _get_request_base(request)
    # Fetch all in parallel
    results = await asyncio.gather(
        *[_fetch_and_cache_stream(str(cid), request_base) for cid in channel_ids],
        return_exceptions=True,
    )
    count = sum(1 for r in results if isinstance(r, dict) and r.get("url"))
    return {"status": "ok", "prewarmed": count}


@app.get("/api/catchup/{channel_id}")
async def get_catchup(channel_id: str, offset: int = 0):
    url = (
        f"https://jiotvapi.cdn.jio.com/apis/v1.3/getepg/get"
        f"?offset={offset}&channel_id={channel_id}&langId=6"
    )
    resp = await _http_client.get(url)
    return resp.json()


@app.get("/api/play/{channel_id}/catchup")
async def play_catchup(
    channel_id: str,
    request: Request,
    showtime: str = "",
    srno: str = "",
    programId: str = "",
    begin: str = "",
    end: str = "",
):
    headers = _sony_headers()
    resp = await _http_client.post(
        "https://jiotvapi.media.jio.com/playback/apis/v1/geturl?langId=6",
        data={
            "channel_id": channel_id,
            "stream_type": "Catchup",
            "begin": begin,
            "end": end,
            "showtime": showtime,
            "srno": srno,
            "programId": programId,
        },
        headers=headers,
    )
    data = resp.json()
    stream_url = data.get("result", "")

    if not stream_url or not stream_url.startswith("http"):
        error_code = data.get("code") or data.get("errorCode") or ""
        if _is_session_expired() or str(error_code) in ("401", "403", "110", "419"):
            raise HTTPException(status_code=401, detail={
                "message": "Session expired, please re-login",
                "api_response": data,
            })
        raise HTTPException(status_code=502, detail={
            "message": "JioTV returned no catchup stream URL",
            "api_response": data,
        })

    cookie = _extract_cookie(stream_url)
    SESSION["_cookie"] = cookie
    _save_session()
    base = _get_request_base(request)
    proxy_url = base + "/api/proxy/m3u8?url=" + quote(stream_url, safe="")
    return {"url": proxy_url, "cookie": cookie, "raw_url": stream_url}


@app.get("/api/proxy/mpd")
async def proxy_mpd(url: str, request: Request, cid: str = ""):
    """Proxy an MPD manifest, rewriting SegmentTemplate to include token."""
    if not url or not url.startswith("http"):
        raise HTTPException(status_code=400, detail="Missing or invalid url parameter")

    try:
        cookie = await _ensure_fresh_token()
        headers = {"user-agent": PLAYBACK_UA, "cookie": cookie}
        fetch_url = _update_url_token(url, cookie)

        resp = await _http_client.get(fetch_url, headers=headers)
        
        if resp.status_code in (403, 410, 401):
            cookie = await _refresh_stream_token()
            headers["cookie"] = cookie
            fetch_url = _update_url_token(url, cookie)
            resp = await _http_client.get(fetch_url, headers=headers)

        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Upstream mpd returned {resp.status_code}")

        xml = resp.text
        # We need the token to append to segment URLs
        token = ""
        if "__hdnea__" in fetch_url:
            parsed = urlparse(fetch_url)
            qs = parse_qs(parsed.query)
            if "__hdnea__" in qs:
                token = qs["__hdnea__"][0]

        request_base = _get_request_base(request)
        
        # We redirect segment fetches through our proxy (which returns a 302 redirect) 
        # so that the token is ALWAYS freshly generated right when the player fetches the segment.
        # This completely eliminates 403 expiry errors while saving local bandwidth!
        xml = xml.replace('<BaseURL>dash/</BaseURL>', '')
        base_dir = fetch_url.split("?")[0].rsplit("/", 1)[0] + "/dash/"
        proxy_prefix = f'{request_base}/api/proxy/segment?url={quote(base_dir, safe="")}'
        
        # Rewrite SegmentTemplate
        xml = xml.replace('initialization="', f'initialization="{proxy_prefix}')
        xml = xml.replace('media="', f'media="{proxy_prefix}')

        # Inject dashif:Laurl if cid is provided and channel has DRM key
        has_key = False
        if cid:
            cached = _get_cached_stream(cid)
            if cached and cached.get("key_url"):
                has_key = True
                
        if has_key:
            request_base = _get_request_base(request)
            laurl = f'<dashif:Laurl>{request_base}/api/proxy/key?cid={cid}</dashif:Laurl>'
            xml = xml.replace('<MPD xmlns:xsi=', '<MPD xmlns:dashif="https://dashif.org/CPI" xmlns:xsi=')
            wv_tag = '<ContentProtection schemeIdUri="urn:uuid:EDEF8BA9-79D6-4ACE-A3C8-27DCD51D21ED">'
            xml = xml.replace(wv_tag, f'{wv_tag}\n        {laurl}')
            
        # Strip SCTE-35 EventStreams to prevent ExoPlayer extractor errors in TiviMate
        import re
        xml = re.sub(r'<EventStream.*?</EventStream>', '', xml, flags=re.DOTALL)
        xml = re.sub(r'<InbandEventStream.*?</InbandEventStream>', '', xml, flags=re.DOTALL)
        # Also remove any self-closing tags just in case
        xml = re.sub(r'<EventStream.*?/>', '', xml)
        xml = re.sub(r'<InbandEventStream.*?/>', '', xml)

        return Response(
            content=xml,
            media_type="application/dash+xml",
            headers={"Cache-Control": "no-cache, no-store"},
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"[mpd] error: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch mpd")


@app.post("/api/proxy/key")
async def proxy_key(request: Request, cid: str = ""):
    """Proxy Widevine DRM license requests for JioTV."""
    body = await request.body()
    
    key_url = ""
    cached = None
    data = None
    # We can use the cid to get the key_url from cache, or fetch it
    if cid:
        cached = _get_cached_stream(cid)
        if cached and cached.get("key_url"):
            key_url = cached["key_url"]
        else:
            data = await _fetch_and_cache_stream(cid, _get_request_base(request))
            key_url = data.get("key_url", "")
            
    if not key_url:
        key_url = request.query_params.get("url", "")
        
    if not key_url or not key_url.startswith("http"):
        raise HTTPException(status_code=404, detail=f"Key URL not found for {cid}")
        
    headers = _sony_headers()
    try:
        resp = await _http_client.post(key_url, content=body, headers=headers)
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            media_type=resp.headers.get("content-type", "application/octet-stream")
        )
    except Exception as e:
        print(f"[key proxy] error: {e}")
        raise HTTPException(status_code=502, detail="Failed to proxy key")


@app.get("/api/proxy/m3u8")
async def proxy_m3u8(url: str, request: Request):
    """Proxy an m3u8 manifest, rewriting inner URLs to also go through proxy."""
    if not url or not url.startswith("http"):
        raise HTTPException(status_code=400, detail="Missing or invalid url parameter")

    try:
        cookie = await _ensure_fresh_token()
        headers = {"user-agent": PLAYBACK_UA, "cookie": cookie}
        fetch_url = _update_url_token(url, cookie)

        resp = await _http_client.get(fetch_url, headers=headers)

        # Token expired → force refresh and retry once
        if resp.status_code in (403, 410, 401):
            print(f"[m3u8] upstream {resp.status_code}, refreshing token…")
            cookie = await _refresh_stream_token()
            headers["cookie"] = cookie
            fetch_url = _update_url_token(url, cookie)
            resp = await _http_client.get(fetch_url, headers=headers)

        if resp.status_code != 200:
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"Upstream m3u8 returned {resp.status_code}",
            )

        body = resp.text
        request_base = _get_request_base(request)
        rewritten = _rewrite_m3u8(body, fetch_url, cookie, request_base)
        return Response(
            content=rewritten,
            media_type=resp.headers.get("content-type", "application/vnd.apple.mpegurl"),
            headers={"Cache-Control": "no-cache, no-store"},
        )
    except HTTPException:
        raise
    except httpx.TimeoutException:
        print(f"[m3u8] timeout fetching {url[:80]}…")
        raise HTTPException(status_code=504, detail="Upstream m3u8 timed out")
    except Exception as e:
        print(f"[m3u8] error: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch m3u8")


@app.get("/api/debug/m3u8")
async def debug_m3u8(url: str, request: Request):
    """Debug: show raw + rewritten m3u8 side by side."""
    if not url or not url.startswith("http"):
        raise HTTPException(status_code=400, detail="Missing or invalid url parameter")
    cookie = SESSION.get("_cookie", "")
    headers = {"user-agent": PLAYBACK_UA, "cookie": cookie}
    resp = await _http_client.get(url, headers=headers)
    raw = resp.text
    request_base = _get_request_base(request)
    rewritten = _rewrite_m3u8(raw, url, cookie, request_base)
    return {"raw": raw, "rewritten": rewritten, "status": resp.status_code}


@app.get("/api/proxy/segment")
async def proxy_segment(url: str):
    """Proxy a TS/segment request via 302 redirect to ensure fresh token and save bandwidth."""
    if not url or not url.startswith("http"):
        raise HTTPException(status_code=400, detail="Missing or invalid url parameter")

    try:
        cookie = await _ensure_fresh_token()
        fetch_url = _update_url_token(url, cookie)
        
        # We use a 302 redirect so the player downloads the actual video data 
        # directly from Jio CDN. This saves massive local bandwidth while still
        # ensuring the token is always fresh to prevent 403 errors!
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=fetch_url, status_code=302)
    except Exception as e:
        print(f"[segment] error: {e}")
        raise HTTPException(status_code=502, detail="Failed to proxy segment")


# ---------------------------------------------------------------------------
# Remote Control — room management + WebSocket relay
# ---------------------------------------------------------------------------
# rooms: { "1234": { "tv": WebSocket | None, "remote": WebSocket | None, "created_at": float } }
rooms: dict = {}

ROOM_EXPIRY_SECS = 3600  # 1 hour


def _generate_room_code() -> str:
    """Generate a unique 4-digit room code."""
    _cleanup_expired_rooms()
    for _ in range(100):
        code = str(random.randint(1000, 9999))
        if code not in rooms:
            return code
    raise HTTPException(status_code=503, detail="Could not generate room code")


def _cleanup_expired_rooms():
    """Remove rooms older than 1 hour."""
    now = time.time()
    expired = [code for code, room in rooms.items() if now - room["created_at"] > ROOM_EXPIRY_SECS]
    for code in expired:
        rooms.pop(code, None)


@app.post("/api/remote/create-room")
async def create_room():
    code = _generate_room_code()
    rooms[code] = {"tv": None, "remote": None, "created_at": time.time()}
    return {"code": code}


@app.get("/api/remote/check-room/{code}")
async def check_room(code: str):
    _cleanup_expired_rooms()
    if code not in rooms:
        raise HTTPException(status_code=404, detail="Room not found")
    return {"code": code, "exists": True}


@app.websocket("/ws/remote/{room_code}")
async def ws_remote(websocket: WebSocket, room_code: str):
    role = websocket.query_params.get("role", "")
    await websocket.accept()

    if role not in ("tv", "remote"):
        await websocket.send_json({"type": "error", "message": "Invalid role"})
        await websocket.close(code=4000)
        return
    if room_code not in rooms:
        await websocket.send_json({"type": "error", "message": "Room not found"})
        await websocket.close(code=4001)
        return

    room = rooms[room_code]
    room[role] = websocket
    peer_role = "remote" if role == "tv" else "tv"

    # Notify the other side that a peer joined
    peer = room.get(peer_role)
    if peer:
        try:
            await peer.send_json({"type": "peer_joined", "role": role})
        except Exception:
            pass

    try:
        while True:
            msg = await websocket.receive()
            # Handle both text and disconnect frames
            if msg.get("type") == "websocket.disconnect":
                break
            text = msg.get("text")
            if text is None:
                continue
            try:
                data = json.loads(text)
            except (json.JSONDecodeError, TypeError):
                continue
            # Relay to the other side
            other = room.get(peer_role)
            if other:
                try:
                    await other.send_json(data)
                except Exception:
                    pass
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[remote ws] error room={room_code} role={role}: {e}")
    finally:
        # Clean up
        if room_code in rooms:
            rooms[room_code][role] = None
            # Notify peer about disconnect
            other = rooms[room_code].get(peer_role)
            if other:
                try:
                    await other.send_json({"type": "peer_left", "role": role})
                except Exception:
                    pass
            # If TV disconnects, destroy the room
            if role == "tv":
                rooms.pop(room_code, None)

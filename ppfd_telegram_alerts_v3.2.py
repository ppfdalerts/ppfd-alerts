# ppfd_telegram_alerts_v3.2.py – Telegram + Tapo P105 Smart Plug integration
import os, json, time, hashlib, datetime, requests, certifi, math, random, re, sys
from collections import defaultdict
from typing import Dict, Any, List, Set, Tuple
from requests.exceptions import ReadTimeout

# --- Log everything to alerts.log (console and errors) ---
def _open_log_with_retry(filename: str, attempts: int = 15, delay: float = 0.2):
    import time as _t
    last_err = None
    for _ in range(max(1, attempts)):
        try:
            return open(filename, "a", encoding='utf-8')
        except Exception as e:
            last_err = e
            try:
                _t.sleep(delay)
            except Exception:
                pass
    # Fallback to stdout if file can't be opened
    try:
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  WARN: log open failed for {filename}: {last_err}")
    except Exception:
        pass
    return sys.stdout

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
    _LOG_FP = os.path.join(_HERE, "alerts.log")
except Exception:
    _LOG_FP = "alerts.log"
sys.stdout = _open_log_with_retry(_LOG_FP)
sys.stderr = sys.stdout

def log(msg: str):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{ts}  {msg}", flush=True)

# Verbose debug logging (turn off with DEBUG_VERBOSE=0)
DEBUG_VERBOSE = 1 if os.environ.get('DEBUG_VERBOSE', '1').strip() not in ('0','false','False') else 0
def dbg(msg: str):
    if DEBUG_VERBOSE:
        log(f"DEBUG: {msg}")

log("PPFD Telegram alert service started")

# --- Minimal .env loader (no external deps) ---
def _load_dotenv(path: str = ".env"):
    # Load from current working dir first, then alongside this script as a fallback.
    try:
        candidates = []
        try:
            candidates.append(path)
        except Exception:
            pass
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            candidates.append(os.path.join(here, ".env"))
        except Exception:
            pass
        # Also check a central config/.env in repo root (both from CWD and relative to this script)
        try:
            candidates.append(os.path.join(os.getcwd(), "config", ".env"))
        except Exception:
            pass
        try:
            candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, "config", ".env"))
        except Exception:
            pass
        loaded_any = False
        for fp in candidates:
            try:
                if not fp or not os.path.exists(fp):
                    continue
                # utf-8-sig will strip BOM if present
                with open(fp, 'r', encoding='utf-8-sig') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        if '=' in line:
                            k, v = line.split('=', 1)
                            k = k.strip().lstrip('\ufeff')
                            v = v.strip().strip('"').strip("'")
                            # If the var is missing OR empty, set from .env
                            if (k not in os.environ) or (not os.environ.get(k)):
                                os.environ[k] = v
                loaded_any = True
                log(f"Loaded .env from {fp}")
            except Exception as e:
                # Keep going to next candidate
                log(f".env load error from {fp}: {e}")
        if not loaded_any:
            log("No .env file found in CWD or script directory")
    except Exception as e:
        try:
            log(f".env load wrapper error: {e}")
        except Exception:
            pass

_load_dotenv()

# --- Test mode guard ---
# When TEST_MODE is enabled (env var set to 1/true/yes/on),
# the script becomes import-safe: it skips starting the main loop
# and avoids sending the initial status message. This enables unit testing.
TEST_MODE = str(os.environ.get("TEST_MODE", "0")).strip().lower() in ("1", "true", "yes", "on")

# --- Config (from environment / .env) ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
try:
    CHAT_ID = int(os.environ.get("CHAT_ID", "0"))
except Exception:
    CHAT_ID = 0

# Startup diagnostics
try:
    dbg(f"cwd={os.getcwd()} file={__file__} pid={os.getpid()} py={sys.version.split()[0]}")
    dbg(f"BOT_TOKEN={'set' if BOT_TOKEN else 'MISSING'} len={len(BOT_TOKEN)} CHAT_ID={CHAT_ID}")
except Exception:
    pass

THREAD_IDS = {
    "GENERAL": 1, "R33": 2, "E33": 3, "T33": 4, "33FD": 5, "LR36": 6,
    "HM33": 7, "R34": 8, "E34": 9, "TR34": 10, "E36": 11, "S36": 12,
    "R36": 13, "E35": 14, "34FD": 15, "36FD": 16, "D35": 17, "R35": 18,
    "35FD": 19, "LOG": 20, "E136": 7126,
}
BASE_URL = "https://911.pinellas.gov/files/Activity.json"
POLL_SEC = 5

WATCH_SET = set(THREAD_IDS) - {"GENERAL"}
ID2UNIT = {v: k for k, v in THREAD_IDS.items()}

# --- Pre-dispatch early alert config ---
# Optional geofence file to enable early alerts before a unit is assigned.
# Create a file named `geofences.json` next to this script using the
# structure shown in `geofences.example.json` (added in repo).
GEOFENCE_FILE = "geofences.json"
# Default disabled unless explicitly enabled (per repo policy)
EARLY_ALERT_ENABLED = str(os.environ.get("ENABLE_EARLY_ALERTS", "0")).strip().lower() in ("1", "true", "yes", "on")
EARLY_ALERT_TITLE_PREFIX = os.environ.get("EARLY_ALERT_TITLE_PREFIX", "EARLY").strip() or "EARLY"
EARLY_SEEN: Set[Tuple[str, str]] = set()  # (incident_id, unit)

# --- Timezone/Session ---
# Use local system timezone (DST-aware) to avoid 1-hour drift across DST changes.
TZ = datetime.datetime.now().astimezone().tzinfo
sess = requests.Session()
sess.verify = certifi.where()
sess.headers.update({"User-Agent": "PPFD-Telegram/3.2"})

# Detect whether the target chat supports forum topics (threaded messages).
# If not, we will omit message_thread_id in Telegram requests.
def _detect_forum_enabled():
    try:
        r = sess.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getChat",
                     params={"chat_id": CHAT_ID}, timeout=10)
        j = r.json()
        if j.get("ok"):
            res = j.get("result", {})
            val = bool(res.get("is_forum"))
            try:
                log(f"Telegram chat forum support: {'ENABLED' if val else 'DISABLED'}")
            except Exception:
                pass
            return val
    except Exception as e:
        try:
            log(f"Forum detection error (ignored): {e}")
        except Exception:
            pass
    return False

# In test mode, assume forum is enabled to keep tests simple; at runtime detect.
FORUM_ENABLED = True if TEST_MODE else _detect_forum_enabled()

# --- Persistence ---
SHIFT_HOUR = 7

# Stats storage directory (default to repo's data/shift_stats, override via SHIFT_STATS_DIR)
try:
    _REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
except Exception:
    _REPO_ROOT = os.getcwd()
DEFAULT_STATS_DIR = os.path.join(_REPO_ROOT, "data", "shift_stats")
STATS_DIR = os.environ.get("SHIFT_STATS_DIR", DEFAULT_STATS_DIR)
try:
    os.makedirs(STATS_DIR, exist_ok=True)
except Exception as _e:
    try:
        log(f"WARN: Could not create stats dir {STATS_DIR}: {_e}")
    except Exception:
        pass

def shift_start(now):
    base = now.replace(hour=SHIFT_HOUR, minute=0, second=0, microsecond=0)
    return base if now >= base else base - datetime.timedelta(days=1)

def stats_file(dt):
    return os.path.join(STATS_DIR, f"shift_stats_{dt:%Y-%m-%d}.json")

def load(fp):
    if os.path.exists(fp):
        try:
            with open(fp) as f:
                j = json.load(f)
                return (defaultdict(int, j.get("calls", {})),
                        defaultdict(int, j.get("dur_sec", {})),
                        defaultdict(int, j.get("after_0000", {})),
                        defaultdict(int, j.get("max_sec", {})))
        except Exception:
            pass
    return defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
def save(fp, calls, dur, after, max_sec):
    try:
        with open(fp + ".tmp", "w") as f:
            json.dump({"calls": calls, "dur_sec": dur, "after_0000": after, "max_sec": max_sec}, f)
        os.replace(fp + ".tmp", fp)
    except Exception as e:
        log(f"Persist error: {e}")

# Fail fast if required Telegram env missing
if not BOT_TOKEN or not CHAT_ID:
    if TEST_MODE:
        log("TEST MODE: Skipping BOT_TOKEN/CHAT_ID check.")
    else:
        log("ERROR: Missing BOT_TOKEN or CHAT_ID environment variables. Configure .env or system env.")
        raise SystemExit(2)

NOW = datetime.datetime.now(TZ)
SHIFT_DT = shift_start(NOW)
STATS_FN = stats_file(SHIFT_DT)
CALLS, DUR_SEC, AFTER_0000, MAX_SEC = load(STATS_FN)


# Startup announce guard
START_ANNOUNCED = False

def _announce_start():
    global START_ANNOUNCED
    try:
        post("LOG", "SCRIPT STATUS", "PPFD alert script started successfully at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        START_ANNOUNCED = True
    except Exception as e:
        try:
            log(f"Startup announce error: {e}")
        except Exception:
            pass

# --- Leaderboard helpers ---
STATS_FILENAME_RE = re.compile(r"shift_stats_(\d{4}-\d{2}-\d{2})\.json")

TIMEFRAME_LENGTHS = {
    "day": 1,
    "week": 7,
    "month": 30,
    "year": 365,
}

LEADERBOARD_COMMANDS = {
    "/day": ("Daily", "day"),
    "/week": ("Weekly", "week"),
    "/month": ("Monthly", "month"),
    "/year": ("Yearly", "year"),
    "/alltime": ("All-time", "alltime"),
}

LEADERBOARD_LABELS = {
    "day": "Daily",
    "week": "Weekly",
    "month": "Monthly",
    "year": "Yearly",
    "alltime": "All-time",
}

def aggregate_timeframe_stats(period_key, now=None):
    now = now or datetime.datetime.now(TZ)
    cutoff_date = None
    if period_key != "alltime":
        shift_date = shift_start(now).date()
        days = TIMEFRAME_LENGTHS[period_key]
        cutoff_date = shift_date - datetime.timedelta(days=days - 1)
    calls = defaultdict(int)
    dur = defaultdict(int)
    after_midnight = defaultdict(int)
    try:
        names = os.listdir(STATS_DIR)
    except Exception:
        names = []
    for name in names:
        match = STATS_FILENAME_RE.fullmatch(name)
        if not match:
            continue
        try:
            file_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()
        except Exception:
            continue
        if cutoff_date and file_date < cutoff_date:
            continue
        file_path = os.path.join(STATS_DIR, name)
        file_calls, file_dur, file_after = load(file_path)
        for unit, count in file_calls.items():
            if unit in WATCH_SET:
                calls[unit] += int(count)
        for unit, seconds in file_dur.items():
            if unit in WATCH_SET:
                dur[unit] += int(seconds)
        for unit, count in file_after.items():
            if unit in WATCH_SET:
                after_midnight[unit] += int(count)
    return dict(calls), dict(dur), dict(after_midnight)

def format_leaderboard_body(label, period_key, calls, dur, after_midnight, now):
    shift_date = shift_start(now).date()
    if period_key == "alltime":
        header = f"{label} runs through {shift_date:%d %b %Y}"
    else:
        days = TIMEFRAME_LENGTHS[period_key]
        if days == 1:
            header = f"{label} runs {shift_date:%d %b %Y}"
        else:
            start_date = shift_date - datetime.timedelta(days=days - 1)
            header = f"{label} runs {start_date:%d %b %Y} - {shift_date:%d %b %Y}"
    if not calls:
        return header + "\nNo runs recorded."
    lines = [header]
    for unit, count in sorted(calls.items(), key=lambda kv: (-kv[1], kv[0])):
        avg_min = (dur.get(unit, 0) / count) / 60 if count else 0
        lines.append(f"{unit}: {count}  |  avg {avg_min:.1f} min  |  after 00:00: {after_midnight.get(unit, 0)}")
    return "\n".join(lines)

# --- Runtime State ---
ACTIVE: dict[Tuple[str, str], dict] = {}
LAST_FINISHED: dict[str, list] = {}
SEEN_MSG: Set[str] = set()
OFFSET = 0
etag = None
last_mod = None

# --- Live Leaderboard State ---
LIVE_STATE_FN = "live_state.json"
def _live_default():
    return {
        "active": False,
        "period": "day",          # one of: day, week, month, year, alltime
        "threads": ["LOG"],        # list of unit keys to show live board in
        "msg_ids": {},             # unit -> telegram message_id for live board
        "next_update_sec": 30,     # min seconds between edits
    }

def load_live_state():
    try:
        if os.path.exists(LIVE_STATE_FN):
            with open(LIVE_STATE_FN, "r", encoding="utf-8") as f:
                st = json.load(f)
                base = _live_default()
                base.update(st)
                base["threads"] = [t for t in base.get("threads", []) if t in THREAD_IDS]
                return base
    except Exception as e:
        log(f"Live state load error: {e}")
    return _live_default()

def save_live_state(state):
    try:
        with open(LIVE_STATE_FN + ".tmp", "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(LIVE_STATE_FN + ".tmp", LIVE_STATE_FN)
    except Exception as e:
        log(f"Live state save error: {e}")

LIVE = load_live_state()
stats_dirty = False

# --- Helpers ---
def api_url():
    return f"{BASE_URL}?_time={int(time.time() * 1000)}"

def parse_ts(hms):
    if not hms: return None
    h, m, s = map(int, hms.split(":"))
    d = datetime.date.today()
    return datetime.datetime(d.year, d.month, d.day, h, m, s, tzinfo=TZ)

# --- Geofence helpers (for pre-dispatch early alerts) ---
def _haversine_km(lat1, lon1, lat2, lon2):
    try:
        from math import radians, sin, cos, sqrt, atan2
        R = 6371.0
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c
    except Exception:
        return 999999.0

def load_geofences(path=GEOFENCE_FILE):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Normalize keys to known units only
            return {k: v for k, v in data.items() if k in THREAD_IDS}
    except Exception as e:
        log(f"Failed to load geofences: {e}")
        return {}

GEOFENCES = load_geofences()

def is_in_geofence(unit: str, grid: str | None, lat: str | float | None, lon: str | float | None) -> bool:
    gf = GEOFENCES.get(unit)
    if not gf:
        return False
    # Grid checks (exact or prefix)
    g = (grid or "").strip().upper()
    for code in gf.get("grids", []) or []:
        code = str(code).strip().upper()
        if not code:
            continue
        if g == code or (code.endswith("*") and g.startswith(code[:-1])):
            return True
    # Circle checks
    try:
        if lat is None or lon is None or lat == "" or lon == "":
            pass
        else:
            latf = float(lat)
            lonf = float(lon)
            for c in gf.get("circles", []) or []:
                try:
                    clat = float(c.get("lat"))
                    clon = float(c.get("lon"))
                    r_m = float(c.get("radius_m", 0))
                    r_km = float(c.get("radius_km", r_m / 1000 if r_m else 0))
                    if r_km <= 0:
                        continue
                    if _haversine_km(latf, lonf, clat, clon) <= r_km:
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False

# --- Telegram Send ---
TG_SEND = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
TG_EDIT = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
def post(unit, title, body):
    tid = THREAD_IDS.get(unit)
    if tid is None:
        return
    payload = {
        "chat_id": CHAT_ID,
        "text": f"<b>{title}</b>\n{body}",
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if FORUM_ENABLED:
        payload["message_thread_id"] = tid
    try:
        resp = sess.post(TG_SEND, json=payload, timeout=15)
        try:
            rj = resp.json()
            if not rj.get("ok", False):
                desc = str(rj.get("description", ""))
                dl = desc.lower()
                if ("thread not found" in dl) or ("forum topics" in dl) or ("not a forum" in dl) or ("forum is" in dl) or ("topic" in dl and "not" in dl):
                    # Fallback: send to chat root without thread id
                    payload2 = dict(payload)
                    payload2.pop("message_thread_id", None)
                    r2 = sess.post(TG_SEND, json=payload2, timeout=15).json()
                    if not r2.get("ok", False):
                        log(f"Telegram send fallback failed [{unit}] {title}: {r2}")
                    else:
                        log(f"SENT (no-thread) [{unit}] {title} - {body.replace(chr(10), ' | ')}")
                else:
                    log(f"Telegram send failed [{unit}] {title}: {rj}")
            else:
                log(f"SENT [{unit}] {title} - {body.replace(chr(10), ' | ')}")
        except Exception:
            # Fall back to status code only if body isn't JSON
            if resp.status_code >= 400:
                log(f"Telegram send HTTP {resp.status_code} [{unit}] {title}")
            else:
                log(f"SENT [{unit}] {title} - {body.replace(chr(10), ' | ')}")
    except ReadTimeout:
        log("Telegram send timeout (ignored)")
    except Exception as e:
        log(f"Telegram send error: {e}")

def post_return_id(unit, title, body):
    tid = THREAD_IDS.get(unit)
    if tid is None: return None
    payload = {
        "chat_id": CHAT_ID,
        "text": f"<b>{title}</b>\n{body}",
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if FORUM_ENABLED:
        payload["message_thread_id"] = tid
    try:
        r = sess.post(TG_SEND, json=payload, timeout=15).json()
        if r.get("ok") and r.get("result"):
            mid = r["result"].get("message_id")
            log(f"SENT-ID [{unit}] {title} mid={mid}")
            return mid
        else:
            desc = str(r.get("description", ""))
            dl = desc.lower()
            if ("thread not found" in dl) or ("forum topics" in dl) or ("not a forum" in dl) or ("forum is" in dl) or ("topic" in dl and "not" in dl):
                payload2 = dict(payload)
                payload2.pop("message_thread_id", None)
                r2 = sess.post(TG_SEND, json=payload2, timeout=15).json()
                if r2.get("ok") and r2.get("result"):
                    mid = r2["result"].get("message_id")
                    log(f"SENT-ID (no-thread) [{unit}] {title} mid={mid}")
                    return mid
                log(f"Telegram send fallback (no id) resp={r2}")
            else:
                log(f"Telegram send (no id) resp={r}")
    except ReadTimeout:
        log("Telegram send timeout (ignored)")
    except Exception as e:
        log(f"Telegram send error: {e}")
    return None

def edit_message(unit, message_id, title, body):
    if message_id is None:
        return False
    payload = {
        "chat_id": CHAT_ID,
        "message_id": message_id,
        "text": f"<b>{title}</b>\n{body}",
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = sess.post(TG_EDIT, json=payload, timeout=15)
        try:
            rj = r.json()
            if rj.get("ok"):
                log(f"EDIT [{unit}] {title} mid={message_id}")
                return True
            else:
                log(f"Telegram edit failed mid={message_id}: {rj}")
                return False
        except Exception:
            if r.status_code >= 400:
                log(f"Telegram edit HTTP {r.status_code} mid={message_id}")
                return False
            log(f"EDIT [{unit}] {title} mid={message_id}")
            return True
    except ReadTimeout:
        log("Telegram edit timeout (ignored)")
    except Exception as e:
        log(f"Telegram edit error: {e}")
    return False

def update_live_leaderboard(now):
    try:
        if not LIVE.get("active"):
            return
        period_key = LIVE.get("period", "day")
        label = LEADERBOARD_LABELS.get(period_key, period_key.title())
        calls, dur, after = aggregate_timeframe_stats(period_key, now=now)
        body = format_leaderboard_body(label, period_key, calls, dur, after, now)
        title = f"LIVE {label.upper()}"
        # Ensure a message exists per configured thread, then edit it
        msg_ids = LIVE.get("msg_ids", {})
        changed = False
        for unit in LIVE.get("threads", []):
            mid = msg_ids.get(unit)
            if mid is None:
                mid = post_return_id(unit, title, body)
                if mid is not None:
                    msg_ids[unit] = mid
                    changed = True
            else:
                ok = edit_message(unit, mid, title, body)
                if not ok:
                    # Try to send a new one if edit failed
                    mid2 = post_return_id(unit, title, body)
                    if mid2 is not None:
                        msg_ids[unit] = mid2
                        changed = True
        if changed:
            LIVE["msg_ids"] = msg_ids
            save_live_state(LIVE)
    except Exception as e:
        log(f"update_live_leaderboard error: {e}")

if not TEST_MODE:
    _announce_start()

# --- Recap/Backoff ---
def next_at(hr, now):
    tgt = now.replace(hour=hr, minute=0, second=0, microsecond=0)
    return tgt if now < tgt else tgt + datetime.timedelta(days=1)
next_morning = next_at(7, NOW)
next_evening = next_at(19, NOW)
next_live = NOW

backoff = POLL_SEC

# --- Sunstar tracking state ---
SUNSTAR_TRACK: dict[tuple, set] = {}  # (incident_id, fd_unit) -> set of sunstar units

# ===================== MAIN LOOP ====================================
while not TEST_MODE:
    now = datetime.datetime.now(TZ)
    headers = {"Cache-Control": "no-cache"}
    if etag: headers["If-None-Match"] = etag
    if last_mod: headers["If-Modified-Since"] = last_mod

    try:
        url = api_url()
        dbg(f"fetch {url}")
        r = sess.get(url, headers=headers, timeout=20)
        if r.status_code == 304:
            dbg("feed 304 not modified")
            time.sleep(backoff + random.uniform(0, 1))
            continue
        # After first successful fetch attempt, ensure startup announce was sent
        if not START_ANNOUNCED and not TEST_MODE:
            try:
                _announce_start()
            except Exception:
                pass

        etag, last_mod = r.headers.get("ETag", etag), r.headers.get("Last-Modified", last_mod)

        data = r.json()
        items = data.get("CallInfo", []) if isinstance(data, dict) else []
        dbg(f"feed 200 items={len(items)}")
        for it in items:
            iid = str(it.get("IncidentNo") or hashlib.sha1(
                f"{it.get('Type')}{it.get('Location')}{it.get('Received')}".encode()).hexdigest())
            units = [str(u.get("ID") or "").strip().upper() for u in it.get("Units", [])]
            statuses = {str(u.get("ID") or "").upper(): str(u.get("Status") or "").lower() for u in it.get("Units", []) if u.get("ID")}

            if any(u in WATCH_SET for u in units):
                try:
                    dbg(f"match iid={iid} units={units} statuses={[statuses.get(u,'') for u in units]}")
                except Exception:
                    pass
            # --- EARLY ALERT: Pre-dispatch geofence alerts by unit ---
            if EARLY_ALERT_ENABLED and GEOFENCES:
                grid = (it.get("Grid") or "").strip().upper()
                lat = it.get("Lat")
                lon = it.get("Lon")
                ctype = it.get("Type", "Call").strip()
                tac = (it.get("Tac") or "").strip()
                for unit in GEOFENCES.keys():
                    if unit in units:
                        continue  # unit already assigned
                    key_early = (iid, unit)
                    if key_early in EARLY_SEEN:
                        continue
                    if is_in_geofence(unit, grid, lat, lon):
                        title = f"{EARLY_ALERT_TITLE_PREFIX}: {ctype}" + (f"  TAC {tac}" if tac else "")
                        body_parts = []
                        # Do not include address for medical calls
                        if not ctype.upper().startswith("MEDICAL"):
                            loc = (it.get("Location") or "").strip()
                            if loc:
                                body_parts.append(loc)
                            if lat and lon:
                                apple = f"https://maps.apple.com/?ll={lat},{lon}"
                                gmap = f"https://www.google.com/maps?q={lat},{lon}"
                                body_parts.append(f'<a href="{apple}">View map (Apple)</a> | <a href="{gmap}">Google</a>')
                        if grid:
                            body_parts.append(f"Grid: {grid}")
                        if units:
                            body_parts.append(f"Units: {', '.join(units)}")
                        else:
                            body_parts.append("Units: none yet")
                        ts = parse_ts(it.get("Received"))
                        body_parts.append(f"Time:  {ts.strftime('%H:%M') if ts else 'N/A'}")
                        body = "\n".join(body_parts)
                        post(unit, title, body)
                        EARLY_SEEN.add(key_early)

            # ---- SUNSTAR LOGIC ----
            def is_sunstar(unit_id):
                return re.fullmatch(r"\d{3}", unit_id) is not None

            fd_units = [u for u in units if u in WATCH_SET]
            sunstar_units = {u for u in units if is_sunstar(u)}

            for fd in fd_units:
                key = (iid, fd)
                prev_sunstar = SUNSTAR_TRACK.get(key, set())
                added = sunstar_units - prev_sunstar
                removed = prev_sunstar - sunstar_units

                # Only alert FD unit if their Sunstar changes (not on initial assignment)
                if prev_sunstar:
                    for ss in added:
                        post(fd, f"SUNSTAR {ss} ADDED TO CALL", "")
                        log(f"SUNSTAR {ss} ADDED for {fd} on {iid}")
                    for ss in removed:
                        post(fd, f"SUNSTAR {ss} REMOVED FROM THE CALL", "")
                        log(f"SUNSTAR {ss} REMOVED for {fd} on {iid}")
                # Update state
                if sunstar_units:
                    SUNSTAR_TRACK[key] = set(sunstar_units)
                elif key in SUNSTAR_TRACK:
                    del SUNSTAR_TRACK[key]

            # Clean up SUNSTAR_TRACK if FD unit is no longer on call
            for key in list(SUNSTAR_TRACK):
                incident_id, fd = key
                if incident_id == iid and fd not in units:
                    del SUNSTAR_TRACK[key]
            # ---- END SUNSTAR LOGIC ----

            seen_now = set()
            for uid in units:
                key = (iid, uid)
                status = statuses.get(uid, "")
                rec = ACTIVE.get(key)

                if rec is None:
                    # Shift-aware init: do not credit on-coming shift for calls received before 07:00 local.
                    rcv = parse_ts(it.get("Received"))
                    before_shift = False
                    try:
                        before_shift = (rcv is not None and rcv < SHIFT_DT)
                    except Exception:
                        before_shift = False
                    ACTIVE[key] = rec = {"status": status, "start": now, "events": [("dispatched", now)], "ignore": bool(before_shift)}
                    if uid in WATCH_SET and not before_shift:
                        CALLS[uid] += 1
                        try:
                            if rcv and rcv.time() < datetime.time(7):
                                AFTER_0000[uid] += 1
                        except Exception:
                            pass
                        save(STATS_FN, CALLS, DUR_SEC, AFTER_0000, MAX_SEC)
                        try:
                            dbg(f"stats init uid={uid} calls={CALLS.get(uid,0)} after={AFTER_0000.get(uid,0)} file={STATS_FN}")
                        except Exception:
                            pass
                        stats_dirty = True
                elif status != rec["status"]:
                    rec["status"] = status
                    rec["events"].append((status, now))
                seen_now.add(key)

            for key in list(ACTIVE):
                if key[0] == iid and key not in seen_now:
                    rec = ACTIVE.pop(key)
                    rec["events"].append(("available", now))
                    uid = key[1]
                    LAST_FINISHED[uid] = rec["events"]
                    if uid in WATCH_SET and not rec.get("ignore"):
                        dur_sec = (rec["events"][-1][1] - rec["events"][0][1]).total_seconds()
                        DUR_SEC[uid] += dur_sec
                        MAX_SEC[uid] = max(int(MAX_SEC.get(uid, 0)), int(dur_sec))
                        save(STATS_FN, CALLS, DUR_SEC, AFTER_0000, MAX_SEC)
                        try:
                            dbg(f"stats updated uid={uid} dur+= {int(dur_sec)} file={STATS_FN}")
                        except Exception:
                            pass
                        stats_dirty = True

            # --- ALERT: New Call (One per unit, and once to LOG/ALL UNITS) ---
            if iid not in SEEN_MSG and any(u in WATCH_SET for u in units):
                ctype = it.get("Type", "Call").strip()
                tac = it.get("Tac", "").strip()
                title = f"{ctype}" + (f"  TAC {tac}" if tac else "")
                body_parts = []
                if not ctype.upper().startswith("MEDICAL"):
                    loc = (it.get("Location") or "").strip()
                    lat = it.get("Lat", ""); lon = it.get("Lon", "")
                    body_parts.append(loc or "Location N/A")
                    if lat and lon:
                        apple = f"https://maps.apple.com/?ll={lat},{lon}"
                        gmap = f"https://www.google.com/maps?q={lat},{lon}"
                        body_parts.append(f'<a href="{apple}">View map (Apple)</a> | <a href="{gmap}">Google</a>')
                body_parts.append(f"Units: {', '.join(units)}")
                ts = parse_ts(it.get("Received"))
                body_parts.append(f"Time:  {ts.strftime('%H:%M') if ts else 'N/A'}")
                body = "\n".join(body_parts)

                notified = set()
                for unit in units:
                    if unit in WATCH_SET and unit not in notified:
                        try:
                            dbg(f"send unit={unit} iid={iid} title={title}")
                        except Exception:
                            pass
                        post(unit, title, body)
                        notified.add(unit)
                post("LOG", title, body)
                SEEN_MSG.add(iid)



            if len(SEEN_MSG) > 5000:
                SEEN_MSG = set(list(SEEN_MSG)[-2500:])

        backoff = POLL_SEC  # reset on success

    except ReadTimeout:
        log("Polling timeout (ignored)")
        backoff = min(backoff * 2, 60)
    except Exception as e:
        log(f"Polling error: {e}")
        backoff = min(backoff * 2, 60)

    try:
        up = sess.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                      params={"timeout": 0, "offset": OFFSET}, timeout=15).json()
        for upd in up["result"]:
            OFFSET = upd["update_id"] + 1
            m = upd.get("message", {})
            text = m.get("text", "").strip().lower()
            tid = m.get("message_thread_id")
            if tid in ID2UNIT:
                unit = ID2UNIT[tid]
                # Live leaderboard commands
                LIVE_MAP = {
                    "/liveday": "day",
                    "/liveweek": "week",
                    "/livemonth": "month",
                    "/liveyear": "year",
                    "/livealltime": "alltime",
                }
                if text in LIVE_MAP:
                    LIVE["active"] = True
                    LIVE["period"] = LIVE_MAP[text]
                    LIVE["threads"] = ["LOG"]  # display in LOG topic for wall display
                    save_live_state(LIVE)
                    update_live_leaderboard(now)
                    post(unit, "LIVE BOARD", f"Enabled live {LEADERBOARD_LABELS.get(LIVE_MAP[text], LIVE_MAP[text])} leaderboard in LOG topic.")
                    continue
                if text == "/livestop":
                    LIVE["active"] = False
                    save_live_state(LIVE)
                    post(unit, "LIVE BOARD", "Live leaderboard stopped.")
                    continue
                if text == "/livehere":
                    # Show live board in this unit thread
                    LIVE["threads"] = [unit]
                    LIVE["active"] = True
                    save_live_state(LIVE)
                    update_live_leaderboard(now)
                    post(unit, "LIVE BOARD", f"Live {LEADERBOARD_LABELS.get(LIVE.get('period','day'),'Shift')} leaderboard will update here.")
                    continue
                if text in LEADERBOARD_COMMANDS:
                    label, period_key = LEADERBOARD_COMMANDS[text]
                    agg_calls, agg_dur, agg_after = aggregate_timeframe_stats(period_key, now=now)
                    body = format_leaderboard_body(label, period_key, agg_calls, agg_dur, agg_after, now)
                    post(unit, "CALL COUNT", body)
                    continue

                


                if text == "/times":
                    tl = LAST_FINISHED.get(unit)
                    if not tl:
                        reply = "No completed run recorded."
                    else:
                        lines = [f"{unit} latest run"]
                        lines.extend(f"{t[1].strftime('%H:%M')}  {t[0]}" for t in tl)
                        reply = "\n".join(lines)
                    post(unit, "TIMES", reply)
    except ReadTimeout:
        log("getUpdates timeout (ignored)")
    except Exception as e:
        log(f"Updates error: {e}")

    if LIVE.get("active") and (stats_dirty or now >= next_live):
        update_live_leaderboard(now)
        next_live = now + datetime.timedelta(seconds=LIVE.get("next_update_sec", 30))
        stats_dirty = False

    if now >= next_evening:
        lines = [f"Mid-shift recap {now:%d %b %Y %H:%M}"]
        for u, c in sorted(CALLS.items(), key=lambda kv: (-kv[1], kv[0])):
            avg = (DUR_SEC[u] / c) / 60 if c else 0
            lines.append(f"{u}: {c}  |  avg {avg:.1f} min  |  after 00:00: {AFTER_0000.get(u,0)}")
        for tid in THREAD_IDS:
            post(tid, "CALL COUNT", "\n".join(lines))
        log("Mid-shift recap sent")
        # Recompute based on current local time to avoid DST drift
        next_evening = next_at(19, now)

    if now >= next_morning:
        # Determine prior shift date using current local time; this keeps rollovers correct across DST
        current_shift_start = shift_start(now)
        yday = (current_shift_start - datetime.timedelta(days=1)).strftime('%d %b %Y')
        lines = [f"Daily runs {yday}"]
        for u, c in sorted(CALLS.items(), key=lambda kv: (-kv[1], kv[0])):
            avg = (DUR_SEC[u] / c) / 60 if c else 0
            lines.append(f"{u}: {c}  |  avg {avg:.1f} min  |  after 00:00: {AFTER_0000.get(u,0)}")
        for tid in THREAD_IDS:
            post(tid, "CALL COUNT", "\n".join(lines))
        log("End-of-shift recap sent")
        save(STATS_FN, CALLS, DUR_SEC, AFTER_0000, MAX_SEC)
        CALLS.clear()
        DUR_SEC.clear()
        AFTER_0000.clear()
        MAX_SEC.clear()
        # Update shift start to the actual 07:00 local for today (not +24h absolute time)
        SHIFT_DT = current_shift_start
        STATS_FN = stats_file(SHIFT_DT)
        save(STATS_FN, CALLS, DUR_SEC, AFTER_0000, MAX_SEC)
        # Recompute next triggers relative to now to respect wall-clock time across DST changes
        next_morning = next_at(7, now)
        next_evening = next_at(19, now)
    time.sleep(backoff + random.uniform(0, 1))




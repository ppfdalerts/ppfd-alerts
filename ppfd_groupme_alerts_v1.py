import os, json, time, datetime, requests, certifi, re, sys, uuid, random, hashlib, atexit
from collections import defaultdict
from typing import Dict, Any, List, Set, Tuple
from requests.exceptions import ReadTimeout

def _open_log_with_retry(filename: str, attempts: int = 10, delay: float = 0.2):
    import time as _t
    last_err = None
    for _ in range(max(1, attempts)):
        try:
            return open(filename, "a", encoding='utf-8')
        except Exception as e:
            last_err = e
            try: _t.sleep(delay)
            except Exception: pass
    try:
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  WARN: log open failed for {filename}: {last_err}")
    except Exception:
        pass
    return sys.stdout

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
    _RUNTIME_ROOT = (os.environ.get("PPFD_STATE_ROOT", "") or "").strip()
    if _RUNTIME_ROOT:
        _RUNTIME_ROOT = os.path.abspath(_RUNTIME_ROOT)
    else:
        _RUNTIME_ROOT = _HERE
    _LOG_FP = os.path.join(_RUNTIME_ROOT, "alerts.log")
    _HANDSHAKE_FP = os.path.join(_RUNTIME_ROOT, "startup_handshake.json")
except Exception:
    _LOG_FP = "alerts.log"
    _HANDSHAKE_FP = "startup_handshake.json"
sys.stdout = _open_log_with_retry(_LOG_FP)
sys.stderr = sys.stdout

def log(msg: str):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{ts}  {msg}", flush=True)

try:
    LOCK_FP = os.path.join(_RUNTIME_ROOT, "ppfd_groupme.lock")
except Exception:
    LOCK_FP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ppfd_groupme.lock")

def _pid_running(pid: int) -> bool:
    """Check if a PID is alive (best-effort, Windows-safe)."""
    try:
        import ctypes  # type: ignore
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, int(pid))
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
    except Exception:
        pass
    return False

def _acquire_lock() -> bool:
    """Prevent multiple instances from running at once."""
    try:
        fd = os.open(LOCK_FP, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as fh:
            fh.write(str(os.getpid()))
        log(f"Instance lock acquired at {LOCK_FP}")
        return True
    except FileExistsError:
        try:
            with open(LOCK_FP) as fh:
                pid_txt = (fh.read() or "").strip()
                pid = int(pid_txt) if pid_txt else None
        except Exception:
            pid = None
        if pid and _pid_running(pid):
            log(f"Another ppfd_groupme_alerts_v1.py instance already running (pid={pid}); exiting.")
            return False
        try:
            os.remove(LOCK_FP)
        except Exception:
            pass
        return _acquire_lock()
    except Exception as e:
        log(f"Instance lock error: {e}")
        return False

def _release_lock():
    try:
        if os.path.exists(LOCK_FP):
            os.remove(LOCK_FP)
    except Exception:
        pass

if not _acquire_lock():
    sys.exit(0)
atexit.register(_release_lock)

DEBUG_VERBOSE = 1 if os.environ.get('DEBUG_VERBOSE', '1').strip() not in ('0','false','False') else 0
def dbg(msg: str):
    if DEBUG_VERBOSE:
        log(f"DEBUG: {msg}")

REQUIRE_STARTUP_CONFIRM = str(os.environ.get("REQUIRE_STARTUP_CONFIRM", "0")).strip().lower() in ("1", "true", "yes", "on")

# Use local naive datetimes so long-running processes track DST transitions
# correctly without fixed-offset drift.
TZ = None
sess = requests.Session()
sess.verify = certifi.where()
sess.headers.update({"User-Agent": "PPFD-GroupMe/1.0"})

TEST_MODE = str(os.environ.get("TEST_MODE", "0")).strip().lower() in ("1","true","yes","on")

THREAD_IDS: dict[str, int | None] = {}
WATCH_SET: set[str] = set()

def _load_dotenv(path: str = ".env"):
    try:
        for fp in [path, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), os.path.join(os.getcwd(), "config", ".env")]:
            if not fp or not os.path.exists(fp):
                continue
            with open(fp, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line:
                        k, v = line.split('=', 1)
                        k = k.strip().lstrip('\ufeff')
                        v = v.strip().strip('"').strip("'")
                        if (k not in os.environ) or (not os.environ.get(k)):
                            os.environ[k] = v
            log(f"Loaded .env from {fp}")
    except Exception as e:
        log(f".env load error: {e}")

_load_dotenv()

# Load GroupMe tokens/config
def _resolve_tokens_file() -> str:
    try:
        env_fp = os.environ.get("GROUPME_TOKENS_FILE", "").strip()
    except Exception:
        env_fp = ""
    if env_fp:
        try:
            if os.path.exists(env_fp):
                return env_fp
        except Exception:
            pass
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        cand = os.path.join(here, "Groupmetokens.txt")
        if os.path.exists(cand):
            return cand
    except Exception:
        pass
    try:
        cwd_cand = os.path.join(os.getcwd(), "Groupmetokens.txt")
        if os.path.exists(cwd_cand):
            return cwd_cand
    except Exception:
        pass
    return env_fp or os.path.join(os.getcwd(), "Groupmetokens.txt")

TOKENS_FN = _resolve_tokens_file()

class GroupMeConfig:
    def __init__(self):
        self.access_token: str = ""
        self.group_id: str = ""
        self.bot_id: str | None = None
        self.topic_ids: dict[str, int] = {}

def _extract_between(text: str, start: str, end: str) -> str | None:
    i = text.find(start)
    if i == -1:
        return None
    j = text.rfind(end)
    if j == -1 or j <= i:
        return None
    return text[i:j+len(end)]

def _extract_json_array(text: str) -> list | None:
    try:
        i = text.find('[')
        j = text.rfind(']')
        if i != -1 and j != -1 and j > i:
            arr_txt = text[i:j+1]
            jv = json.loads(arr_txt)
            if isinstance(jv, list):
                return jv
    except Exception:
        return None
    return None

def load_groupme_config(path: str) -> GroupMeConfig:
    cfg = GroupMeConfig()
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        try:
            dbg(f"Loaded GroupMe tokens from {path}")
        except Exception:
            pass
    except Exception as e:
        log(f"ERROR: reading GroupMe tokens file '{path}': {e}")
        return cfg

    m = re.search(r"(?mi)^\s*User Token\s*=\s*([A-Za-z0-9_\-]+)\s*$", content)
    if m: cfg.access_token = m.group(1).strip()
    m = re.search(r"(?mi)^\s*Group ID\s*=\s*(\d+)\s*$", content)
    if m: cfg.group_id = m.group(1).strip()
    m = re.search(r"(?mi)^\s*Bot ID\s*=\s*([A-Za-z0-9]+)\s*$", content)
    if m: cfg.bot_id = m.group(1).strip()

    for mm in re.finditer(r"(?mi)^\s*([A-Za-z0-9_]+)\s*=\s*https?://api\.groupme\.com/v3/groups/(\d+)/subgroups/(\d+)\s*$", content):
        name = mm.group(1).strip()
        sid = int(mm.group(3))
        cfg.topic_ids[name] = sid

    # Prefer a JSON array if present (list of {topic, id})
    arr = _extract_json_array(content)
    if arr and isinstance(arr, list):
        for it in arr:
            try:
                nm = str((it or {}).get('topic') or '').strip()
                sid = (it or {}).get('id')
                if nm and sid is not None:
                    cfg.topic_ids[nm] = int(sid)
            except Exception:
                continue
    else:
        # Fallback: embedded JSON object with response list
        jtxt = _extract_between(content, '{', '}')
        if jtxt:
            try:
                j = json.loads(jtxt)
                resp_arr = j.get('response') or []
                for it in resp_arr:
                    nm = (it.get('topic') or '').strip()
                    sid = it.get('id')
                    if nm and sid:
                        try: cfg.topic_ids[nm] = int(sid)
                        except Exception: pass
            except Exception:
                pass

    return cfg

GM = load_groupme_config(TOKENS_FN)

DEFAULT_THREAD_NAMES = {
    "GENERAL": None, "R33": None, "E33": None, "T33": None, "33FD": None, "LR36": None,
    "HM33": None, "R34": None, "E34": None, "TR34": None, "E36": None, "S36": None,
    "R36": None, "E35": None, "34FD": None, "36FD": None, "D35": None, "R35": None,
    "35FD": None, "LOG": None, "E136": None,
}

for k in DEFAULT_THREAD_NAMES:
    THREAD_IDS[k] = GM.topic_ids.get(k)

def _tokens_signature(path: str) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
        return (int(st.st_mtime_ns), int(st.st_size))
    except Exception:
        return None

def _apply_groupme_config(cfg: GroupMeConfig):
    GM.access_token = cfg.access_token
    GM.group_id = cfg.group_id
    GM.bot_id = cfg.bot_id
    GM.topic_ids = dict(cfg.topic_ids)
    for name in DEFAULT_THREAD_NAMES:
        THREAD_IDS[name] = GM.topic_ids.get(name)

TOKENS_SIG = _tokens_signature(TOKENS_FN)
GROUPME_LAST_401_LOG_TS = 0.0

def _refresh_groupme_config_if_changed(force: bool = False) -> bool:
    global TOKENS_SIG
    sig = _tokens_signature(TOKENS_FN)
    if (not force) and sig is not None and sig == TOKENS_SIG:
        return False
    cfg = load_groupme_config(TOKENS_FN)
    if not cfg.access_token or not cfg.group_id:
        return False
    changed = (
        cfg.access_token != GM.access_token
        or cfg.group_id != GM.group_id
        or cfg.bot_id != GM.bot_id
        or cfg.topic_ids != GM.topic_ids
    )
    _apply_groupme_config(cfg)
    TOKENS_SIG = sig
    try:
        _discover_topic_ids_if_needed()
    except Exception:
        pass
    if changed:
        log(f"Reloaded GroupMe configuration from {TOKENS_FN}")
    return changed

def _redact_groupme_url(url: str) -> str:
    try:
        return re.sub(r"([?&]token=)[^&]+", r"\1<redacted>", url or "")
    except Exception:
        return url

def _log_groupme_unauthorized():
    global GROUPME_LAST_401_LOG_TS
    now_ts = time.time()
    if (now_ts - GROUPME_LAST_401_LOG_TS) < 300:
        return
    GROUPME_LAST_401_LOG_TS = now_ts
    log(f"GroupMe User Token unauthorized. Update {TOKENS_FN} with a valid User Token; the runner will auto-reload it when the file changes.")

# Try to auto-discover subgroup topic IDs from the API if missing
def _discover_topic_ids_if_needed():
    try:
        if not GM.access_token or not GM.group_id:
            return
        missing = [k for k, v in THREAD_IDS.items() if v is None]
        if not missing:
            return
        def _merge_from_response(j):
            if not isinstance(j, dict):
                return 0
            added = 0
            resp = j.get('response')
            # Case 1: response is a list of subgroup objects
            if isinstance(resp, list):
                for it in resp:
                    try:
                        name = str((it or {}).get('topic') or '').strip()
                        sid = (it or {}).get('id')
                        if name and sid:
                            sid_i = int(sid)
                            if name not in GM.topic_ids:
                                GM.topic_ids[name] = sid_i
                                added += 1
                    except Exception:
                        continue
            # Case 2: response is a dict with nested 'subgroups'
            if isinstance(resp, dict):
                subs = resp.get('subgroups')
                if isinstance(subs, list):
                    for it in subs:
                        try:
                            name = str((it or {}).get('topic') or '').strip()
                            sid = (it or {}).get('id')
                            if name and sid:
                                sid_i = int(sid)
                                if name not in GM.topic_ids:
                                    GM.topic_ids[name] = sid_i
                                    added += 1
                        except Exception:
                            continue
            return added
        # Attempt known endpoints
        paths = [
            f"/groups/{GM.group_id}/subgroups",
            f"/groups/{GM.group_id}",
        ]
        total_added = 0
        for p in paths:
            try:
                url = _gm_url(p)
                r = sess.get(url, timeout=15)
                try:
                    j = r.json()
                except Exception:
                    j = None
                if r.status_code >= 400:
                    dbg(f"discover HTTP {r.status_code} via {p}: {(j or r.text)}")
                    continue
                total_added += (_merge_from_response(j) or 0)
            except Exception as e:
                dbg(f"discover error via {p}: {e}")
        if total_added:
            for k in list(THREAD_IDS.keys()):
                if THREAD_IDS[k] is None and k in GM.topic_ids:
                    THREAD_IDS[k] = GM.topic_ids.get(k)
            try:
                dbg("discovered topics: " + ", ".join([f"{k}:{THREAD_IDS.get(k)}" for k in ("R33","T33","LOG","GENERAL") if k in THREAD_IDS]))
            except Exception:
                pass
    except Exception as e:
        dbg(f"topic discovery error: {e}")

# discovery will run after HTTP helpers are defined

WATCH_SET = set(THREAD_IDS) - {"GENERAL"}

BASE_URL = "https://911.pinellas.gov/files/Activity.json"
POLL_SEC = 5

# Startup diagnostics for topic mapping
try:
    dbg("topic map: " + ", ".join([f"{k}:{THREAD_IDS.get(k)}" for k in ("R33","T33","LOG","GENERAL") if k in THREAD_IDS]))
except Exception:
    pass

GM_API = "https://api.groupme.com/v3"

def _gm_url(path: str) -> str:
    sep = '&' if '?' in path else '?'
    return f"{GM_API}{path}{sep}token={GM.access_token}"

_discover_topic_ids_if_needed()

# Fail fast if required GroupMe config missing
if not GM.access_token or not GM.group_id:
    if TEST_MODE:
        log("TEST MODE: Missing GroupMe User Token or Group ID; continuing without network.")
    else:
        log("ERROR: Missing GroupMe User Token or Group ID. Set GROUPME_TOKENS_FILE or place Groupmetokens.txt next to the script.")
        raise SystemExit(2)

def _extract_mid(j: dict | None) -> str | None:
    if not isinstance(j, dict):
        return None
    try:
        r = j.get('response')
        if isinstance(r, dict):
            # Common shape: response.message.id
            m = r.get('message')
            if isinstance(m, dict) and m.get('id'):
                return str(m.get('id'))
            # Sometimes id is at response.id
            if r.get('id'):
                return str(r.get('id'))
            # Or messages: [ {...} ]
            mm = r.get('messages')
            if isinstance(mm, list) and mm:
                if isinstance(mm[0], dict) and mm[0].get('id'):
                    return str(mm[0].get('id'))
        # Rare: top-level message.id
        m2 = j.get('message')
        if isinstance(m2, dict) and m2.get('id'):
            return str(m2.get('id'))
    except Exception:
        return None
    return None

def gm_post_message(text: str, unit: str | None = None) -> str | None:
    def _send_once() -> tuple[str | None, int | None, list[str], str]:
        message = {"source_guid": str(uuid.uuid4()), "text": text[:10000]}
        sid = THREAD_IDS.get(unit) if unit else None
        # Force LOG to main chat even if a topic id exists
        if unit and unit.upper() == "LOG":
            sid = None
        tried_urls: list[str] = []
        if sid:
            dbg(f"post -> subgroup sid={sid} unit={unit}")
            paths = [
                f"/groups/{sid}/messages",
                f"/subgroups/{sid}/messages",
                f"/groups/{GM.group_id}/subgroups/{sid}/messages",
            ]
            for p in paths:
                url = _gm_url(p)
                tried_urls.append(url)
                r = sess.post(url, json={"message": message}, timeout=15)
                j = None
                try:
                    j = r.json()
                except Exception:
                    pass
                if r.status_code < 400:
                    mid = _extract_mid(j)
                    if mid:
                        return mid, r.status_code, tried_urls, ""
                else:
                    dbg(f"Subgroup send failed {r.status_code} via {p}: {j or r.text}")
        if unit and (sid is None or unit.upper() == "LOG"):
            dbg(f"post -> main group fallback (no topic id for {unit})")
        url = _gm_url(f"/groups/{GM.group_id}/messages")
        tried_urls.append(url)
        r = sess.post(url, json={"message": message}, timeout=15)
        j = None
        try:
            j = r.json()
        except Exception:
            pass
        if r.status_code >= 400:
            return None, r.status_code, tried_urls, str(j or r.text)
        return _extract_mid(j), r.status_code, tried_urls, ""

    try:
        try:
            _refresh_groupme_config_if_changed()
        except Exception:
            pass
        mid, status_code, tried_urls, err_txt = _send_once()
        if mid:
            return mid
        if status_code == 401:
            _log_groupme_unauthorized()
            try:
                if _refresh_groupme_config_if_changed(force=True):
                    mid, status_code, tried_urls, err_txt = _send_once()
                    if mid:
                        return mid
            except Exception:
                pass
        if status_code and status_code >= 400:
            safe_urls = ", ".join(_redact_groupme_url(url) for url in tried_urls)
            log(f"GroupMe send HTTP {status_code} (tried: {safe_urls}): {err_txt}")
            return None
        return mid
    except ReadTimeout:
        log("GroupMe send timeout (ignored)")
    except Exception as e:
        log(f"GroupMe send error: {e}")
    return None

def gm_fetch_recent(unit: str | None, limit: int = 20) -> list[dict]:
    try:
        try:
            _refresh_groupme_config_if_changed()
        except Exception:
            pass
        params = {"limit": max(1, min(100, int(limit)))}
        # Treat LOG as main group even if a topic id exists, to keep
        # command polling consistent with message sending behaviour.
        if unit and str(unit).upper() == "LOG":
            sid = None
        else:
            sid = THREAD_IDS.get(unit) if unit else None
        paths = []
        if sid:
            paths = [
                f"/groups/{sid}/messages",
                f"/subgroups/{sid}/messages",
                f"/groups/{GM.group_id}/subgroups/{sid}/messages",
            ]
        else:
            paths = [f"/groups/{GM.group_id}/messages"]
        for p in paths:
            url = _gm_url(p)
            r = sess.get(url, params=params, timeout=15)
            try:
                j = r.json()
            except Exception:
                j = None
            if r.status_code >= 400:
                dbg(f"gm_fetch_recent HTTP {r.status_code} via {p}: {(j or r.text)}")
                continue
            if not isinstance(j, dict):
                continue
            resp = j.get('response')
            if isinstance(resp, dict):
                msgs = resp.get('messages')
                if isinstance(msgs, list):
                    return msgs
            if isinstance(resp, list):
                return resp
    except Exception as e:
        dbg(f"gm_fetch_recent error for {unit}: {e}")
    return []

def gm_wait_for_message(unit: str | None, message_id: str, timeout_sec: int = 30, poll_interval_sec: float = 2.0) -> bool:
    try:
        target_id = str(message_id).strip()
        if not target_id:
            return False
    except Exception:
        return False
    deadline = time.time() + max(1.0, float(timeout_sec))
    while time.time() < deadline:
        msgs = gm_fetch_recent(unit, limit=50)
        for m in msgs:
            try:
                mid = str(m.get("id") or "").strip()
            except Exception:
                mid = ""
            if mid and mid == target_id:
                return True
        try:
            time.sleep(max(0.25, float(poll_interval_sec)))
        except Exception:
            break
    return False

def post(unit: str, title: str, body: str):
    text = f"{title}\n{body}" if body else title
    mid = gm_post_message(text, unit)
    if mid:
        try:
            log(f"SENT [{unit}] {title} - {body.replace(chr(10), ' | ')}")
        except Exception:
            log(f"SENT [{unit}] {title}")
    else:
        log(f"GroupMe send failed [{unit}] {title}")

def post_return_id(unit: str, title: str, body: str):
    text = f"{title}\n{body}" if body else title
    mid = gm_post_message(text, unit)
    if mid:
        log(f"SENT-ID [{unit}] {title} mid={mid}")
        return mid
    return None

def edit_message(unit: str, message_id: str | None, title: str, body: str) -> bool:
    return False

SHIFT_HOUR = 7
try:
    _REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
except Exception:
    _REPO_ROOT = os.getcwd()
DEFAULT_STATS_DIR = os.path.join(_REPO_ROOT, "data", "shift_stats")
STATS_DIR = os.environ.get("SHIFT_STATS_DIR", DEFAULT_STATS_DIR)
try:
    os.makedirs(STATS_DIR, exist_ok=True)
except Exception as _e:
    log(f"WARN: Could not create stats dir {STATS_DIR}: {_e}")
else:
    try:
        log(f"SHIFT_STATS_DIR={STATS_DIR}")
    except Exception:
        pass

def shift_start(now: datetime.datetime):
    base = now.replace(hour=SHIFT_HOUR, minute=0, second=0, microsecond=0)
    return base if now >= base else base - datetime.timedelta(days=1)

def stats_file(dt: datetime.datetime):
    return os.path.join(STATS_DIR, f"shift_stats_{dt:%Y-%m-%d}.json")

def _stats_load(fp):
    if os.path.exists(fp):
        try:
            with open(fp) as f:
                j = json.load(f)
                duration_known = j.get("duration_known_calls")
                if isinstance(duration_known, dict):
                    duration_known_dd = defaultdict(int, duration_known)
                else:
                    duration_known_dd = defaultdict(int, j.get("calls", {}))
                return (defaultdict(int, j.get("calls", {})),
                        defaultdict(int, j.get("dur_sec", {})),
                        defaultdict(int, j.get("after_0000", {})),
                        defaultdict(int, j.get("max_sec", {})),
                        defaultdict(int, j.get("transporting_count", {})),
                        defaultdict(int, j.get("at_hospital_count", {})),
                        defaultdict(int, j.get("ride_in_count", {})),
                        duration_known_dd)
        except Exception:
            pass
    return (
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
    )

def _stats_save(fp, calls, dur, after, max_sec, transporting_count=None, at_hospital_count=None, ride_in_count=None, duration_known_calls=None):
    try:
        transporting_count = transporting_count if transporting_count is not None else {}
        at_hospital_count = at_hospital_count if at_hospital_count is not None else {}
        ride_in_count = ride_in_count if ride_in_count is not None else {}
        duration_known_calls = duration_known_calls if duration_known_calls is not None else calls
        os.makedirs(os.path.dirname(fp), exist_ok=True)
        with open(fp + ".tmp", "w") as f:
            json.dump(
                {
                    "calls": calls,
                    "dur_sec": dur,
                    "after_0000": after,
                    "max_sec": max_sec,
                    "transporting_count": transporting_count,
                    "at_hospital_count": at_hospital_count,
                    "ride_in_count": ride_in_count,
                    "duration_known_calls": duration_known_calls,
                },
                f,
            )
        os.replace(fp + ".tmp", fp)
    except Exception as e:
        log(f"Persist error: {e}")

def _default_personnel_stats_dir():
    try:
        base = os.path.dirname(STATS_DIR)
    except Exception:
        base = os.getcwd()
    return os.path.join(base, "shift_personnel")

PERSONNEL_STATS_DIR = os.environ.get("PERSONNEL_STATS_DIR", _default_personnel_stats_dir())
try:
    os.makedirs(PERSONNEL_STATS_DIR, exist_ok=True)
except Exception as _e:
    log(f"WARN: Could not create personnel stats dir {PERSONNEL_STATS_DIR}: {_e}")
else:
    try:
        log(f"PERSONNEL_STATS_DIR={PERSONNEL_STATS_DIR}")
    except Exception:
        pass

def personnel_stats_file(dt: datetime.datetime):
    return os.path.join(PERSONNEL_STATS_DIR, f"shift_personnel_{dt:%Y-%m-%d}.json")

def _pstats_load(fp):
    if os.path.exists(fp):
        try:
            with open(fp) as f:
                j = json.load(f)
                names = dict(j.get("names", {}))
                return (
                    names,
                    defaultdict(int, j.get("calls", {})),
                    defaultdict(int, j.get("dur_sec", {})),
                    defaultdict(int, j.get("after_0000", {})),
                    defaultdict(int, j.get("max_sec", {})),
                    defaultdict(int, j.get("transporting_count", {})),
                    defaultdict(int, j.get("at_hospital_count", {})),
                    defaultdict(int, j.get("ride_in_count", {})),
                )
        except Exception:
            pass
    return (
        {},
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
        defaultdict(int),
    )

def _pstats_save(
    fp,
    names,
    calls,
    dur,
    after,
    max_sec,
    transporting_count=None,
    at_hospital_count=None,
    ride_in_count=None,
):
    try:
        transporting_count = transporting_count if transporting_count is not None else {}
        at_hospital_count = at_hospital_count if at_hospital_count is not None else {}
        ride_in_count = ride_in_count if ride_in_count is not None else {}
        os.makedirs(os.path.dirname(fp), exist_ok=True)
        with open(fp + ".tmp", "w") as f:
            json.dump(
                {
                    "names": names,
                    "calls": calls,
                    "dur_sec": dur,
                    "after_0000": after,
                    "max_sec": max_sec,
                    "transporting_count": transporting_count,
                    "at_hospital_count": at_hospital_count,
                    "ride_in_count": ride_in_count,
                },
                f,
            )
        os.replace(fp + ".tmp", fp)
    except Exception as e:
        log(f"Personnel persist error: {e}")

def _is_transporting_status(status: str) -> bool:
    text = (status or "").strip().lower()
    if not text:
        return False
    return "transport" in text

def _is_at_hospital_status(status: str) -> bool:
    text = (status or "").strip().lower()
    if not text:
        return False
    return ("hospital" in text) or ("at hosp" in text)

def _person_has_pm_marker(name: str) -> bool:
    text = (name or "").strip().upper()
    if not text:
        return False
    return "PM" in text

def _eligible_ride_in_personnel_keys(rec: dict):
    keys = rec.get("personnel_keys") or []
    if not keys:
        return []
    eligible = []
    for pkey in keys:
        pname = PERSONNEL_NAMES.get(pkey, "")
        if _person_has_pm_marker(pname):
            eligible.append(pkey)
    return eligible

def _record_ride_in_status(uid: str, rec: dict, status: str):
    if uid not in WATCH_SET or rec.get("ignore"):
        return False, False

    text = (status or "").strip().lower()
    if not text:
        return False, False

    unit_changed = False
    personnel_changed = False
    personnel_keys = _eligible_ride_in_personnel_keys(rec)

    if _is_transporting_status(text) and not rec.get("transporting_recorded"):
        rec["transporting_recorded"] = True
        TRANSPORTING_COUNT[uid] += 1
        unit_changed = True
        for pkey in personnel_keys:
            P_TRANSPORTING_COUNT[pkey] += 1
            personnel_changed = True

    if _is_at_hospital_status(text) and not rec.get("at_hospital_recorded"):
        rec["at_hospital_recorded"] = True
        AT_HOSPITAL_COUNT[uid] += 1
        unit_changed = True
        for pkey in personnel_keys:
            P_AT_HOSPITAL_COUNT[pkey] += 1
            personnel_changed = True

    if (rec.get("transporting_recorded") or rec.get("at_hospital_recorded")) and not rec.get("ride_in_recorded"):
        rec["ride_in_recorded"] = True
        RIDE_IN_COUNT[uid] += 1
        unit_changed = True
        for pkey in personnel_keys:
            P_RIDE_IN_COUNT[pkey] += 1
            personnel_changed = True

    return unit_changed, personnel_changed

ROSTER_DIR = os.environ.get(
    "ROSTER_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "TSlogs"),
)
ROSTER_REFRESH_SEC = 300
_ROSTER_CACHE = {"date": None, "loaded_at": None, "payload": None}

def _roster_path_for_date(d: datetime.date):
    return os.path.join(ROSTER_DIR, f"roster_units_{d:%Y-%m-%d}.json")

def _normalize_unit_code_for_roster(code: str) -> str:
    c = (code or "").strip().upper()
    m = re.match(r"^TR(\d+)$", c)
    if m:
        return f"T{m.group(1)}"
    if c.startswith("DC") and c[2:].isdigit():
        return f"D{c[2:]}"
    return c

def _parse_time_to_minutes(value):
    if value is None:
        return None
    try:
        if isinstance(value, datetime.time):
            return value.hour * 60 + value.minute
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    match = re.match(r"^(\d{1,2}):(\d{2})(?::\d{2})?\s*([AP]M)?$", text, re.IGNORECASE)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    am_pm = match.group(3)
    if am_pm:
        if am_pm.upper() == "PM" and hour != 12:
            hour += 12
        if am_pm.upper() == "AM" and hour == 12:
            hour = 0
    return (hour % 24) * 60 + minute

def _covers_time(entry, at_minute):
    if at_minute is None:
        return True
    start = _parse_time_to_minutes(entry.get("from"))
    end = _parse_time_to_minutes(entry.get("through"))
    hours = entry.get("hours")
    if start is None or end is None:
        return True
    if start == end:
        try:
            hours_val = float(hours)
        except (TypeError, ValueError):
            return True
        if hours_val >= 23.5:
            return True
        duration = int(round(hours_val * 60))
        end = (start + duration) % (24 * 60)
    if start < end:
        return start <= at_minute < end
    return at_minute >= start or at_minute < end

def _load_roster_for_date(d: datetime.date):
    try:
        if _ROSTER_CACHE["date"] == d and _ROSTER_CACHE.get("payload") is not None:
            last = _ROSTER_CACHE.get("loaded_at")
            if last and (time.time() - last) < ROSTER_REFRESH_SEC:
                return _ROSTER_CACHE.get("payload")
    except Exception:
        pass
    roster_path = _roster_path_for_date(d)
    if not os.path.exists(roster_path):
        return None
    try:
        with open(roster_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        _ROSTER_CACHE["date"] = d
        _ROSTER_CACHE["loaded_at"] = time.time()
        _ROSTER_CACHE["payload"] = payload
        return payload
    except Exception:
        return None

def _person_key(entry: dict) -> str | None:
    pid = str(entry.get("id") or "").strip()
    if pid:
        return pid
    name = str(entry.get("name") or "").strip()
    return name or None

def _lookup_staffing(unit_code: str, when: datetime.datetime):
    shift_date = shift_start(when).date()
    roster = _load_roster_for_date(shift_date)
    if not roster:
        return []
    target = _normalize_unit_code_for_roster(unit_code)
    at_minute = when.hour * 60 + when.minute
    seen = {}
    for unit in roster.get("units", []):
        code = _normalize_unit_code_for_roster(unit.get("unit_code", ""))
        if code != target:
            continue
        for entry in unit.get("entries", []) or []:
            if not _covers_time(entry, at_minute):
                continue
            key = _person_key(entry)
            if not key or key in seen:
                continue
            name = str(entry.get("name") or "").strip() or key
            seen[key] = name
    return list(seen.items())

STATS_FILENAME_RE = re.compile(r"shift_stats_(\d{4}-\d{2}-\d{2})\.json")

TIMEFRAME_LENGTHS = {"day": 1, "week": 7, "month": 30, "year": 365}
LEADERBOARD_COMMANDS = {"/day": ("Daily", "day"), "/week": ("Weekly", "week"), "/month": ("Monthly", "month"), "/year": ("Yearly", "year"), "/alltime": ("All-time", "alltime")}
LEADERBOARD_LABELS = {"day": "Daily", "week": "Weekly", "month": "Monthly", "year": "Yearly", "alltime": "All-time"}

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
        file_calls, file_dur, file_after, _file_max, _file_transporting, _file_at_hospital, _file_ride_in, _file_duration_known = _stats_load(file_path)
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

LIVE_STATE_FN = "live_state.json"
def _live_default():
    default_threads = ["LOG"] if "LOG" in THREAD_IDS else ["GENERAL"]
    # Refresh cadence for live leaderboard updates (seconds)
    return {"active": False, "period": "day", "threads": default_threads, "msg_ids": {}, "next_update_sec": 120}

# Commands and live leaderboard controls are disabled; always start
# with a non-active LIVE state and ignore any persisted state file.
def load_live_state():
    return _live_default()

def save_live_state(state):
    # No-op with commands disabled; keep function for compatibility.
    return

LIVE = _live_default()
stats_dirty = False

def update_live_leaderboard(now):
    try:
        if not LIVE.get("active"):
            return
        period_key = LIVE.get("period", "day")
        label = LEADERBOARD_LABELS.get(period_key, period_key.title())
        calls, dur, after = aggregate_timeframe_stats(period_key, now=now)
        body = format_leaderboard_body(label, period_key, calls, dur, after, now)
        title = f"LIVE {label.upper()}"
        msg_ids = LIVE.get("msg_ids", {})
        changed = False
        for unit in LIVE.get("threads", []):
            mid = msg_ids.get(unit)
            ok = False
            if mid:
                ok = edit_message(unit, mid, title, body)
            if not ok:
                mid2 = post_return_id(unit, title, body)
                if mid2 is not None:
                    msg_ids[unit] = mid2
                    changed = True
        if changed:
            LIVE["msg_ids"] = msg_ids
            save_live_state(LIVE)
    except Exception as e:
        log(f"update_live_leaderboard error: {e}")

def parse_ts(hms):
    if not hms: return None
    try:
        h, m, s = map(int, hms.split(":"))
        d = datetime.date.today()
        return datetime.datetime(d.year, d.month, d.day, h, m, s, tzinfo=TZ)
    except Exception:
        return None

# Geofences for early alerts
GEOFENCE_FILE = "geofences.json"
EARLY_ALERT_ENABLED = str(os.environ.get("ENABLE_EARLY_ALERTS", "0")).strip().lower() in ("1", "true", "yes", "on")
EARLY_ALERT_TITLE_PREFIX = os.environ.get("EARLY_ALERT_TITLE_PREFIX", "EARLY").strip() or "EARLY"
EARLY_SEEN: Set[Tuple[str, str]] = set()

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
            return {k: v for k, v in data.items() if k in THREAD_IDS}
    except Exception as e:
        log(f"Failed to load geofences: {e}")
        return {}

GEOFENCES = load_geofences()

def is_in_geofence(unit: str, grid: str | None, lat: str | float | None, lon: str | float | None) -> bool:
    gf = GEOFENCES.get(unit)
    if not gf:
        return False
    g = (grid or "").strip().upper()
    for code in gf.get("grids", []) or []:
        code = str(code).strip().upper()
        if not code:
            continue
        if g == code or (code.endswith("*") and g.startswith(code[:-1])):
            return True
    try:
        if lat is None or lon is None or lat == "" or lon == "":
            pass
        else:
            latf = float(lat); lonf = float(lon)
            for c in gf.get("circles", []) or []:
                try:
                    clat = float(c.get("lat")); clon = float(c.get("lon"))
                    r_m = float(c.get("radius_m", 0))
                    r_km = float(c.get("radius_km", r_m / 1000 if r_m else 0))
                    if r_km > 0 and _haversine_km(latf, lonf, clat, clon) <= r_km:
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False

START_ANNOUNCED = False
def _announce_start():
    global START_ANNOUNCED
    unit = "LOG" if "LOG" in THREAD_IDS else "GENERAL"
    START_ANNOUNCED = True  # avoid spamming on retries
    try:
        body = "PPFD GroupMe alert script started successfully at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mid = post_return_id(unit, "SCRIPT STATUS", body)
        if not mid:
            log(f"Startup announce send failed for unit={unit}")
            if REQUIRE_STARTUP_CONFIRM:
                raise SystemExit(3)
            return
        # For LOG, messages are forced to main group; confirm against main group timeline
        fetch_unit: str | None = None if unit.upper() == "LOG" else unit
        if gm_wait_for_message(fetch_unit, mid, timeout_sec=30, poll_interval_sec=2.0):
            log(f"Startup announce confirmed by GroupMe unit={unit} id={mid}")
            try:
                payload = {
                    "unit": unit,
                    "message_id": mid,
                    "time": datetime.datetime.now(TZ).isoformat(),
                    "status": "ok",
                }
                with open(_HANDSHAKE_FP, "w", encoding="utf-8") as f:
                    json.dump(payload, f)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except Exception:
                        pass
            except Exception as e:
                log(f"Startup handshake file write error: {e}")
        else:
            log(f"Startup announce NOT confirmed by GroupMe within timeout unit={unit} id={mid}")
            if REQUIRE_STARTUP_CONFIRM:
                raise SystemExit(3)
    except SystemExit:
        raise
    except Exception as e:
        log(f"Startup announce error: {e}")
        if REQUIRE_STARTUP_CONFIRM:
            raise SystemExit(3)

MY_USER_ID: str | None = None
def _gm_me_id() -> str | None:
    try:
        try:
            _refresh_groupme_config_if_changed()
        except Exception:
            pass
        url = f"https://api.groupme.com/v3/users/me?token={GM.access_token}"
        r = sess.get(url, timeout=10)
        j = r.json()
        me = j.get('response') or {}
        uid = me.get('id')
        return str(uid) if uid else None
    except Exception as e:
        dbg(f"me id error: {e}")
        return None

LAST_SEEN: dict[str, int] = {}
def poll_commands(now: datetime.datetime):
    # Commands (e.g., /day, /liveday, /times) are disabled.
    # This function is kept as a stub for compatibility.
    return

# Determine destination unit/topic for scheduled leaderboard posts
def _leaderboard_target_unit() -> str:
    # Allow explicit override via env
    env_name = (os.environ.get("LEADERBOARD_TOPIC_NAME") or "").strip()
    if env_name:
        # Use exact match if present in topics, else fall back to GENERAL
        for k in GM.topic_ids.keys():
            if k.strip().lower() == env_name.lower():
                return k
        return "GENERAL"
    # Otherwise, try common synonyms in order of preference
    for synonym in ["MAIN CHAT", "MAIN", "LOG"]:
        for k in GM.topic_ids.keys():
            if (k or "").strip().upper() == synonym:
                return k
    # Default to main group
    return "GENERAL"

# Init shift state and stats
NOW = datetime.datetime.now(TZ)
SHIFT_DT = shift_start(NOW)
STATS_FN = stats_file(SHIFT_DT)
CALLS, DUR_SEC, AFTER_0000, MAX_SEC, TRANSPORTING_COUNT, AT_HOSPITAL_COUNT, RIDE_IN_COUNT, DURATION_KNOWN_CALLS = _stats_load(STATS_FN)
P_STATS_FN = personnel_stats_file(SHIFT_DT)
PERSONNEL_NAMES, P_CALLS, P_DUR_SEC, P_AFTER_0000, P_MAX_SEC, P_TRANSPORTING_COUNT, P_AT_HOSPITAL_COUNT, P_RIDE_IN_COUNT = _pstats_load(P_STATS_FN)

ACTIVE: dict[Tuple[str, str], dict] = {}
LAST_FINISHED: dict[str, list] = {}
SEEN_MSG: Set[str] = set()

if not TEST_MODE:
    _announce_start()

def _build_call_alert_title_body(it: dict, units: list[str]) -> tuple[str, str]:
    ctype = (it.get("Type") or "Call").strip()
    tac = (it.get("Tac") or "").strip()
    title = f"{ctype}" + (f"  TAC {tac}" if tac else "")
    body_parts = []
    if not ctype.upper().startswith("MEDICAL"):
        loc = (it.get("Location") or "").strip()
        lat = it.get("Lat")
        lon = it.get("Lon")
        if loc:
            body_parts.append(loc)
        if lat and lon:
            apple = f"https://maps.apple.com/?ll={lat},{lon}"
            gmap = f"https://www.google.com/maps?q={lat},{lon}"
            body_parts.append(f"Apple: {apple}\nGoogle: {gmap}")
    body_parts.append(f"Units: {', '.join(units) if units else 'none yet'}")
    ts = parse_ts(it.get("Received"))
    body_parts.append(f"Time:  {ts.strftime('%H:%M') if ts else 'N/A'}")
    return title, "\n".join(body_parts)

def _build_fd_attach_alert(added_unit: str, it: dict, units: list[str]) -> tuple[str, str]:
    call_title, call_body = _build_call_alert_title_body(it, units)
    body_parts = [f"Call: {call_title}"]
    if call_body:
        body_parts.append(call_body)
    return f"[{added_unit}] ADDED TO CALL", "\n".join(body_parts)

def next_at(hr, now):
    tgt = now.replace(hour=hr, minute=0, second=0, microsecond=0)
    return tgt if now < tgt else tgt + datetime.timedelta(days=1)

next_morning = next_at(7, NOW)
next_evening = next_at(19, NOW)
next_live = NOW
STATS_REFRESH_SEC = 300
next_stats_refresh = NOW + datetime.timedelta(seconds=STATS_REFRESH_SEC)
etag = None
last_mod = None
stats_dirty = False

def api_url():
    return f"{BASE_URL}?_time={int(time.time() * 1000)}"

# Optional SUNSTAR tracking (3-digit)
SUNSTAR_TRACK: dict[tuple, set] = {}
FD_UNIT_TRACK: dict[str, set[str]] = {}

while not TEST_MODE:
    now = datetime.datetime.now(TZ)
    headers = {"Cache-Control": "no-cache"}
    if etag: headers["If-None-Match"] = etag
    if last_mod: headers["If-Modified-Since"] = last_mod

    try:
        url = api_url(); dbg(f"fetch {url}")
        r = sess.get(url, headers=headers, timeout=20)
        if r.status_code == 304:
            time.sleep(POLL_SEC + random.uniform(0,1)); continue
        if not START_ANNOUNCED:
            try: _announce_start()
            except Exception: pass
        etag, last_mod = r.headers.get("ETag", etag), r.headers.get("Last-Modified", last_mod)
        data = r.json()
        items = data.get("CallInfo", []) if isinstance(data, dict) else []
        seen_incidents: Set[str] = set()
        for it in items:
            iid = str(it.get("IncidentNo") or hashlib.sha1(f"{it.get('Type')}{it.get('Location')}{it.get('Received')}".encode()).hexdigest())
            seen_incidents.add(iid)
            units = [str(u.get("ID") or "").strip().upper() for u in it.get("Units", [])]
            statuses = {str(u.get("ID") or "").upper(): str(u.get("Status") or "").lower() for u in it.get("Units", []) if u.get("ID")}

            # Early alerts by geofence
            if EARLY_ALERT_ENABLED and GEOFENCES:
                grid = (it.get("Grid") or "").strip().upper(); lat = it.get("Lat"); lon = it.get("Lon")
                ctype = it.get("Type", "Call").strip(); tac = (it.get("Tac") or "").strip()
                for unit in GEOFENCES.keys():
                    if unit in units: continue
                    key_early = (iid, unit)
                    if key_early in EARLY_SEEN: continue
                    if is_in_geofence(unit, grid, lat, lon):
                        title = f"{EARLY_ALERT_TITLE_PREFIX}: {ctype}" + (f"  TAC {tac}" if tac else "")
                        body_parts = []
                        if not ctype.upper().startswith("MEDICAL"):
                            loc = (it.get("Location") or "").strip()
                            if loc: body_parts.append(loc)
                            if lat and lon:
                                apple = f"https://maps.apple.com/?ll={lat},{lon}"; gmap = f"https://www.google.com/maps?q={lat},{lon}"
                                body_parts.append(f"Apple: {apple}\nGoogle: {gmap}")
                        if grid: body_parts.append(f"Grid: {grid}")
                        body_parts.append(f"Units: {', '.join(units) if units else 'none yet'}")
                        ts = parse_ts(it.get("Received")); body_parts.append(f"Time:  {ts.strftime('%H:%M') if ts else 'N/A'}")
                        post(unit, title, "\n".join(body_parts))
                        EARLY_SEEN.add(key_early)

            def is_sunstar(uid: str) -> bool:
                return re.fullmatch(r"\d{3}", uid or "") is not None

            # Only consider FD units that are still on the call and not marked available/cleared
            def _unit_is_active(uid: str) -> bool:
                try:
                    s = (statuses.get(uid, "") or "").lower()
                except Exception:
                    s = ""
                inactive_markers = ("available", "clear", "cancel", "released")
                return not any(m in s for m in inactive_markers)

            fd_units = [u for u in units if (u in WATCH_SET and _unit_is_active(u))]
            sunstar_units = {u for u in units if is_sunstar(u)}
            current_fd_units = set(fd_units)

            prev_fd_units = FD_UNIT_TRACK.get(iid, set())
            if prev_fd_units:
                added_fd_units = sorted(current_fd_units - prev_fd_units)
                for added_fd in added_fd_units:
                    title_attach, body_attach = _build_fd_attach_alert(added_fd, it, units)
                    for recipient in sorted(current_fd_units):
                        post(recipient, title_attach, body_attach)
                    post("LOG", title_attach, body_attach)
            if current_fd_units:
                FD_UNIT_TRACK[iid] = set(current_fd_units)
            elif iid in FD_UNIT_TRACK:
                del FD_UNIT_TRACK[iid]

            for fd in fd_units:
                key = (iid, fd)
                prev = SUNSTAR_TRACK.get(key, set())
                added = sunstar_units - prev
                removed = prev - sunstar_units
                if prev:
                    for ss in added:
                        post(fd, f"[{fd}] SUNSTAR {ss} ADDED TO CALL", f"FD UNIT: {fd}")
                    for ss in removed:
                        post(fd, f"[{fd}] SUNSTAR {ss} REMOVED FROM THE CALL", f"FD UNIT: {fd}")
                if sunstar_units:
                    SUNSTAR_TRACK[key] = set(sunstar_units)
                elif key in SUNSTAR_TRACK:
                    del SUNSTAR_TRACK[key]

            for key in list(SUNSTAR_TRACK):
                incident_id, fd = key
                if incident_id == iid and fd not in units:
                    del SUNSTAR_TRACK[key]

            seen_now = set()
            for uid in units:
                key = (iid, uid)
                status = statuses.get(uid, "")
                rec = ACTIVE.get(key)
                if rec is None:
                    rcv = parse_ts(it.get("Received"))
                    before_shift = False
                    try: before_shift = (rcv is not None and rcv < SHIFT_DT)
                    except Exception: before_shift = False
                    start_time = rcv or now
                    ACTIVE[key] = rec = {
                        "status": status,
                        "start": start_time,
                        "events": [("dispatched", start_time)],
                        "ignore": bool(before_shift),
                        "transporting_recorded": False,
                        "at_hospital_recorded": False,
                        "ride_in_recorded": False,
                    }
                    if uid in WATCH_SET and not before_shift:
                        CALLS[uid] += 1
                        try:
                            if rcv and rcv.time() < datetime.time(7): AFTER_0000[uid] += 1
                        except Exception: pass
                        personnel = _lookup_staffing(uid, now)
                        if personnel:
                            rec["personnel_keys"] = [p[0] for p in personnel]
                            for pkey, pname in personnel:
                                PERSONNEL_NAMES[pkey] = pname
                                P_CALLS[pkey] += 1
                                try:
                                    if rcv and rcv.time() < datetime.time(7):
                                        P_AFTER_0000[pkey] += 1
                                except Exception:
                                    pass
                        unit_status_changed, personnel_status_changed = _record_ride_in_status(uid, rec, status)
                        _stats_save(
                            STATS_FN,
                            CALLS,
                            DUR_SEC,
                            AFTER_0000,
                            MAX_SEC,
                            TRANSPORTING_COUNT,
                            AT_HOSPITAL_COUNT,
                            RIDE_IN_COUNT,
                            DURATION_KNOWN_CALLS,
                        )
                        stats_dirty = True
                        if personnel or personnel_status_changed:
                            _pstats_save(
                                P_STATS_FN,
                                PERSONNEL_NAMES,
                                P_CALLS,
                                P_DUR_SEC,
                                P_AFTER_0000,
                                P_MAX_SEC,
                                P_TRANSPORTING_COUNT,
                                P_AT_HOSPITAL_COUNT,
                                P_RIDE_IN_COUNT,
                            )
                elif status != rec["status"]:
                    rec["status"] = status
                    rec["events"].append((status, now))
                    unit_status_changed, personnel_status_changed = _record_ride_in_status(uid, rec, status)
                    if unit_status_changed:
                        _stats_save(
                            STATS_FN,
                            CALLS,
                            DUR_SEC,
                            AFTER_0000,
                            MAX_SEC,
                            TRANSPORTING_COUNT,
                            AT_HOSPITAL_COUNT,
                            RIDE_IN_COUNT,
                            DURATION_KNOWN_CALLS,
                        )
                    if personnel_status_changed:
                        _pstats_save(
                            P_STATS_FN,
                            PERSONNEL_NAMES,
                            P_CALLS,
                            P_DUR_SEC,
                            P_AFTER_0000,
                            P_MAX_SEC,
                            P_TRANSPORTING_COUNT,
                            P_AT_HOSPITAL_COUNT,
                            P_RIDE_IN_COUNT,
                        )
                    if unit_status_changed or personnel_status_changed:
                        stats_dirty = True
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
                        DURATION_KNOWN_CALLS[uid] += 1
                        _stats_save(
                            STATS_FN,
                            CALLS,
                            DUR_SEC,
                            AFTER_0000,
                            MAX_SEC,
                            TRANSPORTING_COUNT,
                            AT_HOSPITAL_COUNT,
                            RIDE_IN_COUNT,
                            DURATION_KNOWN_CALLS,
                        )
                        stats_dirty = True
                        pkeys = rec.get("personnel_keys") or []
                        if pkeys:
                            for pkey in pkeys:
                                P_DUR_SEC[pkey] += dur_sec
                                P_MAX_SEC[pkey] = max(int(P_MAX_SEC.get(pkey, 0)), int(dur_sec))
                            _pstats_save(
                                P_STATS_FN,
                                PERSONNEL_NAMES,
                                P_CALLS,
                                P_DUR_SEC,
                                P_AFTER_0000,
                                P_MAX_SEC,
                                P_TRANSPORTING_COUNT,
                                P_AT_HOSPITAL_COUNT,
                                P_RIDE_IN_COUNT,
                            )

            # --- ALERT: New Call (one-time per incident) ---
            # Post into each matching unit topic (R33, T33, etc.) plus LOG/main
            try:
                _ = SEEN_MSG
            except NameError:
                SEEN_MSG = set()
            if (iid not in SEEN_MSG) and any(u in WATCH_SET for u in units):
                title, body = _build_call_alert_title_body(it, units)

                notified = set()
                for unit in units:
                    if unit in WATCH_SET and unit not in notified:
                        post(unit, title, body)
                        notified.add(unit)
                post("LOG", title, body)
                SEEN_MSG.add(iid)

        for key in list(SUNSTAR_TRACK):
            if key[0] not in seen_incidents:
                del SUNSTAR_TRACK[key]
        for incident_id in list(FD_UNIT_TRACK):
            if incident_id not in seen_incidents:
                del FD_UNIT_TRACK[incident_id]

        # Commands from groups/topics
        poll_commands(now)

    except ReadTimeout:
        log("Feed fetch timeout (ignored)")
    except Exception as e:
        log(f"Feed error: {e}")

    if LIVE.get("active") and (stats_dirty or now >= next_live):
        update_live_leaderboard(now)
        try: next_live = now + datetime.timedelta(seconds=LIVE.get("next_update_sec", 30))
        except Exception: next_live = now + datetime.timedelta(seconds=30)
        stats_dirty = False

    if now >= next_evening:
        lines = [f"Mid-shift recap {now:%d %b %Y %H:%M}"]
        for u, c in sorted(CALLS.items(), key=lambda kv: (-kv[1], kv[0])):
            avg = (DUR_SEC[u] / c) / 60 if c else 0
            lines.append(f"{u}: {c}  |  avg {avg:.1f} min  |  after 00:00: {AFTER_0000.get(u,0)}")
        dest_unit = _leaderboard_target_unit()
        post(dest_unit, "CALL COUNT", "\n".join(lines))
        log(f"Mid-shift recap sent to {dest_unit}")
        next_evening = next_at(19, now)

    if now >= next_morning:
        current_shift_start = shift_start(now)
        yday = (current_shift_start - datetime.timedelta(days=1)).strftime('%d %b %Y')
        lines = [f"Daily runs {yday}"]
        for u, c in sorted(CALLS.items(), key=lambda kv: (-kv[1], kv[0])):
            avg = (DUR_SEC[u] / c) / 60 if c else 0
            lines.append(f"{u}: {c}  |  avg {avg:.1f} min  |  after 00:00: {AFTER_0000.get(u,0)}")
        dest_unit = _leaderboard_target_unit()
        post(dest_unit, "CALL COUNT", "\n".join(lines))
        log(f"End-of-shift recap sent to {dest_unit}")
        _stats_save(STATS_FN, CALLS, DUR_SEC, AFTER_0000, MAX_SEC, TRANSPORTING_COUNT, AT_HOSPITAL_COUNT, RIDE_IN_COUNT, DURATION_KNOWN_CALLS)
        _pstats_save(
            P_STATS_FN,
            PERSONNEL_NAMES,
            P_CALLS,
            P_DUR_SEC,
            P_AFTER_0000,
            P_MAX_SEC,
            P_TRANSPORTING_COUNT,
            P_AT_HOSPITAL_COUNT,
            P_RIDE_IN_COUNT,
        )
        CALLS.clear(); DUR_SEC.clear(); AFTER_0000.clear(); MAX_SEC.clear(); TRANSPORTING_COUNT.clear(); AT_HOSPITAL_COUNT.clear(); RIDE_IN_COUNT.clear()
        PERSONNEL_NAMES.clear(); P_CALLS.clear(); P_DUR_SEC.clear(); P_AFTER_0000.clear(); P_MAX_SEC.clear(); P_TRANSPORTING_COUNT.clear(); P_AT_HOSPITAL_COUNT.clear(); P_RIDE_IN_COUNT.clear(); DURATION_KNOWN_CALLS.clear()
        SHIFT_DT = current_shift_start
        STATS_FN = stats_file(SHIFT_DT)
        _stats_save(STATS_FN, CALLS, DUR_SEC, AFTER_0000, MAX_SEC, TRANSPORTING_COUNT, AT_HOSPITAL_COUNT, RIDE_IN_COUNT, DURATION_KNOWN_CALLS)
        P_STATS_FN = personnel_stats_file(SHIFT_DT)
        _pstats_save(
            P_STATS_FN,
            PERSONNEL_NAMES,
            P_CALLS,
            P_DUR_SEC,
            P_AFTER_0000,
            P_MAX_SEC,
            P_TRANSPORTING_COUNT,
            P_AT_HOSPITAL_COUNT,
            P_RIDE_IN_COUNT,
        )
        next_morning = next_at(7, now)
        next_evening = next_at(19, now)

    # Ensure GitHub Pages live leaderboard data gets a periodic refresh write
    if now >= next_stats_refresh:
        _stats_save(STATS_FN, CALLS, DUR_SEC, AFTER_0000, MAX_SEC, TRANSPORTING_COUNT, AT_HOSPITAL_COUNT, RIDE_IN_COUNT, DURATION_KNOWN_CALLS)
        _pstats_save(
            P_STATS_FN,
            PERSONNEL_NAMES,
            P_CALLS,
            P_DUR_SEC,
            P_AFTER_0000,
            P_MAX_SEC,
            P_TRANSPORTING_COUNT,
            P_AT_HOSPITAL_COUNT,
            P_RIDE_IN_COUNT,
        )
        next_stats_refresh = now + datetime.timedelta(seconds=STATS_REFRESH_SEC)

    time.sleep(POLL_SEC + random.uniform(0, 1))

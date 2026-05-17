import argparse
import base64
import datetime
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from typing import Dict, Tuple

import requests


TZ = datetime.datetime.now().astimezone().tzinfo

# Shift starts at 07:00 local time (matches alerts script)
SHIFT_HOUR = 7

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
    _PARENT = os.path.abspath(os.path.join(_HERE, os.pardir))
    if os.path.isdir(os.path.join(_HERE, "data")):
        _REPO_ROOT = _HERE
    elif os.path.isdir(os.path.join(_PARENT, "data")):
        _REPO_ROOT = _PARENT
    else:
        _REPO_ROOT = _HERE
except Exception:
    _REPO_ROOT = os.getcwd()

DEFAULT_STATS_DIR = os.path.join(_REPO_ROOT, "data", "shift_stats")
STATS_DIR = os.environ.get("SHIFT_STATS_DIR", DEFAULT_STATS_DIR)

# Default leaderboard JSON output (can be overridden via CLI)
DEFAULT_OUTPUT_PATH = os.path.join(_REPO_ROOT, "data", "leaderboards.json")

STATS_FILENAME_RE = re.compile(r"shift_stats_(\d{4}-\d{2}-\d{2})\.json")

# Timeframe lengths expressed in number of shifts (days)
TIMEFRAME_LENGTHS = {
    "day": 1,
    "week": 7,
    "month": 30,
    "year": 365,
}


def log(msg: str) -> None:
    ts = datetime.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts}  {msg}", file=sys.stdout, flush=True)


def shift_start(now: datetime.datetime) -> datetime.datetime:
    """Return the shift start datetime for a given moment."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=TZ)
    base = now.replace(hour=SHIFT_HOUR, minute=0, second=0, microsecond=0)
    return base if now >= base else base - datetime.timedelta(days=1)


def stats_file(dt: datetime.date) -> str:
    return os.path.join(STATS_DIR, f"shift_stats_{dt:%Y-%m-%d}.json")


def _stats_load(fp: str):
    if os.path.exists(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                j = json.load(f)
            return (
                defaultdict(int, j.get("calls", {})),
                defaultdict(float, j.get("dur_sec", {})),
                defaultdict(int, j.get("after_0000", {})),
                defaultdict(int, j.get("max_sec", {})),
            )
        except Exception:
            # Treat unreadable or malformed files as missing
            return defaultdict(int), defaultdict(float), defaultdict(int), defaultdict(
                int
            )
    return defaultdict(int), defaultdict(float), defaultdict(int), defaultdict(int)


def _iter_shift_files() -> Dict[datetime.date, str]:
    """Return a mapping of shift_date -> stats file path."""
    try:
        names = os.listdir(STATS_DIR)
    except Exception:
        return {}

    out: Dict[datetime.date, str] = {}
    for name in names:
        m = STATS_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            d = datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            continue
        out[d] = os.path.join(STATS_DIR, name)
    return out


def _range_aggregate(
    start_date: datetime.date, end_date: datetime.date
) -> Dict[str, Dict[str, float]]:
    """
    Aggregate stats over [start_date, end_date] inclusive.

    Returns per-unit metrics supporting all leaderboard tables.
    """
    files_by_date = _iter_shift_files()
    total_calls: Dict[str, int] = defaultdict(int)
    total_dur: Dict[str, float] = defaultdict(float)
    total_after_midnight: Dict[str, int] = defaultdict(int)
    single_shift_max_calls: Dict[str, int] = defaultdict(int)
    highest_call_dur_sec: Dict[str, int] = defaultdict(int)
    single_shift_max_after_midnight: Dict[str, int] = defaultdict(int)

    d = start_date
    one_day = datetime.timedelta(days=1)
    while d <= end_date:
        fp = files_by_date.get(d)
        if not fp:
            d += one_day
            continue
        calls, dur_sec, after_0000, max_sec = _stats_load(fp)

        for unit, c in calls.items():
            c_int = int(c)
            total_calls[unit] += c_int
            if c_int > single_shift_max_calls[unit]:
                single_shift_max_calls[unit] = c_int

        for unit, secs in dur_sec.items():
            total_dur[unit] += float(secs)

        for unit, c in after_0000.items():
            c_int = int(c)
            total_after_midnight[unit] += c_int
            if c_int > single_shift_max_after_midnight[unit]:
                single_shift_max_after_midnight[unit] = c_int

        for unit, s in max_sec.items():
            s_int = int(s)
            if s_int > highest_call_dur_sec[unit]:
                highest_call_dur_sec[unit] = s_int

        d += one_day

    # Build per-unit payload
    result: Dict[str, Dict[str, float]] = {}
    units = set(
        list(total_calls.keys())
        + list(total_dur.keys())
        + list(total_after_midnight.keys())
        + list(highest_call_dur_sec.keys())
    )
    for unit in sorted(units):
        calls = total_calls.get(unit, 0)
        dur = total_dur.get(unit, 0.0)
        after = total_after_midnight.get(unit, 0)
        max_shift_calls = single_shift_max_calls.get(unit, 0)
        max_shift_after = single_shift_max_after_midnight.get(unit, 0)
        max_call_sec = highest_call_dur_sec.get(unit, 0)

        avg_mins = (dur / calls) / 60.0 if calls else 0.0
        highest_call_mins = max_call_sec / 60.0 if max_call_sec else 0.0

        result[unit] = {
            "total_calls": int(calls),
            "single_shift_max_calls": int(max_shift_calls),
            "avg_call_duration_mins": round(avg_mins, 2),
            "highest_call_duration_mins": round(highest_call_mins, 2),
            "total_calls_after_midnight": int(after),
            "single_shift_max_after_midnight": int(max_shift_after),
        }
    return result


def _single_shift_metrics(
    shift_date: datetime.date,
) -> Tuple[Dict[str, int], Dict[str, float], Dict[str, int], Dict[str, int]]:
    """Return raw per-unit stats for a single shift date."""
    fp = stats_file(shift_date)
    calls, dur_sec, after_0000, max_sec = _stats_load(fp)
    # Coerce to plain dicts
    return (
        {k: int(v) for k, v in calls.items()},
        {k: float(v) for k, v in dur_sec.items()},
        {k: int(v) for k, v in after_0000.items()},
        {k: int(v) for k, v in max_sec.items()},
    )


def _build_shift_table(shift_date: datetime.date) -> Dict[str, Dict[str, float]]:
    """Build the 'Today' / 'Prior Shift' style table for a single date."""
    calls, dur_sec, after_0000, max_sec = _single_shift_metrics(shift_date)
    out: Dict[str, Dict[str, float]] = {}
    units = set(
        list(calls.keys())
        + list(dur_sec.keys())
        + list(after_0000.keys())
        + list(max_sec.keys())
    )
    for unit in sorted(units):
        c = calls.get(unit, 0)
        dur = dur_sec.get(unit, 0.0)
        after = after_0000.get(unit, 0)
        max_call_sec = max_sec.get(unit, 0)
        avg_mins = (dur / c) / 60.0 if c else 0.0
        highest_call_mins = max_call_sec / 60.0 if max_call_sec else 0.0
        out[unit] = {
            "calls": int(c),
            "avg_time_on_call_mins": round(avg_mins, 2),
            "highest_time_on_call_mins": round(highest_call_mins, 2),
            "calls_after_midnight": int(after),
        }
    return out


def build_leaderboard_payload(
    now: datetime.datetime | None = None,
) -> Dict[str, object]:
    """
    Build the complete leaderboard payload for all time windows.

    The payload is designed so that the GitHub-based dashboard can render
    the existing layout (Today, Prior Shift, Week, Month, etc.) without
    needing to know the raw per-shift stats format.
    """
    if now is None:
        now = datetime.datetime.now(TZ)
    shift_dt = shift_start(now)
    today_date = shift_dt.date()
    prior_date = (shift_dt - datetime.timedelta(days=1)).date()

    # Aggregate week / month / year windows based on shift dates
    week_start = today_date - datetime.timedelta(days=TIMEFRAME_LENGTHS["week"] - 1)
    month_start = today_date - datetime.timedelta(days=TIMEFRAME_LENGTHS["month"] - 1)
    year_start = today_date - datetime.timedelta(days=TIMEFRAME_LENGTHS["year"] - 1)

    payload: Dict[str, object] = {
        "generated_at": now.astimezone(TZ).isoformat(),
        "timezone": str(TZ),
        "shift_hour_local": SHIFT_HOUR,
        "today": {
            "shift_date": today_date.isoformat(),
            "units": _build_shift_table(today_date),
        },
        "prior_shift": {
            "shift_date": prior_date.isoformat(),
            "units": _build_shift_table(prior_date),
        },
        "week": {
            "start_date": week_start.isoformat(),
            "end_date": today_date.isoformat(),
            "units": _range_aggregate(week_start, today_date),
        },
        "month": {
            "start_date": month_start.isoformat(),
            "end_date": today_date.isoformat(),
            "units": _range_aggregate(month_start, today_date),
        },
        "year": {
            "start_date": year_start.isoformat(),
            "end_date": today_date.isoformat(),
            "units": _range_aggregate(year_start, today_date),
        },
    }

    return payload


def _detect_git_root(start_dir: str | None = None) -> str | None:
    """
    Find the repo root by walking up for a `.git` entry.

    This avoids invoking `git`, which can fail under Scheduled Tasks running as
    SYSTEM due to Git's "safe.directory" ownership checks.
    """
    start_dir = start_dir or os.getcwd()
    try:
        cur = os.path.abspath(start_dir)
    except Exception:
        cur = start_dir

    while True:
        dotgit = os.path.join(cur, ".git")
        if os.path.isdir(dotgit) or os.path.isfile(dotgit):
            return cur
        parent = os.path.dirname(cur)
        if not parent or parent == cur:
            break
        cur = parent
    return None


def _resolve_git_dir(repo_root: str) -> str | None:
    dotgit = os.path.join(repo_root, ".git")
    if os.path.isdir(dotgit):
        return dotgit
    if os.path.isfile(dotgit):
        # Worktrees/submodules can store a gitdir pointer in a `.git` file.
        try:
            raw = open(dotgit, "r", encoding="utf-8", errors="replace").read()
        except Exception:
            return None
        m = re.search(r"(?im)^[ \t]*gitdir:\s*(.+?)\s*$", raw)
        if not m:
            return None
        gitdir = m.group(1).strip()
        if not gitdir:
            return None
        if not os.path.isabs(gitdir):
            gitdir = os.path.normpath(os.path.join(repo_root, gitdir))
        return gitdir if os.path.isdir(gitdir) else None
    return None


def _load_github_token() -> str | None:
    """Resolve a GitHub token from env or GithubToken.txt next to the script."""
    env_val = (os.environ.get("GITHUB_TOKEN") or "").strip()
    if env_val:
        return env_val
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        here = os.getcwd()
    candidates = [
        os.path.join(here, "GithubToken.txt"),
        os.path.join(os.getcwd(), "GithubToken.txt"),
    ]
    seen = set()
    for fp in candidates:
        if not fp or fp in seen:
            continue
        seen.add(fp)
        try:
            if not os.path.exists(fp):
                continue
            with open(fp, "r", encoding="utf-8") as f:
                for line in f:
                    token = line.strip()
                    if token:
                        return token
        except Exception:
            continue
    return None


def _detect_owner_repo_from_git(repo_root: str) -> Tuple[str | None, str | None]:
    """Try to infer GitHub owner/repo from the local `.git/config` remote.origin.url."""

    def _parse_owner_repo(url: str) -> Tuple[str | None, str | None]:
        u = (url or "").strip()
        if not u:
            return None, None

        lower = u.lower()
        tail = ""
        if lower.startswith("git@github.com:"):
            tail = u[len("git@github.com:") :]
        elif "github.com" in lower:
            idx = lower.index("github.com")
            tail = u[idx + len("github.com") :]
            tail = tail.lstrip(":/")
        else:
            return None, None

        parts = [p for p in tail.split("/") if p]
        if len(parts) < 2:
            return None, None
        owner = parts[0].strip()
        repo = parts[1].strip()
        if repo.endswith(".git"):
            repo = repo[:-4]
        if not owner or not repo:
            return None, None
        return owner, repo

    git_dir = _resolve_git_dir(repo_root)
    if not git_dir:
        return None, None

    config_path = os.path.join(git_dir, "config")
    try:
        lines = open(config_path, "r", encoding="utf-8", errors="replace").read().splitlines()
    except Exception:
        return None, None

    in_origin = False
    origin_url = ""
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            in_origin = line.lower() == '[remote "origin"]'
            continue
        if not in_origin:
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip().lower() == "url":
            origin_url = v.strip()
            break

    return _parse_owner_repo(origin_url)


def _detect_branch_from_git(repo_root: str) -> str | None:
    git_dir = _resolve_git_dir(repo_root)
    if not git_dir:
        return None
    head_path = os.path.join(git_dir, "HEAD")
    try:
        head = open(head_path, "r", encoding="utf-8", errors="replace").read().strip()
    except Exception:
        return None
    m = re.match(r"(?i)^ref:\s*refs/heads/(.+?)\s*$", head)
    if not m:
        return None
    branch = m.group(1).strip()
    return branch or None


def _push_via_github_api(output_path: str, message: str, token: str) -> bool:
    """Create or update the JSON file via GitHub Contents API."""
    repo_root = _detect_git_root(os.path.dirname(os.path.abspath(output_path)))
    if not repo_root:
        log("Git repository not detected; skipping GitHub API push.")
        return False

    owner = (os.environ.get("GITHUB_OWNER") or "").strip()
    repo = (os.environ.get("GITHUB_REPO") or "").strip()
    if not owner or not repo:
        gh_repo = (os.environ.get("GITHUB_REPOSITORY") or "").strip()
        if gh_repo and "/" in gh_repo:
            owner, repo = gh_repo.split("/", 1)
            owner, repo = owner.strip(), repo.strip()
    if not owner or not repo:
        owner, repo = _detect_owner_repo_from_git(repo_root)
    if not owner or not repo:
        log("Unable to determine GitHub owner/repo; skipping GitHub API push.")
        return False

    branch = (
        (os.environ.get("GITHUB_BRANCH") or "").strip()
        or (os.environ.get("GITHUB_REF_NAME") or "").strip()
    )
    if not branch:
        branch = _detect_branch_from_git(repo_root) or "main"

    rel_path = os.path.relpath(os.path.abspath(output_path), repo_root).replace(
        "\\", "/"
    )

    try:
        with open(output_path, "rb") as f:
            content_bytes = f.read()
    except Exception as e:
        log(f"Failed to read leaderboard JSON for GitHub push: {e}")
        return False

    b64_content = base64.b64encode(content_bytes).decode("ascii")

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "PPFD-Leaderboard-Calculator",
    }
    api_base = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
    url = f"{api_base}/repos/{owner}/{repo}/contents/{rel_path}"

    sha = None
    existing_bytes: bytes | None = None
    try:
        resp = requests.get(url, headers=headers, params={"ref": branch}, timeout=30)
    except Exception as e:
        log(f"GitHub GET existing file failed: {e}")
        return False
    if resp.status_code == 200:
        try:
            data = resp.json()
            sha = data.get("sha")
            if data.get("encoding") == "base64" and data.get("content"):
                try:
                    existing_bytes = base64.b64decode(data.get("content"), validate=False)
                except Exception:
                    existing_bytes = None
        except Exception:
            sha = None
    elif resp.status_code not in (200, 404):
        log(f"GitHub GET existing file HTTP {resp.status_code}: {resp.text[:300]}")
        return False

    if existing_bytes is not None and existing_bytes == content_bytes:
        log(f"No changes detected for {owner}/{repo}:{rel_path}; skipping GitHub push.")
        return True

    put_payload = {"message": message, "content": b64_content, "branch": branch}
    if sha:
        put_payload["sha"] = sha

    try:
        resp2 = requests.put(url, headers=headers, json=put_payload, timeout=30)
    except Exception as e:
        log(f"GitHub PUT contents failed: {e}")
        return False

    if resp2.status_code not in (200, 201):
        log(f"GitHub PUT contents HTTP {resp2.status_code}: {resp2.text[:300]}")
        return False

    log(f"Updated GitHub leaderboard at {owner}/{repo}@{branch}:{rel_path}")
    return True


def _git_commit_and_push(output_path: str, message: str) -> None:
    repo_root = _detect_git_root(os.path.dirname(os.path.abspath(output_path)))
    if not repo_root:
        log("Git repository not detected; skipping commit/push.")
        return

    rel_path = os.path.relpath(os.path.abspath(output_path), repo_root)

    def _run_git(args):
        try:
            proc = subprocess.run(
                ["git"] + args,
                cwd=repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except Exception as e:
            log(f"git {' '.join(args)} failed: {e}")
            return 1, ""
        if proc.returncode != 0:
            log(f"git {' '.join(args)} failed (code {proc.returncode}): {proc.stdout}")
        return proc.returncode, proc.stdout

    # Stage the file
    rc, _ = _run_git(["add", rel_path])
    if rc != 0:
        return

    # Only commit if there are staged changes
    rc, status_out = _run_git(["status", "--porcelain", rel_path])
    if rc != 0:
        return
    if not status_out.strip():
        log("No changes to commit for leaderboard JSON.")
        return

    rc, _ = _run_git(["commit", "-m", message])
    if rc != 0:
        return

    # Push using existing auth (PAT/SSH/credential helper)
    _run_git(["push"])


def _push_leaderboards_to_github(output_path: str, message: str) -> None:
    """
    Push leaderboard updates to GitHub, preferring the REST API with a token
    from GithubToken.txt or GITHUB_TOKEN, and falling back to git CLI.
    """
    token = _load_github_token()
    if token:
        ok = _push_via_github_api(output_path, message, token)
        if ok:
            return
    _git_commit_and_push(output_path, message)


def _maybe_update_ppfd_alerts_datajson(stats_dir: str, message: str) -> None:
    """
    If a local clone of the ppfd-alerts GitHub repo is present next to this
    package, run its scripts/generate_leaderboard.py to regenerate docs/data.json
    and push that file to GitHub using the same token logic.
    """
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        here = os.getcwd()
    repo = os.path.join(here, "ppfd-alerts")
    script = os.path.join(repo, "scripts", "generate_leaderboard.py")
    out_path = os.path.join(repo, "docs", "data.json")

    if not os.path.exists(script):
        log("ppfd-alerts/scripts/generate_leaderboard.py not found; skipping data.json generation.")
        return

    abs_stats = os.path.abspath(stats_dir or ".")
    cmd = [sys.executable or "python", script, "--stats-dir", abs_stats, "--out", out_path]

    try:
        proc = subprocess.run(
            cmd,
            cwd=repo,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except Exception as e:
        log(f"Error running ppfd-alerts generate_leaderboard.py: {e}")
        return

    if proc.returncode != 0:
        log(
            "ppfd-alerts generate_leaderboard.py failed "
            f"(code {proc.returncode}): {proc.stdout[:400]}"
        )
        return

    log("Regenerated ppfd-alerts/docs/data.json")
    _push_leaderboards_to_github(out_path, message)

    roster_out = os.path.join(repo, "docs", "roster_units.json")
    if os.path.exists(roster_out):
        _push_leaderboards_to_github(roster_out, message)

    index_out = os.path.join(repo, "docs", "index.html")
    if os.path.exists(index_out):
        _push_leaderboards_to_github(index_out, message)


def write_leaderboards_json(
    output_path: str, do_git_push: bool = True, now: datetime.datetime | None = None
) -> str:
    payload = build_leaderboard_payload(now=now)
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    tmp_path = output_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    os.replace(tmp_path, output_path)

    log(f"Wrote leaderboard JSON to {output_path}")

    if do_git_push:
        message = f"Update leaderboards {payload['generated_at']}"
        _push_leaderboards_to_github(output_path, message)

    return output_path


def main(argv: list[str] | None = None) -> int:
    global STATS_DIR

    default_stats_dir = STATS_DIR
    parser = argparse.ArgumentParser(
        description=(
            "Generate PPFD leaderboard JSON from shift_stats files and optionally "
            "commit/push to GitHub."
        )
    )
    parser.add_argument(
        "--stats-dir",
        default=default_stats_dir,
        help=(
            "Directory containing shift_stats_YYYY-MM-DD.json files. "
            "Defaults to SHIFT_STATS_DIR or ../data/shift_stats."
        ),
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_PATH,
        help=(
            "Output JSON file path for leaderboard data "
            "(default: ../data/leaderboards.json)."
        ),
    )
    parser.add_argument(
        "--no-git",
        action="store_true",
        help="Do not run git add/commit/push after writing JSON.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print JSON payload to stdout instead of writing a file.",
    )

    args = parser.parse_args(argv)

    STATS_DIR = args.stats_dir

    now = datetime.datetime.now(TZ)

    if args.dry_run:
        payload = build_leaderboard_payload(now=now)
        json.dump(payload, sys.stdout, indent=2, sort_keys=True)
        print()
        return 0

    write_leaderboards_json(
        output_path=args.output,
        do_git_push=not args.no_git,
        now=now,
    )

    if not args.no_git:
        msg = f"Update leaderboard data.json {now.isoformat()}"
        _maybe_update_ppfd_alerts_datajson(STATS_DIR, msg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import os
import re
import json
import datetime
from collections import defaultdict
from pathlib import Path
import argparse


SHIFT_HOUR = 7
STATS_FILENAME_RE = re.compile(r"shift_stats_(\d{4}-\d{2}-\d{2})\.json")

THREAD_IDS = {
    "GENERAL": 1, "R33": 2, "E33": 3, "T33": 4, "33FD": 5, "LR36": 6,
    "HM33": 7, "R34": 8, "E34": 9, "TR34": 10, "E36": 11, "S36": 12,
    "R36": 13, "E35": 14, "34FD": 15, "36FD": 16, "D35": 17, "R35": 18,
    "35FD": 19, "LOG": 20, "E136": 7126,
}
WATCH_SET = set(THREAD_IDS) - {"GENERAL"}

TIMEFRAME_LENGTHS = {
    "day": 1,
    "week": 7,
    "month": 30,
    "year": 365,
}


def shift_start(now: datetime.datetime) -> datetime.datetime:
    base = now.replace(hour=SHIFT_HOUR, minute=0, second=0, microsecond=0)
    return base if now >= base else base - datetime.timedelta(days=1)


def load_stats(fp: Path):
    try:
        with fp.open("r", encoding="utf-8") as f:
            j = json.load(f)
            return (
                defaultdict(int, j.get("calls", {})),
                defaultdict(int, j.get("dur_sec", {})),
                defaultdict(int, j.get("after_0000", {})),
            )
    except Exception:
        return defaultdict(int), defaultdict(int), defaultdict(int)


def aggregate_timeframe_stats(stats_dir: Path, period_key: str, now: datetime.datetime | None = None):
    now = now or datetime.datetime.now()
    cutoff_date = None
    if period_key != "alltime":
        shift_date = shift_start(now).date()
        days = TIMEFRAME_LENGTHS[period_key]
        cutoff_date = shift_date - datetime.timedelta(days=days - 1)
    calls = defaultdict(int)
    dur = defaultdict(int)
    after_midnight = defaultdict(int)
    if not stats_dir.exists():
        return dict(calls), dict(dur), dict(after_midnight)
    for name in os.listdir(stats_dir):
        m = STATS_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            file_date = datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            continue
        if cutoff_date and file_date < cutoff_date:
            continue
        file_calls, file_dur, file_after = load_stats(stats_dir / name)
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


def format_leaderboard_body(label: str, period_key: str, calls: dict, dur: dict, after_midnight: dict, now: datetime.datetime) -> str:
    shift_date = shift_start(now).date()
    if period_key == 'alltime':
        header = f"{label} runs through {shift_date:%d %b %Y}"
    else:
        days = TIMEFRAME_LENGTHS[period_key]
        if days == 1:
            header = f"{label} runs {shift_date:%d %b %Y}"
        else:
            start_date = shift_date - datetime.timedelta(days=days - 1)
            header = f"{label} runs {start_date:%d %b %Y} - {shift_date:%d %b %Y}"
    lines = [header]
    if not calls:
        lines.append("No runs recorded.")
    else:
        for unit, count in sorted(calls.items(), key=lambda kv: (-kv[1], kv[0])):
            avg_min = (dur.get(unit, 0) / count) / 60 if count else 0
            lines.append(f"{unit}: {count}  |  avg {avg_min:.1f} min  |  after 00:00: {after_midnight.get(unit, 0)}")
    return "\n".join(lines)


def compute_period(stats_dir: Path, period_key: str):
    now = datetime.datetime.now()
    label = {
        "day": "Daily", "week": "Weekly", "month": "Monthly", "year": "Yearly", "alltime": "All-time"
    }.get(period_key, "Daily")
    calls, dur, after = aggregate_timeframe_stats(stats_dir, period_key, now)
    text = format_leaderboard_body(label, period_key, calls, dur, after, now)
    return {"text": text, "updated": datetime.datetime.utcnow().isoformat() + "Z"}


def compute_prior(stats_dir: Path):
    now = datetime.datetime.now()
    prev_date = (shift_start(now) - datetime.timedelta(days=1)).date()
    fn = stats_dir / f"shift_stats_{prev_date:%Y-%m-%d}.json"
    calls, dur, after = load_stats(fn)
    calls = {k: int(v) for k, v in calls.items() if k in WATCH_SET}
    dur = {k: int(v) for k, v in dur.items() if k in WATCH_SET}
    after = {k: int(v) for k, v in after.items() if k in WATCH_SET}
    text = format_leaderboard_body("Daily", "day", calls, dur, after, now)
    return {"text": text, "updated": datetime.datetime.utcnow().isoformat() + "Z"}


def main():
    parser = argparse.ArgumentParser(description="Generate leaderboard data.json for GitHub Pages")
    parser.add_argument('--stats-dir', default=os.environ.get('SHIFT_STATS_DIR', '.'), help='Directory containing shift_stats_*.json files')
    parser.add_argument('--out', default=str(Path('docs') / 'data.json'), help='Output path for data.json')
    args = parser.parse_args()

    stats_dir = Path(args.stats_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "today": compute_period(stats_dir, "day"),
        "prior": compute_prior(stats_dir),
        "week": compute_period(stats_dir, "week"),
    }

    tmp = out_path.with_suffix('.json.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(out_path)


if __name__ == '__main__':
    main()


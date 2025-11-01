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
                defaultdict(int, j.get("max_sec", {})),
            )
    except Exception:
        return defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)


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
    max_sec = defaultdict(int)
    if not stats_dir.exists():
        return dict(calls), dict(dur), dict(after_midnight), dict(max_sec)
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
        file_calls, file_dur, file_after, file_max = load_stats(stats_dir / name)
        for unit, count in file_calls.items():
            if unit in WATCH_SET:
                calls[unit] += int(count)
        for unit, seconds in file_dur.items():
            if unit in WATCH_SET:
                dur[unit] += int(seconds)
        for unit, count in file_after.items():
            if unit in WATCH_SET:
                after_midnight[unit] += int(count)
        for unit, mx in file_max.items():
            if unit in WATCH_SET:
                if int(mx) > max_sec[unit]:
                    max_sec[unit] = int(mx)
    return dict(calls), dict(dur), dict(after_midnight), dict(max_sec)


def _date_range_for_period(now: datetime.datetime, period_key: str) -> tuple[datetime.date, datetime.date]:
    end_date = shift_start(now).date()
    days = TIMEFRAME_LENGTHS[period_key]
    start_date = end_date - datetime.timedelta(days=days - 1)
    return start_date, end_date


def compute_shift_breakdown(stats_dir: Path, period_key: str, now: datetime.datetime) -> list[dict]:
    start_date, end_date = _date_range_for_period(now, period_key)
    # unit -> letter -> aggregates
    letters = ['A', 'B', 'C']
    sum_calls: dict[str, dict[str, int]] = defaultdict(lambda: {l: 0 for l in letters})
    max_calls: dict[str, dict[str, int]] = defaultdict(lambda: {l: 0 for l in letters})
    sum_dur: dict[str, dict[str, int]] = defaultdict(lambda: {l: 0 for l in letters})  # seconds
    sum_dur_calls: dict[str, dict[str, int]] = defaultdict(lambda: {l: 0 for l in letters})
    max_dur: dict[str, dict[str, int]] = defaultdict(lambda: {l: 0 for l in letters})  # seconds
    sum_after: dict[str, dict[str, int]] = defaultdict(lambda: {l: 0 for l in letters})
    max_after: dict[str, dict[str, int]] = defaultdict(lambda: {l: 0 for l in letters})

    for name in os.listdir(stats_dir):
        m = STATS_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            d = datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            continue
        if not (start_date <= d <= end_date):
            continue
        letter = _shift_letter_for(d)
        file_calls, file_dur, file_after, file_max = load_stats(stats_dir / name)
        for unit in list(file_calls.keys()) + list(file_dur.keys()) + list(file_after.keys()) + list(file_max.keys()):
            if unit not in WATCH_SET:
                continue
            c = int(file_calls.get(unit, 0))
            s = int(file_dur.get(unit, 0))
            a = int(file_after.get(unit, 0))
            mx = int(file_max.get(unit, 0))
            sum_calls[unit][letter] += c
            if c > max_calls[unit][letter]:
                max_calls[unit][letter] = c
            sum_dur[unit][letter] += s
            sum_dur_calls[unit][letter] += c
            if mx > max_dur[unit][letter]:
                max_dur[unit][letter] = mx
            sum_after[unit][letter] += a
            if a > max_after[unit][letter]:
                max_after[unit][letter] = a

    rows = []
    units = sorted(set(list(sum_calls.keys()) + list(sum_dur.keys()) + list(sum_after.keys())))
    for unit in units:
        calls_abc = [int(sum_calls[unit][l]) for l in letters]
        calls_max_abc = [int(max_calls[unit][l]) for l in letters]
        avg_min_abc = []
        for l in letters:
            denom = sum_dur_calls[unit][l]
            avg_min_abc.append(round((sum_dur[unit][l] / denom) / 60.0, 1) if denom else 0.0)
        max_min_abc = [round(max_dur[unit][l] / 60.0, 1) if max_dur[unit][l] else 0.0 for l in letters]
        after_abc = [int(sum_after[unit][l]) for l in letters]
        after_max_abc = [int(max_after[unit][l]) for l in letters]
        rows.append({
            "unit": unit,
            "calls_abc": calls_abc,
            "calls_max_abc": calls_max_abc,
            "avg_min_abc": avg_min_abc,
            "max_min_abc": max_min_abc,
            "after_abc": after_abc,
            "after_max_abc": after_max_abc,
            "total_calls": sum(calls_abc),
            "total_after": sum(after_abc),
        })
    # Sort by total calls desc then unit
    rows.sort(key=lambda r: (-r.get("total_calls", 0), r["unit"]))
    return rows


def _rows_from(calls: dict, dur: dict, after_midnight: dict, max_sec: dict | None = None):
    rows = []
    for unit, count in sorted(calls.items(), key=lambda kv: (-kv[1], kv[0])):
        avg_min = (dur.get(unit, 0) / count) / 60 if count else 0.0
        r = {
            "unit": unit,
            "calls": int(count),
            "avg_min": round(avg_min, 1),
            "after_0000": int(after_midnight.get(unit, 0)),
        }
        if max_sec is not None:
            r["max_min"] = round(int(max_sec.get(unit, 0)) / 60.0, 1)
        rows.append(r)
    return rows


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
        for r in _rows_from(calls, dur, after_midnight):
            lines.append(f"{r['unit']}: {r['calls']}  |  avg {r['avg_min']:.1f} min  |  after 00:00: {r['after_0000']}")
    return "\n".join(lines)


def _shift_letter_for(d: datetime.date) -> str:
    # Allow override via env. Defaults set so that 2025-10-31 = C, 2025-11-01 = A.
    anchor_str = os.environ.get('SHIFT_ANCHOR_DATE', '2025-11-01')
    anchor_letter = os.environ.get('SHIFT_ANCHOR_LETTER', 'A').upper()
    try:
        anchor = datetime.datetime.strptime(anchor_str, '%Y-%m-%d').date()
    except Exception:
        anchor = datetime.date(2025, 1, 1)
    letters = ['A', 'B', 'C']
    try:
        idx0 = letters.index(anchor_letter)
    except ValueError:
        idx0 = 0
    delta = (d - anchor).days
    return letters[(idx0 + (delta % 3)) % 3]


def compute_period(stats_dir: Path, period_key: str):
    now = datetime.datetime.now()
    label = {
        "day": "Daily", "week": "Weekly", "month": "Monthly", "year": "Yearly", "alltime": "All-time"
    }.get(period_key, "Daily")
    calls, dur, after, max_sec = aggregate_timeframe_stats(stats_dir, period_key, now)
    text = format_leaderboard_body(label, period_key, calls, dur, after, now)
    if period_key in ("week", "month"):
        rows = compute_shift_breakdown(stats_dir, period_key, now)
    else:
        rows = _rows_from(calls, dur, after, max_sec)
    meta = {}
    if period_key == 'day':
        sd = shift_start(now).date()
        meta['shift_date'] = f"{_shift_letter_for(sd)}-Shift {sd:%m/%d/%y}"
    elif period_key == 'week':
        sd = shift_start(now).date()
        start_date = sd - datetime.timedelta(days=TIMEFRAME_LENGTHS['week'] - 1)
        meta['range'] = f"{start_date:%b %d} - {sd:%b %d}"
    elif period_key == 'month':
        sd = shift_start(now).date()
        start_date = sd - datetime.timedelta(days=TIMEFRAME_LENGTHS['month'] - 1)
        meta['range'] = f"{start_date:%b %d} - {sd:%b %d}"
    return {
        "label": label,
        "period": period_key,
        "text": text,
        "rows": rows,
        "meta": meta,
        "updated": datetime.datetime.utcnow().isoformat() + "Z",
    }


def compute_prior(stats_dir: Path):
    now = datetime.datetime.now()
    prev_date = (shift_start(now) - datetime.timedelta(days=1)).date()
    fn = stats_dir / f"shift_stats_{prev_date:%Y-%m-%d}.json"
    calls, dur, after, max_sec = load_stats(fn)
    calls = {k: int(v) for k, v in calls.items() if k in WATCH_SET}
    dur = {k: int(v) for k, v in dur.items() if k in WATCH_SET}
    after = {k: int(v) for k, v in after.items() if k in WATCH_SET}
    text = format_leaderboard_body("Daily", "day", calls, dur, after, now)
    rows = _rows_from(calls, dur, after, max_sec)
    return {
        "label": "Daily",
        "period": "day",
        "text": text,
        "rows": rows,
        "meta": {"shift_date": f"{_shift_letter_for(prev_date)}-Shift {prev_date:%m/%d/%y}"},
        "updated": datetime.datetime.utcnow().isoformat() + "Z",
    }


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
        "month": compute_period(stats_dir, "month"),
    }

    tmp = out_path.with_suffix('.json.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(out_path)


if __name__ == '__main__':
    main()

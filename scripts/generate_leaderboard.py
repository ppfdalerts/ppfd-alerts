import os
import re
import json
import datetime
import math
from collections import defaultdict
from pathlib import Path
import argparse


SHIFT_HOUR = 7
STATS_FILENAME_RE = re.compile(r"shift_stats_(\d{4}-\d{2}-\d{2})\.json")
ROSTER_FILENAME_RE = re.compile(r"roster_units_(\d{4}-\d{2}-\d{2})\.json")
PERSONNEL_STATS_FILENAME_RE = re.compile(r"shift_personnel_(\d{4}-\d{2}-\d{2})\.json")

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
    "rolling_year": 365,
}


def shift_start(now: datetime.datetime) -> datetime.datetime:
    base = now.replace(hour=SHIFT_HOUR, minute=0, second=0, microsecond=0)
    return base if now >= base else base - datetime.timedelta(days=1)


def load_stats(fp: Path):
    try:
        with fp.open("r", encoding="utf-8") as f:
            j = json.load(f)
            duration_known = j.get("duration_known_calls")
            return (
                defaultdict(int, j.get("calls", {})),
                defaultdict(int, j.get("dur_sec", {})),
                defaultdict(int, j.get("after_0000", {})),
                defaultdict(int, j.get("max_sec", {})),
                defaultdict(int, j.get("ride_in_count", {})),
                (defaultdict(int, duration_known) if isinstance(duration_known, dict) else None),
            )
    except Exception:
        return defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), None


def load_personnel_stats(fp: Path):
    try:
        with fp.open("r", encoding="utf-8") as f:
            j = json.load(f)
            return (
                dict(j.get("names", {})),
                defaultdict(int, j.get("calls", {})),
                defaultdict(int, j.get("dur_sec", {})),
                defaultdict(int, j.get("after_0000", {})),
                defaultdict(int, j.get("max_sec", {})),
                defaultdict(int, j.get("ride_in_count", {})),
            )
    except Exception:
        return {}, defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)


def _duration_denominator(unit: str, calls: dict, duration_known_calls: dict | None) -> int:
    if duration_known_calls is None:
        return int(calls.get(unit, 0) or 0)
    return int(duration_known_calls.get(unit, 0) or 0)


def _avg_minutes(total_seconds: int | float, known_calls: int) -> float | None:
    if known_calls <= 0:
        return None
    return round((float(total_seconds) / float(known_calls)) / 60.0, 1)


def _max_minutes(max_seconds: int | float, known_calls: int) -> float | None:
    if known_calls <= 0:
        return None
    return round(float(max_seconds) / 60.0, 1)


def _safe_parent(path: Path, idx: int):
    try:
        return path.parents[idx]
    except IndexError:
        return None


def _default_roster_dir(stats_dir: Path | None = None):
    env_dir = os.environ.get("ROSTER_DIR", "").strip()
    if env_dir:
        return Path(env_dir)
    try:
        here = Path(__file__).resolve()
    except Exception:
        here = Path.cwd()
    candidates: list[Path] = []
    if _safe_parent(here, 2):
        candidates.append(_safe_parent(here, 2) / "TSlogs")
    candidates.append(Path.cwd() / "TSlogs")
    candidates.append(Path.cwd().parent / "TSlogs")
    for parent in here.parents:
        candidates.append(parent / "TSlogs")
    if stats_dir:
        try:
            resolved_stats = stats_dir.resolve()
        except Exception:
            resolved_stats = stats_dir
        for parent in [resolved_stats] + list(resolved_stats.parents):
            candidates.append(parent / "TSlogs")
    for c in candidates:
        if c and c.exists():
            return c
    return None


def _default_personnel_stats_dir(stats_dir: Path):
    env_dir = os.environ.get("PERSONNEL_STATS_DIR", "").strip()
    if env_dir:
        return Path(env_dir)
    try:
        if stats_dir.name == "shift_stats":
            return stats_dir.parent / "shift_personnel"
    except Exception:
        pass
    return Path("shift_personnel")


def _pick_latest_roster(roster_dir: Path | None):
    if not roster_dir or not roster_dir.exists():
        return None
    candidates = sorted(
        roster_dir.glob("roster_units_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _normalize_unit_code(code: str) -> str:
    cleaned = (code or "").strip().upper()
    if cleaned.startswith("DC") and cleaned[2:].isdigit():
        return f"D{cleaned[2:]}"
    match = re.match(r"^TR(\d+)$", cleaned)
    if match:
        return f"T{match.group(1)}"
    return cleaned


def _normalize_roster_payload(payload: dict, source: Path):
    if "date" not in payload:
        m = ROSTER_FILENAME_RE.match(source.name)
        if m:
            payload["date"] = m.group(1)
    payload["generated_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    units = payload.get("units", [])
    for unit in units:
        unit_code = _normalize_unit_code(unit.get("unit_code", ""))
        if unit_code:
            unit["unit_code"] = unit_code
    payload["units"] = units
    return payload


def _roster_path_for_date(roster_dir: Path, target_date: datetime.date) -> Path:
    return roster_dir / f"roster_units_{target_date:%Y-%m-%d}.json"


def _load_roster_payload(roster_path: Path):
    if not roster_path.exists():
        return None
    try:
        payload = json.loads(roster_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return _normalize_roster_payload(payload, roster_path)


def _build_roster_bundle(roster_dir: Path):
    now = datetime.datetime.now()
    shift_date = shift_start(now).date()
    prior_date = shift_date - datetime.timedelta(days=1)

    today_payload = _load_roster_payload(_roster_path_for_date(roster_dir, shift_date))
    prior_payload = _load_roster_payload(_roster_path_for_date(roster_dir, prior_date))

    if not today_payload and not prior_payload:
        latest = _pick_latest_roster(roster_dir)
        if latest:
            today_payload = _load_roster_payload(latest)

    if not today_payload and not prior_payload:
        return None

    return {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "today": today_payload,
        "prior": prior_payload,
    }


def _maybe_write_roster_units(roster_dir: Path | None, out_path: Path):
    if not roster_dir or not roster_dir.exists():
        return
    payload = _build_roster_bundle(roster_dir)
    if not payload:
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    tmp.replace(out_path)


SHIFT_SECONDS = 24 * 3600


def _round_half_up(value: float) -> int:
    return int(math.floor(value + 0.5))


def _person_key(entry: dict) -> str | None:
    pid = str(entry.get("id") or "").strip()
    if pid:
        return pid
    name = str(entry.get("name") or "").strip()
    return name or None


def _parse_time_to_minutes(value) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.match(
        r"^(\d{1,2}):(\d{2})(?::\d{2})?\s*([AP]M)?$",
        text,
        re.IGNORECASE,
    )
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


def _parse_hours(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _entry_interval(
    entry: dict,
    shift_date: datetime.date,
    shift_start_dt: datetime.datetime,
) -> tuple[datetime.datetime, datetime.datetime] | None:
    start_min = _parse_time_to_minutes(entry.get("from"))
    end_min = _parse_time_to_minutes(entry.get("through"))
    hours = _parse_hours(entry.get("hours"))
    if start_min is None or end_min is None:
        if hours is None:
            return None
        start_dt = shift_start_dt
        end_dt = start_dt + datetime.timedelta(hours=hours)
        return start_dt, end_dt
    start_dt = datetime.datetime.combine(
        shift_date, datetime.time(start_min // 60, start_min % 60)
    )
    end_dt = datetime.datetime.combine(
        shift_date, datetime.time(end_min // 60, end_min % 60)
    )
    if start_min == end_min and hours is not None:
        end_dt = start_dt + datetime.timedelta(hours=hours)
    elif end_dt <= start_dt:
        end_dt += datetime.timedelta(days=1)
    return start_dt, end_dt


def _overlap_seconds(
    start: datetime.datetime,
    end: datetime.datetime,
    win_start: datetime.datetime,
    win_end: datetime.datetime,
) -> int:
    if end <= win_start or start >= win_end:
        return 0
    left = max(start, win_start)
    right = min(end, win_end)
    return max(0, int((right - left).total_seconds()))


def _build_roster_map(roster_payload: dict, shift_date: datetime.date) -> dict:
    shift_start_dt = datetime.datetime.combine(shift_date, datetime.time(SHIFT_HOUR, 0))
    shift_end_dt = shift_start_dt + datetime.timedelta(hours=24)
    after_start = datetime.datetime.combine(
        shift_date + datetime.timedelta(days=1), datetime.time(0, 0)
    )
    after_end = shift_end_dt
    by_unit: dict[str, dict[str, dict]] = {}
    for unit in roster_payload.get("units", []) or []:
        unit_code = _normalize_unit_code(unit.get("unit_code"))
        if not unit_code:
            continue
        for entry in unit.get("entries", []) or []:
            pkey = _person_key(entry)
            if not pkey:
                continue
            interval = _entry_interval(entry, shift_date, shift_start_dt)
            if not interval:
                continue
            total_sec = _overlap_seconds(interval[0], interval[1], shift_start_dt, shift_end_dt)
            after_sec = _overlap_seconds(interval[0], interval[1], after_start, after_end)
            if total_sec <= 0 and after_sec <= 0:
                continue
            name = str(entry.get("name") or "").strip()
            unit_map = by_unit.setdefault(unit_code, {})
            info = unit_map.setdefault(pkey, {"name": name, "total_sec": 0, "after_sec": 0})
            if name and not info.get("name"):
                info["name"] = name
            info["total_sec"] += total_sec
            info["after_sec"] += after_sec
    return by_unit


def _compute_shift_personnel_from_roster(
    shift_date: datetime.date,
    stats_path: Path,
    roster_payload: dict,
) -> tuple[dict, dict, dict, dict, dict]:
    calls, dur, after, max_sec, _ride_in, _duration_known = load_stats(stats_path)
    roster_map = _build_roster_map(roster_payload, shift_date)

    names: dict[str, str] = {}
    p_calls: dict[str, int] = defaultdict(int)
    p_dur: dict[str, int] = defaultdict(int)
    p_after: dict[str, int] = defaultdict(int)
    p_max: dict[str, int] = defaultdict(int)

    units = set(calls) | set(dur) | set(after) | set(max_sec)
    after_window_seconds = int((datetime.timedelta(hours=SHIFT_HOUR)).total_seconds()) or 1

    for raw_unit in units:
        unit_calls = int(calls.get(raw_unit, 0) or 0)
        unit_dur = float(dur.get(raw_unit, 0) or 0)
        unit_after = int(after.get(raw_unit, 0) or 0)
        unit_max = int(max_sec.get(raw_unit, 0) or 0)
        if not (unit_calls or unit_dur or unit_after or unit_max):
            continue
        unit_code = _normalize_unit_code(raw_unit)
        crew = roster_map.get(unit_code)
        if not crew:
            continue
        for pkey, info in crew.items():
            total_sec = float(info.get("total_sec", 0) or 0)
            after_sec = float(info.get("after_sec", 0) or 0)
            if total_sec <= 0:
                continue
            f_total = min(max(total_sec / SHIFT_SECONDS, 0.0), 1.0)
            f_after = min(max(after_sec / after_window_seconds, 0.0), 1.0)
            calls_est = _round_half_up(unit_calls * f_total)
            after_est = _round_half_up(unit_after * f_after)
            if after_est > calls_est:
                calls_est = after_est
            dur_est = _round_half_up(unit_dur * f_total)
            max_est = (
                _round_half_up(unit_max * min(1.0, f_total)) if calls_est > 0 else 0
            )

            p_calls[pkey] += calls_est
            p_dur[pkey] += dur_est
            p_after[pkey] += after_est
            if max_est > p_max[pkey]:
                p_max[pkey] = max_est

            name = str(info.get("name") or "").strip()
            if name and pkey not in names:
                names[pkey] = name

    return dict(names), dict(p_calls), dict(p_dur), dict(p_after), dict(p_max)


def aggregate_personnel_timeframe_stats_hybrid(
    shift_stats_dir: Path,
    roster_dir: Path | None,
    personnel_dir: Path | None,
    period_key: str,
    now: datetime.datetime | None = None,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
):
    now = now or datetime.datetime.now()
    cutoff_date = None
    if not start_date and not end_date and period_key != "alltime":
        shift_date = shift_start(now).date()
        days = TIMEFRAME_LENGTHS[period_key]
        cutoff_date = shift_date - datetime.timedelta(days=days - 1)

    names: dict[str, str] = {}
    calls: dict[str, int] = defaultdict(int)
    dur: dict[str, int] = defaultdict(int)
    after_midnight: dict[str, int] = defaultdict(int)
    max_sec: dict[str, int] = defaultdict(int)
    ride_in: dict[str, int] = defaultdict(int)
    single_shift_max_calls: dict[str, int] = defaultdict(int)
    single_shift_max_after: dict[str, int] = defaultdict(int)

    if not shift_stats_dir.exists():
        return (
            dict(names),
            dict(calls),
            dict(dur),
            dict(after_midnight),
            dict(max_sec),
            dict(ride_in),
            dict(single_shift_max_calls),
            dict(single_shift_max_after),
        )

    for name in os.listdir(shift_stats_dir):
        m = STATS_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            file_date = datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            continue
        if start_date and file_date < start_date:
            continue
        if end_date and file_date > end_date:
            continue
        if cutoff_date and file_date < cutoff_date:
            continue

        stats_path = shift_stats_dir / name
        shift_names: dict[str, str] = {}
        shift_calls: dict[str, int] = {}
        shift_dur: dict[str, int] = {}
        shift_after: dict[str, int] = {}
        shift_max: dict[str, int] = {}
        shift_ride_in: dict[str, int] = {}

        roster_payload = None
        personnel_path = None
        if personnel_dir and personnel_dir.exists():
            personnel_path = personnel_dir / f"shift_personnel_{file_date:%Y-%m-%d}.json"
        if roster_dir and roster_dir.exists():
            roster_payload = _load_roster_payload(_roster_path_for_date(roster_dir, file_date))

        if roster_payload:
            shift_names, shift_calls, shift_dur, shift_after, shift_max = _compute_shift_personnel_from_roster(
                file_date,
                stats_path,
                roster_payload,
            )
            if personnel_path and personnel_path.exists():
                (
                    file_names,
                    _file_calls,
                    _file_dur,
                    _file_after,
                    _file_max,
                    file_ride_in,
                ) = load_personnel_stats(personnel_path)
                for pid, nm in file_names.items():
                    if nm and pid not in shift_names:
                        shift_names[pid] = nm
                shift_ride_in = {k: int(v) for k, v in file_ride_in.items()}
        elif personnel_dir and personnel_dir.exists():
            if personnel_path.exists():
                (
                    shift_names,
                    shift_calls_dd,
                    shift_dur_dd,
                    shift_after_dd,
                    shift_max_dd,
                    shift_ride_in_dd,
                ) = load_personnel_stats(personnel_path)
                shift_calls = {k: int(v) for k, v in shift_calls_dd.items()}
                shift_dur = {k: int(v) for k, v in shift_dur_dd.items()}
                shift_after = {k: int(v) for k, v in shift_after_dd.items()}
                shift_max = {k: int(v) for k, v in shift_max_dd.items()}
                shift_ride_in = {k: int(v) for k, v in shift_ride_in_dd.items()}

        for pid, nm in shift_names.items():
            if nm and pid not in names:
                names[pid] = nm
        for pid, count in shift_calls.items():
            c_int = int(count)
            calls[pid] += c_int
            if c_int > single_shift_max_calls[pid]:
                single_shift_max_calls[pid] = c_int
        for pid, seconds in shift_dur.items():
            dur[pid] += int(seconds)
        for pid, count in shift_after.items():
            c_int = int(count)
            after_midnight[pid] += c_int
            if c_int > single_shift_max_after[pid]:
                single_shift_max_after[pid] = c_int
        for pid, mx in shift_max.items():
            if int(mx) > max_sec[pid]:
                max_sec[pid] = int(mx)
        for pid, count in shift_ride_in.items():
            ride_in[pid] += int(count)

    return (
        dict(names),
        dict(calls),
        dict(dur),
        dict(after_midnight),
        dict(max_sec),
        dict(ride_in),
        dict(single_shift_max_calls),
        dict(single_shift_max_after),
    )


def aggregate_personnel_timeframe_stats(stats_dir: Path, period_key: str, now: datetime.datetime | None = None):
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
    ride_in = defaultdict(int)
    single_shift_max_calls = defaultdict(int)
    single_shift_max_after = defaultdict(int)
    names = {}
    if not stats_dir.exists():
        return dict(names), dict(calls), dict(dur), dict(after_midnight), dict(max_sec), dict(ride_in), dict(single_shift_max_calls), dict(single_shift_max_after)
    for name in os.listdir(stats_dir):
        m = PERSONNEL_STATS_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            file_date = datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            continue
        if cutoff_date and file_date < cutoff_date:
            continue
        file_names, file_calls, file_dur, file_after, file_max, file_ride_in = load_personnel_stats(stats_dir / name)
        for pid, nm in file_names.items():
            if nm and pid not in names:
                names[pid] = nm
        for pid, count in file_calls.items():
            c_int = int(count)
            calls[pid] += c_int
            if c_int > single_shift_max_calls[pid]:
                single_shift_max_calls[pid] = c_int
        for pid, seconds in file_dur.items():
            dur[pid] += int(seconds)
        for pid, count in file_after.items():
            c_int = int(count)
            after_midnight[pid] += c_int
            if c_int > single_shift_max_after[pid]:
                single_shift_max_after[pid] = c_int
        for pid, mx in file_max.items():
            if int(mx) > max_sec[pid]:
                max_sec[pid] = int(mx)
        for pid, count in file_ride_in.items():
            ride_in[pid] += int(count)
    return dict(names), dict(calls), dict(dur), dict(after_midnight), dict(max_sec), dict(ride_in), dict(single_shift_max_calls), dict(single_shift_max_after)


def compute_personnel_period(stats_dir: Path, period_key: str, now: datetime.datetime):
    names, calls, dur, after, max_sec, ride_in, max_calls, max_after = aggregate_personnel_timeframe_stats(stats_dir, period_key, now)
    rows = []
    for pid, count in calls.items():
        c = int(count)
        total_sec = int(dur.get(pid, 0))
        avg_min = (total_sec / c) / 60.0 if c else 0.0
        max_min = int(max_sec.get(pid, 0)) / 60.0 if max_sec.get(pid, 0) else 0.0
        rows.append({
            "person_id": pid,
            "name": names.get(pid, pid),
            "total_calls": c,
            "ride_in_count": int(ride_in.get(pid, 0)),
            "single_shift_max_calls": int(max_calls.get(pid, 0)),
            "avg_call_duration_mins": round(avg_min, 1),
            "highest_call_duration_mins": round(max_min, 1),
            "total_calls_after_midnight": int(after.get(pid, 0)),
            "single_shift_max_after_midnight": int(max_after.get(pid, 0)),
            "total_call_hours": round(total_sec / 3600.0, 1) if total_sec else 0.0,
        })
    rows.sort(key=lambda r: (-r.get("total_calls", 0), r.get("name", "")))
    meta = {}
    if period_key in ("week", "month", "year", "rolling_year"):
        end_date = shift_start(now).date()
        start_date, end_date = _date_range_for_period(now, period_key)
        if period_key in ("year", "rolling_year"):
            meta["range"] = _format_range(start_date, end_date)
        else:
            meta["range"] = f"{start_date:%b %d} - {end_date:%b %d}"
    return {
        "label": {"week": "Week", "month": "Month", "year": "Year to Date", "rolling_year": "Last 365 Days"}.get(period_key, period_key.title()),
        "period": period_key,
        "rows": rows,
        "meta": meta,
        "updated": datetime.datetime.utcnow().isoformat() + "Z",
    }


def compute_personnel_period_hybrid(
    shift_stats_dir: Path,
    roster_dir: Path | None,
    personnel_dir: Path | None,
    period_key: str,
    now: datetime.datetime,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    delta_map: dict[str, int] | None = None,
):
    names, calls, dur, after, max_sec, ride_in, max_calls, max_after = aggregate_personnel_timeframe_stats_hybrid(
        shift_stats_dir=shift_stats_dir,
        roster_dir=roster_dir,
        personnel_dir=personnel_dir,
        period_key=period_key,
        now=now,
        start_date=start_date,
        end_date=end_date,
    )
    rows = []
    for pid, count in calls.items():
        c = int(count)
        total_sec = int(dur.get(pid, 0))
        avg_min = (total_sec / c) / 60.0 if c else 0.0
        max_min = int(max_sec.get(pid, 0)) / 60.0 if max_sec.get(pid, 0) else 0.0
        rows.append(
            {
                "person_id": pid,
                "name": names.get(pid, pid),
                "total_calls": c,
                "ride_in_count": int(ride_in.get(pid, 0)),
                "single_shift_max_calls": int(max_calls.get(pid, 0)),
                "avg_call_duration_mins": round(avg_min, 1),
                "highest_call_duration_mins": round(max_min, 1),
                "total_calls_after_midnight": int(after.get(pid, 0)),
                "single_shift_max_after_midnight": int(max_after.get(pid, 0)),
                "total_call_hours": round(total_sec / 3600.0, 1) if total_sec else 0.0,
                "delta_calls": int((delta_map or {}).get(pid, 0)),
            }
        )
    rows.sort(key=lambda r: (-r.get("total_calls", 0), r.get("name", "")))
    meta = {}
    partial = False
    if period_key in ("week", "month", "year", "rolling_year"):
        if start_date and end_date:
            start_val = start_date
            end_val = end_date
        else:
            start_val, end_val = _date_range_for_period(now, period_key)
        if period_key in ("year", "rolling_year"):
            meta["range"] = _format_range(start_val, end_val)
        else:
            meta["range"] = f"{start_val:%b %d} - {end_val:%b %d}"
    available_start = _available_personnel_start_date(roster_dir, personnel_dir)
    available_end = _available_personnel_end_date(roster_dir, personnel_dir)
    if available_start and start_date and start_date < available_start:
        partial = True
        meta["available_start"] = available_start.isoformat()
    if available_end and end_date and end_date > available_end:
        partial = True
        meta["available_end"] = available_end.isoformat()
    if partial:
        meta["partial"] = True
    return {
        "label": {"week": "Week", "month": "Month", "year": "Year to Date", "rolling_year": "Last 365 Days"}.get(period_key, period_key.title()),
        "period": period_key,
        "rows": rows,
        "meta": meta,
        "updated": datetime.datetime.utcnow().isoformat() + "Z",
    }


def aggregate_timeframe_stats(
    stats_dir: Path,
    period_key: str,
    now: datetime.datetime | None = None,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
):
    now = now or datetime.datetime.now()
    cutoff_date = None
    if not start_date and not end_date and period_key != "alltime":
        shift_date = shift_start(now).date()
        days = TIMEFRAME_LENGTHS[period_key]
        cutoff_date = shift_date - datetime.timedelta(days=days - 1)
    calls = defaultdict(int)
    dur = defaultdict(int)
    after_midnight = defaultdict(int)
    max_sec = defaultdict(int)
    ride_in = defaultdict(int)
    duration_known_calls = defaultdict(int)
    if not stats_dir.exists():
        return dict(calls), dict(dur), dict(after_midnight), dict(max_sec), dict(ride_in), dict(duration_known_calls)
    for name in os.listdir(stats_dir):
        m = STATS_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            file_date = datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            continue
        if start_date and file_date < start_date:
            continue
        if end_date and file_date > end_date:
            continue
        if cutoff_date and file_date < cutoff_date:
            continue
        file_calls, file_dur, file_after, file_max, file_ride_in, file_duration_known = load_stats(stats_dir / name)
        for unit, count in file_calls.items():
            if unit in WATCH_SET:
                calls[unit] += int(count)
                duration_known_calls[unit] += _duration_denominator(unit, file_calls, file_duration_known)
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
        for unit, count in file_ride_in.items():
            if unit in WATCH_SET:
                ride_in[unit] += int(count)
    return dict(calls), dict(dur), dict(after_midnight), dict(max_sec), dict(ride_in), dict(duration_known_calls)


def _date_range_for_period(now: datetime.datetime, period_key: str) -> tuple[datetime.date, datetime.date]:
    end_date = shift_start(now).date()
    if period_key == "year":
        return datetime.date(end_date.year, 1, 1), end_date
    if period_key == "rolling_year":
        return end_date - datetime.timedelta(days=364), end_date
    days = TIMEFRAME_LENGTHS[period_key]
    start_date = end_date - datetime.timedelta(days=days - 1)
    return start_date, end_date


def _format_range(start_date: datetime.date, end_date: datetime.date) -> str:
    return f"{start_date:%Y-%m-%d} - {end_date:%Y-%m-%d}"


def _list_shift_stat_dates(stats_dir: Path) -> list[datetime.date]:
    dates: list[datetime.date] = []
    if not stats_dir.exists():
        return dates
    for name in os.listdir(stats_dir):
        m = STATS_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            dates.append(datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date())
        except Exception:
            continue
    return sorted(dates)


def _list_roster_dates(roster_dir: Path | None) -> list[datetime.date]:
    dates: list[datetime.date] = []
    if not roster_dir or not roster_dir.exists():
        return dates
    for name in os.listdir(roster_dir):
        m = ROSTER_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            dates.append(datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date())
        except Exception:
            continue
    return sorted(dates)


def _list_personnel_dates(personnel_dir: Path | None) -> list[datetime.date]:
    dates: list[datetime.date] = []
    if not personnel_dir or not personnel_dir.exists():
        return dates
    for name in os.listdir(personnel_dir):
        m = PERSONNEL_STATS_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            dates.append(datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date())
        except Exception:
            continue
    return sorted(dates)


def _sum_unit_calls_for_date(stats_dir: Path, day: datetime.date) -> int:
    stats_path = stats_dir / f"shift_stats_{day:%Y-%m-%d}.json"
    calls, _, _, _, _ride_in, _duration_known = load_stats(stats_path)
    total = 0
    for unit, count in calls.items():
        if unit in WATCH_SET:
            total += int(count)
    return int(total)


def _sum_personnel_calls_for_date(personnel_dir: Path | None, day: datetime.date) -> int:
    if not personnel_dir or not personnel_dir.exists():
        return 0
    fp = personnel_dir / f"shift_personnel_{day:%Y-%m-%d}.json"
    if not fp.exists():
        return 0
    _, calls, _, _, _, _ride_in = load_personnel_stats(fp)
    total = 0
    for v in calls.values():
        total += int(v)
    return int(total)


def _load_json_file(path: Path | None):
    if not path or not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _first_call_date(stats_dir: Path) -> datetime.date | None:
    for date_obj in _list_shift_stat_dates(stats_dir):
        stats_path = stats_dir / f"shift_stats_{date_obj:%Y-%m-%d}.json"
        calls, _, _, _, _ride_in, _duration_known = load_stats(stats_path)
        total_calls = 0
        for unit, count in calls.items():
            if unit in WATCH_SET:
                total_calls += int(count)
        if total_calls > 0:
            return date_obj
    return None


def _yearly_cycle_bounds(
    anchor_date: datetime.date,
    current_date: datetime.date,
    cycle_days: int = 365,
) -> tuple[datetime.date, datetime.date, datetime.date]:
    if current_date < anchor_date:
        cycle_start = anchor_date
    else:
        elapsed_days = (current_date - anchor_date).days
        cycle_index = elapsed_days // cycle_days
        cycle_start = anchor_date + datetime.timedelta(days=(cycle_index * cycle_days))
    next_reset = cycle_start + datetime.timedelta(days=cycle_days)
    cycle_end = min(current_date, next_reset - datetime.timedelta(days=1))
    if cycle_end < cycle_start:
        cycle_end = cycle_start
    return cycle_start, cycle_end, next_reset


def _resolve_yearly_cycle(
    stats_dir: Path,
    now: datetime.datetime,
) -> tuple[datetime.date, datetime.date, datetime.date]:
    end_date = shift_start(now).date()
    start_date = datetime.date(end_date.year, 1, 1)
    next_reset = datetime.date(end_date.year + 1, 1, 1)
    return start_date, end_date, next_reset


def _available_personnel_start_date(
    roster_dir: Path | None,
    personnel_dir: Path | None,
) -> datetime.date | None:
    candidates: list[datetime.date] = []
    roster_dates = _list_roster_dates(roster_dir)
    personnel_dates = _list_personnel_dates(personnel_dir)
    if roster_dates:
        candidates.append(roster_dates[0])
    if personnel_dates:
        candidates.append(personnel_dates[0])
    return min(candidates) if candidates else None


def _latest_personnel_nonzero_date(personnel_dir: Path | None) -> datetime.date | None:
    if not personnel_dir or not personnel_dir.exists():
        return None
    latest: datetime.date | None = None
    for day in _list_personnel_dates(personnel_dir):
        if _sum_personnel_calls_for_date(personnel_dir, day) > 0:
            latest = day
    return latest


def _available_personnel_end_date(
    roster_dir: Path | None,
    personnel_dir: Path | None,
) -> datetime.date | None:
    candidates: list[datetime.date] = []
    roster_dates = _list_roster_dates(roster_dir)
    latest_nonzero_personnel = _latest_personnel_nonzero_date(personnel_dir)
    if roster_dates:
        candidates.append(roster_dates[-1])
    if latest_nonzero_personnel:
        candidates.append(latest_nonzero_personnel)
    return max(candidates) if candidates else None


def compute_yearly_summary(
    stats_dir: Path,
    roster_dir: Path | None,
    personnel_dir: Path | None,
    now: datetime.datetime,
    cycle_bounds: tuple[datetime.date, datetime.date, datetime.date] | None = None,
):
    start_date, end_date, next_reset = cycle_bounds or _resolve_yearly_cycle(stats_dir, now)
    unit_calls, _, _, _, unit_ride_in, _unit_known = aggregate_timeframe_stats(
        stats_dir,
        "year",
        now,
        start_date=start_date,
        end_date=end_date,
    )
    units_total_calls = int(sum(int(v) for v in unit_calls.values()))
    units_total_ride_ins = int(sum(int(v) for v in unit_ride_in.values()))
    personnel_available_start = _available_personnel_start_date(roster_dir, personnel_dir)
    personnel_available_end = _available_personnel_end_date(roster_dir, personnel_dir)
    personnel_partial = (
        (personnel_available_start is not None and start_date < personnel_available_start)
        or (personnel_available_end is not None and end_date > personnel_available_end)
        or personnel_available_end is None
    )
    personnel_total_calls = None
    personnel_total_ride_ins = None
    if not personnel_partial:
        (
            _names,
            personnel_calls,
            _dur,
            _after,
            _max,
            personnel_ride_in,
            _max_calls,
            _max_after,
        ) = aggregate_personnel_timeframe_stats_hybrid(
            shift_stats_dir=stats_dir,
            roster_dir=roster_dir,
            personnel_dir=personnel_dir,
            period_key="year",
            now=now,
            start_date=start_date,
            end_date=end_date,
        )
        personnel_total_calls = int(sum(int(v) for v in personnel_calls.values()))
        personnel_total_ride_ins = int(sum(int(v) for v in personnel_ride_in.values()))

    payload = {
        "label": "Year to Date",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "next_reset_date": next_reset.isoformat(),
        "units_total_calls": int(units_total_calls),
        "personnel_total_calls": personnel_total_calls,
        "units_total_ride_ins": int(units_total_ride_ins),
        "personnel_total_ride_ins": personnel_total_ride_ins,
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
    }
    if personnel_partial:
        payload["personnel_partial"] = True
    if personnel_available_start:
        payload["personnel_available_start"] = personnel_available_start.isoformat()
    if personnel_available_end:
        payload["personnel_available_end"] = personnel_available_end.isoformat()
    return payload


def compute_shift_breakdown(
    stats_dir: Path,
    period_key: str,
    now: datetime.datetime,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    delta_map: dict | None = None,
) -> list[dict]:
    if start_date is None or end_date is None:
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
    sum_ride_in: dict[str, dict[str, int]] = defaultdict(lambda: {l: 0 for l in letters})

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
        file_calls, file_dur, file_after, file_max, file_ride_in, file_duration_known = load_stats(stats_dir / name)
        for unit in (set(file_calls) | set(file_dur) | set(file_after) | set(file_max) | set(file_ride_in)):
            if unit not in WATCH_SET:
                continue
            c = int(file_calls.get(unit, 0))
            s = int(file_dur.get(unit, 0))
            a = int(file_after.get(unit, 0))
            mx = int(file_max.get(unit, 0))
            ri = int(file_ride_in.get(unit, 0))
            known_calls = _duration_denominator(unit, file_calls, file_duration_known)
            sum_calls[unit][letter] += c
            if c > max_calls[unit][letter]:
                max_calls[unit][letter] = c
            sum_dur[unit][letter] += s
            sum_dur_calls[unit][letter] += known_calls
            if mx > max_dur[unit][letter]:
                max_dur[unit][letter] = mx
            sum_after[unit][letter] += a
            if a > max_after[unit][letter]:
                max_after[unit][letter] = a
            sum_ride_in[unit][letter] += ri

    rows = []
    units = sorted(set(list(sum_calls.keys()) + list(sum_dur.keys()) + list(sum_after.keys()) + list(sum_ride_in.keys())))
    for unit in units:
        calls_abc = [int(sum_calls[unit][l]) for l in letters]
        calls_max_abc = [int(max_calls[unit][l]) for l in letters]
        avg_min_abc = [_avg_minutes(sum_dur[unit][l], sum_dur_calls[unit][l]) for l in letters]
        max_min_abc = [_max_minutes(max_dur[unit][l], sum_dur_calls[unit][l]) for l in letters]
        after_abc = [int(sum_after[unit][l]) for l in letters]
        after_max_abc = [int(max_after[unit][l]) for l in letters]
        ride_in_abc = [int(sum_ride_in[unit][l]) for l in letters]
        rows.append({
            "unit": unit,
            "calls_abc": calls_abc,
            "calls_max_abc": calls_max_abc,
            "avg_min_abc": avg_min_abc,
            "max_min_abc": max_min_abc,
            "after_abc": after_abc,
            "after_max_abc": after_max_abc,
            "ride_in_abc": ride_in_abc,
            "total_calls": sum(calls_abc),
            "total_after": sum(after_abc),
            "total_ride_ins": sum(ride_in_abc),
            "delta_calls": int((delta_map or {}).get(unit, 0)),
        })
    # Sort by total calls desc then unit
    rows.sort(key=lambda r: (-r.get("total_calls", 0), r["unit"]))
    return rows


def _rows_from(
    calls: dict,
    dur: dict,
    after_midnight: dict,
    max_sec: dict | None = None,
    ride_in: dict | None = None,
    duration_known_calls: dict | None = None,
    delta_map: dict | None = None,
):
    rows = []
    for unit, count in sorted(calls.items(), key=lambda kv: (-kv[1], kv[0])):
        known_calls = _duration_denominator(unit, calls, duration_known_calls)
        avg_min = _avg_minutes(dur.get(unit, 0), known_calls)
        r = {
            "unit": unit,
            "calls": int(count),
            "ride_in_count": int((ride_in or {}).get(unit, 0)),
            "avg_min": avg_min,
            "after_0000": int(after_midnight.get(unit, 0)),
            "delta_calls": int((delta_map or {}).get(unit, 0)),
        }
        if max_sec is not None:
            r["max_min"] = _max_minutes(int(max_sec.get(unit, 0)), known_calls)
        rows.append(r)
    return rows


def format_leaderboard_body(
    label: str,
    period_key: str,
    calls: dict,
    dur: dict,
    after_midnight: dict,
    now: datetime.datetime,
    duration_known_calls: dict | None = None,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> str:
    shift_date = shift_start(now).date()
    if start_date and end_date:
        header = f"{label} runs {start_date:%d %b %Y} - {end_date:%d %b %Y}"
    elif period_key == 'alltime':
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
        for r in _rows_from(calls, dur, after_midnight, duration_known_calls=duration_known_calls):
            avg_text = f"{r['avg_min']:.1f}" if r.get("avg_min") is not None else "--"
            lines.append(f"{r['unit']}: {r['calls']}  |  avg {avg_text} min  |  after 00:00: {r['after_0000']}")
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


def _normalize_period_bounds(
    now: datetime.datetime,
    period_key: str,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> tuple[datetime.date, datetime.date]:
    if start_date and end_date:
        return start_date, end_date
    if period_key == "day":
        day = shift_start(now).date()
        return day, day
    if period_key not in TIMEFRAME_LENGTHS:
        day = shift_start(now).date()
        return day, day
    return _date_range_for_period(now, period_key)


def _previous_bounds(start_date: datetime.date, end_date: datetime.date) -> tuple[datetime.date, datetime.date]:
    length_days = (end_date - start_date).days + 1
    prev_end = start_date - datetime.timedelta(days=1)
    prev_start = prev_end - datetime.timedelta(days=length_days - 1)
    return prev_start, prev_end


def _unit_delta_map(
    stats_dir: Path,
    now: datetime.datetime,
    period_key: str,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> dict[str, int]:
    cur_start, cur_end = _normalize_period_bounds(now, period_key, start_date, end_date)
    prev_start, prev_end = _previous_bounds(cur_start, cur_end)

    cur_calls, _, _, _, _cur_ride_in, _cur_known = aggregate_timeframe_stats(
        stats_dir,
        period_key,
        now,
        start_date=cur_start,
        end_date=cur_end,
    )
    prev_calls, _, _, _, _prev_ride_in, _prev_known = aggregate_timeframe_stats(
        stats_dir,
        period_key,
        now,
        start_date=prev_start,
        end_date=prev_end,
    )
    units = set(cur_calls.keys()) | set(prev_calls.keys())
    return {u: int(cur_calls.get(u, 0)) - int(prev_calls.get(u, 0)) for u in units}


def _personnel_delta_map(
    shift_stats_dir: Path,
    roster_dir: Path | None,
    personnel_dir: Path | None,
    now: datetime.datetime,
    period_key: str,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> dict[str, int]:
    cur_start, cur_end = _normalize_period_bounds(now, period_key, start_date, end_date)
    prev_start, prev_end = _previous_bounds(cur_start, cur_end)

    (
        _cur_names,
        cur_calls,
        _cur_dur,
        _cur_after,
        _cur_max,
        _cur_ride_in,
        _cur_max_calls,
        _cur_max_after,
    ) = aggregate_personnel_timeframe_stats_hybrid(
        shift_stats_dir=shift_stats_dir,
        roster_dir=roster_dir,
        personnel_dir=personnel_dir,
        period_key=period_key,
        now=now,
        start_date=cur_start,
        end_date=cur_end,
    )
    (
        _prev_names,
        prev_calls,
        _prev_dur,
        _prev_after,
        _prev_max,
        _prev_ride_in,
        _prev_max_calls,
        _prev_max_after,
    ) = aggregate_personnel_timeframe_stats_hybrid(
        shift_stats_dir=shift_stats_dir,
        roster_dir=roster_dir,
        personnel_dir=personnel_dir,
        period_key=period_key,
        now=now,
        start_date=prev_start,
        end_date=prev_end,
    )
    people = set(cur_calls.keys()) | set(prev_calls.keys())
    return {pid: int(cur_calls.get(pid, 0)) - int(prev_calls.get(pid, 0)) for pid in people}


def _unit_shift_detail_map(
    stats_dir: Path,
    start_date: datetime.date,
    end_date: datetime.date,
) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    all_dates = _list_shift_stat_dates(stats_dir)
    for day in all_dates:
        if day < start_date or day > end_date:
            continue
        calls, dur, after, max_sec, ride_in, duration_known = load_stats(stats_dir / f"shift_stats_{day:%Y-%m-%d}.json")
        units = set(calls.keys()) | set(dur.keys()) | set(after.keys()) | set(max_sec.keys()) | set(ride_in.keys())
        for unit in units:
            if unit not in WATCH_SET:
                continue
            c = int(calls.get(unit, 0))
            s = int(dur.get(unit, 0))
            a = int(after.get(unit, 0))
            mx = int(max_sec.get(unit, 0))
            ri = int(ride_in.get(unit, 0))
            known_calls = _duration_denominator(unit, calls, duration_known)
            if c <= 0 and s <= 0 and a <= 0 and mx <= 0 and ri <= 0:
                continue
            out[unit].append(
                {
                    "date": day.isoformat(),
                    "shift": _shift_letter_for(day),
                    "calls": c,
                    "ride_in_count": ri,
                    "avg_min": _avg_minutes(s, known_calls),
                    "max_min": _max_minutes(mx, known_calls),
                    "after_0000": a,
                }
            )
    for unit in out:
        out[unit].sort(key=lambda r: r["date"], reverse=True)
    return dict(out)


def _load_personnel_for_date_hybrid(
    shift_stats_dir: Path,
    roster_dir: Path | None,
    personnel_dir: Path | None,
    day: datetime.date,
) -> tuple[dict, dict, dict, dict, dict, dict]:
    stats_path = shift_stats_dir / f"shift_stats_{day:%Y-%m-%d}.json"
    if not stats_path.exists():
        return {}, {}, {}, {}, {}, {}

    roster_payload = None
    personnel_path = None
    if personnel_dir and personnel_dir.exists():
        personnel_path = personnel_dir / f"shift_personnel_{day:%Y-%m-%d}.json"
    if roster_dir and roster_dir.exists():
        roster_payload = _load_roster_payload(_roster_path_for_date(roster_dir, day))
    if roster_payload:
        names, calls, dur, after, max_sec = _compute_shift_personnel_from_roster(day, stats_path, roster_payload)
        ride_in = {}
        if personnel_path and personnel_path.exists():
            file_names, _file_calls, _file_dur, _file_after, _file_max, file_ride_in = load_personnel_stats(personnel_path)
            for pid, nm in file_names.items():
                if nm and pid not in names:
                    names[pid] = nm
            ride_in = {k: int(v) for k, v in file_ride_in.items()}
        return names, calls, dur, after, max_sec, ride_in

    if personnel_dir and personnel_dir.exists():
        if personnel_path.exists():
            return load_personnel_stats(personnel_path)
    return {}, {}, {}, {}, {}, {}


def _personnel_shift_detail_map(
    shift_stats_dir: Path,
    roster_dir: Path | None,
    personnel_dir: Path | None,
    start_date: datetime.date,
    end_date: datetime.date,
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    all_dates = _list_shift_stat_dates(shift_stats_dir)
    for day in all_dates:
        if day < start_date or day > end_date:
            continue
        names, calls, dur, after, max_sec, ride_in = _load_personnel_for_date_hybrid(
            shift_stats_dir=shift_stats_dir,
            roster_dir=roster_dir,
            personnel_dir=personnel_dir,
            day=day,
        )
        people = set(calls.keys()) | set(dur.keys()) | set(after.keys()) | set(max_sec.keys()) | set(ride_in.keys())
        for pid in people:
            c = int(calls.get(pid, 0))
            s = int(dur.get(pid, 0))
            a = int(after.get(pid, 0))
            mx = int(max_sec.get(pid, 0))
            ri = int(ride_in.get(pid, 0))
            if c <= 0 and s <= 0 and a <= 0 and mx <= 0 and ri <= 0:
                continue
            rec = out.setdefault(
                str(pid),
                {"name": names.get(pid, str(pid)), "shifts": []},
            )
            if names.get(pid):
                rec["name"] = names.get(pid)
            rec["shifts"].append(
                {
                    "date": day.isoformat(),
                    "shift": _shift_letter_for(day),
                    "calls": c,
                    "ride_in_count": ri,
                    "avg_min": round((s / c) / 60.0, 1) if c else 0.0,
                    "max_min": round(mx / 60.0, 1) if mx else 0.0,
                    "after_0000": a,
                }
            )
    for pid in out:
        out[pid]["shifts"].sort(key=lambda r: r["date"], reverse=True)
    return out


def _period_total_calls(period_payload: dict) -> int:
    if not period_payload:
        return 0
    rows = period_payload.get("rows", []) or []
    period = period_payload.get("period")
    total = 0
    if period in ("week", "month", "year", "rolling_year"):
        for row in rows:
            total += int(row.get("total_calls", 0))
    else:
        for row in rows:
            total += int(row.get("calls", 0))
    return int(total)


def _personnel_total_calls(period_payload: dict) -> int:
    if not period_payload:
        return 0
    rows = period_payload.get("rows", []) or []
    total = 0
    for row in rows:
        total += int(row.get("total_calls", 0))
    return int(total)


def compute_integrity_checks(payload: dict) -> dict:
    checks = []
    severity = "ok"
    for period_key in ("week", "month", "year"):
        unit_total = _period_total_calls(payload.get(period_key) or {})
        personnel_payload = (payload.get("personnel") or {}).get(period_key) or {}
        personnel_total = _personnel_total_calls(personnel_payload)
        personnel_partial = bool(((personnel_payload.get("meta") or {}).get("partial")))
        ratio = (float(personnel_total) / float(unit_total)) if unit_total > 0 else None
        issues = []
        status = "ok"
        if personnel_partial:
            issues.append("personnel coverage partial")
        elif unit_total > 0 and personnel_total <= 0:
            status = "warn"
            issues.append("personnel totals missing")
        if (not personnel_partial) and unit_total > 0 and personnel_total < unit_total:
            status = "warn"
            issues.append("personnel calls below unit calls")
        if (not personnel_partial) and ratio is not None and ratio > 25.0:
            status = "warn"
            issues.append("personnel-to-unit ratio unusually high")
        if status == "warn":
            severity = "warn"
            print(
                f"WARN integrity[{period_key}]: unit={unit_total} personnel={personnel_total} ratio={ratio if ratio is not None else 'n/a'}",
                flush=True,
            )
        checks.append(
            {
                "period": period_key,
                "unit_total_calls": int(unit_total),
                "personnel_total_calls": int(personnel_total),
                "difference_calls": int(personnel_total - unit_total),
                "personnel_to_unit_ratio": round(ratio, 2) if ratio is not None else None,
                "personnel_partial": personnel_partial,
                "status": status,
                "issues": issues,
            }
        )
    return {
        "status": severity,
        "checks": checks,
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
    }


def compute_backfill_health(
    stats_dir: Path,
    roster_dir: Path | None,
    personnel_dir: Path | None,
    backfill_status_path: Path | None,
) -> dict:
    stats_dates = _list_shift_stat_dates(stats_dir)
    roster_dates = _list_roster_dates(roster_dir)
    personnel_dates = _list_personnel_dates(personnel_dir)

    latest_stats = stats_dates[-1] if stats_dates else None
    latest_roster = roster_dates[-1] if roster_dates else None
    latest_personnel = _available_personnel_end_date(roster_dir, personnel_dir)
    earliest_roster = roster_dates[0] if roster_dates else None
    earliest_personnel = _available_personnel_start_date(roster_dir, personnel_dir)

    missing_roster_days: list[str] = []
    missing_personnel_days: list[str] = []

    roster_set = set(roster_dates)
    for day in stats_dates:
        if earliest_roster and day < earliest_roster:
            continue
        if day not in roster_set:
            missing_roster_days.append(day.isoformat())

    for day in stats_dates:
        if earliest_personnel and day < earliest_personnel:
            continue
        unit_calls = _sum_unit_calls_for_date(stats_dir, day)
        if unit_calls <= 0:
            continue
        personnel_calls = _sum_personnel_calls_for_date(personnel_dir, day)
        if personnel_calls <= 0:
            missing_personnel_days.append(day.isoformat())

    roster_lag_days = None
    if latest_stats and latest_roster:
        roster_lag_days = max(0, (latest_stats - latest_roster).days)
    personnel_lag_days = None
    if latest_stats and latest_personnel:
        personnel_lag_days = max(0, (latest_stats - latest_personnel).days)

    backfill_status = _load_json_file(backfill_status_path)
    if not backfill_status:
        backfill_status = {
            "status": "unknown",
            "message": "No backfill status available.",
            "updated_at": None,
        }

    return {
        "latest_stats_date": latest_stats.isoformat() if latest_stats else None,
        "latest_roster_date": latest_roster.isoformat() if latest_roster else None,
        "latest_personnel_date": latest_personnel.isoformat() if latest_personnel else None,
        "roster_lag_days": roster_lag_days,
        "personnel_lag_days": personnel_lag_days,
        "missing_roster_days_count": len(missing_roster_days),
        "missing_personnel_days_count": len(missing_personnel_days),
        "missing_roster_days": missing_roster_days,
        "missing_personnel_days": missing_personnel_days,
        "last_backfill_status": backfill_status,
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
    }


def compute_period(
    stats_dir: Path,
    period_key: str,
    now: datetime.datetime | None = None,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
):
    now = now or datetime.datetime.now()
    period_start, period_end = _normalize_period_bounds(now, period_key, start_date, end_date)
    delta_map = _unit_delta_map(
        stats_dir=stats_dir,
        now=now,
        period_key=period_key,
        start_date=period_start,
        end_date=period_end,
    )
    label = {
        "day": "Daily",
        "week": "Weekly",
        "month": "Monthly",
        "year": "Year to Date",
        "rolling_year": "Last 365 Days",
        "alltime": "All-time",
    }.get(period_key, "Daily")
    calls, dur, after, max_sec, ride_in, duration_known_calls = aggregate_timeframe_stats(
        stats_dir,
        period_key,
        now,
        start_date=period_start,
        end_date=period_end,
    )
    text = format_leaderboard_body(
        label,
        period_key,
        calls,
        dur,
        after,
        now,
        duration_known_calls=duration_known_calls,
        start_date=(period_start if period_key != "day" else None),
        end_date=(period_end if period_key != "day" else None),
    )
    if period_key in ("week", "month", "year", "rolling_year"):
        rows = compute_shift_breakdown(
            stats_dir,
            period_key,
            now,
            start_date=period_start,
            end_date=period_end,
            delta_map=delta_map,
        )
    else:
        rows = _rows_from(calls, dur, after, max_sec, ride_in=ride_in, duration_known_calls=duration_known_calls, delta_map=delta_map)
    meta = {}
    if period_key == 'day':
        sd = shift_start(now).date()
        meta['shift_date'] = f"{_shift_letter_for(sd)}-Shift {sd:%m/%d/%y}"
    elif period_key in ('week', 'month', 'year', 'rolling_year'):
        if start_date and end_date:
            start_val = start_date
            end_val = end_date
        else:
            start_val, end_val = _date_range_for_period(now, period_key)
        if period_key in ('year', 'rolling_year'):
            meta['range'] = _format_range(start_val, end_val)
        else:
            meta['range'] = f"{start_val:%b %d} - {end_val:%b %d}"
    return {
        "label": label,
        "period": period_key,
        "text": text,
        "rows": rows,
        "meta": meta,
        "updated": datetime.datetime.utcnow().isoformat() + "Z",
    }


def compute_prior(stats_dir: Path, now: datetime.datetime | None = None):
    now = now or datetime.datetime.now()
    prev_date = (shift_start(now) - datetime.timedelta(days=1)).date()
    prev_prev_date = prev_date - datetime.timedelta(days=1)
    fn = stats_dir / f"shift_stats_{prev_date:%Y-%m-%d}.json"
    fn_prev = stats_dir / f"shift_stats_{prev_prev_date:%Y-%m-%d}.json"
    calls, dur, after, max_sec, ride_in, duration_known_calls = load_stats(fn)
    prev_calls, _, _, _, _prev_ride_in, _prev_duration_known = load_stats(fn_prev)
    calls = {k: int(v) for k, v in calls.items() if k in WATCH_SET}
    dur = {k: int(v) for k, v in dur.items() if k in WATCH_SET}
    after = {k: int(v) for k, v in after.items() if k in WATCH_SET}
    prev_calls = {k: int(v) for k, v in prev_calls.items() if k in WATCH_SET}
    delta_map = {}
    for unit in set(calls.keys()) | set(prev_calls.keys()):
        delta_map[unit] = int(calls.get(unit, 0)) - int(prev_calls.get(unit, 0))
    text = format_leaderboard_body("Daily", "day", calls, dur, after, now, duration_known_calls=duration_known_calls)
    rows = _rows_from(calls, dur, after, max_sec, ride_in=ride_in, duration_known_calls=duration_known_calls, delta_map=delta_map)
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
    parser.add_argument('--roster-dir', default=os.environ.get('ROSTER_DIR', ''), help='Directory containing roster_units_*.json files')
    parser.add_argument('--roster-out', default=str(Path('docs') / 'roster_units.json'), help='Output path for roster_units.json')
    parser.add_argument('--no-roster', action='store_true', help='Skip generating roster_units.json')
    args = parser.parse_args()

    stats_dir = Path(args.stats_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.datetime.now()
    personnel_dir = _default_personnel_stats_dir(stats_dir)
    roster_dir = Path(args.roster_dir) if args.roster_dir else _default_roster_dir(stats_dir)
    year_start, year_end, year_reset = _resolve_yearly_cycle(stats_dir, now)
    rolling_year_start, rolling_year_end = _date_range_for_period(now, "rolling_year")
    cycle_bounds = (year_start, year_end, year_reset)
    shift_day = shift_start(now).date()
    prior_day = shift_day - datetime.timedelta(days=1)
    week_start, week_end = _date_range_for_period(now, "week")
    month_start, month_end = _date_range_for_period(now, "month")

    today_period = compute_period(stats_dir, "day", now=now)
    prior_period = compute_prior(stats_dir, now=now)
    week_period = compute_period(stats_dir, "week", now=now)
    month_period = compute_period(stats_dir, "month", now=now)
    year_period = compute_period(
        stats_dir,
        "year",
        now=now,
        start_date=year_start,
        end_date=year_end,
    )
    rolling_year_period = compute_period(
        stats_dir,
        "rolling_year",
        now=now,
        start_date=rolling_year_start,
        end_date=rolling_year_end,
    )

    personnel_week = compute_personnel_period_hybrid(
        stats_dir,
        roster_dir,
        personnel_dir,
        "week",
        now,
        start_date=week_start,
        end_date=week_end,
        delta_map=_personnel_delta_map(
            stats_dir,
            roster_dir,
            personnel_dir,
            now,
            "week",
            start_date=week_start,
            end_date=week_end,
        ),
    )
    personnel_month = compute_personnel_period_hybrid(
        stats_dir,
        roster_dir,
        personnel_dir,
        "month",
        now,
        start_date=month_start,
        end_date=month_end,
        delta_map=_personnel_delta_map(
            stats_dir,
            roster_dir,
            personnel_dir,
            now,
            "month",
            start_date=month_start,
            end_date=month_end,
        ),
    )
    personnel_year = compute_personnel_period_hybrid(
        stats_dir,
        roster_dir,
        personnel_dir,
        "year",
        now,
        start_date=year_start,
        end_date=year_end,
        delta_map=_personnel_delta_map(
            stats_dir,
            roster_dir,
            personnel_dir,
            now,
            "year",
            start_date=year_start,
            end_date=year_end,
        ),
    )
    personnel_rolling_year = compute_personnel_period_hybrid(
        stats_dir,
        roster_dir,
        personnel_dir,
        "rolling_year",
        now,
        start_date=rolling_year_start,
        end_date=rolling_year_end,
        delta_map=_personnel_delta_map(
            stats_dir,
            roster_dir,
            personnel_dir,
            now,
            "rolling_year",
            start_date=rolling_year_start,
            end_date=rolling_year_end,
        ),
    )

    details_payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "units": {
            "today": _unit_shift_detail_map(stats_dir, shift_day, shift_day),
            "prior": _unit_shift_detail_map(stats_dir, prior_day, prior_day),
            "week": _unit_shift_detail_map(stats_dir, week_start, week_end),
            "month": _unit_shift_detail_map(stats_dir, month_start, month_end),
            "year": _unit_shift_detail_map(stats_dir, year_start, year_end),
            "rolling_year": _unit_shift_detail_map(stats_dir, rolling_year_start, rolling_year_end),
        },
        "personnel": {
            "week": _personnel_shift_detail_map(stats_dir, roster_dir, personnel_dir, week_start, week_end),
            "month": _personnel_shift_detail_map(stats_dir, roster_dir, personnel_dir, month_start, month_end),
            "year": _personnel_shift_detail_map(stats_dir, roster_dir, personnel_dir, year_start, year_end),
            "rolling_year": _personnel_shift_detail_map(stats_dir, roster_dir, personnel_dir, rolling_year_start, rolling_year_end),
        },
    }

    partial_payload = {
        "today": today_period,
        "prior": prior_period,
        "week": week_period,
        "month": month_period,
        "year": year_period,
        "rolling_year": rolling_year_period,
        "personnel": {
            "week": personnel_week,
            "month": personnel_month,
            "year": personnel_year,
            "rolling_year": personnel_rolling_year,
        },
    }

    payload = dict(partial_payload)
    payload["details"] = details_payload
    payload["yearly_summary"] = compute_yearly_summary(
        stats_dir,
        roster_dir,
        personnel_dir,
        now,
        cycle_bounds=cycle_bounds,
    )
    payload["integrity_checks"] = compute_integrity_checks(payload)
    payload["backfill_health"] = compute_backfill_health(
        stats_dir=stats_dir,
        roster_dir=roster_dir,
        personnel_dir=personnel_dir,
        backfill_status_path=(out_path.parent / "backfill_status.json"),
    )

    tmp = out_path.with_suffix('.json.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(out_path)

    if not args.no_roster:
        roster_out = Path(args.roster_out)
        _maybe_write_roster_units(roster_dir, roster_out)


if __name__ == '__main__':
    main()

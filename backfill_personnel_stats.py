#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import math
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


SHIFT_START_HOUR = 7
SHIFT_SECONDS = 24 * 3600
STATS_FILENAME_RE = re.compile(r"shift_stats_(\d{4}-\d{2}-\d{2})\.json")


def _parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise SystemExit("Invalid date format (use YYYY-MM-DD or MM/DD/YYYY).")


def _round_half_up(value: float) -> int:
    return int(math.floor(value + 0.5))


def _normalize_unit_code(code: str | None) -> str:
    cleaned = (code or "").strip().upper()
    if cleaned.startswith("DC") and cleaned[2:].isdigit():
        return f"D{cleaned[2:]}"
    match = re.match(r"^TR(\d+)$", cleaned)
    if match:
        return f"T{match.group(1)}"
    return cleaned


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


def _parse_hours(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _entry_interval(entry: dict, shift_date: dt.date, shift_start: dt.datetime) -> tuple[dt.datetime, dt.datetime] | None:
    start_min = _parse_time_to_minutes(entry.get("from"))
    end_min = _parse_time_to_minutes(entry.get("through"))
    hours = _parse_hours(entry.get("hours"))
    if start_min is None or end_min is None:
        if hours is None:
            return None
        start_dt = shift_start
        end_dt = start_dt + dt.timedelta(hours=hours)
        return start_dt, end_dt
    start_dt = dt.datetime.combine(shift_date, dt.time(start_min // 60, start_min % 60))
    end_dt = dt.datetime.combine(shift_date, dt.time(end_min // 60, end_min % 60))
    if start_min == end_min and hours is not None:
        end_dt = start_dt + dt.timedelta(hours=hours)
    elif end_dt <= start_dt:
        end_dt += dt.timedelta(days=1)
    return start_dt, end_dt


def _overlap_seconds(start: dt.datetime, end: dt.datetime, win_start: dt.datetime, win_end: dt.datetime) -> int:
    if end <= win_start or start >= win_end:
        return 0
    left = max(start, win_start)
    right = min(end, win_end)
    return max(0, int((right - left).total_seconds()))


def _default_stats_dir() -> Path:
    env = os.environ.get("SHIFT_STATS_DIR", "").strip()
    if env:
        return Path(env)
    return Path("data") / "shift_stats"


def _default_roster_dir() -> Path | None:
    env = os.environ.get("ROSTER_DIR", "").strip()
    if env:
        return Path(env).resolve()
    candidates = [Path("TSlogs"), Path.cwd() / "TSlogs", Path.cwd().parent / "TSlogs"]
    for c in candidates:
        if c.exists():
            return c.resolve()
    return None


def _default_personnel_dir(stats_dir: Path) -> Path:
    env = os.environ.get("PERSONNEL_STATS_DIR", "").strip()
    if env:
        return Path(env)
    if stats_dir.name == "shift_stats":
        return stats_dir.parent / "shift_personnel"
    return Path("shift_personnel")


def _stats_dates(stats_dir: Path) -> list[dt.date]:
    dates = []
    if not stats_dir.exists():
        return dates
    for name in os.listdir(stats_dir):
        m = STATS_FILENAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            dates.append(dt.datetime.strptime(m.group(1), "%Y-%m-%d").date())
        except Exception:
            continue
    return sorted(dates)


def _load_shift_stats(stats_path: Path) -> tuple[dict, dict, dict, dict]:
    try:
        data = json.loads(stats_path.read_text(encoding="utf-8"))
    except Exception:
        return {}, {}, {}, {}
    calls = data.get("calls", {}) or {}
    dur = data.get("dur_sec", {}) or {}
    after = data.get("after_0000", {}) or {}
    max_sec = data.get("max_sec", {}) or {}
    return calls, dur, after, max_sec


def _build_roster_map(roster_payload: dict, shift_date: dt.date) -> dict:
    shift_start = dt.datetime.combine(shift_date, dt.time(SHIFT_START_HOUR, 0))
    shift_end = shift_start + dt.timedelta(hours=24)
    after_start = dt.datetime.combine(shift_date + dt.timedelta(days=1), dt.time(0, 0))
    after_end = shift_end
    by_unit: dict[str, dict[str, dict]] = {}
    for unit in roster_payload.get("units", []) or []:
        unit_code = _normalize_unit_code(unit.get("unit_code"))
        if not unit_code:
            continue
        for entry in unit.get("entries", []) or []:
            pkey = _person_key(entry)
            if not pkey:
                continue
            interval = _entry_interval(entry, shift_date, shift_start)
            if not interval:
                continue
            total_sec = _overlap_seconds(interval[0], interval[1], shift_start, shift_end)
            after_sec = _overlap_seconds(interval[0], interval[1], after_start, after_end)
            if total_sec <= 0 and after_sec <= 0:
                continue
            name = str(entry.get("name") or "").strip()
            unit_map = by_unit.setdefault(unit_code, {})
            info = unit_map.setdefault(
                pkey, {"name": name, "total_sec": 0, "after_sec": 0}
            )
            if name and not info.get("name"):
                info["name"] = name
            info["total_sec"] += total_sec
            info["after_sec"] += after_sec
    return by_unit


def _compute_personnel_stats(
    shift_date: dt.date,
    stats_path: Path,
    roster_path: Path,
) -> tuple[dict, dict, dict, dict, dict, list[str]]:
    calls, dur, after, max_sec = _load_shift_stats(stats_path)
    roster_payload = json.loads(roster_path.read_text(encoding="utf-8"))
    roster_map = _build_roster_map(roster_payload, shift_date)

    names: dict[str, str] = {}
    p_calls: dict[str, int] = defaultdict(int)
    p_dur: dict[str, int] = defaultdict(int)
    p_after: dict[str, int] = defaultdict(int)
    p_max: dict[str, int] = defaultdict(int)
    missing_units: list[str] = []

    units = set(calls) | set(dur) | set(after) | set(max_sec)
    after_window_seconds = int((dt.timedelta(hours=SHIFT_START_HOUR)).total_seconds())
    after_window_seconds = after_window_seconds or 1

    for raw_unit in units:
        unit_code = _normalize_unit_code(raw_unit)
        unit_calls = int(calls.get(raw_unit, 0) or 0)
        unit_dur = float(dur.get(raw_unit, 0) or 0)
        unit_after = int(after.get(raw_unit, 0) or 0)
        unit_max = int(max_sec.get(raw_unit, 0) or 0)
        crew = roster_map.get(unit_code)
        if not crew:
            if unit_calls or unit_dur or unit_after or unit_max:
                missing_units.append(unit_code)
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
            if calls_est <= 0:
                max_est = 0
            else:
                max_est = _round_half_up(unit_max * min(1.0, f_total))
            p_calls[pkey] += calls_est
            p_dur[pkey] += dur_est
            p_after[pkey] += after_est
            if max_est > p_max[pkey]:
                p_max[pkey] = max_est
            name = info.get("name")
            if name and pkey not in names:
                names[pkey] = name

    return names, dict(p_calls), dict(p_dur), dict(p_after), dict(p_max), missing_units


def _write_personnel_stats(out_path: Path, names: dict, calls: dict, dur: dict, after: dict, max_sec: dict):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "names": names,
        "calls": calls,
        "dur_sec": dur,
        "after_0000": after,
        "max_sec": max_sec,
    }
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f)


def _run_export(
    roster_dir: Path,
    export_script: Path,
    date_label: str,
    login_file: Path | None,
    insecure: bool,
    ca_bundle: str | None,
) -> int:
    cmd = [sys.executable, str(export_script), "--date", date_label]
    if login_file and login_file.exists():
        cmd += ["--login-file", str(login_file)]
    if insecure:
        cmd.append("--insecure")
    if ca_bundle:
        cmd += ["--ca-bundle", ca_bundle]
    return subprocess.run(cmd, cwd=roster_dir).returncode


def _run_roster_parse(roster_dir: Path, units_script: Path, roster_xlsx: Path) -> int:
    cmd = [sys.executable, str(units_script), "--input", str(roster_xlsx)]
    return subprocess.run(cmd, cwd=roster_dir).returncode


def _ensure_roster_units(
    shift_date: dt.date,
    roster_dir: Path,
    export_missing: bool,
    force_export: bool,
    force_parse: bool,
    login_file: Path | None,
    insecure: bool,
    ca_bundle: str | None,
) -> Path | None:
    date_label = shift_date.strftime("%Y-%m-%d")
    roster_xlsx = roster_dir / f"roster_{date_label}.xlsx"
    roster_json = roster_dir / f"roster_units_{date_label}.json"
    export_script = (roster_dir / "export_roster.py").resolve()
    units_script = (roster_dir / "roster_units.py").resolve()

    if force_export or (export_missing and not roster_xlsx.exists()):
        if export_script.exists():
            code = _run_export(roster_dir, export_script, date_label, login_file, insecure, ca_bundle)
            if code != 0:
                print(f"WARN: roster export failed for {date_label} (exit {code})")
        else:
            print("WARN: export_roster.py not found; skipping roster export.")

    if (force_parse or not roster_json.exists()) and roster_xlsx.exists():
        if units_script.exists():
            code = _run_roster_parse(roster_dir, units_script, roster_xlsx)
            if code != 0:
                print(f"WARN: roster_units.py failed for {date_label} (exit {code})")
        else:
            print("WARN: roster_units.py not found; skipping roster parsing.")

    return roster_json if roster_json.exists() else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill shift_personnel stats from shift_stats and roster_units.",
    )
    parser.add_argument("--start", help="Start date (YYYY-MM-DD or MM/DD/YYYY).")
    parser.add_argument("--end", help="End date (YYYY-MM-DD or MM/DD/YYYY).")
    parser.add_argument("--stats-dir", help="Shift stats directory.")
    parser.add_argument("--roster-dir", help="Roster directory (roster_*.xlsx + roster_units_*.json).")
    parser.add_argument("--out-dir", help="Output personnel stats directory.")
    parser.add_argument("--login-file", help="KRONOS login file for exports.")
    parser.add_argument("--export-missing", action="store_true", help="Export roster XLSX for missing dates.")
    parser.add_argument("--force-export", action="store_true", help="Export roster XLSX even if it exists.")
    parser.add_argument("--force-parse", action="store_true", help="Re-parse roster XLSX into JSON even if it exists.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing shift_personnel files.")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification for roster exports.")
    parser.add_argument("--ca-bundle", help="CA bundle path for roster exports.")
    args = parser.parse_args()

    stats_dir = Path(args.stats_dir) if args.stats_dir else _default_stats_dir()
    roster_dir = Path(args.roster_dir) if args.roster_dir else _default_roster_dir()
    out_dir = Path(args.out_dir) if args.out_dir else _default_personnel_dir(stats_dir)
    stats_dir = stats_dir.resolve()
    out_dir = out_dir.resolve()
    roster_dir = roster_dir.resolve() if roster_dir else roster_dir

    if not stats_dir.exists():
        raise SystemExit(f"Shift stats dir not found: {stats_dir}")
    if not roster_dir or not roster_dir.exists():
        raise SystemExit("Roster directory not found. Use --roster-dir or set ROSTER_DIR.")

    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)
    if not start_date or not end_date:
        dates = _stats_dates(stats_dir)
        if not dates:
            raise SystemExit("No shift_stats files found.")
        start_date = start_date or dates[0]
        end_date = end_date or dates[-1]
    if start_date > end_date:
        raise SystemExit("Start date must be <= end date.")

    login_file = Path(args.login_file) if args.login_file else None
    if not login_file:
        candidate = roster_dir / "KRONOSLOGIN.txt"
        if candidate.exists():
            login_file = candidate

    processed = 0
    skipped = 0
    missing_rosters = 0
    date = start_date
    one_day = dt.timedelta(days=1)

    while date <= end_date:
        stats_path = stats_dir / f"shift_stats_{date:%Y-%m-%d}.json"
        if not stats_path.exists():
            date += one_day
            continue
        out_path = out_dir / f"shift_personnel_{date:%Y-%m-%d}.json"
        if out_path.exists() and not args.overwrite:
            skipped += 1
            date += one_day
            continue
        roster_json = _ensure_roster_units(
            date,
            roster_dir,
            args.export_missing,
            args.force_export,
            args.force_parse,
            login_file,
            args.insecure,
            args.ca_bundle,
        )
        if not roster_json:
            missing_rosters += 1
            print(f"WARN: roster_units missing for {date:%Y-%m-%d}; skipping.")
            date += one_day
            continue
        names, calls, dur, after, max_sec, missing_units = _compute_personnel_stats(
            date, stats_path, roster_json
        )
        _write_personnel_stats(out_path, names, calls, dur, after, max_sec)
        processed += 1
        if missing_units:
            print(
                f"WARN: {date:%Y-%m-%d} missing roster units: {', '.join(sorted(set(missing_units)))}"
            )
        date += one_day

    print(
        f"Backfill complete. processed={processed} skipped={skipped} missing_rosters={missing_rosters}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

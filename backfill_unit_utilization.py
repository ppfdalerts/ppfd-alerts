"""Restore missing unit activity history without overwriting newer live data.

The retained stats directory contains unit call totals and duration aggregates
that predate the current GitHub working copy.  This utility fills only absent
or zero values, merges missing event evidence, and prefers true unit intervals
over incident-wide intervals.  It never treats a CAD incident's ``Involved``
duration as the duration of each assigned unit.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import zipfile
from collections import Counter
from pathlib import Path


STATS_RE = re.compile(r"shift_stats_(\d{4}-\d{2}-\d{2})\.json$")
NUMERIC_MAPS = (
    "calls",
    "dur_sec",
    "after_0000",
    "max_sec",
    "transporting_count",
    "at_hospital_count",
    "ride_in_count",
)


def load_payload(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise RuntimeError(f"Cannot read {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a JSON object in {path}")
    return payload


def write_payload(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def positive_number(value) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def has_unit_duration(interval) -> bool:
    if not isinstance(interval, dict) or not interval:
        return False
    if interval.get("unit_duration_unavailable"):
        return False
    scope = str(interval.get("duration_scope") or "").strip().lower().replace("-", "_")
    source = str(interval.get("source") or "").strip().lower()
    if scope in {"incident", "incident_wide"} or source == "incident_csv_involved":
        return False
    return bool(interval.get("start") and interval.get("end"))


def merge_numeric_map(target: dict, source: dict, field: str, report: Counter) -> bool:
    source_map = source.get(field)
    if not isinstance(source_map, dict):
        return False
    target_map = target.get(field)
    if not isinstance(target_map, dict):
        target_map = {}
        target[field] = target_map
    changed = False
    for key, value in source_map.items():
        if not positive_number(value) or positive_number(target_map.get(key)):
            continue
        target_map[key] = value
        report[f"{field}_values_added"] += 1
        changed = True
    return changed


def merge_duration_known(target: dict, source: dict, report: Counter) -> bool:
    source_known = source.get("duration_known_calls")
    if not isinstance(source_known, dict):
        source_known = {}
    source_calls = source.get("calls") if isinstance(source.get("calls"), dict) else {}
    source_durations = source.get("dur_sec") if isinstance(source.get("dur_sec"), dict) else {}
    target_known = target.get("duration_known_calls")
    if not isinstance(target_known, dict):
        target_known = {}
        target["duration_known_calls"] = target_known
    changed = False
    for unit, duration in source_durations.items():
        if not positive_number(duration) or positive_number(target_known.get(unit)):
            continue
        known = source_known.get(unit)
        if not positive_number(known):
            # Legacy duration aggregates were recorded when each unit cleared;
            # with no partial-coverage ledger, every retained call is known.
            known = source_calls.get(unit)
        if not positive_number(known):
            continue
        target_known[unit] = int(known)
        report["duration_known_values_added"] += 1
        changed = True
    return changed


def merge_list(target: dict, source: dict, field: str, report: Counter) -> bool:
    source_values = source.get(field)
    if not isinstance(source_values, list):
        return False
    target_values = target.get(field)
    if not isinstance(target_values, list):
        target_values = []
    merged = list(target_values)
    seen = {str(value) for value in merged}
    for value in source_values:
        if str(value) in seen:
            continue
        merged.append(value)
        seen.add(str(value))
        report[f"{field}_added"] += 1
    if len(merged) == len(target_values):
        return False
    target[field] = sorted(merged, key=str)
    return True


def merge_call_times(target: dict, source: dict, report: Counter) -> bool:
    source_times = source.get("call_times")
    if not isinstance(source_times, dict):
        return False
    target_times = target.get("call_times")
    if not isinstance(target_times, dict):
        target_times = {}
        target["call_times"] = target_times
    changed = False
    for unit, raw_values in source_times.items():
        if not isinstance(raw_values, list):
            continue
        values = target_times.get(unit)
        if not isinstance(values, list):
            values = []
        merged = list(values)
        seen = {str(value) for value in merged}
        for value in raw_values:
            if str(value) in seen:
                continue
            merged.append(value)
            seen.add(str(value))
            report["call_times_added"] += 1
        if len(merged) != len(values):
            target_times[unit] = sorted(merged, key=str)
            changed = True
    return changed


def merge_keyed_map(target: dict, source: dict, field: str, report: Counter) -> bool:
    source_map = source.get(field)
    if not isinstance(source_map, dict):
        return False
    target_map = target.get(field)
    if not isinstance(target_map, dict):
        target_map = {}
        target[field] = target_map
    changed = False
    for key, value in source_map.items():
        if key in target_map:
            continue
        target_map[key] = value
        report[f"{field}_added"] += 1
        changed = True
    return changed


def merge_intervals(target: dict, source: dict, report: Counter) -> bool:
    source_intervals = source.get("call_intervals")
    if not isinstance(source_intervals, dict):
        return False
    target_intervals = target.get("call_intervals")
    if not isinstance(target_intervals, dict):
        target_intervals = {}
        target["call_intervals"] = target_intervals
    changed = False
    for key, source_interval in source_intervals.items():
        target_interval = target_intervals.get(key)
        if key not in target_intervals:
            target_intervals[key] = source_interval
            report["call_intervals_added"] += 1
            changed = True
        elif has_unit_duration(source_interval) and not has_unit_duration(target_interval):
            target_intervals[key] = source_interval
            report["non_unit_intervals_replaced"] += 1
            changed = True
    return changed


def merge_payload(target: dict, source: dict, report: Counter) -> bool:
    changed = False
    for field in NUMERIC_MAPS:
        changed |= merge_numeric_map(target, source, field, report)
    changed |= merge_duration_known(target, source, report)
    changed |= merge_list(target, source, "counted_calls", report)
    changed |= merge_call_times(target, source, report)
    changed |= merge_keyed_map(target, source, "call_events", report)
    changed |= merge_intervals(target, source, report)
    return changed


def create_backup(target_dir: Path, changed_paths: list[Path], backup_dir: Path) -> Path | None:
    existing = [path for path in changed_paths if path.exists()]
    if not existing:
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"shift_stats_pre_utilization_backfill_{stamp}.zip"
    with zipfile.ZipFile(backup_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in existing:
            archive.write(path, arcname=path.relative_to(target_dir.parent))
    return backup_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", default="data/shift_stats")
    parser.add_argument(
        "--source-dir",
        action="append",
        required=True,
        help="Retained shift-stats directory; may be repeated (first source wins).",
    )
    parser.add_argument("--backup-dir", default="backups")
    parser.add_argument("--start", help="First shift date, YYYY-MM-DD")
    parser.add_argument("--end", help="Last shift date, YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    target_dir = Path(args.target_dir).resolve()
    source_dirs = [Path(value).resolve() for value in args.source_dir]
    start_date = dt.date.fromisoformat(args.start) if args.start else None
    end_date = dt.date.fromisoformat(args.end) if args.end else None
    report: Counter = Counter()
    report["source_directories"] = len(source_dirs)
    pending: dict[Path, dict] = {}
    changed_dates: list[dt.date] = []

    for source_dir in source_dirs:
        if not source_dir.is_dir():
            raise RuntimeError(f"Source directory does not exist: {source_dir}")
        for source_path in sorted(source_dir.glob("shift_stats_*.json")):
            match = STATS_RE.fullmatch(source_path.name)
            if not match:
                continue
            shift_date = dt.date.fromisoformat(match.group(1))
            if start_date and shift_date < start_date:
                continue
            if end_date and shift_date > end_date:
                continue
            report["source_files_scanned"] += 1
            target_path = target_dir / source_path.name
            source_payload = load_payload(source_path)
            if target_path in pending:
                target_payload = pending[target_path]
            elif target_path.exists():
                target_payload = load_payload(target_path)
            else:
                target_payload = {}
            before = json.dumps(target_payload, sort_keys=True, separators=(",", ":"))
            merge_payload(target_payload, source_payload, report)
            after = json.dumps(target_payload, sort_keys=True, separators=(",", ":"))
            if after != before:
                pending[target_path] = target_payload
                changed_dates.append(shift_date)

    changed_paths = sorted(pending)
    report["files_updated"] = len(changed_paths)
    if changed_dates:
        report["earliest_updated_shift"] = min(changed_dates).isoformat()
        report["latest_updated_shift"] = max(changed_dates).isoformat()

    backup_path = None
    if changed_paths and not args.dry_run:
        backup_path = create_backup(target_dir, changed_paths, Path(args.backup_dir).resolve())
        for path in changed_paths:
            write_payload(path, pending[path])
    result = dict(sorted(report.items()))
    result["dry_run"] = bool(args.dry_run)
    result["backup"] = str(backup_path) if backup_path else None
    result["target_directory"] = str(target_dir)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

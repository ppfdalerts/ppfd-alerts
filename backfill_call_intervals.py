"""Backfill exact unit call intervals from retained unit-removal alerts."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from collections import defaultdict
from pathlib import Path


WATCH_UNITS = {
    "R33", "E33", "T33", "33FD", "LR36", "HM33", "R34", "E34", "TR34",
    "E36", "S36", "R36", "E35", "34FD", "36FD", "D35", "R35", "35FD", "E136",
}
STATS_RE = re.compile(r"shift_stats_(\d{4}-\d{2}-\d{2})\.json$")
LOG_RE = re.compile(
    r"^(?P<logged>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+"
    r"SENT \[LOG\].*?REMOVED FROM CALL\s+-\s+"
    r"Changed unit:\s*(?P<unit>[^|]+)\|.*?\bTime:\s*(?P<time>\d{1,2}:\d{2})\s*$",
    re.IGNORECASE,
)


def normalize_unit(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def parse_timestamp(value) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


def parse_removal_line(line: str) -> tuple[str, dt.datetime, dt.datetime] | None:
    match = LOG_RE.match(line.rstrip())
    if not match:
        return None
    try:
        clear_dt = dt.datetime.strptime(match.group("logged"), "%Y-%m-%d %H:%M:%S")
        hour, minute = (int(part) for part in match.group("time").split(":", 1))
    except ValueError:
        return None
    start_dt = clear_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
    # A notification sent shortly after midnight can describe a call from the
    # previous calendar day. Keep the same correction used by call-time import.
    if start_dt > clear_dt + dt.timedelta(hours=2):
        start_dt -= dt.timedelta(days=1)
    unit = normalize_unit(match.group("unit"))
    if unit not in WATCH_UNITS or clear_dt <= start_dt:
        return None
    return unit, start_dt, clear_dt


def load_removals(path: Path) -> list[tuple[str, dt.datetime, dt.datetime]]:
    removals = []
    if not path.exists():
        return removals
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            parsed = parse_removal_line(line)
            if parsed:
                removals.append(parsed)
    # The unit-topic notification and LOG copy are reduced to one LOG record by
    # the parser. Identical retained lines are still safe to deduplicate here.
    return sorted(set(removals), key=lambda value: value[2])


def event_timestamp(value) -> dt.datetime | None:
    if isinstance(value, dict):
        value = value.get("timestamp")
    return parse_timestamp(value)


def shift_date_for(start: dt.datetime) -> dt.date:
    return start.date() if start.hour >= 7 else start.date() - dt.timedelta(days=1)


def write_payload(path: Path, payload: dict) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stats-dir", default="data/shift_stats")
    parser.add_argument("--alerts-log", default="alerts.log")
    parser.add_argument("--start", help="First shift date, YYYY-MM-DD")
    parser.add_argument("--end", help="Last shift date, YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    stats_dir = Path(args.stats_dir)
    start_date = dt.date.fromisoformat(args.start) if args.start else None
    end_date = dt.date.fromisoformat(args.end) if args.end else None
    removals = load_removals(Path(args.alerts_log))
    removals_by_key: dict[tuple[dt.date, str, str], list[dt.datetime]] = defaultdict(list)
    for unit, start_dt, clear_dt in removals:
        shift_date = shift_date_for(start_dt)
        if start_date and shift_date < start_date:
            continue
        if end_date and shift_date > end_date:
            continue
        minute = start_dt.strftime("%Y-%m-%dT%H:%M")
        removals_by_key[(shift_date, unit, minute)].append(clear_dt)

    report = {
        "removal_records": len(removals),
        "candidate_keys": len(removals_by_key),
        "files_updated": [],
        "intervals_added": 0,
        "ambiguous_matches": 0,
        "missing_matches": 0,
        "skipped_existing": 0,
    }

    for path in sorted(stats_dir.glob("shift_stats_*.json")):
        match = STATS_RE.fullmatch(path.name)
        if not match:
            continue
        shift_date = dt.date.fromisoformat(match.group(1))
        if start_date and shift_date < start_date:
            continue
        if end_date and shift_date > end_date:
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw_events = payload.get("call_events") or {}
        if not isinstance(raw_events, dict):
            continue
        intervals = payload.get("call_intervals")
        if not isinstance(intervals, dict):
            intervals = {}
        by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
        for raw_key, raw_event in raw_events.items():
            text_key = str(raw_key)
            if "|" not in text_key:
                continue
            incident_id, raw_unit = text_key.rsplit("|", 1)
            unit = normalize_unit(raw_unit)
            event_dt = event_timestamp(raw_event)
            if not incident_id or unit not in WATCH_UNITS or event_dt is None:
                continue
            if shift_date_for(event_dt) != shift_date:
                continue
            by_key[(unit, event_dt.strftime("%Y-%m-%dT%H:%M"))].append(text_key)

        changed = False
        for (candidate_date, unit, minute), clear_times in removals_by_key.items():
            if candidate_date != shift_date:
                continue
            candidates = by_key.get((unit, minute), [])
            if len(candidates) != 1 or len(clear_times) != 1:
                if candidates and clear_times:
                    report["ambiguous_matches"] += 1
                elif clear_times:
                    report["missing_matches"] += 1
                continue
            key = candidates[0]
            existing = intervals.get(key)
            if isinstance(existing, dict) and existing.get("end"):
                report["skipped_existing"] += 1
                continue
            start_dt = event_timestamp(raw_events.get(key))
            clear_dt = clear_times[0]
            if start_dt is None or clear_dt <= start_dt:
                report["missing_matches"] += 1
                continue
            duration_sec = round((clear_dt - start_dt).total_seconds(), 1)
            if duration_sec <= 0 or duration_sec > 48 * 3600:
                report["missing_matches"] += 1
                continue
            intervals[key] = {
                "start": start_dt.isoformat(),
                "end": clear_dt.isoformat(),
                "duration_sec": duration_sec,
                "ongoing": False,
                "source": "alert_history_removal",
                "evidence": "alerts.log unit-removal notification",
            }
            report["intervals_added"] += 1
            changed = True

        if changed and not args.dry_run:
            payload["call_intervals"] = dict(sorted(intervals.items()))
            write_payload(path, payload)
            report["files_updated"].append(path.name)

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

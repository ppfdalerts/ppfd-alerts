"""Store CSV incident-wide Involved windows without replacing unit durations."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from collections import OrderedDict
from pathlib import Path


WATCH_UNITS = {
    "R33", "E33", "T33", "33FD", "LR36", "HM33", "R34", "E34", "TR34",
    "E36", "S36", "R36", "E35", "34FD", "36FD", "D35", "R35", "35FD", "E136",
}
TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def clean(value) -> str:
    text = str(value or "").strip()
    if text.startswith("="):
        text = text[1:].strip()
    return text.strip().strip('"').strip()


def parse_datetime(value: str) -> dt.datetime | None:
    text = clean(value)
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %I:%M:%S %p"):
        try:
            return dt.datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def parse_duration(value: str) -> float | None:
    text = clean(value)
    parts = text.split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    if minutes > 59 or seconds > 59 or hours < 0:
        return None
    return float(hours * 3600 + minutes * 60 + seconds)


def normalize_unit(value: str) -> str:
    unit = clean(value).upper()
    if unit.startswith("DC") and unit[2:].isdigit():
        return f"D{unit[2:]}"
    return unit


def watched_units(value: str) -> list[str]:
    result = []
    seen = set()
    for token in TOKEN_RE.findall((value or "").upper()):
        unit = normalize_unit(token)
        if unit in WATCH_UNITS and unit not in seen:
            result.append(unit)
            seen.add(unit)
    return result


def load_involved_map(path: Path | None) -> dict[str, str]:
    if not path:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("Involved map must be a JSON object keyed by incident ID")
    return {clean(key): clean(value) for key, value in payload.items() if clean(key) and clean(value)}


def shift_date_for(value: dt.datetime) -> dt.date:
    return value.date() if value.hour >= 7 else value.date() - dt.timedelta(days=1)


def has_unit_duration(interval) -> bool:
    if not isinstance(interval, dict) or not interval:
        return False
    scope = str(interval.get("duration_scope") or "").strip().lower().replace("-", "_")
    source = str(interval.get("source") or "").strip().lower()
    return scope not in {"incident", "incident_wide"} and source != "incident_csv_involved"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_file")
    parser.add_argument("--stats-dir", default="data/shift_stats")
    parser.add_argument("--shift-date", help="Shift date to update, YYYY-MM-DD")
    parser.add_argument(
        "--involved-map",
        help="Optional JSON object of incident IDs to Involved durations when the CSV export omits that column",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    csv_path = Path(args.csv_file)
    stats_dir = Path(args.stats_dir)
    target_date = dt.date.fromisoformat(args.shift_date) if args.shift_date else None
    involved_map = load_involved_map(Path(args.involved_map)) if args.involved_map else {}
    records: OrderedDict[str, dict] = OrderedDict()
    rows = 0
    skipped = 0
    duplicates = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            rows += 1
            incident_id = clean(row.get("Incident"))
            start = parse_datetime(row.get("Date"))
            duration = parse_duration(row.get("Involved"))
            if duration is None:
                duration = parse_duration(involved_map.get(incident_id))
            if not incident_id or start is None or duration is None:
                skipped += 1
                continue
            shift_date = shift_date_for(start)
            if target_date and shift_date != target_date:
                continue
            units = watched_units(row.get("Trucks"))
            if not units:
                skipped += 1
                continue
            for unit in units:
                key = f"{incident_id}|{unit}"
                if key in records:
                    duplicates += 1
                    continue
                records[key] = {
                    "start": start,
                    "duration_sec": duration,
                    "unit": unit,
                    "incident_id": incident_id,
                }

    if not records:
        print(json.dumps({"rows": rows, "records": 0, "skipped": skipped}, indent=2))
        return 0

    dates = {shift_date_for(record["start"]) for record in records.values()}
    if len(dates) != 1:
        raise SystemExit(f"Expected one shift date, found: {sorted(dates)}")
    shift_date = next(iter(dates))
    stats_path = stats_dir / f"shift_stats_{shift_date:%Y-%m-%d}.json"
    payload = json.loads(stats_path.read_text(encoding="utf-8"))
    events = payload.get("call_events") or {}
    counted = {str(value) for value in payload.get("counted_calls") or []}
    intervals = payload.get("call_intervals")
    if not isinstance(intervals, dict):
        intervals = {}

    added = 0
    replaced = 0
    preserved_unit_intervals = 0
    missing_ledger = []
    for key, record in records.items():
        if key not in events and key not in counted:
            missing_ledger.append(key)
            continue
        start = record["start"]
        end = start + dt.timedelta(seconds=record["duration_sec"])
        prior = intervals.get(key)
        # A unit's live dispatch-to-clear interval is authoritative.  The CSV
        # Involved value is scoped to the whole incident and cannot replace it.
        if has_unit_duration(prior):
            preserved_unit_intervals += 1
            continue
        intervals[key] = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "duration_sec": round(record["duration_sec"], 1),
            "ongoing": False,
            "source": "incident_csv_involved",
            "duration_scope": "incident",
        }
        if isinstance(prior, dict) and prior.get("end"):
            replaced += 1
        else:
            added += 1

    if not args.dry_run and (added or replaced):
        payload["call_intervals"] = dict(sorted(intervals.items()))
        temp = stats_path.with_suffix(stats_path.suffix + ".tmp")
        temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temp.replace(stats_path)

    print(json.dumps({
        "csv": str(csv_path),
        "shift_date": shift_date.isoformat(),
        "rows": rows,
        "records": len(records),
        "added": added,
        "replaced": replaced,
        "preserved_unit_intervals": preserved_unit_intervals,
        "duplicates": duplicates,
        "skipped": skipped,
        "missing_ledger": missing_ledger,
        "dry_run": args.dry_run,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

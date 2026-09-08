import argparse
import csv
import datetime
import json
import re
from collections import defaultdict
from pathlib import Path


SHIFT_HOUR = 7

THREAD_IDS = {
    "GENERAL": 1,
    "R33": 2,
    "E33": 3,
    "T33": 4,
    "33FD": 5,
    "LR36": 6,
    "HM33": 7,
    "R34": 8,
    "E34": 9,
    "TR34": 10,
    "E36": 11,
    "S36": 12,
    "R36": 13,
    "E35": 14,
    "34FD": 15,
    "36FD": 16,
    "D35": 17,
    "R35": 18,
    "35FD": 19,
    "LOG": 20,
    "E136": 7126,
}
WATCH_SET = set(THREAD_IDS) - {"GENERAL"}
TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def clean_field(value) -> str:
    text = str(value or "").strip()
    if text.startswith("="):
        text = text[1:].strip()
    return text.strip().strip('"').strip()


def parse_incident_datetime(value: str) -> datetime.datetime | None:
    text = clean_field(value)
    if not text:
        return None
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %I:%M:%S %p"):
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def normalize_unit(token: str) -> str:
    unit = clean_field(token).upper()
    if unit.startswith("DC") and unit[2:].isdigit():
        return f"D{unit[2:]}"
    return unit


def iter_watched_units(raw_trucks: str):
    seen: set[str] = set()
    for token in TOKEN_RE.findall((raw_trucks or "").upper()):
        unit = normalize_unit(token)
        if not unit or unit not in WATCH_SET or unit in seen:
            continue
        seen.add(unit)
        yield unit


def shift_date_for_timestamp(ts: datetime.datetime) -> tuple[datetime.date, bool]:
    after_midnight = ts.time() < datetime.time(hour=SHIFT_HOUR)
    day = ts.date()
    if after_midnight:
        day -= datetime.timedelta(days=1)
    return day, after_midnight


def empty_day_payload():
    return {
        "calls": defaultdict(int),
        "after_0000": defaultdict(int),
        "ride_in_count": defaultdict(int),
        "duration_known_calls": defaultdict(int),
        "associations": set(),
    }


def build_import_data(csv_paths: list[Path]):
    per_day: dict[datetime.date, dict] = {}
    seen_keys: set[tuple[str, str, datetime.date]] = set()
    # A unit can appear on multiple incident rows, but it cannot be attached
    # to two incidents at the exact same timestamp. This catches duplicate
    # incident IDs as well as distinct IDs produced for the same attachment.
    seen_unit_times: set[tuple[str, datetime.datetime]] = set()
    total_rows = 0
    kept_rows = 0
    duplicate_rows = 0

    for csv_path in csv_paths:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                total_rows += 1
                incident_id = clean_field(row.get("Incident"))
                incident_dt = parse_incident_datetime(row.get("Date"))
                _response_secs = clean_field(row.get("Response Secs"))
                if not incident_id or incident_dt is None:
                    continue
                shift_day, after_midnight = shift_date_for_timestamp(incident_dt)
                watched_units = list(iter_watched_units(row.get("Trucks") or ""))
                if not watched_units:
                    continue
                bucket = per_day.setdefault(shift_day, empty_day_payload())
                for unit in watched_units:
                    time_key = (unit, incident_dt)
                    if time_key in seen_unit_times:
                        duplicate_rows += 1
                        continue
                    dedupe_key = (incident_id, unit, shift_day)
                    if dedupe_key in seen_keys:
                        duplicate_rows += 1
                        continue
                    seen_unit_times.add(time_key)
                    seen_keys.add(dedupe_key)
                    kept_rows += 1
                    bucket["calls"][unit] += 1
                    if after_midnight:
                        bucket["after_0000"][unit] += 1
                    bucket["associations"].add((incident_id, unit, after_midnight, incident_dt.isoformat()))

    return per_day, total_rows, kept_rows, duplicate_rows


def build_shift_stats_payload(day_bucket: dict) -> dict:
    units = sorted(day_bucket["calls"].keys())
    counted_calls = sorted(
        f"{incident_id}|{unit}"
        for incident_id, unit, _after_midnight, _timestamp in day_bucket["associations"]
    )
    call_events = {
        f"{incident_id}|{unit}": {"timestamp": timestamp, "source": "incident_csv"}
        for incident_id, unit, _after_midnight, timestamp in sorted(day_bucket["associations"])
    }
    return {
        "calls": {unit: int(day_bucket["calls"][unit]) for unit in units},
        "dur_sec": {},
        "after_0000": {unit: int(day_bucket["after_0000"].get(unit, 0)) for unit in units if int(day_bucket["after_0000"].get(unit, 0)) > 0},
        "max_sec": {},
        "transporting_count": {},
        "at_hospital_count": {},
        "ride_in_count": {unit: int(day_bucket["ride_in_count"].get(unit, 0)) for unit in units},
        "duration_known_calls": {unit: int(day_bucket["duration_known_calls"].get(unit, 0)) for unit in units},
        "counted_calls": counted_calls,
        "call_events": call_events,
    }


def merge_existing_shift_stats(out_path: Path, day_bucket: dict, events_only: bool = False) -> tuple[int, int]:
    """Add only CSV incident/unit pairs absent from the live call ledger."""
    try:
        with out_path.open("r", encoding="utf-8-sig") as handle:
            payload = json.load(handle)
    except Exception as exc:
        raise RuntimeError(f"Cannot read existing stats {out_path}: {exc}") from exc

    counted_raw = payload.get("counted_calls")
    if not isinstance(counted_raw, list):
        if events_only:
            calls = payload.get("calls", {})
            call_events = payload.setdefault("call_events", {})
            if not isinstance(call_events, dict):
                call_events = {}
                payload["call_events"] = call_events

            associations_by_unit: dict[str, list[tuple]] = defaultdict(list)
            for association in day_bucket["associations"]:
                associations_by_unit[association[1]].append(association)

            events_added = 0
            for unit, associations in associations_by_unit.items():
                # Without a live incident ledger, only accept a complete unit/day
                # match. This restores timestamps without guessing which calls count.
                if len(associations) != int(calls.get(unit, 0) or 0):
                    continue
                for incident_id, _unit, _after_midnight, timestamp in sorted(associations):
                    key = f"{incident_id}|{unit}"
                    if key in call_events:
                        continue
                    call_events[key] = {"timestamp": timestamp, "source": "incident_csv_exact_count"}
                    events_added += 1

            if events_added:
                tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
                with tmp_path.open("w", encoding="utf-8") as handle:
                    json.dump(payload, handle, indent=2, sort_keys=True)
                tmp_path.replace(out_path)
            return 0, events_added
        raise RuntimeError(
            f"Refusing to merge {out_path.name}: existing file has no counted_calls ledger"
        )
    counted = {str(value) for value in counted_raw if str(value).strip()}
    calls = payload.setdefault("calls", {})
    after = payload.setdefault("after_0000", {})
    ride_ins = payload.setdefault("ride_in_count", {})
    call_events = payload.setdefault("call_events", {})
    if not isinstance(call_events, dict):
        call_events = {}
        payload["call_events"] = call_events
    added = 0
    events_added = 0
    for incident_id, unit, after_midnight, timestamp in sorted(day_bucket["associations"]):
        key = f"{incident_id}|{unit}"
        if key in counted and key not in call_events:
            call_events[key] = {"timestamp": timestamp, "source": "incident_csv"}
            events_added += 1
        if key in counted:
            continue
        if events_only:
            continue
        calls[unit] = int(calls.get(unit, 0) or 0) + 1
        if after_midnight:
            after[unit] = int(after.get(unit, 0) or 0) + 1
        ride_ins.setdefault(unit, 0)
        counted.add(key)
        call_events[key] = {"timestamp": timestamp, "source": "incident_csv"}
        events_added += 1
        added += 1
    payload["counted_calls"] = sorted(counted)

    if added or events_added:
        tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        tmp_path.replace(out_path)
    return added, events_added


def main():
    parser = argparse.ArgumentParser(description="Backfill missing unit shift_stats from exported incident-history CSV files.")
    parser.add_argument("csv_files", nargs="+", help="One or more incident-history CSV files in PC_*.csv format.")
    parser.add_argument("--stats-dir", default="data/shift_stats", help="Directory containing shift_stats_*.json files.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing shift_stats files instead of skipping them.")
    parser.add_argument(
        "--merge-existing",
        action="store_true",
        help="Merge missing incident/unit pairs into existing files using counted_calls; never alter duration totals.",
    )
    parser.add_argument(
        "--events-only",
        action="store_true",
        help="Add incident-keyed timestamps only for calls already present in counted_calls; never change totals.",
    )
    args = parser.parse_args()

    csv_paths = [Path(p) for p in args.csv_files]
    missing_inputs = [str(p) for p in csv_paths if not p.exists()]
    if missing_inputs:
        raise SystemExit("Missing CSV input(s): " + ", ".join(missing_inputs))

    stats_dir = Path(args.stats_dir)
    stats_dir.mkdir(parents=True, exist_ok=True)

    per_day, total_rows, kept_rows, duplicate_rows = build_import_data(csv_paths)
    created_dates: list[datetime.date] = []
    skipped_dates: list[datetime.date] = []
    merged_dates: list[datetime.date] = []
    merged_rows = 0
    event_rows = 0

    for day in sorted(per_day.keys()):
        out_path = stats_dir / f"shift_stats_{day:%Y-%m-%d}.json"
        if args.events_only and not out_path.exists():
            skipped_dates.append(day)
            continue
        if out_path.exists() and not args.overwrite:
            if args.merge_existing or args.events_only:
                added, events_added = merge_existing_shift_stats(out_path, per_day[day], events_only=args.events_only)
                if added:
                    merged_dates.append(day)
                    merged_rows += added
                event_rows += events_added
            skipped_dates.append(day)
            continue
        payload = build_shift_stats_payload(per_day[day])
        tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        tmp_path.replace(out_path)
        created_dates.append(day)

    print(f"CSV files: {len(csv_paths)}")
    print(f"Rows scanned: {total_rows}")
    print(f"Incident-unit rows imported: {kept_rows}")
    print(f"Incident-unit duplicates skipped: {duplicate_rows}")
    print(f"Shift files created: {len(created_dates)}")
    print(f"Shift files skipped: {len(skipped_dates)}")
    print(f"Existing shift files merged: {len(merged_dates)}")
    print(f"Existing incident-unit pairs added: {merged_rows}")
    print(f"Incident-keyed timestamps added: {event_rows}")
    if created_dates:
        print(f"Created range: {created_dates[0]:%Y-%m-%d} .. {created_dates[-1]:%Y-%m-%d}")
    if skipped_dates:
        print(f"Skipped range: {skipped_dates[0]:%Y-%m-%d} .. {skipped_dates[-1]:%Y-%m-%d}")


if __name__ == "__main__":
    main()

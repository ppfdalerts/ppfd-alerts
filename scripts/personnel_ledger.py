"""Exact, read-only incident-to-staffing attribution for the PPFD dashboard.

Legacy shift aggregates cannot identify individual calls. This module never
turns a share of a shift's total into invented incident records.
"""
from __future__ import annotations

from collections import defaultdict
import datetime as dt
from functools import lru_cache
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ROOT = Path(__file__).resolve().parents[1]
STAFFING = ROOT / "docs" / "staffing"
UTC = dt.timezone.utc


def normalize_unit(value):
    code = re.sub(r"\s+", "", str(value or "").upper())
    return re.sub(r"^DC(?=\d)", "D", re.sub(r"^TR(?=\d)", "T", code))


def timestamp(value):
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if parsed.tzinfo is None:
        try:
            parsed = parsed.replace(tzinfo=ZoneInfo("America/New_York"))
            # A naive fall-back hour cannot establish which real instant it is.
            if parsed.utcoffset() != parsed.replace(fold=1).utcoffset():
                return None
        except ZoneInfoNotFoundError:
            # Older Windows alert environments may lack the IANA data package.
            # US Eastern rules since 2007, restricted to this department's zone.
            if parsed.year < 2007:
                return None
            march = dt.date(parsed.year, 3, 8)
            march += dt.timedelta(days=(6-march.weekday()) % 7)
            november = dt.date(parsed.year, 11, 1)
            november += dt.timedelta(days=(6-november.weekday()) % 7)
            if parsed.date() == november and parsed.hour == 1:
                return None
            spring = dt.datetime.combine(march, dt.time(3))
            fall = dt.datetime.combine(november, dt.time(2))
            offset = -4 if spring <= parsed < fall else -5
            parsed = parsed.replace(tzinfo=dt.timezone(dt.timedelta(hours=offset)))
    return parsed


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (FileNotFoundError, ValueError):
        return {} if default is None else default


def bridge_roster(day):
    path = STAFFING / ("roster_units_" + str(day) + ".json")
    payload = read_json(path)
    if payload.get("date") == str(day) and payload.get("complete") is True and payload.get("operational_only") is True:
        return payload
    return None


def available_dates():
    return sorted(path.stem.removeprefix("roster_units_") for path in STAFFING.glob("roster_units_????-??-??.json"))


def _signature(path):
    try:
        stat = path.stat()
        return str(path.resolve()), stat.st_mtime_ns, stat.st_size
    except FileNotFoundError:
        return str(path.resolve()), 0, 0


def shift_result(stats_path, day):
    previous = day - dt.timedelta(days=1)
    signatures = [_signature(Path(stats_path))]
    signatures.extend(_signature(STAFFING / ("roster_units_" + str(d) + ".json")) for d in (previous, day))
    return _cached_result(str(day), tuple(signatures))


@lru_cache(maxsize=800)
def _cached_result(day_text, signatures):
    day = dt.date.fromisoformat(day_text)
    stats = read_json(signatures[0][0])
    rosters = [read_json(signature[0]) for signature in signatures[1:]]
    return attribute(day, stats, rosters)


def attribute(day, stats, rosters):
    entries = []
    names = {}
    covered = set()
    for payload in rosters:
        if not payload or not payload.get("complete") or not payload.get("operational_only"):
            continue
        covered.add(payload.get("date"))
        for unit in payload.get("units", []):
            code = normalize_unit(unit.get("unit_code"))
            for entry in unit.get("entries", []):
                start, end = timestamp(entry.get("start")), timestamp(entry.get("end"))
                pid = str(entry.get("id") or "")
                if not pid or not start or not end or end <= start:
                    continue
                if entry.get("status") not in {None, "", "APPROVED", "NULL_STATUS"}:
                    continue
                names[pid] = entry.get("name") or pid
                entries.append({**entry, "id": pid, "unit": code, "begin": start, "finish": end})
    events = stats.get("call_events") or {}
    intervals = stats.get("call_intervals") or {}
    if not isinstance(events, dict):
        events = {}
    if not isinstance(intervals, dict):
        intervals = {}
    counted = set(str(value) for value in stats.get("counted_calls", []))
    keys = counted if counted else set(events) | set(intervals)
    by_person = defaultdict(dict)
    unassigned = []
    records = []
    known_unit_keys = set()
    for key in sorted(keys):
        if "|" not in key:
            continue
        incident, raw_unit = key.rsplit("|", 1)
        unit = normalize_unit(raw_unit)
        event = events.get(key) or {}
        event = event if isinstance(event, dict) else {"timestamp": event}
        interval = intervals.get(key) or {}
        scope = str(interval.get("duration_scope") or "").lower().replace("-", "_")
        unit_duration = bool(interval) and not interval.get("unit_duration_unavailable") and scope not in {"incident", "incident_wide"} and interval.get("source") != "incident_csv_involved"
        start = timestamp((interval.get("start") if unit_duration else None) or event.get("timestamp"))
        if start is None:
            unassigned.append({"incident_id": incident, "unit": unit, "reason": "Missing or ambiguous call start"})
            continue
        known_unit_keys.add((incident, unit))
        end = timestamp(interval.get("end")) if unit_duration else None
        if end and end < start:
            end = None
        call = {
            "incident_id": incident, "unit": unit, "date": str(day), "start": start.isoformat(),
            "end": end.isoformat() if end else None,
            "duration_sec": round((end-start).total_seconds(), 1) if end else None,
            "duration_known": end is not None, "ongoing": bool(interval.get("ongoing")),
            "type": event.get("type") or event.get("call_type") or event.get("Type") or None,
            "location": event.get("location") or event.get("Location") or None,
            "source": interval.get("source") or event.get("source") or "retained_call_ledger",
        }
        active = [entry for entry in entries if entry["begin"] <= start < entry["finish"]]
        crew = {}
        conflicting = []
        for entry in active:
            if entry["unit"] != unit:
                continue
            pid = entry["id"]
            # Multiple simultaneous real apparatus assignments are not resolved
            # by whichever unit happens to have the most calls that day.
            if len({row["unit"] for row in active if row["id"] == pid}) > 1:
                conflicting.append(pid)
                continue
            crew.setdefault(pid, entry)
        call["crew"] = [{"id": pid, "name": names[pid], "position": entry.get("rank") or ""}
                        for pid, entry in sorted(crew.items())]
        if not crew or conflicting:
            unassigned.append({"incident_id": incident, "unit": unit, "start": call["start"],
                               "reason": "Conflicting unit assignments" if conflicting else "No matching operational roster interval",
                               "conflicting_people": conflicting})
        records.append(call)
        for pid, entry in crew.items():
            person_call = by_person[pid].setdefault(incident, {
                **{k: v for k, v in call.items() if k != "crew"}, "units": [], "assignments": [],
                "duration_intervals": [], "attribution": "roster_at_unit_dispatch",
            })
            if unit not in person_call["units"]:
                person_call["units"].append(unit)
                person_call["assignments"].append({"unit": unit, "position": entry.get("rank") or "",
                                                    "from": entry["start"], "through": entry["end"]})
            if start < timestamp(person_call["start"]):
                person_call["start"] = call["start"]
            if end:
                right = min(end, entry["finish"])
                person_call["duration_intervals"].append((start.isoformat(), right.isoformat()))
    for pid, calls in by_person.items():
        for call in calls.values():
            ranges = [(timestamp(left), timestamp(right)) for left, right in call.pop("duration_intervals")]
            call["duration_sec"] = merge_seconds(ranges) if ranges else None
            call["duration_known"] = bool(ranges)
            call["units"].sort()
    people = {}
    begin = timestamp(str(day) + "T07:00:00")
    finish = timestamp(str(day + dt.timedelta(days=1)) + "T07:00:00")
    for pid, name in names.items():
        work = [(max(begin, entry["begin"]), min(finish, entry["finish"])) for entry in entries
                if entry["id"] == pid and entry["finish"] > begin and entry["begin"] < finish]
        calls = sorted(by_person.get(pid, {}).values(), key=lambda call: (call["start"], call["incident_id"]), reverse=True)
        known = [call["duration_sec"] for call in calls if call["duration_known"]]
        people[pid] = {"name": name, "calls": calls, "count": len(calls), "dur_sec": sum(known),
                       "duration_known_calls": len(known), "max_sec": max(known, default=0),
                       "after_0000": sum(timestamp(call["start"]).hour < 7 for call in calls),
                       "hours_worked": merge_seconds(work)/3600}
    total_calls = sum(int(value or 0) for value in (stats.get("calls") or {}).values())
    return {"people": people, "records": records,
            "coverage": {"date": str(day), "roster_dates": sorted(covered), "unit_calls": total_calls,
                         "timed_unit_calls": len(known_unit_keys), "attributed_unit_calls": sum(bool(c["crew"]) for c in records),
                         "unattributed": unassigned, "missing_call_records": max(0, total_calls-len(known_unit_keys)),
                         "roster_complete": {str(day-dt.timedelta(days=1)), str(day)} <= covered}}


def merge_seconds(ranges):
    merged = []
    for left, right in sorted(ranges):
        if right <= left:
            continue
        if merged and left <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], right)
        else:
            merged.append([left, right])
    return sum((right.astimezone(UTC)-left.astimezone(UTC)).total_seconds() for left, right in merged)


def stats_tuple(result):
    people = result["people"]
    return tuple({pid: info[field] for pid, info in people.items()} for field in
                 ("name", "count", "dur_sec", "after_0000", "max_sec"))


def build_history(stats_dir, start, end):
    people = {}
    coverage = []
    day = start
    while day <= end:
        path = Path(stats_dir) / ("shift_stats_" + str(day) + ".json")
        if path.exists():
            result = shift_result(path, day)
            coverage.append(result["coverage"])
            for pid, info in result["people"].items():
                person = people.setdefault(pid, {"name": info["name"], "calls": [], "shifts": []})
                person["name"] = info["name"]
                person["calls"].extend(info["calls"])
                person["shifts"].append({"date": str(day), "hours_worked": info["hours_worked"]})
        else:
            coverage.append({"date": str(day), "missing_stats": True, "roster_complete": bridge_roster(day) is not None,
                             "unit_calls": None, "timed_unit_calls": 0, "attributed_unit_calls": 0})
        day += dt.timedelta(days=1)
    for info in people.values():
        # Stable incident identity prevents repeated backfills or a duplicate
        # dispatch record in two shift files from inflating a person's count.
        unique = {}
        for call in sorted(info["calls"], key=lambda value: value["start"]):
            unique.setdefault(call["incident_id"], call)
        info["calls"] = sorted(unique.values(), key=lambda call: call["start"], reverse=True)
    return {"schema_version": 1, "from": str(start), "through": str(end), "people": people,
            "coverage": coverage, "attribution": "Exact retained call start matched to operational staffing interval",
            "missing_fields": "Call type, location, or clear time are unavailable when not retained by the incident collector."}


def backfill_start():
    manifest = read_json(STAFFING / "index.json")
    return dt.date.fromisoformat(manifest.get("backfill_from") or "2026-09-05")


def roster_for_path(path):
    match = re.fullmatch(r"roster_units_(\d{4}-\d{2}-\d{2})\.json", Path(path).name)
    return bridge_roster(match[1]) if match else None


def apply_exact_history(payload, stats_dir, now, output):
    if not (STAFFING / "index.json").exists():
        return
    current = now.date() if now.hour >= 7 else now.date()-dt.timedelta(days=1)
    start = backfill_start()
    history = build_history(stats_dir, start, current)
    path = Path(output).parent / "personnel_calls.json"
    path.write_text(json.dumps(history, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    coverage = history["coverage"]
    summary = {"from": str(start), "through": str(current), "file": "personnel_calls.json",
               "days": len(coverage), "missing_stats_dates": [row["date"] for row in coverage if row.get("missing_stats")],
               "missing_roster_dates": [row["date"] for row in coverage if not row.get("roster_complete")],
               "missing_call_records": sum(row.get("missing_call_records", 0) for row in coverage),
               "unattributed_unit_calls": sum(len(row.get("unattributed", [])) for row in coverage),
               "timed_unit_calls": sum(row.get("timed_unit_calls", 0) for row in coverage),
               "personnel_calls": sum(len(info["calls"]) for info in history["people"].values()),
               "people": len(history["people"]), "method": "Exact roster-at-dispatch match; unknowns are not estimated"}
    summary["complete"] = not (summary["missing_stats_dates"] or summary["missing_roster_dates"] or summary["missing_call_records"] or summary["unattributed_unit_calls"])
    payload["personnel_history"] = summary
    payload["personnel"]["backfill"] = history_period(history, start, current)
    for period, days in (("week", 7), ("month", 30)):
        period_start = current-dt.timedelta(days=days-1)
        if period_start >= start:
            exact = history_period(history, period_start, current)
            exact["period"] = period
            exact["label"] = period.title()
            exact["meta"]["partial"] = any(row.get("missing_stats") or not row.get("roster_complete") or row.get("missing_call_records") or row.get("unattributed")
                                              for row in coverage if str(period_start) <= row["date"] <= str(current))
            payload["personnel"][period] = exact
    for period in ("year", "rolling_year"):
        if period in payload.get("personnel", {}):
            legacy = payload.get("details", {}).get("personnel", {}).get(period, {})
            exact_rows = {row["person_id"]: dict(row) for row in history_period(history, start, current)["rows"]}
            year_start = dt.date(current.year, 1, 1) if period == "year" else current-dt.timedelta(days=364)
            for pid, old in legacy.items():
                shifts = [row for row in old.get("shifts", []) if str(year_start) <= row.get("date", "") < str(start)]
                if not shifts:
                    continue
                row = exact_rows.setdefault(pid, {"person_id": pid, "name": old.get("name") or pid,
                                                 "total_calls": 0, "total_hours_worked": 0, "ride_in_count": 0,
                                                 "single_shift_max_calls": 0, "total_calls_after_midnight": 0,
                                                 "single_shift_max_after_midnight": 0, "total_call_hours": 0,
                                                 "highest_call_duration_mins": None, "avg_call_duration_mins": None})
                legacy_calls = sum(int(shift.get("calls") or 0) for shift in shifts)
                legacy_seconds = sum(float(shift.get("avg_min") or 0)*int(shift.get("calls") or 0)*60 for shift in shifts)
                row["total_calls"] += legacy_calls
                row["total_hours_worked"] = round((row.get("total_hours_worked") or 0) + sum(float(shift.get("hours_worked") or 0) for shift in shifts), 1)
                row["total_call_hours"] = round((row.get("total_call_hours") or 0) + legacy_seconds/3600, 1)
                row["ride_in_count"] = (row.get("ride_in_count") or 0) + sum(int(shift.get("ride_in_count") or 0) for shift in shifts)
                row["single_shift_max_calls"] = max([row.get("single_shift_max_calls") or 0] + [int(shift.get("calls") or 0) for shift in shifts])
                row["total_calls_after_midnight"] += sum(int(shift.get("after_0000") or 0) for shift in shifts)
                row["single_shift_max_after_midnight"] = max([row.get("single_shift_max_after_midnight") or 0] + [int(shift.get("after_0000") or 0) for shift in shifts])
                row["highest_call_duration_mins"] = max([row.get("highest_call_duration_mins") or 0] + [float(shift.get("max_min") or 0) for shift in shifts])
                row["avg_calls_per_hour"] = round(row["total_calls"]/row["total_hours_worked"], 2) if row["total_hours_worked"] else None
                row["avg_call_duration_mins"] = round(row["total_call_hours"]*60/row["total_calls"], 1) if row["total_calls"] else None
                row["delta_calls"] = None
            payload["personnel"][period]["rows"] = sorted(exact_rows.values(), key=lambda row: (-row["total_calls"], row["name"]))
            payload["personnel"][period].setdefault("meta", {})["counting_note"] = "Exact incident matching since " + str(start) + "; older totals retain legacy shift-based estimates."
            payload["personnel"][period]["meta"]["partial"] = not summary["complete"]
    payload["backfill_health"]["exact_personnel"] = summary
    health = payload["backfill_health"]
    health["last_backfill_status"] = {"status": "complete" if summary["complete"] else "partial",
                                      "message": "Historical staffing recovered; incident-level coverage is reported separately.",
                                      "updated_at": dt.datetime.now(UTC).isoformat()}


def display_roster(day):
    """Show only the selected roster date's crew, including split assignments.

    Call attribution separately uses adjacent dates to match overnight calls;
    those carryover assignments must not be added to this day's unit popup.
    """
    roster = bridge_roster(day)
    if not roster:
        return None
    units = defaultdict(list)
    for unit in roster["units"]:
        for entry in unit.get("entries", []):
            left, right = timestamp(entry.get("start")), timestamp(entry.get("end"))
            if left and right and right > left:
                units[normalize_unit(unit["unit_code"])].append(entry)
    if not units:
        return None
    return {"date": str(day), "source": "telestaff-authorized-full-roster", "timezone": "America/New_York",
            "units": [{"unit_code": code, "unit_name": code, "shift": "Fire Department / Suppression", "entries": entries}
                      for code, entries in sorted(units.items())]}


def history_period(history, start, end):
    rows = []
    for pid, info in history["people"].items():
        calls = [call for call in info["calls"] if str(start) <= call["date"] <= str(end)]
        by_shift = defaultdict(int)
        known = []
        hours = sum(row["hours_worked"] for row in info.get("shifts", []) if str(start) <= row["date"] <= str(end))
        after = defaultdict(int)
        for call in calls:
            by_shift[call["date"]] += 1
            if timestamp(call["start"]).hour < 7:
                after[call["date"]] += 1
            if call.get("duration_known"):
                known.append(call["duration_sec"])
        rows.append({"person_id": pid, "name": info["name"], "total_calls": len(calls),
                     "single_shift_max_calls": max(by_shift.values(), default=0), "avg_calls_per_hour": round(len(calls)/hours, 2) if hours else None,
                     "total_hours_worked": round(hours, 1), "ride_in_count": None,
                     "avg_call_duration_mins": round(sum(known)/len(known)/60, 1) if known else None,
                     "highest_call_duration_mins": round(max(known)/60, 1) if known else None,
                     "total_calls_after_midnight": sum(after.values()),
                     "single_shift_max_after_midnight": max(after.values(), default=0),
                     "total_call_hours": round(sum(known)/3600, 1), "delta_calls": None})
    rows.sort(key=lambda row: (-row["total_calls"], row["name"]))
    return {"label": "Since last backfill", "period": "backfill", "rows": rows,
            "meta": {"range": str(start)+" - "+str(end), "exact_incident_records": True},
            "updated": dt.datetime.now(UTC).isoformat()}

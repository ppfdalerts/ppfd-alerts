"""Backfill daily unit call timestamps from retained GroupMe notifications."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import time
from pathlib import Path

import requests


WATCH_UNITS = {
    "R33", "E33", "T33", "33FD", "LR36", "HM33", "R34", "E34", "TR34",
    "E36", "S36", "R36", "E35", "34FD", "36FD", "D35", "R35", "35FD", "E136",
}
STATS_RE = re.compile(r"shift_stats_(\d{4}-\d{2}-\d{2})\.json$")
TIME_RE = re.compile(r"(?i)\bTime:\s*([01]\d|2[0-3]):([0-5]\d)")
UNITS_RE = re.compile(r"(?im)\bUnits:\s*(.*?)(?:\s*\|\s*Time:|\s*$)")
ALERT_LOG_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+SENT \[LOG\]\s+(.*?)\s+-\s+(.*)$")
GROUPME_API = "https://api.groupme.com/v3"


def load_config(path: Path) -> tuple[str, str, dict[str, int]]:
    text = path.read_text(encoding="utf-8")
    token_match = re.search(r"(?mi)^\s*User Token\s*=\s*([A-Za-z0-9_-]+)\s*$", text)
    group_match = re.search(r"(?mi)^\s*Group ID\s*=\s*(\d+)\s*$", text)
    if not token_match or not group_match:
        raise RuntimeError("Groupmetokens.txt must contain User Token and Group ID")
    topics: dict[str, int] = {}
    for match in re.finditer(
        r"(?mi)^\s*([A-Za-z0-9_]+)\s*=\s*https?://api\.groupme\.com/v3/groups/(\d+)/subgroups/(\d+)\s*$",
        text,
    ):
        topics[match.group(1).strip().upper()] = int(match.group(3))
    try:
        start = text.find("[")
        end = text.rfind("]")
        if start >= 0 and end > start:
            values = json.loads(text[start:end + 1])
            for value in values:
                name = str(value.get("topic") or "").strip().upper()
                topic_id = value.get("id")
                if name and topic_id is not None:
                    topics[name] = int(topic_id)
    except Exception:
        pass
    return token_match.group(1), group_match.group(1), topics


def response_messages(response: requests.Response) -> list[dict]:
    response.raise_for_status()
    payload = response.json()
    body = payload.get("response") if isinstance(payload, dict) else None
    if isinstance(body, dict) and isinstance(body.get("messages"), list):
        return [m for m in body["messages"] if isinstance(m, dict)]
    if isinstance(body, list):
        return [m for m in body if isinstance(m, dict)]
    return []


def fetch_channel(session: requests.Session, token: str, paths: list[str], label: str) -> list[dict]:
    for path in paths:
        messages: list[dict] = []
        before_id: str | None = None
        seen_pages: set[str] = set()
        try:
            while True:
                params = {"token": token, "limit": 100}
                if before_id:
                    params["before_id"] = before_id
                response = session.get(GROUPME_API + path, params=params, timeout=30)
                page = response_messages(response)
                if not page:
                    break
                page_key = str(page[-1].get("id") or "")
                if not page_key or page_key in seen_pages:
                    break
                seen_pages.add(page_key)
                messages.extend(page)
                if len(page) < 100:
                    break
                before_id = page_key
                time.sleep(1.0)
            return messages
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "HTTP error"
            print(f"WARN: {label} stopped at HTTP {status}; retaining {len(messages)} downloaded messages")
            if messages:
                return messages
        except Exception as exc:
            print(f"WARN: {label} failed with {type(exc).__name__}; retaining {len(messages)} downloaded messages")
            if messages:
                return messages
    print(f"WARN: no readable messages for {label}")
    return []


def parse_message_time(message: dict) -> tuple[dt.datetime, str] | None:
    text = str(message.get("text") or "")
    created = message.get("created_at")
    try:
        created_dt = dt.datetime.fromtimestamp(float(created), tz=dt.timezone.utc).astimezone()
    except Exception:
        return None
    time_match = TIME_RE.search(text)
    if time_match:
        hour, minute = int(time_match.group(1)), int(time_match.group(2))
        call_dt = created_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
        # Notifications sent after midnight normally describe the same calendar
        # date's call. If the send is just after 07:00 for a pre-07:00 call, keep
        # the previous calendar date instead of moving it to a new shift.
        if call_dt > created_dt + dt.timedelta(hours=2):
            call_dt -= dt.timedelta(days=1)
        return call_dt, f"{hour:02d}:{minute:02d}"
    return created_dt, created_dt.strftime("%H:%M")


def shift_date_for(call_dt: dt.datetime) -> dt.date:
    return call_dt.date() if call_dt.hour >= 7 else call_dt.date() - dt.timedelta(days=1)


def message_units(text: str, channel_unit: str | None) -> list[str]:
    match = UNITS_RE.search(text)
    if match:
        units = [part.strip().upper() for part in match.group(1).split(",")]
        units = [unit for unit in units if unit in WATCH_UNITS]
        if units:
            return sorted(set(units))
    return [channel_unit] if channel_unit in WATCH_UNITS else []


def load_alert_log_messages(path: Path) -> list[dict]:
    messages = []
    if not path.exists():
        return messages
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = ALERT_LOG_RE.match(line.rstrip())
            if not match:
                continue
            try:
                created = dt.datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S").astimezone().timestamp()
            except Exception:
                continue
            messages.append({"text": match.group(2) + "\n" + match.group(3), "created_at": created})
    return messages


def list_stat_dates(stats_dir: Path) -> list[dt.date]:
    dates = []
    for path in stats_dir.glob("shift_stats_*.json"):
        match = STATS_RE.fullmatch(path.name)
        if match:
            dates.append(dt.date.fromisoformat(match.group(1)))
    return sorted(dates)


def load_event_ledger(path: Path) -> tuple[dict[str, list[str]], dict[str, dict]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    counted_by_unit: dict[str, list[str]] = {}
    for raw_key in payload.get("counted_calls") or []:
        key = str(raw_key)
        if "|" not in key:
            continue
        incident_id, unit = key.rsplit("|", 1)
        if unit in WATCH_UNITS and incident_id:
            counted_by_unit.setdefault(unit, []).append(incident_id)
    for unit in counted_by_unit:
        counted_by_unit[unit] = sorted(set(counted_by_unit[unit]), key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value))
    raw_events = payload.get("call_events", {})
    events = dict(raw_events) if isinstance(raw_events, dict) else {}
    return counted_by_unit, events


def event_timestamp(value) -> str:
    if isinstance(value, dict):
        return str(value.get("timestamp") or "")
    return str(value or "")


def minute_key(value: str) -> str:
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed.strftime("%Y-%m-%dT%H:%M")
    except Exception:
        return str(value)[:16]


def save_call_events(path: Path, call_events: dict[str, dict]) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["call_events"] = dict(sorted(call_events.items()))
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tokens", default="Groupmetokens.txt")
    parser.add_argument("--stats-dir", default="data/shift_stats")
    parser.add_argument("--start", help="First shift date to import, YYYY-MM-DD")
    parser.add_argument("--end", help="Last shift date to import, YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--alerts-log", default="alerts.log", help="Optional local SENT [LOG] history to supplement GroupMe")
    parser.add_argument("--include-topics", action="store_true", help="Also read each unit topic; slower and more rate-limit sensitive")
    parser.add_argument("--no-groupme", action="store_true", help="Use only the local alert log")
    args = parser.parse_args()

    stats_dir = Path(args.stats_dir)
    dates = list_stat_dates(stats_dir)
    if not dates:
        raise RuntimeError(f"No shift stats found in {stats_dir}")
    start = dt.date.fromisoformat(args.start) if args.start else dates[0]
    end = dt.date.fromisoformat(args.end) if args.end else dates[-1]
    session = requests.Session()
    all_records: dict[tuple[str, str, str, str], str] = {}
    channels = []
    topics: dict[str, int] = {}
    if not args.no_groupme:
        token, group_id, topics = load_config(Path(args.tokens))
        channels = [("main", None, [f"/groups/{group_id}/messages"])]
    if args.include_topics and not args.no_groupme:
        for unit in sorted(WATCH_UNITS):
            sid = topics.get(unit)
            if sid:
                channels.append((unit, unit, [f"/groups/{sid}/messages", f"/groups/{group_id}/subgroups/{sid}/messages"]))

    def consider_message(message: dict, channel_unit: str | None) -> None:
        text = str(message.get("text") or "")
        if re.search(r"(?im)^\s*Changed unit:", text):
            return
        # Only original call alerts contain the dispatch Time field. Attachment,
        # removal, and status-change notifications must not become extra calls.
        if not TIME_RE.search(text):
            return
        parsed = parse_message_time(message)
        if not parsed:
            return
        call_dt, hhmm = parsed
        shift_date = shift_date_for(call_dt)
        if shift_date < start or shift_date > end:
            return
        units = message_units(text, channel_unit)
        if not units:
            return
        signature_text = re.sub(r"(?im)^\s*Units:.*$", "Units:", text.lower())
        # Existing alert bodies have no incident number. Use the stable
        # notification content to collapse main/topic duplicates.
        location_key = re.sub(r"\s+", " ", signature_text).strip()
        for unit in units:
            key = (unit, shift_date.isoformat(), hhmm, location_key)
            all_records.setdefault(key, call_dt.isoformat())

    for label, channel_unit, paths in channels:
        for message in fetch_channel(session, token, paths, label):
            consider_message(message, channel_unit)
    for message in load_alert_log_messages(Path(args.alerts_log)):
        consider_message(message, None)

    records_by_day_unit: dict[tuple[str, str], list[str]] = {}
    for (unit, shift_date, _hhmm, _signature), timestamp in all_records.items():
        records_by_day_unit.setdefault((shift_date, unit), []).append(timestamp)

    added_by_date: dict[str, int] = {}
    unmatched_by_date: dict[str, int] = {}
    for day in dates:
        if day < start or day > end:
            continue
        path = stats_dir / f"shift_stats_{day:%Y-%m-%d}.json"
        counted_by_unit, call_events = load_event_ledger(path)
        before = len(call_events)
        unmatched = 0
        for unit, incident_ids in counted_by_unit.items():
            candidates = sorted(set(records_by_day_unit.get((day.isoformat(), unit), [])))
            existing_keys = {f"{incident_id}|{unit}" for incident_id in incident_ids if f"{incident_id}|{unit}" in call_events}
            existing_minutes = {minute_key(event_timestamp(call_events[key])) for key in existing_keys}
            candidates = [timestamp for timestamp in candidates if minute_key(timestamp) not in existing_minutes]
            remaining_ids = [incident_id for incident_id in incident_ids if f"{incident_id}|{unit}" not in call_events]
            for incident_id, timestamp in zip(remaining_ids, candidates):
                call_events[f"{incident_id}|{unit}"] = {
                    "timestamp": timestamp,
                    "source": "alert_history_inferred",
                }
            unmatched += max(0, len(remaining_ids) - len(candidates))
        added = len(call_events) - before
        if added and not args.dry_run:
            save_call_events(path, call_events)
        if added:
            added_by_date[day.isoformat()] = added
        if unmatched:
            unmatched_by_date[day.isoformat()] = unmatched

    print(json.dumps({
        "channels": len(channels),
        "notification_records": len(all_records),
        "events_added_by_date": added_by_date,
        "calls_without_retained_timestamp_by_date": unmatched_by_date,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

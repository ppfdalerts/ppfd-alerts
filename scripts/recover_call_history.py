"""Recover retained public call timelines from the site's GitHub history.

Each snapshot must explicitly identify the requested shift date. A daily total
without an incident timeline is retained as a coverage gap, not fabricated calls.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import json
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

import httpx
import personnel_ledger as ledger


def period_day(period):
    meta = period.get("meta") or {}
    if meta.get("shift_start"):
        return dt.date.fromisoformat(meta["shift_start"][:10])
    match = re.search(r"(\d{2}/\d{2}/\d{2,4})", meta.get("shift_date") or "")
    if match:
        return dt.datetime.strptime(match[1], "%m/%d/%Y" if len(match[1]) == 10 else "%m/%d/%y").date()
    return None


def extract_period(period, day):
    if period_day(period) != day:
        return None
    result = {key: {} for key in ("calls", "dur_sec", "max_sec", "after_0000", "ride_in_count", "call_events", "call_intervals")}
    counted = set()
    for row in period.get("rows", []):
        unit = ledger.normalize_unit(row.get("unit"))
        result["calls"][unit] = int(row.get("calls") or 0)
        result["after_0000"][unit] = int(row.get("after_0000") or 0)
        result["ride_in_count"][unit] = row.get("ride_in_count")
        for call in row.get("call_timeline", []):
            if not call.get("incident_id") or not call.get("start"):
                continue
            key = str(call["incident_id"]) + "|" + unit
            counted.add(key)
            result["call_events"][key] = {"timestamp": call["start"], "source": call.get("source") or "public_github_snapshot",
                                           "type": call.get("type"), "location": call.get("location")}
            if call.get("duration_known") and not call.get("unit_duration_unavailable") and call.get("end"):
                result["call_intervals"][key] = {"start": call["start"], "end": call["end"],
                                                   "duration_sec": call.get("duration_sec"), "duration_scope": "unit",
                                                   "source": call.get("source") or "public_github_snapshot",
                                                   "ongoing": bool(call.get("ongoing"))}
    result["counted_calls"] = sorted(counted)
    result["recovery"] = {"date": str(day), "source": "published_github_timeline", "incident_records": len(counted),
                           "unit_calls": sum(result["calls"].values())}
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--calendar-project", type=Path, required=True)
    parser.add_argument("--from-date", type=dt.date.fromisoformat, required=True)
    parser.add_argument("--through-date", type=dt.date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, default=Path("docs/call_archive"))
    args = parser.parse_args()
    sys.path.insert(0, str(args.calendar_project / "src"))
    from roster_sync.config import load_settings
    from roster_sync.github_pages import GitHubClient
    api = GitHubClient(load_settings(args.calendar_project / "config.local.json"))
    args.output.mkdir(parents=True, exist_ok=True)

    def recover(day):
        target = args.output / ("shift_stats_" + str(day) + ".json")
        if target.exists():
            return {"date": str(day), "cached": True}
        cutoff = dt.datetime.combine(day+dt.timedelta(days=1), dt.time(8), ZoneInfo("America/New_York"))
        commits = api.call("GET", "commits", params={"path": "docs/data.json", "until": cutoff.astimezone(dt.timezone.utc).isoformat(), "per_page": 1})
        if not commits:
            return {"date": str(day), "missing_snapshot": True}
        commit = commits[0]
        url = "https://raw.githubusercontent.com/ppfdalerts/ppfd-alerts/" + commit["sha"] + "/docs/data.json"
        response = httpx.get(url, timeout=60)
        response.raise_for_status()
        payload = response.json()
        result = next((data for key in ("prior", "today") if (data := extract_period(payload.get(key) or {}, day)) is not None), None)
        if result is None:
            return {"date": str(day), "missing_snapshot": True, "last_publication": commit["commit"]["committer"]["date"]}
        result["recovery"].update(commit=commit["sha"], publication=commit["commit"]["committer"]["date"])
        target.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        return result["recovery"]

    days = [args.from_date+dt.timedelta(days=i) for i in range((args.through_date-args.from_date).days+1)]
    outcomes = []
    try:
        with ThreadPoolExecutor(max_workers=5) as pool:
            pending = {pool.submit(recover, day): day for day in days}
            for future in as_completed(pending):
                try:
                    result = future.result()
                except Exception as error:
                    result = {"date": str(pending[future]), "error": type(error).__name__}
                outcomes.append(result)
                print(json.dumps(result), flush=True)
    finally:
        api.close()
    manifest = {"from": str(args.from_date), "through": str(args.through_date), "dates": sorted(outcomes, key=lambda item: item["date"])}
    (args.output / "recovery.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

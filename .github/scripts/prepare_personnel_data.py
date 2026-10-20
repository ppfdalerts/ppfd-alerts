"""Build-only staffing/call join, independent of the legacy publisher's host.

It leaves the unit leaderboards and calendar files alone. Only public, retained
incident records and the explicit operational staffing allowlist are joined.
"""
import datetime as dt
import json
from pathlib import Path
import re
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import personnel_ledger as ledger


def live_shift(period):
    meta = period.get("meta") or {}
    if meta.get("shift_start"):
        day = meta["shift_start"][:10]
    else:
        match = re.search(r"(\d{2}/\d{2}/\d{2})", meta.get("shift_date") or "")
        if not match:
            return None, None
        day = str(dt.datetime.strptime(match[1], "%m/%d/%y").date())
    result = {"calls": {}, "call_events": {}, "call_intervals": {}, "counted_calls": []}
    for row in period.get("rows", []):
        unit = ledger.normalize_unit(row.get("unit"))
        result["calls"][unit] = int(row.get("calls") or 0)
        for call in row.get("call_timeline", []):
            if not call.get("incident_id") or not call.get("start"):
                continue
            key = str(call["incident_id"]) + "|" + unit
            result["call_events"][key] = {"timestamp": call["start"], "source": call.get("source"),
                                          "type": call.get("type"), "location": call.get("location")}
            if call.get("duration_known") and not call.get("unit_duration_unavailable") and call.get("end"):
                result["call_intervals"][key] = {"start": call["start"], "end": call["end"], "duration_scope": "unit", "source": call.get("source"), "ongoing": call.get("ongoing", False)}
    result["counted_calls"] = sorted(result["call_events"])
    return day, result


def prepare_data(root=Path("docs")):
    root = Path(root)
    if not (root / "staffing/index.json").exists():
        return {"skipped": "staffing_not_published"}
    ledger.STAFFING = root.resolve() / "staffing"
    payload = ledger.read_json(root / "data.json")
    day, live = live_shift(payload.get("today") or {})
    if day is None:
        raise ValueError("Live leaderboard shift date is not established")
    current = dt.date.fromisoformat(day)
    daily = {}
    # Recover unit totals from the current source, not the empty placeholder
    # shift_stats files tracked in the legacy repository.
    for period in ("rolling_year", "year", "month", "week"):
        for unit, rows in payload.get("details", {}).get("units", {}).get(period, {}).items():
            for row in rows:
                date = row.get("date")
                if not date:
                    continue
                record = daily.setdefault(date, {"calls": {}, "call_events": {}, "call_intervals": {}, "counted_calls": []})
                record["calls"][ledger.normalize_unit(unit)] = int(row.get("calls") or 0)
    for path in (root / "call_archive").glob("shift_stats_????-??-??.json"):
        date = path.stem.removeprefix("shift_stats_")
        recovered = ledger.read_json(path)
        if not recovered.get("recovery") or recovered["recovery"].get("date") != date:
            raise ValueError("Unverified call archive refused")
        current_totals = daily.get(date, {}).get("calls")
        daily[date] = recovered
        if current_totals:
            daily[date]["calls"] = current_totals
    for period in ("prior", "today"):
        date, record = live_shift(payload.get(period) or {})
        if date:
            publication = payload.get(period, {}).get("updated") or payload.get("today", {}).get("updated")
            prior_publication = daily.get(date, {}).get("recovery", {}).get("publication")
            if not prior_publication or not publication or ledger.timestamp(publication) >= ledger.timestamp(prior_publication):
                record["recovery"] = {"date": date, "source": "live_public_timeline", "publication": publication,
                                       "unit_calls": sum(record["calls"].values()), "incident_records": len(record["call_events"])}
                daily[date] = record
                archive = root / "call_archive" / ("shift_stats_" + date + ".json")
                archive.parent.mkdir(parents=True, exist_ok=True)
                archive.write_text(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    with tempfile.TemporaryDirectory(prefix="ppfd-personnel-") as directory:
        stats = Path(directory)
        for date, record in daily.items():
            (stats / ("shift_stats_" + date + ".json")).write_text(json.dumps(record), encoding="utf-8")
        now = dt.datetime.combine(current, dt.time(12))
        ledger.apply_exact_history(payload, stats, now, root / "data.json")
    summary = payload["personnel_history"]
    summary["built_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    summary["source_updated"] = payload.get("today", {}).get("updated")
    health = payload["backfill_health"]
    health["latest_roster_date"] = day
    health["roster_lag_days"] = 0
    health["latest_personnel_date"] = day
    health["personnel_lag_days"] = 0
    health["missing_roster_days"] = summary["missing_roster_dates"]
    health["missing_roster_days_count"] = len(summary["missing_roster_dates"])
    history = ledger.read_json(root / "personnel_calls.json")
    missing = [row["date"] for row in history["coverage"] if row.get("missing_stats") or row.get("missing_call_records") or row.get("unattributed") or not row.get("roster_complete")]
    health["missing_personnel_days"] = missing
    health["missing_personnel_days_count"] = len(missing)
    if payload.get("yearly_summary"):
        yearly = payload["yearly_summary"]
        yearly["personnel_total_calls"] = sum(row["total_calls"] for row in payload["personnel"]["year"]["rows"])
        yearly["personnel_partial"] = not summary["complete"]
        yearly["personnel_available_end"] = day
    for check in payload.get("integrity_checks", {}).get("checks", []):
        period = check.get("period")
        if period in payload["personnel"]:
            count = sum(row["total_calls"] for row in payload["personnel"][period]["rows"])
            check["personnel_total_calls"] = count
            check["difference_calls"] = count - int(check.get("unit_total_calls") or 0)
            check["personnel_to_unit_ratio"] = round(count/check["unit_total_calls"], 2) if check.get("unit_total_calls") else None
            check["personnel_partial"] = not summary["complete"]
            check["status"] = "partial" if not summary["complete"] else "ok"
    payload.setdefault("integrity_checks", {})["status"] = "partial" if not summary["complete"] else "ok"
    (root / "data.json").write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    roster = {"generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "today": ledger.display_roster(current), "prior": ledger.display_roster(current-dt.timedelta(days=1))}
    (root / "roster_units.json").write_text(json.dumps(roster, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"personnel_build": summary}, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    prepare_data()

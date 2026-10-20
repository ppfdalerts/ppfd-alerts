import datetime as dt
import importlib.util
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import personnel_ledger as ledger


DAY = dt.date(2026, 10, 20)


def roster(day=DAY, entries=None):
    return {"date": str(day), "complete": True, "operational_only": True,
            "units": [{"unit_code": unit, "entries": rows} for unit, rows in (entries or {}).items()]}


def employee(pid, start="07:30", end="19:30", day=DAY, **kwargs):
    return {"id": pid, "name": "Employee " + pid, "rank": "Firefighter", "status": "APPROVED",
            "start": f"{day}T{start}:00-04:00", "end": f"{day}T{end}:00-04:00", **kwargs}


def stats(events, intervals=None):
    counts = {}
    for key in events:
        unit = key.split("|")[1]
        counts[unit] = counts.get(unit, 0)+1
    return {"calls": counts, "call_events": {key: {"timestamp": value, "source": "test"} for key, value in events.items()},
            "counted_calls": list(events), "call_intervals": intervals or {}}


def test_split_shift_exact_boundary_and_repeat_idempotency():
    source = roster(entries={"E34": [employee("1", end="12:00"), employee("2", start="12:00")]})
    calls = stats({"a|E34": "2026-10-20T11:59:59", "b|E34": "2026-10-20T12:00:00"})
    first = ledger.attribute(DAY, calls, [source])
    assert first == ledger.attribute(DAY, calls, [source])
    assert [c["incident_id"] for c in first["people"]["1"]["calls"]] == ["a"]
    assert [c["incident_id"] for c in first["people"]["2"]["calls"]] == ["b"]


def test_unit_change_and_same_incident_count_once():
    source = roster(entries={"E34": [employee("1", end="12:00")], "R34": [employee("1", start="12:00")]})
    calls = stats({"a|E34": "2026-10-20T11:50:00", "a|R34": "2026-10-20T12:10:00", "b|R34": "2026-10-20T13:00:00"})
    result = ledger.attribute(DAY, calls, [source])["people"]["1"]
    assert result["count"] == 2
    assert next(c for c in result["calls"] if c["incident_id"] == "a")["units"] == ["E34", "R34"]
    assert result["hours_worked"] == 12


def test_overnight_crew_and_pending_not_counted():
    prior = DAY-dt.timedelta(days=1)
    prev = employee("1", day=prior, end="07:30")
    prev["end"] = f"{DAY}T07:30:00-04:00"
    source = roster(prior, {"E34": [prev]})
    current = roster(entries={"E34": [employee("2", status="APPROVAL_PENDING")]})
    result = ledger.attribute(DAY, stats({"a|E34": f"{DAY}T07:15:00", "b|E34": f"{DAY}T08:00:00"}), [source, current])
    assert result["people"]["1"]["count"] == 1
    assert result["coverage"]["roster_complete"]
    assert result["coverage"]["attributed_unit_calls"] == 1


def test_display_roster_keeps_days_separate_and_preserves_split_assignments(monkeypatch):
    prior = DAY-dt.timedelta(days=1)
    previous = employee("prior", day=prior)
    previous["end"] = f"{DAY}T07:30:00-04:00"
    early = employee("early", start="06:00", end="12:00")
    late = employee("late", start="12:00", end="07:30")
    late["end"] = f"{DAY+dt.timedelta(days=1)}T07:30:00-04:00"
    sources = {prior: roster(prior, {"E34": [previous]}),
               DAY: roster(entries={"E34": [early, late]})}
    monkeypatch.setattr(ledger, "bridge_roster", sources.get)

    today = ledger.display_roster(DAY)
    yesterday = ledger.display_roster(prior)
    assert today["date"] == str(DAY)
    assert today["units"][0]["entries"] == [early, late]
    assert yesterday["date"] == str(prior)
    assert yesterday["units"][0]["entries"] == [previous]


def test_display_roster_does_not_fall_back_to_prior_day(monkeypatch):
    prior = DAY-dt.timedelta(days=1)
    carryover = employee("prior", day=prior)
    carryover["end"] = f"{DAY}T07:30:00-04:00"
    monkeypatch.setattr(ledger, "bridge_roster", {prior: roster(prior, {"E34": [carryover]})}.get)
    assert ledger.display_roster(DAY) is None


def test_simultaneous_unit_conflict_flagged_not_guessed():
    source = roster(entries={"E34": [employee("1")], "R34": [employee("1")]})
    result = ledger.attribute(DAY, stats({"a|E34": f"{DAY}T08:00:00"}), [source])
    assert result["people"]["1"]["count"] == 0
    assert result["coverage"]["unattributed"][0]["reason"] == "Conflicting unit assignments"


def test_missing_records_no_proportional_estimate():
    source = roster(entries={"E34": [employee("1")]})
    result = ledger.attribute(DAY, {"calls": {"E34": 10}}, [source])
    assert result["people"]["1"]["count"] == 0
    assert result["coverage"]["missing_call_records"] == 10


def test_unknown_incident_wide_duration_and_metadata():
    source = roster(entries={"E34": [employee("1")]})
    data = stats({"a|E34": f"{DAY}T08:00:00"}, {"a|E34": {"start": f"{DAY}T08:00:00", "end": f"{DAY}T09:00:00", "duration_scope": "incident"}})
    data["call_events"]["a|E34"].update(type="Public call type", location="Public location")
    call = ledger.attribute(DAY, data, [source])["people"]["1"]["calls"][0]
    assert call["duration_sec"] is None
    assert call["type"] == "Public call type" and call["location"] == "Public location"


def test_only_dispatch_crew_and_work_duration_clipped_at_relief():
    source = roster(entries={"E34": [employee("1", end="12:00"), employee("2", start="12:00")]})
    calls = stats({"a|E34": f"{DAY}T11:50:00"}, {"a|E34": {"start": f"{DAY}T11:50:00", "end": f"{DAY}T12:20:00", "duration_scope": "unit"}})
    result = ledger.attribute(DAY, calls, [source])["people"]
    assert result["1"]["count"] == 1 and result["2"]["count"] == 0
    assert result["1"]["dur_sec"] == 600


def test_naive_ambiguous_dst_fails_closed():
    assert ledger.timestamp("2026-11-01T01:30:00") is None
    assert ledger.timestamp("2026-11-01T01:30:00-04:00") is not None
    assert ledger.timestamp("2026-11-01T01:30:00-05:00") is not None

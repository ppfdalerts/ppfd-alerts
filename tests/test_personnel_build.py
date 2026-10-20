import datetime as dt
import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / ".github/scripts"))
import personnel_ledger as ledger
import prepare_personnel_data as build
import pytest


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_full_story_exact_totals_history_roster_and_idempotency(tmp_path, monkeypatch):
    monkeypatch.setattr(ledger, "STAFFING", tmp_path / "staffing")
    write(tmp_path / "staffing/index.json", {"backfill_from": "2026-10-19"})
    for day in ("2026-10-18", "2026-10-19", "2026-10-20"):
        next_day = dt.date.fromisoformat(day)+dt.timedelta(days=1)
        write(tmp_path / f"staffing/roster_units_{day}.json", {
            "date": day, "complete": True, "operational_only": True,
            "units": [{"unit_code": "E34", "entries": [{"id": "1", "name": "Test, Firefighter", "rank": "Engine Operator",
                       "start": day+"T07:30:00-04:00", "end": str(next_day)+"T07:30:00-04:00", "status": "APPROVED"}]}]})
    def period(day, incident):
        return {"meta": {"shift_start": day+"T07:00:00"}, "updated": "2026-10-20T19:00:00Z",
                "rows": [{"unit": "E34", "calls": 1, "call_timeline": [{"incident_id": incident,
                           "start": day+"T08:00:00", "end": day+"T08:20:00", "duration_known": True}]}]}
    payload = {"today": period("2026-10-20", "b"), "prior": period("2026-10-19", "a"),
               "personnel": {key: {"rows": [], "meta": {}} for key in ("week", "month", "year", "rolling_year")},
               "backfill_health": {}, "details": {"units": {}, "personnel": {"year": {"1": {
                   "name": "Test, Firefighter", "shifts": [{"date": "2026-09-04", "calls": 9, "hours_worked": 24}]}}}}}
    write(tmp_path / "data.json", payload)
    build.prepare_data(tmp_path)
    first = json.loads((tmp_path / "data.json").read_text())
    assert first["today"] == payload["today"] and first["prior"] == payload["prior"]
    assert first["personnel"]["backfill"]["rows"][0]["total_calls"] == 2
    assert first["personnel"]["year"]["rows"][0]["total_calls"] == 11
    history = json.loads((tmp_path / "personnel_calls.json").read_text())
    assert {call["incident_id"] for call in history["people"]["1"]["calls"]} == {"a", "b"}
    roster = json.loads((tmp_path / "roster_units.json").read_text())
    for period, day in (("today", "2026-10-20"), ("prior", "2026-10-19")):
        assert roster[period]["date"] == day
        entries = roster[period]["units"][0]["entries"]
        assert len(entries) == 1
        assert entries[0]["id"] == "1"
        assert entries[0]["start"].startswith(day)
    build.prepare_data(tmp_path)
    second = json.loads((tmp_path / "data.json").read_text())
    assert second["personnel"]["backfill"]["rows"] == first["personnel"]["backfill"]["rows"]
    assert second["personnel"]["year"]["rows"] == first["personnel"]["year"]["rows"]


def test_ui_overlay_is_idempotent():
    import importlib.util
    path = ROOT / ".github/scripts/prepare-calendar-site.py"
    spec = importlib.util.spec_from_file_location("calendar_site", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    html = (ROOT / "docs/index.html").read_text(encoding="utf-8-sig")
    first = module.personnel_ui(html)
    assert module.personnel_ui(first) == first
    assert first.count('data-ppfd-personnel-bridge') == 1
    assert first.count('value="backfill"') == 1
    assert first.count('data-ppfd-fit-type') == 1
    assert first.count('leaderboard-type.css?') == 1


def test_overlay_repairs_a_legacy_publisher_template():
    path = ROOT / ".github/scripts/prepare-calendar-site.py"
    spec = importlib.util.spec_from_file_location("legacy_site", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    html = '''<!doctype html><title>PPFD Live Leaderboards</title>
    <select id="personnelPeriod"></select><script>
    appState.payload = data || null;
    personnelState.data.week = personnel.week || null;
    function coversTime(entry, atMinute){
    if(atMinute==null) return true;
    var useActive=!!(el.rosterNow && el.rosterNow.checked);
    time.textContent=from+'-'+through;
    </script>'''
    result = module.personnel_ui(html)
    assert module.personnel_ui(result) == result
    assert 'Date.parse(entry.start)' in result
    assert 'time.title=entry.start' in result
    assert 'periodKey ===' in result
    assert '<meta charset="utf-8">' in result
    assert 'data-ppfd-personnel-bridge' in result
    assert 'leaderboard-type.js?v=' in result

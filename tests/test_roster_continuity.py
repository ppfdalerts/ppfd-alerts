import datetime as dt
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / ".github/scripts"))
import personnel_ledger as ledger
from roster_continuity import display_bundle, continuity_ui


def write(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def test_exact_saved_plan_survives_auth_pause_without_entering_call_attribution(tmp_path, monkeypatch):
    monkeypatch.setattr(ledger, "STAFFING", tmp_path / "staffing")
    current = dt.date(2026, 11, 19)
    original = "2026-11-13T05:02:06+00:00"
    write(tmp_path/"staffing/sync-status.json", {"source_state": "auth_required", "last_success": original})
    write(tmp_path/"staffing/roster-cache.json", {
        "source": "telestaff-saved-roster-plan", "collected_at": original,
        "dates": {str(current): {"date": str(current), "units": [{"unit_code": "E34", "entries": [
            {"id": "1", "name": "Example", "start": "2026-11-19T07:30:00-05:00", "end": "2026-11-20T07:30:00-05:00"}]}]}}})
    bundle = display_bundle(tmp_path, ledger, current)
    assert bundle["today"]["date"] == str(current) and bundle["today"]["saved_plan"]
    assert bundle["today"]["last_checked"] == original and bundle["prior"] is None
    assert bundle["sync"]["source_state"] == "auth_required"
    assert ledger.bridge_roster(current) is None
    assert display_bundle(tmp_path, ledger, current+dt.timedelta(days=1))["today"] is None


def test_verified_archive_takes_precedence_and_keeps_source_freshness(tmp_path, monkeypatch):
    monkeypatch.setattr(ledger, "STAFFING", tmp_path/"staffing")
    current = dt.date(2026, 11, 19)
    checked = "2026-11-19T13:11:46+00:00"
    write(tmp_path/"staffing/sync-status.json", {"source_state": "updated", "last_success": checked})
    write(tmp_path/"staffing/roster_units_2026-11-19.json", {
        "date": str(current), "complete": True, "operational_only": True,
        "units": [{"unit_code": "E34", "entries": [{"id": "1", "name": "Example",
                   "start": "2026-11-19T07:30:00-05:00", "end": "2026-11-20T07:30:00-05:00"}]}]})
    bundle = display_bundle(tmp_path, ledger, current)
    assert not bundle["today"]["saved_plan"]
    assert bundle["today"]["last_checked"] == checked
    assert ledger.bridge_roster(current) is not None


def test_build_overlay_handles_null_bundle_and_retains_loaded_roster_on_network_error():
    original = (ROOT/"docs/index.html").read_text(encoding="utf-8-sig")
    updated = continuity_ui(original)
    assert continuity_ui(updated) == updated
    assert updated.count('id="rosterSyncStatus"') == 1
    assert "Object.prototype.hasOwnProperty.call(j,'today')" in updated
    assert "Showing the last loaded roster" in updated
    assert "Saved plan · checked" in updated
    assert '        rosterState.index = { today: null, prior: null };' not in updated

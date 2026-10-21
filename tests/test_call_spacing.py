import copy
import datetime as dt
import importlib.util
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / ".github/scripts"))
import prepare_call_spacing as spacing


@pytest.mark.parametrize("hours,calls,expected", [(24,12,2), (8,4,2), (7*24,67,2.51), (360.5,83,4.34), (24,0,None), (None,3,None), (0,3,None), (-5,3,None), (24,float('nan'),None)])
def test_hours_per_call(hours, calls, expected):
    assert spacing.hours_per_call(hours,calls) == expected


def test_exact_period_exposure_not_inverse_of_rounded_rate():
    payload = {
        "today":{"updated":"2026-10-21T14:00:00Z", "meta":{"shift_start":"2026-10-21T07:00:00", "is_current_shift":True},
                 "rows":[{"calls":7,"avg_calls_per_hour":2.33}]},
        "prior":{"meta":{}, "rows":[{"calls":10,"avg_calls_per_hour":0.42}]},
        "week":{"meta":{"range":"Oct 15 - Oct 21"},"rows":[{"total_calls":67,"avg_calls_per_hour":0.4}]},
        "month":{"meta":{"range":"Sep 22 - Oct 21"},"rows":[{"total_calls":131}]},
        "year":{"meta":{"range":"2026-01-01 - 2026-10-21"},"rows":[{"total_calls":2}]},
        "personnel":{"backfill":{"rows":[{"total_hours_worked":360.5,"total_calls":83,"avg_calls_per_hour":0.23}]}}
    }
    original = copy.deepcopy(payload)
    result = spacing.add_spacing(payload)
    assert result["today"]["rows"][0][spacing.FIELD] == 0.43
    assert result["prior"]["rows"][0][spacing.FIELD] == 2.4  # Not 1 / 0.42.
    assert result["week"]["rows"][0][spacing.FIELD] == 2.51  # Not 1 / 0.4.
    assert result["month"]["meta"]["call_spacing"]["period_hours"] == 30*24
    assert result["year"]["meta"]["call_spacing"]["period_hours"] == 294*24
    assert result["personnel"]["backfill"]["rows"][0][spacing.FIELD] == 4.34
    assert result["today"]["meta"]["call_spacing"]["units"] == "hours"
    assert result["personnel"]["backfill"]["rows"][0]["avg_calls_per_hour"] == original["personnel"]["backfill"]["rows"][0]["avg_calls_per_hour"]
    assert spacing.add_spacing(copy.deepcopy(result)) == result


def test_year_boundary_and_missing_timestamps():
    assert spacing.period_hours("week", {"meta":{"range":"Dec 28 - Jan 03"}}, dt.date(2027,1,3)) == 7*24
    assert spacing.period_hours("today", {"meta":{}}, None) is None
    assert spacing.period_hours("year", {"meta":{}}, None) is None


def test_ui_all_boards_sorting_and_csv_are_converted_idempotently():
    spec = importlib.util.spec_from_file_location("calendar_spacing", ROOT / ".github/scripts/prepare-calendar-site.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    html = module.personnel_ui((ROOT / "docs/index.html").read_text(encoding="utf-8-sig"))
    assert module.personnel_ui(html) == html
    assert 'data-sort="'+spacing.FIELD+'"' in html
    assert 'hrs between calls' in html
    assert 'avg_calls_per_hour' not in html and 'calls/hr' not in html and 'Avg Calls/Hr' not in html
    assert "Number(r."+spacing.FIELD+").toFixed(1)" not in html
    assert "'Avg. Time Between Calls (hours)'" in html  # CSV heading, not only visible UI.
    assert 'avg_time_between_calls_mins' not in html and 'min between calls' not in html
    assert 'csvMaybeFloat(r.'+spacing.FIELD+', 2)' in html
    assert '// ppfd-spacing-nulls-last' in html
    assert '// ppfd-call-spacing-csv' in html
    assert r"lines.join('\\r\\n')" not in html
    assert 'decimals' not in html.split('function formatMaybeFloat(',1)[1].split('\n  }',1)[0]


def test_legacy_rate_and_duration_formats_upgrade_independently():
    original = '''<th data-sort="avg_calls_per_hour">Avg Calls/Hr</th>
  function csvMaybeFloat(value){
    var num=Number(value);
    return num.toFixed(1);
  }
  function formatMaybeFloat(value){
    var num=Number(value);
    return num.toFixed(1);
  }
  summary.textContent=(rate == null ? '--' : Number(rate).toFixed(2))+' calls/hr • '+Number(totalCalls || 0)+' calls • '+activeSummary;
  csvMaybeFloat(r.avg_calls_per_hour);
'''
    converted = spacing.update_ui(original)
    assert spacing.update_ui(converted) == converted
    assert 'hrs between calls' in converted
    assert 'csvMaybeFloat(r.'+spacing.FIELD+', 2)' in converted
    assert 'decimals' not in converted.split('function formatMaybeFloat(',1)[1].split('\n  }',1)[0]

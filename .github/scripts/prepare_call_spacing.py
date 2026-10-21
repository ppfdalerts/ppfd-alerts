"""Replace call rate with its time-per-call equivalent, without rounding a rate.

This is observed/scheduled hours per call, not measured end-to-next-start
downtime. Keep the legacy rate fields for compatibility with the source host.
"""
import datetime as dt
import json
import math
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import personnel_ledger as ledger

FIELD = "avg_time_between_calls_hours"
LABEL = "Avg. Time Between Calls (hours)"
NOTE = ("Average hours per call: period hours / calls; personnel use hours worked. "
        "Includes time on calls, not just downtime between calls. Blank when there are no calls or no valid hours.")


def positive(value):
    try:
        result = float(value)
        return result if math.isfinite(result) and result > 0 else None
    except (TypeError, ValueError):
        return None


def hours_per_call(hours, calls):
    hours, calls = positive(hours), positive(calls)
    return round(hours / calls, 2) if hours is not None and calls is not None else None


def shift_day(payload):
    meta = payload.get("today", {}).get("meta") or {}
    try:
        return dt.date.fromisoformat(meta.get("shift_start", "")[:10])
    except ValueError:
        match = re.search(r"\b\d{2}/\d{2}/\d{2}\b", meta.get("shift_date") or "")
        return dt.datetime.strptime(match.group(), "%m/%d/%y").date() if match else None


def period_hours(key, period, current):
    meta = period.get("meta") or {}
    if key == "prior" or (key == "today" and meta.get("is_current_shift") is False):
        return 24.0
    if key == "today":
        start, updated = ledger.timestamp(meta.get("shift_start")), ledger.timestamp(period.get("updated"))
        if not start or not updated or updated < start:
            return None
        elapsed = (updated.astimezone(dt.timezone.utc)-start.astimezone(dt.timezone.utc)).total_seconds()/3600
        # Preserve the source rate's first-hour minimum and 24-hour cap.
        return min(24.0, max(1.0, elapsed))
    text = meta.get("range") or ""
    dates = re.fullmatch(r"(\d{4}-\d{2}-\d{2})\s+-\s+(\d{4}-\d{2}-\d{2})", text)
    if dates:
        start, end = (dt.date.fromisoformat(value) for value in dates.groups())
    elif current and re.fullmatch(r"[A-Za-z]{3} \d{1,2} - [A-Za-z]{3} \d{1,2}", text):
        first, last = text.split(" - ")
        end = dt.datetime.strptime(last + " " + str(current.year), "%b %d %Y").date()
        if end > current:
            end = end.replace(year=end.year-1)
        start = dt.datetime.strptime(first + " " + str(end.year), "%b %d %Y").date()
        if start > end:
            start = start.replace(year=start.year-1)
    elif not text and key in {"week", "month"}:
        return {"week":7, "month":30}[key] * 24.0
    else:
        return None
    return ((end-start).days+1)*24.0 if end >= start else None


def add_spacing(payload):
    current = shift_day(payload)
    for key in ("today", "prior", "week", "month", "year", "rolling_year"):
        period = payload.get(key)
        if not isinstance(period, dict):
            continue
        hours = period_hours(key, period, current)
        period.setdefault("meta", {})["call_spacing"] = {"period_hours":hours, "formula":"Period hours / calls", "units":"hours"}
        for row in period.get("rows", []):
            calls = row.get("calls") if key in {"today", "prior"} else row.get("total_calls")
            row.pop("avg_time_between_calls_mins", None)
            row[FIELD] = hours_per_call(hours, calls)
    for period in payload.get("personnel", {}).values():
        if not isinstance(period, dict):
            continue
        period.setdefault("meta", {})["call_spacing"] = {"formula":"Hours worked / calls", "units":"hours"}
        for row in period.get("rows", []):
            row.pop("avg_time_between_calls_mins", None)
            row[FIELD] = hours_per_call(row.get("total_hours_worked"), row.get("total_calls"))
    return payload


def update_ui(html):
    html = html.replace("avg_calls_per_hour", FIELD).replace("Avg Calls/Hr", LABEL)
    html = html.replace("avg_time_between_calls_mins", FIELD).replace("Avg. Time Between Calls (min)", LABEL)
    html = html.replace("Average minutes per call: period hours x 60 / calls;", "Average hours per call: period hours / calls;")
    html = html.replace("Number(r."+FIELD+").toFixed(1)", "Number(r."+FIELD+").toFixed(2)")
    old = "summary.textContent=(rate == null ? '--' : Number(rate).toFixed(2))+' calls/hr • '+Number(totalCalls || 0)+' calls • '+activeSummary;"
    timeline = "summary.textContent=(rate == null ? '--' : Number(rate).toFixed(2))+' hrs between calls • '+Number(totalCalls || 0)+' calls • '+activeSummary;"
    new = (timeline +
           "\n      summary.title=" + json.dumps(NOTE) + "; // ppfd-call-spacing-v1")
    html = html.replace(old, new)
    html = html.replace("summary.textContent=(rate == null ? '--' : Number(rate).toFixed(1))+' min between calls • '+Number(totalCalls || 0)+' calls • '+activeSummary;", timeline)
    html = html.replace("function csvMaybeFloat(value){", "function csvMaybeFloat(value, decimals){")
    html = re.sub(r"function csvMaybeFloat\(value, decimals\)\{.*?\n  \}",
                  lambda match: match.group().replace("return num.toFixed(1);", "return num.toFixed(decimals == null ? 1 : decimals);"), html, count=1, flags=re.S)
    html = html.replace("csvMaybeFloat(r."+FIELD+")", "csvMaybeFloat(r."+FIELD+", 2)")
    html = html.replace("personnelState.sortDir = (key === 'name') ? 'asc' : 'desc';",
                        "personnelState.sortDir = (key === 'name' || key === '"+FIELD+"') ? 'asc' : 'desc';")
    if '// ppfd-spacing-nulls-last' not in html:
        html = html.replace("rows.sort(function(a,b){\n      var av =", """rows.sort(function(a,b){
      // ppfd-spacing-nulls-last
      if(sortKey === 'avg_time_between_calls_hours'){
        var missingA=a[sortKey]==null, missingB=b[sortKey]==null;
        if(missingA !== missingB) return missingA ? 1 : -1;
      }
      var av =""", 1)
    if '// ppfd-call-spacing-csv' not in html:
        html = html.replace("function exportPanelCsv(panel){", """function exportPanelCsv(panel){
    // ppfd-call-spacing-csv: the timeline's formatter is local to renderTable.
    function formatActivityDuration(seconds){
      if(seconds == null || !isFinite(Number(seconds))) return '--';
      var total=Math.max(0, Math.round(Number(seconds)/60));
      var hours=Math.floor(total/60), mins=total%60;
      if(hours && mins) return hours+'h '+mins+'m';
      if(hours) return hours+'h';
      return mins+'m';
    }""", 1)
    # Older generated templates escaped the JS source twice, producing literal
    # backslash-r/backslash-n in downloads instead of CSV row separators.
    escape = r'''function csvEscape(value){
    var text = value == null ? '' : String(value);
    if(/[",\r\n]/.test(text)) return '"' + text.replace(/"/g, '""') + '"';
    return text;
  }'''
    html = re.sub(r"function csvEscape\(value\)\{.*?\n  \}", lambda match: escape, html, count=1, flags=re.S)
    html = html.replace(r"lines.join('\\r\\n')", r"lines.join('\r\n')")
    # Let the longer label wrap within the existing column instead of widening
    # the grid because the old unit-rate header used white-space:nowrap.
    html = re.sub(r"(<th\b[^>]*>)" + re.escape(LABEL) + r"</th>",
                  lambda match: match.group(1) + '<span style="display:block;white-space:normal" title="'+NOTE+'">'+LABEL+'</span></th>', html)
    if " calls/hr" in html or "Avg Calls/Hr" in html:
        raise ValueError("Call-rate UI changed; refusing to relabel an unconverted rate")
    return html


def prepare(root=Path("docs")):
    path = Path(root) / "data.json"
    payload = add_spacing(json.loads(path.read_text(encoding="utf-8-sig")))
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")

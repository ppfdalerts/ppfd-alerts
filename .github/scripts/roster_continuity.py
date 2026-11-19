"""Keep saved roster display distinct from verified historical attribution."""
import datetime as dt
import json


def read(path):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {}


def display_bundle(root, ledger, current):
    health = read(root / "staffing/sync-status.json")
    cache = read(root / "staffing/roster-cache.json")

    def selected(day):
        roster = ledger.display_roster(day)
        if roster is not None:
            return {**roster, "saved_plan": False, "last_checked": health.get("last_success")}
        # This cache is used only for display. ledger.bridge_roster and all
        # personnel/call joins continue to require the operational archive.
        planned = (cache.get("dates") or {}).get(str(day))
        if (cache.get("source") != "telestaff-saved-roster-plan" or not cache.get("collected_at")
                or not isinstance(planned, dict) or planned.get("date") != str(day)
                or not isinstance(planned.get("units"), list)):
            return None
        return {**planned, "source": "telestaff-saved-roster-plan", "saved_plan": True,
                "last_checked": cache["collected_at"]}

    return {"generated_at": dt.datetime.now(dt.timezone.utc).isoformat(), "sync": health,
            "today": selected(current), "prior": selected(current-dt.timedelta(days=1))}


def continuity_ui(html):
    if "// ppfd-roster-continuity" in html:
        return html
    hooks = ("  function fetchRoster(){", "        var todayPayload = (j && j.today) ? j.today : j;",
             "    metaParts.push(periodLabel);", '<div class="wrap">')
    if not all(hook in html for hook in hooks):
        raise ValueError("Roster display hooks changed; refusing an unverified continuity overlay")
    html = html.replace('<div class="wrap">', '<div class="wrap">\n<div id="rosterSyncStatus" role="status" hidden></div>', 1)
    html = html.replace("</style>", """
  #rosterSyncStatus{margin:0 0 10px;padding:9px 12px;border:1px solid #334155;border-radius:8px;color:#cbd5e1;font-size:13px;overflow-wrap:anywhere}
  #rosterSyncStatus.warning{border-color:#a16207;background:#422006;color:#fef3c7}
  #rosterPop{max-width:min(420px,calc(100vw - 18px));box-sizing:border-box}
</style>""", 1)
    code = r"""  // ppfd-roster-continuity
  function rosterCheckedLabel(value){
    var when=new Date(value);
    return value && !isNaN(when.getTime()) ? when.toLocaleString('en-US', {timeZone:'America/New_York',month:'short',day:'numeric',hour:'numeric',minute:'2-digit'})+' ET' : 'unknown';
  }
  function rosterNeedsUpdate(){
    var sync=rosterState.sync || {};
    var age=Date.now()-Date.parse(sync.last_success);
    return ['auth_required','failed'].indexOf(sync.source_state)>=0 || age>(sync.stale_after_hours || 6)*3600000;
  }
  function showRosterSync(data, failed){
    if(data){ rosterState.sync=data.sync || {}; }
    var node=document.getElementById('rosterSyncStatus');
    if(!node) return;
    var sync=rosterState.sync || {};
    var saved=(rosterState.payload.today && rosterState.payload.today.saved_plan) || (rosterState.payload.prior && rosterState.payload.prior.saved_plan);
    var warning=!!failed || rosterNeedsUpdate() || saved;
    node.hidden=!failed && !sync.last_success && !sync.source_state && !saved;
    node.className=warning ? 'warning' : '';
    var message=failed ? 'Roster refresh is unavailable. Showing the last loaded roster; staffing may have changed.' :
      sync.source_state==='auth_required' ? 'Roster updates need Okta verification. Showing saved staffing; assignments may have changed.' :
      warning ? 'Showing saved staffing; roster updates are delayed and assignments may have changed.' : 'Roster is up to date.';
    node.textContent=message+(sync.last_success ? ' Last checked '+rosterCheckedLabel(sync.last_success)+'.' : '');
    rosterState.refreshFailed=!!failed;
  }
  function rosterPopupFreshness(payload){
    if(payload && payload.saved_plan) return 'Saved plan · checked '+rosterCheckedLabel(payload.last_checked);
    if(rosterState.refreshFailed || rosterNeedsUpdate()) return 'Saved roster · checked '+rosterCheckedLabel(payload && payload.last_checked);
    return '';
  }

"""
    html = html.replace(hooks[0], code+hooks[0], 1)
    html = html.replace(hooks[1], "        var todayPayload = j && Object.prototype.hasOwnProperty.call(j,'today') ? j.today : j;", 1)
    payload_hook = "        rosterState.payload = { today: todayPayload, prior: priorPayload };"
    html = html.replace(payload_hook, payload_hook+"\n        showRosterSync(j, false);", 1)
    catch_hook = """        rosterState.payload = { today: null, prior: null };
        rosterState.index = { today: null, prior: null };"""
    if catch_hook not in html:
        raise ValueError("Roster error handler changed; refusing to discard saved staffing")
    html = html.replace(catch_hook, "        showRosterSync(null, true);\n        if(rosterState.open){ renderRosterPopup(rosterState.open.unit, rosterState.open.period); }", 1)
    html = html.replace(hooks[2], hooks[2]+"\n    var freshness=rosterPopupFreshness(payload);\n    if(freshness){ metaParts.push(freshness); }", 1)
    return html

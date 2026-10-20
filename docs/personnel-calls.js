/* PPFD personnel call history: read-only, no credentials or calendar tokens. */
(function () {
  'use strict';
  var payload = null, history = null, historyKey = '', loading = null;
  var selected = null, filtered = [], limit = 50, query = '';
  var dialog = document.createElement('dialog');
  dialog.id = 'personnelCallsDialog';
  dialog.className = 'personnel-calls-dialog';
  dialog.innerHTML = '<header><div><h2 id="personnelCallsTitle">Personnel calls</h2><p id="personnelCallsRange"></p></div><button type="button" id="personnelCallsClose" aria-label="Close call history">Close</button></header>' +
    '<div class="personnel-call-tools"><label>Find a call <input type="search" id="personnelCallsSearch" placeholder="Incident, unit, type, location"></label><label>Show <select id="personnelCallsScope"><option value="period">Selected period</option><option value="all">All since last backfill</option></select></label><button type="button" id="personnelCallsCsv">Download calls (CSV)</button></div>' +
    '<p id="personnelCallsNote" class="personnel-call-note"></p><div class="personnel-call-scroll"><table><thead><tr><th>Date / recorded start (Eastern)</th><th>Incident</th><th>Unit / position</th><th>Call type</th><th>Location</th><th>Recorded end (Eastern)</th><th>Recorded call time</th></tr></thead><tbody id="personnelCallsRows"></tbody></table></div><footer><span id="personnelCallsCount"></span><button type="button" id="personnelCallsMore">Show more</button></footer>';
  document.body.appendChild(dialog);
  var byId = function (id) { return document.getElementById(id); };
  var tableBody = byId('personnelCallsRows');
  function cell(row, text) {
    var td = document.createElement('td');
    td.textContent = text == null || text === '' ? '—' : String(text);
    row.appendChild(td);
  }
  function localTime(value) {
    if (!value) return 'Not retained';
    return new Intl.DateTimeFormat('en-US', {timeZone:'America/New_York', month:'short', day:'numeric', year:'numeric', hour:'numeric', minute:'2-digit', second:'2-digit'}).format(new Date(value));
  }
  function bounds() {
    var end = history.through, start = history.from;
    var period = byId('personnelPeriod') ? byId('personnelPeriod').value : 'backfill';
    if (byId('personnelCallsScope').value === 'all') return [start, end];
    var day = new Date(end + 'T12:00:00Z');
    if (period === 'week' || period === 'month' || period === 'rolling_year') {
      day.setUTCDate(day.getUTCDate() - ({week:6, month:29, rolling_year:364}[period]));
      start = day.toISOString().slice(0, 10);
    } else if (period === 'year') {
      start = end.slice(0, 4) + '-01-01';
    }
    return [start < history.from ? history.from : start, end];
  }
  function render() {
    if (!history || !selected) return;
    var person = history.people[selected.id], range = bounds();
    var calls = person ? person.calls : [];
    filtered = calls.filter(function (call) {
      return call.date >= range[0] && call.date <= range[1] && (!query || JSON.stringify(call).toLowerCase().indexOf(query) !== -1);
    });
    byId('personnelCallsTitle').textContent = person ? person.name : selected.name;
    byId('personnelCallsRange').textContent = range[0] + ' through ' + range[1] + ' · One count per incident per person';
    byId('personnelCallsNote').textContent = 'Matched to the unit’s roster at the recorded call start, using exact assignment times. “Not retained” means the older source did not save that field. Recorded end times can be limited by the shift window. Missing roster or call records are not estimated.';
    tableBody.replaceChildren();
    filtered.slice(0, limit).forEach(function (call) {
      var tr = document.createElement('tr');
      cell(tr, localTime(call.start));
      cell(tr, call.incident_id);
      cell(tr, (call.assignments || []).map(function (assignment) { return assignment.unit + (assignment.position ? ' · ' + assignment.position : ''); }).join('\n') || (call.units || []).join(', '));
      cell(tr, call.type || 'Not retained');
      cell(tr, call.location || 'Not retained');
      cell(tr, call.ongoing ? 'Active at last source update' : localTime(call.end));
      cell(tr, call.duration_known ? (call.duration_sec / 60).toFixed(1) + ' min' : 'Not retained');
      tableBody.appendChild(tr);
    });
    if (!filtered.length) {
      var empty = document.createElement('tr');
      cell(empty, query ? 'No calls match this search.' : 'No verified incident records for this person in this period. Older shift totals do not contain individual call details.');
      empty.firstChild.colSpan = 7;
      tableBody.appendChild(empty);
    }
    byId('personnelCallsCount').textContent = Math.min(limit, filtered.length) + ' of ' + filtered.length + ' calls';
    byId('personnelCallsMore').hidden = limit >= filtered.length;
  }
  function loadHistory() {
    var key = payload && payload.today ? payload.today.updated : 'current';
    if (history && key === historyKey) return Promise.resolve(history);
    if (loading) return loading;
    loading = fetch('personnel_calls.json?v=' + encodeURIComponent(key), {cache:'no-store'}).then(function (response) {
      if (!response.ok) throw new Error('Call history is not published yet.');
      return response.json();
    }).then(function (result) {
      if (result.schema_version !== 1 || !result.people) throw new Error('Call history format is unavailable.');
      history = result; historyKey = key; return result;
    }).finally(function () { loading = null; });
    return loading;
  }
  function open(id, name) {
    selected = {id:id, name:name}; limit = 50; query = ''; filtered = [];
    byId('personnelCallsSearch').value = '';
    byId('personnelCallsScope').value = 'period';
    byId('personnelCallsTitle').textContent = name || 'Personnel calls';
    byId('personnelCallsNote').textContent = 'Loading verified call history…';
    tableBody.replaceChildren();
    if (!dialog.open) dialog.showModal();
    loadHistory().then(render).catch(function (error) { byId('personnelCallsNote').textContent = error.message + ' The existing leaderboard remains available.'; });
  }
  document.addEventListener('click', function (event) {
    var row = event.target.closest && event.target.closest('#board-personnel tr[data-person-id]');
    if (!row || !(payload && payload.personnel_history)) return;
    event.preventDefault(); event.stopImmediatePropagation();
    open(row.dataset.personId, row.cells[1] ? row.cells[1].textContent : row.dataset.personId);
  }, true);
  byId('personnelCallsClose').onclick = function () { dialog.close(); };
  byId('personnelCallsSearch').oninput = function () { query = this.value.trim().toLowerCase(); limit = 50; render(); };
  byId('personnelCallsScope').onchange = function () { limit = 50; render(); };
  byId('personnelCallsMore').onclick = function () { limit += 100; render(); };
  byId('personnelCallsCsv').onclick = function () {
    if (!history || !selected) return;
    var quote = function (value) { var text = value == null ? '' : String(value); if (/^[=+@-]/.test(text)) text = "'" + text; return '"' + text.replace(/"/g, '""') + '"'; };
    var rows = [['Incident','Dispatch','Units','Positions','Call type','Location','Unit clear','Recorded seconds','Attribution']];
    filtered.forEach(function (call) { rows.push([call.incident_id,call.start,(call.units||[]).join('; '),(call.assignments||[]).map(function (a) { return a.unit + ': ' + a.position; }).join('; '),call.type,call.location,call.end,call.duration_sec,call.attribution]); });
    var blob = new Blob(['\uFEFF' + rows.map(function (row) { return row.map(quote).join(','); }).join('\r\n')], {type:'text/csv;charset=utf-8'});
    var a = document.createElement('a'), url = URL.createObjectURL(blob);
    a.href = url; a.download = 'ppfd-personnel-' + selected.id.replace(/[^a-zA-Z0-9_-]/g, '') + '-calls.csv';
    a.click(); setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
  };
  function update(data) {
    payload = data;
    var container = document.querySelector('.personnel-controls');
    var note = byId('personnelCoverageNote');
    if (!container || !data.personnel_history) return;
    if (!note) { note = document.createElement('p'); note.id = 'personnelCoverageNote'; note.className = 'personnel-coverage-note'; container.insertAdjacentElement('afterend', note); }
    var info = data.personnel_history;
    var gaps = (info.missing_stats_dates || []).length + (info.missing_roster_dates || []).length;
    var period = byId('personnelPeriod') ? byId('personnelPeriod').value : 'backfill';
    var countingNote = data.personnel && data.personnel[period] && data.personnel[period].meta ? data.personnel[period].meta.counting_note : '';
    note.textContent = 'Click a name for individual calls. Roster matching since ' + info.from + '. ' + (info.complete ? 'Backfill complete.' : 'Partial history: ' + gaps + ' day-coverage gaps; ' + (info.missing_call_records || 0) + ' unit calls without saved incident records; ' + (info.unattributed_unit_calls || 0) + ' unit calls without a verified crew match.') + (countingNote ? ' ' + countingNote : '');
    if (dialog.open) loadHistory().then(render).catch(function () { /* Keep the last verified open view during a refresh failure. */ });
  }
  document.addEventListener('ppfd:data', function (event) { update(event.detail); });
  document.addEventListener('change', function (event) { if (event.target.id === 'personnelPeriod' && payload) update(payload); });
  if (window.ppfdData) update(window.ppfdData);
})();

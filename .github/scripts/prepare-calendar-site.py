"""Build-only navigation overlay; never change alert/data-generation programs."""
import hashlib
import json
from pathlib import Path
import re


def personnel_ui(html):
    """Keep the read-only history UI even if the legacy publisher sends old HTML."""
    if '<meta charset=' not in html.lower():
        html = html.replace('<!doctype html>', '<!doctype html>\n<meta charset="utf-8">', 1)
    if 'window.ppfdData = data;' not in html:
        hook = 'appState.payload = data || null;'
        if hook not in html:
            raise ValueError("Leaderboard data hook changed; refusing an unverified personnel overlay")
        html = html.replace(hook, hook + "\n    window.ppfdData = data;\n    document.dispatchEvent(new CustomEvent('ppfd:data', {detail:data}));", 1)
    if '<option value="backfill">' not in html:
        html = html.replace('<select id="personnelPeriod">', '<select id="personnelPeriod"><option value="backfill">Since last backfill</option>', 1)
    if 'personnelState.data.backfill' not in html:
        html = html.replace('personnelState.data.week = personnel.week || null;',
                            'personnelState.data.backfill = personnel.backfill || null;\n    personnelState.data.week = personnel.week || null;', 1)
    html = html.replace("data: { week: null, month: null, year: null, rolling_year: null },\n    period: 'week',",
                        "data: { backfill: null, week: null, month: null, year: null, rolling_year: null },\n    period: 'backfill',")
    if 'personnel-calls.css?' not in html:
        html = html.replace('<title>PPFD Live Leaderboards</title>', '<title>PPFD Live Leaderboards</title>\n<link rel="stylesheet" href="personnel-calls.css?v=20261020-1">', 1)
    if 'data-ppfd-personnel-bridge' not in html:
        html += '\n<script src="personnel-calls.js?v=20261020-1" data-ppfd-personnel-bridge></script>\n'
    return html


def prepare(root=Path("docs")):
    path = root / "index.html"
    html = path.read_text(encoding="utf-8-sig")
    if 'id="rosterCalendarLink"' not in html:
        match = re.search(r'<button\b[^>]*id="toolbarToggle"[^>]*>Controls</button>', html)
        if not match:
            raise ValueError("Controls navigation changed; refusing to publish an unverified overlay")
        navigation = ('<div class="toolbar-shortcuts">' + match.group() +
                      '<a id="rosterCalendarLink" class="toolbar-toggle calendar-navigation" href="calendar/">Calendars</a></div>')
        html = html[:match.start()] + navigation + html[match.end():]
        html = html.replace("</style>", "\n.toolbar-shortcuts{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.calendar-navigation{display:inline-block;text-decoration:none}\n</style>", 1)
    html = personnel_ui(html)
    path.write_text(html, encoding="utf-8", newline="\n")
    # The legacy collector runs on a different computer. Build-time attribution
    # ensures its frequent data pushes cannot overwrite the repaired personnel
    # totals or replace current staffing with a stale cached roster.
    from prepare_personnel_data import prepare_data
    prepare_data(root)
    version = root / "version.json"
    if version.exists():
        metadata = json.loads(version.read_text(encoding="utf-8-sig"))
        metadata["index_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        metadata["data_sha256"] = hashlib.sha256((root / "data.json").read_bytes()).hexdigest()
        version.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print("Calendar navigation and exact personnel data prepared; unit totals and alert service unchanged.")


if __name__ == "__main__":
    prepare()

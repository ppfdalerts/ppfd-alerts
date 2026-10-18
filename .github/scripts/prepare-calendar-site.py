"""Build-only navigation overlay; never change alert/data-generation programs."""
import hashlib
import json
from pathlib import Path
import re


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
    path.write_text(html, encoding="utf-8", newline="\n")
    version = root / "version.json"
    if version.exists():
        metadata = json.loads(version.read_text(encoding="utf-8-sig"))
        metadata["index_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        version.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print("Calendar navigation prepared; leaderboard data and alert source unchanged.")


if __name__ == "__main__":
    prepare()

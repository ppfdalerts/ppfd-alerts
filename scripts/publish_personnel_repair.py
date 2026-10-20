"""Publish explicitly named repair files over the latest tree, never force push.

Uses the calendar project's already-authorized credential loader; no secret is
copied into this repository. Does not stage or commit generated local data.
"""
import argparse
import base64
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--calendar-project", type=Path, required=True)
    parser.add_argument("--message", required=True)
    parser.add_argument("--include-call-archive", action="store_true")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    sys.path.insert(0, str(args.calendar_project / "src"))
    from roster_sync.config import load_settings
    from roster_sync.github_pages import GitHubClient, blob_sha
    api = GitHubClient(load_settings(args.calendar_project / "config.local.json"))
    root = Path(__file__).resolve().parents[1]
    allowed = ("scripts/", "tests/", ".github/", "docs/personnel-", "docs/call_archive/", "docs/index.html", "start_leaderboard.ps1", "README_PERSONNEL.md", ".gitignore")
    changes = {}
    if args.include_call_archive:
        args.files.extend(path.relative_to(root).as_posix() for path in (root / "docs/call_archive").glob("*.json"))
    for name in args.files:
        name = Path(name).as_posix()
        target = (root / name).resolve()
        if not target.is_relative_to(root) or not name.startswith(allowed):
            raise ValueError("Unapproved publication path")
        changes[name] = target.read_bytes()
    try:
        for attempt in range(3):
            head = api.call("GET", "git/ref/heads/main")["object"]["sha"]
            commit = api.call("GET", "git/commits/" + head)
            tree = api.call("GET", "git/trees/" + commit["tree"]["sha"], params={"recursive": "1"})
            existing = {row["path"]: row["sha"] for row in tree["tree"] if row["type"] == "blob"}
            elements = [{"path": name, "type": "blob", "mode": "100644", "content": body.decode("utf-8")}
                        for name, body in changes.items() if existing.get(name) != blob_sha(body)]
            if not elements:
                print({"changed_files": 0, "commit": head})
                return
            result = api.call("POST", "git/trees", json={"base_tree": commit["tree"]["sha"], "tree": elements})
            created = api.call("POST", "git/commits", json={"message": args.message, "tree": result["sha"], "parents": [head]})
            response = api.client.patch("git/refs/heads/main", json={"sha": created["sha"], "force": False})
            if response.status_code == 200:
                print({"changed_files": len(elements), "commit": created["sha"]})
                return
            if response.status_code not in {409, 422} or attempt == 2:
                raise RuntimeError("Safe branch update refused (HTTP " + str(response.status_code) + ")")
    finally:
        api.close()


if __name__ == "__main__":
    main()

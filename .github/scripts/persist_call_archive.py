"""Persist only validated public call archives over the latest Git tree.

The job token cannot cause another push-triggered run. The explicit skip marker
also keeps this maintenance commit out of unrelated CI. Never force push and
never replace a newer archive with an older queued Pages build.
"""
import base64
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request


def call(method, path, payload=None):
    url = "https://api.github.com/repos/" + os.environ["GITHUB_REPOSITORY"] + "/" + path
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": "Bearer " + os.environ["GITHUB_TOKEN"], "Accept": "application/vnd.github+json",
        "Content-Type": "application/json", "X-GitHub-Api-Version": "2022-11-28"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def publication(payload):
    value = payload.get("recovery", {}).get("publication") or "1970-01-01T00:00:00Z"
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def main(root=Path("docs/call_archive")):
    if not os.environ.get("GITHUB_TOKEN"):
        print("Archive persistence skipped: no job token (local preview).")
        return
    files = {}
    for path in root.glob("shift_stats_????-??-??.json"):
        data = path.read_bytes()
        payload = json.loads(data)
        if payload.get("recovery", {}).get("source") == "live_public_timeline":
            if path.stem != "shift_stats_" + payload["recovery"]["date"]:
                raise ValueError("Archive date mismatch")
            files[path.as_posix()] = (data, payload)
    if not files:
        return
    for attempt in range(3):
        head = call("GET", "git/ref/heads/main")["object"]["sha"]
        commit = call("GET", "git/commits/" + head)
        tree = call("GET", "git/trees/" + commit["tree"]["sha"] + "?recursive=1")
        existing = {row["path"]: row["sha"] for row in tree["tree"] if row["type"] == "blob"}
        changes = []
        for path, (body, candidate) in files.items():
            sha = hashlib.sha1(b"blob " + str(len(body)).encode() + b"\0" + body).hexdigest()
            if existing.get(path) == sha:
                continue
            if path in existing:
                blob = call("GET", "git/blobs/" + existing[path])
                previous = json.loads(base64.b64decode(blob["content"]))
                if publication(previous) > publication(candidate):
                    continue
            changes.append({"path": path, "mode": "100644", "type": "blob", "content": body.decode()})
        if not changes:
            print("Public incident archives already current.")
            return
        created_tree = call("POST", "git/trees", {"base_tree": commit["tree"]["sha"], "tree": changes})
        created = call("POST", "git/commits", {"message": "Archive retained public call timelines [skip ci]", "tree": created_tree["sha"], "parents": [head]})
        try:
            call("PATCH", "git/refs/heads/main", {"sha": created["sha"], "force": False})
            print("Persisted public call archives:", len(changes))
            return
        except urllib.error.HTTPError as error:
            if error.code not in {409, 422} or attempt == 2:
                raise
    raise RuntimeError("Archive branch changed repeatedly")


if __name__ == "__main__":
    main()

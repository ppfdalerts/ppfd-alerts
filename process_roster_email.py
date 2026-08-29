"""Download and process the newest weekly roster email once."""

import argparse
import base64
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


GMAIL_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
DATE_RANGE_RE = re.compile(r"(\d{4}-\d{2}-\d{2}).*?(\d{4}-\d{2}-\d{2})")


def log(message: str, log_path: Path) -> None:
    line = f"{dt.datetime.now():%Y-%m-%d %H:%M:%S} {message}"
    print(line, flush=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def load_state(state_path: Path, legacy_path: Path | None) -> dict:
    if state_path.exists():
        try:
            value = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                return value
        except Exception:
            pass
    if legacy_path and legacy_path.exists():
        lines = [line.strip() for line in legacy_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if lines:
            return {"message_id": lines[0], "attachment": lines[1] if len(lines) > 1 else ""}
    return {}


def save_state(state_path: Path, message_id: str, attachment: str) -> None:
    state_path.write_text(
        json.dumps(
            {
                "message_id": message_id,
                "attachment": attachment,
                "processed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def refresh_credentials(token_path: Path) -> Credentials:
    credentials = Credentials.from_authorized_user_file(str(token_path), [GMAIL_SCOPE])
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
        token_path.write_text(credentials.to_json(), encoding="utf-8")
    if not credentials.valid:
        raise RuntimeError("Gmail credentials are not valid; rerun gmail_oauth_setup.py.")
    return credentials


def roster_attachments(payload: dict) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []

    def walk(part: dict) -> None:
        filename = str(part.get("filename") or "")
        body = part.get("body") or {}
        if filename.lower().endswith((".xlsx", ".xlsm", ".zip")) and body.get("attachmentId"):
            found.append((filename, str(body["attachmentId"])))
        for child in part.get("parts", []) or []:
            walk(child)

    walk(payload.get("payload") or {})
    return found


def run_command(command: list[str], log_path: Path) -> None:
    result = subprocess.run(command, text=True, capture_output=True)
    for line in (result.stdout + result.stderr).splitlines():
        log(line, log_path)
    if result.returncode:
        raise RuntimeError(f"command failed with exit {result.returncode}: {command[0]}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--token", type=Path, required=True)
    parser.add_argument("--importer", type=Path, required=True)
    parser.add_argument("--backfill", type=Path, required=True)
    parser.add_argument("--stats-dir", type=Path, required=True)
    parser.add_argument("--roster-dir", type=Path, required=True)
    parser.add_argument("--personnel-dir", type=Path, required=True)
    parser.add_argument("--inbox-dir", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--sender", default="zjw499@gmail.com")
    args = parser.parse_args()
    args.log.parent.mkdir(parents=True, exist_ok=True)

    try:
        credentials = refresh_credentials(args.token)
        service = build("gmail", "v1", credentials=credentials, cache_discovery=False)
        query = f'from:{args.sender} subject:(Weekly Apparatus Roster) has:attachment newer_than:14d'
        messages = service.users().messages().list(userId="me", q=query, maxResults=10).execute().get("messages", [])
        if not messages:
            log("No matching roster email found.", args.log)
            return 0

        message_details = [
            service.users().messages().get(userId="me", id=item["id"], format="minimal").execute()
            for item in messages
        ]
        latest_message = max(
            message_details,
            key=lambda item: int(item.get("internalDate") or 0),
        )
        latest = latest_message["id"]
        state = load_state(args.state, args.state.with_name("gmail_roster_last_message.txt"))
        if state.get("message_id") == latest:
            log(f"Roster email {latest} already processed; skipping.", args.log)
            return 0

        message = service.users().messages().get(userId="me", id=latest, format="full").execute()
        attachments = roster_attachments(message)
        if not attachments:
            raise RuntimeError(f"Roster email {latest} has no supported spreadsheet attachment.")
        if len(attachments) > 1:
            log(f"Found {len(attachments)} spreadsheet attachments; processing all.", args.log)

        args.inbox_dir.mkdir(parents=True, exist_ok=True)
        downloaded: list[Path] = []
        for filename, attachment_id in attachments:
            data = service.users().messages().attachments().get(
                userId="me", messageId=latest, id=attachment_id
            ).execute()["data"]
            decoded = base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))
            output = args.inbox_dir / filename
            output.write_bytes(decoded)
            downloaded.append(output)
            log(f"Downloaded {filename} ({len(decoded)} bytes).", args.log)

        first_date = last_date = None
        for input_path in downloaded:
            match = DATE_RANGE_RE.search(input_path.name)
            if match:
                start, end = (dt.date.fromisoformat(value) for value in match.groups())
                first_date = start if first_date is None else min(first_date, start)
                last_date = end if last_date is None else max(last_date, end)
            run_command([
                sys.executable, str(args.importer), str(input_path),
                "--out-dir", str(args.roster_dir), "--overwrite",
            ], args.log)

        if first_date is None or last_date is None:
            raise RuntimeError("Roster attachment filename does not contain a YYYY-MM-DD date range.")
        run_command([
            sys.executable, str(args.backfill),
            "--start", first_date.isoformat(), "--end", last_date.isoformat(),
            "--stats-dir", str(args.stats_dir),
            "--roster-dir", str(args.roster_dir),
            "--out-dir", str(args.personnel_dir),
            "--overwrite",
        ], args.log)
        save_state(args.state, latest, downloaded[-1].name)
        log(f"Processed roster email {latest} for {first_date} through {last_date}.", args.log)
        return 0
    except Exception as exc:
        log(f"ERROR: {exc}", args.log)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

Headless run options for PPFD GroupMe alerts

Option A â€” Windows Scheduled Task (recommended)
- Install: run `install_groupme_task.ps1` in PowerShell (as your user)
  - Creates task `PPFD-GroupMe-Alerts` that runs at startup and at logon
  - Runs hidden and restarts up to 3 times if it exits unexpectedly
  - Uses `start_groupme.ps1` to launch `ppfd_groupme_alerts_v1.py`
- Uninstall: run `uninstall_groupme_task.ps1`
- Logs: see `alerts.log` in this folder (stdout/stderr redirected by the script itself)

Option B â€” Manual background start
- PowerShell: `powershell -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File .\start_groupme.ps1`

Notes
- The launcher prefers `venv\Scripts\python.exe` if present; otherwise uses `py -3` or `python` on PATH.
- Leaderboard JSON now writes to `data\shift_stats` inside this folder (env `SHIFT_STATS_DIR` is set for you). Sync that folder to GitHub Pages to refresh the public leaderboard.
- Set `LEADERBOARD_TOPIC_NAME` if you want to force scheduled 7PM/7AM leaderboard posts to a specific subgroup name.
- Set `DEBUG_VERBOSE=1` before launching for more detailed logs.
- `install_groupme_task.ps1` also registers `PPFD-Leaderboards-Live` which runs `start_leaderboard.ps1 -SkipRoster -IntervalSec 30` to keep GitHub Pages data fresh; rerun the installer if your task was created before the “no 72h time limit” fix.
- Optional GitHub sync (keeps leaderboard JSON pushed automatically): set `GITHUB_TOKEN` (repo contents write), `GITHUB_STATS_REPO` (`owner/repo`), and optionally `GITHUB_STATS_BRANCH`, `GITHUB_STATS_PATH`, `GITHUB_SYNC_INTERVAL_SECONDS`, `GITHUB_COMMIT_AUTHOR_NAME`, `GITHUB_COMMIT_AUTHOR_EMAIL`. The launcher will start `sync_stats_to_github.ps1` to push `data\shift_stats`.



Quick Deploy
- Double-click Start-Headless.bat. It bootstraps Python (venv), installs dependencies, registers autorun, and starts the alerts headless. No manual steps required after unzip.
- Logs: lerts.log in this folder.
- To remove autorun: run uninstall_groupme_task.ps1.

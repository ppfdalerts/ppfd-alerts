PPFD GitHub Leaderboards – Deployment Notes
===========================================

This folder contains everything needed to run the standalone leaderboard job, separate from the GroupMe alerts script, and publish the live leaderboards to the `ppfdalerts/ppfd-alerts` GitHub repo.

Contents
--------

- `ppfd_leaderboard_calculator.py`  
  Standalone Python script that:
  - Reads `shift_stats_YYYY-MM-DD.json` files (per-shift stats).
  - Generates aggregate leaderboard metrics.
  - Writes a local `data/leaderboards.json` (for any future consumers).
  - Regenerates `ppfd-alerts/docs/data.json` using the official GitHub dashboard generator.
  - Pushes `docs/data.json` to GitHub using a personal access token.

- `GithubToken.txt`  
  One-line GitHub personal access token (PAT) used to authenticate API writes. In production, replace the contents of this file with your real PAT that has `repo` write access.

- `ppfd-alerts/`  
  Local clone of the GitHub repo `ppfdalerts/ppfd-alerts` containing:
  - `data/shift_stats/` – expected stats directory for the GitHub dashboard.
  - `docs/index.html` – the leaderboard UI (GitHub Pages).
  - `docs/data.json` – JSON payload the dashboard reads (this file is regenerated).
  - `scripts/generate_leaderboard.py` – the official generator for `docs/data.json`.

- `start_leaderboard.ps1`  
  Convenience launcher for Windows. It:
  - Picks Python (prefers `venv\Scripts\python.exe`, falls back to `python` / `py`).
  - Resolves a stats directory (`ppfd-alerts\data\shift_stats` or `data\shift_stats`) and sets `SHIFT_STATS_DIR`.
  - Runs `ppfd_leaderboard_calculator.py` (or `--dry-run` when requested).


How to deploy into the real working directory
---------------------------------------------

Assume your real working directory already contains the live alerts script (for example `ppfd_groupme_alerts_v1.py`) and `data/shift_stats` where the alerts process writes stats.

1. Open this `dumpfolder` in Explorer.
2. Select everything inside it:
   - `ppfd_leaderboard_calculator.py`
   - `GithubToken.txt`
   - `ppfd-alerts` directory
   - `start_leaderboard.ps1`
3. Drag/copy those items into your real working directory (the same folder that holds `ppfd_groupme_alerts_v1.py`).
4. In the real directory, update `GithubToken.txt` if needed so it contains your production PAT (single line, no spaces).

After this, your layout should look like:

- `<real-dir>\ppfd_groupme_alerts_v1.py`
- `<real-dir>\ppfd_leaderboard_calculator.py`
- `<real-dir>\GithubToken.txt`
- `<real-dir>\ppfd-alerts\...`
- `<real-dir>\data\shift_stats\shift_stats_YYYY-MM-DD.json` (from the alerts script)


Running the leaderboard job manually
------------------------------------

From the real working directory:

1. Open PowerShell.
2. Run:

   ```powershell
   powershell -NoProfile -ExecutionPolicy Bypass -File .\start_leaderboard.ps1
   ```

This will:

- Resolve `SHIFT_STATS_DIR` (prefer `ppfd-alerts\data\shift_stats`, then `data\shift_stats`).
- Use the local Python (prefer `venv\Scripts\python.exe`) to run:
  - `ppfd_leaderboard_calculator.py`
  - Which in turn regenerates `ppfd-alerts\docs\data.json` and pushes it to GitHub.

Dry‑run (no GitHub writes)
--------------------------

To test without touching GitHub:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\start_leaderboard.ps1 -DryRun
```

This prints the leaderboard JSON to stdout instead of writing/pushing.


Scheduling automatic updates (Windows Task Scheduler)
-----------------------------------------------------

To update the GitHub leaderboard on a schedule (for example, every 15 minutes):

1. Open **Task Scheduler**.
2. Create a new task, e.g. `PPFD-Leaderboards`.
3. Set **Triggers**:
   - “On a schedule” → Daily → Repeat task every `15 minutes` for a duration of `1 day`.
4. Set **Action**:
   - Program/script: `powershell.exe`
   - Add arguments:

     ```text
     -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "C:\path\to\start_leaderboard.ps1"
     ```

   - Start in: `C:\path\to\your\real\working\directory`

5. Save the task. The scheduler will now regenerate and push the GitHub leaderboard on that cadence, fully decoupled from the GroupMe alerts process.

Live updates (every 30 seconds)
-------------------------------

`start_leaderboard.ps1` supports a built-in loop, so you don’t have to rely on the scheduler’s “repeat every” setting:

```powershell
powershell -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File .\start_leaderboard.ps1 -SkipRoster -IntervalSec 30
```

If you use a Scheduled Task for this, ensure its “Stop the task if it runs longer than…” setting is disabled/indefinite (older installs defaulted to ~72 hours).


Notes
-----

- The GitHub push uses:
  - `GithubToken.txt` or `GITHUB_TOKEN` env var.
  - `git remote.origin.url` and current branch (or `GITHUB_REPOSITORY` / `GITHUB_REF_NAME`) to determine `owner/repo/branch`.
- The UI/layout in `ppfd-alerts/docs/index.html` is unchanged; we only ever update `docs/data.json` with fresh stats.
- The alerts script continues to be the sole writer of `shift_stats_*.json`; this leaderboard job only reads those files. 

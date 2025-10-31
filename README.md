**Overview**
- Public GitHub Pages site shows your leaderboard and updates every 1–5 minutes.
- Telegram alerts remain fast by keeping the Python bot running locally or on a self‑hosted runner.

**What’s Included**
- `docs/index.html`: static page for GitHub Pages.
- `scripts/generate_leaderboard.py`: builds `docs/data.json` from local `shift_stats_*.json` files.
- `.github/workflows/pages-data.yml`: scheduled workflow (every 5 minutes) that regenerates and commits `docs/data.json`.
- `.gitignore`: avoids committing logs, `.env`, stats, and binaries.

**How It Works**
- Your bot (`ppfd_telegram_alerts_v3.2.py`) already writes `shift_stats_YYYY-MM-DD.json` each shift.
- A self‑hosted runner on the same Windows machine runs the scheduled workflow. It reads those files and commits `docs/data.json` back to GitHub.
- GitHub Pages serves `docs/index.html` and `docs/data.json` at a public URL.

**Setup Steps**
1) Create a GitHub repo and push this directory.
2) Settings → Pages → Build and deployment → Deploy from branch → `main`/`master` and folder `docs/`.
3) Install a self‑hosted runner on the Windows PC that runs the bot:
   - GitHub → Repo → Settings → Actions → Runners → New self‑hosted runner → Windows → follow steps.
   - Run the runner as a service so it’s always available.
4) Repo → Settings → Variables → New variable `SHIFT_STATS_DIR` with the absolute path of your stats folder, e.g.
   - `C:\Users\County\python alerts`
5) Enable the scheduled workflow:
   - Actions tab → “Publish leaderboard data” → Enable workflows → Run workflow (optional for first run).

Within a few minutes, Pages will serve the site and begin refreshing as `docs/data.json` updates.

**Telegram Alerts (Fast)**
- Keep running `ppfd_telegram_alerts_v3.2.py` locally via Task Scheduler/Service (recommended) or as a long‑running job on a self‑hosted runner.
- Do not publish sensitive tokens. Move `BOT_TOKEN`, `CHAT_ID`, and TAPO credentials to environment variables or a local `.env` only.
- Rotate the existing bot token before pushing to GitHub if it’s currently hard‑coded.

**Optional: Run the bot on a self‑hosted runner**
- Create another workflow that runs the bot continuously with `runs-on: self-hosted` and a large `timeout-minutes`.
- Keep this separate from the scheduled Pages data job, or install a second runner if you want both as Actions jobs.

**Notes**
- If `shift_stats_*.json` files aren’t present, the page will show “No runs recorded.” Populate them by letting the bot run.
- You can adjust refresh rate in `docs/index.html` (default 60s) and the cron schedule in `.github/workflows/pages-data.yml`.


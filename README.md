**Overview**
- Public GitHub Pages site shows the leaderboard and refreshes from generated data.
- The repo now includes both the leaderboard code and the Windows phone-notification runtime scripts.
- Production alerts still run headlessly on the Windows PC; GitHub stores the source, not the local secrets.

**What's Included**
- `docs/index.html`: GitHub Pages dashboard UI.
- `scripts/generate_leaderboard.py`: builds `docs/data.json` from local shift stats.
- `.github/workflows/pages-data.yml`: scheduled workflow for repo-side regeneration.
- Runtime Windows scripts in repo root: `start_groupme.ps1`, `start_leaderboard.ps1`, `ppfd_groupme_alerts_v1.py`, task install scripts, and launcher helpers.
- `ppfd_telegram_alerts_v3.2.py`: Telegram alert variant.
- `.gitignore`: excludes secrets, logs, runtime caches, and machine-local folders.

**How It Works**
- The local alert process writes `shift_stats_YYYY-MM-DD.json` and related runtime files on the Windows PC.
- `start_leaderboard.ps1` generates the leaderboard payload and publishes GitHub Pages assets.
- The same publisher now mirrors the live runtime source files into this repo so the full system is stored in GitHub.
- GitHub Pages serves `docs/index.html` and `docs/data.json` at the public site URL.

**Local-Only Items**
- `Groupmetokens.txt`
- `GithubToken.txt` / `Githubclassictoken.txt`
- `TSlogs/`
- `venv/`
- runtime logs, lock files, and machine-specific cache files

Those files stay on the Windows PC and are intentionally excluded from the repo.

**Setup Notes**
1. Keep the Windows scheduled tasks or headless launchers running locally for live alerts.
2. Use GitHub Pages with the `docs/` folder for the dashboard.
3. If you edit repo source from another computer, pull the updated source back to the Windows machine before expecting runtime behavior to change here.

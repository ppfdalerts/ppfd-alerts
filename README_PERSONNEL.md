# Personnel leaderboard and staffing repair

The existing alert collector continues publishing unit statistics. The Pages
build joins retained incident dispatch times to the operational TeleStaff
roster exported by the calendar collector. No alert is sent by this integration.

- Staffing bridge: `docs/staffing/roster_units_YYYY-MM-DD.json` plus its index.
  Only real apparatus assignments and eligible working rows are exported.
  Pending requests, leave, administrative records, and login/calendar secrets
  are excluded.
- Incidents: `docs/call_archive/shift_stats_YYYY-MM-DD.json`. Recovery records
  cite the exact GitHub snapshot. The Pages build also retains the current and
  prior shift timelines. A final job step persists these allowlisted archives
  over the latest remote tree without force pushes or a CI loop.
- Join: `scripts/personnel_ledger.py`. A person receives one count per incident
  when assigned to that dispatched unit at its recorded start. Intervals use
  Eastern timestamps, including overnight shifts and 07:00/07:30 changeovers.
  Conflicting assignments and missing timestamps are not guessed.
- Display: `docs/personnel_calls.json` contains the individual records.
  Names open a searchable, downloadable call history. The default personnel
  period is "Since last backfill". Unit clicks use fresh roster data.
- Persistence: the build-only overlay is maintained in `.github/scripts/` and
  `publish-calendar-pages.yml`, so an older publisher cannot overwrite the
  repaired personnel view. Existing calendar URLs and the Controls button stay
  intact. The build refreshes both integrity hashes.

## Evidence limits

The last personnel backfill ended September 4, 2026. The authorized historical
roster was recovered from September 4 through October 20. Public GitHub snapshots
retain individual incident timelines starting September 29; September 5–28
contain unit totals but no individual incident records. Those calls are reported
as missing incident detail, not estimated per-person counts. The original
collector's complete incident archive would fill that gap if supplied later.
Call type, location, and clear time display "Not retained" where absent.
Unmatched unit staffing is listed in the coverage report; no reserve-unit or
cross-staffing relationship is assumed without confirmation.

Year-to-date and rolling-year totals retain pre-backfill legacy shift estimates,
clearly distinguished from incident-level matching since September 5. A partial
backfill is never presented as complete.

## Verification

The rate column is displayed as **Avg. Time Between Calls (hours)**: period hours
divided by calls, or hours worked divided by a person's calls, to two decimals.
This means "one call every X hours", including time spent on calls, not measured
clear-to-next-dispatch downtime. The current unit shift uses reported elapsed
time (the source's one-hour minimum and 24-hour cap); prior shifts use 24 hours,
and summary periods use their full calendar windows. Zero calls or unknown hours
produce a blank/`--`, not zero or infinity. Partial-history caveats still apply.
`.github/scripts/prepare_call_spacing.py` computes from counts and hours, not the
rounded legacy rate. Labels, personnel sorting and CSV exports use the new field.

Run `python -m pytest -q tests`. The tests cover exact interval boundaries,
overnight carryover, approved/pending work, split units, incident deduplication,
ambiguous DST times, missing fields, source-to-UI data, and repeat-build
idempotency. Compile the scripts and run `node --check docs/personnel-calls.js`.
For a local preview run `python .github/scripts/prepare-calendar-site.py` and
serve `docs/` over loopback. This generates only public site artifacts.

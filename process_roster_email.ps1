param()

$ErrorActionPreference = 'Stop'
$repoRoot = $PSScriptRoot
$stateRoot = Split-Path -Parent $repoRoot
$python = Join-Path $stateRoot 'venv\Scripts\python.exe'
$processor = Join-Path $repoRoot 'process_roster_email.py'
$client = 'C:\Users\PPFD\Downloads\client_secret_826977434979-ikt1rm2r4bsir4mn0hoig1o068lrdjtv.apps.googleusercontent.com.json'

if (-not (Test-Path $python)) { throw "Python not found: $python" }
if (-not (Test-Path $processor)) { throw "Roster email processor not found: $processor" }
if (-not (Test-Path $client)) { throw "Google OAuth client file not found: $client" }

& $python $processor `
  --client $client `
  --token (Join-Path $stateRoot 'gmail_token.json') `
  --importer (Join-Path $repoRoot 'import_apparatus_roster.py') `
  --backfill (Join-Path $repoRoot 'backfill_personnel_stats.py') `
  --stats-dir (Join-Path $stateRoot 'data\shift_stats') `
  --roster-dir (Join-Path $stateRoot 'TSlogs') `
  --personnel-dir (Join-Path $stateRoot 'data\shift_personnel') `
  --inbox-dir (Join-Path $stateRoot 'gmail_roster_inbox') `
  --state (Join-Path $stateRoot 'gmail_roster_state.json') `
  --log (Join-Path $stateRoot 'gmail_roster_processor.log')
exit $LASTEXITCODE

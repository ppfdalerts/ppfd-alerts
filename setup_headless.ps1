$ErrorActionPreference = 'Stop'

function Write-Info($msg) {
  $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  Write-Output "$ts  $msg"
}

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

# Ensure tokens are discoverable by the Python process
$tokens = Join-Path $here 'Groupmetokens.txt'
if (Test-Path $tokens) { $env:GROUPME_TOKENS_FILE = $tokens }

Write-Info 'Bootstrapping runtime and dependencies...'
$env:PPFD_HEADLESS_SETUP = '1'
powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $here 'start_groupme.ps1')
if ($LASTEXITCODE -ne 0) {
  Write-Info "Bootstrap failed (exit code $LASTEXITCODE). Check alerts.log for details."
  exit $LASTEXITCODE
}

Write-Info 'Registering autorun (Scheduled Task or Startup shortcut)...'
powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $here 'install_groupme_task.ps1') | Out-Null

Write-Info 'Setup complete. Logs will appear in alerts.log.'

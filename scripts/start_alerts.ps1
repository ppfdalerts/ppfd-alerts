$ErrorActionPreference = 'Stop'

# Resolve repo root (folder containing this script is scripts/)
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Start-AlertsProcess {
  # Prefer the Python launcher (py -3), fallback to python
  $py = Get-Command py -ErrorAction SilentlyContinue
  if ($py) {
    Start-Process -FilePath $py.Path -ArgumentList @('-3', 'ppfd_telegram_alerts_v3.2.py') -WorkingDirectory $repoRoot -WindowStyle Hidden | Out-Null
    return
  }
  $python = Get-Command python -ErrorAction SilentlyContinue
  if ($python) {
    Start-Process -FilePath $python.Path -ArgumentList @('ppfd_telegram_alerts_v3.2.py') -WorkingDirectory $repoRoot -WindowStyle Hidden | Out-Null
    return
  }
  Write-Host 'ERROR: Python not found. Install Python 3 and ensure py.exe or python.exe is on PATH.'
  exit 1
}

Start-AlertsProcess


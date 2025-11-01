$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

function Stop-IfMatches([string]$pattern){
  try {
    $procs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -match $pattern }
    foreach($p in $procs){
      try { Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop } catch {}
    }
  } catch {}
}

# Kill any prior running instances (alerts and updater)
Stop-IfMatches 'ppfd_telegram_alerts_v3\.2\.py'
Stop-IfMatches 'update_pages\.ps1'

# Launch fresh instances
Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File', (Join-Path $repoRoot 'scripts\start_alerts.ps1')) -WorkingDirectory $repoRoot -WindowStyle Hidden | Out-Null
Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File', (Join-Path $repoRoot 'update_pages.ps1')) -WorkingDirectory $repoRoot -WindowStyle Hidden | Out-Null

Write-Host 'Restarted alerts and updater.'


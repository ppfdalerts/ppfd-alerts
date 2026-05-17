$ErrorActionPreference = 'Stop'

param(
  [string]$TaskName = 'PPFD-GroupMe-Alerts'
)

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
  Write-Host "Stopping task '$TaskName'" -ForegroundColor Yellow
  try { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null } catch {}
  Write-Host "Unregistering task '$TaskName'" -ForegroundColor Yellow
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
  Write-Host "Uninstalled '$TaskName'" -ForegroundColor Green
} else {
  Write-Host "No task named '$TaskName' found." -ForegroundColor Yellow
}


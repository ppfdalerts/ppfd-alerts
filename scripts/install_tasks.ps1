$ErrorActionPreference = 'Stop'

param(
  [int]$UpdaterMinutes = 3
)

function Test-Admin {
  try {
    $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
  } catch { return $false }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$alertsWrapper = Join-Path $repoRoot 'scripts\start_alerts.ps1'
$updaterScript = Join-Path $repoRoot 'scripts\start_updater.ps1'

if (-not (Test-Path $alertsWrapper)) { Write-Error "Missing $alertsWrapper" }
if (-not (Test-Path $updaterScript)) { Write-Error "Missing $updaterScript" }

$alertsTaskName = 'PPFD Telegram Alerts'
$updaterTaskName = 'PPFD Pages Updater'

# Common settings
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -RestartCount 3 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -MultipleInstances IgnoreNew

# Principal: prefer SYSTEM when running elevated; otherwise current user
if (Test-Admin) {
  $principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
} else {
  $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest
}

# Actions
$alertsAction = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$alertsWrapper`""
$updaterAction = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$updaterScript`""

# Triggers
$tAtStartup = New-ScheduledTaskTrigger -AtStartup
$tAtLogon   = New-ScheduledTaskTrigger -AtLogOn
$startNow   = (Get-Date)
$tRepeat    = New-ScheduledTaskTrigger -Once $startNow -RepetitionInterval (New-TimeSpan -Minutes $UpdaterMinutes) -RepetitionDuration ([TimeSpan]::MaxValue)

# Clean out any previous tasks
Get-ScheduledTask -TaskName $alertsTaskName -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$false
Get-ScheduledTask -TaskName $updaterTaskName -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$false

# Register tasks
$alertsTask  = New-ScheduledTask -Action $alertsAction  -Trigger @($tAtStartup, $tAtLogon) -Principal $principal -Settings $settings
Register-ScheduledTask -TaskName $alertsTaskName -InputObject $alertsTask | Out-Null

$updaterTask = New-ScheduledTask -Action $updaterAction -Trigger @($tAtStartup, $tRepeat) -Principal $principal -Settings $settings
Register-ScheduledTask -TaskName $updaterTaskName -InputObject $updaterTask | Out-Null

Write-Host "Installed tasks: '$alertsTaskName' and '$updaterTaskName' (Updater every $UpdaterMinutes min)."

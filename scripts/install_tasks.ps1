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

# Principal: prefer SYSTEM when running elevated; otherwise current user (DOMAIN\User)
$currentUser = try { ([Security.Principal.WindowsIdentity]::GetCurrent()).Name } catch { "$env:USERDOMAIN\$env:USERNAME" }
if (Test-Admin) {
  $principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
} else {
  $principal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Highest
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

# Register tasks (with schtasks.exe fallback if ScheduledTasks registration fails)
try {
  $alertsTask  = New-ScheduledTask -Action $alertsAction  -Trigger @($tAtStartup, $tAtLogon) -Principal $principal -Settings $settings
  Register-ScheduledTask -TaskName $alertsTaskName -InputObject $alertsTask | Out-Null
} catch {
  Write-Warning "Register-ScheduledTask failed for '$alertsTaskName': $($_.Exception.Message). Trying schtasks.exe fallback."
  $cmd = @(
    '/Create','/F',
    '/TN', 'PPFD Telegram Alerts',
    '/TR', "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$alertsWrapper`"",
    '/RL','HIGHEST','/SC','ONLOGON','/RU', $currentUser
  )
  $p = Start-Process -FilePath schtasks.exe -ArgumentList $cmd -PassThru -Wait -NoNewWindow
  if ($p.ExitCode -ne 0) { throw "schtasks.exe failed for alerts task (exit $($p.ExitCode))" }
}

try {
  $updaterTask = New-ScheduledTask -Action $updaterAction -Trigger @($tAtStartup, $tRepeat) -Principal $principal -Settings $settings
  Register-ScheduledTask -TaskName $updaterTaskName -InputObject $updaterTask | Out-Null
} catch {
  Write-Warning "Register-ScheduledTask failed for '$updaterTaskName': $($_.Exception.Message). Trying schtasks.exe fallback."
  $cmd = @(
    '/Create','/F',
    '/TN', 'PPFD Pages Updater',
    '/TR', "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$updaterScript`"",
    '/RL','HIGHEST','/SC','MINUTE','/MO', [Math]::Max(1,$UpdaterMinutes), '/RU', $currentUser
  )
  $p = Start-Process -FilePath schtasks.exe -ArgumentList $cmd -PassThru -Wait -NoNewWindow
  if ($p.ExitCode -ne 0) { throw "schtasks.exe failed for updater task (exit $($p.ExitCode))" }
}

# Start tasks now (best-effort)
try { Start-ScheduledTask -TaskName $alertsTaskName } catch {}
try { Start-ScheduledTask -TaskName $updaterTaskName } catch {}

Write-Host "Installed tasks: '$alertsTaskName' and '$updaterTaskName' (Updater every $UpdaterMinutes min)."

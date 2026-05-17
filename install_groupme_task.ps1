param(
  [string]$TaskName = 'PPFD-GroupMe-Alerts',
  [string]$LeaderboardTaskName = 'PPFD-Leaderboards',
  [string]$LeaderboardLiveTaskName = 'PPFD-Leaderboards-Live'
)

$ErrorActionPreference = 'Stop'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$ps    = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$start = Join-Path $here 'start_groupme.ps1'

if (-not (Test-Path $start)) {
  throw "start_groupme.ps1 not found at $start"
}

Write-Host "Registering scheduled task '$TaskName' to run $start" -ForegroundColor Cyan

$action = New-ScheduledTaskAction -Execute $ps -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$start`" -ForceRestart" -WorkingDirectory $here
$triggers = @(
  (New-ScheduledTaskTrigger -AtStartup)
  (New-ScheduledTaskTrigger -AtLogOn)
)
# These launchers are intended to run indefinitely; disable the default 72h execution time limit.
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Seconds 0)

try {
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
} catch {}

try {
  Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $triggers -Settings $settings -Description "Runs PPFD GroupMe alerts continuously" | Out-Null
} catch {
  Write-Host "Scheduled task registration failed (non-admin?). Falling back to Startup shortcut..." -ForegroundColor Yellow
  $startup = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Startup'
  if (-not (Test-Path $startup)) { New-Item -ItemType Directory -Path $startup | Out-Null }
  $lnk = Join-Path $startup 'PPFD-GroupMe-Alerts.lnk'
  $ws = New-Object -ComObject WScript.Shell
  $sc = $ws.CreateShortcut($lnk)
  $sc.TargetPath = $ps
  $sc.Arguments  = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$start`" -ForceRestart"
  $sc.WorkingDirectory = $here
  $sc.Description = 'PPFD GroupMe Alerts'
  $sc.Save()
  Write-Host "Created Startup shortcut at: $lnk" -ForegroundColor Green
}

Write-Host "Starting task '$TaskName'" -ForegroundColor Green
Start-ScheduledTask -TaskName $TaskName

function Register-LeaderboardTask {
  param(
    [string]$TaskName,
    [string]$WorkingDir,
    [string]$PowerShellExe
  )

  $leaderboardScript = Join-Path $WorkingDir 'start_leaderboard.ps1'
  if (-not (Test-Path $leaderboardScript)) {
    Write-Host "Leaderboard script not found at $leaderboardScript; skipping leaderboard task." -ForegroundColor Yellow
    return
  }

  $leaderboardVbs = Join-Path $WorkingDir 'start_leaderboard_silent.vbs'
  if (Test-Path $leaderboardVbs) {
    $wscript = Join-Path $env:SystemRoot 'System32\wscript.exe'
    $action = New-ScheduledTaskAction -Execute $wscript -Argument "`"$leaderboardVbs`"" -WorkingDirectory $WorkingDir
  } else {
    $action = New-ScheduledTaskAction -Execute $PowerShellExe -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$leaderboardScript`"" -WorkingDirectory $WorkingDir
  }

  $triggers = @(
    (New-ScheduledTaskTrigger -Daily -At 7:35AM)
    (New-ScheduledTaskTrigger -Daily -At 7:35PM)
  )
  $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Seconds 0)

  Write-Host "Registering scheduled task '$TaskName' for leaderboard updates" -ForegroundColor Cyan
  try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
  } catch {}
  try {
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $triggers -Settings $settings -Description "Runs PPFD leaderboard updates at startup, logon, and 7:35 AM/PM daily" | Out-Null
  } catch {
    Write-Host "WARN: Failed to register '$TaskName' scheduled task: $($_.Exception.Message)" -ForegroundColor Yellow
    return
  }

  try {
    Start-ScheduledTask -TaskName $TaskName | Out-Null
  } catch {
    Write-Host "WARN: Unable to start '$TaskName': $($_.Exception.Message)" -ForegroundColor Yellow
  }
}

function Register-LeaderboardLiveTask {
  param(
    [string]$TaskName,
    [string]$WorkingDir,
    [string]$PowerShellExe,
    [int]$IntervalSec
  )

  $leaderboardScript = Join-Path $WorkingDir 'start_leaderboard.ps1'
  if (-not (Test-Path $leaderboardScript)) {
    Write-Host "Leaderboard script not found at $leaderboardScript; skipping live leaderboard task." -ForegroundColor Yellow
    return
  }

  $args = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$leaderboardScript`" -SkipRoster -IntervalSec $IntervalSec"
  $action = New-ScheduledTaskAction -Execute $PowerShellExe -Argument $args -WorkingDirectory $WorkingDir
  $triggers = @(
    (New-ScheduledTaskTrigger -AtStartup)
    (New-ScheduledTaskTrigger -AtLogOn)
  )
  $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -StartWhenAvailable -Hidden -ExecutionTimeLimit (New-TimeSpan -Seconds 0)
  $principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest

  Write-Host "Registering scheduled task '$TaskName' for live leaderboard updates" -ForegroundColor Cyan
  try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
  } catch {}
  try {
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $triggers -Settings $settings -Principal $principal -Description "Runs PPFD leaderboard updates every $IntervalSec seconds (no roster export)" | Out-Null
  } catch {
    Write-Host "WARN: Failed to register '$TaskName' scheduled task: $($_.Exception.Message)" -ForegroundColor Yellow
    return
  }

  try {
    Start-ScheduledTask -TaskName $TaskName | Out-Null
  } catch {
    Write-Host "WARN: Unable to start '$TaskName': $($_.Exception.Message)" -ForegroundColor Yellow
  }
}

Register-LeaderboardTask -TaskName $LeaderboardTaskName -WorkingDir $here -PowerShellExe $ps
Register-LeaderboardLiveTask -TaskName $LeaderboardLiveTaskName -WorkingDir $here -PowerShellExe $ps -IntervalSec 30

Write-Host "Done. Check alerts.log for runtime output." -ForegroundColor Green

param(
  [switch]$Headless,
  [switch]$InstallTask,
  [switch]$NoRun,
  [switch]$Loop,
  [int]$IntervalSec = 300,
  [string]$TaskName = 'PPFD-GitHub-Deploy',
  [string]$RepoUrl = 'https://github.com/ppfdalerts/ppfd-alerts.git',
  [string]$Branch = 'main',
  [string]$CloneDir
)

$ErrorActionPreference = 'Stop'

$script:StateRoot = $PSScriptRoot
if (-not $script:StateRoot) {
  try {
    $script:StateRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
  } catch {
    $script:StateRoot = Get-Location
  }
}
$script:SelfPath = $MyInvocation.MyCommand.Path
if (-not $script:SelfPath) {
  $script:SelfPath = Join-Path $script:StateRoot 'deploy_github_live.ps1'
}
if (-not $CloneDir) {
  $CloneDir = Join-Path $script:StateRoot 'github-working-copy'
}
$script:CloneDir = [System.IO.Path]::GetFullPath($CloneDir)
$script:LogPath = Join-Path $script:StateRoot 'github_deploy.log'
$script:DeployLockPath = Join-Path $script:StateRoot 'github_deploy.lock'
$script:HasLock = $false

function Write-Info($msg) {
  $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  $line = "$ts  $msg"
  if (-not $Headless) {
    Write-Host $line
  }
  try {
    Add-Content -Path $script:LogPath -Value $line -Encoding UTF8
  } catch {}
}

function Try-AcquireLock {
  if ($script:HasLock) { return $true }

  if (Test-Path $script:DeployLockPath) {
    $existingPid = $null
    try {
      $raw = (Get-Content -Path $script:DeployLockPath -ErrorAction Stop | Select-Object -First 1).Trim()
      [void][int]::TryParse($raw, [ref]$existingPid)
    } catch {}

    if ($existingPid -and $existingPid -ne $PID -and (Get-Process -Id $existingPid -ErrorAction SilentlyContinue)) {
      Write-Info "Another GitHub deploy process is already running (pid=$existingPid); exiting."
      return $false
    }

    try { Remove-Item -Path $script:DeployLockPath -Force -ErrorAction SilentlyContinue } catch {}
  }

  Set-Content -Path $script:DeployLockPath -Value $PID -Encoding ASCII -NoNewline -Force
  $script:HasLock = $true
  return $true
}

function Release-Lock {
  if (-not $script:HasLock) { return }
  try {
    if (Test-Path $script:DeployLockPath) {
      Remove-Item -Path $script:DeployLockPath -Force -ErrorAction SilentlyContinue
    }
  } catch {}
  $script:HasLock = $false
}

function Get-GitExe {
  $candidates = @(
    (Join-Path $script:StateRoot 'mingit\cmd\git.exe'),
    (Join-Path $script:StateRoot 'mingit\bin\git.exe')
  )
  foreach ($candidate in $candidates) {
    if (Test-Path $candidate) { return $candidate }
  }

  $cmd = Get-Command git -ErrorAction SilentlyContinue
  if ($cmd -and $cmd.Source) { return $cmd.Source }
  throw "git.exe not found."
}

function Invoke-Git {
  param(
    [string[]]$GitArgs,
    [string]$WorkingDirectory
  )

  $gitExe = Get-GitExe
  $allArgs = @()
  if ($WorkingDirectory) {
    $allArgs += @('-C', $WorkingDirectory)
  }
  $allArgs += $GitArgs
  & $gitExe @allArgs
  if ($LASTEXITCODE -ne 0) {
    throw "git failed ($($GitArgs -join ' ')) with exit code $LASTEXITCODE"
  }
}

function Get-GitOutput {
  param(
    [string[]]$GitArgs,
    [string]$WorkingDirectory
  )

  $gitExe = Get-GitExe
  $allArgs = @()
  if ($WorkingDirectory) {
    $allArgs += @('-C', $WorkingDirectory)
  }
  $allArgs += $GitArgs
  $out = & $gitExe @allArgs
  if ($LASTEXITCODE -ne 0) {
    throw "git failed ($($GitArgs -join ' ')) with exit code $LASTEXITCODE"
  }
  return @($out)
}

function Test-TrackedWorkingTreeClean {
  param(
    [Parameter(Mandatory=$true)][string]$RepoRoot
  )

  $status = Get-GitOutput -WorkingDirectory $RepoRoot -GitArgs @('status', '--porcelain', '--untracked-files=no')
  return (-not $status -or $status.Count -eq 0)
}

function Ensure-WorkingCopy {
  if (-not (Test-Path $script:CloneDir)) {
    $parent = Split-Path -Parent $script:CloneDir
    if ($parent -and -not (Test-Path $parent)) {
      New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    Write-Info "Cloning GitHub working copy to $script:CloneDir"
    Invoke-Git -GitArgs @('clone', '--branch', $Branch, '--single-branch', $RepoUrl, $script:CloneDir)
    $head = (Get-GitOutput -WorkingDirectory $script:CloneDir -GitArgs @('rev-parse', 'HEAD') | Select-Object -First 1).Trim()
    return [pscustomobject]@{
      Changed = $true
      RestartRequired = $true
      Head = $head
    }
  }

  if (-not (Test-Path (Join-Path $script:CloneDir '.git'))) {
    throw "Clone directory exists but is not a git repo: $script:CloneDir"
  }

  if (-not (Test-TrackedWorkingTreeClean -RepoRoot $script:CloneDir)) {
    Write-Info "WARN: GitHub working copy has tracked local changes; skipping pull until cleaned."
    $head = (Get-GitOutput -WorkingDirectory $script:CloneDir -GitArgs @('rev-parse', 'HEAD') | Select-Object -First 1).Trim()
    return [pscustomobject]@{
      Changed = $false
      RestartRequired = $false
      Head = $head
    }
  }

  Invoke-Git -WorkingDirectory $script:CloneDir -GitArgs @('fetch', 'origin', $Branch, '--prune')
  $current = (Get-GitOutput -WorkingDirectory $script:CloneDir -GitArgs @('rev-parse', 'HEAD') | Select-Object -First 1).Trim()
  $remote = (Get-GitOutput -WorkingDirectory $script:CloneDir -GitArgs @('rev-parse', ("origin/{0}" -f $Branch)) | Select-Object -First 1).Trim()

  if ($current -ne $remote) {
    $changedFiles = @(Get-GitOutput -WorkingDirectory $script:CloneDir -GitArgs @('diff', '--name-only', 'HEAD', ("origin/{0}" -f $Branch)))
    $generatedOnly = $true
    foreach ($rawPath in $changedFiles) {
      $path = ([string]$rawPath).Trim().Replace('\', '/')
      if (-not $path) { continue }
      if (@(
        'docs/data.json',
        'data/leaderboards.json',
        'docs/roster_units.json',
        'docs/version.json',
        'docs/backfill_status.json'
      ) -notcontains $path) {
        $generatedOnly = $false
        break
      }
    }
    Write-Info "Pulling latest GitHub code ($current -> $remote)"
    Invoke-Git -WorkingDirectory $script:CloneDir -GitArgs @('pull', '--ff-only', 'origin', $Branch)
    $current = (Get-GitOutput -WorkingDirectory $script:CloneDir -GitArgs @('rev-parse', 'HEAD') | Select-Object -First 1).Trim()
    return [pscustomobject]@{
      Changed = $true
      RestartRequired = (-not $generatedOnly)
      Head = $current
    }
  }

  return [pscustomobject]@{
    Changed = $false
    RestartRequired = $false
    Head = $current
  }
}

function New-PowerShellHiddenCommand {
  param(
    [Parameter(Mandatory=$true)][string]$ScriptPath,
    [Parameter(Mandatory=$true)][string[]]$ScriptArgs
  )

  $psExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
  $stateSafe = $script:StateRoot -replace "'", "''"
  $scriptSafe = $ScriptPath -replace "'", "''"
  $argText = $ScriptArgs -join ' '
  return ('"{0}" -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -Command "& {{ $env:PPFD_STATE_ROOT = ''{1}''; & ''{2}'' {3} }}"' -f $psExe, $stateSafe, $scriptSafe, $argText)
}

function Write-HiddenLauncher {
  param(
    [Parameter(Mandatory=$true)][string]$OutPath,
    [Parameter(Mandatory=$true)][string]$CommandLine
  )

  $cmdEscaped = ($CommandLine.Trim() -replace "`r?`n", ' ') -replace '"', '""'
  $body = "Set shell = CreateObject(""Wscript.Shell"")`r`ncmd = ""$cmdEscaped""`r`nshell.Run cmd, 0, False`r`n"
  [System.IO.File]::WriteAllText($OutPath, $body, [System.Text.UTF8Encoding]::new($false))
}

function Register-LoopTask {
  param(
    [Parameter(Mandatory=$true)][string]$Name,
    [Parameter(Mandatory=$true)][string]$LauncherPath,
    [Parameter(Mandatory=$true)][string]$Description
  )

  $wscriptExe = Join-Path $env:SystemRoot 'System32\wscript.exe'
  $action = New-ScheduledTaskAction -Execute $wscriptExe -Argument ('"' + $LauncherPath + '"') -WorkingDirectory $script:StateRoot
  $triggers = @(
    (New-ScheduledTaskTrigger -AtStartup),
    (New-ScheduledTaskTrigger -AtLogOn)
  )
  $settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -Hidden

  $expectedArgs = '"' + $LauncherPath + '"'
  $existing = Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue
  if ($existing) {
    try {
      $existingAction = $existing.Actions | Select-Object -First 1
      if ($existingAction -and
          $existingAction.Execute -ieq $wscriptExe -and
          ([string]$existingAction.Arguments).Trim() -eq $expectedArgs -and
          ([string]$existingAction.WorkingDirectory).Trim() -eq $script:StateRoot) {
        return $false
      }
    } catch {}
    Unregister-ScheduledTask -TaskName $Name -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
  }

  $userId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
  $principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive
  $task = New-ScheduledTask -Action $action -Trigger $triggers -Settings $settings -Principal $principal -Description $Description
  Register-ScheduledTask -TaskName $Name -InputObject $task -Force | Out-Null
  return $true
}

function Stop-LeaderboardProcesses {
  $matches = Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe'" -ErrorAction SilentlyContinue | Where-Object {
    $_.ProcessId -ne $PID -and $_.CommandLine -and (
      $_.CommandLine -match '(?i)start_leaderboard\.ps1' -or
      $_.CommandLine -match '(?i)start_leaderboard_silent\.vbs' -or
      $_.CommandLine -match '(?i)start_leaderboard_autostart\.vbs'
    )
  }
  foreach ($proc in $matches) {
    try {
      Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
      Write-Info "Stopped leaderboard process pid=$($proc.ProcessId)"
    } catch {
      Write-Info "WARN: Failed to stop leaderboard process pid=$($proc.ProcessId): $($_.Exception.Message)"
    }
  }
}

function Get-AlertsProcesses {
  Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -and $_.CommandLine -match 'ppfd_groupme_alerts_v1\.py'
  }
}

function Ensure-LiveTaskRegistration {
  param(
    [Parameter(Mandatory=$true)][string]$CodeRoot
  )

  $groupmeScript = Join-Path $CodeRoot 'start_groupme.ps1'
  $leaderboardScript = Join-Path $CodeRoot 'start_leaderboard.ps1'
  if (-not (Test-Path $groupmeScript)) { throw "GitHub working copy missing $groupmeScript" }
  if (-not (Test-Path $leaderboardScript)) { throw "GitHub working copy missing $leaderboardScript" }

  $groupmeLauncher = Join-Path $script:StateRoot 'github_live_groupme.vbs'
  $leaderboardLauncher = Join-Path $script:StateRoot 'github_live_leaderboard.vbs'
  $groupmeCmd = New-PowerShellHiddenCommand -ScriptPath $groupmeScript -ScriptArgs @('-ForceRestart')
  $leaderboardCmd = New-PowerShellHiddenCommand -ScriptPath $leaderboardScript -ScriptArgs @('-Headless', '-IntervalSec', '30')
  Write-HiddenLauncher -OutPath $groupmeLauncher -CommandLine $groupmeCmd
  Write-HiddenLauncher -OutPath $leaderboardLauncher -CommandLine $leaderboardCmd

  $changed = $false
  if (Register-LoopTask -Name 'PPFD-GroupMe-Alerts' -LauncherPath $groupmeLauncher -Description 'Runs PPFD GroupMe alerts from the GitHub working copy') {
    $changed = $true
  }
  if (Register-LoopTask -Name 'PPFD-Leaderboard-Sync' -LauncherPath $leaderboardLauncher -Description 'Runs PPFD leaderboard sync from the GitHub working copy') {
    $changed = $true
  }

  foreach ($conflict in @('PPFD-Leaderboards', 'PPFD-Leaderboards-Live')) {
    try {
      Disable-ScheduledTask -TaskName $conflict -ErrorAction SilentlyContinue | Out-Null
    } catch {}
  }
  return $changed
}

function Ensure-LiveProcesses {
  param(
    [switch]$Restart
  )

  if ($Restart) {
    Stop-LeaderboardProcesses
    try { Start-ScheduledTask -TaskName 'PPFD-GroupMe-Alerts' | Out-Null } catch {}
    try { Start-ScheduledTask -TaskName 'PPFD-Leaderboard-Sync' | Out-Null } catch {}
    return
  }

  $alertsRunning = @(Get-AlertsProcesses).Count -gt 0
  if (-not $alertsRunning) {
    Write-Info "Alerts process not running; starting PPFD-GroupMe-Alerts task."
    try { Start-ScheduledTask -TaskName 'PPFD-GroupMe-Alerts' | Out-Null } catch {}
  }

  $leaderboardRunning = @(Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe'" -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -and $_.CommandLine -match '(?i)start_leaderboard\.ps1'
  }).Count -gt 0
  if (-not $leaderboardRunning) {
    Write-Info "Leaderboard loop not running; starting PPFD-Leaderboard-Sync task."
    try { Start-ScheduledTask -TaskName 'PPFD-Leaderboard-Sync' | Out-Null } catch {}
  }
}

function Invoke-DeployOnce {
  $sync = Ensure-WorkingCopy
  $tasksChanged = Ensure-LiveTaskRegistration -CodeRoot $script:CloneDir
  Ensure-LiveProcesses -Restart:($sync.RestartRequired -or $tasksChanged)
  Write-Info "GitHub live deploy complete at $($sync.Head.Substring(0,7)) (changed=$($sync.Changed), restart=$($sync.RestartRequired -or $tasksChanged))."
}

function Install-DeployTask {
  $launcherPath = Join-Path $script:StateRoot 'github_live_deploy.vbs'
  $psExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
  $cmd = '"{0}" -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "{1}" -Headless -Loop -IntervalSec {2}' -f $psExe, $script:SelfPath, $IntervalSec
  Write-HiddenLauncher -OutPath $launcherPath -CommandLine $cmd
  Register-LoopTask -Name $TaskName -LauncherPath $launcherPath -Description 'Pulls PPFD system updates from GitHub and reconciles the live tasks'
}

if ($IntervalSec -lt 30) {
  throw "IntervalSec must be at least 30 seconds."
}

if ($InstallTask) {
  Install-DeployTask
  if ($NoRun) {
    Write-Info "Deploy task installed; exiting due to -NoRun."
    exit 0
  }
}

if (-not (Try-AcquireLock)) {
  exit 0
}

try {
  if ($Loop) {
    Write-Info "Starting GitHub deploy loop every $IntervalSec seconds."
    while ($true) {
      try {
        Invoke-DeployOnce
      } catch {
        Write-Info "WARN: GitHub deploy cycle failed: $($_.Exception.Message)"
        if ($_.ScriptStackTrace) { Write-Info $_.ScriptStackTrace }
      }
      Start-Sleep -Seconds $IntervalSec
    }
  } else {
    Invoke-DeployOnce
  }
} finally {
  Release-Lock
}

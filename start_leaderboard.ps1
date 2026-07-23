param(
  [switch]$DryRun,
  [switch]$SkipRoster,
  [int]$IntervalSec = 0,
  [switch]$Headless,
  [switch]$InstallAutostart,
  [switch]$RemoveAutostart,
  [switch]$NoRun,
  [string]$AutostartTaskName = 'PPFD-Leaderboard-Sync',
  [int]$AutostartIntervalSec = 30,
  [int]$AutostartDelaySec = 45
)

$ErrorActionPreference = 'Stop'

$script:ScriptRoot = $PSScriptRoot
if (-not $script:ScriptRoot) {
  try {
    $script:ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
  } catch {
    $script:ScriptRoot = Get-Location
  }
}
$script:ScriptPath = $MyInvocation.MyCommand.Path
if (-not $script:ScriptPath) {
  $script:ScriptPath = Join-Path $script:ScriptRoot 'start_leaderboard.ps1'
}
$script:StateRoot = ([string]$env:PPFD_STATE_ROOT).Trim()
if (-not $script:StateRoot) {
  $script:StateRoot = $script:ScriptRoot
} else {
  try {
    $script:StateRoot = [System.IO.Path]::GetFullPath($script:StateRoot)
  } catch {}
}
if (-not (Test-Path $script:StateRoot)) {
  New-Item -ItemType Directory -Path $script:StateRoot -Force | Out-Null
}
$script:CodeRepoRoot = $script:ScriptRoot
if (-not (Test-Path (Join-Path $script:CodeRepoRoot 'scripts\generate_leaderboard.py'))) {
  $candidateRepoRoot = Join-Path $script:ScriptRoot 'ppfd-alerts'
  if (Test-Path (Join-Path $candidateRepoRoot 'scripts\generate_leaderboard.py')) {
    $script:CodeRepoRoot = $candidateRepoRoot
  }
}
$stateNorm = $script:StateRoot.TrimEnd('\').ToLowerInvariant()
$repoNorm = $script:CodeRepoRoot.TrimEnd('\').ToLowerInvariant()
$script:GeneratedRoot = if ($stateNorm -eq $repoNorm) { $script:CodeRepoRoot } else { Join-Path $script:StateRoot 'github_live_generated' }
$script:LastErrorAt = $null
$script:LastErrorMessage = $null
$script:LastStatsFingerprint = $null
$script:LastRunFingerprint = $null
$script:LastSourceSyncFingerprint = $null
$script:LastBackfillFingerprint = $null
$script:LastSuccessfulBackfillGapSignature = $null
$script:FullBackfillCompleted = $false
$script:BackfillStatus = [ordered]@{
  status = 'unknown'
  action = 'none'
  message = 'No backfill run yet.'
  updated_at = (Get-Date).ToUniversalTime().ToString('o')
}
$script:RunLockPath = Join-Path $script:StateRoot 'leaderboard_sync.lock'
$script:HasRunLock = $false
$script:LogPath = Join-Path $script:StateRoot 'leaderboard_runner.log'

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

function Set-BackfillStatus {
  param(
    [Parameter(Mandatory=$true)][string]$Status,
    [Parameter(Mandatory=$true)][string]$Action,
    [Parameter(Mandatory=$true)][string]$Message,
    [datetime]$StartDate,
    [datetime]$EndDate,
    [string]$ErrorMessage
  )

  $payload = [ordered]@{
    status = $Status
    action = $Action
    message = $Message
    updated_at = (Get-Date).ToUniversalTime().ToString('o')
  }
  if ($StartDate) { $payload.start_date = $StartDate.ToString('yyyy-MM-dd') }
  if ($EndDate) { $payload.end_date = $EndDate.ToString('yyyy-MM-dd') }
  if ($ErrorMessage) { $payload.error = $ErrorMessage }
  $script:BackfillStatus = $payload
}

function Write-BackfillStatusFile {
  param(
    [Parameter(Mandatory=$true)][string]$Path
  )

  try {
    $dir = Split-Path -Parent $Path
    if ($dir -and -not (Test-Path $dir)) {
      New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    $json = $script:BackfillStatus | ConvertTo-Json -Depth 6
    [System.IO.File]::WriteAllText($Path, $json, [System.Text.UTF8Encoding]::new($false))
  } catch {
    Write-Info "WARN: Failed to write backfill status file '$Path': $($_.Exception.Message)"
  }
}

function Get-HttpStatusCode {
  param($ErrorRecord)

  try {
    $status = $ErrorRecord.Exception.Response.StatusCode
    if ($status -ne $null) { return [int]$status }
  } catch {}
  return $null
}

function Get-RetryAfterSeconds {
  param($ErrorRecord)

  try {
    $headers = $ErrorRecord.Exception.Response.Headers
    if (-not $headers) { return $null }

    $raw = $null
    try { $raw = $headers['Retry-After'] } catch {}
    if (-not $raw) {
      try {
        $vals = $headers.GetValues('Retry-After')
        if ($vals) { $raw = $vals[0] }
      } catch {}
    }
    if (-not $raw) { return $null }
    if ($raw -is [array]) { $raw = $raw | Select-Object -First 1 }
    $s = [string]$raw

    $sec = 0
    if ([int]::TryParse($s, [ref]$sec)) {
      if ($sec -lt 1) { $sec = 1 }
      return $sec
    }

    $dt = $null
    if ([datetime]::TryParse($s, [ref]$dt)) {
      $delta = [int][math]::Ceiling(($dt.ToUniversalTime() - (Get-Date).ToUniversalTime()).TotalSeconds)
      if ($delta -lt 1) { $delta = 1 }
      return $delta
    }
  } catch {}

  return $null
}

function Get-RetryDelaySeconds {
  param(
    $ErrorRecord,
    [int]$Attempt
  )

  $retryAfter = Get-RetryAfterSeconds -ErrorRecord $ErrorRecord
  if ($retryAfter) { return [math]::Min($retryAfter, 120) }

  $exp = [math]::Pow(2, [math]::Min($Attempt, 6))
  $jitter = Get-Random -Minimum 0 -Maximum 3
  return [int]([math]::Min($exp, 60) + $jitter)
}

function Test-IsRetryableGitHubError {
  param($ErrorRecord)

  $status = Get-HttpStatusCode -ErrorRecord $ErrorRecord
  if ($status -eq $null) { return $true }
  if (@(408, 409, 423, 429, 500, 502, 503, 504) -contains $status) { return $true }
  if ($status -eq 403 -and ([string]$ErrorRecord.Exception.Message -match '(?i)rate limit|secondary rate|abuse')) {
    return $true
  }
  return $false
}

function Try-AcquireRunLock {
  if ($script:HasRunLock) { return $true }

  if (Test-Path $script:RunLockPath) {
    $existingPid = $null
    try {
      $raw = (Get-Content -Path $script:RunLockPath -ErrorAction Stop | Select-Object -First 1).Trim()
      [void][int]::TryParse($raw, [ref]$existingPid)
    } catch {}

    if ($existingPid -and $existingPid -ne $PID -and (Get-Process -Id $existingPid -ErrorAction SilentlyContinue)) {
      Write-Info "Another leaderboard process is already running (pid=$existingPid); exiting."
      return $false
    }

    try { Remove-Item -Path $script:RunLockPath -Force -ErrorAction SilentlyContinue } catch {}
  }

  try {
    Set-Content -Path $script:RunLockPath -Value $PID -Encoding ASCII -NoNewline -Force -ErrorAction Stop
    $script:HasRunLock = $true
    return $true
  } catch {
    Write-Info "WARN: Failed to create lock file '$script:RunLockPath': $($_.Exception.Message)"
    return $false
  }
}

function Release-RunLock {
  if (-not $script:HasRunLock) { return }

  try {
    if (Test-Path $script:RunLockPath) {
      $raw = (Get-Content -Path $script:RunLockPath -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
      if ($raw -eq [string]$PID) {
        Remove-Item -Path $script:RunLockPath -Force -ErrorAction SilentlyContinue
      }
    }
  } catch {}

  $script:HasRunLock = $false
}

function Get-AutostartActionArgs {
  param(
    [Parameter(Mandatory=$true)][int]$LoopIntervalSec,
    [switch]$SkipRoster
  )

  $scriptPathEscaped = '"' + $script:ScriptPath + '"'
  $args = @(
    '-NoProfile',
    '-ExecutionPolicy', 'Bypass',
    '-WindowStyle', 'Hidden',
    '-File', $scriptPathEscaped,
    '-Headless',
    '-IntervalSec', [string]$LoopIntervalSec
  )
  if ($SkipRoster) { $args += '-SkipRoster' }
  return ($args -join ' ')
}

function Ensure-HeadlessAutostartLauncher {
  param(
    [Parameter(Mandatory=$true)][int]$LoopIntervalSec,
    [switch]$SkipRoster
  )

  $psExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
  if (-not (Test-Path $psExe)) {
    throw "PowerShell executable not found at $psExe"
  }
  $cmd = '"{0}" -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "{1}" -Headless -IntervalSec {2}' -f $psExe, $script:ScriptPath, $LoopIntervalSec
  if ($SkipRoster) { $cmd += ' -SkipRoster' }
  $cmdEscaped = ($cmd.Trim() -replace "`r?`n", ' ') -replace '"', '""'

  $vbsPath = Join-Path $script:ScriptRoot 'start_leaderboard_autostart.vbs'
  $vbsBody = "Set shell = CreateObject(""Wscript.Shell"")`r`ncmd = ""$cmdEscaped""`r`nshell.Run cmd, 0, False`r`n"
  [System.IO.File]::WriteAllText($vbsPath, $vbsBody, [System.Text.UTF8Encoding]::new($false))
  return $vbsPath
}

function Install-AutostartTask {
  param(
    [Parameter(Mandatory=$true)][string]$TaskName,
    [Parameter(Mandatory=$true)][int]$LoopIntervalSec,
    [int]$DelaySec = 45,
    [switch]$SkipRoster
  )

  if ($LoopIntervalSec -lt 1) {
    throw "Autostart loop interval must be >= 1 seconds."
  }
  if (-not (Test-Path $script:ScriptPath)) {
    throw "Cannot install autostart: script path not found: $script:ScriptPath"
  }
  if (-not (Get-Command New-ScheduledTaskAction -ErrorAction SilentlyContinue)) {
    throw "ScheduledTasks module/cmdlets are unavailable on this system."
  }

  $wscriptExe = Join-Path $env:SystemRoot 'System32\wscript.exe'
  if (-not (Test-Path $wscriptExe)) {
    throw "wscript executable not found at $wscriptExe"
  }

  $launcherPath = Ensure-HeadlessAutostartLauncher -LoopIntervalSec $LoopIntervalSec -SkipRoster:$SkipRoster
  $action = New-ScheduledTaskAction -Execute $wscriptExe -Argument ('"' + $launcherPath + '"')
  $trigger = New-ScheduledTaskTrigger -AtLogOn
  if ($DelaySec -gt 0) {
    if ($DelaySec -lt 60) { $DelaySec = 60 }
    $delayMins = [int][math]::Ceiling($DelaySec / 60.0)
    $trigger.Delay = "PT${delayMins}M"
  }

  $settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -Hidden `
    -MultipleInstances IgnoreNew `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0)

  $userId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
  $principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive
  $task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description 'PPFD leaderboard sync headless loop'
  Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null
  Write-Info "Autostart task '$TaskName' installed for $userId (IntervalSec=$LoopIntervalSec, DelaySec=$DelaySec)."
}

function Remove-AutostartTask {
  param(
    [Parameter(Mandatory=$true)][string]$TaskName
  )

  $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
  if (-not $existing) {
    Write-Info "Autostart task '$TaskName' was not present."
    return
  }
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop
  Write-Info "Autostart task '$TaskName' removed."
}

function Disable-ConflictingLeaderboardTasks {
  param(
    [Parameter(Mandatory=$true)][string]$KeepTaskName
  )

  $tasks = Get-ScheduledTask -ErrorAction SilentlyContinue | Where-Object {
    $_.TaskName -ne $KeepTaskName -and
    ($_.Actions | Where-Object {
      ($_.Arguments -match '(?i)start_leaderboard\.ps1') -or
      ($_.Arguments -match '(?i)start_leaderboard_silent\.vbs') -or
      ($_.Arguments -match '(?i)start_leaderboard_autostart\.vbs')
    })
  }
  foreach ($task in $tasks) {
    try {
      Disable-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath -ErrorAction Stop | Out-Null
      Write-Info "Disabled conflicting task '$($task.TaskName)' at '$($task.TaskPath)'."
    } catch {
      Write-Info "WARN: Failed to disable conflicting task '$($task.TaskName)': $($_.Exception.Message)"
    }
  }
}

function Get-StatsFingerprint {
  param(
    [Parameter(Mandatory=$true)][string]$StatsDir
  )

  try {
    $latestFiles = Get-ChildItem -Path $StatsDir -Filter 'shift_stats_*.json' -File -ErrorAction Stop |
      Sort-Object LastWriteTimeUtc -Descending |
      Select-Object -First 3
    if (-not $latestFiles) { return 'none' }

    $parts = New-Object System.Collections.Generic.List[string]
    foreach ($f in $latestFiles) {
      $hashVal = $null
      try {
        $h = Get-FileHash -Path $f.FullName -Algorithm SHA256 -ErrorAction Stop
        $hashVal = $h.Hash
      } catch {
        $hashVal = $f.LastWriteTimeUtc.Ticks
      }
      $parts.Add(("{0}|{1}|{2}" -f $f.Name, $f.Length, $hashVal)) | Out-Null
    }
    return ($parts -join ';')
  } catch {
    return 'error'
  }
}

function Get-FileFingerprint {
  param(
    [string]$Path
  )

  if (-not $Path -or -not (Test-Path $Path)) { return 'missing' }

  try {
    $item = Get-Item -Path $Path -ErrorAction Stop
    $hash = (Get-FileHash -Path $Path -Algorithm SHA256 -ErrorAction Stop).Hash
    return ("{0}|{1}|{2}|{3}" -f $item.Name, $item.Length, $item.LastWriteTimeUtc.Ticks, $hash)
  } catch {
    try {
      $item = Get-Item -Path $Path -ErrorAction Stop
      return ("{0}|{1}|{2}|hash-error" -f $item.Name, $item.Length, $item.LastWriteTimeUtc.Ticks)
    } catch {
      return 'error'
    }
  }
}

function Get-SystemSourceMappings {
  param(
    [Parameter(Mandatory=$true)][string]$WorkspaceRoot,
    [Parameter(Mandatory=$true)][string]$RepoRoot
  )

  $pairs = @(
    @{ Base = $WorkspaceRoot; Rel = 'backfill_personnel_stats.py';      Dest = 'backfill_personnel_stats.py' },
    @{ Base = $WorkspaceRoot; Rel = 'install_groupme_task.ps1';         Dest = 'install_groupme_task.ps1' },
    @{ Base = $WorkspaceRoot; Rel = 'README_LEADERBOARD.md';            Dest = 'README_LEADERBOARD.md' },
    @{ Base = $WorkspaceRoot; Rel = 'README_RUN.md';                    Dest = 'README_RUN.md' },
    @{ Base = $WorkspaceRoot; Rel = 'requirements.txt';                 Dest = 'requirements.txt' },
    @{ Base = $WorkspaceRoot; Rel = 'setup_headless.ps1';               Dest = 'setup_headless.ps1' },
    @{ Base = $WorkspaceRoot; Rel = 'Start-Headless.bat';               Dest = 'Start-Headless.bat' },
    @{ Base = $WorkspaceRoot; Rel = 'start_groupme.ps1';                Dest = 'start_groupme.ps1' },
    @{ Base = $WorkspaceRoot; Rel = 'start_leaderboard.ps1';            Dest = 'start_leaderboard.ps1' },
    @{ Base = $WorkspaceRoot; Rel = 'start_leaderboard_autostart.vbs';  Dest = 'start_leaderboard_autostart.vbs' },
    @{ Base = $WorkspaceRoot; Rel = 'start_leaderboard_silent.vbs';     Dest = 'start_leaderboard_silent.vbs' },
    @{ Base = $WorkspaceRoot; Rel = 'sync_stats_to_github.ps1';         Dest = 'sync_stats_to_github.ps1' },
    @{ Base = $WorkspaceRoot; Rel = 'uninstall_groupme_task.ps1';       Dest = 'uninstall_groupme_task.ps1' },
    @{ Base = $WorkspaceRoot; Rel = 'ppfd_groupme_alerts_v1.py';        Dest = 'ppfd_groupme_alerts_v1.py' },
    @{ Base = $WorkspaceRoot; Rel = 'ppfd_leaderboard_calculator.py';   Dest = 'ppfd_leaderboard_calculator.py' },
    @{ Base = $WorkspaceRoot; Rel = 'ppfd_telegram_alerts_v3.2.py';     Dest = 'ppfd_telegram_alerts_v3.2.py' },
    @{ Base = $RepoRoot;      Rel = '.github\workflows\pages-data.yml'; Dest = '.github/workflows/pages-data.yml' },
    @{ Base = $RepoRoot;      Rel = '.gitignore';                       Dest = '.gitignore' },
    @{ Base = $RepoRoot;      Rel = 'README.md';                        Dest = 'README.md' },
    @{ Base = $RepoRoot;      Rel = 'restart_all.bat';                  Dest = 'restart_all.bat' },
    @{ Base = $RepoRoot;      Rel = 'scripts\generate_leaderboard.py';  Dest = 'scripts/generate_leaderboard.py' },
    @{ Base = $RepoRoot;      Rel = 'scripts\install_tasks.ps1';        Dest = 'scripts/install_tasks.ps1' },
    @{ Base = $RepoRoot;      Rel = 'scripts\restart_all.ps1';          Dest = 'scripts/restart_all.ps1' },
    @{ Base = $RepoRoot;      Rel = 'scripts\start_alerts.ps1';         Dest = 'scripts/start_alerts.ps1' },
    @{ Base = $RepoRoot;      Rel = 'scripts\start_updater.ps1';        Dest = 'scripts/start_updater.ps1' }
  )

  $items = New-Object System.Collections.Generic.List[object]
  foreach ($pair in $pairs) {
    $sourcePath = Join-Path $pair.Base $pair.Rel
    if (-not (Test-Path $sourcePath)) { continue }
    $items.Add([pscustomobject]@{
      SourcePath = $sourcePath
      DestPath   = $pair.Dest
      Fingerprint = Get-FileFingerprint -Path $sourcePath
    }) | Out-Null
  }

  return $items | Sort-Object DestPath
}

function Get-SystemSourceFingerprint {
  param(
    [Parameter(Mandatory=$true)][object[]]$Mappings
  )

  if (-not $Mappings -or $Mappings.Count -eq 0) { return 'none' }

  $parts = New-Object System.Collections.Generic.List[string]
  foreach ($mapping in $Mappings) {
    $parts.Add(("{0}|{1}" -f $mapping.DestPath, $mapping.Fingerprint)) | Out-Null
  }
  return ($parts -join ';;')
}

function Sync-SystemSourceToGitHub {
  param(
    [Parameter(Mandatory=$true)][string]$Owner,
    [Parameter(Mandatory=$true)][string]$Repo,
    [Parameter(Mandatory=$true)][string]$Branch,
    [Parameter(Mandatory=$true)][string]$Token,
    [Parameter(Mandatory=$true)][string]$ApiBase,
    [Parameter(Mandatory=$true)][object[]]$Mappings
  )

  if (-not $Mappings -or $Mappings.Count -eq 0) {
    Write-Info "No system source files found for GitHub sync; skipping."
    return
  }

  $msg = "Sync system source $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
  foreach ($mapping in $Mappings) {
    Push-GitHubFile -Owner $Owner -Repo $Repo -Branch $Branch -DestPath $mapping.DestPath -SourcePath $mapping.SourcePath -Token $Token -ApiBase $ApiBase -Message $msg | Out-Null
  }
}

function Get-GitHubToken {
  $envTok = ([string]$env:GITHUB_TOKEN).Trim()
  if ($envTok) { return $envTok }

  $candidates = @(
    (Join-Path $script:StateRoot 'GithubToken.txt'),
    (Join-Path $script:StateRoot 'Githubclassictoken.txt'),
    (Join-Path $script:ScriptRoot 'GithubToken.txt'),
    (Join-Path $script:ScriptRoot 'Githubclassictoken.txt'),
    (Join-Path (Get-Location) 'GithubToken.txt')
  )
  foreach ($fp in $candidates) {
    try {
      if (-not (Test-Path $fp)) { continue }
      $tok = (Get-Content -Path $fp -ErrorAction Stop | Select-Object -First 1).Trim()
      if ($tok) { return $tok }
    } catch {}
  }
  return $null
}

function Resolve-GitDir {
  param(
    [Parameter(Mandatory=$true)][string]$RepoRoot
  )

  $dotgit = Join-Path $RepoRoot '.git'
  if (Test-Path $dotgit -PathType Container) { return $dotgit }
  if (Test-Path $dotgit -PathType Leaf) {
    try {
      $raw = Get-Content -Path $dotgit -Raw -ErrorAction Stop
      $m = [regex]::Match($raw, '(?im)^[ \t]*gitdir:\s*(.+?)\s*$')
      if (-not $m.Success) { return $null }
      $gitDir = $m.Groups[1].Value.Trim()
      if (-not $gitDir) { return $null }
      if (-not ([System.IO.Path]::IsPathRooted($gitDir))) {
        $gitDir = Join-Path $RepoRoot $gitDir
      }
      $gitDir = [System.IO.Path]::GetFullPath($gitDir)
      if (Test-Path $gitDir -PathType Container) { return $gitDir }
    } catch {}
  }
  return $null
}

function Get-GitHeadSha {
  param(
    [Parameter(Mandatory=$true)][string]$RepoRoot
  )

  $gitDir = Resolve-GitDir -RepoRoot $RepoRoot
  if (-not $gitDir) { return $null }

  try {
    $headPath = Join-Path $gitDir 'HEAD'
    $head = (Get-Content -Path $headPath -ErrorAction Stop | Select-Object -First 1).Trim()
    if (-not $head) { return $null }

    if ($head -match '^[0-9a-fA-F]{40}$') {
      return $head.ToLowerInvariant()
    }

    $m = [regex]::Match($head, '^ref:\s+(.+)$')
    if (-not $m.Success) { return $null }

    $refRel = $m.Groups[1].Value.Trim().Replace('/', '\')
    $refPath = Join-Path $gitDir $refRel
    if (Test-Path $refPath) {
      $sha = (Get-Content -Path $refPath -ErrorAction Stop | Select-Object -First 1).Trim()
      if ($sha -match '^[0-9a-fA-F]{40}$') { return $sha.ToLowerInvariant() }
    }

    $packedRefs = Join-Path $gitDir 'packed-refs'
    if (Test-Path $packedRefs) {
      $refName = $m.Groups[1].Value.Trim()
      $lines = Get-Content -Path $packedRefs -ErrorAction SilentlyContinue
      foreach ($line in $lines) {
        if (-not $line -or $line.StartsWith('#') -or $line.StartsWith('^')) { continue }
        $parts = $line -split '\s+', 2
        if ($parts.Count -lt 2) { continue }
        if ($parts[1].Trim() -eq $refName -and $parts[0] -match '^[0-9a-fA-F]{40}$') {
          return $parts[0].ToLowerInvariant()
        }
      }
    }
  } catch {}

  return $null
}

function Get-GitHubRepoContext {
  param(
    [Parameter(Mandatory=$true)][string]$RepoRoot
  )

  $owner = ([string]$env:GITHUB_OWNER).Trim()
  $repo  = ([string]$env:GITHUB_REPO).Trim()
  $branch = ([string]$env:GITHUB_BRANCH).Trim()
  if (-not $branch) { $branch = ([string]$env:GITHUB_REF_NAME).Trim() }
  if (-not $branch) { $branch = ([string]$env:GITHUB_STATS_BRANCH).Trim() }

  if (-not $owner -or -not $repo) {
    $repoSpec = ([string]$env:GITHUB_REPOSITORY).Trim()
    if (-not $repoSpec) { $repoSpec = ([string]$env:GITHUB_STATS_REPO).Trim() }
    if ($repoSpec) {
      $mRepo = [regex]::Match($repoSpec, '^\s*([^/\s]+)/([^/\s]+)\s*$')
      if ($mRepo.Success) {
        if (-not $owner) { $owner = $mRepo.Groups[1].Value }
        if (-not $repo) { $repo = $mRepo.Groups[2].Value }
      }
    }
  }

  $gitDir = Resolve-GitDir -RepoRoot $RepoRoot
  if (-not $branch -and $gitDir) {
    try {
      $head = (Get-Content -Path (Join-Path $gitDir 'HEAD') -ErrorAction Stop | Select-Object -First 1).Trim()
      $m = [regex]::Match($head, '^(?i)ref:\s*refs/heads/(.+?)\s*$')
      if ($m.Success) { $branch = $m.Groups[1].Value.Trim() }
    } catch {}
  }
  if (-not $branch) { $branch = 'main' }

  if (-not $owner -or -not $repo) {
    if ($gitDir) {
      try {
        $configPath = Join-Path $gitDir 'config'
        $lines = Get-Content -Path $configPath -ErrorAction Stop
        $inOrigin = $false
        $originUrl = $null
        foreach ($raw in $lines) {
          $line = ([string]$raw).Trim()
          if (-not $line -or $line.StartsWith('#') -or $line.StartsWith(';')) { continue }
          if ($line.StartsWith('[') -and $line.EndsWith(']')) {
            $inOrigin = ($line -ieq '[remote "origin"]')
            continue
          }
          if (-not $inOrigin) { continue }
          $m = [regex]::Match($line, '^(?i)url\s*=\s*(.+?)\s*$')
          if ($m.Success) { $originUrl = $m.Groups[1].Value.Trim(); break }
        }

        if ($originUrl) {
          $m1 = [regex]::Match($originUrl, '^(?i)git@github\.com:(.+?)/(.+?)(?:\.git)?$')
          $m2 = [regex]::Match($originUrl, '^(?i)(?:https?|ssh)://(?:[^@/\s]+@)?github\.com[:/](.+?)/(.+?)(?:\.git)?/?$')
          if ($m1.Success) { $owner = $m1.Groups[1].Value; $repo = $m1.Groups[2].Value }
          elseif ($m2.Success) { $owner = $m2.Groups[1].Value; $repo = $m2.Groups[2].Value }
        }
      } catch {}
    }
  }

  return [pscustomobject]@{
    Owner  = $owner
    Repo   = $repo
    Branch = $branch
    ApiBase = if ($env:GITHUB_API_URL) { $env:GITHUB_API_URL.Trim().TrimEnd('/') } else { 'https://api.github.com' }
  }
}

function Get-GitHubFileMeta {
  param(
    [Parameter(Mandatory=$true)][string]$Owner,
    [Parameter(Mandatory=$true)][string]$Repo,
    [Parameter(Mandatory=$true)][string]$Branch,
    [Parameter(Mandatory=$true)][string]$Path,
    [Parameter(Mandatory=$true)][string]$Token,
    [Parameter(Mandatory=$true)][string]$ApiBase
  )

  $url = "$ApiBase/repos/$Owner/$Repo/contents/$Path`?ref=$Branch"
  $maxAttempts = 5
  for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    try {
      $resp = Invoke-RestMethod -Method Get -Uri $url -Headers @{
        Authorization = "Bearer $Token"
        'User-Agent'  = 'PPFD-Leaderboard-Updater'
        Accept        = 'application/vnd.github+json'
      } -TimeoutSec 30
      break
    } catch {
      $status = Get-HttpStatusCode -ErrorRecord $_
      if ($status -eq 404) {
        return [pscustomobject]@{ Sha = $null; Bytes = $null }
      }
      if (($attempt -ge $maxAttempts) -or (-not (Test-IsRetryableGitHubError -ErrorRecord $_))) {
        throw
      }
      $delay = Get-RetryDelaySeconds -ErrorRecord $_ -Attempt $attempt
      Write-Info "WARN: GitHub metadata request failed for $Path (status=$status): $($_.Exception.Message). Retrying in $delay sec..."
      Start-Sleep -Seconds $delay
    }
  }

  $sha = $resp.sha
  $bytes = $null
  try {
    if ($resp.encoding -eq 'base64' -and $resp.content) {
      $b64 = ($resp.content -replace '\s', '')
      $bytes = [System.Convert]::FromBase64String($b64)
    }
  } catch {
    $bytes = $null
  }

  return [pscustomobject]@{ Sha = $sha; Bytes = $bytes }
}

function Push-GitHubFile {
  param(
    [Parameter(Mandatory=$true)][string]$Owner,
    [Parameter(Mandatory=$true)][string]$Repo,
    [Parameter(Mandatory=$true)][string]$Branch,
    [Parameter(Mandatory=$true)][string]$DestPath,
    [Parameter(Mandatory=$true)][string]$SourcePath,
    [Parameter(Mandatory=$true)][string]$Token,
    [Parameter(Mandatory=$true)][string]$ApiBase,
    [Parameter(Mandatory=$true)][string]$Message
  )

  if (-not (Test-Path $SourcePath)) {
    Write-Info "WARN: Missing file, skipping push: $SourcePath"
    return $false
  }

  $localBytes = [System.IO.File]::ReadAllBytes($SourcePath)
  $maxAttempts = 5

  for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    $meta = Get-GitHubFileMeta -Owner $Owner -Repo $Repo -Branch $Branch -Path $DestPath -Token $Token -ApiBase $ApiBase
    if ($meta.Bytes -ne $null -and $meta.Bytes.Length -eq $localBytes.Length) {
      $same = $true
      for ($i = 0; $i -lt $localBytes.Length; $i++) {
        if ($localBytes[$i] -ne $meta.Bytes[$i]) { $same = $false; break }
      }
      if ($same) {
        Write-Info "No changes detected for $DestPath; skipping GitHub push."
        return $true
      }
    }

    $b64 = [System.Convert]::ToBase64String($localBytes)
    $payload = @{
      message = $Message
      content = $b64
      branch  = $Branch
    }
    if ($meta.Sha) { $payload.sha = $meta.Sha }

    $url = "$ApiBase/repos/$Owner/$Repo/contents/$DestPath"
    try {
      $resp2 = Invoke-RestMethod -Method Put -Uri $url -Headers @{
        Authorization = "Bearer $Token"
        'User-Agent'  = 'PPFD-Leaderboard-Updater'
        Accept        = 'application/vnd.github+json'
      } -ContentType 'application/json' -Body ($payload | ConvertTo-Json -Depth 6) -TimeoutSec 30

      $shaShort = $null
      try { $shaShort = ($resp2.content.sha.Substring(0,7)) } catch {}
      if ($shaShort) {
        Write-Info "Pushed $DestPath (${Branch}), commit: $shaShort"
      } else {
        Write-Info "Pushed $DestPath (${Branch})"
      }
      return $true
    } catch {
      $status = Get-HttpStatusCode -ErrorRecord $_
      if (($attempt -ge $maxAttempts) -or (-not (Test-IsRetryableGitHubError -ErrorRecord $_))) {
        throw
      }

      $delay = Get-RetryDelaySeconds -ErrorRecord $_ -Attempt $attempt
      Write-Info "WARN: GitHub push failed for $DestPath (status=$status): $($_.Exception.Message). Retrying in $delay sec..."
      Start-Sleep -Seconds $delay
    }
  }

  throw "Push failed for $DestPath after $maxAttempts attempts."
}

function Resolve-StatsDir {
  param(
    [Parameter(Mandatory=$true)][string]$BaseDir
  )

  $candidates = @(
    (Join-Path $BaseDir 'data\shift_stats'),
    (Join-Path $BaseDir 'ppfd-alerts\data\shift_stats')
  )

  $bestPath = $null
  $bestTicks = -1
  foreach ($candidate in $candidates) {
    if (-not $candidate -or -not (Test-Path $candidate)) { continue }

    try {
      $resolved = (Resolve-Path $candidate -ErrorAction Stop).Path
    } catch {
      continue
    }

    $latest = Get-ChildItem -Path $resolved -Filter 'shift_stats_*.json' -File -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTimeUtc -Descending |
      Select-Object -First 1

    if ($latest) {
      $ticks = $latest.LastWriteTimeUtc.Ticks
      if ($ticks -gt $bestTicks) {
        $bestTicks = $ticks
        $bestPath = $resolved
      }
    } elseif (-not $bestPath) {
      $bestPath = $resolved
    }
  }

  return $bestPath
}

function Get-DateFromFileName {
  param(
    [Parameter(Mandatory=$true)][string]$FileName,
    [Parameter(Mandatory=$true)][string]$Pattern
  )

  $m = [regex]::Match($FileName, $Pattern)
  if (-not $m.Success) { return $null }
  try {
    return [datetime]::ParseExact(
      $m.Groups[1].Value,
      'yyyy-MM-dd',
      [System.Globalization.CultureInfo]::InvariantCulture
    ).Date
  } catch {
    return $null
  }
}

function Get-ShiftStatsEntries {
  param(
    [Parameter(Mandatory=$true)][string]$StatsDir
  )

  $items = New-Object System.Collections.Generic.List[object]
  $files = Get-ChildItem -Path $StatsDir -Filter 'shift_stats_*.json' -File -ErrorAction SilentlyContinue
  foreach ($f in $files) {
    $d = Get-DateFromFileName -FileName $f.Name -Pattern '^shift_stats_(\d{4}-\d{2}-\d{2})\.json$'
    if (-not $d) { continue }
    $items.Add([pscustomobject]@{
      Date = $d
      Path = $f.FullName
      Name = $f.Name
    }) | Out-Null
  }
  return $items | Sort-Object Date
}

function Get-TotalCallsFromJsonFile {
  param(
    [Parameter(Mandatory=$true)][string]$Path
  )

  if (-not (Test-Path $Path)) { return 0 }
  try {
    $raw = Get-Content -Path $Path -Raw -ErrorAction Stop
    if (-not $raw) { return 0 }
    $obj = $raw | ConvertFrom-Json -ErrorAction Stop
    if (-not $obj -or -not $obj.calls) { return 0 }
    $sum = 0
    foreach ($p in $obj.calls.PSObject.Properties) {
      try { $sum += [int]([double]$p.Value) } catch {}
    }
    return [int]$sum
  } catch {
    return 0
  }
}

function Get-LatestRosterDate {
  param(
    [Parameter(Mandatory=$true)][string]$RosterDir
  )

  $latest = $null
  $files = Get-ChildItem -Path $RosterDir -Filter 'roster_units_*.json' -File -ErrorAction SilentlyContinue
  foreach ($f in $files) {
    $d = Get-DateFromFileName -FileName $f.Name -Pattern '^roster_units_(\d{4}-\d{2}-\d{2})\.json$'
    if (-not $d) { continue }
    if (-not $latest -or $d -gt $latest) { $latest = $d }
  }
  return $latest
}

function Get-BadPersonnelDates {
  param(
    [Parameter(Mandatory=$true)][object[]]$StatsEntries,
    [Parameter(Mandatory=$true)][string]$PersonnelDir
  )

  $bad = New-Object System.Collections.Generic.List[datetime]
  foreach ($entry in $StatsEntries) {
    $statsCalls = Get-TotalCallsFromJsonFile -Path $entry.Path
    if ($statsCalls -le 0) { continue }

    $dateLabel = $entry.Date.ToString('yyyy-MM-dd')
    $personnelPath = Join-Path $PersonnelDir ("shift_personnel_{0}.json" -f $dateLabel)
    if (-not (Test-Path $personnelPath)) {
      $bad.Add($entry.Date) | Out-Null
      continue
    }

    $personnelCalls = Get-TotalCallsFromJsonFile -Path $personnelPath
    if ($personnelCalls -le 0) {
      $bad.Add($entry.Date) | Out-Null
    }
  }
  return $bad | Sort-Object -Unique
}

function Invoke-BackfillPersonnelScript {
  param(
    [Parameter(Mandatory=$true)][string]$Python,
    [Parameter(Mandatory=$true)][string]$StatsDir,
    [Parameter(Mandatory=$true)][string]$RosterDir,
    [Parameter(Mandatory=$true)][string]$PersonnelDir,
    [datetime]$StartDate,
    [datetime]$EndDate
  )

  $here = $script:ScriptRoot
  $backfillScript = Join-Path $here 'backfill_personnel_stats.py'
  if (-not (Test-Path $backfillScript)) {
    Write-Info "WARN: backfill_personnel_stats.py not found; skipping backfill catch-up."
    return $false
  }

  $args = @(
    $backfillScript,
    '--stats-dir', $StatsDir,
    '--roster-dir', $RosterDir,
    '--out-dir', $PersonnelDir,
    '--export-missing',
    '--overwrite'
  )

  if ($StartDate) { $args += @('--start', $StartDate.ToString('yyyy-MM-dd')) }
  if ($EndDate) { $args += @('--end', $EndDate.ToString('yyyy-MM-dd')) }

  $insecureEnv = ([string]$env:BACKFILL_INSECURE).Trim().ToLowerInvariant()
  if (@('1', 'true', 'yes', 'y', 'on') -contains $insecureEnv) {
    $args += '--insecure'
  }
  $caBundle = ([string]$env:BACKFILL_CA_BUNDLE).Trim()
  if ($caBundle) {
    $args += @('--ca-bundle', $caBundle)
  }

  & $Python @args
  $exitCode = $LASTEXITCODE
  if ($exitCode -ne 0) {
    Write-Info "WARN: backfill_personnel_stats.py failed (exit $exitCode)."
    return $false
  }
  return $true
}

function Invoke-BackfillCatchup {
  param(
    [Parameter(Mandatory=$true)][string]$Python,
    [Parameter(Mandatory=$true)][string]$StatsDir,
    [Parameter(Mandatory=$true)][string]$RosterDir,
    [Parameter(Mandatory=$true)][string]$PersonnelDir,
    [Parameter(Mandatory=$true)][string]$StatsFingerprint
  )

  if ($script:LastBackfillFingerprint -and $script:LastBackfillFingerprint -eq $StatsFingerprint) {
    Set-BackfillStatus -Status 'skipped' -Action 'none' -Message 'Backfill check skipped (fingerprint unchanged).'
    return
  }

  $script:LastBackfillFingerprint = $StatsFingerprint

  if (-not (Test-Path $StatsDir)) {
    Set-BackfillStatus -Status 'skipped' -Action 'none' -Message "Stats dir not found: $StatsDir"
    return
  }
  if (-not (Test-Path $RosterDir)) {
    Write-Info "WARN: Roster dir not found for catch-up: $RosterDir"
    Set-BackfillStatus -Status 'failed' -Action 'none' -Message "Roster dir not found: $RosterDir"
    return
  }

  $statsEntries = @(Get-ShiftStatsEntries -StatsDir $StatsDir)
  if (-not $statsEntries -or $statsEntries.Count -eq 0) {
    Set-BackfillStatus -Status 'skipped' -Action 'none' -Message 'No shift_stats files found for backfill.'
    return
  }

  $latestStatsDate = ($statsEntries | Select-Object -Last 1).Date
  if (-not $latestStatsDate) {
    Set-BackfillStatus -Status 'skipped' -Action 'none' -Message 'Unable to determine latest shift stats date.'
    return
  }

  $badDates = @(Get-BadPersonnelDates -StatsEntries $statsEntries -PersonnelDir $PersonnelDir)
  if ($badDates.Count -gt 0) {
    $firstBad = ($badDates | Select-Object -First 1).ToString('yyyy-MM-dd')
    $lastBad = ($badDates | Select-Object -Last 1).ToString('yyyy-MM-dd')
    $gapSignature = (($badDates | ForEach-Object { $_.ToString('yyyy-MM-dd') }) -join ',')
    if ($script:FullBackfillCompleted -and $script:LastSuccessfulBackfillGapSignature -eq $gapSignature) {
      Set-BackfillStatus -Status 'skipped' -Action 'none' -Message "Known personnel gap unchanged ($firstBad..$lastBad); backfill already attempted."
      return
    }
    Write-Info "Detected $($badDates.Count) personnel gap day(s) ($firstBad..$lastBad); running full backfill."
    $ok = Invoke-BackfillPersonnelScript -Python $Python -StatsDir $StatsDir -RosterDir $RosterDir -PersonnelDir $PersonnelDir
    if ($ok) {
      $script:FullBackfillCompleted = $true
      $script:LastSuccessfulBackfillGapSignature = $gapSignature
      Write-Info "Full personnel backfill completed."
      Set-BackfillStatus -Status 'success' -Action 'full' -Message "Full personnel backfill completed for gap days ($firstBad..$lastBad)." -StartDate (($badDates | Select-Object -First 1)) -EndDate (($badDates | Select-Object -Last 1))
    } else {
      Set-BackfillStatus -Status 'failed' -Action 'full' -Message "Full personnel backfill failed for gap days ($firstBad..$lastBad)." -StartDate (($badDates | Select-Object -First 1)) -EndDate (($badDates | Select-Object -Last 1))
    }
    return
  }

  $latestRosterDate = Get-LatestRosterDate -RosterDir $RosterDir
  if (-not $latestRosterDate) {
    Write-Info "No roster_units_*.json files detected; running full backfill."
    $ok = Invoke-BackfillPersonnelScript -Python $Python -StatsDir $StatsDir -RosterDir $RosterDir -PersonnelDir $PersonnelDir
    if ($ok) {
      $script:FullBackfillCompleted = $true
      Write-Info "Full personnel backfill completed."
      Set-BackfillStatus -Status 'success' -Action 'full' -Message 'Full personnel backfill completed (no roster_units files found).'
    } else {
      Set-BackfillStatus -Status 'failed' -Action 'full' -Message 'Full personnel backfill failed (no roster_units files found).'
    }
    return
  }

  if ($latestRosterDate -lt $latestStatsDate) {
    $start = $latestRosterDate.AddDays(1).Date
    $end = $latestStatsDate.Date
    Write-Info "Roster lag detected ($($latestRosterDate.ToString('yyyy-MM-dd')) -> $($latestStatsDate.ToString('yyyy-MM-dd'))); backfilling range."
    $ok = Invoke-BackfillPersonnelScript -Python $Python -StatsDir $StatsDir -RosterDir $RosterDir -PersonnelDir $PersonnelDir -StartDate $start -EndDate $end
    if ($ok) {
      Set-BackfillStatus -Status 'success' -Action 'range' -Message "Range backfill completed ($($start.ToString('yyyy-MM-dd'))..$($end.ToString('yyyy-MM-dd')))." -StartDate $start -EndDate $end
    } else {
      Set-BackfillStatus -Status 'failed' -Action 'range' -Message "Range backfill failed ($($start.ToString('yyyy-MM-dd'))..$($end.ToString('yyyy-MM-dd')))." -StartDate $start -EndDate $end
    }
    return
  }

  Set-BackfillStatus -Status 'skipped' -Action 'none' -Message 'Backfill check complete; no gaps detected.'
}

function Invoke-ErrorHook {
  param(
    [string]$Message
  )

  $now = Get-Date
  $msg = if ($Message) { $Message } else { "Unknown error" }

  try {
    $headers = @{ "Title" = "PPFD Leaderboard Error" }
    Invoke-WebRequest -Uri "https://ntfy.sh/Zjw499" -Method Post -Headers $headers -Body $msg -TimeoutSec 10 | Out-Null
  } catch {
    Write-Info "WARN: Failed to send ntfy notification: $($_.Exception.Message)"
  }

  if ($Headless) {
    Write-Info "Headless mode: skipping interactive error console."
    return
  }

  try {
    $psExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    $msgSafe = ($msg -replace "`r?`n", " ") -replace "'", "''"
    $cmd = "& { Write-Host 'PPFD leaderboard update failed' -ForegroundColor Red; Write-Host ('Time: ' + (Get-Date)); Write-Host ''; Write-Host 'Error: $msgSafe'; Write-Host ''; Read-Host 'Press Enter to close'; }"
    Start-Process -FilePath $psExe -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-NoExit", "-Command", $cmd) -WindowStyle Normal | Out-Null
  } catch {
    Write-Info "WARN: Failed to launch error console: $($_.Exception.Message)"
  }
}

function Invoke-LeaderboardRun {
  param(
    [switch]$SkipRoster,
    [switch]$DryRun
  )

  try {
    $here = $script:ScriptRoot
    $stateRoot = $script:StateRoot
    $repoRoot = $script:CodeRepoRoot
    $generatedRoot = $script:GeneratedRoot
    Set-Location $stateRoot

    # Prefer the same venv used by the alerts script if present
    $venvPy = Join-Path $stateRoot 'venv\Scripts\python.exe'
    if (Test-Path $venvPy) {
      $python = $venvPy
    } else {
      $python = (Get-Command python -ErrorAction SilentlyContinue).Source
      if (-not $python) {
        $python = (Get-Command py -ErrorAction SilentlyContinue).Source
      }
    }

    if (-not $python) {
      throw "Python not found. Install Python or ensure it is on PATH."
    }

    # Resolve stats directory from the freshest available source.
    $statsDir = Resolve-StatsDir -BaseDir $stateRoot

    if ($statsDir) {
      $env:SHIFT_STATS_DIR = $statsDir
    }

    $personnelStats = Join-Path $stateRoot 'data\shift_personnel'
    if (Test-Path $personnelStats) {
      try {
        $env:PERSONNEL_STATS_DIR = (Resolve-Path $personnelStats).Path
      } catch {
        Write-Info "WARN: Failed to resolve personnel stats dir: $($_.Exception.Message)"
      }
    } else {
      $env:PERSONNEL_STATS_DIR = $personnelStats
    }

    # Always set ROSTER_DIR when TSlogs exists (even when -SkipRoster is used),
    # so downstream generators can still resolve roster_units JSON files.
    $tsLogs = Join-Path $stateRoot 'TSlogs'
    if (Test-Path $tsLogs) {
      try {
        $env:ROSTER_DIR = (Resolve-Path $tsLogs).Path
      } catch {
        Write-Info "WARN: Failed to resolve TSlogs path: $($_.Exception.Message)"
      }
    }

    if ($DryRun -or $SkipRoster) {
      if ($DryRun) {
        Write-Info "DryRun enabled; skipping roster export."
      } elseif ($SkipRoster) {
        Write-Info "SkipRoster enabled; skipping roster export."
      }
    } else {
      # Export roster + build roster_units JSON for UI popups
      if (Test-Path $tsLogs) {
        $exportScript = Join-Path $tsLogs 'export_roster.py'
        $unitsScript = Join-Path $tsLogs 'roster_units.py'

        $now = Get-Date
        $shiftStart = Get-Date -Hour 7 -Minute 0 -Second 0
        if ($now -lt $shiftStart) {
          $shiftDate = $now.Date.AddDays(-1)
        } else {
          $shiftDate = $now.Date
        }
        $priorDate = $shiftDate.AddDays(-1)

        if (Test-Path $exportScript) {
          $loginFile = Join-Path $tsLogs 'KRONOSLOGIN.txt'
          Push-Location $tsLogs
          try {
            foreach ($d in @($shiftDate, $priorDate)) {
              $dateLabel = $d.ToString('yyyy-MM-dd')
              Write-Info "Exporting roster ($dateLabel)..."
              if (Test-Path $loginFile) {
                & $python $exportScript --login-file $loginFile --date $dateLabel
              } else {
                Write-Info "WARN: KRONOSLOGIN.txt not found; running export without --login-file."
                & $python $exportScript --date $dateLabel
              }
              if ($LASTEXITCODE -ne 0) {
                Write-Info "WARN: roster export failed (exit $LASTEXITCODE)"
              }
            }
          } catch {
            Write-Info "WARN: roster export failed: $($_.Exception.Message)"
          } finally {
            Pop-Location
          }
        } else {
          Write-Info "WARN: export_roster.py not found; skipping roster export."
        }

        if (Test-Path $unitsScript) {
          try {
            foreach ($d in @($shiftDate, $priorDate)) {
              $dateLabel = $d.ToString('yyyy-MM-dd')
              $rosterFile = Join-Path $tsLogs ("roster_{0}.xlsx" -f $dateLabel)
              if (Test-Path $rosterFile) {
                Write-Info "Parsing roster to unit staffing JSON ($dateLabel)..."
                & $python $unitsScript --input $rosterFile
                if ($LASTEXITCODE -ne 0) {
                  Write-Info "WARN: roster_units.py failed (exit $LASTEXITCODE)"
                }
              } else {
                Write-Info "WARN: roster file not found for $dateLabel; skipping parse."
              }
            }
          } catch {
            Write-Info "WARN: roster_units.py failed: $($_.Exception.Message)"
          }
        } else {
          Write-Info "WARN: roster_units.py not found; skipping roster parsing."
        }
      } else {
        Write-Info "WARN: TSlogs folder not found; skipping roster export."
      }
    }

    $fp = if ($statsDir) { Get-StatsFingerprint -StatsDir $statsDir } else { 'none' }
    if (-not $DryRun -and $statsDir -and (Test-Path $statsDir) -and $tsLogs -and (Test-Path $tsLogs)) {
      $personnelDir = if ($env:PERSONNEL_STATS_DIR) { $env:PERSONNEL_STATS_DIR } else { $personnelStats }
      try {
        Invoke-BackfillCatchup -Python $python -StatsDir $statsDir -RosterDir $tsLogs -PersonnelDir $personnelDir -StatsFingerprint $fp
      } catch {
        Write-Info "WARN: Backfill catch-up failed: $($_.Exception.Message)"
        Set-BackfillStatus -Status 'failed' -Action 'none' -Message 'Backfill catch-up threw an exception.' -ErrorMessage $_.Exception.Message
      }
    }

    $genScript = Join-Path $repoRoot 'scripts\generate_leaderboard.py'
    $indexOut = Join-Path $repoRoot 'docs\index.html'
    $outData = Join-Path $generatedRoot 'docs\data.json'
    $outRoster = Join-Path $generatedRoot 'docs\roster_units.json'
    $versionOut = Join-Path $generatedRoot 'docs\version.json'
    $backfillStatusOut = Join-Path $generatedRoot 'docs\backfill_status.json'
    $legacyCalc = Join-Path $here 'ppfd_leaderboard_calculator.py'
    $legacyOut = Join-Path $generatedRoot 'data\leaderboards.json'
    foreach ($dir in @(
      (Split-Path -Parent $outData),
      (Split-Path -Parent $legacyOut)
    )) {
      if ($dir -and -not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
      }
    }
    Write-BackfillStatusFile -Path $backfillStatusOut
    $runFingerprint = @(
      $fp,
      (Get-FileFingerprint -Path $genScript),
      (Get-FileFingerprint -Path $indexOut)
    ) -join ';;'
    $sourceMappings = Get-SystemSourceMappings -WorkspaceRoot $here -RepoRoot $repoRoot
    $sourceFingerprint = Get-SystemSourceFingerprint -Mappings $sourceMappings

    $shouldUpdateLeaderboard = $true
    if ($script:IsLoop -and -not $DryRun -and $script:LastRunFingerprint -and $runFingerprint -eq $script:LastRunFingerprint) {
      $shouldUpdateLeaderboard = $false
    }

    $shouldSyncSystemSource = $false
    if (-not $DryRun) {
      if (-not $script:LastSourceSyncFingerprint -or $sourceFingerprint -ne $script:LastSourceSyncFingerprint) {
        $shouldSyncSystemSource = $true
      }
    }

    if ($script:IsLoop -and -not $DryRun -and -not $shouldUpdateLeaderboard -and -not $shouldSyncSystemSource) {
      Write-Info "No leaderboard or system source changes detected; skipping GitHub update."
      return $true
    }

    if ($DryRun) {
      Write-Info "DryRun enabled; generating output but skipping GitHub push."
    }

    if (-not (Test-Path $genScript)) {
      throw "Generator not found: $genScript"
    }

    if ($shouldUpdateLeaderboard) {
      if (Test-Path $legacyCalc) {
        Write-Info "Generating local leaderboards.json:"
        Write-Info "  OUT=$legacyOut"
        & $python $legacyCalc --stats-dir $statsDir --output $legacyOut --no-git
        if ($LASTEXITCODE -ne 0) {
          Write-Info "WARN: Legacy leaderboard JSON generation failed (exit $LASTEXITCODE)."
        }
      }

      Write-Info "Generating GitHub Pages data.json:"
      Write-Info "  PYTHON=$python"
      if ($statsDir) { Write-Info "  SHIFT_STATS_DIR=$statsDir" }
      Write-Info "  OUT=$outData"

      & $python $genScript --stats-dir $statsDir --out $outData --roster-out $outRoster
      if ($LASTEXITCODE -ne 0) {
        $hex = if ($LASTEXITCODE -lt 0) { ('0x{0:X8}' -f ([uint32]$LASTEXITCODE)) } else { $null }
        if ($hex) { throw "Leaderboard generator failed (exit $LASTEXITCODE / $hex)." }
        throw "Leaderboard generator failed (exit $LASTEXITCODE)."
      }

      try {
        $headSha = Get-GitHeadSha -RepoRoot $repoRoot
        $headShort = if ($headSha) { $headSha.Substring(0,7) } else { 'unknown' }
        $builtAtUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
        $buildId = ((Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmss') + '-' + $headShort)
        $dataSha = if (Test-Path $outData) { (Get-FileHash -Path $outData -Algorithm SHA256 -ErrorAction Stop).Hash.ToLowerInvariant() } else { $null }
        $indexSha = if (Test-Path $indexOut) { (Get-FileHash -Path $indexOut -Algorithm SHA256 -ErrorAction Stop).Hash.ToLowerInvariant() } else { $null }
        $versionPayload = [ordered]@{
          generated_at      = $builtAtUtc
          commit            = $headSha
          commit_short      = $headShort
          build_id          = $buildId
          data_sha256       = $dataSha
          index_sha256      = $indexSha
          stats_fingerprint = $fp
        }
        $versionJson = $versionPayload | ConvertTo-Json -Depth 6
        $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
        [System.IO.File]::WriteAllText($versionOut, $versionJson, $utf8NoBom)
      } catch {
        Write-Info "WARN: Failed to write docs/version.json: $($_.Exception.Message)"
      }
    } else {
      Write-Info "Leaderboard inputs unchanged; skipping regeneration."
    }

    if (-not $DryRun) {
      $token = Get-GitHubToken
      if (-not $token) { throw "GitHub token not found (set GITHUB_TOKEN, GithubToken.txt, or Githubclassictoken.txt)." }
      $ctx = Get-GitHubRepoContext -RepoRoot $repoRoot
      if (-not $ctx.Owner -or -not $ctx.Repo) { throw "Unable to determine GitHub owner/repo from $repoRoot." }

      if ($shouldUpdateLeaderboard) {
        $msg = "Update leaderboard data $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
        Push-GitHubFile -Owner $ctx.Owner -Repo $ctx.Repo -Branch $ctx.Branch -DestPath 'docs/data.json' -SourcePath $outData -Token $token -ApiBase $ctx.ApiBase -Message $msg | Out-Null
        if (Test-Path $legacyOut) {
          Push-GitHubFile -Owner $ctx.Owner -Repo $ctx.Repo -Branch $ctx.Branch -DestPath 'data/leaderboards.json' -SourcePath $legacyOut -Token $token -ApiBase $ctx.ApiBase -Message $msg | Out-Null
        }
        if (Test-Path $outRoster) {
          Push-GitHubFile -Owner $ctx.Owner -Repo $ctx.Repo -Branch $ctx.Branch -DestPath 'docs/roster_units.json' -SourcePath $outRoster -Token $token -ApiBase $ctx.ApiBase -Message $msg | Out-Null
        }
        if (Test-Path $indexOut) {
          Push-GitHubFile -Owner $ctx.Owner -Repo $ctx.Repo -Branch $ctx.Branch -DestPath 'docs/index.html' -SourcePath $indexOut -Token $token -ApiBase $ctx.ApiBase -Message $msg | Out-Null
        }
        if (Test-Path $versionOut) {
          Push-GitHubFile -Owner $ctx.Owner -Repo $ctx.Repo -Branch $ctx.Branch -DestPath 'docs/version.json' -SourcePath $versionOut -Token $token -ApiBase $ctx.ApiBase -Message $msg | Out-Null
        }
        if (Test-Path $backfillStatusOut) {
          Push-GitHubFile -Owner $ctx.Owner -Repo $ctx.Repo -Branch $ctx.Branch -DestPath 'docs/backfill_status.json' -SourcePath $backfillStatusOut -Token $token -ApiBase $ctx.ApiBase -Message $msg | Out-Null
        }
      }

      if ($shouldSyncSystemSource) {
        try {
          Sync-SystemSourceToGitHub -Owner $ctx.Owner -Repo $ctx.Repo -Branch $ctx.Branch -Token $token -ApiBase $ctx.ApiBase -Mappings $sourceMappings
          $script:LastSourceSyncFingerprint = $sourceFingerprint
        } catch {
          Write-Info "WARN: System source sync failed: $($_.Exception.Message)"
          if ($_.ScriptStackTrace) { Write-Info $_.ScriptStackTrace }
        }
      }
    }

    $script:LastStatsFingerprint = $fp
    $script:LastRunFingerprint = $runFingerprint

    return $true
  }
  catch {
    $script:LastErrorMessage = $_.Exception.Message
    Write-Info "ERROR starting leaderboard generator: $($_.Exception.Message)"
    if ($_.ScriptStackTrace) { Write-Info $_.ScriptStackTrace }
    $shouldPause = ($Host.Name -match 'ConsoleHost|Windows Terminal|Visual Studio Code') -or ($env:WT_SESSION)
    if (-not $Headless -and $shouldPause -and -not $script:IsLoop) {
      Write-Host "Press Enter to exit..." -ForegroundColor Yellow
      [void](Read-Host)
    }
    return $false
  }
}

if ($InstallAutostart -and $RemoveAutostart) {
  throw "Use either -InstallAutostart or -RemoveAutostart, not both."
}
if ($AutostartIntervalSec -lt 1) {
  throw "AutostartIntervalSec must be >= 1."
}
if ($AutostartDelaySec -lt 0) {
  throw "AutostartDelaySec must be >= 0."
}

if ($RemoveAutostart) {
  Remove-AutostartTask -TaskName $AutostartTaskName
}
if ($InstallAutostart) {
  $taskLoopInterval = if ($IntervalSec -gt 0) { $IntervalSec } else { $AutostartIntervalSec }
  Install-AutostartTask -TaskName $AutostartTaskName -LoopIntervalSec $taskLoopInterval -DelaySec $AutostartDelaySec -SkipRoster:$SkipRoster
  Disable-ConflictingLeaderboardTasks -KeepTaskName $AutostartTaskName
}
if ($NoRun) {
  Write-Info "NoRun enabled; exiting after task setup."
  exit 0
}

if ($IntervalSec -lt 0) { $IntervalSec = 0 }
if ($Headless -and $IntervalSec -lt 1) {
  $IntervalSec = $AutostartIntervalSec
  Write-Info "Headless mode enabled with no loop interval; defaulting IntervalSec=$IntervalSec."
}
$script:IsLoop = ($IntervalSec -gt 0)

if (-not (Try-AcquireRunLock)) {
  exit 0
}

try {
  if ($IntervalSec -gt 0) {
    Write-Info "Starting leaderboard loop every $IntervalSec seconds (SkipRoster=$SkipRoster, DryRun=$DryRun, Headless=$Headless)"
    while ($true) {
      $ok = Invoke-LeaderboardRun -SkipRoster:$SkipRoster -DryRun:$DryRun
      if (-not $ok) {
        Write-Info "WARN: leaderboard run failed."
        if (-not $script:LastErrorAt -or ((Get-Date) - $script:LastErrorAt).TotalMinutes -ge 5) {
          $script:LastErrorAt = Get-Date
          Invoke-ErrorHook -Message $script:LastErrorMessage
        }
      }
      Start-Sleep -Seconds $IntervalSec
    }
  } else {
    $ok = Invoke-LeaderboardRun -SkipRoster:$SkipRoster -DryRun:$DryRun
    if (-not $ok) {
      exit 1
    }
  }
} finally {
  Release-RunLock
}

param(
  [switch]$TestMode,
  [switch]$ForceRestart
)

$ErrorActionPreference = 'Stop'

$script:CodeRoot = $PSScriptRoot
if (-not $script:CodeRoot) {
  try {
    $script:CodeRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
  } catch {
    $script:CodeRoot = Get-Location
  }
}
$script:StateRoot = ([string]$env:PPFD_STATE_ROOT).Trim()
if (-not $script:StateRoot) {
  $script:StateRoot = $script:CodeRoot
} else {
  try {
    $script:StateRoot = [System.IO.Path]::GetFullPath($script:StateRoot)
  } catch {}
}

function Write-Info($msg) {
  $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  Write-Output "$ts  $msg"
}

function Get-AlertsProcesses {
  param(
    [string]$ScriptName = 'ppfd_groupme_alerts_v1.py'
  )
  $matches = @()
  try {
    $procs = Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" -ErrorAction SilentlyContinue
    foreach ($p in $procs) {
      try {
        $cmd = $p.CommandLine
      } catch {
        $cmd = $null
      }
      if ($cmd -and $cmd -like "*$ScriptName*") {
        $matches += $p
      }
    }
  } catch {
    # If process inspection fails, fall back to starting a new instance.
  }
  return $matches
}

function Test-PythonExecutable {
  param(
    [string]$ExePath
  )
  if (-not $ExePath) { return $false }
  if (-not (Test-Path $ExePath)) { return $false }

  # Ignore Microsoft Store "python.exe" shim which only opens the Store
  $winApps = Join-Path $env:LocalAppData 'Microsoft\WindowsApps'
  if ($ExePath.StartsWith($winApps, [System.StringComparison]::OrdinalIgnoreCase)) {
    return $false
  }

  # If the file exists and is not the Store shim, treat it as valid
  return $true
}

function Get-CurrentShiftDate {
  param(
    [datetime]$Now = (Get-Date),
    [int]$ShiftHour = 7
  )

  $shiftStart = Get-Date -Date $Now.Date -Hour $ShiftHour -Minute 0 -Second 0
  if ($Now -lt $shiftStart) {
    return $Now.Date.AddDays(-1)
  }
  return $Now.Date
}

function Get-StatsHeartbeat {
  param(
    [Parameter(Mandatory=$true)][string]$StatsDir,
    [int]$StaleMinutes = 15
  )

  $shiftDate = Get-CurrentShiftDate
  $statsPath = Join-Path $StatsDir ("shift_stats_{0}.json" -f $shiftDate.ToString('yyyy-MM-dd'))
  if (-not (Test-Path $statsPath)) {
    return [pscustomobject]@{
      Exists = $false
      IsFresh = $false
      AgeMinutes = [double]::PositiveInfinity
      Path = $statsPath
    }
  }

  $item = Get-Item -Path $statsPath -ErrorAction SilentlyContinue
  if (-not $item) {
    return [pscustomobject]@{
      Exists = $false
      IsFresh = $false
      AgeMinutes = [double]::PositiveInfinity
      Path = $statsPath
    }
  }

  $age = (New-TimeSpan -Start $item.LastWriteTime -End (Get-Date)).TotalMinutes
  return [pscustomobject]@{
    Exists = $true
    IsFresh = ($age -le $StaleMinutes)
    AgeMinutes = $age
    Path = $statsPath
  }
}

try {
  $codeRoot = $script:CodeRoot
  $stateRoot = $script:StateRoot
  if (-not (Test-Path $stateRoot)) {
    New-Item -ItemType Directory -Path $stateRoot -Force | Out-Null
  }
  Set-Location $stateRoot
  $handshake = Join-Path $stateRoot 'startup_handshake.json'
  $statsDir = Join-Path $stateRoot 'data\shift_stats'
  $legacyStatsDir = Join-Path (Split-Path $stateRoot -Parent) 'data\shift_stats'
  $personnelDir = Join-Path $stateRoot 'data\shift_personnel'
  $rosterDir = Join-Path $stateRoot 'TSlogs'

  # Guard against concurrent launches (scheduled task + manual, duplicate triggers, etc.)
  $mutex = $null
  $haveMutex = $false
  try {
    $mutex = New-Object System.Threading.Mutex($false, 'Global\PPFDGroupMeAlerts')
    if ($mutex.WaitOne(0, $false)) {
      $haveMutex = $true
    } else {
      Write-Info "Another PPFD GroupMe start attempt is already running; exiting."
      return
    }
  } catch {
    Write-Info "WARN: Unable to acquire launch mutex: $($_.Exception.Message)"
  }

  # Keep leaderboard stats inside the package folder (for GitHub Pages sync, etc.)
  try {
    if (-not (Test-Path $statsDir)) {
      New-Item -ItemType Directory -Force -Path $statsDir | Out-Null
    }
    if (Test-Path $legacyStatsDir) {
      Get-ChildItem $legacyStatsDir -Filter 'shift_stats_*.json' -ErrorAction SilentlyContinue | ForEach-Object {
        $dest = Join-Path $statsDir $_.Name
        if (-not (Test-Path $dest)) {
          Copy-Item $_.FullName $dest -Force
        }
      }
    }
    $env:SHIFT_STATS_DIR = $statsDir
    Write-Info "Stats directory set to $statsDir"
  } catch {
    Write-Info "WARN: Failed to initialize stats directory: $($_.Exception.Message)"
  }

  try {
    if (-not (Test-Path $personnelDir)) {
      New-Item -ItemType Directory -Force -Path $personnelDir | Out-Null
    }
    $env:PERSONNEL_STATS_DIR = $personnelDir
    Write-Info "Personnel stats directory set to $personnelDir"
  } catch {
    Write-Info "WARN: Failed to initialize personnel stats directory: $($_.Exception.Message)"
  }

  if (Test-Path $rosterDir) {
    try {
      $env:ROSTER_DIR = (Resolve-Path $rosterDir).Path
      Write-Info "Roster directory set to $($env:ROSTER_DIR)"
    } catch {
      Write-Info "WARN: Failed to resolve roster directory: $($_.Exception.Message)"
    }
  }

  $venvPy = Join-Path $stateRoot 'venv\Scripts\python.exe'
  $existing = @(Get-AlertsProcesses)
  if ($existing.Count -gt 0) {
    $local = $existing | Where-Object { $_.ExecutablePath -and ($_.ExecutablePath -ieq $venvPy) }
    $other = $existing | Where-Object { -not $_.ExecutablePath -or ($_.ExecutablePath -ine $venvPy) }
    if (-not $ForceRestart) {
      if ($local) {
        $hb = Get-StatsHeartbeat -StatsDir $statsDir -StaleMinutes 15
        if ($hb.IsFresh) {
          Write-Info "Existing PPFD GroupMe alerts process already running (pid=$($local[0].ProcessId)); heartbeat fresh ($([math]::Round($hb.AgeMinutes,1)) min)."
          return
        }
        if (-not $hb.Exists) {
          Write-Info "Existing alerts process detected but heartbeat file is missing ($($hb.Path)); forcing restart."
        } else {
          Write-Info "Existing alerts process detected but heartbeat is stale ($([math]::Round($hb.AgeMinutes,1)) min old); forcing restart."
        }
        $ForceRestart = $true
      }
      if ($other -and -not $ForceRestart) {
        $paths = ($other | ForEach-Object { $_.ExecutablePath } | Where-Object { $_ } | Sort-Object -Unique) -join ', '
        if (-not $paths) { $paths = 'unknown python path' }
        Write-Info "WARN: Another ppfd_groupme_alerts_v1.py is running ($paths). Run start_groupme.ps1 -ForceRestart to take over."
        return
      }
    }
    if ($ForceRestart) {
      foreach ($p in $existing) {
        try {
          Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop
          Write-Info "Stopped existing alerts process pid=$($p.ProcessId) ($($p.ExecutablePath))"
        } catch {
          Write-Info "WARN: Failed to stop existing alerts process pid=$($p.ProcessId): $($_.Exception.Message)"
        }
      }
    }
  }

  if (Test-Path $handshake) { Remove-Item -Force -ErrorAction SilentlyContinue $handshake }

  function Install-Python {
    param(
      [string]$Version = '3.11.9'
    )
    $urls = @(
      "https://www.python.org/ftp/python/$Version/python-$Version-amd64.exe",
      # Fallback to 3.12.7 if 3.11.x not available
      'https://www.python.org/ftp/python/3.12.7/python-3.12.7-amd64.exe'
    )
    $tmp = Join-Path $stateRoot 'python-installer.exe'
    foreach ($u in $urls) {
      try {
        Write-Info "Downloading Python: $u"
        Invoke-WebRequest -UseBasicParsing -Uri $u -OutFile $tmp -TimeoutSec 120
        if (Test-Path $tmp) { break }
      } catch {
        Remove-Item -Force -ErrorAction SilentlyContinue $tmp
      }
    }
    if (-not (Test-Path $tmp)) { throw 'Unable to download Python installer.' }
    Write-Info 'Installing Python (user, quiet)'
    # Quiet per-user install with pip and PATH updates
    Start-Process -FilePath $tmp -ArgumentList '/quiet InstallAllUsers=0 PrependPath=1 Include_pip=1' -Wait -NoNewWindow
    Remove-Item -Force -ErrorAction SilentlyContinue $tmp
  }

  function Find-UserPython {
    $candidates = @(
      (Join-Path $env:LocalAppData 'Programs\Python\Python311\python.exe'),
      (Join-Path $env:LocalAppData 'Programs\Python\Python312\python.exe'),
      (Join-Path $env:LocalAppData 'Programs\Python\Python313\python.exe')
    )
    foreach ($p in $candidates) {
      if (Test-PythonExecutable $p) { return $p }
    }

    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) {
      $p = $cmd.Source
      if (Test-PythonExecutable $p) { return $p }
    }
    $cmd = Get-Command py -ErrorAction SilentlyContinue
    if ($cmd) {
      $p = $cmd.Source
      if (Test-PythonExecutable $p) { return $p }
    }
    return $null
  }

  # Ensure local venv with deps, downloading Python if needed
  if (-not (Test-Path $venvPy)) {
    $basePy = Find-UserPython
    if (-not $basePy) {
      Install-Python
      $basePy = Find-UserPython
      if (-not $basePy) { throw 'Python installation not found after install.' }
    }
    Write-Info "Creating virtual environment ($basePy -m venv venv)"
    & $basePy -m venv (Join-Path $stateRoot 'venv')
    if (-not (Test-Path $venvPy)) { throw 'Failed to create virtual environment.' }
    Write-Info 'Installing dependencies'
    & $venvPy -m pip install --upgrade pip
    if (Test-Path (Join-Path $codeRoot 'requirements.txt')) {
      & $venvPy -m pip install -r (Join-Path $codeRoot 'requirements.txt')
    } else {
      & $venvPy -m pip install requests
    }
  }
  ${python} = $venvPy
  ${argsList} = @('"' + (Join-Path $codeRoot 'ppfd_groupme_alerts_v1.py') + '"')

  # Environment for quieter logging by default
  if (-not $env:DEBUG_VERBOSE) { $env:DEBUG_VERBOSE = '0' }
  if (-not $env:REQUIRE_STARTUP_CONFIRM) { $env:REQUIRE_STARTUP_CONFIRM = '1' }
  if ($TestMode) { $env:TEST_MODE = '1' } else { $env:TEST_MODE = '0' }
  $env:PPFD_STATE_ROOT = $stateRoot
  # Ensure the Python script can find the tokens file regardless of working dir
  $tokens = Join-Path $stateRoot 'Groupmetokens.txt'
  if (Test-Path $tokens) { $env:GROUPME_TOKENS_FILE = $tokens }

  Write-Info "Launching (detached): $python $($argsList -join ' ')"
  # Detach so this wrapper can exit immediately; Python writes to alerts.log itself
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $python
  $psi.Arguments = ($argsList -join ' ')
  $psi.WorkingDirectory = $stateRoot
  $psi.UseShellExecute = $false
  $psi.CreateNoWindow = $true

  $proc = [System.Diagnostics.Process]::Start($psi)
  if ($proc -and -not $proc.HasExited) {
    Write-Info "Started python pid=$($proc.Id)"
    if (-not $TestMode) {
      $deadline = [DateTime]::UtcNow.AddSeconds(60)
      while (-not (Test-Path $handshake)) {
        if ($proc.HasExited) {
          throw "Python process exited before startup confirmation (exit code $($proc.ExitCode))."
        }
        if ([DateTime]::UtcNow -gt $deadline) {
          throw "Timed out waiting for startup confirmation from GroupMe."
        }
        Start-Sleep -Seconds 2
      }
      Write-Info "Startup confirmation received from GroupMe."
    }
  } else {
    throw "Failed to start python process."
  }

  # Kick off GitHub stats sync loop if configured
  if ($env:GITHUB_STATS_REPO -and $env:GITHUB_TOKEN) {
    try {
      $sync = Join-Path $codeRoot 'sync_stats_to_github.ps1'
      if (Test-Path $sync) {
        $argsSync = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$sync`""
        Write-Info "Starting GitHub stats sync loop ($argsSync)"
        $psExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
        Start-Process -FilePath $psExe -ArgumentList $argsSync -WorkingDirectory $stateRoot -WindowStyle Hidden | Out-Null
      }
    } catch {
      Write-Info "WARN: Unable to start GitHub stats sync: $($_.Exception.Message)"
    }
  }
}
catch {
  Write-Host "ERROR starting GroupMe alerts:" -ForegroundColor Red
  Write-Host ($_.Exception.Message) -ForegroundColor Red
  if ($_.ScriptStackTrace) { Write-Host $_.ScriptStackTrace -ForegroundColor DarkGray }
  $shouldPause = ($Host.Name -match 'ConsoleHost|Windows Terminal|Visual Studio Code') -or ($env:WT_SESSION)
  if ($shouldPause -and -not $env:PPFD_HEADLESS_SETUP) {
    Write-Host "Press Enter to exit..." -ForegroundColor Yellow
    [void](Read-Host)
  }
  exit 1
}
finally {
  try {
    if ($mutex) {
      if ($haveMutex) { $mutex.ReleaseMutex() }
      $mutex.Dispose()
    }
  } catch {
    # ignore cleanup errors
  }
}

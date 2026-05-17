<#
  Sync shift stats JSON files to a GitHub repo using the Contents API.

  Required env vars:
    GITHUB_TOKEN        - GitHub token with repo:contents scope (fine-grained: contents write).
    GITHUB_STATS_REPO   - owner/repo (e.g., youruser/ppfd-leaderboard).

  Optional env vars:
    GITHUB_STATS_BRANCH - branch to commit to (default: main).
    GITHUB_STATS_PATH   - path inside repo to write files (default: data/shift_stats).
    GITHUB_SYNC_INTERVAL_SECONDS - how often to sync (default: 300).
    GITHUB_COMMIT_AUTHOR_NAME / GITHUB_COMMIT_AUTHOR_EMAIL - override author/committer.
    SHIFT_STATS_DIR     - source stats folder (default: .\data\shift_stats next to this script).
#>
param(
  [int]$IntervalSec = 0
)

$ErrorActionPreference = 'Stop'

function Write-Info($msg) {
  $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  Write-Output "$ts  $msg"
}

try {
  $here = Split-Path -Parent $MyInvocation.MyCommand.Path
} catch {
  $here = Get-Location
}

$statsDir = $env:SHIFT_STATS_DIR
if (-not $statsDir) { $statsDir = Join-Path $here 'data\shift_stats' }
$repo     = ($env:GITHUB_STATS_REPO) -replace '\s',''
$branch   = $env:GITHUB_STATS_BRANCH; if (-not $branch) { $branch = 'main' }
$target   = $env:GITHUB_STATS_PATH;   if (-not $target) { $target = 'data/shift_stats' }
$token    = $env:GITHUB_TOKEN
$interval = if ($IntervalSec -gt 0) { $IntervalSec } elseif ($env:GITHUB_SYNC_INTERVAL_SECONDS) { [int]$env:GITHUB_SYNC_INTERVAL_SECONDS } else { 300 }
$authorN  = $env:GITHUB_COMMIT_AUTHOR_NAME
$authorE  = $env:GITHUB_COMMIT_AUTHOR_EMAIL

if (-not $token)  { Write-Info 'GITHUB_TOKEN not set; aborting sync.'; exit 0 }
if (-not $repo)   { Write-Info 'GITHUB_STATS_REPO not set; aborting sync.'; exit 0 }
if (-not (Test-Path $statsDir)) { Write-Info "Stats dir not found: $statsDir"; exit 0 }

# Simple lock to avoid multiple sync loops
$lockPath = Join-Path $here 'github_sync.lock'
if (Test-Path $lockPath) {
  try {
    $existingPid = Get-Content $lockPath | Select-Object -First 1
    if ($existingPid -and (Get-Process -Id $existingPid -ErrorAction SilentlyContinue)) {
      Write-Info "Sync already running (pid=$existingPid); exiting."
      exit 0
    }
  } catch {}
}
try { Set-Content -Path $lockPath -Value $PID -Encoding ASCII -Force } catch {}

function Get-GitHubFileSha {
  param($Path)
  try {
    $url = "https://api.github.com/repos/$repo/contents/$Path?ref=$branch"
    $resp = Invoke-RestMethod -Method Get -Uri $url -Headers @{ Authorization = "Bearer $token"; 'User-Agent' = 'PPFD-Stats-Sync' }
    return $resp.sha
  } catch {
    return $null
  }
}

function Push-File {
  param($SourcePath)
  $name = Split-Path $SourcePath -Leaf
  $destPath = "$target/$name"
  $bytes = [System.IO.File]::ReadAllBytes($SourcePath)
  $sha = Get-GitHubFileSha -Path $destPath
  $body = @{
    message = "chore: update $name"
    content = [Convert]::ToBase64String($bytes)
    branch  = $branch
  }
  if ($sha) { $body.sha = $sha }
  if ($authorN -and $authorE) {
    $author = @{ name = $authorN; email = $authorE }
    $body.author = $author
    $body.committer = $author
  }
  try {
    $url = "https://api.github.com/repos/$repo/contents/$destPath"
    $resp = Invoke-RestMethod -Method Put -Uri $url -Headers @{
      Authorization = "Bearer $token"
      'User-Agent'  = 'PPFD-Stats-Sync'
    } -ContentType 'application/json' -Body ($body | ConvertTo-Json -Depth 6)
    Write-Info "Pushed $name -> $destPath (${branch}), commit: $($resp.content.sha.Substring(0,7))"
    return $true
  } catch {
    Write-Info ("WARN: Push failed for {0}: {1}" -f $name, $_.Exception.Message)
    return $false
  }
}

# Track last push timestamps to avoid needless commits
$state = @{}

function Sync-Once {
  Get-ChildItem -Path $statsDir -Filter 'shift_stats_*.json' -File -ErrorAction SilentlyContinue | ForEach-Object {
    $fp = $_.FullName
    $ts = $_.LastWriteTimeUtc.Ticks
    if ($state[$fp] -eq $ts) { return }
    if (Push-File -SourcePath $fp) {
      $state[$fp] = $ts
    }
  }
}

while ($true) {
  Sync-Once
  if ($interval -lt 10) { break }
  Start-Sleep -Seconds $interval
}

$ErrorActionPreference = 'Stop'

# Resolve repo root (folder containing this script is scripts/)
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

& "$repoRoot\update_pages.ps1"


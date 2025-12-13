param(
  # auto: detect impacted guard levels from git status/diff
  # all : run gate + golden (and optionally physical)
  [ValidateSet('auto','all')]
  [string]$Mode = 'auto',
  [string]$Configuration = 'Release',
  [switch]$Physical
)

$ErrorActionPreference = 'Stop'

function Get-ChangedFiles {
  $files = New-Object System.Collections.Generic.HashSet[string] ([System.StringComparer]::OrdinalIgnoreCase)

  try {
    $d1 = git diff --name-only HEAD 2>$null
    foreach ($f in ($d1 | Where-Object { $_ })) { $null = $files.Add($f) }
  } catch { }

  try {
    $d2 = git diff --name-only --cached 2>$null
    foreach ($f in ($d2 | Where-Object { $_ })) { $null = $files.Add($f) }
  } catch { }

  try {
    $st = git status --porcelain 2>$null
    foreach ($line in ($st | Where-Object { $_ })) {
      # "XY path" or "XY old -> new"
      $path = $line.Substring(3).Trim()
      if ($path -match '^(?<old>.+?)\s+->\s+(?<new>.+)$') { $path = $Matches['new'] }
      if ($path) { $null = $files.Add($path) }
    }
  } catch { }

  return $files.ToArray() | Sort-Object
}

function Any-Match {
  param(
    [string[]]$Files,
    [string[]]$Patterns
  )
  foreach ($f in $Files) {
    foreach ($p in $Patterns) {
      if ($f -match $p) { return $true }
    }
  }
  return $false
}

$repo = Resolve-Path (Join-Path $PSScriptRoot '..')
Push-Location $repo
try {
  $changed = Get-ChangedFiles
  if ($Mode -eq 'all') { $changed = @('__force__') }

  $affectsL4 = Any-Match -Files $changed -Patterns @(
    '^src/Query/Builders/Functions/',
    '^src/Query/Builders/Statements/',
    '^src/Query/Builders/Visitors/',
    '^src/Query/Hub/',
    '^src/Query/Planning/',
    '^src/Query/Analysis/',
    '^src/Query/Dsl/'
  )

  $affectsL3 = $affectsL4

  $affectsHopping = Any-Match -Files $changed -Patterns @(
    '^src/Runtime/Hopping',
    '^src/Query/Dsl/.*Hopping',
    '^tests/Query/Builders/Hopping',
    '^features/hopping/',
    '^examples/Hopping/'
  )

  Write-Host "[run-guard] Mode=$Mode Config=$Configuration"
  if ($changed.Count -gt 0 -and $Mode -eq 'auto') {
    Write-Host "[run-guard] Changed files:"
    $changed | ForEach-Object { Write-Host "  - $_" }
  }

  if (-not $affectsL3 -and -not $affectsL4 -and $Mode -eq 'auto') {
    Write-Host "[run-guard] No L3/L4-impact detected. Running fast (L1|L2)."
    pwsh -NoLogo -File .\tests\run-ut.ps1 -Level fast -Configuration $Configuration
    return
  }

  if ($affectsL3) {
    Write-Host "[run-guard] Running gate (L3)."
    pwsh -NoLogo -File .\tests\run-ut.ps1 -Level gate -Configuration $Configuration
  }

  if ($affectsL4) {
    Write-Host "[run-guard] Running golden (L4)."
    pwsh -NoLogo -File .\tests\run-golden.ps1 -Configuration $Configuration
  }

  if ($Physical) {
    if ($affectsHopping) {
      Write-Host "[run-guard] Running physical Hopping tests (optional)."
      pwsh -NoLogo -File .\physicalTests\run-one-with-reset.ps1 -TestFqn 'Ksql.Linq.Tests.Integration.HoppingPhysicalTests.Hopping_Ctas_StartsAndRegisters' -NoBuild
      pwsh -NoLogo -File .\physicalTests\run-one-with-reset.ps1 -TestFqn 'Ksql.Linq.Tests.Integration.HoppingPhysicalTests.Hopping_CompositeKey_StartsAndRegisters' -NoBuild
    }
    else {
      Write-Host "[run-guard] Physical flag set, but no Hopping impact detected. Skipping physical tests."
    }
  }
}
finally {
  Pop-Location
}


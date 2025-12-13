param(
  [switch]$Write,
  [switch]$Verify,
  [string]$OutPath = "AI_ASSISTANT_GUIDE.md"
)

$ErrorActionPreference = 'Stop'

function NormalizeLf([string]$s) {
  if ($null -eq $s) { return "" }
  return ($s -replace "`r`n", "`n" -replace "`r", "`n")
}

$repo = Resolve-Path (Join-Path $PSScriptRoot '..')
Push-Location $repo
try {
  $sources = @(
    "docs/ai_guide_intro_and_workflows.md",
    "docs/ai_guide_conversation_patterns.md",
    "docs/ai_guide_technical_sections.md"
  )

  foreach ($p in $sources) {
    if (-not (Test-Path $p)) {
      throw "Missing source file: $p"
    }
  }

  $parts = @()
  foreach ($p in $sources) {
    $parts += (Get-Content $p -Raw)
  }

  $expected = NormalizeLf(($parts -join "`n`n"))
  if (-not $expected.EndsWith("`n")) { $expected += "`n" }

  if ($Verify) {
    if (-not (Test-Path $OutPath)) {
      throw "Missing generated file: $OutPath (run with -Write to generate)"
    }
    $actual = NormalizeLf((Get-Content $OutPath -Raw))
    if (-not $actual.EndsWith("`n")) { $actual += "`n" }
    if ($actual -ne $expected) {
      throw "AI_ASSISTANT_GUIDE.md is out of date. Update sources under docs/ai_guide_* and regenerate (tools/build-ai-assistant-guide.ps1 -Write)."
    }
    Write-Host "[ai-guide] OK: $OutPath matches docs/ai_guide_* sources"
  }

  if ($Write) {
    $fullOut = Resolve-Path (Join-Path (Get-Location) $OutPath)
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($fullOut, $expected, $utf8NoBom)
    Write-Host "[ai-guide] Wrote $OutPath from docs/ai_guide_* sources"
  }

  if (-not $Write -and -not $Verify) {
    Write-Host $expected
  }
}
finally { Pop-Location }


param(
  [ValidateSet('L1','L2','L3','L4','L5','fast','gate','all')]
  [string]$Level = 'fast',
  [string]$Configuration = 'Release'
)

$ErrorActionPreference = 'Stop'

function Resolve-Filter {
  param([string]$Level)
  switch ($Level) {
    'L1'   { 'Level=L1' }
    'L2'   { 'Level=L2' }
    'L3'   { 'Level=L3' }
    'L4'   { 'Level=L4' }
    'L5'   { 'Level=L5' }
    'fast' { 'Level=L1|Level=L2' }
    'gate' { 'Level=L3' }
    'all'  { 'Level=L1|Level=L2|Level=L3|Level=L4|Level=L5' }
  }
}

$repo = Resolve-Path (Join-Path $PSScriptRoot '..')
Push-Location $repo
try {
  $filter = Resolve-Filter -Level $Level
  Write-Host "[run-ut] Level=$Level Filter=$filter Config=$Configuration"
  dotnet test tests/Ksql.Linq.Tests.csproj -c $Configuration --filter $filter -v q --nologo
}
finally {
  Pop-Location
}


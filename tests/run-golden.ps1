param(
  [switch]$Approve,
  [string]$Configuration = 'Release'
)
$ErrorActionPreference='Stop'
$repo = Resolve-Path (Join-Path $PSScriptRoot '..')
Push-Location $repo
try {
  if($Approve){ $env:GOLDEN_UPDATE='1' } else { Remove-Item Env:\GOLDEN_UPDATE -ErrorAction SilentlyContinue }
  Write-Host "[run-golden] Approve=$Approve Config=$Configuration"
  dotnet test tests/Ksql.Linq.Tests.csproj -c $Configuration --filter "Level=L4" -v q --nologo
}
finally { Pop-Location }


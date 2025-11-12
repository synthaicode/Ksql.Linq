param(
  [switch]$IncludeLongRun,
  [int]$TimeoutSec = 1800,
  [ValidateSet('quiet','minimal','normal','detailed')]
  [string]$Verbosity = 'detailed'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$proj     = Join-Path $repoRoot 'physicalTests\Ksql.Linq.Tests.Integration.csproj'

# List tests
Write-Host "[run-all-with-reset] Listing tests..."
$listFile = Join-Path $repoRoot '.ptests_list.txt'
dotnet test -c Release $proj --list-tests | Set-Content -Path $listFile
$all = Get-Content $listFile | Where-Object { $_ -match '^\s{4}Kafka\.Ksql\.Linq\.Tests\.Integration\.' } | ForEach-Object { $_.Trim() }
$trimmed = $all | ForEach-Object { $_ -replace '\(.+\)$','' } | Sort-Object -Unique

$long = 'Kafka.Ksql.Linq.Tests.Integration.BarDslLongRunTests.LongRun_1h_Ohlc_And_Grace_Verify'
if (-not $IncludeLongRun) { $targets = $trimmed | Where-Object { $_ -ne $long } } else { $targets = $trimmed }

$batchDir = Join-Path $repoRoot '.pt_batch'
if (!(Test-Path $batchDir)) { New-Item -ItemType Directory -Path $batchDir | Out-Null }
$summary = Join-Path $batchDir ('summary_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '.md')

$lines = @('# Physical Tests Batch (one-by-one with env reset)')
$lines += ("- Timestamp: " + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))
$lines += ("- Project: physicalTests/Kafka.Ksql.Linq.Tests.Integration.csproj")
$lines += ("- Count: " + $targets.Count)
$lines += ("- Verbosity: " + $Verbosity)
if (-not $IncludeLongRun) { $lines += ("- Excluded: " + $long) }
$lines += ''
$lines += '## Failures'

$failCount = 0
$i = 0
foreach($t in $targets){
  $i++
  Write-Host ("[run-all-with-reset] ({0}/{1}) {2}" -f $i,$targets.Count,$t)
  $safe = ($t -replace '[^A-Za-z0-9_.-]','_')
  $before = Get-Date
  & (Join-Path $repoRoot 'physicalTests\run-one-with-reset.ps1') -TestFqn $t -TimeoutSec $TimeoutSec -NoBuild -Verbosity $Verbosity
  $after = Get-Date
  $cleanLog = Join-Path $repoRoot ('.ptresults_clean\' + $safe + '.txt')
  $rawLog   = Join-Path $repoRoot ('.pt_long\' + $safe + '.out.txt')
  $content = if(Test-Path $cleanLog){ Get-Content $cleanLog -Raw } else { '' }
  if ($content -match 'Failed:\s*[1-9]'){
    $failCount++
    $lines += ("- Test: ``" + $t + "``")
    $lines += ("  - Window: " + [int]($after - $before).TotalSeconds + "s")
    $lines += ("  - Clean log: ``" + $cleanLog + "``")
    $lines += ("  - Raw log: ``" + $rawLog + "``")
    $tail = ($content -split "`n") | Select-Object -Last 60
    $lines += '  - Tail:'
    $lines += '```'
    $lines += ($tail -join "`n")
    $lines += '```'
  }
}

if ($failCount -eq 0){ $lines += '- なし（全テスト成功）' }
$lines | Set-Content -Path $summary -NoNewline
Write-Host ("[run-all-with-reset] Done. Failures: {0} | Report: {1}" -f $failCount,$summary)

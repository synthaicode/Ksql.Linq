param(
  [Parameter(Mandatory=$true)] [string]$TestFqn,
  [int]$TimeoutSec = 5400,
  [switch]$NoBuild,
  [ValidateSet('quiet','minimal','normal','detailed')]
  [string]$Verbosity = 'detailed'
)

$ErrorActionPreference = 'Stop'

# Ensure Docker engine is running (Windows: try starting com.docker.service)
$script:EnsureDockerTimeoutSec = 90
function Test-DockerReady {
  try {
    $ver = docker version --format '{{.Server.Version}}' 2>$null
    return -not [string]::IsNullOrWhiteSpace($ver)
  } catch { return $false }
}

function Ensure-DockerEngine {
  if (Test-DockerReady) { return }
  Write-Host "[run-one-with-reset] Docker engine not ready. Trying to start service..."
  try {
    $svc = Get-Service -Name 'com.docker.service' -ErrorAction SilentlyContinue
    if ($null -ne $svc -and $svc.Status -ne 'Running') {
      Start-Service -Name 'com.docker.service' -ErrorAction SilentlyContinue
    }
  } catch { }
  $until = (Get-Date).AddSeconds($script:EnsureDockerTimeoutSec)
  do {
    if (Test-DockerReady) { Write-Host "[run-one-with-reset] Docker engine is ready."; return }
    Start-Sleep -Seconds 3
  } while ((Get-Date) -lt $until)
  throw "Docker engine is not available. Please start Docker Desktop/Engine and retry."
}

$repoRoot   = Resolve-Path (Join-Path $PSScriptRoot '..')
$compose    = Join-Path $repoRoot 'physicalTests\docker-compose.yaml'
$resetScript= Join-Path $repoRoot 'physicalTests\reset.ps1'
$downScript = Join-Path $repoRoot 'physicalTests\down.ps1'
$proj       = Join-Path $repoRoot 'physicalTests\Ksql.Linq.Tests.Integration.csproj'

# Prepare output paths
$safe       = ($TestFqn -replace '[^A-Za-z0-9_.-]','_')
$longDir    = Join-Path $repoRoot '.pt_long'
$cleanDir   = Join-Path $repoRoot '.ptresults_clean'
if (!(Test-Path $longDir))  { New-Item -ItemType Directory -Path $longDir  | Out-Null }
if (!(Test-Path $cleanDir)) { New-Item -ItemType Directory -Path $cleanDir | Out-Null }
$rawFile    = Join-Path $longDir  ($safe + '.out.txt')
$errFile    = Join-Path $longDir  ($safe + '.err.txt')
$cleanFile  = Join-Path $cleanDir ($safe + '.txt')

function Save-KsqlJson {
  param(
    [Parameter(Mandatory=$true)] [string]$Endpoint,
    [Parameter(Mandatory=$true)] [string]$BodyJson,
    [Parameter(Mandatory=$true)] [string]$OutPath
  )
  try {
    $uri = 'http://127.0.0.1:18088' + $Endpoint
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($BodyJson)
    $resp = Invoke-WebRequest -Uri $uri -Method Post -ContentType 'application/vnd.ksql.v1+json; charset=utf-8' -Body $bytes -UseBasicParsing -TimeoutSec 30
    $dir = Split-Path -Parent $OutPath
    if (!(Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
    $resp.Content | Out-File -FilePath $OutPath -Encoding UTF8
    Write-Host "[diag] saved: $OutPath"
  }
  catch {
    Write-Host "[diag] save failed: $Endpoint -> $OutPath : $($_.Exception.Message)"
  }
}

function Wait-KsqlReady {
  param([int]$TimeoutSec=180)
  $uri = 'http://127.0.0.1:18088/healthcheck'
  $until = (Get-Date).AddSeconds($TimeoutSec)
  do {
    try {
      $r = Invoke-WebRequest -Uri $uri -UseBasicParsing -TimeoutSec 5
      if ($r.StatusCode -eq 200) { return $true }
    } catch {}
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $until)
  throw 'ksqlDB healthcheck timeout'
}

function Wait-KafkaPort {
  param([string]$BootstrapHost='127.0.0.1',[int]$Port=39092,[int]$TimeoutSec=120)
  $until = (Get-Date).AddSeconds($TimeoutSec)
  do {
    try {
      $client = [System.Net.Sockets.TcpClient]::new()
      $iar = $client.BeginConnect($BootstrapHost, $Port, $null, $null)
      $ok = $iar.AsyncWaitHandle.WaitOne(2000)
      if ($ok -and $client.Connected) {
        $client.EndConnect($iar)
        $client.Close()
        return $true
      }
      $client.Close()
    } catch {}
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $until)
  throw "Kafka port not reachable: ${BootstrapHost}:${Port}"
}

Ensure-DockerEngine
Write-Host "[run-one-with-reset] Resetting environment (down -v, clean RocksDB, unique ServiceId)..."
& $resetScript | Out-Null

# A) DESCRIBE BAR_1M_LIVE EXTENDED
$null = Wait-KsqlReady -TimeoutSec 180
$descPath = Join-Path $repoRoot 'reports/physical/describe_BAR_1M_LIVE.json'
$descObj  = @{ ksql = "DESCRIBE EXTENDED BAR_1M_LIVE;" }
$descJson = $descObj | ConvertTo-Json -Compress
Save-KsqlJson -Endpoint '/ksql' -BodyJson $descJson -OutPath $descPath

# B) SHOW QUERIES EXTENDED
$null = Wait-KsqlReady -TimeoutSec 180
$queriesPath = Join-Path $repoRoot 'reports/physical/show_queries_extended.json'
$queriesObj  = @{ ksql = "SHOW QUERIES EXTENDED;" }
$queriesJson = $queriesObj | ConvertTo-Json -Compress
Save-KsqlJson -Endpoint '/ksql' -BodyJson $queriesJson -OutPath $queriesPath

# Configure test process env
$null = Wait-KafkaPort -BootstrapHost '127.0.0.1' -Port 39092 -TimeoutSec 180
$env:DOTNET_CLI_UI_LANGUAGE = 'en'
$env:TERM                   = 'dumb'
$env:DOTNET_NOLOGO          = '1'
$env:Logging__LogLevel__Default = 'Debug'
$env:Logging__LogLevel__Kafka   = 'Debug'

# Run tests
$args = @('test','-c','Release')
if ($NoBuild) { $args += '--no-build' }
$args += @($proj,'--logger',"console;verbosity=$Verbosity",'--filter',"FullyQualifiedName=$TestFqn")
Write-Host "[run-one-with-reset] Running: $TestFqn"
$proc = Start-Process -FilePath dotnet -ArgumentList $args -NoNewWindow -PassThru -RedirectStandardOutput $rawFile -RedirectStandardError $errFile
$ok = $proc.WaitForExit($TimeoutSec * 1000)
if (-not $ok) { try { $proc.Kill() } catch {}; Add-Content $rawFile "`n[ERROR] Timeout after $TimeoutSec seconds" }
$exit = $proc.ExitCode

# Clean snapshot
$ansi  = "`e\[[0-9;?]*[ -/]*[@-~]"
$raw   = if (Test-Path $rawFile) { Get-Content $rawFile -Raw } else { '' }
$clean = [regex]::Replace($raw, $ansi, '')
Set-Content -Path $cleanFile -Value $clean -Encoding UTF8

# C) Pull probe on failure
if ($exit -ne 0) {
  $probePath = Join-Path $repoRoot 'reports/physical/pull_BAR_1M_LIVE_probe.json'
  $probeSql  = "SELECT BROKER,SYMBOL,BUCKETSTART,OPEN,HIGH,LOW,KSQLTIMEFRAMECLOSE FROM BAR_1M_LIVE WHERE BROKER='B1' AND SYMBOL='S1' LIMIT 10;"
  $probeObj  = @{ ksql = $probeSql }
  $probeJson = $probeObj | ConvertTo-Json -Compress
  Save-KsqlJson -Endpoint '/query' -BodyJson $probeJson -OutPath $probePath
}

# Minimal presence check
$missing = @()
foreach ($p in @($descPath,$queriesPath)) { if (!(Test-Path $p) -or ((Get-Item $p).Length -eq 0)) { $missing += $p } }
if ($exit -ne 0) {
  $probePath = Join-Path $repoRoot 'reports/physical/pull_BAR_1M_LIVE_probe.json'
  if (!(Test-Path $probePath) -or ((Get-Item $probePath).Length -eq 0)) { $missing += $probePath }
}
if ($missing.Count -gt 0) { Write-Host "[warn] missing diagnostics: $($missing -join ', ')" }

Write-Host "[run-one-with-reset] Done. RAW=$rawFile CLEAN=$cleanFile"
Write-Host "[run-one-with-reset] Stopping environment (post-run)..."
& $downScript | Out-Null

 

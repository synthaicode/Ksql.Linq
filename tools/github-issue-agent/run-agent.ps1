# GitHub Issue Agent - Run Script
# This script is used by Task Scheduler for automated execution

param(
    [string]$Mode = "draft",
    [switch]$Log
)

$ErrorActionPreference = "Continue"

# Get script directory
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# Setup logging
$LogFile = "data/logs/run-$(Get-Date -Format 'yyyyMMdd').log"
$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")

    $LogMessage = "[$Timestamp] [$Level] $Message"

    if ($Log) {
        Add-Content -Path $LogFile -Value $LogMessage -Encoding UTF8
    }

    Write-Host $LogMessage
}

Write-Log "======================================"
Write-Log "GitHub Issue Agent - Starting"
Write-Log "Mode: $Mode"
Write-Log "======================================"

# Check if virtual environment exists
if (Test-Path "venv\Scripts\Activate.ps1") {
    Write-Log "Activating virtual environment..."
    & ".\venv\Scripts\Activate.ps1"
} else {
    Write-Log "Virtual environment not found, using system Python" "WARNING"
}

# Check if .env exists
if (-not (Test-Path ".env")) {
    Write-Log ".env file not found!" "ERROR"
    exit 1
}

# Run agent
Write-Log "Executing agent..."
try {
    python agent.py run --mode $Mode 2>&1 | ForEach-Object {
        Write-Log $_
    }

    $exitCode = $LASTEXITCODE

    if ($exitCode -eq 0) {
        Write-Log "Agent completed successfully" "INFO"
    } else {
        Write-Log "Agent exited with code $exitCode" "WARNING"
    }
} catch {
    Write-Log "Error executing agent: $_" "ERROR"
    exit 1
}

Write-Log "======================================"
Write-Log "GitHub Issue Agent - Finished"
Write-Log "======================================"

exit 0
